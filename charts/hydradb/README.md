# HydraDB Helm Chart

This chart deploys HydraDB query nodes and independent graph-indexer workers.
SlateDB stores durable state in object storage and fences stale writers; every
Pod and cache volume is disposable.

## Requirements

- Kubernetes 1.27 or newer.
- An object-store bucket and workload credentials.
- TLS Secrets, or cert-manager with configured issuers.
- External Secrets Operator only when `auth.externalSecret.enabled=true`.
- Prometheus Operator only when `serviceMonitor.enabled=true`.

## Install

Create a production values file from `examples/values-eks.yaml`, replace every account, DNS, issuer, bucket, and image value, then run:

```bash
helm upgrade --install hydradb charts/hydradb \
  --namespace hydradb \
  --create-namespace \
  --values values-production.yaml \
  --atomic \
  --timeout 15m
```

Verify the deployment:

```bash
kubectl -n hydradb get pods,deployments,statefulsets,services
helm test hydradb -n hydradb
```

Query nodes serve Bolt/HTTPS reads and canonical writes. Indexer workers have no
client listener: they open SlateDB as durable readers, build immutable CSC graph
index generations, and publish a compare-and-swap `current` pointer in object
storage. Query nodes discover generations asynchronously and never perform a
full topology build on a request thread. Scale `node.replicaCount` for query
capacity and `indexer.replicaCount` for background indexing capacity.

## Image Publication

`.github/workflows/container.yml` builds the production Dockerfile on pull
requests and publishes `linux/amd64` images to GitHub Container Registry when a
`v*` tag is pushed. Published images are named
`ghcr.io/<repository-owner>/hydradb` and receive the release version, compatible
minor and major versions, the commit SHA, and `latest` tags. Release images
also include an OCI digest, an SBOM, and build-provenance attestation.

The workflow uses the repository-scoped `GITHUB_TOKEN`; no long-lived registry
credentials are required. Grant the workflow `packages: write` permission and
set the resulting GHCR package visibility to match the intended deployment.
Deployment systems should pin the image digest rather than `latest`.

## Security

Public TLS is enabled by default. With cert-manager disabled, provide the release-scoped Secret shown by `helm template`, or set `tls.public.secretName`. The chart can generate a client token, use an existing Secret, or materialize one through External Secrets. Production deployments should use an existing or external secret rather than a token in Helm values.

When `tls.public.trustBundle.enabled=true`, install trust-manager first and put
the configured source Secret in trust-manager's trust namespace. The chart
publishes only public CA certificates into explicitly selected client
namespaces; private keys never leave the source namespace. During a CA rotation
or trust-namespace migration, `tls.public.trustBundle.additionalSources` can
publish the old and new trust anchors together using named Secret or ConfigMap
keys, inline PEM, or trust-manager's default CA package. The release namespace
must match the bundle's namespace selector when the Helm connection test is
used, because the test consumes the distributed ConfigMap rather than reaching
into the trust source namespace.

Each release serves one deployment root containing dynamically selected tenant
and subtenant graph scopes. Clients select a scope with the versioned Bolt
database name, and every scope receives its own `cell-0`, SlateDB WAL, writer
fence, caches, and graph indexes under the shared object-store prefix. Do not
deploy a separate release per tenant or subtenant. `runtime.maxOpenScopes`
bounds warm scopes per query node; idle scopes are closed and reopen from S3.
`runtime.maxWalFlushesBeforeL0Flush` bounds the number of durable WAL objects
that a sparse scope may accumulate before SlateDB consolidates its memtable into
L0. Keep it at or below 4,096; the default of 128 limits cold replay fan-in
without changing the durable WAL acknowledgement path.
`runtime.walFlushIntervalMs` controls that path's maximum group-commit delay.
SlateDB skips empty ticks, and concurrent writes received within the configured
window can share one WAL object. The 10 ms default reduces tiny object-store
writes while keeping durable acknowledgement latency bounded. Valid values are
1 through 1,000 milliseconds.

Client ingress is denied by default. Set `networkPolicy.clientIngressFrom` to
the HydraDB and ingestion namespaces, Pods, or CIDRs that may reach Bolt and
HTTPS. Load balancers should be internal unless public access is explicitly
required.

Outbound HTTPS is also denied by default. Set `networkPolicy.httpsEgressTo` to
private peers that cover, on AWS with IRSA, the private S3 and STS endpoint
addresses. Prefer
interface VPC endpoints with private DNS so this traffic stays inside the VPC.
Kubernetes NetworkPolicy cannot restrict traffic by DNS name, so the chart
rejects empty selectors and universal CIDRs instead of opening TCP/443 to the
Internet.

## Cache Storage

`emptyDir` is the default because S3 is ground truth. Use `persistentVolume` to retain warm SSD cache across pod replacement. Cache loss affects cold-start latency, not durable graph data.

`runtime.slatedbCacheBytes` is a separate process-wide RAM budget shared by
all SlateDB readers and writers in a node or indexer process. It defaults to
640 MiB; it is not multiplied by the number of open graph scopes. Set it to
zero to rely on the disposable disk cache without admitting SlateDB blocks to
RAM. The node exports `graph_slatedb_cache_capacity_bytes`,
`graph_slatedb_cache_resident_bytes`, and `graph_slatedb_cache_entries`.

`runtime.dataCacheMaxOpenFiles` bounds the file handles retained by all
SlateDB disk caches in one graph-node process. The runtime divides it across
`runtime.maxOpenScopes`, graph cells, and possible reader/writer handles. It
must be at least twice the scope and cell product so every cache receives one
handle; the chart and graph node reject smaller values.

Indexer Pods intentionally use disposable temporary storage. Published CSC
generations live in object storage, while query-node NVMe and memory hold only
reconstructible hydrated/compiled copies. The indexer retains
`indexer.retainPreviousGenerations` older generations after each successful
publish; generation keys carry their durable sequence so cleanup uses object
listing rather than downloading large artifacts.

indexer.buildMode defaults to full. Set it to incremental to patch the previous
CSC generation from the durable WAL tail instead of rescanning the entire
canonical adjacency. indexer.incrementalMinEdges keeps smaller indexes on the
full path, and any unavailable or oversized tail safely falls back to a full
rebuild. `indexer.maxWalTailFiles` bounds that tail, while
`indexer.walTailFetchConcurrency` bounds parallel immutable WAL reads and edge
resolution. `indexer.maxArtifactBuildEdges` (default 10M) is the most edges one
relationship type's generation may hold; a type above it gets no generation,
its builds are retried at a backoff that grows to `indexer.fullSweepIntervalMs`,
and those refusals do not count against readiness. Each scheduler pass streams at most `indexer.scopesPerCycle`
registered scopes through `indexer.scopeConcurrency` continuously, so one slow
scope does not leave capacity idle at a batch boundary. One rotation over every
registered scope takes at least `indexer.fullSweepIntervalMs` (default one
hour), its cycles spread evenly across that period; a rotation that falls
behind its pace runs its cycles back to back. `indexer.intervalMs` is the
retry delay after a failed pass. Every scope visit refreshes that scope's
SlateDB reader with object-store LIST and GET requests, and changed scopes
already arrive through write hints, so the sweep is paced as a recovery path
rather than run continuously. The indexer advances a CAS-protected cursor after each contiguous
64-scope checkpoint, bounding crash replay while avoiding a cursor write for
every concurrency window. Resident readers skip the redundant object-store
existence probe and refresh their snapshot directly. Scopes that just published
a generation are rechecked from a bounded hot budget. When the refreshed
storage sequence exactly matches the sequence already covered by that retained
reader, dirty-edge discovery and generation-pointer reads are skipped. Clean
snapshot refreshes run at up to twice `indexer.scopeConcurrency` so object-store
latency can overlap, while changed scopes acquire the original concurrency gate
before dirty discovery, GraphBLAS work, or publication. A changed scope reuses
the sequence obtained by its clean-scope probe instead of refreshing SlateDB a
second time. One-time idle scan readers are closed instead of evicting useful
readers from `indexer.maxOpenScopes`.
Successful graph mutations also persist unique changed-scope hints. After the
durable hint lands, graph nodes send a coalesced wake to the indexer's internal
admin Service using the mounted graph bearer secret. The indexer rejects
unauthenticated wakes. A burst of wakes coalesces into one pending pass, and
passes start at least `indexer.changeMinIntervalMs` apart, so clients cannot
force unbounded object-store discovery and no wake is dropped. The same wake
reaches the planner statistics worker, which lists its request markers no more
than once per `indexer.changeMinIntervalMs` on the same terms. The fast lane
then indexes each changed scope once and removes only the hints included in
that successful build. Without a wake it polls every
`indexer.dirtyPollIntervalMs` (default one minute), so a lost wake or an
unavailable indexer delays work by at most that long and cannot lose it. This keeps normal update discovery independent of total tenant
count and removes the polling delay from healthy writes, while the fair scan
remains a durable recovery path for missed hints and older clients.
`graph_indexer_pending_change_notifications` exposes the current backlog and
`graph_indexer_last_change_lag_ms` reports write-hint-to-index completion lag.
`graph_indexer_change_push_wakes` counts accepted immediate wakes;
`graph_indexer_change_push_rejections` exposes rejected requests. Durable hint
observation and clearing remain the authoritative completion counters.
`graph_indexer_change_notification_failures` should remain zero; a nonzero
value means the fair sweep, rather than the fast lane, is providing recovery.
Set `indexer.maxOpenScopes` at or above the registered scope population when
the indexer memory budget permits it. `graph_indexer_registered_scopes` and
`graph_indexer_scope_cache_capacity` expose that coverage directly; a smaller
cache remains safe but must reopen scopes outside its resident set.
`indexer.readinessFailureThreshold` prevents one transient object-store error
from making the Pod unready while repeated failed bounded cycles still remove
it from service.

`graph_indexer_last_success_ms` reports the last successful bounded scheduler
pass. Registry-wide freshness uses `graph_indexer_last_full_sweep_ms`. Exclude
its zero value so an indexer that has not completed its first rotation displays
as unavailable rather than as a timestamp near the Unix epoch:

```promql
clamp_min(time() - graph_indexer_last_full_sweep_ms / 1000, 0)
and on() (graph_indexer_last_full_sweep_ms > 0)
```

The development example references an existing RustFS Service named `minio`
to exercise the S3-compatible path without AWS. The chart does not install RustFS.
The EKS example leaves the custom endpoint unset so production uses AWS S3 directly.
When a custom HTTP or HTTPS endpoint is configured, the chart derives its port
and creates the matching egress rule. A non-empty
`objectStore.aws.endpointEgressTo` is required while NetworkPolicy is enabled
to restrict that rule to the endpoint namespace, Pod selector, or a
non-universal CIDR. Empty lists, empty label selectors, universal CIDRs, and
peers without a destination selector are rejected before installation.

## Upgrades

Graph nodes use ordered StatefulSet rolling updates with a disruption budget.
Every ready Pod can serve reads through a SlateDB `DbReader`; the Bolt routing
table advertises the active object-store CAS lease owner for writes. Rendezvous
placement chooses the contender when no lease exists, while the lease prevents
different heartbeat views from opening competing writers. SlateDB's writer
epoch and WAL barrier remain the final fence. This is controllerless: the nodes
coordinate through conditional object-store writes, with no writable placement
service in the data path. `runtime.writerLeaseMs` controls the lease window and
accepts values from 3,000 through 300,000 milliseconds; nodes renew it every
one third of that value. Indexer Deployments can roll, fail, or scale
independently without blocking canonical reads or writes. While an index generation lags,
query nodes combine its CSC base with the committed SlateDB WAL tail; if no
usable generation exists, correctness falls back to bounded canonical reads.
Tenant and subtenant scopes are discovered and opened dynamically inside the
release. Use separate releases only for separate environments, security
boundaries, or object-store roots.
