# Turbolay Helm Chart

This chart deploys Turbolay query nodes and independent graph-indexer workers.
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
helm upgrade --install turbolay charts/turbolay \
  --namespace turbolay \
  --create-namespace \
  --values values-production.yaml \
  --atomic \
  --timeout 15m
```

Verify the deployment:

```bash
kubectl -n turbolay get pods,deployments,statefulsets,services
helm test turbolay -n turbolay
```

Query nodes serve Bolt/HTTPS reads and canonical writes. Indexer workers have no
client listener: they open SlateDB as durable readers, build immutable CSC graph
index generations, and publish a compare-and-swap `current` pointer in object
storage. Query nodes discover generations asynchronously and never perform a
full topology build on a request thread. Scale `node.replicaCount` for query
capacity and `indexer.replicaCount` for background indexing capacity.

## Image Publication

`.github/workflows/container.yml` builds the production Dockerfile on pull
requests and publishes `linux/amd64` images to the regional
`staging/turbolay` Amazon ECR repository after a push to `main`. It follows the
HydraDB deployment convention by publishing the full commit SHA and `latest`
tags through the shared AWS role. Every published image also has an OCI digest,
an SBOM, and build-provenance attestation. Argo CD is pinned to the digest and
never deploys by `latest`.

The publisher uses GitHub OIDC to assume the organization-provided
`AWS_ROLE_ARN` secret, matching `hydradb-application`. Do not store AWS access
keys in GitHub. The ECR repository and role access are provisioned through the
team's existing AWS infrastructure process.

After publishing, the workflow checks out `usecortex/hydradb-argocd` with the
existing `INFRA_REPO_TOKEN`, synchronizes this canonical chart into the infra
repository, updates the staging tag and digest, validates the Helm release, and
pushes the deployment commit to `main`. This is the same promotion path used by
HydraDB application and ingestion services.

Set the `TURBOLAY_STAGING_DEPLOY_ENABLED` repository variable to `true` only
after the staging ECR repository, S3 roles, certificates, and client-auth Secret
exist. The workflow then uses the shared `ARGOCD_AUTH_TOKEN` to refresh, sync,
and wait for the `turbolay-staging` application to become healthy.

EKS nodes pull the private image through their IAM node role. Attach
`AmazonEC2ContainerRegistryPullOnly` or equivalent repository-scoped pull
permissions; no registry password or Kubernetes image-pull Secret is required.

## Security

Public TLS is enabled by default. With cert-manager disabled, provide the release-scoped Secret shown by `helm template`, or set `tls.public.secretName`. The chart can generate a client token, use an existing Secret, or materialize one through External Secrets. Production deployments should use an existing or external secret rather than a token in Helm values.

When `tls.public.trustBundle.enabled=true`, install trust-manager first and
configure its trust source namespace to this release namespace. The chart then
publishes only the public CA certificate into explicitly selected client
namespaces; private keys never leave the Turbolay namespace.

Each release serves one deployment root containing dynamically selected tenant
and subtenant graph scopes. Clients select a scope with the versioned Bolt
database name, and every scope receives its own `cell-0`, SlateDB WAL, writer
fence, caches, and graph indexes under the shared object-store prefix. Do not
deploy a separate release per tenant or subtenant. `runtime.maxOpenScopes`
bounds warm scopes per query node; idle scopes are closed and reopen from S3.

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
resolution. Registered scopes run in batches of `indexer.scopeConcurrency`.
The indexer advances a CAS-protected cursor after each completed batch, so a
restart resumes fairly and can replay at most the unfinished batch. Read-only
scope handles remain open in a bounded LRU of `indexer.maxOpenScopes`; retained
SlateDB readers refresh only the new durable WAL instead of replaying the whole
tail on every cycle.

The development example references an existing MinIO Service only to exercise
the S3-compatible path without AWS. The chart does not install MinIO, and the
EKS example leaves the custom endpoint unset so production uses AWS S3 directly.
When a custom HTTP or HTTPS endpoint is configured, the chart derives its port
and creates the matching egress rule. A non-empty
`objectStore.aws.endpointEgressTo` is required while NetworkPolicy is enabled
to restrict that rule to the endpoint namespace, Pod selector, or a
non-universal CIDR. Empty lists, empty label selectors, universal CIDRs, and
peers without a destination selector are rejected before installation.

## Upgrades

Graph nodes use ordered StatefulSet rolling updates with a disruption budget.
Every ready Pod can serve reads through a SlateDB `DbReader`; the Bolt routing
table advertises one stable Pod as the preferred writer to preserve cache
locality. This preference is disposable: SlateDB's writer epoch and WAL barrier
remain the authoritative fence. No writable controller or placement database is
part of the data path. Indexer Deployments can roll, fail, or scale independently
without blocking canonical reads or writes. While an index generation lags,
query nodes combine its CSC base with the committed SlateDB WAL tail; if no
usable generation exists, correctness falls back to bounded canonical reads.
Tenant and subtenant scopes are discovered and opened dynamically inside the
release. Use separate releases only for separate environments, security
boundaries, or object-store roots.
