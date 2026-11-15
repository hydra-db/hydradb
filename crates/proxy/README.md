# hydradb proxy

one bolt and http endpoint in front of every graph-node. pools backend
connections, sends every write to its tenant's owner, routes reads by
`GRAPH_READ_ROUTING` (`owner` or `fleet`), answers bolt ROUTE with the ready
proxy replicas, and retries what never ran.

## crates

many small crates, so an edit rebuilds only its crate and what sits above it.
heavy deps sit in leaves that rarely change

- `types`: plain data and the pure retry policy `classify`. every other crate
  depends on it, so it holds nothing else
- `topology`: membership (static `GRAPH_BOLT_NODE_ADDRESSES`) and the replica
  set (static, a polled file, or k8s), merged into one `RouteSnapshot`
- `k8s`: the EndpointSlice replica source. the only crate that builds kube
- `core`: placement, router, owner cache, per node admission, metrics. no
  protocol or pingora types
- `bolt`: the bolt adapter over any `AsyncRead + AsyncWrite`. no pingora
- `http`: the pingora `ProxyHttp` adapter. the only crate that builds
  pingora's http side and boringssl
- `bin`: config, the pingora server, admin endpoint, tls
- `testkit`: fake graph-nodes, a bolt client, a process spawner, the load
  generator, the null relay

shared with graph-node, in `crates/`: `scope` (scope identity and its
spellings), `protocol` (bolt framing, handshakes, codes, the owner hint),
`placement` (`rank`, built without `store` here so no slatedb)

## running it locally

none of this needs native graph libraries

- `just test-proxy`: unit tests, every bolt branch in process, and the multi
  replica tests that spawn real proxy processes in front of fake nodes. the
  replica set comes from a file the tests rewrite, which is what EndpointSlice
  events do in k8s
- `just test-proxy-image`: the k8s source and the image feature set
- `just proxy-e2e`: three real graph-nodes in docker with RustFS, two proxies
  on the host, driven by the official neo4j driver under both read policies.
  needs colima and a python 3.10+ (`PYTHON=...`)
  Storage images use the shared digest pins. Override them with
  `GRAPH_RUSTFS_IMAGE` and `GRAPH_RC_IMAGE`.
- `just proxy-bench`: throughput and cpu against a fake fleet, with a direct
  and a null relay baseline
- `just proxy-bench-sweep`: relay against proxy, repeated with alternating
  order, bolt and http at 2, 4 and 8 threads
- `just proxy-microbench`: per unit cost of the decisions and the framing

## config

shared names keep graph-node's spelling and defaults:
`GRAPH_BOLT_NODE_ADDRESSES`, `GRAPH_READ_ROUTING`, `GRAPH_DATABASE`, `GRAPH_NAMESPACE`, `GRAPH_ID`,
`GRAPH_CELL_ID`, `GRAPH_AUTH_TOKEN_FILE`, `GRAPH_ALLOW_PLAINTEXT`,
`GRAPH_TLS_CERTIFICATE`, `GRAPH_TLS_PRIVATE_KEY`. a blank value counts as
unset, as on graph-node

proxy only, all `TURBOLAY_PROXY_`:

- `BOLT_ADDR`, `HTTP_ADDR`, `ADMIN_ADDR`, `ADVERTISED_BOLT_ADDR`
- `HTTP_NODE_ADDRESSES`: same `id=host:port` format and ids as the bolt map.
  http is off without it
- `SET`: `static` (with `PEERS`), `file` (with `PEERS_FILE`, `PEERS_POLL_MS`)
  or `k8s` (with `K8S_NAMESPACE`, `K8S_SERVICE`, `K8S_PORT`)
- `ADVERTISE`: `replicas` or `service:<host:port>`
- `UPSTREAM_H2`: h2c to the nodes, on by default. `MAX_H2_STREAMS` caps the
  streams per upstream connection
- `NODE_BUDGET`, `ADMIT_WAIT_MS`, `ROUTE_TTL_SECS`, `MAX_ATTEMPTS`,
  `THREADS`

admin: `/readyz`, `/metrics`, `/debug/route?database=..&access=read|write`
