# Turbolay Architecture

Turbolay is a Rust graph database built on SlateDB. S3-compatible object
storage is the durable source of truth. Query and indexing processes are
stateless with respect to durable graph state; their memory and SSD/NVMe data
are disposable caches.

SlateDB owns writer fencing, commit ordering, WAL durability, storage
snapshots, compaction, and object-store coordination. Turbolay owns the graph
model, canonical graph records, query planning, OpenCypher execution,
content-addressed graph indexes, GraphBLAS traversal, public protocols, and
operational limits.

## Design Invariants

- Every namespace, graph, and cell has one active SlateDB writer and any number
  of SlateDB readers.
- Every ready query node can serve reads for every configured cell.
- Every promotable query node can receive a write; SlateDB's writer epoch and
  WAL barrier decide which writer may commit.
- A preferred writer address is cache affinity, not correctness authority.
- Every query pins one SlateDB storage snapshot.
- Canonical graph records remain correct without a graph index.
- Graph indexes are immutable, asynchronous, and reconstructible.
- Local memory and SSD/NVMe loss affects latency, not durability.
- No writable graph controller or separate consensus database is in the data
  path.

## High-Level Architecture

```mermaid
flowchart TB
    C["Applications<br/>Neo4j drivers / HTTPS clients"]
    LB["Kubernetes Service / Load Balancer"]

    subgraph Q["Stateless Query Tier"]
        Q1["Query Node 1<br/>Bolt + HTTPS<br/>Reads + writes"]
        Q2["Query Node 2<br/>Bolt + HTTPS<br/>Reads + writes"]
        QN["Query Node N<br/>Bolt + HTTPS<br/>Reads + writes"]
    end

    subgraph I["Stateless Indexing Tier"]
        I1["Graph Indexer 1"]
        IN["Graph Indexer N"]
    end

    subgraph LC["Disposable Local State"]
        MEM["Bounded memory<br/>Plans + GraphBLAS matrices + result caches"]
        SSD["SSD / NVMe<br/>SlateDB object cache"]
    end

    subgraph OS["Durable Object Store"]
        WAL["SlateDB WAL"]
        SST["SlateDB manifests + compacted SSTs"]
        IDX["Immutable CSC generations"]
        PTR["Atomic current-index pointers"]
    end

    C --> LB
    LB --> Q1
    LB --> Q2
    LB --> QN

    Q1 <--> MEM
    Q1 <--> SSD
    Q2 <--> MEM
    Q2 <--> SSD
    QN <--> MEM
    QN <--> SSD

    Q1 <--> WAL
    Q2 <--> WAL
    QN <--> WAL
    Q1 <--> SST
    Q2 <--> SST
    QN <--> SST
    IDX --> Q1
    IDX --> Q2
    IDX --> QN
    PTR --> Q1
    PTR --> Q2
    PTR --> QN

    WAL --> I1
    SST --> I1
    WAL --> IN
    SST --> IN
    I1 --> IDX
    IN --> IDX
    I1 --> PTR
    IN --> PTR
```

The architecture separates live request processing from expensive index
construction. Query nodes serve public reads and canonical writes. Indexers
independently build sparse traversal indexes and publish them to object
storage. Both tiers can be replaced or scaled without moving durable graph
state.

## Client And Protocol Layer

```mermaid
flowchart LR
    APP["Application"]
    BOLT["Bolt 5.1-5.4 over TLS"]
    HTTP["HTTPS JSON / NDJSON"]
    SERVICE["ClientQueryService"]
    AUTH["Authentication<br/>Namespace authorization"]
    LIMITS["Quotas, deadlines<br/>cancellation, result limits"]
    CLASSIFY["OpenCypher access classification"]
    ENGINE["Query engine"]

    APP --> BOLT
    APP --> HTTP
    BOLT --> SERVICE
    HTTP --> SERVICE
    SERVICE --> AUTH
    AUTH --> LIMITS
    LIMITS --> CLASSIFY
    CLASSIFY -->|"Read"| ENGINE
    CLASSIFY -->|"Write / delete"| ENGINE
```

The shared client service performs the following work before a request reaches
the graph engine:

1. Authenticate the Bolt or HTTPS session.
2. Resolve and authorize the namespace, graph, and cell scope.
3. Classify the OpenCypher statement as read or write before authorization.
4. Validate parameters and supported bounded batch forms.
5. Enforce global and namespace concurrency, query size, runtime, page, and
   result-memory limits.
6. Validate bookmarks and apply the requested read-consistency mode.
7. Create a scoped cancellation token and execute the request.
8. Return bounded rows, a server-side cursor when needed, and a durable
   sequence bookmark.

Bolt supports Neo4j-driver-compatible auto-commit `RUN`, `PULL`, `DISCARD`,
`RESET`, `GOODBYE`, and `ROUTE`. Explicit cross-query transactions remain
unsupported until their semantics can be guaranteed.

## Query Node Internals

```mermaid
flowchart TB
    REQUEST["Authorized query"]
    PARSE["OpenCypher parser"]
    PLAN["Logical + physical planner"]
    SNAP["Pin SlateDB snapshot"]
    ROUTE{"Query type"}

    INDEX["Discover current CSC generation"]
    CACHE["Hydrate from memory / NVMe / S3"]
    GB["Compiled GraphBLAS traversal"]
    TAIL["Apply committed WAL tail"]
    ROW["Row engine<br/>filters, joins, projection, ordering"]
    WRITE["Canonical mutation executor"]
    RESULT["Bounded result / cursor / bookmark"]

    REQUEST --> PARSE --> PLAN --> SNAP --> ROUTE
    ROUTE -->|"Traversal"| INDEX --> CACHE --> GB --> TAIL --> ROW
    ROUTE -->|"Metadata or indexed lookup"| ROW
    ROUTE -->|"Mutation"| WRITE
    ROW --> RESULT
    WRITE --> RESULT
```

Each query uses exactly one `DbSnapshot` or `DbReaderSnapshot`. Canonical graph
records, metadata, indexes, tombstones, and idempotency state are therefore
evaluated at one durable SlateDB storage sequence.

Suitable sparse traversals use the compiled GraphBLAS path. The row engine
handles metadata predicates, relationship identity and properties, ordering,
projection, aggregation, pagination, and query forms that do not map to the
sparse kernel.

## Read Consistency And Index Overlay

Turbolay exposes two read-consistency modes.

```mermaid
flowchart TB
    READ["Read request"]
    MODE{"Consistency mode"}

    CAUSAL["Use local durable reader view"]
    BOOKMARK{"Bookmark supplied?"}
    WAIT["Refresh until bookmark sequence is visible"]
    STRONG["Refresh SlateDB reader from object storage"]
    PIN["Pin storage snapshot at sequence M"]

    BASE["Load CSC built through sequence N"]
    WALTAIL["Read committed topology changes N+1 through M"]
    COMBINE["GraphBLAS base + exact WAL overlay"]
    RETURN["Return rows + bookmark M"]

    READ --> MODE
    MODE -->|"Causal"| CAUSAL --> BOOKMARK
    BOOKMARK -->|"No"| PIN
    BOOKMARK -->|"Yes"| WAIT --> PIN
    MODE -->|"Strong"| STRONG --> PIN
    PIN --> BASE
    BASE --> WALTAIL
    WALTAIL --> COMBINE
    COMBINE --> RETURN
```

### Causal reads

Causal is the default low-latency mode. It uses the node's current durable
reader view. Without a bookmark it performs no mandatory object-store freshness
check. With a bookmark, the reader refreshes until it can observe at least the
bookmark's storage sequence.

### Strong reads

Strong mode refreshes the SlateDB reader from object storage before pinning the
query snapshot. It observes durable writes committed before that refresh
completed and intentionally pays the object-store freshness cost.

### Indexed base plus WAL tail

An index generation built at sequence `N` remains usable for a query pinned at
sequence `M`. The engine reads affected topology records from the committed WAL
range `N+1..M`, resolves their final state from the same pinned snapshot, and
overlays those changes on the immutable CSC base. A query therefore does not
need to wait for the next indexing cycle to observe recent committed writes.

If the necessary WAL tail has already been compacted away, correctness falls
back to bounded source-scoped canonical reads rather than returning stale
results.

## Write And Delete Path

```mermaid
sequenceDiagram
    participant C as Client
    participant Q as Query Node
    participant G as GraphShard
    participant D as SlateDB
    participant S as Object Store

    C->>Q: CREATE / MERGE / DELETE / batch
    Q->>G: Ensure local writer
    G->>D: Lazily promote to writer
    D->>S: Claim newer writer epoch using CAS
    S-->>D: Writer fence acquired
    G->>D: Serializable mutation transaction
    Note over G,D: Canonical graph records, metadata,<br/>indexes, counts, idempotency,<br/>tombstones, and dirty markers
    D->>S: Commit WAL durably
    S-->>D: Durable sequence M
    D-->>G: Commit successful
    G-->>Q: Invalidate affected local caches
    Q-->>C: Success + bookmark M
```

Writes, deletes, detach deletes, relationship mutations, imports, and supported
batch operations all use the same writer-fenced path. Successful API return
means the mutation has reached SlateDB's durable commit point.

If another query node obtains a newer SlateDB writer epoch, the previous writer
is fenced and cannot commit. The stale node closes its writer handle and may
continue serving reads through `DbReader`.

`CREATE` intentionally creates new graph entities and is not inherently safe
to retry after an ambiguous client response. Retriable production mutations
should use stable-ID `MERGE` forms or explicit idempotency keys.

## Indexer Architecture

```mermaid
flowchart TB
    LOOP["Periodic index cycle"]
    REFRESH["Refresh SlateDB reader"]
    DIRTY["Find dirty edge types"]
    SNAP["Pin durable snapshot at sequence N"]
    ADJ["Read canonical adjacency"]
    CSC["Build canonical CSC matrix"]
    HASH["Encode + content hash"]
    PUT["Create immutable generation object"]
    CAS["CAS current pointer"]
    GC["Retain configured previous generations"]

    LOOP --> REFRESH --> DIRTY
    DIRTY --> SNAP --> ADJ --> CSC --> HASH --> PUT --> CAS --> GC
    GC --> LOOP
```

Indexer workers have no client query listener and never become graph writers.
They:

1. Refresh their SlateDB readers.
2. Discover edge types marked dirty by canonical mutations.
3. Pin a durable snapshot and materialize canonical adjacency.
4. Encode one canonical compressed-sparse-column matrix.
5. Derive a content hash and create an immutable generation object.
6. Atomically advance the small `current` pointer with object-store CAS.
7. Garbage-collect older generations while retaining the configured rollback
   window.

Multiple indexers may inspect the same dirty work. Duplicate computation is
possible, but content-addressed objects and CAS publication prevent current
index regression. Indexer outage increases WAL-tail work but does not block
canonical reads or writes.

## Cache And Storage Hierarchy

```mermaid
flowchart LR
    QUERY["Query"]
    RAM["Memory<br/>parsed plans<br/>compiled GraphBLAS<br/>bounded relationship rows"]
    NVME["SSD / NVMe<br/>SlateDB SST and object cache"]
    S3["Object store<br/>WAL + SST + manifests<br/>immutable CSC indexes"]

    QUERY --> RAM
    RAM -->|"Miss"| NVME
    NVME -->|"Miss"| S3
    S3 -->|"Populate"| NVME
    NVME -->|"Hydrate / compile"| RAM
```

| Tier | Contents | Durable |
| --- | --- | --- |
| Memory | Parsed row queries, compiled GraphBLAS matrices, generation metadata, and bounded relationship-result caches | No |
| SSD/NVMe | SlateDB object and SST cache plus hydrated object data | No |
| Object store | SlateDB WAL, manifests, compacted SSTs, immutable CSC generations, and current pointers | Yes |

Memory caches are bounded by entry counts and resident-byte limits. Oversized
results remain correct but are not retained. Losing a query-node Pod, memory
cache, or cache volume changes cold-start latency only.

## Namespace And Object-Store Layout

```text
<base>/
+-- _graph_scopes/v1/<graph-id>/<root-namespace>/
    +-- <encoded-tenant>/<encoded-subtenant>/__scope__
+-- namespaces/<tenant>/
    +-- subnamespaces/<subtenant>/...
        +-- graphs/<graph-id>/
            +-- <cell-id>/
                +-- SlateDB manifests
                +-- SlateDB WAL
                +-- SlateDB compacted SSTs
                +-- _graph_index/
                    +-- <cell-id>/<edge-type>/
                        +-- current
                        +-- generations/
                            +-- <sequence>-<content-hash>.csc
```

The namespace hierarchy supports a tenant plus up to seven nested
subnamespaces. A graph is selected within that namespace path, and each cell is
opened as an independent SlateDB database under the graph. Consequently, every
`(namespace path, graph, cell)` has independent WAL, snapshots, writer fencing,
indexes, and cache locality.

Bolt adapters select these scopes with a versioned database name:
`<base-database>.scope1.<base64url-tenant>.<base64url-subtenant>`. The encoded
components themselves are the storage-safe child namespace IDs; `_` denotes an
absent subtenant. This mapping is deterministic and collision-free for opaque
UTF-8 tenant and collection identifiers. Query nodes lazily open a bounded
number of scopes, while immutable `__scope__` markers let indexers discover
them with object-store LIST operations and no writable catalog service.

Canonical graph keys are records inside SlateDB WAL/SST data. They include
outbound topology, segment data and tombstones, vertex and relationship
metadata, property indexes, degree/count records, idempotency state, and cell
lifecycle markers. They are not emitted as one S3 object per graph record.

## Multi-Node Routing And Failover

```mermaid
flowchart TB
    ROUTE["Bolt ROUTE + configured node directory"]
    HEALTH["HTTP readiness probes"]
    READERS["All ready query nodes<br/>READ"]
    PREF["Stable preferred node<br/>WRITE"]
    FAILURE["Preferred node unavailable"]
    NEXT["Another ready node receives write"]
    PROMOTE["SlateDB promotion<br/>new writer epoch"]
    FENCE["Previous writer fenced"]

    ROUTE --> HEALTH
    HEALTH --> READERS
    HEALTH --> PREF
    PREF --> FAILURE --> NEXT --> PROMOTE --> FENCE
```

Kubernetes or static runtime configuration supplies node membership. Readiness
probes determine which addresses Bolt may advertise. Every ready node is a read
candidate; one stable address is preferred for writes to preserve writer and
cache locality.

There is no controller or external leader-election service. Failover is
request-driven: rendezvous placement selects a contender, which must win a
server-timestamped object-store CAS lease before it may open the SlateDB writer.
The lease is renewed while the writer is held and lease loss closes that handle.
SlateDB's writer epoch and WAL barrier remain the final split-writer protection.

## Authority Boundaries

| Concern | Authoritative component |
| --- | --- |
| Durable graph state | SlateDB data in object storage |
| Writer admission | Object-store CAS lease scoped by graph and cell |
| Final writer fencing | SlateDB writer epoch and WAL barrier |
| Query snapshot | Query-scoped SlateDB snapshot |
| Causal position | SlateDB durable sequence bookmark |
| Latest traversal index | Object-store CAS `current` pointer |
| Canonical graph correctness | SlateDB graph records plus tombstones |
| Node membership | Kubernetes or configured static directory |
| Node eligibility | Runtime readiness probe |
| Preferred writer | Bolt routing hint only |
| Memory and NVMe state | Disposable performance cache only |

In compact form, Turbolay is:

```text
stateless query compute
        +
stateless indexing compute
        +
disposable memory and NVMe caches
        +
SlateDB single-writer / multi-reader storage
        +
S3 durability, CAS, and shared ground truth
```
