/* ==========================================================================
   TurboLay Interactive Textbook — shared concept glossary
   Each entry powers a nested tooltip. `html` may itself contain
   <span class="concept" data-concept="slug">…</span> to spawn child tooltips.
   Keep definitions tight; the chapter body carries the full story.
   ========================================================================== */
window.TB_GLOSSARY = {
  /* ------------------------------ storage foundations ------------------ */
  "object-store": {
    title: "Object store",
    html: "<p>A storage service that holds opaque <em>objects</em> (blobs) under string keys — Amazon S3 is the canonical example. You <code>PUT</code>, <code>GET</code>, and <code>LIST</code> whole objects; there is no in-place edit and no file-system seek.</p><p>It is cheap, effectively infinite, and highly durable, but each request has high latency. That trade-off — durable and shared, but slow per call — is the ground TurboLay is built on.</p>"
  },
  "slatedb": {
    title: "SlateDB",
    html: "<p>An embedded <span class=\"concept\" data-concept=\"lsm\">log-structured</span> key–value database that stores <em>all</em> of its state in an <span class=\"concept\" data-concept=\"object-store\">object store</span> rather than on a local disk. It provides ordered keys, atomic writes, snapshots, and single-writer <span class=\"concept\" data-concept=\"fencing\">fencing</span>.</p><p>TurboLay embeds one SlateDB database per <span class=\"concept\" data-concept=\"cell\">cell</span> and leans on its guarantees instead of building its own locking or replication.</p>"
  },
  "lsm": {
    title: "Log-structured merge tree (LSM)",
    html: "<p>A storage design that never edits data in place. New writes append to an in-memory buffer and a <span class=\"concept\" data-concept=\"wal\">write-ahead log</span>; when the buffer fills it is flushed to an immutable sorted file (an <span class=\"concept\" data-concept=\"sst\">SST</span>). Background <span class=\"concept\" data-concept=\"compaction\">compaction</span> merges those files.</p><p>Append-only immutability is exactly what makes an object store — which cannot edit in place either — a natural home for an LSM.</p>"
  },
  "sst": {
    title: "SST (Sorted String Table)",
    html: "<p>An immutable file of key–value pairs held in sorted key order, so a reader can binary-search it and merge several SSTs in one ordered pass. Once written, an SST is never modified — only superseded and eventually deleted by <span class=\"concept\" data-concept=\"compaction\">compaction</span>.</p>"
  },
  "compaction": {
    title: "Compaction",
    html: "<p>The background job of an <span class=\"concept\" data-concept=\"lsm\">LSM</span> store that merges many small <span class=\"concept\" data-concept=\"sst\">SSTs</span> into fewer larger ones, dropping superseded and deleted keys. It reclaims space and keeps reads fast without ever mutating a live file.</p>"
  },
  "wal": {
    title: "Write-ahead log (WAL)",
    html: "<p>An append-only log written <em>before</em> a change is applied to the main structure, so a crash mid-write can be recovered by replaying it. It is the durability record every committed write already pays for.</p><p>TurboLay reuses SlateDB's WAL for a second purpose: a read folds the recent WAL tail on top of a lagging index so it costs <em>no extra write</em> to stay current.</p>"
  },
  "manifest": {
    title: "Manifest",
    html: "<p>A small, atomically-replaced pointer file that names the current set of live <span class=\"concept\" data-concept=\"sst\">SSTs</span> and the current writer. Reading the manifest tells you the exact committed state of the database; claiming a newer writer epoch in it is how <span class=\"concept\" data-concept=\"fencing\">fencing</span> works.</p>"
  },
  "fencing": {
    title: "Fencing",
    html: "<p>A negative guarantee: once a newer writer claims a database, the older writer is <em>prevented</em> from committing. In SlateDB, opening a writer stamps a newer epoch into the <span class=\"concept\" data-concept=\"manifest\">manifest</span>; the superseded writer discovers it has been fenced the next time it refreshes and is closed rather than allowed to write.</p><p>Because the storage layer enforces this, TurboLay needs no lock, lease, or owner map to keep two writers from corrupting a <span class=\"concept\" data-concept=\"cell\">cell</span>.</p>"
  },

  /* ------------------------------ transactions & versions -------------- */
  "mvcc": {
    title: "MVCC (multi-version concurrency control)",
    html: "<p>Instead of locking a record to read it, the store keeps multiple versions and hands each reader the version current as of some point in time. Readers never block writers and vice-versa; each reader sees a stable past.</p><p>A <span class=\"concept\" data-concept=\"sequence-number\">sequence number</span> is what names that point in time.</p>"
  },
  "snapshot-isolation": {
    title: "Snapshot isolation",
    html: "<p>A transaction reads from one frozen snapshot of the database taken at its start, so every key it reads reflects the same instant no matter how long the transaction runs. Built on <span class=\"concept\" data-concept=\"mvcc\">MVCC</span>.</p><p>TurboLay pins one SlateDB <code>DbSnapshot</code> for the life of a query, which is what makes a long read <em>coherent</em> — it can't drift to newer data halfway through.</p>"
  },
  "serializable": {
    title: "Serializable snapshot transaction",
    html: "<p>The strongest isolation: the outcome is equivalent to running the transactions one-at-a-time in some order. A serializable-snapshot implementation reads from a <span class=\"concept\" data-concept=\"snapshot-isolation\">snapshot</span> and, at commit, aborts if a concurrent transaction touched overlapping data.</p><p>TurboLay wraps one logical edge mutation in a single such transaction and simply <em>retries</em> on conflict.</p>"
  },
  "sequence-number": {
    title: "Storage sequence number",
    html: "<p>A monotonically increasing integer SlateDB assigns to each committed state. Snapshot number <em>S</em> means “every write up to and including S is visible.” It is a logical clock for the database.</p><p>TurboLay calls this an <span class=\"concept\" data-concept=\"epoch\">epoch</span> and — crucially — uses the <em>same</em> value for both record visibility and index freshness, so the two can never disagree.</p>"
  },
  "idempotency": {
    title: "Idempotency",
    html: "<p>An operation is idempotent when doing it twice has the same effect as doing it once. A client that retries after a dropped response must not double-apply the write.</p><p>TurboLay binds an idempotency key to the exact mutation: replaying the same request returns the recorded result, and reusing the key for a <em>different</em> edge is rejected as a conflict.</p>"
  },
  "two-phase-commit": {
    title: "Two-phase commit (2PC)",
    html: "<p>A protocol to make several independent databases commit or abort together: a coordinator asks each to “prepare,” then tells all to commit only if every one voted yes. It is the classic way to get a transaction that spans partitions — and it is exactly what TurboLay deliberately does <em>not</em> build across <span class=\"concept\" data-concept=\"cell\">cells</span>.</p>"
  },

  /* ------------------------------ graph & query ------------------------ */
  "property-graph": {
    title: "Property graph",
    html: "<p>A data model of <em>vertices</em> (nodes) and <em>edges</em> (relationships), where both can carry typed key–value properties and labels. “Insert edge 1→2 of type FOLLOWS” is the atomic change; queries walk edges to answer questions like “friends of friends.”</p>"
  },
  "cypher": {
    title: "Cypher",
    html: "<p>A declarative query language for <span class=\"concept\" data-concept=\"property-graph\">property graphs</span> (from Neo4j, now openCypher). Patterns are drawn in ASCII-art: <code>(a)-[:FOLLOWS]->(b)</code> matches an edge. Like SQL, you describe the shape you want and the engine plans the traversal.</p>"
  },
  "adjacency": {
    title: "Adjacency",
    html: "<p>Who is connected to whom. An <em>adjacency list</em> maps each vertex to the set of its neighbours; an <em>adjacency matrix</em> is the same relation as a grid where cell (i, j) is set when an edge i→j exists.</p><p>TurboLay hydrates adjacency as rows it can traverse quickly, and (as future work) would compress each row as a <span class=\"concept\" data-concept=\"roaring\">Roaring bitmap</span>.</p>"
  },
  "sparse-matrix": {
    title: "Sparse matrix",
    html: "<p>A matrix in which almost every entry is zero, so you store only the non-zeros. A graph's <span class=\"concept\" data-concept=\"adjacency\">adjacency matrix</span> is extremely sparse — a user follows thousands of accounts, not millions — so traversal is really sparse-matrix work.</p>"
  },
  "graphblas": {
    title: "GraphBLAS",
    html: "<p>A standard that expresses graph algorithms as linear algebra over <span class=\"concept\" data-concept=\"sparse-matrix\">sparse matrices</span>: a breadth-first step is a matrix–vector multiply on the <span class=\"concept\" data-concept=\"adjacency\">adjacency matrix</span>. It lets traversal reuse decades of optimized sparse-linear-algebra kernels.</p>"
  },
  "csc": {
    title: "CSC (compressed sparse column)",
    html: "<p>A compact layout for a <span class=\"concept\" data-concept=\"sparse-matrix\">sparse matrix</span> that stores, per column, only the row indices of its non-zeros plus a column-offset array. Compact and cache-friendly for column-wise traversal — TurboLay publishes CSC chunks as a durable accelerator for <span class=\"concept\" data-concept=\"graphblas\">GraphBLAS</span> hydration.</p>"
  },
  "roaring": {
    title: "Roaring bitmap",
    html: "<p>A compressed bitmap (set of integers) that adapts its container per 65 536-value block — bit-array for dense blocks, sorted array for sparse ones — giving small size <em>and</em> fast set operations. A planned change would store each <span class=\"concept\" data-concept=\"adjacency\">adjacency</span> row as a <code>RoaringTreemap</code> in place of a <code>BTreeSet</code>.</p>"
  },
  "btreeset": {
    title: "BTreeSet / BTreeMap",
    html: "<p>Rust's ordered collections, backed by a B-tree, keeping elements sorted for range scans and ordered iteration. TurboLay's current hydrated adjacency row is a <code>BTreeSet&lt;VertexId&gt;</code>; the neighbour set of one vertex, kept in order.</p>"
  },
  "degree": {
    title: "Degree",
    html: "<p>The number of edges incident to a vertex — out-degree counts outgoing edges, in-degree incoming. Storing degree counters lets a query answer “how many followers?” without walking the whole neighbour set, so TurboLay maintains them as part of each write.</p>"
  },

  /* ------------------------------ systems / concurrency ---------------- */
  "semaphore": {
    title: "Semaphore & backpressure",
    html: "<p>A semaphore is a counter that caps how many tasks may hold a resource at once; a task must acquire a permit before proceeding and release it after. Using one to bound concurrent work is <em>backpressure</em> — it keeps a burst of writers from overwhelming the process. It orders work locally; it guarantees nothing across the fleet.</p>"
  },
  "scatter-gather": {
    title: "Scatter / gather",
    html: "<p>A distribution pattern: <em>scatter</em> a request to many workers, run them concurrently, then <em>gather</em> and merge their partial results. TurboLay's distributed query is bounded scatter/gather over <span class=\"concept\" data-concept=\"cell\">cells</span> with a small coordinator merge — no global planner.</p>"
  },
  "consensus": {
    title: "Consensus",
    html: "<p>The problem of getting several nodes to agree on one value under failure (Paxos, Raft). It is powerful but expensive and is what a <em>correctness</em>-bearing routing table would drag in. TurboLay sidesteps it: routing is only a cache <em>hint</em>, and safety comes from storage-layer <span class=\"concept\" data-concept=\"fencing\">fencing</span> instead.</p>"
  },
  "fnv-hash": {
    title: "FNV hash",
    html: "<p>Fowler–Noll–Vo, a tiny non-cryptographic hash: start from a fixed offset, then for each byte XOR it in and multiply by a fixed prime. Fast and well-spread. TurboLay hashes a cell id with it to pick a node — purely to keep the <em>same</em> cell landing on the same warm cache, never to decide correctness.</p>"
  },
  "heap": {
    title: "Heap & cache (runtime state)",
    html: "<p>The <em>heap</em> is a process's in-memory working store; a <em>cache</em> holds recomputable copies of data to avoid slow fetches. Both vanish when a process dies. TurboLay's design turns on the fact that losing them costs only speed, never truth — the durable copy lives in the <span class=\"concept\" data-concept=\"object-store\">object store</span>.</p>"
  },

  /* ------------------------------ TurboLay nouns ----------------------- */
  "cell": {
    title: "Cell",
    html: "<p>TurboLay's unit of storage path, storage sequence, and write authority. Each cell is an independent <span class=\"concept\" data-concept=\"slatedb\">SlateDB</span> database at <code>base_path/cell_id</code> — its own <span class=\"concept\" data-concept=\"manifest\">manifest</span>, <span class=\"concept\" data-concept=\"wal\">WAL</span>, and <span class=\"concept\" data-concept=\"fencing\">fencing</span>. Two cells share the bucket and nothing else, and no transaction spans two of them.</p>"
  },
  "epoch": {
    title: "Epoch",
    html: "<p>In TurboLay an epoch <em>is</em> a SlateDB <span class=\"concept\" data-concept=\"sequence-number\">storage sequence number</span> — nothing more. There is no epoch counter to allocate and no epoch key to read, so a write cannot lose a race for one; it simply reports the sequence its commit landed on.</p>"
  },
  "index-generation": {
    title: "Index generation",
    html: "<p>An immutable, durable query structure (matrix tiles, <span class=\"concept\" data-concept=\"csc\">CSC</span> chunks) rebuilt out-of-process from canonical edges alone, stamped with the <span class=\"concept\" data-concept=\"sequence-number\">base sequence</span> it was built from. It is an accelerator, never a competing truth — it may lag, and the <span class=\"concept\" data-concept=\"wal-tail-overlay\">WAL-tail overlay</span> closes the gap.</p>"
  },
  "writer-promotion": {
    title: "Writer promotion",
    html: "<p>A node opens every <span class=\"concept\" data-concept=\"cell\">cell</span> as a <em>reader</em>. The first write routed to a <em>promotable</em> node lazily opens that cell's SlateDB writer handle — promotion. It is per-cell, leaves no record anywhere else, and two nodes may race to promote because <span class=\"concept\" data-concept=\"fencing\">fencing</span> lets at most one survive.</p>"
  },
  "wal-tail-overlay": {
    title: "WAL-tail overlay",
    html: "<p>The bridge between what an <span class=\"concept\" data-concept=\"index-generation\">index generation</span> knows (up to its base sequence) and what a read must see (its epoch). TurboLay reads the missing interval straight out of SlateDB's <span class=\"concept\" data-concept=\"wal\">write-ahead log</span> — bytes durability already wrote — so the accelerator costs nothing extra on the write path. If the WAL is gone it falls back to reading adjacency from the snapshot.</p>"
  },
  "replaceable-compute": {
    title: "Replaceable compute",
    html: "<p>TurboLay's stance that a compute node holds only <span class=\"concept\" data-concept=\"heap\">runtime state</span> — caches, a writer role, parsed plans — none of which is the sole durable copy of the graph. A replacement node re-opens the same <span class=\"concept\" data-concept=\"object-store\">object-store</span> path and serves the identical graph from a cold start. More precise than “stateless.”</p>"
  }
};
