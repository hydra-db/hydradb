//! The write path: `UpsertNode` / `UpsertEdge` / `DeleteNode` / `DeleteEdge`
//! (RFC 0004 §"Write path"), plus recovery on open (§"Logical sequence
//! protocol"). **M1 deliverables 5 and 6.**
//!
//! [`Writer`] is the single-writer session (RFC 0002 D2): it owns the
//! [`GraphStorage`] handle, the [`GraphAllocators`] (uid/pred/label/prop/seq
//! id-spaces), the [`SchemaCache`] (name interning accelerator), and the
//! writer's own `latest_seq` cursor. Every public op method lowers its request
//! into exactly **one** `Vec<RecordOp>`, folds in whatever the building blocks
//! it calls hand back (xid resolution, schema interning, posting adds/deletes,
//! seq-block reservations), appends the changelog entry and `Meta["latest_seq"]`
//! bump, and commits it all in **one** [`GraphStorage::apply_with_options`]
//! call with `await_durable: true` and an injected `seqnum` equal to the
//! allocated logical seq — so turbolay's logical seq and SlateDB's durable seq
//! are the same number (M1 integration point #2, the
//! `2026-07-03-m1-spike-and-seqnum-decision.md` "Option A" decision). The
//! method returns that seq as the session token.
//!
//! # What this module reuses, verbatim
//!
//! - [`crate::ids::resolve_or_create_xid_batched`] / [`GraphAllocators`] — xid
//!   resolution and every id-space allocation, folding their `RecordOp`s.
//! - [`crate::posting_ops::{add, delete_node_op, delete_edge_op, neighbors}`] —
//!   all adjacency mutation and the read-side filtered resolve.
//! - [`crate::schema::SchemaCache`] — the name→id accelerator; new names are
//!   interned here (this module owns writing the `SchemaName`/`SchemaId`
//!   records; `SchemaCache` itself never writes).
//! - [`crate::value::{V0NodeCodec, ChangeRecord, NodeRecord}`] — node encoding
//!   (with its 1 MiB cap) and the changelog value.
//! - [`crate::value::{encode_edge_props, decode_edge_props}`] — the
//!   `EdgeProp` companion codec ([`Self::upsert_edge_with_props`],
//!   [`Self::edge_props`]; Workstream A, RFC 0005 §"Edge facets").
//!
//! # Degree counter (D5's "optionally maintain a degree/count Meta counter")
//!
//! `UpsertEdge`/`DeleteEdge` bump a per-`(pred, dir, anchor)` `i64` counter
//! under `Meta["count/<pred><dir><anchor>"]` via the merge operator's
//! **counter-sum** row (`crate::merge`, `MetaKind::Counter`). This is the
//! simplified M1 counter the handoff calls out as optional — **not** the full
//! RFC 0006 degree-bucket `Count` index (which *moves* an anchor between
//! `Count[pred][dir][old_deg]`/`Count[pred][dir][new_deg]` bucket keys and is
//! M2 scope). Being a plain merge-sum, it is O(1)/degree-independent (no read
//! before the bump) but, as a consequence, **not idempotent**: re-`UpsertEdge`ing
//! an already-present edge (or `DeleteEdge`ing an already-absent one) skews the
//! count. Acceptable for M1 because the *adjacency* posting itself (via
//! `posting_ops::add`'s underlying set union) stays perfectly idempotent
//! regardless; only this secondary counter can drift under duplicate calls.
//!
//! # Split/rollup: post-commit, not in-band
//!
//! [`crate::posting_ops::maybe_split`] is deliberately **not** part of the
//! atomic write batch. RFC 0004 acceptance #6's "exactly one SlateDB seq" fan-out
//! is Node?+EdgeOut+EdgeIn+Log+`latest_seq` — split is a physical
//! reorganization of an existing posting's storage layout that preserves its
//! logical member set (RFC 0005 §"Splitting supernodes"), not a new logical
//! write, so it does not need its own changelog entry or seq. `upsert_edge`
//! commits the logical write first, then runs `maybe_split` on both touched
//! adjacency keys as a best-effort follow-up `storage.apply` (no injected
//! seqnum — it doesn't need one, and doesn't advance `latest_seq`). If the
//! follow-up split fails or is never reached (process dies between the two),
//! nothing is lost: the next `maybe_split` call (the next add to that key, or
//! an explicit maintenance sweep) re-evaluates from the currently-stored
//! posting and completes it — split-checking is idempotent and stateless
//! across calls.
//!
//! # Recovery on open (M1 D6)
//!
//! [`Writer::open`]/[`Writer::in_memory`]/[`Writer::from_storage`]: load
//! [`GraphAllocators`] (each id-space resumes past its last reserved block —
//! monotonic, no reuse, RFC 0004 §UID), rebuild the [`SchemaCache`] from every
//! stored `SchemaId` record, and read `Meta["latest_seq"]` to seed the writer's
//! cursor. A batch that was mid-flight when a prior writer died either
//! committed as a whole (SlateDB's `WriteBatch` atomicity) or is entirely
//! absent — there is no partial state to repair, so recovery is purely "reload
//! the in-memory accelerators from durable storage," not a repair pass.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use common::storage::RecordOp;
use common::{Record, StorageConfig, WriteOptions};
use roaring::RoaringTreemap;

use crate::ids::{GraphAllocators, resolve_or_create_xid_batched};
use crate::obs;
use crate::posting_ops;
use crate::schema::{Directives, SchemaCache, SchemaEntry, ValType};
use crate::serde::keys::{
    edge_prop_key, log_key, meta_key, node_key, schema_id_key, schema_name_key, xid_key,
};
use crate::serde::{Direction, LabelId, PredId, PropId, SchemaKind, Uid};
use crate::storage::GraphStorage;
use crate::value::{
    ChangeOp, ChangeRecord, EdgeProps, LabelDelta, NodeCodec, NodeRecord, TypedValue, V0NodeCodec,
    decode_edge_props, encode_edge_props,
};
use crate::{Error, Result};

/// Per-write-request phase-duration accumulator (RFC 0017 §3.2, principle #4
/// — "one stats struct per operation, emitted once at completion"). Each
/// public [`Writer`] method times its own phases with [`Instant`] and stashes
/// them here; [`Writer::commit`] fills in `batch_commit`/`latest_seq` and
/// [`WriteStats::emit`] renders every present phase into
/// `turbolay_write_phase_duration_seconds{phase}` **after** the durable
/// commit succeeds — never mid-fan-out, never on a batch that didn't land.
/// Phases that don't apply to a given op (e.g. `encode_node` for a delete)
/// are simply absent, not faked as zero — `index_fanout` is the one
/// deliberate exception (see its doc in [`obs::write`]).
#[derive(Default)]
struct WriteStats {
    encode_node: Option<Duration>,
    encode_out: Option<Duration>,
    encode_in: Option<Duration>,
    index_fanout: Option<Duration>,
    batch_commit: Option<Duration>,
    latest_seq: Option<Duration>,
}

impl WriteStats {
    fn emit(&self, seq: Seq) {
        let phase = obs::write::record_phase;
        if let Some(d) = self.encode_node {
            phase(obs::write::PHASE_ENCODE_NODE, d);
        }
        if let Some(d) = self.encode_out {
            phase(obs::write::PHASE_ENCODE_OUT, d);
        }
        if let Some(d) = self.encode_in {
            phase(obs::write::PHASE_ENCODE_IN, d);
        }
        if let Some(d) = self.index_fanout {
            phase(obs::write::PHASE_INDEX_FANOUT, d);
        }
        if let Some(d) = self.batch_commit {
            phase(obs::write::PHASE_BATCH_COMMIT, d);
        }
        if let Some(d) = self.latest_seq {
            phase(obs::write::PHASE_LATEST_SEQ, d);
        }
        obs::write::set_latest_seq(seq);
    }
}

/// Records one write request's terminal outcome
/// (`turbolay_write_requests_total{op,outcome}`) — the `oversize_node`
/// outcome maps from [`Error::Value`], every other `Err` is `outcome=error`.
/// Called once per public op method, at its one terminal `Result<Seq>` (a
/// pre-commit rejection or the `commit` call itself); errors surfaced by
/// helpers further upstream (xid resolution, schema interning, posting adds)
/// via `?` are not separately attributed here — see [`obs::write`]'s module
/// doc for the scope of this (optional, RFC 0017 §3.2) counter.
fn record_write_outcome(op: &'static str, result: &Result<Seq>) {
    let outcome = match result {
        Ok(_) => obs::write::OUTCOME_OK,
        Err(Error::Value(_)) => obs::write::OUTCOME_OVERSIZE_NODE,
        Err(_) => obs::write::OUTCOME_ERROR,
    };
    obs::write::record_request(op, outcome);
}

/// The `Meta` key holding the highest durably-committed logical seq (RFC 0004
/// §"Logical sequence protocol").
const META_LATEST_SEQ: &[u8] = b"latest_seq";

/// The session token every write op returns: turbolay's own logical seq,
/// injected as SlateDB's durable seqnum for the same commit (M1 integration
/// point #2). A freshness gate is `durable_seq >= token`.
pub type Seq = u64;

/// One logical record for [`Writer::ingest_batch`] — the batched-ingest
/// counterpart of a single [`Writer::upsert_node`] /
/// [`Writer::upsert_edge_with_props`] call. An empty `props` map on `Edge` is
/// exactly the plain [`Writer::upsert_edge`] case, matching that method's own
/// convention.
#[derive(Debug, Clone, PartialEq)]
pub enum IngestRecord {
    /// `UpsertNode(xid, labels, props)` — see [`Writer::upsert_node`].
    Node {
        xid: Vec<u8>,
        labels: Vec<String>,
        props: BTreeMap<String, TypedValue>,
    },
    /// `UpsertEdge(src_xid, pred, dst_xid, props)` — see
    /// [`Writer::upsert_edge_with_props`].
    Edge {
        src_xid: Vec<u8>,
        pred: String,
        dst_xid: Vec<u8>,
        props: BTreeMap<String, TypedValue>,
    },
}

/// The single-writer session for one graph namespace (RFC 0002 D2; M1
/// deliverables 5 + 6).
///
/// Not `Send`-shared by design — [`GraphAllocators`] and [`SchemaCache`] are
/// plain in-memory accelerators mutated by `&mut self`, matching the "single
/// writer, no internal synchronization" posture the id allocators already
/// document.
pub struct Writer {
    storage: GraphStorage,
    allocs: GraphAllocators,
    schema: SchemaCache,
    latest_seq: Seq,
    /// Read-side decode cache (the `*_cached` read methods). Holds decoded
    /// node blobs and fully-resolved (deleted-subtracted) adjacency sets so a
    /// value touched across hops / repeated queries is decoded **once**, not
    /// re-parsed from KV on every access — matching an in-RAM engine's model
    /// and the RFC 0007 §8 / RFC 0017 read-path target.
    ///
    /// Correctness: **cleared on every durable write** ([`Self::commit`] /
    /// [`Self::commit_batch`]), so it never serves state older than the
    /// writer's last commit. It caches *membership/decoded results*, which are
    /// invariant under the post-commit `maybe_split` reorganization (a split
    /// preserves the member set), so split — the one write that bypasses
    /// commit — needs no invalidation. Interior-mutable behind `Mutex` (never
    /// held across an `.await`) so the concurrent `*_batch`/`*_each` fan-outs
    /// can share it through `&self`.
    node_cache: Mutex<HashMap<u64, Option<NodeRecord>>>,
    neigh_cache: Mutex<HashMap<(u8, u64, u32), RoaringTreemap>>,
}

/// Max concurrent per-anchor cached-neighbor reads in a batched expansion —
/// mirrors [`posting_ops`]'s own frontier concurrency.
const FRONTIER_CONCURRENCY: usize = 64;

/// Neighbor-cache key: `(dir_tag, anchor_uid, pred_id)`.
fn nbr_cache_key(dir: Direction, anchor: Uid, pred: PredId) -> (u8, u64, u32) {
    let dtag = match dir {
        Direction::Out => 0u8,
        Direction::In => 1u8,
    };
    (dtag, anchor.get(), pred.get())
}

impl Writer {
    /// Opens a namespace from a storage config and recovers writer state (M1
    /// D6): [`GraphAllocators::load`], [`SchemaCache::rebuild_from_storage`],
    /// and the `Meta["latest_seq"]` cursor.
    pub async fn open(config: &StorageConfig) -> Result<Self> {
        Self::from_storage(GraphStorage::open(config).await?).await
    }

    /// [`Self::open`] against an in-memory namespace — the correctness-testing
    /// entrypoint (RFC 0017 D12: no S3/LocalStack for correctness).
    pub async fn in_memory() -> Result<Self> {
        Self::from_storage(GraphStorage::in_memory().await?).await
    }

    /// Recovers writer state over an already-open [`GraphStorage`] handle (M1
    /// D6's recovery-on-open, factored out so tests can simulate "drop the
    /// writer, reopen against the same backing store" without tearing down
    /// the storage itself — the realistic in-memory stand-in for a process
    /// restart, since `GraphStorage` is a cheap `Arc`-cloneable handle over
    /// the one shared backend).
    pub async fn from_storage(storage: GraphStorage) -> Result<Self> {
        let allocs = GraphAllocators::load(storage.inner().as_ref()).await?;
        let schema = SchemaCache::rebuild_from_storage(storage.inner().as_ref()).await?;
        let latest_seq = match storage.get(meta_key(META_LATEST_SEQ)).await? {
            Some(bytes) => decode_u64_le(&bytes, "Meta[latest_seq]")?,
            None => 0,
        };
        Ok(Self {
            storage,
            allocs,
            schema,
            latest_seq,
            node_cache: Mutex::new(HashMap::new()),
            neigh_cache: Mutex::new(HashMap::new()),
        })
    }

    /// Drop the whole read-side decode cache — called after every durable
    /// write so cached reads never predate the writer's latest commit.
    /// Conservative (clears everything, not just touched keys); the read cache
    /// re-warms on the next query, and writes are rare relative to reads in
    /// the query workloads this serves.
    fn invalidate_read_cache(&self) {
        self.node_cache.lock().unwrap().clear();
        self.neigh_cache.lock().unwrap().clear();
    }

    /// The storage handle this writer drives (read escape hatch for tests /
    /// building a reader over the same backend).
    pub fn storage(&self) -> &GraphStorage {
        &self.storage
    }

    /// The highest logical seq this writer has durably committed (or resumed
    /// from `Meta["latest_seq"]` on open).
    pub fn latest_seq(&self) -> Seq {
        self.latest_seq
    }

    /// Resolves a known `xid` to its `uid` — a **read-only** lookup that never
    /// allocates or creates (unlike [`resolve_or_create_xid_batched`]). Used by
    /// `DeleteNode`/`DeleteEdge` (RFC 0004: "if absent, no-op" — deletes must
    /// not conjure a node into existence) and exposed for tests/callers that
    /// need to check xid state without mutating it.
    pub async fn lookup_uid(&self, xid: &[u8]) -> Result<Option<Uid>> {
        match self.storage.get(xid_key(xid)).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(Uid(decode_u64_be(&bytes, "Xid mapping")?))),
        }
    }

    /// Looks up an already-interned schema id by name (cache-only, no I/O) —
    /// `None` means the name has never been interned by this writer.
    pub fn schema_id(&self, kind: SchemaKind, name: &str) -> Option<u32> {
        self.schema.id_of(kind, name)
    }

    /// Reads back a node record (read escape hatch for tests/callers).
    pub async fn get_node(&self, uid: Uid) -> Result<Option<NodeRecord>> {
        match self.storage.get(node_key(uid)).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(V0NodeCodec::decode(&bytes)?)),
        }
    }

    /// Batched [`Self::get_node`]: fetch many node blobs with bounded
    /// concurrency ([`GraphStorage::multi_get`]) and decode them, **preserving
    /// input order** (result `i` is the node for `uids[i]`, `None` if absent).
    /// Turns the read path's per-result-node serial `get_node` loop into one
    /// batched fan-out (RFC 0007 §8 N+1 — the materialize phase's lever).
    pub async fn get_nodes(&self, uids: &[Uid]) -> Result<Vec<Option<NodeRecord>>> {
        let keys: Vec<Bytes> = uids.iter().map(|u| node_key(*u)).collect();
        self.storage
            .multi_get(keys)
            .await?
            .into_iter()
            .map(|opt| opt.map(|bytes| V0NodeCodec::decode(&bytes)).transpose())
            .collect()
    }

    /// Reads back an edge's `EdgeProp` companion record (RFC 0005 §"Edge
    /// facets", as amended: a single copy keyed by full edge identity) — read
    /// escape hatch for tests/callers, mirroring [`Self::get_node`]. `None`
    /// means the edge has no companion: either it was never upserted with
    /// props ([`Self::upsert_edge`] / an empty-map [`Self::upsert_edge_with_props`]
    /// call), or it was deleted ([`Self::delete_edge`] blind-deletes the
    /// companion). Like `get_node`, this is a **raw point-get** — it does not
    /// consult `Meta["deleted_edges"]`/`Meta["deleted_nodes"]` tombstones, so
    /// a companion for a since-tombstoned edge (deleted via the adjacency
    /// bitmap rather than `delete_edge`, which can't happen through this
    /// module's own API but could via a future path) would still read back
    /// here until vacuum (RFC 0012) physically reaps it.
    pub async fn edge_props(&self, src: Uid, pred: PredId, dst: Uid) -> Result<Option<EdgeProps>> {
        match self.storage.get(edge_prop_key(src, pred, dst)).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(decode_edge_props(&bytes)?)),
        }
    }

    /// Batched [`Self::edge_props`]: fetch many edge companions with bounded
    /// concurrency, **preserving input order** (result `i` is for `edges[i]`).
    /// Used by valued-edge queries (e.g. IC07's per-`(fan, message)`
    /// `likeCreationDate`) to replace a serial per-pair `edge_props` loop.
    pub async fn edge_props_batch(
        &self,
        edges: &[(Uid, PredId, Uid)],
    ) -> Result<Vec<Option<EdgeProps>>> {
        let keys: Vec<Bytes> = edges
            .iter()
            .map(|(src, pred, dst)| edge_prop_key(*src, *pred, *dst))
            .collect();
        self.storage
            .multi_get(keys)
            .await?
            .into_iter()
            .map(|opt| opt.map(|bytes| decode_edge_props(&bytes)).transpose())
            .collect()
    }

    // -----------------------------------------------------------------
    // Cached read path (decode-once; see the `node_cache`/`neigh_cache` doc).
    // These are what the query/traversal path uses; the uncached siblings
    // above stay for the write path and tests.
    // -----------------------------------------------------------------

    /// Cache-aware batched node fetch: returns cached decoded nodes, batches a
    /// single [`Self::get_nodes`] fan-out for the misses, populates the cache,
    /// and preserves input order. A node touched by an earlier hop/query is a
    /// pure in-memory clone — no re-decode, no storage read.
    pub async fn get_nodes_cached(&self, uids: &[Uid]) -> Result<Vec<Option<NodeRecord>>> {
        let mut resolved: Vec<Option<Option<NodeRecord>>> = vec![None; uids.len()];
        let mut miss_uids: Vec<Uid> = Vec::new();
        let mut miss_pos: Vec<usize> = Vec::new();
        {
            let cache = self.node_cache.lock().unwrap();
            for (i, u) in uids.iter().enumerate() {
                match cache.get(&u.get()) {
                    Some(hit) => resolved[i] = Some(hit.clone()),
                    None => {
                        miss_uids.push(*u);
                        miss_pos.push(i);
                    }
                }
            }
        }
        if !miss_uids.is_empty() {
            let fetched = self.get_nodes(&miss_uids).await?;
            let mut cache = self.node_cache.lock().unwrap();
            for (j, node) in fetched.into_iter().enumerate() {
                cache.insert(miss_uids[j].get(), node.clone());
                resolved[miss_pos[j]] = Some(node);
            }
        }
        Ok(resolved.into_iter().map(|x| x.expect("every slot resolved")).collect())
    }

    /// Cache-aware [`posting_ops::neighbors`]: returns the fully-resolved
    /// (deleted-subtracted) neighbor set from cache, or computes it once and
    /// caches it. On a hit this issues **zero** storage reads (the base
    /// adjacency read, the split-part reads, and the deleted-edges read are
    /// all skipped).
    pub async fn neighbors_cached(
        &self,
        anchor: Uid,
        pred: PredId,
        dir: Direction,
        deleted_nodes: &RoaringTreemap,
    ) -> Result<RoaringTreemap> {
        let key = nbr_cache_key(dir, anchor, pred);
        if let Some(hit) = self.neigh_cache.lock().unwrap().get(&key) {
            return Ok(hit.clone());
        }
        let set = posting_ops::neighbors(&self.storage, anchor, pred, dir, deleted_nodes).await?;
        self.neigh_cache.lock().unwrap().insert(key, set.clone());
        Ok(set)
    }

    /// Cache-aware, concurrent frontier expansion + union (the batched hop).
    pub async fn neighbors_batch_cached(
        &self,
        frontier: impl IntoIterator<Item = Uid>,
        pred: PredId,
        dir: Direction,
        deleted_nodes: &RoaringTreemap,
    ) -> Result<RoaringTreemap> {
        use futures::stream::{self, StreamExt, TryStreamExt};
        let sets: Vec<RoaringTreemap> = stream::iter(frontier)
            .map(|u| self.neighbors_cached(u, pred, dir, deleted_nodes))
            .buffer_unordered(FRONTIER_CONCURRENCY)
            .try_collect()
            .await?;
        let mut out = RoaringTreemap::new();
        for s in sets {
            out |= s;
        }
        Ok(out)
    }

    /// Cache-aware, concurrent per-anchor neighbor fetch preserving the
    /// anchor→set binding (for materialization that needs the src→dst pairing).
    pub async fn neighbors_each_cached(
        &self,
        anchors: impl IntoIterator<Item = Uid>,
        pred: PredId,
        dir: Direction,
        deleted_nodes: &RoaringTreemap,
    ) -> Result<Vec<(Uid, RoaringTreemap)>> {
        use futures::stream::{self, StreamExt, TryStreamExt};
        stream::iter(anchors)
            .map(|u| async move {
                self.neighbors_cached(u, pred, dir, deleted_nodes)
                    .await
                    .map(|set| (u, set))
            })
            .buffer_unordered(FRONTIER_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Reads the current `(pred, dir, anchor)` degree counter (0 if never
    /// bumped) — see the module-level note on the M1 simplified degree
    /// counter.
    pub async fn degree(&self, pred: PredId, dir: Direction, anchor: Uid) -> Result<i64> {
        match self.storage.get(degree_key(pred, dir, anchor)).await? {
            None => Ok(0),
            Some(bytes) => decode_i64_le(&bytes, "degree counter"),
        }
    }

    /// The live, deleted-filtered adjacency set for `(anchor, pred, dir)` —
    /// [`posting_ops::neighbors`] plus the writer's own current
    /// `Meta["deleted_nodes"]` bitmap (read escape hatch for tests/callers;
    /// the M2 query planner is the real read path).
    pub async fn neighbors(
        &self,
        anchor: Uid,
        pred: PredId,
        dir: Direction,
    ) -> Result<RoaringTreemap> {
        let deleted_nodes = match self.storage.get(posting_ops::deleted_nodes_key()).await? {
            Some(bytes) => RoaringTreemap::deserialize_from(bytes.as_ref())
                .map_err(|e| Error::encoding(format!("corrupt deleted_nodes bitmap: {e}")))?,
            None => RoaringTreemap::new(),
        };
        posting_ops::neighbors(&self.storage, anchor, pred, dir, &deleted_nodes).await
    }

    // -----------------------------------------------------------------
    // UpsertNode
    // -----------------------------------------------------------------

    /// `UpsertNode(xid, labels, props)` (RFC 0004 §"Write path"). Resolves or
    /// creates `xid`'s uid, interns any new label/prop names, merges `labels`
    /// (additive union) and `props` (overlay-by-key) into the prior
    /// `NodeRecord` (if any), and commits the whole write — xid mapping,
    /// schema interning, the node record, changelog, `latest_seq` — at one
    /// seq. Rejects with an `oversize_node` [`Error::Value`] (RFC 0004 §"Node
    /// size cap") if the merged record's encoding exceeds the 1 MiB cap,
    /// **without** committing anything (the cap check runs before the batch is
    /// built).
    pub async fn upsert_node(
        &mut self,
        xid: &[u8],
        labels: &[String],
        props: BTreeMap<String, TypedValue>,
    ) -> Result<Seq> {
        let mut ops = Vec::new();

        let (uid, xid_ops) =
            resolve_or_create_xid_batched(self.storage.inner().as_ref(), &mut self.allocs, xid)
                .await?;
        ops.extend(xid_ops);

        let mut label_ids = Vec::with_capacity(labels.len());
        for name in labels {
            label_ids.push(LabelId(self.intern(SchemaKind::Label, name, &mut ops)));
        }

        let mut new_props = BTreeMap::new();
        for (name, value) in props {
            let prop_id = PropId(self.intern(SchemaKind::PropertyKey, &name, &mut ops));
            new_props.insert(prop_id, value);
        }

        let prior = self.get_node(uid).await?;
        let xid_string = xid_utf8(xid)?;
        let (merged, label_delta) = merge_node_record(prior, label_ids, new_props, xid_string);

        // Cap check happens here, before any ops are queued for commit — an
        // oversize node aborts the whole op with nothing written.
        let encode_start = Instant::now();
        let encoded = V0NodeCodec::encode(&merged);
        let encode_node_elapsed = encode_start.elapsed();
        let encoded = match encoded {
            Ok(bytes) => bytes,
            Err(e) => {
                let result: Result<Seq> = Err(e);
                record_write_outcome(obs::write::OP_UPSERT_NODE, &result);
                return result;
            }
        };
        ops.push(RecordOp::Put(Record::new(node_key(uid), encoded).into()));

        let change = ChangeRecord {
            seq: 0, // filled in by `commit`
            op: ChangeOp::UpsertNode,
            subject_uid: uid,
            pred_id: None,
            object_uid: None,
            value: None,
            label_delta: Some(label_delta),
        };
        let stats = WriteStats {
            encode_node: Some(encode_node_elapsed),
            ..WriteStats::default()
        };
        let result = self.commit(ops, change, stats).await;
        record_write_outcome(obs::write::OP_UPSERT_NODE, &result);
        result
    }

    // -----------------------------------------------------------------
    // UpsertEdge
    // -----------------------------------------------------------------

    /// `UpsertEdge(src_xid, pred, dst_xid)` (RFC 0004 §"Write path"). Resolves
    /// (or creates, with a stub `NodeRecord`) both endpoints, interns `pred`,
    /// adds `dst` to `src`'s `EdgeOut` posting and `src` to `dst`'s `EdgeIn`
    /// posting (D10: unconditional bidirectional storage, same batch), bumps
    /// both sides' degree counters, and commits changelog + `latest_seq` at
    /// one seq. After the commit, runs [`posting_ops::maybe_split`] on both
    /// touched adjacency keys as a best-effort, non-atomic follow-up (see the
    /// module-level note).
    ///
    /// A thin, back-compat wrapper over [`Self::upsert_edge_inner`] with an
    /// empty props map — it **never** touches the edge's `EdgeProp` companion
    /// (RFC 0005 §"Edge facets"), so re-`upsert_edge`-ing an already-valued
    /// edge preserves whatever props [`Self::upsert_edge_with_props`]
    /// previously wrote for it.
    pub async fn upsert_edge(&mut self, src_xid: &[u8], pred: &str, dst_xid: &[u8]) -> Result<Seq> {
        self.upsert_edge_inner(src_xid, pred, dst_xid, BTreeMap::new())
            .await
    }

    /// `UpsertEdge(src_xid, pred, dst_xid, props)` — [`Self::upsert_edge`]'s
    /// valued/faceted-edge counterpart (Workstream A, RFC 0005 §"Edge
    /// facets"). Identical fan-out to `upsert_edge`, plus: each prop name is
    /// interned to a [`PropId`] (exactly like [`Self::upsert_node`] interns
    /// node props) and, **if `props` is non-empty**, the edge's single
    /// `EdgeProp[src][pred][dst]` companion record is put in the same atomic
    /// batch as the adjacency/degree/changelog writes. If `props` is empty,
    /// no companion is written — behaves exactly like plain `upsert_edge`.
    /// Emits the same `ChangeOp::UpsertEdge` changelog record as
    /// `upsert_edge` (no new `ChangeOp`, no `ChangeRecord` format change: the
    /// companion is a storage-layer detail, not a change-stream event).
    pub async fn upsert_edge_with_props(
        &mut self,
        src_xid: &[u8],
        pred: &str,
        dst_xid: &[u8],
        props: BTreeMap<String, TypedValue>,
    ) -> Result<Seq> {
        self.upsert_edge_inner(src_xid, pred, dst_xid, props).await
    }

    /// Shared `UpsertEdge` fan-out for [`Self::upsert_edge`] and
    /// [`Self::upsert_edge_with_props`] — an empty `props` map is exactly the
    /// plain-edge case (no `EdgeProp` companion is written either way).
    async fn upsert_edge_inner(
        &mut self,
        src_xid: &[u8],
        pred: &str,
        dst_xid: &[u8],
        props: BTreeMap<String, TypedValue>,
    ) -> Result<Seq> {
        let mut ops = Vec::new();

        let src_uid = self.resolve_and_stub_node(src_xid, &mut ops).await?;
        let dst_uid = self.resolve_and_stub_node(dst_xid, &mut ops).await?;
        let pred_id = PredId(self.intern(SchemaKind::Predicate, pred, &mut ops));

        let encode_out_start = Instant::now();
        let out_ops =
            posting_ops::add(&self.storage, Direction::Out, src_uid, pred_id, dst_uid).await?;
        let encode_out_elapsed = encode_out_start.elapsed();
        ops.extend(out_ops);

        let encode_in_start = Instant::now();
        let in_ops =
            posting_ops::add(&self.storage, Direction::In, dst_uid, pred_id, src_uid).await?;
        let encode_in_elapsed = encode_in_start.elapsed();
        ops.extend(in_ops);

        ops.push(bump_degree(pred_id, Direction::Out, src_uid, 1));
        ops.push(bump_degree(pred_id, Direction::In, dst_uid, 1));

        if !props.is_empty() {
            let mut edge_props: EdgeProps = BTreeMap::new();
            for (name, value) in props {
                let prop_id = PropId(self.intern(SchemaKind::PropertyKey, &name, &mut ops));
                edge_props.insert(prop_id, value);
            }
            ops.push(RecordOp::Put(
                Record::new(
                    edge_prop_key(src_uid, pred_id, dst_uid),
                    encode_edge_props(&edge_props),
                )
                .into(),
            ));
        }

        let change = ChangeRecord {
            seq: 0,
            op: ChangeOp::UpsertEdge,
            subject_uid: src_uid,
            pred_id: Some(pred_id),
            object_uid: Some(dst_uid),
            value: None,
            label_delta: None,
        };
        let stats = WriteStats {
            encode_out: Some(encode_out_elapsed),
            encode_in: Some(encode_in_elapsed),
            ..WriteStats::default()
        };
        let result = self.commit(ops, change, stats).await;
        record_write_outcome(obs::write::OP_UPSERT_EDGE, &result);
        let seq = result?;

        // Post-commit, best-effort, non-atomic (see module doc). A failure
        // here must not turn an already-committed logical write into a reported
        // failure; the next touch of either key re-evaluates split from scratch.
        let mut split_ops = Vec::new();
        for (dir, anchor) in [(Direction::Out, src_uid), (Direction::In, dst_uid)] {
            match posting_ops::maybe_split(&self.storage, dir, anchor, pred_id).await {
                Ok(more_ops) => split_ops.extend(more_ops),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        anchor = anchor.get(),
                        pred = pred_id.get(),
                        "maybe_split check failed after an edge commit; the edge already \
                         committed durably, only the deferred split reorganization is affected \
                         and will be retried on the next touch of this posting"
                    );
                }
            }
        }
        if !split_ops.is_empty()
            && let Err(e) = self.storage.apply(split_ops).await
        {
            tracing::warn!(
                error = %e,
                "applying maybe_split ops failed after an edge commit; the edge already \
                 committed durably"
            );
        }

        Ok(seq)
    }

    // -----------------------------------------------------------------
    // Bulk ingest (batched write path)
    // -----------------------------------------------------------------

    /// Bulk-ingest path: groups up to `records.len()` logical
    /// `UpsertNode`/`UpsertEdge` writes into **one** physical
    /// [`GraphStorage::apply_with_options`] call, while preserving every
    /// invariant a sequence of individual [`Self::upsert_node`] /
    /// [`Self::upsert_edge_with_props`] calls would have produced. This is
    /// the throughput path for large loads (RFC 0004's write path is
    /// otherwise one durable commit per logical write, which is one network
    /// round trip per element on a real object-store-backed SlateDB — fine
    /// for OLTP-shaped traffic, far too slow for bulk load).
    ///
    /// # Invariants preserved
    ///
    /// - **Seq protocol** (RFC 0004 §"Logical sequence protocol"): every
    ///   logical record in `records` still gets its own strictly-monotonic
    ///   seq and its own `Log[seq]` [`ChangeRecord`], allocated via the exact
    ///   same [`GraphAllocators::next_seq`] a per-record [`Self::commit`]
    ///   uses (see [`Self::commit_batch`]) — so seqs stay strictly increasing
    ///   across batches exactly as they are across individual per-record
    ///   commits, and `Meta["latest_seq"]` only ever moves forward. The one
    ///   physical commit's injected `WriteOptions.seqnum` is the batch's
    ///   **max** seq (its last-allocated one, since `next_seq` only
    ///   increases) — required because SlateDB rejects a non-monotonic
    ///   injected seqnum (see `should_reject_non_monotonic_injected_seqnum`),
    ///   so batch K's max must be < batch K+1's min, which holds for the same
    ///   reason two individual commits' seqs never collide.
    /// - **In-batch xid resolution**: [`resolve_or_create_xid_batched`] only
    ///   consults durable storage, so a second reference to a brand-new xid
    ///   later in this *same*, not-yet-committed batch would not see the
    ///   first reference's queued-but-uncommitted mapping `Put` and would
    ///   double-allocate a uid. [`Self::resolve_xid_cached`] is this method's
    ///   in-batch `xid -> uid` cache: the first reference allocates and
    ///   queues the mapping ops (and, if new, a stub node), every later
    ///   reference in the batch reuses the cached uid. Schema-name interning
    ///   ([`Self::intern`]) needs no equivalent batch cache — it already
    ///   updates the in-memory [`SchemaCache`] synchronously on first use, so
    ///   calling it repeatedly across this whole batch loop is already
    ///   correct (a second reference to the same name in the batch is a
    ///   cache hit, no re-intern).
    /// - **Node-record merge coalescing**: an edge referencing a brand-new
    ///   endpoint queues a stub `NodeRecord`; a later `Node` record for that
    ///   same xid later in the batch must merge onto *that* stub (there's
    ///   nothing for it in storage yet) and must not emit a second, colliding
    ///   `Put` for the same node key. `pending_nodes` tracks the batch's
    ///   coalesced, not-yet-committed node state per uid and is flushed to
    ///   exactly one `Put` per touched uid at the end — mirroring how a
    ///   sequence of individual `upsert_node`/`upsert_edge` calls would leave
    ///   exactly one current `NodeRecord` per uid, just without the
    ///   intermediate round trips.
    /// - **Atomicity**: everything above — every xid/schema/posting/degree/
    ///   node op, every per-record `Log[seq]`, and the one `latest_seq` bump
    ///   — is folded into one `Vec<RecordOp>` committed by one
    ///   `apply_with_options` call. A failure commits nothing from the batch,
    ///   exactly like a single-record [`Self::commit`] (nothing is
    ///   partially applied; the whole batch is atomic-or-nothing).
    ///
    /// An oversize node (RFC 0004 §"Node size cap") anywhere in the batch
    /// aborts the **whole batch** with nothing committed — same all-or-
    /// nothing cap-check-before-commit policy as [`Self::upsert_node`], just
    /// scoped to the batch rather than one record.
    ///
    /// Post-commit, [`posting_ops::maybe_split`] runs once per distinct
    /// `(dir, anchor, pred)` posting touched anywhere in the batch (instead
    /// of once per edge, as [`Self::upsert_edge_inner`]'s per-record path
    /// does) — the same best-effort, non-atomic follow-up: **a failure here
    /// is logged and swallowed, never surfaced as an ingest failure**, since
    /// the batch has already committed durably by the time split-checking
    /// runs (see the module-level "Split/rollup: post-commit, not in-band"
    /// doc).
    ///
    /// Returns the batch's max seq, or the writer's current `latest_seq`
    /// unchanged if `records` is empty (no seq is consumed for an empty
    /// batch).
    pub async fn ingest_batch(&mut self, records: &[IngestRecord]) -> Result<Seq> {
        if records.is_empty() {
            return Ok(self.latest_seq);
        }

        let mut ops: Vec<RecordOp> = Vec::new();
        let mut changes: Vec<ChangeRecord> = Vec::new();
        let mut xid_cache: HashMap<Vec<u8>, Uid> = HashMap::new();
        let mut pending_nodes: BTreeMap<Uid, NodeRecord> = BTreeMap::new();
        let mut touched: BTreeSet<(Direction, Uid, PredId)> = BTreeSet::new();

        for record in records {
            match record {
                IngestRecord::Node { xid, labels, props } => {
                    let uid = self
                        .resolve_xid_cached(xid, &mut xid_cache, &mut ops, &mut pending_nodes)
                        .await?;

                    let mut label_ids = Vec::with_capacity(labels.len());
                    for name in labels {
                        label_ids.push(LabelId(self.intern(SchemaKind::Label, name, &mut ops)));
                    }
                    let mut new_props = BTreeMap::new();
                    for (name, value) in props {
                        let prop_id = PropId(self.intern(SchemaKind::PropertyKey, name, &mut ops));
                        new_props.insert(prop_id, value.clone());
                    }

                    // Prior state is the batch's own coalesced state if this
                    // uid was already touched earlier in the batch (a stub
                    // from an edge endpoint, or an earlier `Node` record for
                    // the same xid) — there is nothing newer in storage than
                    // that. Only fall back to a storage read if this is the
                    // uid's first touch in the whole batch.
                    let prior = match pending_nodes.get(&uid) {
                        Some(node) => Some(node.clone()),
                        None => self.get_node(uid).await?,
                    };
                    let xid_string = xid_utf8(xid)?;
                    let (merged, label_delta) =
                        merge_node_record(prior, label_ids, new_props, xid_string);

                    // Cap check now, before anything commits — an oversize
                    // node aborts the whole batch with nothing written, same
                    // policy as the per-record path.
                    if let Err(e) = V0NodeCodec::encode(&merged) {
                        let result: Result<Seq> = Err(e);
                        record_write_outcome(obs::write::OP_INGEST_BATCH, &result);
                        return result;
                    }
                    pending_nodes.insert(uid, merged);

                    changes.push(ChangeRecord {
                        seq: 0, // filled in by `commit_batch`
                        op: ChangeOp::UpsertNode,
                        subject_uid: uid,
                        pred_id: None,
                        object_uid: None,
                        value: None,
                        label_delta: Some(label_delta),
                    });
                }
                IngestRecord::Edge {
                    src_xid,
                    pred,
                    dst_xid,
                    props,
                } => {
                    let src_uid = self
                        .resolve_xid_cached(src_xid, &mut xid_cache, &mut ops, &mut pending_nodes)
                        .await?;
                    let dst_uid = self
                        .resolve_xid_cached(dst_xid, &mut xid_cache, &mut ops, &mut pending_nodes)
                        .await?;
                    let pred_id = PredId(self.intern(SchemaKind::Predicate, pred, &mut ops));

                    let out_ops =
                        posting_ops::add(&self.storage, Direction::Out, src_uid, pred_id, dst_uid)
                            .await?;
                    ops.extend(out_ops);
                    let in_ops =
                        posting_ops::add(&self.storage, Direction::In, dst_uid, pred_id, src_uid)
                            .await?;
                    ops.extend(in_ops);
                    touched.insert((Direction::Out, src_uid, pred_id));
                    touched.insert((Direction::In, dst_uid, pred_id));

                    ops.push(bump_degree(pred_id, Direction::Out, src_uid, 1));
                    ops.push(bump_degree(pred_id, Direction::In, dst_uid, 1));

                    if !props.is_empty() {
                        let mut edge_props: EdgeProps = BTreeMap::new();
                        for (name, value) in props {
                            let prop_id =
                                PropId(self.intern(SchemaKind::PropertyKey, name, &mut ops));
                            edge_props.insert(prop_id, value.clone());
                        }
                        ops.push(RecordOp::Put(
                            Record::new(
                                edge_prop_key(src_uid, pred_id, dst_uid),
                                encode_edge_props(&edge_props),
                            )
                            .into(),
                        ));
                    }

                    changes.push(ChangeRecord {
                        seq: 0, // filled in by `commit_batch`
                        op: ChangeOp::UpsertEdge,
                        subject_uid: src_uid,
                        pred_id: Some(pred_id),
                        object_uid: Some(dst_uid),
                        value: None,
                        label_delta: None,
                    });
                }
            }
        }

        // Flush the batch's coalesced node state: exactly one `Put` per
        // touched uid, reflecting every `Node` record (and edge-endpoint
        // stub) this batch queued for it. Every entry here already passed
        // the cap check above, so this re-encode cannot fail.
        for (uid, node) in &pending_nodes {
            let encoded = V0NodeCodec::encode(node)
                .expect("every pending node was already cap-checked before insertion above");
            ops.push(RecordOp::Put(Record::new(node_key(*uid), encoded).into()));
        }

        let result = self.commit_batch(ops, changes).await;
        record_write_outcome(obs::write::OP_INGEST_BATCH, &result);
        let seq = result?;

        // Post-commit, best-effort, non-atomic maybe_split — once per
        // distinct (dir, anchor, pred) touched anywhere in the batch (not
        // once per edge). Per the module doc: a failure here must NOT be
        // surfaced as an ingest failure — the batch already committed
        // durably, and split-checking is idempotent/stateless across calls,
        // so the next touch of any of these postings re-evaluates it anyway.
        let mut split_ops = Vec::new();
        for (dir, anchor, pred) in touched {
            match posting_ops::maybe_split(&self.storage, dir, anchor, pred).await {
                Ok(more_ops) => split_ops.extend(more_ops),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        anchor = anchor.get(),
                        pred = pred.get(),
                        "maybe_split check failed after a batched ingest commit; the ingest \
                         already committed durably, only the deferred split reorganization is \
                         affected and will be retried on the next touch of this posting"
                    );
                }
            }
        }
        if !split_ops.is_empty()
            && let Err(e) = self.storage.apply(split_ops).await
        {
            tracing::warn!(
                error = %e,
                "applying batched maybe_split ops failed after a batched ingest commit; \
                 the ingest already committed durably"
            );
        }

        Ok(seq)
    }

    // -----------------------------------------------------------------
    // DeleteNode
    // -----------------------------------------------------------------

    /// `DeleteNode(xid)` (RFC 0004 §"Write path"): tombstone-and-filter, O(1).
    /// If `xid` has no mapping, this is a no-op (RFC 0004: "if absent,
    /// no-op") — returns the writer's current `latest_seq` unchanged, without
    /// allocating a new seq or touching storage.
    pub async fn delete_node(&mut self, xid: &[u8]) -> Result<Seq> {
        let Some(uid) = self.lookup_uid(xid).await? else {
            let result = Ok(self.latest_seq);
            record_write_outcome(obs::write::OP_DELETE, &result);
            return result;
        };

        let ops = vec![posting_ops::delete_node_op(uid)];
        let change = ChangeRecord {
            seq: 0,
            op: ChangeOp::DeleteNode,
            subject_uid: uid,
            pred_id: None,
            object_uid: None,
            value: None,
            label_delta: None,
        };
        let result = self.commit(ops, change, WriteStats::default()).await;
        record_write_outcome(obs::write::OP_DELETE, &result);
        result
    }

    // -----------------------------------------------------------------
    // DeleteEdge
    // -----------------------------------------------------------------

    /// `DeleteEdge(src_xid, pred, dst_xid)` (RFC 0004 §"Write path"):
    /// tombstones the edge in both directions (`Meta["deleted_edges"/pred/src]`
    /// and the symmetric `.../dst]`), decrements both degree counters, and
    /// blind-deletes the edge's `EdgeProp[src][pred][dst]` companion (RFC 0005
    /// §"Edge facets") if one exists — a `Delete` on a Put-only key that was
    /// never written is a safe no-op, so this doesn't need its own presence
    /// check. No-op (current `latest_seq`, nothing written) if either
    /// endpoint's xid is unmapped or `pred` was never interned — there is no
    /// edge to delete.
    pub async fn delete_edge(&mut self, src_xid: &[u8], pred: &str, dst_xid: &[u8]) -> Result<Seq> {
        let (Some(src_uid), Some(dst_uid)) = (
            self.lookup_uid(src_xid).await?,
            self.lookup_uid(dst_xid).await?,
        ) else {
            let result = Ok(self.latest_seq);
            record_write_outcome(obs::write::OP_DELETE, &result);
            return result;
        };
        let Some(pred_raw) = self.schema_id(SchemaKind::Predicate, pred) else {
            let result = Ok(self.latest_seq);
            record_write_outcome(obs::write::OP_DELETE, &result);
            return result;
        };
        let pred_id = PredId(pred_raw);

        let ops = vec![
            posting_ops::delete_edge_op(pred_id, src_uid, dst_uid),
            posting_ops::delete_edge_op(pred_id, dst_uid, src_uid),
            bump_degree(pred_id, Direction::Out, src_uid, -1),
            bump_degree(pred_id, Direction::In, dst_uid, -1),
            RecordOp::Delete(edge_prop_key(src_uid, pred_id, dst_uid)),
        ];
        let change = ChangeRecord {
            seq: 0,
            op: ChangeOp::DeleteEdge,
            subject_uid: src_uid,
            pred_id: Some(pred_id),
            object_uid: Some(dst_uid),
            value: None,
            label_delta: None,
        };
        let result = self.commit(ops, change, WriteStats::default()).await;
        record_write_outcome(obs::write::OP_DELETE, &result);
        result
    }

    // -----------------------------------------------------------------
    // Shared internals
    // -----------------------------------------------------------------

    /// Resolves `xid` to a uid, folding any new xid-mapping ops. If the xid
    /// was never seen before (a brand-new uid), also queues a minimal stub
    /// `NodeRecord{labels: [], props: {}, xid}` — RFC 0004 §"UpsertEdge" step 1
    /// ("each may create a node"): an edge to a never-seen xid implicitly
    /// creates that endpoint, matching Dgraph's mutation semantics.
    async fn resolve_and_stub_node(&mut self, xid: &[u8], ops: &mut Vec<RecordOp>) -> Result<Uid> {
        let (uid, xid_ops) =
            resolve_or_create_xid_batched(self.storage.inner().as_ref(), &mut self.allocs, xid)
                .await?;
        let is_new = !xid_ops.is_empty();
        ops.extend(xid_ops);

        if is_new {
            let stub = NodeRecord {
                labels: Vec::new(),
                props: BTreeMap::new(),
                xid: xid_utf8(xid)?,
            };
            // A label-less, prop-less stub can never cross the node-size cap.
            let encoded = V0NodeCodec::encode(&stub)?;
            ops.push(RecordOp::Put(Record::new(node_key(uid), encoded).into()));
        }
        Ok(uid)
    }

    /// Interns `name` under `kind`, returning its id. Cache hit: pure in-memory
    /// lookup, no ops. Cache miss: allocates a fresh id (folding any seq-block
    /// reservation), queues the `SchemaName`/`SchemaId` records, and updates
    /// the in-memory [`SchemaCache`] so a second reference to the same name
    /// within the *same* request (or a later one) hits the cache instead of
    /// re-allocating.
    fn intern(&mut self, kind: SchemaKind, name: &str, ops: &mut Vec<RecordOp>) -> u32 {
        if let Some(id) = self.schema.id_of(kind, name) {
            return id;
        }

        let (id, block_record) = match kind {
            SchemaKind::Label => {
                let (id, record) = self.allocs.allocate_label();
                (id.get(), record)
            }
            SchemaKind::Predicate => {
                let (id, record) = self.allocs.allocate_pred();
                (id.get(), record)
            }
            SchemaKind::PropertyKey => {
                let (id, record) = self.allocs.allocate_prop();
                (id.get(), record)
            }
        };
        if let Some(record) = block_record {
            ops.push(RecordOp::Put(record.into()));
        }

        // v0 directive defaults: predicates always materialize the reverse
        // (`EdgeIn`) projection (RFC 0003 D10 — `crate::schema::Directives`'s
        // own doc note); everything else is a plain, non-indexed name. Value
        // indexing/list/count directives are a schema-authoring feature this
        // write path doesn't expose yet (M2/RFC 0006 territory).
        let entry = SchemaEntry {
            name: name.to_string(),
            value_type: ValType::None,
            directives: Directives {
                reverse: matches!(kind, SchemaKind::Predicate),
                ..Directives::default()
            },
        };
        ops.push(RecordOp::Put(
            Record::new(schema_name_key(kind, name.as_bytes()), encode_u32_le(id)).into(),
        ));
        ops.push(RecordOp::Put(
            Record::new(schema_id_key(kind, id), entry.encode()).into(),
        ));

        self.schema.insert(kind, id, entry);
        id
    }

    /// [`Self::ingest_batch`]'s in-batch counterpart of
    /// [`Self::resolve_and_stub_node`]: resolves `xid` to a uid, but first
    /// consults `cache` — a scratch `xid -> uid` map scoped to one
    /// `ingest_batch` call — before falling through to
    /// [`resolve_or_create_xid_batched`]'s durable-storage lookup.
    ///
    /// This is required, not just an optimization: `resolve_or_create_xid_batched`
    /// decides "new vs. existing" purely by reading storage, so without this
    /// cache a second reference to a brand-new xid later in the *same*,
    /// not-yet-committed batch would not see the first reference's queued
    /// mapping `Put` and would allocate (and durably persist) a second uid
    /// for the same xid — silently corrupting xid stability. `cache` closes
    /// that gap: every reference to `xid` within one `ingest_batch` call
    /// after the first is a pure in-memory cache hit.
    ///
    /// If `xid` is brand new *anywhere in the batch so far* (a cache miss and
    /// `resolve_or_create_xid_batched` returns non-empty ops), also queues a
    /// minimal stub `NodeRecord{labels: [], props: {}, xid}` into
    /// `pending_nodes` — the batched equivalent of
    /// `resolve_and_stub_node`'s stub `Put`, routed through the batch's
    /// coalesced node state instead of a bare `ops` push so that a later
    /// `Node` record for the same xid in the same batch merges onto this
    /// stub instead of colliding with it (see [`Self::ingest_batch`]'s doc).
    async fn resolve_xid_cached(
        &mut self,
        xid: &[u8],
        cache: &mut HashMap<Vec<u8>, Uid>,
        ops: &mut Vec<RecordOp>,
        pending_nodes: &mut BTreeMap<Uid, NodeRecord>,
    ) -> Result<Uid> {
        if let Some(uid) = cache.get(xid) {
            return Ok(*uid);
        }

        let (uid, xid_ops) =
            resolve_or_create_xid_batched(self.storage.inner().as_ref(), &mut self.allocs, xid)
                .await?;
        let is_new = !xid_ops.is_empty();
        ops.extend(xid_ops);
        cache.insert(xid.to_vec(), uid);

        if is_new {
            pending_nodes.insert(
                uid,
                NodeRecord {
                    labels: Vec::new(),
                    props: BTreeMap::new(),
                    xid: xid_utf8(xid)?,
                },
            );
        }
        Ok(uid)
    }

    /// Finalizes a batch of `N` logical writes for [`Self::ingest_batch`]
    /// (RFC 0004 §"Logical sequence protocol", batched form of
    /// [`Self::commit`]): allocates one seq per entry in `changes`, in order,
    /// via the exact same [`GraphAllocators::next_seq`] a per-record
    /// `commit` uses (folding in any `SeqBlock` reservation exactly as
    /// `commit` does), appends one `Log[seq]` per change, bumps
    /// `Meta["latest_seq"]` **once** to the batch's max seq (the
    /// last-allocated one — `next_seq` only ever increases, so "last" and
    /// "max" are the same value here), and commits `ops` plus every
    /// `Log[seq]` plus that one `latest_seq` bump in **one**
    /// `GraphStorage::apply_with_options` call with `await_durable: true` and
    /// `seqnum` injected as that same max seq. A failure here leaves nothing
    /// from the batch committed — identical failure behavior to
    /// [`Self::commit`], just for `N` logical writes instead of one.
    async fn commit_batch(
        &mut self,
        mut ops: Vec<RecordOp>,
        mut changes: Vec<ChangeRecord>,
    ) -> Result<Seq> {
        debug_assert!(
            !changes.is_empty(),
            "ingest_batch already returns early on an empty records slice"
        );

        let mut max_seq = self.latest_seq;
        for change in changes.iter_mut() {
            let (raw_seq, seq_block) = self.allocs.next_seq();
            // Same +1 seed as `commit` — see that method's own comment on why
            // the logical seq space starts at 1, not 0.
            let seq = raw_seq + 1;
            if let Some(record) = seq_block {
                ops.push(RecordOp::Put(record.into()));
            }
            change.seq = seq;
            ops.push(RecordOp::Put(
                Record::new(log_key(seq), change.encode()).into(),
            ));
            max_seq = seq;
        }

        ops.push(RecordOp::Put(
            Record::new(meta_key(META_LATEST_SEQ), encode_u64_le(max_seq)).into(),
        ));

        self.storage
            .apply_with_options(
                ops,
                WriteOptions {
                    await_durable: true,
                    seqnum: max_seq,
                },
            )
            .await?;

        self.latest_seq = max_seq;
        self.invalidate_read_cache();
        obs::write::set_latest_seq(max_seq);
        Ok(max_seq)
    }

    /// Finalizes one write request: allocates the next logical seq (folding
    /// any seq-block reservation), appends `Log[seq]` and bumps
    /// `Meta["latest_seq"]`, and commits `ops` in **one**
    /// `GraphStorage::apply_with_options` call with `await_durable: true` and
    /// `seqnum: seq` injected (RFC 0004 §"Logical sequence protocol"; M1
    /// integration point #2) — this is the single atomic `WriteBatch` every
    /// public op method funnels through.
    ///
    /// Also the single emission point for `stats` (RFC 0017 §3.2, principle
    /// #4): times `index_fanout` (a genuinely-near-zero span — M1 has no
    /// index framework yet, RFC 0006 is M2), `batch_commit` (the durable
    /// `apply_with_options` call), and `latest_seq` (encoding+queuing the
    /// `Meta["latest_seq"]` `RecordOp`, bundled into the very same batch —
    /// not a separate round trip), then renders every phase this call and its
    /// caller accumulated, and sets the `turbolay_latest_seq` gauge, **after**
    /// the commit durably succeeds. On failure, `stats` is dropped unemitted
    /// — this M1 Phase 0 scope emits only on the success path (RFC 0017's
    /// fuller "emit once, on success or error" principle is a candidate M2
    /// refinement once there's a per-request stats/debug surface to render
    /// a partial struct into).
    async fn commit(
        &mut self,
        mut ops: Vec<RecordOp>,
        mut change: ChangeRecord,
        mut stats: WriteStats,
    ) -> Result<Seq> {
        let (raw_seq, seq_block) = self.allocs.next_seq();
        // Seed the logical seq space at 1 (not 0): SlateDB/`common`'s injected
        // seqnum contract requires strictly-greater-than-current-max, and a
        // fresh backend's max is 0 — see the M1 spike-and-seqnum-decision doc,
        // "we seed the logical seq above SlateDB's initial max on a fresh DB."
        let seq = raw_seq + 1;
        if let Some(record) = seq_block {
            ops.push(RecordOp::Put(record.into()));
        }

        change.seq = seq;
        ops.push(RecordOp::Put(
            Record::new(log_key(seq), change.encode()).into(),
        ));

        // `index_fanout`: no index framework exists in M1 (RFC 0006 is M2) —
        // timed anyway, at whatever this genuinely-empty span measures, so
        // the phase taxonomy is stable before real index fan-out work lands.
        let index_fanout_start = Instant::now();
        stats.index_fanout = Some(index_fanout_start.elapsed());

        let latest_seq_start = Instant::now();
        ops.push(RecordOp::Put(
            Record::new(meta_key(META_LATEST_SEQ), encode_u64_le(seq)).into(),
        ));
        stats.latest_seq = Some(latest_seq_start.elapsed());

        let commit_start = Instant::now();
        self.storage
            .apply_with_options(
                ops,
                WriteOptions {
                    await_durable: true,
                    seqnum: seq,
                },
            )
            .await?;
        stats.batch_commit = Some(commit_start.elapsed());

        self.latest_seq = seq;
        self.invalidate_read_cache();
        stats.emit(seq);
        Ok(seq)
    }
}

// ---------------------------------------------------------------------------
// NodeRecord merge (UpsertNode)
// ---------------------------------------------------------------------------

/// Merges an `UpsertNode`'s new labels/props into the prior `NodeRecord` (if
/// any): labels accumulate (v0 upsert is additive-only; explicit label
/// removal is a later feature), properties overlay by `prop_id` (new value
/// wins), `xid` is carried through unchanged (it is the resolution key, never
/// mutated by an upsert). Returns the merged record and the [`LabelDelta`]
/// describing what changed for the changelog.
fn merge_node_record(
    prior: Option<NodeRecord>,
    new_labels: Vec<LabelId>,
    new_props: BTreeMap<PropId, TypedValue>,
    xid: String,
) -> (NodeRecord, LabelDelta) {
    match prior {
        None => {
            let mut labels: BTreeSet<LabelId> = new_labels.into_iter().collect();
            let added: Vec<LabelId> = labels.iter().copied().collect();
            let labels: Vec<LabelId> = std::mem::take(&mut labels).into_iter().collect();
            let delta = LabelDelta {
                added,
                removed: Vec::new(),
            };
            (
                NodeRecord {
                    labels,
                    props: new_props,
                    xid,
                },
                delta,
            )
        }
        Some(mut old) => {
            let before: BTreeSet<LabelId> = old.labels.iter().copied().collect();
            let mut merged: BTreeSet<LabelId> = before.clone();
            merged.extend(new_labels);
            let added: Vec<LabelId> = merged.difference(&before).copied().collect();
            let labels: Vec<LabelId> = merged.into_iter().collect();

            for (prop_id, value) in new_props {
                old.props.insert(prop_id, value);
            }

            let delta = LabelDelta {
                added,
                removed: Vec::new(),
            };
            (
                NodeRecord {
                    labels,
                    props: old.props,
                    xid,
                },
                delta,
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Degree counter (Meta["count/<pred:4 BE><dir:1><anchor:8 BE>"])
// ---------------------------------------------------------------------------

/// Builds the degree-counter `Meta` key for `(pred, dir, anchor)`. Starts with
/// the `count/` prefix so [`crate::serde::keys::meta_kind`] classifies it
/// `MetaKind::Counter` (i64 LE merge-sum) — the same dispatch row
/// `crate::merge`'s own tests exercise, just with a per-anchor suffix instead
/// of only per-predicate.
fn degree_key(pred: PredId, dir: Direction, anchor: Uid) -> Bytes {
    let mut sub = BytesMut::with_capacity(6 + 1 + 8);
    sub.extend_from_slice(b"count/");
    sub.put_u32(pred.get());
    sub.put_u8(dir.as_byte());
    sub.put_u64(anchor.get());
    meta_key(&sub)
}

/// Builds the `RecordOp::Merge` that bumps `(pred, dir, anchor)`'s degree
/// counter by `delta` (an 8-byte LE `i64` operand — the bare encoding
/// `MetaKind::Counter`'s merge row expects).
fn bump_degree(pred: PredId, dir: Direction, anchor: Uid, delta: i64) -> RecordOp {
    let key = degree_key(pred, dir, anchor);
    let operand = Bytes::copy_from_slice(&delta.to_le_bytes());
    RecordOp::Merge(Record::new(key, operand).into())
}

// ---------------------------------------------------------------------------
// Small byte-level helpers
// ---------------------------------------------------------------------------

fn xid_utf8(xid: &[u8]) -> Result<String> {
    String::from_utf8(xid.to_vec())
        .map_err(|e| Error::encoding(format!("xid must be valid utf-8: {e}")))
}

fn encode_u64_le(v: u64) -> Bytes {
    Bytes::copy_from_slice(&v.to_le_bytes())
}

fn encode_u32_le(v: u32) -> Bytes {
    Bytes::copy_from_slice(&v.to_le_bytes())
}

fn decode_u64_le(bytes: &[u8], what: &str) -> Result<u64> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::encoding(format!("{what}: expected 8 bytes, got {}", bytes.len())))?;
    Ok(u64::from_le_bytes(arr))
}

fn decode_u64_be(bytes: &[u8], what: &str) -> Result<u64> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::encoding(format!("{what}: expected 8 bytes, got {}", bytes.len())))?;
    Ok(u64::from_be_bytes(arr))
}

fn decode_i64_le(bytes: &[u8], what: &str) -> Result<i64> {
    decode_u64_le(bytes, what).map(|v| v as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use common::storage::StorageSnapshot;
    use common::storage::in_memory::FailingStorage;
    use common::storage::in_memory::InMemoryStorage;
    use common::{
        BytesRange, CheckpointInfo, MergeRecordOp, PutRecordOp, Storage, StorageError,
        StorageIterator, StorageRead, StorageResult, WriteResult,
    };

    fn props(pairs: &[(&str, TypedValue)]) -> BTreeMap<String, TypedValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    struct FailFirstGetAfterApplyStorage {
        inner: Arc<dyn Storage>,
        fail_next_get: AtomicBool,
    }

    impl FailFirstGetAfterApplyStorage {
        fn wrap(inner: Arc<dyn Storage>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                fail_next_get: AtomicBool::new(false),
            })
        }
    }

    #[async_trait::async_trait]
    impl StorageRead for FailFirstGetAfterApplyStorage {
        async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
            if self.fail_next_get.swap(false, Ordering::SeqCst) {
                return Err(StorageError::Storage(
                    "simulated post-commit split read failure".into(),
                ));
            }
            self.inner.get(key).await
        }

        async fn scan_iter(
            &self,
            range: BytesRange,
        ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
            self.inner.scan_iter(range).await
        }
    }

    #[async_trait::async_trait]
    impl Storage for FailFirstGetAfterApplyStorage {
        async fn apply_with_options(
            &self,
            ops: Vec<RecordOp>,
            options: WriteOptions,
        ) -> StorageResult<WriteResult> {
            let result = self.inner.apply_with_options(ops, options).await;
            if result.is_ok() {
                self.fail_next_get.store(true, Ordering::SeqCst);
            }
            result
        }

        async fn put_with_options(
            &self,
            records: Vec<PutRecordOp>,
            options: WriteOptions,
        ) -> StorageResult<WriteResult> {
            self.inner.put_with_options(records, options).await
        }

        async fn merge_with_options(
            &self,
            records: Vec<MergeRecordOp>,
            options: WriteOptions,
        ) -> StorageResult<WriteResult> {
            self.inner.merge_with_options(records, options).await
        }

        async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>> {
            self.inner.snapshot().await
        }

        async fn flush(&self) -> StorageResult<()> {
            self.inner.flush().await
        }

        fn subscribe_durable(&self) -> tokio::sync::watch::Receiver<u64> {
            self.inner.subscribe_durable()
        }

        async fn create_checkpoint(&self) -> StorageResult<CheckpointInfo> {
            self.inner.create_checkpoint().await
        }
    }

    // -- Basic upsert/read round trip -----------------------------------

    #[tokio::test]
    async fn should_upsert_node_and_read_it_back() {
        let mut writer = Writer::in_memory().await.unwrap();

        let seq = writer
            .upsert_node(
                b"user:alice",
                &["Person".to_string()],
                props(&[("name", TypedValue::String("Alice".to_string()))]),
            )
            .await
            .unwrap();
        assert_eq!(seq, 1, "first commit on a fresh writer is logical seq 1");

        let uid = writer.lookup_uid(b"user:alice").await.unwrap().unwrap();
        let node = writer.get_node(uid).await.unwrap().unwrap();
        assert_eq!(node.xid, "user:alice");
        assert_eq!(node.labels.len(), 1);
        assert_eq!(node.props.len(), 1);
    }

    #[tokio::test]
    async fn should_be_idempotent_on_repeat_upsert_of_the_same_xid() {
        let mut writer = Writer::in_memory().await.unwrap();

        writer
            .upsert_node(b"user:bob", &["Person".to_string()], BTreeMap::new())
            .await
            .unwrap();
        let uid1 = writer.lookup_uid(b"user:bob").await.unwrap().unwrap();

        writer
            .upsert_node(b"user:bob", &[], props(&[("age", TypedValue::Int(30))]))
            .await
            .unwrap();
        let uid2 = writer.lookup_uid(b"user:bob").await.unwrap().unwrap();

        assert_eq!(
            uid1, uid2,
            "re-upserting the same xid must not mint a new uid"
        );
        let node = writer.get_node(uid1).await.unwrap().unwrap();
        assert_eq!(node.labels.len(), 1, "label from the first upsert persists");
        assert_eq!(node.props.len(), 1, "prop from the second upsert merges in");
    }

    #[tokio::test]
    async fn should_reject_oversize_node_without_writing_anything() {
        let mut writer = Writer::in_memory().await.unwrap();

        let huge = TypedValue::Bytes(Bytes::from(vec![0u8; 2 * 1024 * 1024]));
        let err = writer
            .upsert_node(b"doc:huge", &[], props(&[("blob", huge)]))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("oversize_node"),
            "expected oversize_node error, got: {err}"
        );

        assert_eq!(
            writer.lookup_uid(b"doc:huge").await.unwrap(),
            None,
            "a rejected upsert must not create the xid mapping either"
        );
        assert_eq!(
            writer.latest_seq(),
            0,
            "no seq should be consumed by a rejected write"
        );
    }

    // -- RFC 0004 acceptance #6: atomic fan-out in exactly one seq -------

    #[tokio::test]
    async fn should_fan_out_upsert_edge_atomically_at_one_seq() {
        let mut writer = Writer::in_memory().await.unwrap();
        let mut durable_rx = writer.storage().subscribe_durable();
        assert_eq!(*durable_rx.borrow(), 0);

        let token = writer
            .upsert_edge(b"user:a", "knows", b"user:b")
            .await
            .unwrap();

        // Durable seq advanced by exactly one, and matches the returned token.
        durable_rx.changed().await.unwrap();
        assert_eq!(*durable_rx.borrow(), token);
        assert_eq!(token, 1, "first commit on a fresh writer is logical seq 1");

        let src = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let dst = writer.lookup_uid(b"user:b").await.unwrap().unwrap();
        let pred = writer.schema_id(SchemaKind::Predicate, "knows").unwrap();
        let pred = PredId(pred);

        // Both stub node records exist (endpoints were brand new).
        assert!(writer.get_node(src).await.unwrap().is_some());
        assert!(writer.get_node(dst).await.unwrap().is_some());

        // Both adjacency projections exist and are populated.
        let out = writer.neighbors(src, pred, Direction::Out).await.unwrap();
        assert!(out.contains(dst.get()));
        let inn = writer.neighbors(dst, pred, Direction::In).await.unwrap();
        assert!(inn.contains(src.get()));

        // Degree counters reflect the add.
        assert_eq!(writer.degree(pred, Direction::Out, src).await.unwrap(), 1);
        assert_eq!(writer.degree(pred, Direction::In, dst).await.unwrap(), 1);

        // Changelog + latest_seq are set to the same token.
        let log = writer.storage().get(log_key(token)).await.unwrap().unwrap();
        let change = ChangeRecord::decode(&log).unwrap();
        assert_eq!(change.seq, token);
        assert_eq!(change.op, ChangeOp::UpsertEdge);
        assert_eq!(writer.latest_seq(), token);
    }

    #[tokio::test]
    async fn should_return_committed_seq_when_post_commit_split_read_fails() {
        let raw: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            crate::merge::GraphMergeOperator,
        )));
        let flaky = FailFirstGetAfterApplyStorage::wrap(raw);
        let mut writer = Writer::from_storage(GraphStorage::from_storage(flaky))
            .await
            .unwrap();

        let seq = writer
            .upsert_edge(b"user:a", "knows", b"user:b")
            .await
            .expect("post-commit split read failure must not fail the committed edge write");

        assert_eq!(seq, 1);
        assert_eq!(writer.latest_seq(), 1);

        let src = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let dst = writer.lookup_uid(b"user:b").await.unwrap().unwrap();
        let pred = PredId(writer.schema_id(SchemaKind::Predicate, "knows").unwrap());
        let out = writer.neighbors(src, pred, Direction::Out).await.unwrap();

        assert!(
            out.contains(dst.get()),
            "the edge must be visible even though post-commit split maintenance failed"
        );
        assert_eq!(writer.degree(pred, Direction::Out, src).await.unwrap(), 1);
        assert_eq!(writer.degree(pred, Direction::In, dst).await.unwrap(), 1);
    }

    // -- Workstream A: EdgeProp companion (valued/faceted edges) ---------

    #[tokio::test]
    async fn should_write_and_read_back_valued_edge_props() {
        let mut writer = Writer::in_memory().await.unwrap();

        writer
            .upsert_edge_with_props(
                b"user:a",
                "knows",
                b"user:b",
                props(&[("since", TypedValue::Int(2020))]),
            )
            .await
            .unwrap();

        let src = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let dst = writer.lookup_uid(b"user:b").await.unwrap().unwrap();
        let pred = PredId(writer.schema_id(SchemaKind::Predicate, "knows").unwrap());

        let got = writer.edge_props(src, pred, dst).await.unwrap().unwrap();
        let prop_id = PropId(writer.schema_id(SchemaKind::PropertyKey, "since").unwrap());
        assert_eq!(got.len(), 1);
        assert_eq!(got.get(&prop_id), Some(&TypedValue::Int(2020)));
    }

    #[tokio::test]
    async fn should_commit_edge_props_at_the_same_seq_as_the_edge() {
        let mut writer = Writer::in_memory().await.unwrap();

        let token = writer
            .upsert_edge_with_props(
                b"user:a",
                "knows",
                b"user:b",
                props(&[("since", TypedValue::Int(2020))]),
            )
            .await
            .unwrap();

        let src = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let dst = writer.lookup_uid(b"user:b").await.unwrap().unwrap();
        let pred = PredId(writer.schema_id(SchemaKind::Predicate, "knows").unwrap());

        // The companion is already readable right after the token is
        // returned — it was committed in the same atomic batch.
        assert!(writer.edge_props(src, pred, dst).await.unwrap().is_some());

        let log = writer.storage().get(log_key(token)).await.unwrap().unwrap();
        let change = ChangeRecord::decode(&log).unwrap();
        assert_eq!(change.seq, token);
        assert_eq!(
            change.op,
            ChangeOp::UpsertEdge,
            "no new ChangeOp for a valued edge — the companion is a storage \
             detail, not a change-stream event"
        );
    }

    #[tokio::test]
    async fn should_remove_companion_on_delete_edge() {
        let mut writer = Writer::in_memory().await.unwrap();
        writer
            .upsert_edge_with_props(
                b"user:a",
                "knows",
                b"user:b",
                props(&[("since", TypedValue::Int(2020))]),
            )
            .await
            .unwrap();

        let src = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let dst = writer.lookup_uid(b"user:b").await.unwrap().unwrap();
        let pred = PredId(writer.schema_id(SchemaKind::Predicate, "knows").unwrap());
        assert!(writer.edge_props(src, pred, dst).await.unwrap().is_some());

        writer
            .delete_edge(b"user:a", "knows", b"user:b")
            .await
            .unwrap();

        assert_eq!(
            writer.edge_props(src, pred, dst).await.unwrap(),
            None,
            "delete_edge must blind-delete the EdgeProp companion too"
        );
    }

    #[tokio::test]
    async fn should_preserve_props_on_plain_re_upsert_of_a_valued_edge() {
        let mut writer = Writer::in_memory().await.unwrap();
        writer
            .upsert_edge_with_props(
                b"user:a",
                "knows",
                b"user:b",
                props(&[("since", TypedValue::Int(2020))]),
            )
            .await
            .unwrap();

        // A plain upsert_edge of the same (src, pred, dst) must never touch
        // the companion.
        writer
            .upsert_edge(b"user:a", "knows", b"user:b")
            .await
            .unwrap();

        let src = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let dst = writer.lookup_uid(b"user:b").await.unwrap().unwrap();
        let pred = PredId(writer.schema_id(SchemaKind::Predicate, "knows").unwrap());
        let got = writer.edge_props(src, pred, dst).await.unwrap().unwrap();
        assert_eq!(got.len(), 1, "prior props must survive a plain re-upsert");
    }

    #[tokio::test]
    async fn should_write_no_companion_for_empty_props_map() {
        let mut writer = Writer::in_memory().await.unwrap();
        writer
            .upsert_edge_with_props(b"user:a", "knows", b"user:b", BTreeMap::new())
            .await
            .unwrap();

        let src = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let dst = writer.lookup_uid(b"user:b").await.unwrap().unwrap();
        let pred = PredId(writer.schema_id(SchemaKind::Predicate, "knows").unwrap());
        assert_eq!(writer.edge_props(src, pred, dst).await.unwrap(), None);
    }

    #[tokio::test]
    async fn should_write_no_companion_for_plain_upsert_edge() {
        let mut writer = Writer::in_memory().await.unwrap();
        writer
            .upsert_edge(b"user:a", "knows", b"user:b")
            .await
            .unwrap();

        let src = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let dst = writer.lookup_uid(b"user:b").await.unwrap().unwrap();
        let pred = PredId(writer.schema_id(SchemaKind::Predicate, "knows").unwrap());
        assert_eq!(writer.edge_props(src, pred, dst).await.unwrap(), None);
    }

    // -- RFC 0004 acceptance #4: delete correctness ----------------------

    #[tokio::test]
    async fn should_filter_deleted_edge_both_directions_and_update_degree() {
        let mut writer = Writer::in_memory().await.unwrap();
        writer
            .upsert_edge(b"user:a", "knows", b"user:b")
            .await
            .unwrap();

        let src = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let dst = writer.lookup_uid(b"user:b").await.unwrap().unwrap();
        let pred = PredId(writer.schema_id(SchemaKind::Predicate, "knows").unwrap());

        writer
            .delete_edge(b"user:a", "knows", b"user:b")
            .await
            .unwrap();

        let out = writer.neighbors(src, pred, Direction::Out).await.unwrap();
        assert!(
            !out.contains(dst.get()),
            "Out side must filter the deleted edge"
        );
        let inn = writer.neighbors(dst, pred, Direction::In).await.unwrap();
        assert!(
            !inn.contains(src.get()),
            "In side must filter the deleted edge too"
        );

        assert_eq!(writer.degree(pred, Direction::Out, src).await.unwrap(), 0);
        assert_eq!(writer.degree(pred, Direction::In, dst).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn should_filter_deleted_node_from_neighbors_in_both_directions() {
        let mut writer = Writer::in_memory().await.unwrap();
        writer
            .upsert_edge(b"user:a", "knows", b"user:b")
            .await
            .unwrap();
        writer
            .upsert_edge(b"user:c", "knows", b"user:a")
            .await
            .unwrap();

        let a = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let b = writer.lookup_uid(b"user:b").await.unwrap().unwrap();
        let c = writer.lookup_uid(b"user:c").await.unwrap().unwrap();
        let pred = PredId(writer.schema_id(SchemaKind::Predicate, "knows").unwrap());

        writer.delete_node(b"user:a").await.unwrap();

        // b's In-list still stores a, but the read path filters it.
        let b_in = writer.neighbors(b, pred, Direction::In).await.unwrap();
        assert!(
            !b_in.contains(a.get()),
            "deleted anchor must be filtered from downstream reads"
        );

        // a's own Out-list, if queried directly, also comes back empty via the
        // deleted-node filter (belt-and-suspenders: a is gone everywhere).
        let a_out = writer.neighbors(a, pred, Direction::Out).await.unwrap();
        assert!(
            a_out.contains(b.get()),
            "the raw posting is untouched (tombstone-and-filter, not physical purge)"
        );

        // c's Out-list still *physically* stores a (tombstone-and-filter, not
        // physical purge — that's vacuum, RFC 0012). But a live `neighbors`
        // read subtracts the deleted-node bitmap from every result, so a is
        // filtered out wherever it appears, including as a destination.
        let c_out = writer.neighbors(c, pred, Direction::Out).await.unwrap();
        assert!(
            !c_out.contains(a.get()),
            "a deleted node is filtered from neighbor reads in every position"
        );
    }

    #[tokio::test]
    async fn should_no_op_delete_of_unknown_xid_or_edge() {
        let mut writer = Writer::in_memory().await.unwrap();
        assert_eq!(writer.delete_node(b"nope").await.unwrap(), 0);
        assert_eq!(
            writer
                .delete_edge(b"nope", "knows", b"also-nope")
                .await
                .unwrap(),
            0
        );
        assert_eq!(writer.latest_seq(), 0);
    }

    // -- RFC 0004 acceptance #5: xid stability + uid monotonicity across
    //    a half-used-block restart ---------------------------------------

    #[tokio::test]
    async fn should_keep_xid_stable_and_uids_monotonic_across_restart() {
        let storage = GraphStorage::in_memory().await.unwrap();

        let (alice_uid, bob_uid, seq1) = {
            let mut writer = Writer::from_storage(storage.clone()).await.unwrap();
            let seq = writer
                .upsert_edge(b"user:alice", "knows", b"user:bob")
                .await
                .unwrap();
            let alice = writer.lookup_uid(b"user:alice").await.unwrap().unwrap();
            let bob = writer.lookup_uid(b"user:bob").await.unwrap().unwrap();
            (alice, bob, seq)
        }; // writer dropped here — simulates a restart with a half-used uid/seq block.

        let mut writer2 = Writer::from_storage(storage.clone()).await.unwrap();
        assert_eq!(writer2.latest_seq(), seq1, "recovery resumes latest_seq");

        // Same xids resolve to the same uids — no re-minting across restart.
        assert_eq!(
            writer2.lookup_uid(b"user:alice").await.unwrap(),
            Some(alice_uid)
        );
        assert_eq!(
            writer2.lookup_uid(b"user:bob").await.unwrap(),
            Some(bob_uid)
        );

        // A brand-new xid after restart gets a uid beyond every previously
        // allocated one (uid space never reuses, even mid-block).
        let seq2 = writer2
            .upsert_edge(b"user:carol", "knows", b"user:alice")
            .await
            .unwrap();
        assert!(seq2 > seq1, "seq must be strictly monotonic across restart");
        let carol_uid = writer2.lookup_uid(b"user:carol").await.unwrap().unwrap();
        assert!(
            carol_uid.get() > alice_uid.get() && carol_uid.get() > bob_uid.get(),
            "post-restart uid must exceed every pre-restart uid"
        );
    }

    // -- RFC 0004 acceptance #3: read-your-writes across a reader --------

    #[tokio::test]
    async fn should_read_a_committed_write_through_a_separate_reader_handle() {
        // `common::create_storage_read`'s `StorageConfig::InMemory` branch
        // constructs a brand-new, disconnected `InMemoryStorage` (see
        // `vendor/common/src/storage/factory.rs`'s `create_storage_read`) —
        // it does NOT share data with a writer opened via
        // `StorageConfig::InMemory`, so it cannot stand in for "a reader over
        // the same namespace" in this backend. The in-memory-correct
        // equivalent of "a separate reader handle to the same namespace,
        // built with the same merge operator" is simply another
        // `GraphStorage` over the *same* `Arc<dyn Storage>` — which
        // `GraphStorage::clone()` already gives us (it's a thin,
        // `Arc`-cloning handle, per its own doc comment). This is the
        // strongest RYW test the `common` in-memory backend's public API
        // supports; a real `DbReader`-backed test needs the SlateDB backend,
        // which is out of scope here (RFC 0017 D12, correctness on
        // `common::InMemoryStorage` only).
        let mut writer = Writer::in_memory().await.unwrap();
        let reader_storage = writer.storage().clone();

        let token = writer
            .upsert_edge(b"user:a", "knows", b"user:b")
            .await
            .unwrap();

        // Read-your-writes holds by commit ordering: `upsert_edge` committed
        // with `await_durable: true`, so `token` is already durable on return
        // and the write is immediately visible through a separate handle.
        //
        // NOTE: we deliberately do NOT block on `subscribe_durable().changed()`
        // here. `common`'s in-memory backend never drives the durable-seq watch
        // channel (that machinery is SlateDB-only — see
        // `in_memory.rs::apply_with_options`, which never sends on `durable_tx`),
        // so awaiting it would hang forever. The subscribe-and-wait freshness
        // gate is exercised end-to-end on the SlateDB backend in M2 (RFC 0001);
        // RFC 0017 D12 keeps M1 correctness on the in-memory backend only.
        assert!(token > 0, "a committed write must yield a durable token");

        // Minimal raw reader-side read (not the M2 query planner): the node
        // record is visible through the separate handle.
        let uid = writer.lookup_uid(b"user:a").await.unwrap().unwrap();
        let raw = reader_storage.get(node_key(uid)).await.unwrap();
        assert!(
            raw.is_some(),
            "just-written node must be visible via the reader handle"
        );
    }

    // -- RFC 0004 acceptance #1: crash-recovery mid-batch ----------------
    //
    // `common::storage::in_memory::FailingStorage` (the `test-utils`-gated
    // fault-injection wrapper) can only fail a whole `apply_with_options`
    // call *before* it reaches the inner backend — there is no hook to
    // truncate a `WriteBatch` partway through (SlateDB's own `WriteBatch` is
    // atomic by construction, and `common` doesn't expose a way to defeat
    // that atomicity for testing). So this test proves the strongest
    // available guarantee: a "crash" (failed `apply`) leaves **zero** trace
    // of the attempted write — not a merge operand applied without its
    // sibling ops, not a changelog entry without `latest_seq`, nothing — and
    // that a fresh `Writer` reopened over the same underlying (un-wrapped)
    // storage resumes cleanly and keeps allocating strictly-increasing seqs.
    #[tokio::test]
    async fn should_leave_no_partial_state_when_a_batch_fails_to_apply() {
        use common::StorageError;

        let raw: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            crate::merge::GraphMergeOperator,
        )));
        let failing = FailingStorage::wrap(raw.clone());

        let mut writer = Writer::from_storage(GraphStorage::from_storage(failing.clone()))
            .await
            .unwrap();

        // A first write succeeds normally — baseline state.
        let baseline = writer
            .upsert_edge(b"user:a", "knows", b"user:b")
            .await
            .unwrap();
        assert_eq!(baseline, 1);

        // Now inject a failure and attempt a second write — the "crash".
        failing.fail_apply_once(StorageError::Storage("simulated crash".into()));
        let result = writer.upsert_edge(b"user:c", "knows", b"user:d").await;
        assert!(
            result.is_err(),
            "the injected failure must surface as an error"
        );

        // Nothing from the failed attempt is visible even through the
        // failing handle itself (the call never reached the inner backend).
        assert_eq!(writer.lookup_uid(b"user:c").await.unwrap(), None);
        assert_eq!(
            writer.latest_seq(),
            baseline,
            "latest_seq must not advance on a failed commit"
        );

        // "Reopen after crash": a fresh writer over the *raw*, un-wrapped
        // storage (bypassing the now-healed FailingStorage entirely, the way
        // a real process restart would bypass whatever transient fault killed
        // it) sees exactly the baseline state and nothing from the failed
        // attempt.
        let mut recovered = Writer::from_storage(GraphStorage::from_storage(raw.clone()))
            .await
            .unwrap();
        assert_eq!(recovered.latest_seq(), baseline);
        assert!(recovered.lookup_uid(b"user:a").await.unwrap().is_some());
        assert_eq!(recovered.lookup_uid(b"user:c").await.unwrap(), None);

        // And it continues the seq lineage strictly past the baseline on the
        // next successful write (no reuse, no gap-induced collision).
        let next = recovered
            .upsert_edge(b"user:e", "knows", b"user:f")
            .await
            .unwrap();
        assert!(next > baseline);
    }

    // -- RFC 0004 acceptance #2: zombie-writer fencing -------------------
    //
    // BLOCKED: `vendor/common` exposes no writer-epoch fencing concept at
    // all for the in-memory backend — there is no `CloseReason`, no
    // `Fenced` variant, nothing resembling SlateDB's single-writer-per-path
    // fence anywhere in `vendor/common/src` (confirmed by grep: zero
    // occurrences of "Fenced"/"CloseReason" in the vendored crate). Fencing
    // is inherently a property of SlateDB's real object-store-backed `Db`
    // (one manifest-holding writer per path) — `InMemoryStorage` is just a
    // shared `BTreeMap` with no notion of "writer identity" to fence at all,
    // so there is no in-memory-only way to exercise this. Testing it for
    // real requires opening two `StorageConfig::SlateDb` writers against the
    // same path/object store, which needs a real (if local-disk) SlateDB
    // instance — explicitly out of this M1 milestone's test policy ("no
    // S3, no LocalStack... correctness on `common` in-memory storage").
    // No test is implemented for this item; see the handoff report for the
    // precise gap.

    // -- WriteOptions.seqnum plumbing sanity (supporting evidence for the
    //    logical == durable seq identity acceptance #6 leans on) ----------

    #[tokio::test]
    async fn should_reject_non_monotonic_injected_seqnum() {
        // Sanity check on the `common` primitive this whole module is built
        // on: a directly-injected non-increasing seqnum is rejected, which is
        // what guarantees `commit`'s `seq = raw_seq + 1` (monotonic via the
        // allocator) can never collide.
        let storage = InMemoryStorage::new();
        let first: WriteResult = storage
            .apply_with_options(
                vec![],
                WriteOptions {
                    await_durable: true,
                    seqnum: 5,
                },
            )
            .await
            .unwrap();
        assert_eq!(first.seqnum, 5);

        let rejected = storage
            .apply_with_options(
                vec![],
                WriteOptions {
                    await_durable: true,
                    seqnum: 5,
                },
            )
            .await;
        assert!(rejected.is_err());
    }

    // -- Bug #1: a post-commit split must not break the next write --------
    //
    // `upsert_edge_inner`/`ingest_batch` run `maybe_split` AFTER the durable
    // injected-seqnum commit and apply its ops with a **bare**
    // `storage.apply(split_ops)` — i.e. `WriteOptions::seqnum == 0`, which the
    // backend treats as *auto-generate* and which advances `written_seq`
    // out-of-band (`in_memory.rs::next_seqnum`). The writer's `latest_seq`
    // never learns about that bump, so its NEXT injected seqnum is no longer
    // strictly greater than `written_seq` and the commit is rejected. This is
    // latent only because no existing test drives a real 512 KiB split through
    // the writer (the split tests apply through a test helper); here we seed
    // one so the post-commit split actually fires.
    #[tokio::test]
    async fn post_commit_split_must_not_break_the_next_write() {
        let raw: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            crate::merge::GraphMergeOperator,
        )));
        let gs = GraphStorage::from_storage(raw.clone());

        // Phase 1: a normal writer creates node "a", predicate "knows", and one
        // edge — establishing the uid/pred the oversized posting will live at.
        let mut w1 = Writer::from_storage(GraphStorage::from_storage(raw.clone()))
            .await
            .unwrap();
        let s1 = w1.upsert_edge(b"a", "knows", b"b").await.unwrap();
        assert_eq!(s1, 1);
        let src = w1.lookup_uid(b"a").await.unwrap().unwrap();
        let pred = PredId(w1.schema_id(SchemaKind::Predicate, "knows").unwrap());
        drop(w1);

        // Build an Out-adjacency posting that exceeds the 512 KiB split
        // threshold. One uid per treemap bucket (`k << 32`) maximizes
        // bytes-per-uid, so a few tens of thousands of uids suffice — no need
        // for the millions a dense, run-compressible set would require.
        let mut big = RoaringTreemap::new();
        let mut k: u64 = 1;
        while 2 + big.serialized_size() <= posting_ops::SPLIT_THRESHOLD {
            for _ in 0..4096 {
                big.insert(k << 32);
                k += 1;
            }
        }
        let oversized = crate::posting::PostingValue::single(big).serialize();

        // Seed it at (Out, src, knows) with an injected seqnum just above the
        // phase-1 commit. Recovery below resumes the logical-seq allocator well
        // past this (the abandoned seq block), so it does not collide.
        gs.apply_with_options(
            vec![RecordOp::Put(PutRecordOp::new(Record::new(
                crate::serde::keys::edge_key(Direction::Out, src, pred),
                oversized,
            )))],
            WriteOptions {
                await_durable: true,
                seqnum: 2,
            },
        )
        .await
        .unwrap();

        // Phase 2: recover a fresh writer (its logical-seq allocator resumes
        // past the abandoned block, safely above the seeded seqnum).
        let mut w2 = Writer::from_storage(GraphStorage::from_storage(raw.clone()))
            .await
            .unwrap();

        // This edge commits, then its post-commit `maybe_split` fires on the
        // oversized (Out, src) posting and bare-applies the split ops.
        let s_c = w2.upsert_edge(b"a", "knows", b"c").await.unwrap();

        // Precondition: the split really happened (else the test is vacuous).
        let base = gs
            .get(crate::serde::keys::edge_key(Direction::Out, src, pred))
            .await
            .unwrap()
            .unwrap();
        assert!(
            crate::posting::PostingValue::deserialize(base.as_ref())
                .unwrap()
                .is_split(),
            "precondition: the post-commit split must have fired",
        );

        // THE BUG: the next write is rejected because the bare split apply
        // auto-generated a backend seqnum (one past the c-edge commit), so
        // w2's next injected seqnum is no longer strictly greater than the
        // backend max and the commit is refused.
        let next = w2.upsert_edge(b"a", "knows", b"d").await;
        assert!(
            next.is_ok(),
            "a write after a post-commit split must still succeed; got {next:?}",
        );
        assert!(
            next.unwrap() > s_c,
            "the follow-up write must advance the logical seq past the split",
        );
    }

    // -- Bug #2: a failed commit must not poison the schema cache ----------
    //
    // `intern` mutates the in-memory `SchemaCache` (and advances the id
    // allocator) BEFORE the batch commits, and only queues the durable
    // `SchemaId` `Put` on a cache MISS. If the enclosing commit then fails,
    // the cache keeps `name -> id` but nothing durable backs it; a later
    // SUCCESSFUL write for the same name hits the cache, emits no schema op,
    // and commits a node referencing an id whose `SchemaId` record was never
    // persisted — leaving it unresolvable after recovery.
    #[tokio::test]
    async fn failed_commit_must_not_poison_the_schema_cache() {
        let raw: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            crate::merge::GraphMergeOperator,
        )));
        let failing = FailingStorage::wrap(raw.clone());
        let mut w = Writer::from_storage(GraphStorage::from_storage(failing.clone()))
            .await
            .unwrap();

        // First upsert interns the new prop "blob" (mutating the cache), then
        // its commit is forced to fail — nothing durable is written.
        failing.fail_apply_once(StorageError::Storage("simulated crash".into()));
        let failed = w
            .upsert_node(b"doc:huge", &[], props(&[("blob", TypedValue::Int(1))]))
            .await;
        assert!(
            failed.is_err(),
            "the injected failure must surface as an error"
        );

        // Second upsert of the SAME prop name succeeds — but on the poisoned
        // cache it emits no `SchemaId` op, committing a node that references
        // the interned prop id with no durable name record behind it.
        w.upsert_node(b"doc:small", &[], props(&[("blob", TypedValue::Int(5))]))
            .await
            .unwrap();
        let uid = w.lookup_uid(b"doc:small").await.unwrap().unwrap();
        let node = w.get_node(uid).await.unwrap().unwrap();
        assert!(
            !node.props.is_empty(),
            "the second upsert must have committed the 'blob' property",
        );

        // Recover over the raw (healed) storage: `SchemaCache` is rebuilt from
        // durable `SchemaId` records only. "blob" is referenced by a committed
        // node, so its name MUST be resolvable — today it is not (None).
        let recovered = Writer::from_storage(GraphStorage::from_storage(raw.clone()))
            .await
            .unwrap();
        assert!(
            recovered
                .schema_id(SchemaKind::PropertyKey, "blob")
                .is_some(),
            "prop 'blob' is referenced by a committed node but has no durable \
             SchemaId record after recovery — the schema cache was poisoned by \
             the earlier failed commit",
        );
    }

    // -- Batched ingest: correctness oracle (batched == per-record) -------
    //
    // `ingest_batch` exists purely as a throughput optimization over the
    // per-record path — every test in this section holds it to the standard
    // that its *observable* result must be indistinguishable from the
    // sequential per-record calls it replaces, for the exact cases the
    // module doc flags as the crux: an xid touched twice in one batch (both
    // as a `Node` record and as an edge endpoint stub), an xid referenced
    // from two different edges in the same batch (in-batch xid cache reuse),
    // and a duplicate edge in the same batch (repeated posting/degree merges
    // to the same key in one physical commit).

    /// Three `Node` records (`alice` touched twice — must merge onto her own
    /// in-batch stub/prior state, not collide) and four `Edge` records
    /// (`carol` is brand new and only ever referenced via edges, referenced
    /// from two different edges; the last edge duplicates the first).
    fn sample_ingest_records() -> (Vec<IngestRecord>, Vec<IngestRecord>) {
        let nodes = vec![
            IngestRecord::Node {
                xid: b"user:alice".to_vec(),
                labels: vec!["Person".to_string()],
                props: props(&[
                    ("name", TypedValue::String("Alice".to_string())),
                    ("age", TypedValue::Int(30)),
                ]),
            },
            IngestRecord::Node {
                xid: b"user:bob".to_vec(),
                labels: vec!["Person".to_string()],
                props: props(&[("name", TypedValue::String("Bob".to_string()))]),
            },
            // A second record for the same xid: must merge onto the first
            // (additive labels, prop overlay), not double-allocate alice's
            // uid or collide with her already-queued Put.
            IngestRecord::Node {
                xid: b"user:alice".to_vec(),
                labels: vec!["Admin".to_string()],
                props: props(&[("age", TypedValue::Int(31))]),
            },
        ];
        let edges = vec![
            IngestRecord::Edge {
                src_xid: b"user:alice".to_vec(),
                pred: "knows".to_string(),
                dst_xid: b"user:bob".to_vec(),
                props: props(&[("since", TypedValue::Int(2020))]),
            },
            // carol is brand new and only ever referenced via edges — must
            // get a stub NodeRecord and a stable uid.
            IngestRecord::Edge {
                src_xid: b"user:alice".to_vec(),
                pred: "knows".to_string(),
                dst_xid: b"user:carol".to_vec(),
                props: BTreeMap::new(),
            },
            // carol referenced again — must resolve to the same uid the
            // batch already minted for her (in-batch xid cache), not a
            // second uid.
            IngestRecord::Edge {
                src_xid: b"user:bob".to_vec(),
                pred: "knows".to_string(),
                dst_xid: b"user:carol".to_vec(),
                props: BTreeMap::new(),
            },
            // A duplicate of the first edge (same src/pred/dst) — exercises
            // repeated posting/degree merges targeting the same key within
            // one physical commit.
            IngestRecord::Edge {
                src_xid: b"user:alice".to_vec(),
                pred: "knows".to_string(),
                dst_xid: b"user:bob".to_vec(),
                props: BTreeMap::new(),
            },
        ];
        (nodes, edges)
    }

    async fn apply_per_record(writer: &mut Writer, nodes: &[IngestRecord], edges: &[IngestRecord]) {
        for n in nodes {
            let IngestRecord::Node { xid, labels, props } = n else {
                panic!("sample_ingest_records' nodes must all be Node variants");
            };
            writer
                .upsert_node(xid, labels, props.clone())
                .await
                .unwrap();
        }
        for e in edges {
            let IngestRecord::Edge {
                src_xid,
                pred,
                dst_xid,
                props,
            } = e
            else {
                panic!("sample_ingest_records' edges must all be Edge variants");
            };
            writer
                .upsert_edge_with_props(src_xid, pred, dst_xid, props.clone())
                .await
                .unwrap();
        }
    }

    /// The observable state the correctness oracle compares: every sample
    /// node's decoded record, every sample `(anchor,pred,dir)`'s neighbor
    /// set, every sample degree counter, and the valued edge's props.
    #[derive(Debug, PartialEq)]
    struct IngestSnapshot {
        alice_node: NodeRecord,
        bob_node: NodeRecord,
        carol_node: NodeRecord,
        alice_out_knows: Vec<u64>,
        bob_out_knows: Vec<u64>,
        carol_in_knows: Vec<u64>,
        alice_out_degree: i64,
        bob_in_degree: i64,
        carol_in_degree: i64,
        alice_knows_bob_props: Option<EdgeProps>,
    }

    async fn snapshot(writer: &Writer) -> IngestSnapshot {
        let alice = writer.lookup_uid(b"user:alice").await.unwrap().unwrap();
        let bob = writer.lookup_uid(b"user:bob").await.unwrap().unwrap();
        let carol = writer.lookup_uid(b"user:carol").await.unwrap().unwrap();
        let pred = PredId(writer.schema_id(SchemaKind::Predicate, "knows").unwrap());

        let mut alice_out_knows: Vec<u64> = writer
            .neighbors(alice, pred, Direction::Out)
            .await
            .unwrap()
            .iter()
            .collect();
        alice_out_knows.sort_unstable();
        let mut bob_out_knows: Vec<u64> = writer
            .neighbors(bob, pred, Direction::Out)
            .await
            .unwrap()
            .iter()
            .collect();
        bob_out_knows.sort_unstable();
        let mut carol_in_knows: Vec<u64> = writer
            .neighbors(carol, pred, Direction::In)
            .await
            .unwrap()
            .iter()
            .collect();
        carol_in_knows.sort_unstable();

        IngestSnapshot {
            alice_node: writer.get_node(alice).await.unwrap().unwrap(),
            bob_node: writer.get_node(bob).await.unwrap().unwrap(),
            carol_node: writer.get_node(carol).await.unwrap().unwrap(),
            alice_out_knows,
            bob_out_knows,
            carol_in_knows,
            alice_out_degree: writer.degree(pred, Direction::Out, alice).await.unwrap(),
            bob_in_degree: writer.degree(pred, Direction::In, bob).await.unwrap(),
            carol_in_degree: writer.degree(pred, Direction::In, carol).await.unwrap(),
            alice_knows_bob_props: writer.edge_props(alice, pred, bob).await.unwrap(),
        }
    }

    #[tokio::test]
    async fn should_produce_identical_state_via_batched_and_per_record_ingest() {
        let (nodes, edges) = sample_ingest_records();

        // Oracle: sequential per-record upsert_node/upsert_edge_with_props
        // calls, one durable commit each — today's only write path.
        let mut per_record = Writer::in_memory().await.unwrap();
        apply_per_record(&mut per_record, &nodes, &edges).await;
        let oracle = snapshot(&per_record).await;

        // Candidate 1: one single `ingest_batch` call over every record
        // (nodes then edges, same order as the oracle), one physical commit
        // for the whole dataset.
        let mut single_batch = Writer::in_memory().await.unwrap();
        let mut all = nodes.clone();
        all.extend(edges.clone());
        single_batch.ingest_batch(&all).await.unwrap();
        assert_eq!(
            oracle,
            snapshot(&single_batch).await,
            "a single ingest_batch call must match the per-record oracle exactly"
        );

        // Candidate 2: the bench loader's actual shape — one `ingest_batch`
        // call for the node phase, then one for the edge phase (edges
        // reference node xids already committed by the node phase).
        let mut split_batch = Writer::in_memory().await.unwrap();
        split_batch.ingest_batch(&nodes).await.unwrap();
        split_batch.ingest_batch(&edges).await.unwrap();
        assert_eq!(
            oracle,
            snapshot(&split_batch).await,
            "a node-phase-then-edge-phase ingest_batch pair must match the per-record oracle"
        );
    }

    // -- Seq monotonicity across batches + reopen recovery ----------------

    #[tokio::test]
    async fn should_keep_seqs_strictly_monotonic_across_batches_and_recover_cleanly() {
        let storage = GraphStorage::in_memory().await.unwrap();
        let (nodes, edges) = sample_ingest_records();

        let (seq_batch1, seq_batch2, seq_batch3) = {
            let mut writer = Writer::from_storage(storage.clone()).await.unwrap();
            assert_eq!(writer.latest_seq(), 0);

            // Batch 1: the node phase — 3 logical records, one physical commit.
            let seq1 = writer.ingest_batch(&nodes).await.unwrap();
            assert_eq!(
                seq1, 3,
                "3 logical Node records should consume seqs 1..=3, max = 3"
            );
            assert_eq!(writer.latest_seq(), seq1);

            // Each logical record's own Log[seq] entry exists, strictly
            // increasing, 1..=3 — not just one Log entry for the whole batch.
            for expected_seq in 1..=3u64 {
                let raw = writer
                    .storage()
                    .get(log_key(expected_seq))
                    .await
                    .unwrap()
                    .unwrap();
                let change = ChangeRecord::decode(&raw).unwrap();
                assert_eq!(change.seq, expected_seq);
            }

            // Batch 2: the edge phase — 4 logical records, one physical
            // commit, seqs must continue strictly past batch 1's max.
            let seq2 = writer.ingest_batch(&edges).await.unwrap();
            assert_eq!(
                seq2, 7,
                "4 more logical Edge records should consume seqs 4..=7, max = 7"
            );
            assert!(seq2 > seq1, "batch 2's max seq must exceed batch 1's max");
            for expected_seq in 4..=7u64 {
                let raw = writer
                    .storage()
                    .get(log_key(expected_seq))
                    .await
                    .unwrap()
                    .unwrap();
                let change = ChangeRecord::decode(&raw).unwrap();
                assert_eq!(change.seq, expected_seq);
            }

            // A plain per-record commit after two batches keeps the same
            // strictly-monotonic lineage.
            let seq3 = writer
                .upsert_edge(b"user:carol", "knows", b"user:alice")
                .await
                .unwrap();
            assert!(seq3 > seq2);

            (seq1, seq2, seq3)
        }; // writer dropped — simulates a restart.

        let recovered = Writer::from_storage(storage.clone()).await.unwrap();
        assert_eq!(
            recovered.latest_seq(),
            seq_batch3,
            "reopening after a batched load must recover latest_seq exactly"
        );
        assert!(seq_batch1 < seq_batch2 && seq_batch2 < seq_batch3);

        // And every xid/uid/node state from the batched load is intact
        // post-recovery too (not just the seq counter).
        assert!(recovered.lookup_uid(b"user:alice").await.unwrap().is_some());
        assert!(recovered.lookup_uid(b"user:carol").await.unwrap().is_some());
        let alice = recovered.lookup_uid(b"user:alice").await.unwrap().unwrap();
        let node = recovered.get_node(alice).await.unwrap().unwrap();
        assert_eq!(
            node.labels.len(),
            2,
            "Person + Admin from the batched node phase"
        );
    }

    #[tokio::test]
    async fn should_leave_no_partial_state_when_an_ingest_batch_fails_to_apply() {
        use common::StorageError;

        let raw: std::sync::Arc<dyn Storage> =
            std::sync::Arc::new(InMemoryStorage::with_merge_operator(std::sync::Arc::new(
                crate::merge::GraphMergeOperator,
            )));
        let failing = FailingStorage::wrap(raw.clone());

        let mut writer = Writer::from_storage(GraphStorage::from_storage(failing.clone()))
            .await
            .unwrap();

        let (nodes, edges) = sample_ingest_records();
        failing.fail_apply_once(StorageError::Storage("simulated crash mid-batch".into()));
        let result = writer.ingest_batch(&nodes).await;
        assert!(
            result.is_err(),
            "the injected failure must surface as an error"
        );

        // Nothing from the failed batch is visible — no xid mapping, no
        // node, no seq consumed.
        assert_eq!(writer.lookup_uid(b"user:alice").await.unwrap(), None);
        assert_eq!(writer.latest_seq(), 0);

        // A fresh writer over the raw (un-wrapped) storage confirms there is
        // truly nothing to recover — the batch committed zero of its ops.
        let recovered = Writer::from_storage(GraphStorage::from_storage(raw.clone()))
            .await
            .unwrap();
        assert_eq!(recovered.latest_seq(), 0);
        assert_eq!(recovered.lookup_uid(b"user:alice").await.unwrap(), None);

        // And a subsequent successful batch on the original writer (the
        // transient fault has "healed") proceeds cleanly. Like the
        // per-record `commit` precedent (`should_leave_no_partial_state_...`
        // above), the seq allocator's in-memory cursor already advanced past
        // the failed attempt's seqs even though nothing committed — so this
        // is `> 0`, not necessarily `== 3` (no reuse, but a gap is fine).
        let seq = writer.ingest_batch(&nodes).await.unwrap();
        assert!(seq > 0, "batch must succeed and consume fresh seqs");
        assert!(writer.lookup_uid(b"user:alice").await.unwrap().is_some());
        let _ = edges; // reserved for symmetry with the other test's dataset
    }
}
