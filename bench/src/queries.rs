//! Hand-planned executors for the LDBC SNB Interactive Complex Read queries
//! this bench covers (IC02 / IC07 / IC08 / IC09) plus two synthetic
//! deeper-hop extensions (IC3H / IC4H). Pattern-matched from the NamiDB
//! reference harness (`namidb-bench/src/queries.rs`), which renders the same
//! shapes as Cypher text for its own query engine — turbolay has no query
//! engine yet (M3/RFC 0008), so each query here is a direct
//! BFS-over-`posting_ops::neighbors` translation of the same MATCH pattern,
//! materializing rows by hand instead of parsing/planning/executing Cypher.
//!
//! Cypher shapes (see `bench/py/falkordb_runner.py::cypher_for`, which this
//! must stay row-shape-compatible with):
//!
//! - ic02: `(p)-[:KNOWS]->(friend)<-[:HAS_CREATOR]-(message:Post)`
//! - ic07: `(p)<-[:HAS_CREATOR]-(message:Post)<-[liker:LIKES]-(fan)`
//! - ic08: `(p)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF]-(reply:Comment)`
//! - ic09: `(p)-[:KNOWS]->(friend)-[:KNOWS]->(fof)<-[:HAS_CREATOR]-(msg:Post)`
//! - ic3h: `(p)-[:KNOWS]->(f1)-[:KNOWS]->(f2)-[:KNOWS]->(f3)<-[:HAS_CREATOR]-(msg:Post)`
//! - ic4h: `(p)-[:KNOWS]->(f1)-...->(f4)<-[:HAS_CREATOR]-(msg:Post)`
//!
//! ## Edge direction bookkeeping
//!
//! `dataset.rs`/`loader.rs` always write edges `src -> dst` as:
//! - `KNOWS`: Person -> Person
//! - `HAS_CREATOR`: Post/Comment -> Person (the message is `src`, its author
//!   is `dst`)
//! - `LIKES`: Person -> Post/Comment (the fan is `src`, the message is `dst`)
//! - `REPLY_OF`: Comment -> Post|Comment (the reply is `src`, the parent is
//!   `dst`)
//!
//! So "messages authored by person X" / "likers of message M" / "replies to
//! post P" are all *reverse* traversals from the Cypher-pattern's own
//! anchor — `Direction::In` from X/M/P respectively — because in every one of
//! these predicates the thing we're looking *up* from is the edge's `dst`,
//! not its `src`. `KNOWS` is the one predicate walked forward
//! (`Direction::Out`), matching how it's loaded (`p -> friend`).
//!
//! ## Determinism vs. real Cypher path semantics
//!
//! Each hop's frontier is a [`RoaringTreemap`] **set** (dedup is automatic,
//! unlike a Cypher engine's per-path row enumeration, which can revisit the
//! same node via multiple distinct paths and emit a row per path). This bench
//! intentionally trades exact path-count parity for a simple, fast,
//! deterministically-sortable BFS — the row *shape* and the top-20
//! `ORDER BY ... DESC` ranking match FalkorDB's, but exact row *counts* on
//! graphs with multiple paths to the same node may not match a raw (non-
//! `DISTINCT`) Cypher `MATCH`. Because of this, [`execute`]'s `LIMIT
//! 20`-truncated rows (the timed `run` path) are *not* directly diffable
//! against FalkorDB row-for-row — but the **DISTINCT** result set is:
//! turbolay's per-hop set dedup and Cypher's `RETURN DISTINCT` both collapse
//! multiple paths to the same node down to one row, so [`execute_distinct`]
//! (no `LIMIT`, deduped by the full returned column tuple) is the
//! apples-to-apples comparison. That's what `Cmd::Verify` /
//! `bench/py/verify_diff.py` diff — see `Cmd::Verify`'s doc in `main.rs`.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Context, Result};
use roaring::RoaringTreemap;
use serde_json::{Value, json};
use turbolay::value::{NodeRecord, TypedValue};
use turbolay::write::Writer;
use turbolay::{Direction, LabelId, PredId, PropId, SchemaKind, Uid, posting_ops};

/// Batch-fetch and decode the nodes for `uids` into a lookup map (present
/// nodes only). One [`Writer::get_nodes`] fan-out instead of a serial
/// `get_node` per uid, and the `BTreeSet` input dedups so a node reached
/// several ways in one query is fetched+decoded **once** (the intra-query
/// portion of the "no cache / re-decode" cost — R4/R5).
async fn fetch_node_map(writer: &Writer, uids: &BTreeSet<u64>) -> Result<HashMap<u64, NodeRecord>> {
    let ordered: Vec<Uid> = uids.iter().map(|u| Uid(*u)).collect();
    let nodes = writer.get_nodes_cached(&ordered).await?;
    Ok(ordered
        .iter()
        .zip(nodes)
        .filter_map(|(u, n)| n.map(|node| (u.get(), node)))
        .collect())
}

/// One of the LDBC SNB Complex Read queries this bench supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Query {
    /// IC2 — recent messages by friends of a given Person (1-hop KNOWS).
    Ic02,
    /// IC7 — recent likers of any of a Person's messages.
    Ic07,
    /// IC8 — recent replies to any of a Person's messages.
    Ic08,
    /// IC9 — recent messages by friends-of-friends (2-hop KNOWS).
    Ic09,
    /// IC3H — synthetic 3-hop KNOWS traversal.
    Ic3h,
    /// IC4H — synthetic 4-hop KNOWS traversal.
    Ic4h,
}

impl Query {
    pub fn name(self) -> &'static str {
        match self {
            Query::Ic02 => "ic02",
            Query::Ic07 => "ic07",
            Query::Ic08 => "ic08",
            Query::Ic09 => "ic09",
            Query::Ic3h => "ic3h",
            Query::Ic4h => "ic4h",
        }
    }

    /// Number of outgoing `KNOWS` hops the `Ic02`/`Ic09`/`Ic3h`/`Ic4h` family
    /// walks before the final `HAS_CREATOR` hop. `Ic07`/`Ic08` don't use
    /// this (they have their own, differently-shaped executors).
    fn knows_hops(self) -> Option<usize> {
        match self {
            Query::Ic02 => Some(1),
            Query::Ic09 => Some(2),
            Query::Ic3h => Some(3),
            Query::Ic4h => Some(4),
            Query::Ic07 | Query::Ic08 => None,
        }
    }

    /// The query's natural KNOWS-prefix depth when no explicit `--hops` sweep
    /// value is given: ic02=1, ic09=2, ic3h=3, ic4h=4, and ic07/ic08=0 (their
    /// tail applies to the anchor itself). The **single source of truth** for
    /// the effective-hops label both the `run` timing path and the `verify`
    /// dump emit, so their `(query, hops)` keys can never drift apart.
    pub fn natural_hops(self) -> usize {
        self.knows_hops().unwrap_or(0)
    }
}

/// One output row: column values in RETURN-clause order (see each query's
/// `execute` arm below for the exact column list). Serializes as a bare JSON
/// array (`#[serde(transparent)]`) so `Cmd::Verify`'s `rows: [[col, ...],
/// ...]` dump matches the shape `bench/py/falkordb_runner.py` would produce
/// for the same rows.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(transparent)]
pub struct Row(pub Vec<Value>);

/// Predicate/property ids resolved once per loaded `Writer` and reused
/// across every query call — schema interning already happened during load,
/// so this is a pure in-memory cache lookup (`Writer::schema_id`), not I/O.
pub struct Schema {
    pub knows: PredId,
    pub has_creator: PredId,
    pub likes: PredId,
    pub reply_of: PredId,
    pub first_name: PropId,
    pub last_name: PropId,
    pub content: PropId,
    pub creation_date: PropId,
    /// `:Post` label id — used to filter `HAS_CREATOR`-reachable nodes down
    /// to messages the Cypher `message:Post`/`post:Post` label constraint
    /// would keep (`HAS_CREATOR` alone is authored-by-Person for both Posts
    /// *and* Comments, so without this filter turbolay over-matches).
    pub post: LabelId,
    /// `:Comment` label id — used to filter IC08's `reply` node the same way
    /// (`reply:Comment` in the Cypher).
    pub comment: LabelId,
}

impl Schema {
    pub fn resolve(writer: &Writer) -> Result<Self> {
        Ok(Self {
            knows: pred(writer, "KNOWS")?,
            has_creator: pred(writer, "HAS_CREATOR")?,
            likes: pred(writer, "LIKES")?,
            reply_of: pred(writer, "REPLY_OF")?,
            first_name: prop(writer, "firstName")?,
            last_name: prop(writer, "lastName")?,
            content: prop(writer, "content")?,
            creation_date: prop(writer, "creationDate")?,
            post: label(writer, "Post")?,
            comment: label(writer, "Comment")?,
        })
    }
}

fn pred(writer: &Writer, name: &str) -> Result<PredId> {
    writer
        .schema_id(SchemaKind::Predicate, name)
        .map(PredId)
        .with_context(|| format!("predicate {name:?} was never interned — load the dataset first"))
}

fn prop(writer: &Writer, name: &str) -> Result<PropId> {
    writer
        .schema_id(SchemaKind::PropertyKey, name)
        .map(PropId)
        .with_context(|| format!("property {name:?} was never interned — load the dataset first"))
}

fn label(writer: &Writer, name: &str) -> Result<LabelId> {
    writer
        .schema_id(SchemaKind::Label, name)
        .map(LabelId)
        .with_context(|| format!("label {name:?} was never interned — load the dataset first"))
}

/// Controls how [`finish_rows`] turns a scored candidate list into the final
/// row list — the two bench modes need genuinely different semantics, not
/// just a different truncation point (see each variant's doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowLimit {
    /// The timed `run`/`Cmd::Run` path: LDBC's `ORDER BY ... DESC LIMIT 20`,
    /// unchanged from before the distinct-set oracle existed — ranked and
    /// truncated, no value-level dedup pass.
    Top20,
    /// The correctness oracle (`Cmd::Verify` / [`execute_distinct`]): no
    /// `LIMIT`, and collapsed to the `RETURN DISTINCT`-equivalent row set
    /// (deduped by the full returned column tuple) so it's apples-to-apples
    /// with FalkorDB's `RETURN DISTINCT ...` (no `LIMIT`) — see the module
    /// doc's note on per-path row enumeration vs. turbolay's per-hop node
    /// dedup.
    Distinct,
}

/// Runs `query` for the Person identified by `person_id_hex` (the dataset's
/// 32-hex-char external id, used directly as the anchor's xid). Returns an
/// empty result (no error) if the anchor id doesn't resolve to a uid —
/// mirrors a Cypher `MATCH` whose anchor predicate matches nothing.
///
/// This is the timed `run` path: `ORDER BY ... DESC LIMIT 20`, exactly as
/// before the distinct-set verify oracle was added. For the full,
/// un-truncated distinct row set (used by `Cmd::Verify`), see
/// [`execute_distinct`].
pub async fn execute(
    writer: &Writer,
    schema: &Schema,
    query: Query,
    person_id_hex: &str,
) -> Result<Vec<Row>> {
    execute_mode(writer, schema, query, person_id_hex, RowLimit::Top20, None).await
}

/// Like [`execute`], but with an explicit KNOWS-prefix hop count that
/// overrides the query's natural hop depth — the bench hop-sweep entrypoint.
///
/// Every query is generalized to "walk `hops` outgoing `KNOWS` hops from the
/// anchor to build a person frontier, then apply the query's tail to that
/// frontier": messages-authored (ic02/ic09), likers-of-messages (ic07), or
/// replies-to-posts (ic08). `hops = 0` recovers each query's original
/// anchor-only behavior; `hops = 1` is the anchor's direct friends, etc.
/// `None` falls back to the query's natural hop (`Query::knows_hops`, i.e.
/// ic02=1/ic09=2/ic3h=3/ic4h=4, ic07/ic08=0).
pub async fn execute_with_hops(
    writer: &Writer,
    schema: &Schema,
    query: Query,
    person_id_hex: &str,
    hops: Option<usize>,
) -> Result<Vec<Row>> {
    execute_mode(writer, schema, query, person_id_hex, RowLimit::Top20, hops).await
}

/// Like [`execute`], but returns every DISTINCT row (deduped by the full
/// column tuple, no `LIMIT`) — the correctness-oracle counterpart to
/// `execute`'s timed-bench `LIMIT 20`. `Cmd::Verify` uses this so its dump
/// can be diffed against FalkorDB's `RETURN DISTINCT ...` (no `LIMIT`)
/// output straight across, rather than comparing two different
/// top-20-under-possibly-different-tie-breaking prefixes.
pub async fn execute_distinct(
    writer: &Writer,
    schema: &Schema,
    query: Query,
    person_id_hex: &str,
) -> Result<Vec<Row>> {
    execute_mode(writer, schema, query, person_id_hex, RowLimit::Distinct, None).await
}

/// Hop-aware [`execute_distinct`] — the correctness oracle for the hop sweep.
/// Same DISTINCT/no-LIMIT semantics, but with an explicit KNOWS-prefix depth
/// so `verify` can dump the full row set at each swept hop.
pub async fn execute_distinct_with_hops(
    writer: &Writer,
    schema: &Schema,
    query: Query,
    person_id_hex: &str,
    hops: Option<usize>,
) -> Result<Vec<Row>> {
    execute_mode(writer, schema, query, person_id_hex, RowLimit::Distinct, hops).await
}

async fn execute_mode(
    writer: &Writer,
    schema: &Schema,
    query: Query,
    person_id_hex: &str,
    mode: RowLimit,
    hops_override: Option<usize>,
) -> Result<Vec<Row>> {
    // Loaded once per query call (not per frontier node) — the deleted-nodes
    // bitmap is a single `Meta` key, and `posting_ops::neighbors` wants it
    // passed in rather than re-reading it per anchor.
    let deleted_nodes = load_deleted_nodes(writer).await?;

    let Some(anchor) = writer.lookup_uid(person_id_hex.as_bytes()).await? else {
        return Ok(Vec::new());
    };

    // Effective KNOWS-prefix depth: an explicit sweep override wins; otherwise
    // the query's natural hop (ic02=1/ic09=2/ic3h=3/ic4h=4, ic07/ic08=0).
    let hops = hops_override.unwrap_or_else(|| query.knows_hops().unwrap_or(0));

    // Walk `hops` outgoing KNOWS expansions to build the person frontier every
    // query's tail then operates over (hops=0 → just the anchor itself).
    let frontier = knows_frontier(writer, schema, anchor, &deleted_nodes, hops).await?;

    match query {
        Query::Ic02 | Query::Ic09 | Query::Ic3h | Query::Ic4h => {
            messages_by_frontier(writer, schema, &frontier, &deleted_nodes, mode).await
        }
        Query::Ic07 => ic07(writer, schema, &frontier, &deleted_nodes, mode).await,
        Query::Ic08 => ic08(writer, schema, &frontier, &deleted_nodes, mode).await,
    }
}

/// Walks `hops` outgoing `KNOWS` expansions from `anchor`, returning the
/// resulting person frontier (deduped as a `RoaringTreemap`). `hops = 0`
/// returns the singleton `{anchor}`.
async fn knows_frontier(
    writer: &Writer,
    schema: &Schema,
    anchor: Uid,
    deleted_nodes: &RoaringTreemap,
    hops: usize,
) -> Result<RoaringTreemap> {
    let mut frontier = RoaringTreemap::new();
    frontier.insert(anchor.get());
    for _ in 0..hops {
        frontier =
            expand_frontier(writer, &frontier, schema.knows, Direction::Out, deleted_nodes).await?;
    }
    Ok(frontier)
}

async fn load_deleted_nodes(writer: &Writer) -> Result<RoaringTreemap> {
    match writer
        .storage()
        .get(posting_ops::deleted_nodes_key())
        .await?
    {
        Some(bytes) => {
            RoaringTreemap::deserialize_from(bytes.as_ref()).context("corrupt deleted_nodes bitmap")
        }
        None => Ok(RoaringTreemap::new()),
    }
}

/// Expands every uid in `frontier` by one `(pred, dir)` hop and unions the
/// results — the frontier itself is a `RoaringTreemap`, so cross-node dedup
/// within a single hop is automatic (see the module doc's determinism note).
///
/// Uses [`posting_ops::neighbors_batch`] so the frontier's per-node adjacency
/// reads run concurrently (bounded), not one serial `await` per node — the
/// N+1 fix (RFC 0007 §8.1). The unioned result is identical to the old serial
/// loop; only the I/O scheduling changed.
async fn expand_frontier(
    writer: &Writer,
    frontier: &RoaringTreemap,
    pred: PredId,
    dir: Direction,
    deleted_nodes: &RoaringTreemap,
) -> Result<RoaringTreemap> {
    Ok(writer
        .neighbors_batch_cached(frontier.iter().map(Uid), pred, dir, deleted_nodes)
        .await?)
}

fn string_prop(props: &BTreeMap<PropId, TypedValue>, id: PropId) -> String {
    match props.get(&id) {
        Some(TypedValue::String(s)) => s.clone(),
        _ => String::new(),
    }
}

fn int_prop(props: &BTreeMap<PropId, TypedValue>, id: PropId) -> i64 {
    match props.get(&id) {
        Some(TypedValue::Int(v)) => *v,
        _ => 0,
    }
}

/// IC02/IC09/IC3H/IC4H tail: every message whose `HAS_CREATOR` points at a
/// node in `frontier` (the person set the KNOWS-prefix walk produced).
/// `HAS_CREATOR` is stored `(Post)-[HAS_CREATOR]->(Person)`, so "messages
/// created by X" is X's *incoming* `HAS_CREATOR` neighbors (`Direction::In`).
///
/// Returns `[creator.firstName, creator.lastName, message.content,
/// message.creationDate]` rows, sorted by `creationDate` DESC then message
/// xid ASC (tie-break), capped at LDBC's `LIMIT 20`.
async fn messages_by_frontier(
    writer: &Writer,
    schema: &Schema,
    frontier: &RoaringTreemap,
    deleted_nodes: &RoaringTreemap,
    mode: RowLimit,
) -> Result<Vec<Row>> {
    // 1. Every frontier person's authored messages, concurrently, keeping the
    //    person→messages binding (creator name is a returned column).
    let persons: Vec<Uid> = frontier.iter().map(Uid).collect();
    let per_person = writer
        .neighbors_each_cached(
            persons.iter().copied(),
            schema.has_creator,
            Direction::In,
            deleted_nodes,
        )
        .await?;

    // 2. One batched node fetch for every person + every message they created.
    let mut want: BTreeSet<u64> = frontier.iter().collect();
    for (_, msgs) in &per_person {
        want.extend(msgs.iter());
    }
    let nodes = fetch_node_map(writer, &want).await?;

    // 3. Build rows in memory — no further I/O.
    let mut scored: Vec<(i64, String, Row)> = Vec::new();
    for (person_uid, messages) in &per_person {
        let Some(person_node) = nodes.get(&person_uid.get()) else {
            continue;
        };
        let first_name = string_prop(&person_node.props, schema.first_name);
        let last_name = string_prop(&person_node.props, schema.last_name);

        for raw_msg in messages.iter() {
            let Some(msg_node) = nodes.get(&raw_msg) else {
                continue;
            };
            // `HAS_CREATOR` is authored-by-Person for both Posts and
            // Comments; the Cypher pattern's `message:Post` label
            // constraint means Comments authored by the frontier person
            // must be excluded here too.
            if !msg_node.labels.contains(&schema.post) {
                continue;
            }
            let content = string_prop(&msg_node.props, schema.content);
            let creation_date = int_prop(&msg_node.props, schema.creation_date);
            scored.push((
                creation_date,
                msg_node.xid.clone(),
                Row(vec![
                    json!(first_name),
                    json!(last_name),
                    json!(content),
                    json!(creation_date),
                ]),
            ));
        }
    }
    Ok(finish_rows(scored, mode))
}

/// IC07: for every author in `frontier` (the KNOWS-prefix person set —
/// `{anchor}` at hops=0, matching the original IC07), the messages that
/// author created (their incoming `HAS_CREATOR` neighbors), then every Person
/// who `LIKES` one of those messages (`LIKES` is stored
/// `(Person)-[LIKES]->(Post|Comment)`, so "likers of message M" is M's
/// *incoming* `LIKES` neighbors). Returns `[fan.firstName, fan.lastName,
/// likeCreationDate, message.content]` rows — `likeCreationDate` is the
/// `LIKES` edge's own `creationDate` prop (`Writer::edge_props` per `(fan,
/// message)` pair), not a node property. Sorted by `likeCreationDate` DESC
/// then fan xid ASC, capped at 20.
async fn ic07(
    writer: &Writer,
    schema: &Schema,
    frontier: &RoaringTreemap,
    deleted_nodes: &RoaringTreemap,
    mode: RowLimit,
) -> Result<Vec<Row>> {
    // 1. Messages authored by the frontier (each message has one creator, so
    //    the union across authors is already distinct).
    let authors: Vec<Uid> = frontier.iter().map(Uid).collect();
    let per_author = writer
        .neighbors_each_cached(authors, schema.has_creator, Direction::In, deleted_nodes)
        .await?;
    let mut msg_set: BTreeSet<u64> = BTreeSet::new();
    for (_, msgs) in &per_author {
        msg_set.extend(msgs.iter());
    }

    // 2. Fetch message nodes (batched) to keep only `:Post` and read content.
    let msg_nodes = fetch_node_map(writer, &msg_set).await?;
    let post_msgs: Vec<Uid> = msg_set
        .iter()
        .filter(|m| {
            msg_nodes
                .get(m)
                .is_some_and(|n| n.labels.contains(&schema.post))
        })
        .map(|m| Uid(*m))
        .collect();

    // 3. Likers of each post message, concurrently, keeping the message→fans
    //    binding (message content + the per-pair LIKES edge prop).
    let per_msg_fans = writer
        .neighbors_each_cached(post_msgs, schema.likes, Direction::In, deleted_nodes)
        .await?;

    // 4. Batched fan-node fetch + batched per-(fan,message) edge-prop fetch.
    let mut fan_set: BTreeSet<u64> = BTreeSet::new();
    let mut edge_keys: Vec<(Uid, PredId, Uid)> = Vec::new();
    for (msg, fans) in &per_msg_fans {
        for raw_fan in fans.iter() {
            fan_set.insert(raw_fan);
            edge_keys.push((Uid(raw_fan), schema.likes, *msg));
        }
    }
    let fan_nodes = fetch_node_map(writer, &fan_set).await?;
    let edge_props = writer.edge_props_batch(&edge_keys).await?;

    // 5. Build rows in memory. `ei` walks `edge_props` in the exact order
    //    `edge_keys` was built (one slot per fan, advanced unconditionally).
    let mut scored: Vec<(i64, String, Row)> = Vec::new();
    let mut ei = 0usize;
    for (msg, fans) in &per_msg_fans {
        let content = msg_nodes
            .get(&msg.get())
            .map(|n| string_prop(&n.props, schema.content))
            .unwrap_or_default();
        for raw_fan in fans.iter() {
            let props = &edge_props[ei];
            ei += 1;
            let Some(fan_node) = fan_nodes.get(&raw_fan) else {
                continue;
            };
            let first_name = string_prop(&fan_node.props, schema.first_name);
            let last_name = string_prop(&fan_node.props, schema.last_name);
            let like_creation_date = props
                .as_ref()
                .and_then(|p| p.get(&schema.creation_date).cloned())
                .map(|v| match v {
                    TypedValue::Int(i) => i,
                    _ => 0,
                })
                .unwrap_or(0);
            scored.push((
                like_creation_date,
                fan_node.xid.clone(),
                Row(vec![
                    json!(first_name),
                    json!(last_name),
                    json!(like_creation_date),
                    json!(content),
                ]),
            ));
        }
    }
    Ok(finish_rows(scored, mode))
}

/// IC08: for every author in `frontier` (the KNOWS-prefix person set —
/// `{anchor}` at hops=0, matching the original IC08), the Posts that author
/// created, then every Comment `REPLY_OF` one of those posts (`REPLY_OF` is
/// stored `(Comment)-[REPLY_OF]->(Post|Comment)`, so "replies to post P" is
/// P's *incoming* `REPLY_OF` neighbors). Returns `[reply.content,
/// reply.creationDate, post.content]` rows, sorted by `replyDate` DESC then
/// reply xid ASC, capped at 20.
async fn ic08(
    writer: &Writer,
    schema: &Schema,
    frontier: &RoaringTreemap,
    deleted_nodes: &RoaringTreemap,
    mode: RowLimit,
) -> Result<Vec<Row>> {
    // 1. Posts authored by the frontier (each has one creator → already distinct).
    let authors: Vec<Uid> = frontier.iter().map(Uid).collect();
    let per_author = writer
        .neighbors_each_cached(authors, schema.has_creator, Direction::In, deleted_nodes)
        .await?;
    let mut post_set: BTreeSet<u64> = BTreeSet::new();
    for (_, posts) in &per_author {
        post_set.extend(posts.iter());
    }

    // 2. Fetch message nodes (batched); keep only `:Post` and read content.
    let post_nodes = fetch_node_map(writer, &post_set).await?;
    let posts: Vec<Uid> = post_set
        .iter()
        .filter(|p| {
            post_nodes
                .get(p)
                .is_some_and(|n| n.labels.contains(&schema.post))
        })
        .map(|p| Uid(*p))
        .collect();

    // 3. Replies to each post, concurrently, keeping the post→replies binding
    //    (post content is a returned column).
    let per_post_replies = writer
        .neighbors_each_cached(posts, schema.reply_of, Direction::In, deleted_nodes)
        .await?;

    // 4. Batched reply-node fetch.
    let mut reply_set: BTreeSet<u64> = BTreeSet::new();
    for (_, replies) in &per_post_replies {
        reply_set.extend(replies.iter());
    }
    let reply_nodes = fetch_node_map(writer, &reply_set).await?;

    // 5. Build rows in memory.
    let mut scored: Vec<(i64, String, Row)> = Vec::new();
    for (post, replies) in &per_post_replies {
        let post_content = post_nodes
            .get(&post.get())
            .map(|n| string_prop(&n.props, schema.content))
            .unwrap_or_default();
        for raw_reply in replies.iter() {
            let Some(reply_node) = reply_nodes.get(&raw_reply) else {
                continue;
            };
            // `reply:Comment` in the Cypher pattern — `REPLY_OF` can also point
            // Comment->Comment (a reply to a reply), so exclude those here too
            // (only direct Post replies survive the filter above, but the inner
            // reply itself must be a Comment).
            if !reply_node.labels.contains(&schema.comment) {
                continue;
            }
            let reply_content = string_prop(&reply_node.props, schema.content);
            let reply_date = int_prop(&reply_node.props, schema.creation_date);
            scored.push((
                reply_date,
                reply_node.xid.clone(),
                Row(vec![
                    json!(reply_content),
                    json!(reply_date),
                    json!(post_content),
                ]),
            ));
        }
    }
    Ok(finish_rows(scored, mode))
}

/// Shared `ORDER BY <sort key> DESC` (then the tie-break id ASC — the
/// underlying node's xid, used only to make the sort total, not itself a
/// returned column). Behavior then forks on `mode`:
///
/// - [`RowLimit::Top20`]: truncate to LDBC's `LIMIT 20`, exactly as before
///   the distinct-set oracle existed — no value-level dedup pass (matches
///   `run`'s pre-existing, unchanged behavior).
/// - [`RowLimit::Distinct`]: no truncation; collapse to one row per distinct
///   column tuple. Rows with identical returned columns always sort
///   adjacent here — the sort key itself (`creationDate`/`likeCreationDate`/
///   `replyDate`) is one of the returned columns, so two rows can only have
///   equal returned-column tuples if they also have equal sort keys — so a
///   single consecutive-dedup pass over the sorted list is a correct full
///   dedup, no hashing/re-sorting needed.
fn finish_rows(mut scored: Vec<(i64, String, Row)>, mode: RowLimit) -> Vec<Row> {
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    match mode {
        RowLimit::Top20 => {
            scored.truncate(20);
            scored.into_iter().map(|(_, _, row)| row).collect()
        }
        RowLimit::Distinct => {
            let mut rows: Vec<Row> = Vec::with_capacity(scored.len());
            for (_, _, row) in scored {
                if rows.last().is_some_and(|prev: &Row| prev.0 == row.0) {
                    continue;
                }
                rows.push(row);
            }
            rows
        }
    }
}
