//! Graph reads from the adjacency tables `hydradb.graph_vertices` and
//! `hydradb.graph_adjacency`, which triggers keep current in the writing
//! transaction.
//!
//! A query loads only the vertices and edges it reaches, a frontier per
//! lookup, and keeps them for the rest of the query. Every lookup runs with
//! `read_only` SPI, so it reads the calling statement's snapshot, including the
//! transaction's own writes.
//!
//! A hop reads only adjacency rows, from the covering index. Vertex rows are
//! read when a query needs them: to order a layer that a limit cuts, so the
//! traversal keeps the same vertices and edges a full load keeps, to check the
//! vertices it reached exist, and to name them in the result. Each vertex's
//! order key encodes `(sort_key, label, key)`, which compares as a full load's
//! numbering does, and edge ids follow `(label, key)` among each vertex pair's
//! parallel edges, so the vertices and edges kept at a limit are the same.

use std::collections::{BTreeSet, HashMap, HashSet};

use hydradb_pg_graph_core::{Direction, Edge, Expanded, GraphError, GraphRead, GraphSnapshot, Vertex};
use hydradb_pg_graph_csr::{CsrGraph, Incident};
use hydradb_pg_graph_query::{PathConfig, DEFAULT_PATH_LEN, MAX_PATH_LEN};
use pgrx::{pg_sys, IntoDatum};
use serde_json::{json, Value};

use crate::keys::{parse_seeds, Key, Label, Seed};
use crate::spi::{select, Row, LOOKUP_BATCH};
use crate::storage::{EDGE_ROW_BYTES, INTEGER_BYTES, MAX_SNAPSHOT_TEXT_BYTES, VERTEX_ROW_BYTES};

// Sort keys and kinds are in no index, so nothing bounds their size: each
// lookup returns one only up to the 65536-byte field limit
// (MAX_SNAPSHOT_TEXT_BYTES), with its length, so a longer one is refused by its
// length without being read.
const SEED_SQL: &str = "SELECT key, vid, rows, CASE WHEN octet_length(sort_key) <= 65536 THEN sort_key END,
    octet_length(sort_key) FROM hydradb.graph_vertices WHERE graph = $1 AND label = $2 AND key = ANY($3)";
const DETAILS_SQL: &str = "SELECT vid, key, rows, CASE WHEN octet_length(sort_key) <= 65536 THEN sort_key END,
    octet_length(sort_key) FROM hydradb.graph_vertices WHERE graph = $1 AND vid = ANY($2)";
/// A vertex's edges in one direction. Without a kind column the lookup reads
/// only the covering index; kinds come from the table.
const OUTGOING_SQL: &str = "SELECT source, label, key, target, NULL::text, NULL::integer FROM hydradb.graph_adjacency
    WHERE graph = $1 AND source = ANY($2)";
const INCOMING_SQL: &str = "SELECT target, label, key, source, NULL::text, NULL::integer FROM hydradb.graph_adjacency
    WHERE graph = $1 AND target = ANY($2)";
const OUTGOING_KIND_SQL: &str = "SELECT source, label, key, target, CASE WHEN octet_length(kind) <= 65536 THEN kind END,
    octet_length(kind) FROM hydradb.graph_adjacency WHERE graph = $1 AND source = ANY($2)";
const INCOMING_KIND_SQL: &str = "SELECT target, label, key, source, CASE WHEN octet_length(kind) <= 65536 THEN kind END,
    octet_length(kind) FROM hydradb.graph_adjacency WHERE graph = $1 AND target = ANY($2)";

/// The most bytes a stored key takes: a text key over 2000 bytes is never
/// stored (hydradb.long_key_check), and an integer key takes eight.
const MAX_KEY_BYTES: usize = 2000;

/// Rows per fetch so that a batch of rows of at most `row_bytes` stays within
/// what the byte budget leaves, at most a lookup batch and at least one row.
fn rows_per_fetch(bytes_left: usize, row_bytes: usize) -> usize {
    (bytes_left / row_bytes).clamp(1, LOOKUP_BATCH)
}

/// A path search's `max_edge_visits` while its region loads. The search examines
/// every relationship a frontier lookup returns, so once `charged` plus the
/// rows returned passes `limit`, the search would stop at that limit too. A
/// self loop is returned by both lookups of a both-direction frontier and
/// examined once, so there the rows count half.
#[derive(Clone, Copy)]
struct WorkCap {
    charged: u64,
    limit: u64,
    both: bool,
    /// Rows the outgoing lookup of a both-direction frontier returned.
    earlier_rows: u64,
}

impl WorkCap {
    fn exceeded(&self, rows: u64) -> bool {
        let rows = if self.both { (self.earlier_rows + rows).div_ceil(2) } else { rows };
        self.charged + rows > self.limit
    }

    /// native_paths' error for its visit past the limit.
    fn error(&self) -> GraphError {
        GraphError::LimitExceeded {
            operation: "native_path_edges",
            actual: self.limit + 1,
            limit: self.limit,
        }
    }
}

/// The length of a text column a lookup returns beside it.
fn text_length(row: &Row, column: i32) -> usize {
    row.get::<i32>(column).map_or(0, |length| length.max(0) as usize)
}

/// A lookup argument as a datum. Integers and arrays always convert; an
/// error rather than a panic all the same.
fn datum(value: impl IntoDatum) -> Result<pg_sys::Datum, GraphError> {
    value
        .into_datum()
        .ok_or(GraphError::InconsistentGraph("a lookup argument has no datum"))
}

pub(crate) fn source_error(error: impl std::fmt::Display) -> GraphError {
    GraphError::Source(error.to_string())
}

const REGISTRATION_SQL: &str = "SELECT registration FROM hydradb.graphs WHERE name = $1 AND needs_refresh IS NULL";

/// The registration of graph `name` on the calling statement's snapshot, read
/// with a kept plan as the adjacency tables' owner; none while the graph needs
/// refresh_graph, which a write in another backend can set without
/// invalidating this one's cached description.
pub fn registration(name: &str, owner: pg_sys::Oid) -> Result<Option<i64>, GraphError> {
    let name = name.into_datum().ok_or_else(|| source_error("invalid graph name"))?;
    let mut found = None;
    select(REGISTRATION_SQL, owner, &[(pg_sys::TEXTOID, name)], 1, |row| {
        found = Some(row.require::<i64>(1)?);
        Ok(())
    })?;
    Ok(found)
}

/// Encodes `(sort_key, label, key)` as a string with the same order: the sort
/// key with NUL escaped and a NUL pair after it, the label as four hex digits,
/// then an integer key as sixteen hex digits with the sign bit flipped, or a
/// text key as is. Text keys order bytewise, as `Key` does.
fn order_key(sort_key: &str, label: u16, key: &Key) -> String {
    let mut encoded = sort_key.replace('\0', "\0\u{1}");
    encoded.push_str("\0\0");
    encoded.push_str(&format!("{label:04x}"));
    match key {
        Key::Int(n) => encoded.push_str(&format!("{:016x}", (*n as u64) ^ (1 << 63))),
        Key::Text(text) => encoded.push_str(text),
    }
    encoded
}

fn key_bytes(key: &Key) -> usize {
    match key {
        Key::Int(_) => INTEGER_BYTES,
        Key::Text(text) => text.len(),
    }
}

/// Fails for a value a full load would refuse as over the field limit.
fn field_limit(name: &str, bytes: usize) -> Result<usize, GraphError> {
    if bytes > MAX_SNAPSHOT_TEXT_BYTES {
        return Err(source_error(format!("snapshot {name} exceeds the 65536-byte field limit")));
    }
    Ok(bytes)
}

/// What a vertex row says, loaded when a query needs it: to order a layer a
/// limit cuts, to name the vertex in the result, or to check that it exists.
struct Details {
    key: Key,
    /// `(sort_key, label, key)` as `order_key` encodes it.
    order: String,
}

struct VertexEntry {
    slot: usize,
    label: u16,
    /// An edge that leads to the vertex, for the error a missing one raises.
    via: Option<i64>,
    details: Option<Details>,
}

/// The error an exceeded byte budget raises, with what was loaded, as a full
/// load reports it.
fn budget_exceeded(byte_limit: usize, vertices: usize, edges: usize) -> GraphError {
    crate::usage::fail("max_snapshot_bytes");
    source_error(format!(
        "snapshot byte budget exceeded: {byte_limit} bytes, after {vertices} vertices and {edges} edges"
    ))
}

/// Table-backed graph for one query.
pub struct AdjacencyGraph {
    graph: i64,
    owner: pg_sys::Oid,
    vertex_labels: Vec<Label>,
    edge_labels: Vec<Label>,
    /// Per edge table, the vertex tables of its source and target.
    edge_ends: Vec<(u16, u16)>,
    /// `max_snapshot_bytes`. A vertex is reached through a seed or a new
    /// edge, both charged, so it also bounds how many are loaded.
    byte_limit: usize,
    /// Loaded rows charged as a full load charges them.
    bytes: usize,
    vertices: Vec<Vertex>,
    vertex_entries: HashMap<i64, VertexEntry>,
    /// Edge id is the index.
    edges: Vec<Edge>,
    edge_keys: Vec<(u16, Key)>,
    edge_ids: HashMap<(u16, Key), i64>,
    /// Edge ids incident to a vertex, outgoing when the flag is set, in id order.
    lists: HashMap<(i64, bool), Vec<usize>>,
    /// Whether any edge table has a kind column.
    kind_column: bool,
    lookups: usize,
}

impl AdjacencyGraph {
    /// Labels are in label order; `kind_column` is whether any edge table has one.
    pub fn new(
        graph: i64,
        owner: pg_sys::Oid,
        vertex_labels: Vec<Label>,
        edge_labels: Vec<Label>,
        edge_ends: Vec<(u16, u16)>,
        byte_limit: usize,
        kind_column: bool,
    ) -> Self {
        Self {
            graph,
            owner,
            vertex_labels,
            edge_labels,
            edge_ends,
            byte_limit,
            bytes: 0,
            vertices: Vec::new(),
            vertex_entries: HashMap::new(),
            edges: Vec::new(),
            edge_keys: Vec::new(),
            edge_ids: HashMap::new(),
            lists: HashMap::new(),
            kind_column,
            lookups: 0,
        }
    }

    pub fn loaded_vertices(&self) -> usize {
        self.vertices.len()
    }

    pub fn loaded_edges(&self) -> usize {
        self.edges.len()
    }

    pub fn loaded_bytes(&self) -> usize {
        self.bytes
    }

    fn charge(&mut self, bytes: usize) -> Result<(), GraphError> {
        let total = self.bytes + bytes;
        if total > self.byte_limit {
            return Err(budget_exceeded(self.byte_limit, self.vertices.len(), self.edges.len()));
        }
        self.bytes = total;
        Ok(())
    }

    pub fn lookups(&self) -> usize {
        self.lookups
    }

    /// Records a vertex the first time an edge or seed reaches it, without its
    /// details.
    fn add_vertex(&mut self, vid: i64, label: u16, via: Option<i64>) -> Result<(), GraphError> {
        if self.vertex_entries.contains_key(&vid) {
            return Ok(());
        }
        let slot = self.vertices.len();
        self.vertices.push(Vertex {
            id: vid,
            sort_key: String::new(),
        });
        self.vertex_entries.insert(
            vid,
            VertexEntry {
                slot,
                label,
                via,
                details: None,
            },
        );
        Ok(())
    }

    /// Stores a vertex row's details. A key with no source row, or with
    /// several, is the error a full load reports for it.
    fn set_details(&mut self, vid: i64, key: Key, rows: i32, sort_key: Option<String>, sort_bytes: usize) -> Result<(), GraphError> {
        let (label, via) = self
            .vertex_entries
            .get(&vid)
            .map(|entry| (entry.label, entry.via))
            .ok_or(GraphError::InconsistentGraph("details for a vertex that was never reached"))?;
        let name = &self.vertex_labels[label as usize].name;
        if rows > 1 {
            return Err(source_error(format!("duplicate key {key} in vertex table {name}")));
        }
        if rows == 0 {
            let edge = via.map(|id| self.edge_keys[id as usize].clone());
            return Err(source_error(match edge {
                Some((label, edge_key)) => format!(
                    "edge {} {edge_key} refers to missing vertex {name} {key}",
                    self.edge_labels[label as usize].name
                ),
                None => format!("missing vertex {name} {key}"),
            }));
        }
        let key_size = field_limit("key", key_bytes(&key))?;
        let sort_size = field_limit("sort_key", sort_bytes)?;
        self.charge(VERTEX_ROW_BYTES + key_size + sort_size)?;
        let order = order_key(sort_key.as_deref().unwrap_or_default(), label, &key);
        if let Some(entry) = self.vertex_entries.get_mut(&vid) {
            entry.details = Some(Details { key, order });
        }
        Ok(())
    }

    /// Loads the details of these vertices that are not loaded yet, in one lookup.
    fn load_details(&mut self, ids: &[i64]) -> Result<(), GraphError> {
        let mut missing = ids
            .iter()
            .copied()
            .filter(|id| self.vertex_entries.get(id).is_some_and(|entry| entry.details.is_none()))
            .collect::<Vec<_>>();
        missing.sort_unstable();
        missing.dedup();
        if missing.is_empty() {
            return Ok(());
        }
        self.lookups += 1;
        let mut rows = Vec::with_capacity(missing.len());
        // Each row is charged as it arrives, as set_details charges it below.
        let (entries, labels) = (&self.vertex_entries, &self.vertex_labels);
        let bytes_left = self.byte_limit.saturating_sub(self.bytes);
        let (byte_limit, vertices, edges) = (self.byte_limit, self.vertices.len(), self.edges.len());
        let mut charged = 0usize;
        select(
            DETAILS_SQL,
            self.owner,
            &[(pg_sys::INT8OID, datum(self.graph)?), (pg_sys::INT8ARRAYOID, datum(missing)?)],
            rows_per_fetch(bytes_left, VERTEX_ROW_BYTES + MAX_KEY_BYTES + MAX_SNAPSHOT_TEXT_BYTES),
            |row| {
                let vid = row.require::<i64>(1)?;
                let label = entries
                    .get(&vid)
                    .ok_or(GraphError::InconsistentGraph("details for a vertex that was never reached"))?
                    .label;
                let key = Key::parse(labels[label as usize].kind, row.require::<String>(2)?)
                    .ok_or(GraphError::InconsistentGraph("vertex key does not parse"))?;
                let sort_bytes = text_length(row, 5);
                charged += VERTEX_ROW_BYTES + field_limit("key", key_bytes(&key))? + field_limit("sort_key", sort_bytes)?;
                if charged > bytes_left {
                    return Err(budget_exceeded(byte_limit, vertices, edges));
                }
                rows.push((vid, key, row.require::<i32>(3)?, row.get::<String>(4), sort_bytes));
                Ok(())
            },
        )?;
        for (vid, key, count, sort_key, sort_bytes) in rows {
            self.set_details(vid, key, count, sort_key, sort_bytes)?;
        }
        Ok(())
    }

    /// Checks every vertex the query reached and loads what naming them needs.
    /// A full load fails on a dangling endpoint or duplicate key anywhere; this
    /// fails on one the query reached.
    pub fn finish(&mut self) -> Result<(), GraphError> {
        let ids = self.vertex_entries.keys().copied().collect::<Vec<_>>();
        self.load_details(&ids)
    }

    /// The vertices the seeds name. A seed whose key has no source row names
    /// none.
    pub fn seeds(&mut self, seeds: &Value) -> Result<Vec<Seed>, GraphError> {
        let parsed = parse_seeds(&self.vertex_labels, seeds)?;
        let mut found = HashMap::new();
        for label in parsed.iter().map(|&(label, _)| label).collect::<BTreeSet<_>>() {
            let keys = parsed
                .iter()
                .filter(|&&(l, _)| l == label)
                .map(|(_, key)| key.text())
                .collect::<Vec<_>>();
            self.lookups += 1;
            let mut rows = Vec::new();
            // Seeds are charged as they arrive against the bytes the budget
            // leaves, as set_details charges them: only details not loaded
            // yet, so a source that is also a target is charged once.
            let (key_kind, entries) = (self.vertex_labels[label as usize].kind, &self.vertex_entries);
            let bytes_left = self.byte_limit.saturating_sub(self.bytes);
            let (byte_limit, vertices, edges) = (self.byte_limit, self.vertices.len(), self.edges.len());
            let mut charged = 0usize;
            select(
                SEED_SQL,
                self.owner,
                &[
                    (pg_sys::INT8OID, datum(self.graph)?),
                    (pg_sys::INT2OID, datum(label as i16)?),
                    (pg_sys::TEXTARRAYOID, datum(keys)?),
                ],
                rows_per_fetch(bytes_left, VERTEX_ROW_BYTES + MAX_KEY_BYTES + MAX_SNAPSHOT_TEXT_BYTES),
                |row| {
                    let key = Key::parse(key_kind, row.require::<String>(1)?)
                        .ok_or(GraphError::InconsistentGraph("vertex key does not parse"))?;
                    let (vid, count) = (row.require::<i64>(2)?, row.require::<i32>(3)?);
                    let sort_bytes = text_length(row, 5);
                    let loaded = entries.get(&vid);
                    if count > 0 && loaded.is_none_or(|entry| entry.details.is_none()) {
                        charged += VERTEX_ROW_BYTES + field_limit("key", key_bytes(&key))? + field_limit("sort_key", sort_bytes)?;
                        if charged > bytes_left {
                            return Err(budget_exceeded(byte_limit, vertices, edges));
                        }
                    }
                    rows.push((key, vid, count, row.get::<String>(4), sort_bytes));
                    Ok(())
                },
            )?;
            for (key, vid, count, sort_key, sort_bytes) in rows {
                if count > 0 {
                    self.add_vertex(vid, label, None)?;
                    if self.vertex_entries[&vid].details.is_none() {
                        self.set_details(vid, key.clone(), count, sort_key, sort_bytes)?;
                    }
                    found.insert((label, key), vid);
                }
            }
        }
        Ok(parsed
            .into_iter()
            .map(|(label, key)| Seed {
                id: found.get(&(label, key.clone())).copied(),
                label: self.vertex_labels[label as usize].name.clone(),
                key,
            })
            .collect())
    }

    /// Loads the edges of `ids` in one direction that are not loaded yet, from
    /// the adjacency rows alone.
    fn load(&mut self, ids: &[i64], outgoing: bool, cap: Option<WorkCap>) -> Result<u64, GraphError> {
        let mut missing = ids
            .iter()
            .copied()
            .filter(|&id| !self.lists.contains_key(&(id, outgoing)))
            .collect::<Vec<_>>();
        missing.sort_unstable();
        missing.dedup();
        if missing.is_empty() {
            return Ok(0);
        }
        struct Fetched {
            near: i64,
            label: u16,
            key: Key,
            far: i64,
            kind: Option<String>,
            /// Not loaded before this lookup.
            new: bool,
        }
        let mut fetched = Vec::new();
        self.lookups += 1;
        let (edge_labels, known) = (&self.edge_labels, &self.edge_ids);
        // What the budget leaves for new edges. Each row is a distinct edge, so
        // the new ones are charged as they arrive, as below once they are
        // numbered, and reading stops as soon as they cannot fit.
        let bytes_left = self.byte_limit.saturating_sub(self.bytes);
        let (byte_limit, vertices, edges) = (self.byte_limit, self.vertices.len(), self.edges.len());
        let (mut new_edges, mut new_bytes) = (0usize, 0usize);
        let mut rows = 0u64;
        select(
            match (outgoing, self.kind_column) {
                (true, false) => OUTGOING_SQL,
                (false, false) => INCOMING_SQL,
                (true, true) => OUTGOING_KIND_SQL,
                (false, true) => INCOMING_KIND_SQL,
            },
            self.owner,
            &[
                (pg_sys::INT8OID, datum(self.graph)?),
                (pg_sys::INT8ARRAYOID, datum(missing.clone())?),
            ],
            rows_per_fetch(
                bytes_left,
                EDGE_ROW_BYTES + MAX_KEY_BYTES + 2 * INTEGER_BYTES + if self.kind_column { MAX_SNAPSHOT_TEXT_BYTES } else { 0 },
            ),
            |row| {
                rows += 1;
                if let Some(cap) = cap.filter(|cap| cap.exceeded(rows)) {
                    return Err(cap.error());
                }
                let label = u16::try_from(row.require::<i16>(2)?)
                    .ok()
                    .filter(|&label| (label as usize) < edge_labels.len())
                    .ok_or(GraphError::InconsistentGraph("adjacency row names an unknown edge table"))?;
                let key = Key::parse(edge_labels[label as usize].kind, row.require(3)?)
                    .ok_or(GraphError::InconsistentGraph("edge key does not parse"))?;
                let kind: Option<String> = row.get(5);
                let new = !known.contains_key(&(label, key.clone()));
                if new {
                    // Without a kind column, the edge's kind is a copy of its
                    // table's label, charged as a full load charges it. A kind
                    // past the field limit arrives as NULL with its length.
                    let kind_bytes = text_length(row, 6);
                    let kind_size = if kind.is_none() && kind_bytes == 0 {
                        edge_labels[label as usize].name.len()
                    } else {
                        field_limit("kind", kind_bytes)?
                    };
                    new_bytes += EDGE_ROW_BYTES + field_limit("key", key_bytes(&key))? + 2 * INTEGER_BYTES + kind_size;
                    if new_bytes > bytes_left {
                        return Err(budget_exceeded(byte_limit, vertices, edges + new_edges));
                    }
                    new_edges += 1;
                }
                fetched.push(Fetched {
                    near: row.require(1)?,
                    label,
                    key,
                    far: row.require(4)?,
                    kind,
                    new,
                });
                Ok(())
            },
        )?;
        // Number new edges in (label, key) order. Every edge between a pair of
        // vertices arrives with the first load of either end, so ids order each
        // pair's parallel edges as a full load numbers them.
        let mut fresh = fetched.iter().filter(|edge| edge.new).collect::<Vec<_>>();
        fresh.sort_unstable_by(|a, b| (a.label, &a.key).cmp(&(b.label, &b.key)));
        fresh.dedup_by(|a, b| a.label == b.label && a.key == b.key);
        for edge in fresh {
            let (source, target) = if outgoing { (edge.near, edge.far) } else { (edge.far, edge.near) };
            // As at fetch time: the table's label when there is no kind.
            let kind_size = match &edge.kind {
                Some(kind) => field_limit("kind", kind.len())?,
                None => self.edge_labels[edge.label as usize].name.len(),
            };
            self.charge(EDGE_ROW_BYTES + field_limit("key", key_bytes(&edge.key))? + 2 * INTEGER_BYTES + kind_size)?;
            let id = self.edges.len() as i64;
            let (source_label, target_label) = self.edge_ends[edge.label as usize];
            self.add_vertex(edge.far, if outgoing { target_label } else { source_label }, Some(id))?;
            self.edges.push(Edge {
                id,
                source,
                target,
                kind: edge
                    .kind
                    .clone()
                    .unwrap_or_else(|| self.edge_labels[edge.label as usize].name.clone()),
            });
            self.edge_keys.push((edge.label, edge.key.clone()));
            self.edge_ids.insert((edge.label, edge.key.clone()), id);
        }
        let mut lists = HashMap::<i64, Vec<usize>>::new();
        for edge in &fetched {
            lists
                .entry(edge.near)
                .or_default()
                .push(self.edge_ids[&(edge.label, edge.key.clone())] as usize);
        }
        for id in missing {
            let mut list = lists.remove(&id).unwrap_or_default();
            list.sort_unstable();
            self.lists.insert((id, outgoing), list);
        }
        Ok(rows)
    }

    /// A vertex's edges in `direction`, in id order, once `load_frontier`
    /// loaded them; a self loop is in both lists and comes once.
    fn loaded(&self, id: i64, direction: Direction) -> Option<Incident<'_>> {
        let list = |outgoing| self.lists.get(&(id, outgoing)).map(Vec::as_slice);
        let outgoing = if direction != Direction::Incoming { list(true)? } else { &[] };
        let incoming = if direction != Direction::Outgoing { list(false)? } else { &[] };
        Some(Incident::new(&self.edges, outgoing, incoming))
    }

    /// `{"label", "key"}` of a vertex, after `finish`.
    pub fn vertex_name(&self, id: i64) -> Value {
        let entry = &self.vertex_entries[&id];
        let key = entry.details.as_ref().map_or(Value::Null, |details| details.key.to_json());
        json!({ "label": self.vertex_labels[entry.label as usize].name, "key": key })
    }

    /// `{"label", "key"}` of an edge.
    pub fn edge_name(&self, id: i64) -> Value {
        let (label, key) = &self.edge_keys[id as usize];
        json!({ "label": self.edge_labels[*label as usize].name, "key": key.to_json() })
    }

    /// Loads a frontier's edges in `direction`, stopping with the path search's
    /// work-limit error once `cap` says it would stop too.
    fn load_frontier(&mut self, ids: &[i64], direction: Direction, cap: Option<WorkCap>) -> Result<(), GraphError> {
        let mut earlier_rows = 0;
        if direction != Direction::Incoming {
            earlier_rows = self.load(ids, true, cap)?;
        }
        if direction != Direction::Outgoing {
            self.load(ids, false, cap.map(|cap| WorkCap { earlier_rows, ..cap }))?;
        }
        Ok(())
    }

    /// Loads what `native_paths` reads for these sources and targets: the
    /// breadth-first pass it makes, back from the targets when there are any
    /// and out from the sources otherwise. It takes a path's relationships
    /// from the lists that pass scans, so it reads nothing else. Invalid options load nothing; `native_paths` reports them. Those passes
    /// examine the relationships `native_paths` charges to `max_edge_visits`, in
    /// the same order, so loading stops with its error as soon as they exceed
    /// it, rather than after loading the whole region; and it polls for
    /// cancellation at every vertex.
    pub fn load_path_region(&mut self, config: &PathConfig, sources: &[i64], targets: Option<&[i64]>) -> Result<(), GraphError> {
        // No source starts a path, so native_paths reads nothing.
        if sources.is_empty() {
            return Ok(());
        }
        let direction = match config.direction.as_deref().map(str::to_ascii_lowercase).as_deref() {
            None | Some("outgoing") => Direction::Outgoing,
            Some("incoming") => Direction::Incoming,
            Some("both") => Direction::Both,
            Some(_) => return Ok(()),
        };
        let max_len = config.max_depth.unwrap_or(DEFAULT_PATH_LEN);
        if max_len > MAX_PATH_LEN || !(1..=10_000_000).contains(&config.max_edge_visits) {
            return Ok(());
        }
        let limit = config.max_edge_visits;
        let mut visits = 0u64;
        let cap = |visits: u64, direction: Direction| {
            Some(WorkCap {
                charged: visits,
                limit,
                both: direction == Direction::Both,
                earlier_rows: 0,
            })
        };
        // One examined relationship, charged and polled for cancellation as
        // native_paths charges and polls it.
        let visit = |visits: &mut u64| {
            pgrx::check_for_interrupts!();
            *visits += 1;
            if *visits > limit {
                return Err(GraphError::LimitExceeded {
                    operation: "native_path_edges",
                    actual: *visits,
                    limit,
                });
            }
            Ok(())
        };
        let max_len = max_len as usize;
        let kinds = config.edge_kinds.iter().map(String::as_str).collect::<HashSet<_>>();
        let far = |vertex: i64, edge: &Edge| if edge.source == vertex { edge.target } else { edge.source };
        // native_paths makes one breadth-first pass: back from the targets
        // when there are any, out from the sources otherwise. Each start
        // vertex once, as native_paths walks them, so a repeated one is not
        // charged to max_edge_visits again.
        let (start, walk) = match targets {
            Some(targets) => (
                targets,
                match direction {
                    Direction::Outgoing => Direction::Incoming,
                    Direction::Incoming => Direction::Outgoing,
                    Direction::Both => Direction::Both,
                },
            ),
            None => (sources, direction),
        };
        let mut seen = start.iter().copied().collect::<HashSet<_>>();
        let mut frontier = seen.iter().copied().collect::<Vec<_>>();
        for depth in 0..max_len {
            if frontier.is_empty() {
                break;
            }
            self.load_frontier(&frontier, walk, cap(visits, walk))?;
            let mut next = Vec::new();
            for &vertex in &frontier {
                pgrx::check_for_interrupts!();
                let edges = self.loaded(vertex, walk).ok_or(GraphError::InconsistentGraph(
                    "a path region read the edges of a vertex it did not load",
                ))?;
                for edge in edges {
                    visit(&mut visits)?;
                    if !kinds.is_empty() && !kinds.contains(edge.kind.as_str()) {
                        continue;
                    }
                    // As native_paths counts them against max_candidates:
                    // only the vertices it will expand.
                    let neighbour = far(vertex, edge);
                    if depth + 1 < max_len && seen.insert(neighbour) {
                        next.push(neighbour);
                    }
                }
            }
            if seen.len() > config.max_candidates {
                break;
            }
            frontier = next;
        }
        Ok(())
    }

    /// A CSR over everything loaded so far, numbered by `(label, key)` as a
    /// full load numbers the whole graph, with the maps back to this graph's
    /// vertex and edge ids.
    pub fn loaded_csr(&mut self) -> Result<LoadedCsr, GraphError> {
        self.finish()?;
        // finish loaded every reached vertex's details; one missing means the
        // adjacency tables name a vertex graph_vertices does not have.
        let details = self
            .vertex_entries
            .iter()
            .map(|(&vid, entry)| {
                let details = entry.details.as_ref().ok_or(GraphError::InconsistentGraph(
                    "graph_adjacency names a vertex graph_vertices does not have; run hydradb.refresh_graph",
                ))?;
                Ok((vid, (entry.label, details)))
            })
            .collect::<Result<HashMap<_, _>, GraphError>>()?;
        let mut vertex_ids = details.keys().copied().collect::<Vec<_>>();
        vertex_ids.sort_by(|a, b| (details[a].0, &details[a].1.key).cmp(&(details[b].0, &details[b].1.key)));
        let local = vertex_ids
            .iter()
            .enumerate()
            .map(|(i, &vid)| (vid, i as i64))
            .collect::<HashMap<_, _>>();
        let edge_keys = &self.edge_keys;
        let mut edge_ids = (0..edge_keys.len() as i64).collect::<Vec<_>>();
        edge_ids.sort_by(|&a, &b| edge_keys[a as usize].cmp(&edge_keys[b as usize]));
        let snapshot = GraphSnapshot {
            vertices: vertex_ids
                .iter()
                .map(|vid| Vertex {
                    id: local[vid],
                    sort_key: details[vid].1.order.clone(),
                })
                .collect(),
            edges: edge_ids
                .iter()
                .enumerate()
                .map(|(i, &id)| {
                    let edge = &self.edges[id as usize];
                    Edge {
                        id: i as i64,
                        source: local[&edge.source],
                        target: local[&edge.target],
                        kind: edge.kind.clone(),
                    }
                })
                .collect(),
        };
        Ok(LoadedCsr {
            // As a full load's build: cancelling raises from
            // check_for_interrupts.
            graph: CsrGraph::build_cancellable(snapshot, || {
                pg_sys::check_for_interrupts!();
                false
            })?,
            vertex_ids,
            edge_ids,
            local,
        })
    }
}

/// What `AdjacencyGraph::loaded_csr` builds.
pub struct LoadedCsr {
    pub graph: CsrGraph,
    /// Local vertex id to `AdjacencyGraph` vertex id.
    pub vertex_ids: Vec<i64>,
    /// Local edge id to `AdjacencyGraph` edge id.
    pub edge_ids: Vec<i64>,
    pub local: HashMap<i64, i64>,
}

impl GraphRead for AdjacencyGraph {
    /// Only the vertices loaded so far; the kernels never list them all.
    fn vertices(&self) -> &[Vertex] {
        &[]
    }

    fn vertex(&self, id: i64) -> Option<&Vertex> {
        self.vertices.get(self.vertex_entries.get(&id)?.slot)
    }

    fn edge(&self, id: i64) -> Option<&Edge> {
        self.edges.get(usize::try_from(id).ok()?)
    }

    fn load_edges(&mut self, ids: &[i64], direction: Direction) -> Result<(), GraphError> {
        self.load_frontier(ids, direction, None)
    }

    /// An edge's id is its position in `edges`, so `after` cuts the lists at it.
    fn loaded_edges(&self, _token: Expanded, id: i64, direction: Direction, after: Option<i64>) -> Option<impl Iterator<Item = &Edge>> {
        let edges = self.loaded(id, direction)?;
        match after {
            None => Some(edges),
            Some(edge) => Some(edges.after(usize::try_from(edge).ok()?)),
        }
    }

    fn ordered(&self) -> bool {
        false
    }

    /// Loads the vertices' details, whose `order` encodes
    /// `(sort_key, label, key)`, and sorts by it. Keys are unique within a
    /// label, so the order is total. It keeps them: the vertices are ones the
    /// walk reached, whose details `finish` loads anyway to report a dangling
    /// endpoint or duplicate key among them.
    fn sort_by_order(&mut self, ids: &[i64]) -> Result<Vec<i64>, GraphError> {
        self.load_details(ids)?;
        let mut keyed = ids
            .iter()
            .map(|&id| {
                let details = self.vertex_entries.get(&id).and_then(|entry| entry.details.as_ref());
                let details = details.ok_or(GraphError::InconsistentGraph("a vertex to order has no details"))?;
                Ok((details.order.as_str(), id))
            })
            .collect::<Result<Vec<_>, GraphError>>()?;
        keyed.sort_unstable();
        keyed.dedup();
        Ok(keyed.into_iter().map(|(_, id)| id).collect())
    }
}
