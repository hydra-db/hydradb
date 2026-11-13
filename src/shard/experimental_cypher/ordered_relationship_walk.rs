//! One page of an ordered walk over a relationship property index, for the
//! experimental engine's `OrderedRelationshipPropertyScan`.
//!
//! A relationship property lives in one of two indexes. `eprop_idx` holds
//! structural edge properties, keyed `<value>/<src>/<dst>`; `rprop_idx` holds
//! identified relationship properties, keyed `<value>/<src>/<dst>/<rid>`. Both
//! use the vertex index's type-tagged value encoding, so the walk opens the
//! vertex walk's type streams ([`STREAMS`], bounded by the same sound
//! [`stream_range`]) over *both* indexes and merges all of them under
//! [`compare_property_values`], then `(src, dst)`, then structural before
//! identified, then relationship id, each in the walk direction.
//!
//! Each entry becomes the relationships expansion reports for its edge
//! (`native_path_relationships_at`), and each of those exactly once:
//!
//! - an `rprop_idx` entry yields its identified relationship, whose own
//!   property it indexes, merged over the structural edge's properties;
//! - an `eprop_idx` entry yields the structural edge when the edge has no
//!   identified relationships, and otherwise every identified relationship
//!   that inherits the property from the edge instead of carrying its own.
//!
//! Every entry is re-checked against the record it names, so a stale index
//! entry never surfaces a value the relationship no longer holds.
use std::cmp::Ordering;

use hydradb_cypher_engine::{compare_property_values, OrderedPropertyScanRequest, OrderedRelationshipPropertyScanRequest};

use super::ordered_walk::{stream_range, tighter_lower, tighter_upper, Stream, STREAMS};
use std::ops::Bound;

use super::*;
use crate::codec::parse_relationship_property_index_key;
use crate::shard::query::parse_edge_property_index_key;
use crate::shard::query::{IndexedRelationshipBatch, IndexedRelationshipPairMemo, ORDERED_RELATIONSHIP_FETCH_BATCH};

/// One relationship of an ordered page: its identity (`None` for a
/// structural edge, which the adapter names), endpoints, and the properties
/// expansion reports for it.
pub(in crate::shard) struct OrderedRelationshipRow {
    pub(in crate::shard) relationship_id: Option<RelationshipId>,
    pub(in crate::shard) src: VertexId,
    pub(in crate::shard) dst: VertexId,
    pub(in crate::shard) metadata: EdgeMetadata,
}

pub(in crate::shard) struct OrderedRelationshipPropertyPage {
    pub(in crate::shard) rows: Vec<OrderedRelationshipRow>,
    /// Index entries this page read, resumed entries included. A walk that
    /// resumes at its saved key reads a constant number beyond the page; one
    /// that restarts at the value reads the whole run again, so this is what a
    /// test can hold a paged tie to.
    ///
    /// Retained for observability; carried through the walk even when no
    /// reader asserts it, so the field can stay `-D dead-code` clean.
    #[allow(dead_code)]
    pub(in crate::shard) index_entries: u64,
    /// The cursor to resume after, opaque to the engine.
    pub(in crate::shard) last: Option<Vec<u8>>,
    /// Every stream inside the request's restrictions is spent. Reported
    /// rather than inferred from a short page, for the reason
    /// `OrderedPropertyScanPage::exhausted` gives.
    pub(in crate::shard) exhausted: bool,
}

/// Which index an entry came from. Structural entries sort first within an
/// `(value, src, dst)` tie when walking ascending.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Index {
    Edge,
    Relationship,
}

impl Index {
    fn tag(self) -> u8 {
        match self {
            Self::Edge => b'e',
            Self::Relationship => b'r',
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            b'e' => Some(Self::Edge),
            b'r' => Some(Self::Relationship),
            _ => None,
        }
    }
}

/// An index entry's position in the merged walk.
#[derive(Clone, Debug)]
struct Position {
    value: ScalarValue,
    src: VertexId,
    dst: VertexId,
    index: Index,
    relationship_id: RelationshipId,
}

impl Position {
    /// Ascending merge order; a descending walk reverses it.
    fn cmp(&self, other: &Self) -> Ordering {
        compare_property_values(&self.value, &other.value).then_with(|| {
            (self.src, self.dst, self.index, self.relationship_id).cmp(&(other.src, other.dst, other.index, other.relationship_id))
        })
    }
}

/// The opaque cursor: the last entry consumed -- which index, and its key
/// below the property prefix -- so a later page resumes where this one
/// stopped. It carries no candidate tally: the cap is charged to the request's
/// budget, which already spans this walk's pages and every other operator's.
struct Cursor {
    index: Index,
    suffix: Vec<u8>,
}

impl Cursor {
    const VERSION: u8 = 1;

    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(2 + self.suffix.len());
        bytes.push(Self::VERSION);
        bytes.push(self.index.tag());
        bytes.extend_from_slice(&self.suffix);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let corrupt = |reason: &str| GraphError::CorruptValue {
            key: "experimental ordered relationship scan cursor".to_string(),
            reason: reason.to_string(),
        };
        let [version, rest @ ..] = bytes else {
            return Err(corrupt("empty cursor"));
        };
        if *version != Self::VERSION {
            return Err(corrupt("unknown cursor version"));
        }
        let Some((index, suffix)) = rest.split_first() else {
            return Err(corrupt("cursor names no index"));
        };
        let index = Index::from_tag(*index).ok_or_else(|| corrupt("unknown cursor index"))?;
        if suffix.is_empty() {
            return Err(corrupt("cursor has no index position"));
        }
        Ok(Self {
            index,
            suffix: suffix.to_vec(),
        })
    }

    /// The position the suffix names. The suffix is `<tag><segment>/<src>/
    /// <dst>` or `.../<rid>`, the key below the property prefix.
    fn position(&self) -> Result<Position> {
        let corrupt = || GraphError::CorruptValue {
            key: "experimental ordered relationship scan cursor".to_string(),
            reason: "cursor position is not an index entry".to_string(),
        };
        let suffix = std::str::from_utf8(&self.suffix).map_err(|_| corrupt())?;
        let parts = suffix.split('/').collect::<Vec<_>>();
        let (encoded, src, dst, relationship_id) = match (self.index, parts.as_slice()) {
            (Index::Edge, [encoded, src, dst]) => (*encoded, *src, *dst, "0"),
            (Index::Relationship, [encoded, src, dst, rid]) => (*encoded, *src, *dst, *rid),
            _ => return Err(corrupt()),
        };
        let number = |text: &str| text.parse::<u64>().map_err(|_| corrupt());
        Ok(Position {
            value: property_to_scalar(&decode_vertex_property_value_key(encoded)?),
            src: number(src)?,
            dst: number(dst)?,
            index: self.index,
            relationship_id: number(relationship_id)?,
        })
    }
}

struct Entry {
    position: Position,
    /// The value segment, type tag included, as stored in the key.
    encoded: String,
    /// The key below the property prefix: the cursor position.
    suffix: Vec<u8>,
}

struct OpenStream {
    iter: slatedb::DbIterator,
    index: Index,
    prefix_len: usize,
    head: Option<Entry>,
}

impl OpenStream {
    async fn advance(&mut self, edge_type: &str, property: &str, budget: &QueryBudget, index_entries: &mut u64) -> Result<()> {
        loop {
            *index_entries = index_entries.saturating_add(1);
            let Some(kv) = budget
                .read_only_io("experimental_ordered_relationship_index_next", async {
                    self.iter.next().await.map_err(GraphError::from)
                })
                .await?
            else {
                self.head = None;
                return Ok(());
            };
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (indexed_type, indexed_property, encoded, src, dst, relationship_id) = match self.index {
                Index::Edge => {
                    let (_cell, edge_type, property, encoded, src, dst) = parse_edge_property_index_key(&key)?;
                    (edge_type, property, encoded, src, dst, 0)
                }
                Index::Relationship => {
                    let (_cell, edge_type, property, encoded, src, dst, relationship_id) = parse_relationship_property_index_key(&key)?;
                    (edge_type, property, encoded, src, dst, relationship_id)
                }
            };
            // A property whose name extends this one shares the byte prefix.
            if indexed_type != edge_type || indexed_property != property {
                continue;
            }
            self.head = Some(Entry {
                position: Position {
                    value: property_to_scalar(&decode_vertex_property_value_key(&encoded)?),
                    src,
                    dst,
                    index: self.index,
                    relationship_id,
                },
                encoded,
                suffix: kv.key.get(self.prefix_len..).unwrap_or_default().to_vec(),
            });
            return Ok(());
        }
    }
}

impl GraphShard {
    async fn fetch_identified_ordered_entries(
        &self,
        cell_id: &str,
        edge_type: &str,
        property: &str,
        batch: (&[Entry], &mut IndexedRelationshipPairMemo),
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<Vec<Option<EdgeMetadata>>> {
        let (entries, memo) = batch;
        let candidates = entries
            .iter()
            .map(|entry| {
                (
                    entry.position.src,
                    entry.position.dst,
                    entry.position.relationship_id,
                    entry.encoded.clone(),
                )
            })
            .collect::<Vec<_>>();
        self.indexed_relationship_metadata_batch_at(IndexedRelationshipBatch {
            cell_id,
            edge_type,
            property,
            candidates: &candidates,
            read_epoch,
            inherit_structural: true,
            budget,
            memo,
        })
        .await
    }

    fn append_identified_ordered_entries(
        &self,
        rows: &mut Vec<OrderedRelationshipRow>,
        kept: &mut usize,
        pending: &mut Vec<Entry>,
        metadata: Vec<Option<EdgeMetadata>>,
    ) -> Result<()> {
        for (entry, metadata) in pending.drain(..).zip(metadata) {
            if let Some(metadata) = metadata {
                rows.push(OrderedRelationshipRow {
                    relationship_id: Some(entry.position.relationship_id),
                    src: entry.position.src,
                    dst: entry.position.dst,
                    metadata,
                });
                *kept += 1;
                self.ensure_query_intermediate_rows("experimental_ordered_relationship_property_rows", rows.len())?;
            }
        }
        Ok(())
    }

    /// Walk one page of the `request.relationship_type`.`request.property`
    /// indexes in order. Returns the relationships of up to `required`
    /// qualifying entries and never decides the window: the engine owns the
    /// residual, endpoint labels, the top-k and the boundary tie.
    pub(in crate::shard) async fn scan_ordered_relationship_property_page_at(
        &self,
        cell_id: &str,
        request: &OrderedRelationshipPropertyScanRequest,
        read_epoch: StorageSequence,
        budget: &QueryBudget,
    ) -> Result<OrderedRelationshipPropertyPage> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", &request.relationship_type)?;
        validate_component("property", &request.property)?;
        if request.required == 0 {
            return Ok(OrderedRelationshipPropertyPage {
                rows: Vec::new(),
                last: None,
                exhausted: false,
                index_entries: 0,
            });
        }
        let cursor = request.after.as_ref().map(|cursor| Cursor::decode(&cursor.0)).transpose()?;
        let resume = cursor.as_ref().map(Cursor::position).transpose()?;
        let edge_type = request.relationship_type.as_str();
        let property = request.property.as_str();
        // The vertex walk's stream ranges take its request type; only the
        // restrictions and direction matter to them.
        let restrictions = OrderedPropertyScanRequest {
            property: request.property.clone(),
            labels: Vec::new(),
            ascending: request.ascending,
            id_ascending: request.ascending,
            prefix: request.prefix.clone(),
            lower: request.lower.clone(),
            upper: request.upper.clone(),
            required: request.required,
            after: None,
            // Only the restrictions and direction are read from this; the
            // relationship walk settles its own window.
            index_order: true,
            allowed: None,
            max_index_entries: None,
        };
        let resume_value = resume.as_ref().map(|position| (position.value.clone(), position.src));
        let prefixes = [
            (Index::Edge, keys::edge_property_index_property_prefix(cell_id, edge_type, property)),
            (
                Index::Relationship,
                keys::relationship_property_index_property_prefix(cell_id, edge_type, property),
            ),
        ];
        let scan_options = remote_scan_options().with_order(if request.ascending {
            slatedb::IterationOrder::Ascending
        } else {
            slatedb::IterationOrder::Descending
        });

        // Streams grouped by value rank and opened a group at a time, as in
        // the vertex walk, so a window that fills on numbers never seeks
        // strings.
        let mut groups = Vec::<Vec<Stream>>::new();
        for stream in STREAMS {
            match groups.last_mut() {
                Some(group) if group[0].rank == stream.rank => group.push(stream),
                _ => groups.push(vec![stream]),
            }
        }
        if !request.ascending {
            groups.reverse();
        }
        let mut groups = groups.into_iter();
        let mut open: Vec<OpenStream> = Vec::new();

        let mut index_entries = 0_u64;
        let mut rows = Vec::<OrderedRelationshipRow>::new();
        let mut entries = 0_usize;
        let mut pending_identified = Vec::with_capacity(ORDERED_RELATIONSHIP_FETCH_BATCH);
        let mut pair_memo = IndexedRelationshipPairMemo::default();
        // The exact entry the previous page stopped on, split into the index
        // it belongs to, the stream tag its key starts with, and the rest of
        // that key. The saved suffix is relative to the property prefix and so
        // carries the tag; a scan prefix carries the tag too, so dropping it
        // puts the key in the frame `stream_range` works in.
        let resume_key = cursor
            .as_ref()
            .and_then(|cursor| cursor.suffix.split_first().map(|(tag, key)| (cursor.index, *tag, key.to_vec())));
        let mut last = cursor.map(|cursor| (cursor.index, cursor.suffix));
        let mut exhausted = false;
        while entries < request.required {
            if open.iter().all(|stream| stream.head.is_none()) {
                let Some(group) = groups.next() else {
                    exhausted = true;
                    break;
                };
                open.clear();
                for stream in group {
                    // The value-level range. `stream_range` names an exact
                    // resume entry only for a caller whose key layout it
                    // knows, and the relationship indexes key by endpoints, so
                    // it gets `None` and the cursor is applied below instead.
                    let Some(range) = stream_range(stream, &restrictions, resume_value.as_ref(), &|_, _| None)? else {
                        continue;
                    };
                    for (index, property_prefix) in &prefixes {
                        let prefix = format!("{property_prefix}{}", stream.tag);
                        // Resume strictly past the saved entry on the one
                        // index and stream it came from. Without this the
                        // value-level bound restarts at the first entry of the
                        // run, and every page of a tie rereads and re-skips
                        // the ones before it: a run of n costs O(n^2) across
                        // its pages. Every other stream keeps the value bound,
                        // which the order filter below still makes sound.
                        //
                        // "The stream it came from" is decided by the key
                        // landing inside this stream's own range, not by the
                        // tag: the two halves of the `n` stream share a tag
                        // and split around zero, and the merge orders entries
                        // by value, under which `-0.0` and `+0.0` are equal.
                        // So the halves interleave, a page can stop on a
                        // `+0.0` entry while `-0.0` entries that sort after it
                        // are still unread, and a bound applied to the other
                        // half by tag alone lands outside its disjoint range
                        // and empties it, dropping those rows.
                        let (mut start, mut end) = (range.0.clone(), range.1.clone());
                        if let Some((resume_index, tag, key)) = &resume_key {
                            let within = |lower: &Bound<Vec<u8>>, upper: &Bound<Vec<u8>>| {
                                let above = match lower {
                                    Bound::Included(bound) => key >= bound,
                                    Bound::Excluded(bound) => key > bound,
                                    Bound::Unbounded => true,
                                };
                                let below = match upper {
                                    Bound::Included(bound) => key <= bound,
                                    Bound::Excluded(bound) => key < bound,
                                    Bound::Unbounded => true,
                                };
                                above && below
                            };
                            if resume_index == index && u32::from(*tag) == stream.tag as u32 && within(&range.0, &range.1) {
                                if request.ascending {
                                    start = tighter_lower(start, Bound::Excluded(key.clone()));
                                } else {
                                    end = tighter_upper(end, Bound::Excluded(key.clone()));
                                }
                            }
                        }
                        let iter = budget
                            .read_only_io(
                                "experimental_ordered_relationship_index_seek",
                                self.storage.scan_prefix_range_with_options(
                                    prefix.as_bytes(),
                                    // This API seeks from an inclusive start,
                                    // so an excluded bound becomes the
                                    // immediate successor key.
                                    match start.clone() {
                                        Bound::Included(key) => Some(key),
                                        Bound::Excluded(mut key) => {
                                            key.push(0);
                                            Some(key)
                                        }
                                        Bound::Unbounded => None,
                                    },
                                    match end.clone() {
                                        Bound::Excluded(key) => Some(key),
                                        Bound::Included(mut key) => {
                                            key.push(0);
                                            Some(key)
                                        }
                                        Bound::Unbounded => None,
                                    },
                                    &scan_options,
                                ),
                            )
                            .await?;
                        let mut stream = OpenStream {
                            iter,
                            index: *index,
                            prefix_len: property_prefix.len(),
                            head: None,
                        };
                        stream.advance(edge_type, property, budget, &mut index_entries).await?;
                        open.push(stream);
                    }
                }
                continue;
            }
            let mut best: Option<usize> = None;
            for (index, stream) in open.iter().enumerate() {
                let Some(head) = &stream.head else {
                    continue;
                };
                let better = best.is_none_or(|best| {
                    let current = &open[best].head.as_ref().expect("selected head").position;
                    let ordering = head.position.cmp(current);
                    if request.ascending {
                        ordering.is_lt()
                    } else {
                        ordering.is_gt()
                    }
                });
                if better {
                    best = Some(index);
                }
            }
            let stream = &mut open[best.expect("a stream has a head")];
            let mut entry = stream.head.take().expect("selected head");
            stream.advance(edge_type, property, budget, &mut index_entries).await?;
            budget.check("experimental_ordered_relationship_property_index")?;

            if let Some(resume) = &resume {
                let ordering = entry.position.cmp(resume);
                let after = if request.ascending { ordering.is_gt() } else { ordering.is_lt() };
                if !after {
                    continue;
                }
            }
            last = Some((entry.position.index, std::mem::take(&mut entry.suffix)));
            // Charged before the entry is judged, for the reason the vertex
            // walk charges there: a restriction the seek could only
            // approximate -- `> v`, which seeks inclusively to `v` -- rejects
            // here, and an unbounded run of such entries must still reach the
            // admission error rather than be read for free.
            //
            // Charged to the request's budget rather than to a tally on the
            // cursor. The cursor spans one walk's pages and nothing else, so a
            // query running several of these walks -- the arms of a UNION, say
            // -- gave each one a fresh allowance and could read a multiple of
            // the cap without an admission error. The budget is the whole
            // request's, which is the scope the cap is written in.
            self.charge_query_index_candidates("experimental_ordered_relationship_property_candidates", budget, 1)?;
            if !request.accepts(&entry.position.value) {
                continue;
            }
            if entry.position.index == Index::Relationship {
                pending_identified.push(entry);
                if pending_identified.len() >= ORDERED_RELATIONSHIP_FETCH_BATCH.min(request.required - entries) {
                    let metadata = self
                        .fetch_identified_ordered_entries(
                            cell_id,
                            edge_type,
                            property,
                            (&pending_identified, &mut pair_memo),
                            read_epoch,
                            budget,
                        )
                        .await?;
                    self.append_identified_ordered_entries(&mut rows, &mut entries, &mut pending_identified, metadata)?;
                }
                continue;
            }
            if !pending_identified.is_empty() {
                let metadata = self
                    .fetch_identified_ordered_entries(
                        cell_id,
                        edge_type,
                        property,
                        (&pending_identified, &mut pair_memo),
                        read_epoch,
                        budget,
                    )
                    .await?;
                self.append_identified_ordered_entries(&mut rows, &mut entries, &mut pending_identified, metadata)?;
            }
            let Position { src, dst, .. } = entry.position;
            let records = self
                .unmerged_relationships_at(cell_id, edge_type, src, dst, read_epoch, budget)
                .await?;
            let structural = records.iter().find_map(|(id, metadata)| id.is_none().then_some(metadata));
            let identified = records.iter().filter_map(|(id, metadata)| id.map(|id| (id, metadata)));
            let holds_entry = |metadata: &EdgeMetadata| {
                metadata
                    .properties
                    .get(property)
                    .is_some_and(|value| encode_vertex_property_value_key(value) == entry.encoded)
            };
            let merged = |own: &EdgeMetadata| {
                let mut metadata = structural.cloned().unwrap_or_default();
                metadata.properties.extend(own.properties.clone());
                metadata
            };
            let before = rows.len();
            match structural.filter(|metadata| holds_entry(metadata)) {
                None => {}
                // Expansion reports the identified relationships in place
                // of the edge; those without their own value inherit it.
                Some(_) if records.iter().any(|(id, _)| id.is_some()) => {
                    for (id, own) in identified.filter(|(_, own)| !own.properties.contains_key(property)) {
                        rows.push(OrderedRelationshipRow {
                            relationship_id: Some(id),
                            src,
                            dst,
                            metadata: merged(own),
                        });
                    }
                }
                Some(structural) => rows.push(OrderedRelationshipRow {
                    relationship_id: None,
                    src,
                    dst,
                    metadata: structural.clone(),
                }),
            }
            if rows.len() > before {
                entries += 1;
                self.ensure_query_intermediate_rows("experimental_ordered_relationship_property_rows", rows.len())?;
            }
        }
        if !pending_identified.is_empty() {
            let metadata = self
                .fetch_identified_ordered_entries(
                    cell_id,
                    edge_type,
                    property,
                    (&pending_identified, &mut pair_memo),
                    read_epoch,
                    budget,
                )
                .await?;
            self.append_identified_ordered_entries(&mut rows, &mut entries, &mut pending_identified, metadata)?;
        }
        let last = last.map(|(index, suffix)| Cursor { index, suffix }.encode());
        // The caller's operator span declares this; outside one it is silently
        // dropped, as in the vertex walk.
        tracing::Span::current().record("hydradb.query.scan.index_entries", index_entries);
        Ok(OrderedRelationshipPropertyPage {
            rows,
            last,
            exhausted,
            index_entries,
        })
    }
}
