//! Deterministic FalkorDB-to-Turbolay identity and scope resolver.
//!
//! This crate accepts only already-validated [`falkordb_rdb::GraphDump`] values.
//! It contains no object-store writer, which keeps source anomaly handling
//! independently testable from publication.

use std::collections::{BTreeMap, BTreeSet};

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use falkordb_rdb::{Entity, GraphDump, Properties, PropertyValue, RelationshipCell};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod byog;
pub use byog::{byog_scope, resolve_byog_graph};

mod finalize;
mod kv_sort;
mod local_store;
mod snapshot;
pub use finalize::{finalize_image, offline_import_limits, publish_planner_statistics, FinalizedImage, ImageFinalize, VerifiedEdgeType};
pub use kv_sort::{ExternalKvSorter, KvSortError, SortedRows};
pub use local_store::SingleWriterLocalStore;
pub use snapshot::{
    build_snapshot_plan, build_snapshot_spill, build_snapshot_spill_excluding, build_snapshot_spill_selected,
    build_snapshot_spill_with_layout, build_snapshot_spill_with_source_layout, load_spilled_scope, DedicatedLayout, ExcludedGraph,
    GraphSelection, SnapshotError, SnapshotGraph, SnapshotPlan, SnapshotSpill, SourceLayout, SpilledScope, SpilledUnscoped,
};

const IDENTITY_MASK: u64 = i64::MAX as u64;

#[derive(Debug, Error)]
pub enum ResolveError {
    #[error("graph {graph:?}, node {node}: {message}")]
    Node { graph: String, node: u64, message: String },
    #[error("graph {graph:?}, edge {edge}: {message}")]
    Edge { graph: String, edge: u64, message: String },
    #[error("scope {scope}: {message}")]
    Scope { scope: Scope, message: String },
}

pub type Result<T> = std::result::Result<T, ResolveError>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Scope {
    pub tenant_id: String,
    pub sub_tenant_id: String,
}

impl std::fmt::Display for Scope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.tenant_id, self.sub_tenant_id)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolvedVertex {
    pub id: u64,
    pub labels: BTreeSet<String>,
    pub properties: Properties,
    pub sources: Vec<SourceObject>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolvedRelationship {
    pub id: u64,
    pub logical_id: u64,
    pub variant_id: u64,
    pub edge_type: String,
    pub src: u64,
    pub dst: u64,
    pub properties: Properties,
    pub sources: Vec<SourceObject>,
    /// A BRIDGE whose bridge_id came from FalkorDB. Every other bridge's bridge_id
    /// is its final id, set again whenever that id is.
    #[serde(default)]
    pub source_bridge_id: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SourceObject {
    pub graph: String,
    pub falkor_id: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrphanEdge {
    pub source: SourceObject,
    pub properties: Properties,
}

/// A node nothing can scope (no tenant_id, no scoped neighbour, a graph key that
/// names no scope) and no edge depends on: kept in the quarantine report, not imported.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QuarantinedVertex {
    pub source: SourceObject,
    pub labels: BTreeSet<String>,
    pub properties: Properties,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipAnomalyDisposition {
    ImportedWithoutEdgeRecord,
    ImportedWithoutAdjacencyPair,
    ImportedWithFallbackIdentity,
    ImportedWithDisambiguatedIdentity,
    QuarantinedCrossScope,
    QuarantinedMissingEndpoint,
    /// An endpoint is a quarantined vertex no single scope can hold.
    QuarantinedUnscopableEndpoint,
    /// A multi-edge cell whose edge-id list FalkorDB left out of the dump
    /// ([`falkordb_rdb::DanglingTensorCell`]): the cell's edges cannot be
    /// imported. `source.falkor_id` is 0, as no edge id is known, and the
    /// `cell_value` property holds the cell's raw value.
    QuarantinedMissingMultiEdgeList,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RelationshipAnomaly {
    pub source: SourceObject,
    pub edge_type: String,
    pub src_falkor_id: u64,
    pub dst_falkor_id: u64,
    pub disposition: RelationshipAnomalyDisposition,
    pub properties: Properties,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResolutionDiagnostics {
    pub merged_vertices: u64,
    pub merged_relationships: u64,
    pub inherited_scopes: u64,
    pub graph_key_scopes: u64,
    pub reused_edge_id_relationships: u64,
    pub relationship_anomalies: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolvedScope {
    /// BYOG records retain source identities and user-owned properties.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub byog: bool,
    pub scope: Scope,
    pub vertices: BTreeMap<u64, ResolvedVertex>,
    pub relationships: BTreeMap<u64, ResolvedRelationship>,
    pub orphan_edges: Vec<OrphanEdge>,
    pub relationship_anomalies: Vec<RelationshipAnomaly>,
    pub diagnostics: ResolutionDiagnostics,
}

impl ResolvedScope {
    fn empty(scope: Scope) -> Self {
        Self {
            byog: false,
            scope,
            vertices: BTreeMap::new(),
            relationships: BTreeMap::new(),
            orphan_edges: Vec::new(),
            relationship_anomalies: Vec::new(),
            diagnostics: ResolutionDiagnostics::default(),
        }
    }
}

/// One graph resolved: its scopes, and the vertices no scope can hold, which
/// belong to the graph's quarantine report rather than to any scope's.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphResolution {
    pub scopes: Vec<ResolvedScope>,
    pub unscoped_vertices: Vec<QuarantinedVertex>,
}

#[derive(Clone)]
struct NodeResolution {
    label: String,
    identity: String,
    /// How a RELATES_TO names this vertex as its target: the source_id, or for a
    /// placeholder Source "provider:external_id" whatever its scope.
    relation_key: String,
    /// An Entity's name, which the by-name RELATES writers hash.
    name: Option<String>,
    scope: Scope,
    id: u64,
}

pub fn stable_id(scope: &Scope, label: &str, identity: &str) -> u64 {
    let graph_name = ascii_json_array([scope.tenant_id.as_str(), scope.sub_tenant_id.as_str()]);
    let payload = ascii_json_array([graph_name.as_str(), label, identity]);
    let mut hasher = Blake2bVar::new(8).expect("8-byte BLAKE2b output is valid");
    hasher.update(payload.as_bytes());
    let mut digest = [0u8; 8];
    hasher.finalize_variable(&mut digest).expect("output has exact requested length");
    let value = u64::from_be_bytes(digest) & IDENTITY_MASK;
    value.max(1)
}

pub fn ascii_json_array<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut output = String::from("[");
    for (index, value) in values.into_iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push('"');
        for character in value.chars() {
            match character {
                '"' => output.push_str("\\\""),
                '\\' => output.push_str("\\\\"),
                '\u{0008}' => output.push_str("\\b"),
                '\u{000c}' => output.push_str("\\f"),
                '\n' => output.push_str("\\n"),
                '\r' => output.push_str("\\r"),
                '\t' => output.push_str("\\t"),
                character if (' '..='~').contains(&character) => output.push(character),
                character if (character as u32) <= 0xffff => {
                    use std::fmt::Write as _;
                    write!(output, "\\u{:04x}", character as u32).expect("String writes cannot fail");
                }
                character => {
                    use std::fmt::Write as _;
                    let scalar = character as u32 - 0x1_0000;
                    let high = 0xd800 + (scalar >> 10);
                    let low = 0xdc00 + (scalar & 0x3ff);
                    write!(output, "\\u{high:04x}\\u{low:04x}").expect("String writes cannot fail");
                }
            }
        }
        output.push('"');
    }
    output.push(']');
    output
}

/// The scopes of a graph that has no unscoped vertices; one that has them is
/// refused, so a caller can never drop them unawares (resolve_graph_report keeps them).
pub fn resolve_graph(graph: GraphDump) -> Result<Vec<ResolvedScope>> {
    let resolution = resolve_graph_report(graph)?;
    if let Some(vertex) = resolution.unscoped_vertices.first() {
        return Err(ResolveError::Node {
            graph: vertex.source.graph.clone(),
            node: vertex.source.falkor_id,
            message: "no scope can hold it; resolve_graph_report keeps it for the quarantine report".into(),
        });
    }
    Ok(resolution.scopes)
}

/// Resolves a graph it consumes: each entity's properties and labels move into
/// the one record that keeps them instead of being copied, so the dump and its
/// resolution are not both held whole.
pub fn resolve_graph_report(graph: GraphDump) -> Result<GraphResolution> {
    resolve_with_source_scope(graph, None)
}

fn resolve_with_source_scope(graph: GraphDump, source_scope: Option<&Scope>) -> Result<GraphResolution> {
    if graph.name.starts_with("byog:") {
        return Err(ResolveError::Node {
            graph: graph.name,
            node: 0,
            message: "BYOG graph requires the BYOG resolver".into(),
        });
    }
    let GraphDump {
        name,
        nodes,
        mut edges,
        labels: mut node_labels,
        relationships,
        adjacency,
        dangling_tensor_cells,
        ..
    } = graph;
    let fallback_scope = source_scope.cloned().or_else(|| graph_key_scope(&name));
    // A node's scope comes from its decoded relationships. A dangling cell's
    // edges are lost but its endpoints are known, so it is consulted only when
    // those give no scope; and only decoded relationships make a node depend
    // on a scope, so a node no scope reaches and no imported edge touches is
    // quarantined rather than an error.
    let mut neighbors: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
    for relationship in &relationships {
        neighbors.entry(relationship.src).or_default().insert(relationship.dst);
        neighbors.entry(relationship.dst).or_default().insert(relationship.src);
    }
    let mut dangling_neighbors: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
    for cell in &dangling_tensor_cells {
        dangling_neighbors.entry(cell.src).or_default().insert(cell.dst);
        dangling_neighbors.entry(cell.dst).or_default().insert(cell.src);
    }
    let mut direct_scopes = BTreeMap::new();
    if let Some(scope) = source_scope {
        for (kind, records) in [("node", &nodes), ("relationship", &edges)] {
            for (&id, entity) in records {
                for (key, expected) in [("tenant_id", &scope.tenant_id), ("sub_tenant_id", &scope.sub_tenant_id)] {
                    match entity.properties.get(key) {
                        None | Some(PropertyValue::Null) => {}
                        Some(PropertyValue::String(value)) if value.is_empty() || value == expected => {}
                        _ => {
                            return Err(ResolveError::Scope {
                                scope: scope.clone(),
                                message: format!("graph {name:?} {kind} {id} has {key} inconsistent with dedicated source placement"),
                            })
                        }
                    }
                }
            }
        }
    }
    for (&id, entity) in &nodes {
        if let Some(scope) = source_scope {
            direct_scopes.insert(id, scope.clone());
        } else if let Some(scope) = entity_scope(entity, &name, id)? {
            direct_scopes.insert(id, scope);
        }
    }
    let mut scopes = direct_scopes.clone();
    let mut inherited = 0u64;
    let mut graph_key = 0u64;
    let mut unscopable = BTreeSet::new();
    for (&id, entity) in &nodes {
        if scopes.contains_key(&id) {
            continue;
        }
        let scopes_of = |neighbors: &BTreeMap<u64, BTreeSet<u64>>| -> BTreeSet<Scope> {
            neighbors
                .get(&id)
                .into_iter()
                .flatten()
                .filter_map(|neighbor| direct_scopes.get(neighbor))
                .cloned()
                .collect()
        };
        let mut candidates = scopes_of(&neighbors);
        if candidates.is_empty() {
            let dangling = scopes_of(&dangling_neighbors);
            if dangling.len() == 1 {
                candidates = dangling;
            }
        }
        if candidates.len() != 1 && fallback_scope.is_none() && !neighbors.contains_key(&id) {
            // Nothing scopes it and no imported edge depends on it (a bare stub
            // such as an entity_id-only :Entity, or nodes only a dangling cell
            // joins): quarantined below, never imported.
            unscopable.insert(id);
            continue;
        }
        let declared_sub_tenant = scope_component(entity, "sub_tenant_id", &name, id)?;
        let scope = match candidates.len() {
            1 => {
                inherited += 1;
                candidates.into_iter().next().unwrap()
            }
            0 => {
                graph_key += 1;
                fallback_scope.clone().ok_or_else(|| ResolveError::Node {
                    graph: name.clone(),
                    node: id,
                    message: "has no tenant_id, no scoped neighbour, and graph key does not encode a scope".into(),
                })?
            }
            _ => {
                // Its neighbours sit in several scopes and it declares none (a bare
                // stub linked across sub-tenants): no one Turbolay cell can hold it,
                // so it and its edges are quarantined below.
                unscopable.insert(id);
                continue;
            }
        };
        if let Some(declared_sub_tenant) = declared_sub_tenant {
            if scope.sub_tenant_id != declared_sub_tenant {
                return Err(ResolveError::Node {
                    graph: name.clone(),
                    node: id,
                    message: format!("declares sub_tenant_id {declared_sub_tenant:?} but inferred scope is {scope}"),
                });
            }
        }
        scopes.insert(id, scope);
    }
    drop((neighbors, dangling_neighbors, direct_scopes));

    let mut node_resolutions = BTreeMap::new();
    let mut outputs = BTreeMap::<Scope, ResolvedScope>::new();
    // A selected empty dedicated graph still has a known scope to register.
    if let Some(scope) = source_scope {
        outputs.insert(scope.clone(), ResolvedScope::empty(scope.clone()));
    }
    let mut unscopable_entities = BTreeMap::new();
    for (falkor_id, entity) in nodes {
        if unscopable.contains(&falkor_id) {
            unscopable_entities.insert(falkor_id, entity);
            continue;
        }
        let labels = node_labels.remove(&falkor_id).unwrap_or_default();
        let identities = labels
            .iter()
            .filter_map(|label| vertex_identity(label, &entity).map(|identity| (label.clone(), identity)))
            .collect::<Vec<_>>();
        if identities.len() != 1 {
            return Err(ResolveError::Node {
                graph: name.clone(),
                node: falkor_id,
                message: format!("has {} supported identity labels, expected exactly one", identities.len()),
            });
        }
        let (label, identity) = identities.into_iter().next().unwrap();
        let scope = scopes.get(&falkor_id).expect("every node receives a scope").clone();
        let id = stable_id(&scope, &label, &identity);
        node_resolutions.insert(
            falkor_id,
            NodeResolution {
                label: label.clone(),
                relation_key: relation_key(&label, &entity).unwrap_or_else(|| identity.clone()),
                identity,
                name: string_property(&entity.properties, "name"),
                scope: scope.clone(),
                id,
            },
        );
        let mut properties = entity.properties;
        properties.insert("id", PropertyValue::Integer(id as i64));
        properties.insert("falkor_id", PropertyValue::Integer(falkor_id as i64));
        let candidate = ResolvedVertex {
            id,
            labels,
            properties,
            sources: vec![SourceObject {
                graph: name.clone(),
                falkor_id,
            }],
        };
        let output = outputs.entry(scope.clone()).or_insert_with(|| ResolvedScope::empty(scope));
        match output.vertices.get_mut(&id) {
            Some(existing) => {
                merge_vertex(existing, candidate);
                output.diagnostics.merged_vertices += 1;
            }
            None => {
                output.vertices.insert(id, candidate);
            }
        }
    }

    let mut edge_id_occurrences = BTreeMap::<u64, usize>::new();
    for cell in &relationships {
        *edge_id_occurrences.entry(cell.edge_id).or_default() += 1;
    }
    // A reused edge id's cells each need its properties: all but its last
    // cell copy them, and the last moves them out.
    let mut reused_cells_left: BTreeMap<u64, usize> = edge_id_occurrences
        .iter()
        .filter(|(_, &cells)| cells > 1)
        .map(|(&edge_id, &cells)| (edge_id, cells))
        .collect();
    for cell in &relationships {
        let source = node_resolutions.get(&cell.src);
        let destination = node_resolutions.get(&cell.dst);
        if source.is_none() || destination.is_none() {
            let unscopable_endpoint = unscopable.contains(&cell.src) || unscopable.contains(&cell.dst);
            let scope = source
                .or(destination)
                .map(|node| node.scope.clone())
                .or_else(|| {
                    if outputs.len() == 1 || unscopable_endpoint {
                        outputs.keys().next().cloned()
                    } else {
                        None
                    }
                })
                .ok_or_else(|| edge_error(&name, cell, "both endpoint nodes are absent and no unique graph scope exists"))?;
            let output = outputs.get_mut(&scope).expect("quarantine scope belongs to an existing output");
            let entity = edge_for_cell(&mut edges, &mut reused_cells_left, cell.edge_id);
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: if unscopable_endpoint {
                    RelationshipAnomalyDisposition::QuarantinedUnscopableEndpoint
                } else {
                    RelationshipAnomalyDisposition::QuarantinedMissingEndpoint
                },
                properties: entity.as_ref().map(|entity| entity.properties.clone()).unwrap_or_default(),
            });
            output.diagnostics.relationship_anomalies += 1;
            continue;
        }
        let source = source.expect("checked above");
        let destination = destination.expect("checked above");
        let output = outputs.get_mut(&source.scope).expect("source scope was created with its vertex");
        let entity = edge_for_cell(&mut edges, &mut reused_cells_left, cell.edge_id);
        if source.scope != destination.scope {
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::QuarantinedCrossScope,
                properties: entity.as_ref().map(|entity| entity.properties.clone()).unwrap_or_default(),
            });
            output.diagnostics.relationship_anomalies += 1;
            continue;
        }
        if !adjacency.contains(&(cell.src, cell.dst)) {
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::ImportedWithoutAdjacencyPair,
                properties: entity.as_ref().map(|entity| entity.properties.clone()).unwrap_or_default(),
            });
            output.diagnostics.relationship_anomalies += 1;
        }
        if entity.is_none() {
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::ImportedWithoutEdgeRecord,
                properties: Properties::new(),
            });
            output.diagnostics.relationship_anomalies += 1;
        }
        let falkor_id = cell.edge_id.to_string();
        let known_identity = entity
            .as_ref()
            .and_then(|entity| relationship_identity(&cell.edge_type, source, destination, entity));
        if entity.is_some() && known_identity.is_none() {
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::ImportedWithFallbackIdentity,
                properties: entity.as_ref().map(|entity| entity.properties.clone()).unwrap_or_default(),
            });
            output.diagnostics.relationship_anomalies += 1;
        }
        let writer_identity = known_identity.is_some();
        let identity = known_identity.unwrap_or_else(|| {
            ascii_json_array([
                "falkor_edge",
                cell.edge_type.as_str(),
                source.label.as_str(),
                source.identity.as_str(),
                destination.label.as_str(),
                destination.identity.as_str(),
                falkor_id.as_str(),
            ])
        });
        if edge_id_occurrences.get(&cell.edge_id).copied().unwrap_or(0) > 1 {
            output.diagnostics.reused_edge_id_relationships += 1;
            output.relationship_anomalies.push(RelationshipAnomaly {
                source: SourceObject {
                    graph: name.clone(),
                    falkor_id: cell.edge_id,
                },
                edge_type: cell.edge_type.clone(),
                src_falkor_id: cell.src,
                dst_falkor_id: cell.dst,
                disposition: RelationshipAnomalyDisposition::ImportedWithDisambiguatedIdentity,
                properties: entity.as_ref().map(|entity| entity.properties.clone()).unwrap_or_default(),
            });
            output.diagnostics.relationship_anomalies += 1;
        }
        let logical_id = stable_id(&source.scope, &format!("relationship_{}", cell.edge_type), &identity);
        let variant_identity = ascii_json_array([
            identity.as_str(),
            cell.edge_type.as_str(),
            source.label.as_str(),
            source.identity.as_str(),
            destination.label.as_str(),
            destination.identity.as_str(),
        ]);
        let variant_id = stable_id(
            &source.scope,
            &format!("relationship_{}_variant", cell.edge_type),
            &variant_identity,
        );
        let id = variant_id;
        let mut properties = entity.map(|entity| entity.properties).unwrap_or_default();
        // A writer-identity bridge gets a new id, so its bridge_id follows it
        // (canonicalize_relationship_ids). A fallback bridge keeps its own.
        let source_bridge_id = cell.edge_type == "BRIDGE" && !writer_identity && properties.contains_key("bridge_id");
        properties.insert("id", PropertyValue::Integer(id as i64));
        properties.insert("falkor_id", PropertyValue::Integer(cell.edge_id as i64));
        let candidate = ResolvedRelationship {
            id,
            logical_id,
            variant_id,
            edge_type: cell.edge_type.clone(),
            src: source.id,
            dst: destination.id,
            properties,
            sources: vec![SourceObject {
                graph: name.clone(),
                falkor_id: cell.edge_id,
            }],
            source_bridge_id,
        };
        match output.relationships.get_mut(&id) {
            Some(existing)
                if existing.edge_type == candidate.edge_type && existing.src == candidate.src && existing.dst == candidate.dst =>
            {
                merge_relationship(existing, candidate);
                output.diagnostics.merged_relationships += 1;
            }
            Some(existing) => {
                return Err(ResolveError::Edge {
                    graph: name.clone(),
                    edge: cell.edge_id,
                    message: format!(
                        "stable relationship id {id} conflicts with {} {} -> {}",
                        existing.edge_type, existing.src, existing.dst
                    ),
                })
            }
            None => {
                output.relationships.insert(id, candidate);
            }
        }
    }
    for cell in dangling_tensor_cells {
        let scope = node_resolutions
            .get(&cell.src)
            .or_else(|| node_resolutions.get(&cell.dst))
            .map(|node| node.scope.clone())
            .or_else(|| fallback_scope.clone())
            .or_else(|| outputs.keys().next().cloned())
            .ok_or_else(|| ResolveError::Edge {
                graph: name.clone(),
                edge: 0,
                message: format!(
                    "{} multi-edge cell {} -> {} has no edge list and the graph has no scope to report it in",
                    cell.edge_type, cell.src, cell.dst
                ),
            })?;
        let output = outputs.entry(scope.clone()).or_insert_with(|| ResolvedScope::empty(scope));
        output.relationship_anomalies.push(RelationshipAnomaly {
            source: SourceObject {
                graph: name.clone(),
                falkor_id: 0,
            },
            edge_type: cell.edge_type,
            src_falkor_id: cell.src,
            dst_falkor_id: cell.dst,
            disposition: RelationshipAnomalyDisposition::QuarantinedMissingMultiEdgeList,
            properties: Properties::from_iter([("cell_value".to_string(), PropertyValue::String(format!("{:#x}", cell.value)))]),
        });
        output.diagnostics.relationship_anomalies += 1;
    }
    // Every edge record a cell reached was moved out by it: the rest are orphans.
    for (edge_id, entity) in edges {
        let source = SourceObject {
            graph: name.clone(),
            falkor_id: edge_id,
        };
        let scope = fallback_scope
            .clone()
            .or_else(|| outputs.keys().next().cloned())
            .ok_or_else(|| ResolveError::Edge {
                graph: name.clone(),
                edge: edge_id,
                message: "orphan edge cannot be assigned to a report scope because the graph has no resolved vertices".into(),
            })?;
        outputs
            .entry(scope.clone())
            .or_insert_with(|| ResolvedScope::empty(scope))
            .orphan_edges
            .push(OrphanEdge {
                source,
                properties: entity.properties,
            });
    }
    let unscoped_vertices = unscopable_entities
        .into_iter()
        .map(|(id, entity)| QuarantinedVertex {
            source: SourceObject {
                graph: name.clone(),
                falkor_id: id,
            },
            labels: node_labels.remove(&id).unwrap_or_default(),
            properties: entity.properties,
        })
        .collect();
    for output in outputs.values_mut() {
        output.diagnostics.inherited_scopes = inherited;
        output.diagnostics.graph_key_scopes = graph_key;
        output.orphan_edges.sort_by(|left, right| left.source.cmp(&right.source));
        sort_relationship_anomalies(&mut output.relationship_anomalies);
        canonicalize_relationship_ids(output)?;
    }
    Ok(GraphResolution {
        scopes: outputs.into_values().collect(),
        unscoped_vertices,
    })
}

pub fn merge_scopes(inputs: impl IntoIterator<Item = ResolvedScope>) -> Result<Vec<ResolvedScope>> {
    let mut outputs = BTreeMap::<Scope, ResolvedScope>::new();
    for input in inputs {
        if input.byog {
            if outputs.contains_key(&input.scope) {
                return Err(ResolveError::Scope {
                    scope: input.scope,
                    message: "duplicate BYOG collection scope".into(),
                });
            }
            outputs.insert(input.scope.clone(), input);
            continue;
        }
        if outputs.get(&input.scope).is_some_and(|scope| scope.byog) {
            return Err(ResolveError::Scope {
                scope: input.scope,
                message: "managed data overlaps a BYOG scope".into(),
            });
        }
        let output = outputs
            .entry(input.scope.clone())
            .or_insert_with(|| ResolvedScope::empty(input.scope.clone()));
        for (id, candidate) in input.vertices {
            match output.vertices.get_mut(&id) {
                Some(existing) => {
                    merge_vertex(existing, candidate);
                    output.diagnostics.merged_vertices += 1;
                }
                None => {
                    output.vertices.insert(id, candidate);
                }
            }
        }
        for mut candidate in input.relationships.into_values() {
            if candidate.edge_type == "BRIDGE" && !candidate.source_bridge_id {
                // Generated from this fragment's id; canonicalize_relationship_ids
                // sets it again from the merged one.
                candidate.properties.remove("bridge_id");
            }
            let variant_id = candidate.variant_id;
            match output.relationships.get_mut(&variant_id) {
                Some(existing)
                    if existing.edge_type == candidate.edge_type && existing.src == candidate.src && existing.dst == candidate.dst =>
                {
                    merge_relationship(existing, candidate);
                    output.diagnostics.merged_relationships += 1;
                }
                Some(existing) => {
                    return Err(ResolveError::Scope {
                        scope: output.scope.clone(),
                        message: format!(
                            "relationship variant id {variant_id} conflicts between {} {} -> {} and {} {} -> {}",
                            existing.edge_type, existing.src, existing.dst, candidate.edge_type, candidate.src, candidate.dst
                        ),
                    })
                }
                None => {
                    output.relationships.insert(variant_id, candidate);
                }
            }
        }
        output.orphan_edges.extend(input.orphan_edges);
        output.relationship_anomalies.extend(input.relationship_anomalies);
        output.diagnostics.inherited_scopes += input.diagnostics.inherited_scopes;
        output.diagnostics.graph_key_scopes += input.diagnostics.graph_key_scopes;
        output.diagnostics.reused_edge_id_relationships += input.diagnostics.reused_edge_id_relationships;
        output.diagnostics.relationship_anomalies += input.diagnostics.relationship_anomalies;
    }
    for output in outputs.values_mut() {
        if output.byog {
            continue;
        }
        output.orphan_edges.sort_by(|left, right| left.source.cmp(&right.source));
        sort_relationship_anomalies(&mut output.relationship_anomalies);
        for vertex in output.vertices.values_mut() {
            vertex.sources.sort();
            vertex.sources.dedup();
        }
        for relationship in output.relationships.values_mut() {
            relationship.sources.sort();
            relationship.sources.dedup();
        }
        canonicalize_relationship_ids(output)?;
    }
    Ok(outputs.into_values().collect())
}

fn canonicalize_relationship_ids(scope: &mut ResolvedScope) -> Result<()> {
    let mut by_logical_id = BTreeMap::<u64, Vec<ResolvedRelationship>>::new();
    for relationship in std::mem::take(&mut scope.relationships).into_values() {
        by_logical_id.entry(relationship.logical_id).or_default().push(relationship);
    }

    let mut relationships = BTreeMap::new();
    for (logical_id, mut variants) in by_logical_id {
        variants.sort_by_key(|relationship| relationship.variant_id);
        for (index, mut relationship) in variants.into_iter().enumerate() {
            let id = if index == 0 { logical_id } else { relationship.variant_id };
            relationship.id = id;
            relationship.properties.insert("id", PropertyValue::Integer(id as i64));
            if relationship.edge_type == "BRIDGE" {
                // A relationship's id can be matched but not read on Turbolay, so the
                // bridge writers also store it as bridge_id, the key bridges are read by.
                // A fallback bridge that already has one keeps it.
                if !relationship.source_bridge_id {
                    relationship.properties.insert("bridge_id", PropertyValue::Integer(id as i64));
                }
            }
            if let Some(existing) = relationships.insert(id, relationship) {
                return Err(ResolveError::Scope {
                    scope: scope.scope.clone(),
                    message: format!(
                        "canonical relationship id {id} collides between {} {} -> {} and another relationship",
                        existing.edge_type, existing.src, existing.dst
                    ),
                });
            }
        }
    }
    scope.relationships = relationships;
    Ok(())
}

fn sort_relationship_anomalies(anomalies: &mut [RelationshipAnomaly]) {
    anomalies.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.edge_type.cmp(&right.edge_type))
            .then_with(|| left.src_falkor_id.cmp(&right.src_falkor_id))
            .then_with(|| left.dst_falkor_id.cmp(&right.dst_falkor_id))
            .then_with(|| left.disposition.cmp(&right.disposition))
    });
}

fn entity_scope(entity: &Entity, graph: &str, node: u64) -> Result<Option<Scope>> {
    let Some(tenant) = scope_component(entity, "tenant_id", graph, node)? else {
        return Ok(None);
    };
    let sub = scope_component(entity, "sub_tenant_id", graph, node)?.unwrap_or_default();
    Ok(Some(Scope {
        tenant_id: tenant,
        sub_tenant_id: sub,
    }))
}

fn scope_component(entity: &Entity, key: &str, graph: &str, node: u64) -> Result<Option<String>> {
    match entity.properties.get(key) {
        None | Some(PropertyValue::Null) => Ok(None),
        Some(PropertyValue::String(value)) if value.is_empty() => Ok(None),
        Some(PropertyValue::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(ResolveError::Node {
            graph: graph.into(),
            node,
            message: format!("{key} is not a string"),
        }),
    }
}

fn graph_key_scope(name: &str) -> Option<Scope> {
    let [tenant_id, sub_tenant_id]: [String; 2] = serde_json::from_str(name).ok()?;
    (!tenant_id.is_empty()).then_some(Scope { tenant_id, sub_tenant_id })
}

fn vertex_identity(label: &str, entity: &Entity) -> Option<String> {
    let key = match label {
        "Source" => "source_id",
        "Chunk" => "chunk_id",
        "Entity" => "entity_id",
        "Actor" => "actor_id",
        "Attachment" => "attachment_id",
        "Comment" => "comment_id",
        _ => return None,
    };
    string_property(&entity.properties, key).or_else(|| {
        if label != "Source" {
            return None;
        }
        let provider = string_property(&entity.properties, "app_provider")?;
        let external = string_property(&entity.properties, "app_external_id")?;
        // _external_source_identity: a scoped placeholder is kept apart from a
        // historical unscoped one by the JSON list (an empty scope is unscoped, as
        // the Go writer treats it).
        Some(match string_property(&entity.properties, "app_external_scope") {
            Some(scope) => ascii_json_array(["external_source", scope.as_str(), provider.as_str(), external.as_str()]),
            None => format!("{provider}:{external}"),
        })
    })
}

/// How the RELATES_TO writers name a Source: its source_id, else (a placeholder)
/// "provider:external_id", which they never qualify by the placeholder's scope.
fn relation_key(label: &str, entity: &Entity) -> Option<String> {
    if label != "Source" {
        return None;
    }
    string_property(&entity.properties, "source_id").or_else(|| {
        let provider = string_property(&entity.properties, "app_provider")?;
        let external = string_property(&entity.properties, "app_external_id")?;
        Some(format!("{provider}:{external}"))
    })
}

/// Whether a RELATES edge was written by entity name (decision-trace linking's
/// DEPENDS_ON, a trace merge's re-pointed edge): its relationship_id is then
/// md5(source name:destination name:predicate:chunk_id), with the predicate its
/// canonical relation, else its raw relation, else "RELATES". A build edge hashes
/// entity ids instead.
fn written_by_name(relationship_id: &str, source: &NodeResolution, destination: &NodeResolution, entity: &Entity) -> bool {
    let (Some(source_name), Some(destination_name)) = (&source.name, &destination.name) else {
        return false;
    };
    let predicate = string_property(&entity.properties, "canonical_relation")
        .or_else(|| string_property(&entity.properties, "raw_relation"))
        .unwrap_or_else(|| "RELATES".into());
    let chunk_id = string_property(&entity.properties, "chunk_id").unwrap_or_default();
    let digest = Md5::digest(format!("{source_name}:{destination_name}:{predicate}:{chunk_id}").as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    hex == relationship_id
}

fn relationship_identity(edge_type: &str, source: &NodeResolution, destination: &NodeResolution, entity: &Entity) -> Option<String> {
    match edge_type {
        "RELATES" => {
            let relationship_id = string_property(&entity.properties, "relationship_id")?;
            // The by-name writers key every endpoint pair apart (a name can match
            // several vertices) as relationship_id|source vertex|destination vertex.
            Some(if written_by_name(&relationship_id, source, destination, entity) {
                format!("{relationship_id}|{}|{}", source.id, destination.id)
            } else {
                relationship_id
            })
        }
        // turbolayBridgeID: the endpoint entity ids in order, then the method.
        "BRIDGE" if source.label == "Entity" && destination.label == "Entity" => {
            let method = string_property(&entity.properties, "method")?;
            let (low, high) = if destination.identity < source.identity {
                (&destination.identity, &source.identity)
            } else {
                (&source.identity, &destination.identity)
            };
            Some(format!("{low}|{high}|{method}"))
        }
        "ACTED_ON" => Some(format!(
            "{}:{}:{}",
            source.identity,
            destination.identity,
            string_property(&entity.properties, "role")?
        )),
        "RELATES_TO" => Some(format!(
            "{}:{}:{}",
            source.relation_key,
            destination.relation_key,
            string_property(&entity.properties, "relation_type").unwrap_or_default()
        )),
        "ALIAS_OF" if source.label == "Entity" && destination.label == "Entity" => Some(source.identity.clone()),
        "ALIAS_OF" | "HAS_CHUNK" | "PRESENT_IN" | "HAS_ATTACHMENT" | "HAS_COMMENT" | "AUTHORED_COMMENT" => {
            Some(format!("{}:{}", source.identity, destination.identity))
        }
        "FORCEFUL_RELATION" => Some(ascii_json_array([source.identity.as_str(), destination.identity.as_str()])),
        _ => None,
    }
}

fn string_property(properties: &Properties, key: &str) -> Option<String> {
    match properties.get(key) {
        Some(PropertyValue::String(value)) if !value.is_empty() => Some(value.clone()),
        _ => None,
    }
}

fn merge_vertex(existing: &mut ResolvedVertex, candidate: ResolvedVertex) {
    let candidate_newer = recency(&candidate.properties) > recency(&existing.properties);
    let (newer, older) = if candidate_newer {
        (candidate, existing.clone())
    } else {
        (existing.clone(), candidate)
    };
    let mut properties = newer.properties;
    for (key, value) in older.properties {
        if matches!(properties.get(&key), None | Some(PropertyValue::Null)) && !matches!(value, PropertyValue::Null) {
            properties.insert(&key, value);
        }
    }
    existing.labels.extend(newer.labels);
    existing.labels.extend(older.labels);
    existing.properties = properties;
    existing.sources.extend(newer.sources);
    existing.sources.extend(older.sources);
    existing.sources.sort();
    existing.sources.dedup();
}

fn merge_relationship(existing: &mut ResolvedRelationship, candidate: ResolvedRelationship) {
    let candidate_newer = recency(&candidate.properties) > recency(&existing.properties);
    let (newer, older) = if candidate_newer {
        (candidate, existing.clone())
    } else {
        (existing.clone(), candidate)
    };
    let mut properties = newer.properties;
    for (key, value) in older.properties {
        if matches!(properties.get(&key), None | Some(PropertyValue::Null)) && !matches!(value, PropertyValue::Null) {
            properties.insert(&key, value);
        }
    }
    existing.properties = properties;
    existing.source_bridge_id |= newer.source_bridge_id || older.source_bridge_id;
    existing.sources.extend(newer.sources);
    existing.sources.extend(older.sources);
    existing.sources.sort();
    existing.sources.dedup();
}

fn recency(properties: &Properties) -> (String, String) {
    (
        sortable_property(properties.get("updated_at")),
        sortable_property(properties.get("created_at")),
    )
}
fn sortable_property(value: Option<&PropertyValue>) -> String {
    match value {
        Some(PropertyValue::String(value)) => value.clone(),
        Some(PropertyValue::Integer(value)) => format!("{value:+020}"),
        Some(PropertyValue::Float(value)) => format!("{value:+030.9}"),
        _ => String::new(),
    }
}
fn edge_error(graph: &str, cell: &RelationshipCell, message: impl Into<String>) -> ResolveError {
    ResolveError::Edge {
        graph: graph.to_string(),
        edge: cell.edge_id,
        message: message.into(),
    }
}

/// The edge record for one of its cells, moved out of `edges` on its last
/// cell; a reused edge id's earlier cells get a copy.
fn edge_for_cell(edges: &mut BTreeMap<u64, Entity>, reused_cells_left: &mut BTreeMap<u64, usize>, edge_id: u64) -> Option<Entity> {
    match reused_cells_left.get_mut(&edge_id) {
        Some(cells) if *cells > 1 => {
            *cells -= 1;
            edges.get(&edge_id).cloned()
        }
        _ => edges.remove(&edge_id),
    }
}
