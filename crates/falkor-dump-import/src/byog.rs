//! BYOG is an arbitrary property graph, not the managed ingestion schema.
//! Scope comes only from its physical collection key. Source record IDs are
//! kept within that scope; no business-key merging or property stamping occurs.
use std::collections::BTreeSet;

use falkordb_rdb::GraphDump;

use crate::{GraphResolution, ResolveError, ResolvedRelationship, ResolvedScope, ResolvedVertex, Result, Scope, SourceObject};

/// Matches the Go runner's SplitN(byog key, ':', 2) / ScopedDatabaseName route.
/// Database tenants are minted IDs; collections may themselves contain ':'.
pub fn byog_scope(name: &str) -> Result<Scope> {
    let invalid = || ResolveError::Node {
        graph: name.into(),
        node: 0,
        message: "expected BYOG graph key byog:<database-tenant>:<collection> with nonempty, control-free components".into(),
    };
    let (tenant, collection) = name.strip_prefix("byog:").and_then(|key| key.split_once(':')).ok_or_else(invalid)?;
    if tenant.trim().is_empty() || collection.trim().is_empty() || name.chars().any(char::is_control) {
        return Err(invalid());
    }
    Ok(Scope {
        tenant_id: tenant.into(),
        sub_tenant_id: collection.into(),
    })
}

/// A BYOG import fails closed on missing/ambiguous source records. A collection
/// cannot be silently repaired, deduplicated or partly quarantined: arbitrary
/// customer data has no managed writer semantics from which to reconstruct it.
pub fn resolve_byog_graph(mut graph: GraphDump) -> Result<GraphResolution> {
    let scope = byog_scope(&graph.name)?;
    let fail = |message: &str| ResolveError::Scope {
        scope: scope.clone(),
        message: message.into(),
    };
    if graph
        .node_schemas
        .iter()
        .chain(&graph.edge_schemas)
        .any(|schema| schema.index_count != 0 || schema.constraint_count != 0)
    {
        return Err(fail(
            "BYOG source declares indexes or constraints; their DDL semantics require an explicit migration before import",
        ));
    }
    if !graph.dangling_tensor_cells.is_empty() || graph.adjacency_pairs_without_relationships() != 0 {
        return Err(fail("BYOG topology has unresolved relationship cells or adjacency pairs"));
    }
    if graph.nodes.values().chain(graph.edges.values()).any(|record| {
        record
            .properties
            .iter()
            .any(|(_, value)| matches!(value, falkordb_rdb::PropertyValue::Null))
    }) {
        return Err(fail(
            "BYOG contains explicit null property records that Turbolay cannot store losslessly",
        ));
    }
    let mut seen = BTreeSet::new();
    for cell in &graph.relationships {
        if cell.edge_id > i64::MAX as u64 || !seen.insert(cell.edge_id) {
            return Err(fail("BYOG relationship ID is duplicated or exceeds the signed Cypher ID range"));
        }
        if !graph.nodes.contains_key(&cell.src) || !graph.nodes.contains_key(&cell.dst) || !graph.edges.contains_key(&cell.edge_id) {
            return Err(fail("BYOG relationship is missing an endpoint or property record"));
        }
        if !graph.adjacency.contains(&(cell.src, cell.dst)) {
            return Err(fail("BYOG relationship is missing its adjacency pair"));
        }
    }
    if seen.len() != graph.edges.len() {
        return Err(fail("BYOG contains edge property records without a relationship"));
    }
    if graph.nodes.iter().any(|(&id, entity)| id > i64::MAX as u64 || id != entity.id)
        || graph.edges.iter().any(|(&id, entity)| id != entity.id)
        || graph.labels.keys().any(|id| !graph.nodes.contains_key(id))
    {
        return Err(fail("BYOG source record IDs or labels are inconsistent"));
    }
    let mut output = ResolvedScope::empty(scope);
    output.byog = true;
    for (id, node) in graph.nodes {
        output.vertices.insert(
            id,
            ResolvedVertex {
                id,
                labels: graph.labels.remove(&id).unwrap_or_default(),
                properties: node.properties,
                sources: vec![SourceObject {
                    graph: graph.name.clone(),
                    falkor_id: id,
                }],
            },
        );
    }
    for cell in graph.relationships {
        let id = cell.edge_id;
        output.relationships.insert(
            id,
            ResolvedRelationship {
                id,
                logical_id: id,
                variant_id: id,
                edge_type: cell.edge_type,
                src: cell.src,
                dst: cell.dst,
                properties: graph.edges.remove(&id).expect("validated property record").properties,
                sources: vec![SourceObject {
                    graph: graph.name.clone(),
                    falkor_id: id,
                }],
                source_bridge_id: false,
            },
        );
    }
    Ok(GraphResolution {
        scopes: vec![output],
        unscoped_vertices: Vec::new(),
    })
}
