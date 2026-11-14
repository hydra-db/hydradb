//! The six `GetAuxiliaryByChunks` queries, as a real workload issues them.
//!
//! The text is kept whole rather than paraphrased on purpose. These queries
//! are the reason `IN` and undirected hops exist on this engine, and a
//! simplified stand-in can pass every check here while the real text still
//! fails: the real ones carry inline property maps that a single-condition
//! query does not have.

mod common;

use common::MemoryStorage;
use hydradb_cypher_engine::{
    CypherEngine, ListParameterMap, ParameterMap, QueryRow, QueryValue, RelationshipRecord, ScalarValue, VertexRecord,
};
const PRESENT_IN: &str = r#"MATCH (e:Entity {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
WHERE e.entity_id IN $entity_ids
MATCH (e)-[:PRESENT_IN]->(c:Chunk {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
MATCH (c)-[:HAS_CHUNK]-(s:Source {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
RETURN DISTINCT
	e.entity_id AS src_id, e.name AS src_name, e.type AS src_type,
	e.namespace AS src_namespace, e.identifier AS src_identifier,
	s.source_id AS tgt_id, s.app_external_id AS tgt_name,
	'SOURCE' AS tgt_type, s.app_provider AS tgt_provider,
	s.source_id AS tgt_source_id, s.app_provider AS tgt_app_provider,
	'' AS raw_predicate, NULL AS created_at
ORDER BY src_id, tgt_id
LIMIT $cap
"#;
const HAS_COMMENT: &str = r#"MATCH (c:Chunk {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
WHERE c.chunk_id IN $chunk_ids
MATCH (c)-[:HAS_CHUNK]-(s:Source {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
MATCH (s)-[e:HAS_COMMENT]->(cmt:Comment {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
RETURN DISTINCT
	s.source_id AS src_id, s.app_external_id AS src_name,
	'SOURCE' AS src_type, s.app_provider AS src_provider,
	s.source_id AS src_source_id, s.app_provider AS src_app_provider,
	cmt.comment_id AS tgt_id, cmt.author_display AS tgt_name,
	'COMMENT' AS tgt_type, s.app_provider AS tgt_provider,
	'' AS raw_predicate, e.created_at AS created_at
ORDER BY src_id, tgt_id
LIMIT $cap
"#;
const HAS_ATTACHMENT: &str = r#"MATCH (c:Chunk {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
WHERE c.chunk_id IN $chunk_ids
MATCH (c)-[:HAS_CHUNK]-(s:Source {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
MATCH (s)-[e:HAS_ATTACHMENT]->(a:Attachment {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
RETURN DISTINCT
	s.source_id AS src_id, s.app_external_id AS src_name,
	'SOURCE' AS src_type, s.app_provider AS src_provider,
	s.source_id AS src_source_id, s.app_provider AS src_app_provider,
	a.attachment_id AS tgt_id, a.file_name AS tgt_name,
	'ATTACHMENT' AS tgt_type, s.app_provider AS tgt_provider,
	'' AS raw_predicate, e.created_at AS created_at
ORDER BY src_id, tgt_id
LIMIT $cap
"#;
const ACTED_ON: &str = r#"MATCH (c:Chunk {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
WHERE c.chunk_id IN $chunk_ids
MATCH (c)-[:HAS_CHUNK]-(s:Source {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
MATCH (act:Actor {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})-[e:ACTED_ON]->(s)
RETURN DISTINCT
	act.actor_id AS src_id, act.email AS src_name,
	'ACTOR' AS src_type, s.app_provider AS src_provider,
	s.source_id AS tgt_id, s.app_external_id AS tgt_name,
	'SOURCE' AS tgt_type, s.app_provider AS tgt_provider,
	s.source_id AS tgt_source_id, s.app_provider AS tgt_app_provider,
	e.role AS raw_predicate, e.created_at AS created_at
ORDER BY src_id, tgt_id
LIMIT $cap
"#;
const AUTHORED_COMMENT: &str = r#"MATCH (c:Chunk {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
WHERE c.chunk_id IN $chunk_ids
MATCH (c)-[:HAS_CHUNK]-(s:Source {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
MATCH (s)-[:HAS_COMMENT]->(cmt:Comment {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
MATCH (act:Actor {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})-[e:AUTHORED_COMMENT]->(cmt)
RETURN DISTINCT
	act.actor_id AS src_id, act.email AS src_name,
	'ACTOR' AS src_type, s.app_provider AS src_provider,
	cmt.comment_id AS tgt_id, cmt.author_display AS tgt_name,
	'COMMENT' AS tgt_type, s.app_provider AS tgt_provider,
	'' AS raw_predicate, e.created_at AS created_at
ORDER BY src_id, tgt_id
LIMIT $cap
"#;
const RELATES_TO: &str = r#"MATCH (c:Chunk {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
WHERE c.chunk_id IN $chunk_ids
MATCH (c)-[:HAS_CHUNK]-(s:Source {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
MATCH (s)-[e:RELATES_TO]->(t:Source {tenant_id: $tenant_id, sub_tenant_id: $sub_tenant_id})
RETURN DISTINCT
	s.source_id AS src_id, s.app_external_id AS src_name,
	'SOURCE' AS src_type, s.app_provider AS src_provider,
	s.source_id AS src_source_id, s.app_provider AS src_app_provider,
	t.source_id AS tgt_id, t.app_external_id AS tgt_name,
	'SOURCE' AS tgt_type, t.app_provider AS tgt_provider,
	t.source_id AS tgt_source_id, t.app_provider AS tgt_app_provider,
	e.relation_type AS raw_predicate, e.created_at AS created_at
ORDER BY src_id, tgt_id
LIMIT $cap
"#;
const AUXILIARY_QUERIES: [(&str, &str); 6] = [
    ("present_in", PRESENT_IN),
    ("has_comment", HAS_COMMENT),
    ("has_attachment", HAS_ATTACHMENT),
    ("acted_on", ACTED_ON),
    ("authored_comment", AUTHORED_COMMENT),
    ("relates_to", RELATES_TO),
];

/// The scalar parameters the application binds on every auxiliary query.
/// `cap` is `auxFetchCap + 1`: the queries over-fetch by one row to detect the
/// ceiling, so 5001 rather than 5000 is the value that actually gets bound.
fn scalar_parameters() -> ParameterMap {
    ParameterMap::from([
        ("tenant_id".to_string(), ScalarValue::String("tenant-1".into())),
        ("sub_tenant_id".to_string(), ScalarValue::String("sub-1".into())),
        ("cap".to_string(), ScalarValue::Integer(5001)),
    ])
}

/// Both id lists are bound on every query even though each query reads only
/// one of them, because that is what the application does: it builds one
/// parameter map and reuses it across all six.
fn list_parameters() -> ListParameterMap {
    ListParameterMap::from([
        (
            "chunk_ids".to_string(),
            vec![ScalarValue::String("chunk-a".into()), ScalarValue::String("chunk-b".into())],
        ),
        ("entity_ids".to_string(), vec![ScalarValue::String("entity-a".into())]),
    ])
}

fn prepare(query: &str) -> hydradb_cypher_engine::PreparedQuery {
    CypherEngine::new(())
        .prepare_with_lists(query, &scalar_parameters(), &list_parameters())
        .expect("auxiliary query prepares")
}

/// The cheapest check that matters, and the one that would have caught every
/// blocker this work fixed: does the engine understand the text at all.
#[test]
fn every_auxiliary_query_prepares() {
    for (kind, query) in AUXILIARY_QUERIES {
        if let Err(error) = CypherEngine::new(()).prepare_with_lists(query, &scalar_parameters(), &list_parameters()) {
            panic!("auxiliary query {kind} failed to prepare: {error}");
        }
    }
}

/// Every query projects the same triplet shape so the service layer has one
/// decode path. Column order is part of that contract, not an accident.
#[test]
fn every_auxiliary_query_projects_the_shared_triplet_columns() {
    for (kind, query) in AUXILIARY_QUERIES {
        let columns = prepare(query).column_names().expect("projected columns");
        for required in ["src_id", "tgt_id", "raw_predicate", "created_at"] {
            assert!(
                columns.iter().any(|column| column == required),
                "auxiliary query {kind} is missing the {required} column: {columns:?}"
            );
        }
        assert_eq!(
            columns.first().map(String::as_str),
            Some("src_id"),
            "auxiliary query {kind} must project src_id first"
        );
    }
}

/// The constant and NULL columns survive planning as values. `present_in` is
/// the only query with a literal null, projected to pad the column count so all
/// six results have the same shape.
#[test]
fn constant_and_null_projections_survive_planning() {
    let columns = prepare(PRESENT_IN).column_names().expect("columns");
    assert!(columns.iter().any(|column| column == "tgt_type"));
    assert!(columns.iter().any(|column| column == "created_at"));
    assert_eq!(columns.len(), 13);
}

/// Every query reaches its Source through `-[:HAS_CHUNK]-`, written without an
/// arrow. If this hop were planned as a directed expansion the queries would
/// silently miss every edge stored the other way round.
#[test]
fn the_has_chunk_hop_is_planned_undirected() {
    for (kind, query) in AUXILIARY_QUERIES {
        let plan = prepare(query).explain();
        assert!(
            plan.contains("direction=undirected"),
            "auxiliary query {kind} lost its undirected hop:\n{plan}"
        );
    }
}

/// A deliberate record of work that was deferred, not an endorsement of it.
///
/// The `IN` is understood, but it does not yet decide what gets read: the two
/// conditions on `c` sit in separate filter layers, and only the inner one is
/// offered to the access-path choice. So the plan seeks on `tenant_id` and
/// reads the whole tenant before discarding everything outside the id list.
/// Answers are correct; the cost scales with the tenant rather than the list.
///
/// When that is fixed this test fails, which is the point — it should fail, and
/// whoever fixes it should replace it with the multi-seek assertion below.
#[test]
fn the_chunk_id_list_does_not_yet_narrow_the_access_path() {
    let plan = prepare(HAS_COMMENT).explain();
    assert!(
        plan.contains("VertexScan(provider=slatedb, access=property-point, binding=c, labels=[Chunk], property=tenant_id"),
        "expected the deferred tenant-wide seek, got:\n{plan}"
    );
    assert!(
        !plan.contains("access=property-points"),
        "the id list now drives the access path - delete this test and assert \
         a property-points access on chunk_id instead:\n{plan}"
    );
}

fn source(id: u64, source_id: &str, external: &str) -> VertexRecord {
    VertexRecord::new(id)
        .with_label("Source")
        .with_property("source_id", ScalarValue::String(source_id.into()))
        .with_property("app_external_id", ScalarValue::String(external.into()))
        .with_property("app_provider", ScalarValue::String("slack".into()))
        .with_property("tenant_id", ScalarValue::String("tenant-1".into()))
        .with_property("sub_tenant_id", ScalarValue::String("sub-1".into()))
}

fn chunk(id: u64, chunk_id: &str) -> VertexRecord {
    VertexRecord::new(id)
        .with_label("Chunk")
        .with_property("chunk_id", ScalarValue::String(chunk_id.into()))
        .with_property("tenant_id", ScalarValue::String("tenant-1".into()))
        .with_property("sub_tenant_id", ScalarValue::String("sub-1".into()))
}

fn comment(id: u64, comment_id: &str, author: &str) -> VertexRecord {
    VertexRecord::new(id)
        .with_label("Comment")
        .with_property("comment_id", ScalarValue::String(comment_id.into()))
        .with_property("author_display", ScalarValue::String(author.into()))
        .with_property("tenant_id", ScalarValue::String("tenant-1".into()))
        .with_property("sub_tenant_id", ScalarValue::String("sub-1".into()))
}

/// The whole query, end to end, against a seeded graph.
///
/// Two things make this worth more than the planning tests above. The
/// `HAS_CHUNK` edge is seeded Source -> Chunk, the opposite of the direction
/// the query reads it in, so a directed plan would return nothing. And a second
/// chunk sits outside `$chunk_ids` with its own source and comment, so an `IN`
/// that failed to filter would show up as an extra row rather than as an error.
#[tokio::test]
async fn the_has_comment_query_returns_its_rows() {
    let storage = MemoryStorage::with_graph(
        [
            chunk(1, "chunk-a"),
            source(2, "src-1", "DOC-1"),
            comment(3, "cmt-1", "Ada"),
            chunk(4, "chunk-z"),
            source(5, "src-2", "DOC-2"),
            comment(6, "cmt-2", "Grace"),
        ],
        [
            // Stored the other way round on purpose.
            RelationshipRecord::new(50, 2, 1, "HAS_CHUNK"),
            RelationshipRecord::new(51, 2, 3, "HAS_COMMENT").with_property("created_at", ScalarValue::Integer(1700)),
            RelationshipRecord::new(52, 5, 4, "HAS_CHUNK"),
            RelationshipRecord::new(53, 5, 6, "HAS_COMMENT").with_property("created_at", ScalarValue::Integer(1800)),
        ],
    );
    let engine = CypherEngine::new(storage);
    let lists = ListParameterMap::from([("chunk_ids".to_string(), vec![ScalarValue::String("chunk-a".into())])]);
    let prepared = engine
        .prepare_with_lists(HAS_COMMENT, &scalar_parameters(), &lists)
        .expect("prepare has_comment");

    let result = engine.execute_prepared(&prepared).await.expect("execute has_comment");

    let text = |value: &str| QueryValue::Scalar(ScalarValue::String(value.into()));
    assert_eq!(
        result.rows,
        vec![QueryRow {
            values: vec![
                text("src-1"),
                text("DOC-1"),
                text("SOURCE"),
                text("slack"),
                text("src-1"),
                text("slack"),
                text("cmt-1"),
                text("Ada"),
                text("COMMENT"),
                text("slack"),
                text(""),
                QueryValue::Scalar(ScalarValue::Integer(1700)),
            ],
        }],
        "the out-of-scope chunk must not contribute a row"
    );
}
