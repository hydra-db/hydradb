//! A pattern is anchored on the binding an earlier pattern already produced,
//! not on whichever node the caller wrote first.
//!
//! `NaturalJoin` runs correlated: the left row seeds the right side, and a scan
//! whose binding the seed already holds returns that row instead of scanning.
//! So the two spellings of one hop cost wildly different things. Written away
//! from the bound end, `(s)<-[:HAS_CHUNK]-(c:Chunk)` seeks one Source and walks
//! its edges. Written toward it, `(c:Chunk)-[:HAS_CHUNK]->(s)` scans every
//! Chunk in the cell, hydrates each one, expands it, and throws away all but
//! the row the seed already held.
//!
//! Staging ran the second spelling for days: `graph.fetch_source_chunks`
//! materialised 8,661 rows to return 1, spending 30.2s of 30.4s in storage.
//! Both spellings are the same Cypher, so which one the caller typed must not
//! decide the cost.

mod common;

use std::collections::BTreeSet;

use common::MemoryStorage;
use hydradb_cypher_engine::{
    CypherEngine, ExpandDirection, ParameterMap, QueryRow, QueryValue, RelationshipRecord, ScalarValue, VertexRecord,
};

const SOURCE_WITH_CHUNKS: u64 = 1;
const SOURCE_WITHOUT_CHUNKS: u64 = 2;
const TAG_ENTITY: u64 = 3;

fn source(id: u64, source_id: &str) -> VertexRecord {
    VertexRecord::new(id)
        .with_label("Source")
        .with_property("source_id", ScalarValue::String(source_id.into()))
        // Negative on purpose, so a test can write it as a signed literal.
        .with_property("rank", ScalarValue::Integer(-1))
}

/// `owner` repeats, as a plain property, the Source the chunk links to. It
/// exists so a pattern can write an inline property that reads the far end of
/// its own hop.
fn chunk(id: u64, chunk_id: &str, owner: &str) -> VertexRecord {
    VertexRecord::new(id)
        .with_label("Chunk")
        .with_property("chunk_id", ScalarValue::String(chunk_id.into()))
        .with_property("owner", ScalarValue::String(owner.into()))
        // Repeated by the HAS_CHUNK edge as `kind`, so a pattern can write an
        // inline property that reads its own hop's relationship.
        .with_property("tier", ScalarValue::String("primary".into()))
}

/// Two Sources and eight Chunks, of which only two belong to `src-1`. The
/// other six exist so that anchoring on `Chunk` is visible as a scan rather
/// than inferred from the plan alone.
fn fixture() -> MemoryStorage {
    let mut vertices = vec![
        source(SOURCE_WITH_CHUNKS, "src-1"),
        source(SOURCE_WITHOUT_CHUNKS, "src-2"),
        // `tag` repeats the Source's own id, so an optional hop to it yields a
        // binding a later inline map can read without changing any answer.
        VertexRecord::new(TAG_ENTITY)
            .with_label("Entity")
            .with_property("tag", ScalarValue::String("src-1".into()))
            // A denominator, so a test can write an inline map that divides.
            .with_property("zero", ScalarValue::Integer(0)),
    ];
    // Only c-10 and c-11 own an edge, and `owner` agrees with it; the other
    // six name the other Source so the property is not a constant.
    vertices.extend((10..18).map(|id| {
        let owner = if id < 12 { "src-1" } else { "src-2" };
        chunk(id, &format!("c-{id}"), owner)
    }));
    MemoryStorage::with_graph(
        vertices,
        [
            // Stored direction is Chunk -> Source, as the pattern writes it.
            // `kind` agrees with the chunk's `tier` on both edges.
            RelationshipRecord::new(100, 10, SOURCE_WITH_CHUNKS, "HAS_CHUNK").with_property("kind", ScalarValue::String("primary".into())),
            RelationshipRecord::new(101, 11, SOURCE_WITH_CHUNKS, "HAS_CHUNK").with_property("kind", ScalarValue::String("primary".into())),
            RelationshipRecord::new(200, SOURCE_WITH_CHUNKS, TAG_ENTITY, "TAGGED"),
        ],
    )
}

fn parameters(source_id: &str) -> ParameterMap {
    ParameterMap::from([("source_id".to_string(), ScalarValue::String(source_id.into()))])
}

/// The chunk ids a result carries, deduplicated. The fixture's storage returns
/// duplicated and reversed records on purpose, so *which* chunks matched is a
/// question about the set. How many rows carry them is a separate question the
/// callers assert on directly: the engine collapses the stub's duplicate, and
/// a change in row multiplicity should fail rather than hide inside the set.
fn chunk_ids(rows: &[QueryRow]) -> BTreeSet<String> {
    rows.iter()
        .filter_map(|row| match row.values.as_slice() {
            [_, QueryValue::Scalar(ScalarValue::String(chunk_id))] => Some(chunk_id.to_string()),
            _ => None,
        })
        .collect()
}

fn null_chunk_rows(rows: &[QueryRow]) -> usize {
    rows.iter()
        .filter(|row| matches!(row.values.as_slice(), [_, QueryValue::Null]))
        .count()
}

const OPTIONAL_TOWARD_BOUND_END: &str = "MATCH (s:Source {source_id: $source_id}) \
     OPTIONAL MATCH (c:Chunk)-[:HAS_CHUNK]->(s) \
     RETURN s.source_id AS source_id, c.chunk_id AS chunk_id";

const OPTIONAL_FROM_BOUND_END: &str = "MATCH (s:Source {source_id: $source_id}) \
     OPTIONAL MATCH (s)<-[:HAS_CHUNK]-(c:Chunk) \
     RETURN s.source_id AS source_id, c.chunk_id AS chunk_id";

#[tokio::test]
async fn a_hop_written_toward_its_bound_end_anchors_on_that_end() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    let prepared = engine.prepare(OPTIONAL_TOWARD_BOUND_END, &parameters("src-1")).expect("prepare");

    let explain = prepared.explain();
    assert!(explain.contains("ExpandExec(from=s,"), "the bound end anchors the hop: {explain}");
    assert!(explain.contains("direction=incoming"), "{explain}");
    assert!(explain.contains("to=c,"), "{explain}");
    assert!(explain.contains("target_labels=[Chunk]"), "{explain}");
    assert!(!explain.contains("access=label-prefix"), "no Chunk scan survives: {explain}");

    let result = engine.execute_prepared(&prepared).await.expect("execute anchored hop");
    assert_eq!(chunk_ids(&result.rows), BTreeSet::from(["c-10".to_string(), "c-11".to_string()]));
    assert_eq!(
        result.rows.len(),
        2,
        "one row per matching chunk, duplicates collapsed: {:?}",
        result.rows
    );

    // The plan shape alone would not prove this: an unbound scan under a
    // correlated join is skipped by the seed, so the claim worth pinning is
    // that no scan was ever issued.
    assert!(
        storage.label_scan_limits.lock().unwrap().is_empty(),
        "anchoring on the bound end must issue no label scan"
    );
    let expansions = storage.expand_requests.lock().unwrap();
    assert!(!expansions.is_empty(), "the hop still expands");
    for request in expansions.iter() {
        assert_eq!(request.direction, ExpandDirection::Incoming);
        assert!(
            request.input_vertex_ids.iter().all(|id| *id == SOURCE_WITH_CHUNKS),
            "expansion walks the seeded Source, not every Chunk: {:?}",
            request.input_vertex_ids
        );
    }
}

#[tokio::test]
async fn both_spellings_of_one_hop_plan_and_answer_alike() {
    let engine = CypherEngine::new(fixture());
    let toward = engine
        .prepare(OPTIONAL_TOWARD_BOUND_END, &parameters("src-1"))
        .expect("prepare toward");
    let from = engine.prepare(OPTIONAL_FROM_BOUND_END, &parameters("src-1")).expect("prepare from");
    assert_eq!(
        toward.explain(),
        from.explain(),
        "the rewrite lands on the plan the other spelling already had"
    );

    let toward_rows = engine.execute_prepared(&toward).await.expect("execute");
    let from_rows = engine.execute_prepared(&from).await.expect("execute");
    assert_eq!(chunk_ids(&toward_rows.rows), chunk_ids(&from_rows.rows));
    assert_eq!(
        toward_rows.rows.len(),
        from_rows.rows.len(),
        "the rewrite must not change how many rows a spelling returns"
    );
}

#[tokio::test]
async fn an_optional_hop_that_matches_nothing_still_returns_its_row() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    let prepared = engine.prepare(OPTIONAL_TOWARD_BOUND_END, &parameters("src-2")).expect("prepare");

    let result = engine.execute_prepared(&prepared).await.expect("execute optional miss");
    assert!(chunk_ids(&result.rows).is_empty(), "src-2 owns no chunks: {:?}", result.rows);
    assert_eq!(
        null_chunk_rows(&result.rows),
        1,
        "the left row survives exactly once, with a null chunk: {:?}",
        result.rows
    );
    assert_eq!(
        result.rows.len(),
        1,
        "a miss null-pads the one left row rather than multiplying it: {:?}",
        result.rows
    );
}

#[tokio::test]
async fn a_required_clause_is_anchored_the_same_way() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             MATCH (c:Chunk)-[:HAS_CHUNK]->(s) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(explain.contains("ExpandExec(from=s,"), "{explain}");
    assert!(explain.contains("direction=incoming"), "{explain}");
    assert!(!explain.contains("access=label-prefix"), "{explain}");

    let result = engine.execute_prepared(&prepared).await.expect("execute");
    assert_eq!(chunk_ids(&result.rows), BTreeSet::from(["c-10".to_string(), "c-11".to_string()]));
    assert_eq!(result.rows.len(), 2, "{:?}", result.rows);
    assert!(storage.label_scan_limits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_second_pattern_in_one_clause_sees_the_first_pattern_bindings() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}), (c:Chunk)-[:HAS_CHUNK]->(s) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(explain.contains("ExpandExec(from=s,"), "{explain}");
    assert!(!explain.contains("access=label-prefix"), "{explain}");
    assert!(storage.label_scan_limits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_hop_with_neither_end_bound_keeps_the_order_it_was_written_in() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    let prepared = engine
        .prepare(
            "MATCH (c:Chunk)-[:HAS_CHUNK]->(s:Source) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &ParameterMap::new(),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("VertexScan(provider=slatedb, access=label-prefix, binding=c"),
        "nothing is bound, so the written anchor stands: {explain}"
    );
    assert!(explain.contains("ExpandExec(from=c,"), "{explain}");
    assert!(explain.contains("direction=outgoing"), "{explain}");

    engine.execute_prepared(&prepared).await.expect("execute");
    assert!(
        !storage.label_scan_limits.lock().unwrap().is_empty(),
        "this shape genuinely scans, which is what makes the anchored case meaningful"
    );
}

#[tokio::test]
async fn a_hop_already_anchored_on_its_bound_end_is_left_alone() {
    let engine = CypherEngine::new(fixture());
    let prepared = engine
        .prepare(
            "MATCH (c:Chunk {chunk_id: $chunk_id}) \
             OPTIONAL MATCH (c)-[:HAS_CHUNK]->(s:Source) \
             RETURN c.chunk_id AS chunk_id, s.source_id AS source_id",
            &ParameterMap::from([("chunk_id".to_string(), ScalarValue::String("c-10".into()))]),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("ExpandExec(from=c,"),
        "the bound end is already the anchor: {explain}"
    );
    assert!(explain.contains("direction=outgoing"), "{explain}");
}

#[tokio::test]
async fn an_undirected_hop_keeps_meaning_both_orientations() {
    let engine = CypherEngine::new(fixture());
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             OPTIONAL MATCH (c:Chunk)-[:HAS_CHUNK]-(s) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(explain.contains("ExpandExec(from=s,"), "{explain}");
    assert!(
        explain.contains("direction=undirected"),
        "reversing an undirected hop must not commit it to one orientation: {explain}"
    );
}

#[tokio::test]
async fn a_variable_length_hop_is_left_alone() {
    let engine = CypherEngine::new(fixture());
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             OPTIONAL MATCH (c:Chunk)-[:HAS_CHUNK*1..2]->(s) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("VariableExpandExec(from=c,"),
        "variable-length hops are out of scope for the rewrite: {explain}"
    );
    assert!(
        explain.contains("VertexScan(provider=slatedb, access=label-prefix, binding=c"),
        "{explain}"
    );
}

#[tokio::test]
async fn a_multi_hop_pattern_is_left_alone() {
    let engine = CypherEngine::new(fixture());
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             OPTIONAL MATCH (e:Entity)-[:PRESENT_IN]->(c:Chunk)-[:HAS_CHUNK]->(s) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("VertexScan(provider=slatedb, access=label-prefix, binding=e"),
        "more than one hop is out of scope for the rewrite: {explain}"
    );
}

#[tokio::test]
async fn a_binding_an_optional_clause_might_produce_anchors_and_short_circuits() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    // `s` is bound by an OPTIONAL clause, so a row whose optional side missed
    // carries it as null rather than not at all. That is what makes anchoring
    // on it safe: a pattern on a null name matches nothing, so the reversed
    // anchor answers from the seed on a hit and short-circuits to no rows on a
    // miss. Neither case scans, which is why an optional name may anchor.
    let prepared = engine
        .prepare(
            "MATCH (root:Chunk {chunk_id: $chunk_id}) \
             OPTIONAL MATCH (root)-[:HAS_CHUNK]->(s:Source) \
             OPTIONAL MATCH (c:Chunk)-[:HAS_CHUNK]->(s) \
             RETURN root.chunk_id AS chunk_id, c.chunk_id AS other",
            &ParameterMap::from([("chunk_id".to_string(), ScalarValue::String("c-12".into()))]),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("ExpandExec(from=s,"),
        "an optional name anchors like any other: {explain}"
    );
    assert!(
        !explain.contains("VertexScan(provider=slatedb, access=label-prefix, binding=c"),
        "the Chunk scan is exactly what anchoring removes: {explain}"
    );

    // c-12 owns no HAS_CHUNK edge, so the middle clause misses and `s` is
    // null. The third clause is optional, so its row survives null-padded --
    // and the anchor it reversed onto never scanned to get there.
    let result = engine.execute_prepared(&prepared).await.expect("execute chained optional");
    assert_eq!(result.rows.len(), 1, "{:?}", result.rows);
    assert_eq!(null_chunk_rows(&result.rows), 1, "{:?}", result.rows);
    assert!(
        storage.label_scan_limits.lock().unwrap().is_empty(),
        "a missed optional must short-circuit, not scan"
    );
}

#[tokio::test]
async fn a_required_clause_reusing_a_missed_optional_binding_returns_no_rows() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    // The same shape with a *required* third clause. An OPTIONAL that matched
    // nothing binds `s` to null, and a required pattern on a null name matches
    // nothing, so the row is eliminated and the query answers empty.
    //
    // Before a miss was recorded as null it was simply absent, and this
    // returned the two chunks of an unrelated Source: the third clause scanned
    // for `s`, bound whatever it found, and nothing contradicted it.
    let prepared = engine
        .prepare(
            "MATCH (root:Chunk {chunk_id: $chunk_id}) \
             OPTIONAL MATCH (root)-[:HAS_CHUNK]->(s:Source) \
             MATCH (c:Chunk)-[:HAS_CHUNK]->(s) \
             RETURN root.chunk_id AS chunk_id, c.chunk_id AS other",
            &ParameterMap::from([("chunk_id".to_string(), ScalarValue::String("c-12".into()))]),
        )
        .expect("prepare");

    let result = engine
        .execute_prepared(&prepared)
        .await
        .expect("execute required reuse of a missed optional");
    assert!(
        result.rows.is_empty(),
        "a required match on a null binding matches nothing: {:?}",
        result.rows
    );
    assert!(
        storage.label_scan_limits.lock().unwrap().is_empty(),
        "and it reaches that answer without scanning"
    );
}

#[tokio::test]
async fn a_required_clause_reusing_a_matched_optional_binding_keeps_its_rows() {
    let engine = CypherEngine::new(fixture());
    // The mirror: c-10 owns an edge, so the optional hits, `s` is a real
    // Source, and the required clause finds both of its chunks. Eliminating
    // the miss must not eliminate the hit.
    let prepared = engine
        .prepare(
            "MATCH (root:Chunk {chunk_id: $chunk_id}) \
             OPTIONAL MATCH (root)-[:HAS_CHUNK]->(s:Source) \
             MATCH (c:Chunk)-[:HAS_CHUNK]->(s) \
             RETURN root.chunk_id AS chunk_id, c.chunk_id AS other",
            &ParameterMap::from([("chunk_id".to_string(), ScalarValue::String("c-10".into()))]),
        )
        .expect("prepare");

    let result = engine
        .execute_prepared(&prepared)
        .await
        .expect("execute required reuse of a matched optional");
    assert_eq!(
        chunk_ids(&result.rows),
        BTreeSet::from(["c-10".to_string(), "c-11".to_string()]),
        "{:?}",
        result.rows
    );
    assert_eq!(result.rows.len(), 2, "{:?}", result.rows);
}

#[tokio::test]
async fn a_required_clause_still_promotes_its_bindings_to_later_clauses() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    // The mirror of the test above: a required middle clause does bind `s` for
    // certain, so the third clause anchors on it as usual.
    let prepared = engine
        .prepare(
            "MATCH (root:Chunk {chunk_id: $chunk_id}) \
             MATCH (root)-[:HAS_CHUNK]->(s:Source) \
             OPTIONAL MATCH (c:Chunk)-[:HAS_CHUNK]->(s) \
             RETURN root.chunk_id AS chunk_id, c.chunk_id AS other",
            &ParameterMap::from([("chunk_id".to_string(), ScalarValue::String("c-10".into()))]),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(explain.contains("ExpandExec(from=s,"), "{explain}");
    assert!(
        !explain.contains("VertexScan(provider=slatedb, access=label-prefix, binding=c"),
        "a certain binding anchors the hop: {explain}"
    );
}

#[tokio::test]
async fn an_inline_property_reading_the_far_end_keeps_the_written_order() {
    let engine = CypherEngine::new(fixture());
    // `{source_id: c.owner}` sits on the bound end but reads `c`, which this
    // pattern has yet to bind. Reversed, that map would become a filter over
    // the opening scan of `s`, a hop before `c` exists: it would read null and
    // drop both matching chunks. The written order applies it after the
    // expansion, so the rewrite has to decline.
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             MATCH (c:Chunk)-[:HAS_CHUNK]->(s {source_id: c.owner}) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("ExpandExec(from=c,"),
        "the anchor stays where it was written: {explain}"
    );
    assert!(
        !explain.contains("ExpandExec(from=s,"),
        "reversing would run the inline filter a hop early: {explain}"
    );

    let result = engine
        .execute_prepared(&prepared)
        .await
        .expect("execute cross-referencing inline property");
    assert_eq!(
        chunk_ids(&result.rows),
        BTreeSet::from(["c-10".to_string(), "c-11".to_string()]),
        "both chunks agree with their owner and must survive: {:?}",
        result.rows
    );
    assert_eq!(result.rows.len(), 2, "{:?}", result.rows);
}

#[tokio::test]
async fn an_inline_property_reading_only_bound_names_still_anchors() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    // The mirror of the test above, so the guard is no wider than its reason:
    // this map reads a parameter, which is substituted before the plan runs
    // and so is answerable at the anchor. The rewrite still applies.
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             MATCH (c:Chunk)-[:HAS_CHUNK]->(s {source_id: $source_id}) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("ExpandExec(from=s,"),
        "a map that reads nothing unbound must not block the rewrite: {explain}"
    );
    assert!(explain.contains("direction=incoming"), "{explain}");

    let result = engine
        .execute_prepared(&prepared)
        .await
        .expect("execute parameter-only inline property");
    assert_eq!(
        chunk_ids(&result.rows),
        BTreeSet::from(["c-10".to_string(), "c-11".to_string()]),
        "{:?}",
        result.rows
    );
    assert_eq!(result.rows.len(), 2, "{:?}", result.rows);
    assert!(
        storage.label_scan_limits.lock().unwrap().is_empty(),
        "anchoring on the bound end must issue no label scan"
    );
}

#[tokio::test]
async fn a_source_map_reading_the_hop_relationship_keeps_the_written_order() {
    let engine = CypherEngine::new(fixture());
    // The mirror of the guard above, in the other direction. `{tier: r.kind}`
    // sits on the far end, whose map the reversal moves *later* -- past the
    // expansion that binds `r`. Written, it compares against an absent `r` and
    // matches nothing; reversed, `r` is bound and both chunks would qualify.
    // A rewrite that is meant to change only cost must not turn no rows into
    // two.
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             MATCH (c:Chunk {tier: r.kind})-[r:HAS_CHUNK]->(s) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("ExpandExec(from=c,"),
        "the anchor stays where it was written: {explain}"
    );
    assert!(
        !explain.contains("ExpandExec(from=s,"),
        "reversing would run the far end's map after the hop bound `r`: {explain}"
    );

    let result = engine
        .execute_prepared(&prepared)
        .await
        .expect("execute relationship-reading inline property");
    assert!(
        result.rows.is_empty(),
        "`r` is unbound where this map is written, so nothing matches: {:?}",
        result.rows
    );
}

#[tokio::test]
async fn a_map_reading_an_earlier_optional_binding_still_anchors() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    // `o` comes from an earlier OPTIONAL clause, so it is not in `bound`. It
    // is still safe to read: this hop neither produces nor consumes it, and
    // the seed row carries it -- or does not -- identically on both sides of
    // the move. Only what the hop itself produces may block the rewrite.
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             OPTIONAL MATCH (s)-[:TAGGED]->(o:Entity) \
             MATCH (c:Chunk)-[:HAS_CHUNK]->(s {source_id: o.tag}) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("ExpandExec(from=s,"),
        "an earlier clause's name must not block the rewrite: {explain}"
    );
    assert!(
        !explain.contains("VertexScan(provider=slatedb, access=label-prefix, binding=c"),
        "the Chunk scan is exactly what anchoring removes: {explain}"
    );

    let result = engine
        .execute_prepared(&prepared)
        .await
        .expect("execute optional-reading inline property");
    assert_eq!(
        chunk_ids(&result.rows),
        BTreeSet::from(["c-10".to_string(), "c-11".to_string()]),
        "{:?}",
        result.rows
    );
    assert_eq!(result.rows.len(), 2, "{:?}", result.rows);
}

#[tokio::test]
async fn two_patterns_do_not_share_a_synthesized_relationship_binding() {
    let engine = CypherEngine::new(fixture());
    // Neither hop names its relationship, so both synthesize one. Numbered per
    // pattern they would collide, and a row cannot hold one name for two
    // edges: the join would force `c` and `d` to be the same chunk and return
    // the diagonal instead of the pairs.
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             MATCH (s)<-[:HAS_CHUNK {kind: 'primary'}]-(c:Chunk) \
             MATCH (s)<-[:HAS_CHUNK {kind: 'primary'}]-(d:Chunk) \
             RETURN c.chunk_id AS first, d.chunk_id AS second",
            &parameters("src-1"),
        )
        .expect("prepare");

    let result = engine.execute_prepared(&prepared).await.expect("execute");
    assert_eq!(
        result.rows.len(),
        4,
        "two independent hops over two chunks pair every way: {:?}",
        result.rows
    );
}

#[tokio::test]
async fn a_missed_optional_does_not_null_a_later_patterns_relationship() {
    let engine = CypherEngine::new(fixture());
    // The optional hop names no relationship and misses, so its synthesized
    // binding is marked null. The required hop below synthesizes one too, and
    // if the two shared a name the null would eliminate every row it matched.
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             OPTIONAL MATCH (s)-[:MISSING {kind: 'x'}]->(m) \
             MATCH (s)<-[:HAS_CHUNK {kind: 'primary'}]-(c:Chunk) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let result = engine
        .execute_prepared(&prepared)
        .await
        .expect("execute after a missed optional hop");
    assert_eq!(
        chunk_ids(&result.rows),
        BTreeSet::from(["c-10".to_string(), "c-11".to_string()]),
        "a miss elsewhere must not eliminate these: {:?}",
        result.rows
    );
    assert_eq!(result.rows.len(), 2, "{:?}", result.rows);
}

#[tokio::test]
async fn a_map_that_can_error_is_not_moved() {
    let engine = CypherEngine::new(fixture());
    // `1 / o.zero` divides by zero. Written, the map is applied after an
    // expansion that matches nothing, so it never runs and the query answers
    // empty. Reversed it would be a filter over the opening scan and would
    // raise. Moving a map changes when it is evaluated, so one that can fail
    // is not moved at all.
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             OPTIONAL MATCH (s)-[:TAGGED]->(o:Entity) \
             MATCH (c:Chunk)-[:NONEXISTENT]->(s {rank: 1 / o.zero}) \
             RETURN c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("ExpandExec(from=c,"),
        "a map that can fail keeps the order it was written in: {explain}"
    );

    let result = engine
        .execute_prepared(&prepared)
        .await
        .expect("a map the written order never evaluates must not raise");
    assert!(result.rows.is_empty(), "{:?}", result.rows);
}

#[tokio::test]
async fn a_relationship_an_earlier_pattern_bound_does_not_block_anchoring() {
    let engine = CypherEngine::new(fixture());
    // `r` is bound by the first pattern, so the second one's map reads it from
    // the seed row whichever side of the expansion it runs on. Only a name
    // *this* hop produces can read differently after the move, so a name
    // already bound must not be mistaken for one.
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id})<-[r:HAS_CHUNK]-(x:Chunk) \
             MATCH (c:Chunk {chunk_id: r.kind})-[r:HAS_CHUNK]->(s) \
             RETURN c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("ExpandExec(from=s, relationship=r, types=[HAS_CHUNK], direction=incoming, to=c,"),
        "an already-bound relationship must not block the rewrite: {explain}"
    );
    assert!(
        !explain.contains("VertexScan(provider=slatedb, access=property-presence, binding=c"),
        "the Chunk scan is exactly what anchoring removes: {explain}"
    );
}

#[tokio::test]
async fn a_variable_spelled_like_a_synthesized_one_is_refused() {
    let engine = CypherEngine::new(fixture());
    // A synthesized relationship name shares the row with the caller's own
    // variables, and nothing stops the caller writing one: it is a legal
    // identifier. Left alone, the node below would be seeded with a
    // relationship of the same name, join with nothing, and the query would
    // answer empty -- silently, and only when the pattern happens to
    // synthesize that name. Refusing it says so instead.
    let error = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             MATCH (s)<-[:HAS_CHUNK {kind: 'primary'}]-(c:Chunk) \
             MATCH (__hydradb_inline_relationship_0:Chunk) \
             RETURN c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect_err("a reserved variable name must not prepare");
    assert!(
        error.to_string().contains("__hydradb_"),
        "the message has to name the prefix it is refusing: {error}"
    );
}

#[tokio::test]
async fn a_signed_literal_still_anchors() {
    let storage = fixture();
    let engine = CypherEngine::new(storage.clone());
    // `-1` parses as a sign on a literal rather than as a literal, so a rule
    // that asks only whether a map value is a leaf would refuse to move it and
    // keep the broad Chunk scan. A sign on a literal is a constant: the
    // literal is in range by the time it parses, so negating it cannot fail.
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             MATCH (c:Chunk)-[:HAS_CHUNK]->(s {rank: -1}) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("ExpandExec(from=s,"),
        "a constant map value must not block the rewrite: {explain}"
    );
    assert!(
        !explain.contains("VertexScan(provider=slatedb, access=label-prefix, binding=c"),
        "the Chunk scan is exactly what anchoring removes: {explain}"
    );

    let result = engine.execute_prepared(&prepared).await.expect("execute");
    assert_eq!(
        chunk_ids(&result.rows),
        BTreeSet::from(["c-10".to_string(), "c-11".to_string()]),
        "{:?}",
        result.rows
    );
}

#[tokio::test]
async fn a_reserved_looking_name_that_cannot_collide_is_allowed() {
    let engine = CypherEngine::new(fixture());
    // Only a name the lowering can actually synthesize is refused. The prefix
    // alone is not enough: no lowering produces `__hydradb_customer`, so it
    // can never arrive twice on one row, and a query already using it must
    // keep working.
    engine
        .prepare(
            "MATCH (__hydradb_customer:Chunk) \
             RETURN __hydradb_customer.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("a name no lowering can synthesize must prepare");
}

#[tokio::test]
async fn a_sign_on_a_non_numeric_literal_is_not_moved() {
    let engine = CypherEngine::new(fixture());
    // `-true` is a sign on a literal but not a constant: the executor takes a
    // sign on an integer and nothing else, so evaluating it raises. Written,
    // the map is applied after an expansion that matches nothing and never
    // runs. Reversed it would be a filter over the opening scan and would
    // raise, so only a signed *integer* counts as safe to move.
    let prepared = engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             MATCH (c:Chunk)-[:NONEXISTENT]->(s {rank: -true}) \
             RETURN s.source_id AS source_id, c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("prepare");

    let explain = prepared.explain();
    assert!(
        explain.contains("ExpandExec(from=c,"),
        "a sign on a non-numeric literal keeps the written order: {explain}"
    );

    let result = engine
        .execute_prepared(&prepared)
        .await
        .expect("a map the written order never evaluates must not raise");
    assert!(result.rows.is_empty(), "{:?}", result.rows);
}

#[tokio::test]
async fn a_synthesized_looking_name_prepares_when_nothing_is_synthesized() {
    let engine = CypherEngine::new(fixture());
    // No relationship here carries inline properties, so the lowering names
    // nothing and there is nothing for this variable to collide with. What
    // decides the refusal is the name the query actually produced, not the
    // shape the caller happened to spell.
    engine
        .prepare(
            "MATCH (__hydradb_inline_relationship_0:Chunk) \
             RETURN __hydradb_inline_relationship_0.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("nothing was synthesized, so nothing collides");
}

#[tokio::test]
async fn an_index_beyond_what_was_synthesized_prepares() {
    let engine = CypherEngine::new(fixture());
    // One inline-property hop synthesizes `_0` and stops. `_1` is free, and a
    // rule reading the shape rather than the names would refuse it anyway.
    engine
        .prepare(
            "MATCH (s:Source {source_id: $source_id}) \
             MATCH (s)<-[:HAS_CHUNK {kind: 'primary'}]-(c:Chunk) \
             MATCH (__hydradb_inline_relationship_1:Chunk) \
             RETURN c.chunk_id AS chunk_id",
            &parameters("src-1"),
        )
        .expect("only `_0` was synthesized, so `_1` is free");
}
