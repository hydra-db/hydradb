//! Deterministic payload-copy budgets for projections and quantified traversal.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use common::MemoryStorage;
use hydradb_cypher_engine::{CypherEngine, ParameterMap, QueryValue, RelationshipRecord, ScalarValue, VertexRecord};

const PAYLOAD_BYTES: usize = 4093;

thread_local! {
    static PAYLOAD_COPIES: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingAllocator;

fn count_payload(size: usize) {
    if size == PAYLOAD_BYTES {
        let _ = PAYLOAD_COPIES.try_with(|copies| {
            if let Some(count) = copies.get() {
                copies.set(Some(count + 1));
            }
        });
    }
}

// Forward every allocation unchanged; count only the fixture's string size.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_payload(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_payload(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count_payload(size);
        unsafe { System.realloc(pointer, layout, size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct PayloadCopies;

impl PayloadCopies {
    fn start() -> Self {
        PAYLOAD_COPIES.with(|copies| assert!(copies.replace(Some(0)).is_none()));
        Self
    }

    fn finish(self) -> usize {
        PAYLOAD_COPIES.with(|copies| copies.take().unwrap())
    }
}

impl Drop for PayloadCopies {
    fn drop(&mut self) {
        PAYLOAD_COPIES.with(|copies| copies.set(None));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_projections_do_not_copy_payloads_for_empty_fixed_frames() {
    let mut measured = Vec::new();
    for depth in [0, 1, 2, 4, 8, 16] {
        let node = VertexRecord::new(1)
            .with_label("Payload")
            .with_property("payload", ScalarValue::String("x".repeat(PAYLOAD_BYTES).into()));
        let engine = CypherEngine::new(MemoryStorage::with_graph([node], []));
        let query = format!("MATCH (n:Payload) {} RETURN n", "WITH n ".repeat(depth));
        let prepared = engine.prepare(&query, &ParameterMap::new()).unwrap();
        let copies = PayloadCopies::start();
        let result = engine.execute_prepared(&prepared).await.unwrap();
        let copies = copies.finish();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values, [QueryValue::VertexId(1)]);
        eprintln!("projections={depth} payload_copies={copies}");
        measured.push((depth, copies));
    }
    for (depth, copies) in measured {
        assert!(
            copies <= depth + 1,
            "{depth} ordinary projections copied the node payload {copies} times"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn constrained_iteration_copies_node_payloads_linearly_in_depth() {
    let mut measured = Vec::new();
    for capture in ["anonymous", "groups", "path"] {
        for depth in [32_u64, 64, 128, 192] {
            let vertices = (0..=depth).map(|id| {
                VertexRecord::new(id)
                    .with_label("Chain")
                    .with_property("k", ScalarValue::Integer(i128::from(id)))
                    .with_property("payload", ScalarValue::String("x".repeat(PAYLOAD_BYTES).into()))
            });
            let edges = (0..depth).map(|id| RelationshipRecord::new(id, id, id + 1, "R"));
            let engine = CypherEngine::new(MemoryStorage::with_graph(vertices, edges));
            let query = match capture {
                "anonymous" => format!("MATCH (start:Chain {{k: 0}}) ((:Chain)-[:R]->(:Chain)){{{depth}}} (last) RETURN last.k AS k"),
                "groups" => format!("MATCH (start:Chain {{k: 0}}) ((a:Chain)-[e:R]->(b:Chain)){{{depth}}} (last) RETURN last.k AS k, b"),
                _ => format!("MATCH p = (start:Chain {{k: 0}}) ((a:Chain)-[e:R]->(b:Chain)){{{depth}}} (last) RETURN last.k AS k, p"),
            };
            let prepared = engine.prepare(&query, &ParameterMap::new()).unwrap();
            let copies = PayloadCopies::start();
            let result = engine.execute_prepared(&prepared).await.unwrap();
            let copies = copies.finish();
            assert_eq!(result.rows.len(), 1);
            assert_eq!(
                result.rows[0].values[0],
                QueryValue::Scalar(ScalarValue::Integer(i128::from(depth)))
            );
            if capture == "groups" {
                assert_eq!(
                    result.rows[0].values[1],
                    QueryValue::List((1..=depth).map(QueryValue::VertexId).collect())
                );
            }
            if capture == "path" {
                let QueryValue::Path(path) = &result.rows[0].values[1] else {
                    panic!("expected a path")
                };
                assert_eq!(path.nodes.iter().map(|n| n.id).collect::<Vec<_>>(), (0..=depth).collect::<Vec<_>>());
                assert_eq!(
                    path.relationships.iter().map(|r| r.id).collect::<Vec<_>>(),
                    (0..depth).collect::<Vec<_>>()
                );
                for node in &path.nodes {
                    assert_eq!(node.properties["payload"], ScalarValue::String("x".repeat(PAYLOAD_BYTES).into()));
                }
            }
            eprintln!("capture={capture} depth={depth} payload_bytes={PAYLOAD_BYTES} allocations={copies}");
            measured.push((capture, depth, copies));
        }
    }
    for (capture, depth, copies) in measured {
        assert!(
            copies <= 64 * (depth as usize + 1),
            "capture={capture} depth={depth}: {copies} property payload copies exceed a linear budget"
        );
    }
}
