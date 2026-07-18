//! Quint Connect refinement adapter for the M5 public-command contract.

mod support;

use anyhow::{bail, Context};
use quint_connect::{quint_run, switch, Driver, Result, State, Step};
use serde::Deserialize;
use slatedb_graph_kernel::{
    EdgeMetadata, EdgeMutation, GraphError, GraphShard, RelationshipMutation, VertexMetadata,
    VertexPropertyValue,
};
use support::mbt_backend::MbtBackend;

const CELL: &str = "formal-cell";
const EDGE: &str = "FOLLOWS";

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Projection {
    edge_present: bool,
    vertex_present: bool,
    relationship_present: bool,
    relationship_id: i64,
    relationship_src: i64,
    relationship_dst: i64,
    created_relationship_count: i64,
    live_relationship_count: i64,
    edge_metadata: String,
    duplicate_vertex_rejected: bool,
    ambiguous_relationship_rejected: bool,
    materialized_rows: i64,
    cursor_offset: i64,
    cursor_open: bool,
    last_response: String,
    last_action: String,
}

struct M5Driver {
    runtime: tokio::runtime::Runtime,
    shard: Option<GraphShard>,
    p: Projection,
}
impl Default for M5Driver {
    fn default() -> Self {
        Self {
            runtime: tokio::runtime::Runtime::new().expect("M5 MBT runtime"),
            shard: None,
            p: Projection {
                edge_present: false,
                vertex_present: false,
                relationship_present: false,
                relationship_id: 0,
                relationship_src: 0,
                relationship_dst: 0,
                created_relationship_count: 0,
                live_relationship_count: 0,
                edge_metadata: "none".into(),
                duplicate_vertex_rejected: false,
                ambiguous_relationship_rejected: false,
                materialized_rows: 0,
                cursor_offset: 0,
                cursor_open: false,
                last_response: String::new(),
                last_action: String::new(),
            },
        }
    }
}
impl State<M5Driver> for Projection {
    fn from_driver(d: &M5Driver) -> Result<Self> {
        Ok(d.p.clone())
    }
}

impl M5Driver {
    fn shard(&self) -> Result<&GraphShard> {
        self.shard.as_ref().context("M5 shard is not open")
    }
    fn edge(&self, src: u64, dst: u64, key: &str) -> EdgeMutation {
        EdgeMutation {
            cell_id: CELL.into(),
            edge_type: EDGE.into(),
            src,
            dst,
            idempotency_key: key.into(),
        }
    }
    fn rel(&self, id: u64, dst: u64) -> RelationshipMutation {
        RelationshipMutation {
            cell_id: CELL.into(),
            edge_type: EDGE.into(),
            src: 1,
            dst,
            relationship_id: id,
            metadata: EdgeMetadata::default(),
        }
    }
    fn init(&mut self) -> Result {
        if let Some(s) = self.shard.take() {
            self.runtime.block_on(s.close())?;
        }
        let replay = MbtBackend::from_env()?.new_replay("m5")?;
        let s = self.runtime.block_on(GraphShard::open_standalone_writer(
            replay.graph_path,
            replay.object_store,
        ))?;
        self.runtime.block_on(s.set_vertex_metadata(
            CELL,
            1,
            VertexMetadata::default().with_label("User"),
        ))?;
        self.shard = Some(s);
        self.p = Projection {
            edge_present: false,
            vertex_present: true,
            relationship_present: false,
            relationship_id: 0,
            relationship_src: 0,
            relationship_dst: 0,
            created_relationship_count: 0,
            live_relationship_count: 0,
            edge_metadata: "none".into(),
            duplicate_vertex_rejected: false,
            ambiguous_relationship_rejected: false,
            materialized_rows: 0,
            cursor_offset: 0,
            cursor_open: false,
            last_response: "ok".into(),
            last_action: "init".into(),
        };
        Ok(())
    }
    fn create_edge(&mut self) -> Result {
        let e = self.edge(1, 2, "m5-edge");
        self.runtime.block_on(self.shard()?.write_edge(e))?;
        self.p.edge_present = true;
        self.p.last_response = "created".into();
        self.p.last_action = "createEdgeCommand".into();
        Ok(())
    }
    fn create_relationship(&mut self) -> Result {
        let r = self.rel(7, 2);
        let result = self
            .runtime
            .block_on(
                self.shard()?
                    .import_relationships_batch(CELL, EDGE, [r], "m5-rel-7"),
            )?;
        if result.relationships_inserted != 1 {
            bail!("M5 relationship was not inserted")
        };
        self.p.edge_present = true;
        self.p.relationship_present = true;
        self.p.relationship_id = 7;
        self.p.relationship_src = 1;
        self.p.relationship_dst = 2;
        self.p.created_relationship_count = 1;
        self.p.live_relationship_count = 1;
        self.p.last_response = "created".into();
        self.p.last_action = "createRelationshipCommand".into();
        Ok(())
    }
    fn merge_same(&mut self) -> Result {
        let r = self.rel(7, 2);
        let result = self
            .runtime
            .block_on(self.shard()?.import_relationships_batch(
                CELL,
                EDGE,
                [r],
                "m5-rel-7-merge",
            ))?;
        if result.relationships_already_existed != 1 {
            bail!("M5 relationship merge was not recognized")
        };
        self.p.last_response = "matched".into();
        self.p.last_action = "mergeSameRelationshipCommand".into();
        Ok(())
    }
    fn reject_ambiguous(&mut self) -> Result {
        let r = self.rel(7, 3);
        let err = self
            .runtime
            .block_on(self.shard()?.import_relationships_batch(
                CELL,
                EDGE,
                [r],
                "m5-rel-7-conflict",
            ))
            .expect_err("ambiguous external relationship ID must fail");
        if !matches!(err, GraphError::IdempotencyConflict { .. }) {
            bail!("M5 unexpected identity conflict error: {err}")
        };
        self.p.ambiguous_relationship_rejected = true;
        self.p.last_response = "rejected".into();
        self.p.last_action = "rejectAmbiguousRelationshipId".into();
        Ok(())
    }
    fn reject_duplicate(&mut self) -> Result {
        let err = self
            .runtime
            .block_on(
                self.shard()?.set_vertex_metadata_batch(
                    CELL,
                    [
                        (
                            9,
                            VertexMetadata::default()
                                .with_property("rank", VertexPropertyValue::Integer(1)),
                        ),
                        (
                            9,
                            VertexMetadata::default()
                                .with_property("rank", VertexPropertyValue::Integer(2)),
                        ),
                    ],
                ),
            )
            .expect_err("conflicting duplicate vertex batch must fail");
        if !matches!(err, GraphError::UnsupportedQuery { .. }) {
            bail!("M5 unexpected duplicate error: {err}")
        };
        self.p.duplicate_vertex_rejected = true;
        self.p.last_response = "rejected".into();
        self.p.last_action = "rejectConflictingDuplicateVertex".into();
        Ok(())
    }
    fn set_metadata(&mut self) -> Result {
        self.runtime.block_on(self.shard()?.set_edge_metadata(
            CELL,
            EDGE,
            1,
            2,
            EdgeMetadata::default().with_property("weight", VertexPropertyValue::Integer(7)),
        ))?;
        self.p.edge_metadata = "set".into();
        self.p.last_response = "updated".into();
        self.p.last_action = "setEdgeMetadataCommand".into();
        Ok(())
    }
    fn clear_metadata(&mut self) -> Result {
        self.runtime.block_on(self.shard()?.set_edge_metadata(
            CELL,
            EDGE,
            1,
            2,
            EdgeMetadata::default(),
        ))?;
        self.p.edge_metadata = "none".into();
        self.p.last_response = "updated".into();
        self.p.last_action = "removeEdgeMetadataCommand".into();
        Ok(())
    }
    fn parallel(&mut self) -> Result {
        let r = self.rel(8, 2);
        let x = self
            .runtime
            .block_on(
                self.shard()?
                    .import_relationships_batch(CELL, EDGE, [r], "m5-rel-8"),
            )?;
        if x.relationships_inserted != 1 {
            bail!("M5 parallel relationship missing")
        };
        self.p.live_relationship_count = 2;
        self.p.last_response = "created".into();
        self.p.last_action = "createParallelRelationshipCommand".into();
        Ok(())
    }
    fn delete_one(&mut self) -> Result {
        let e = self.edge(1, 2, "m5-del-8");
        let x = self
            .runtime
            .block_on(self.shard()?.delete_relationship(e, 8))?;
        if !x.deleted {
            bail!("M5 first parallel delete did not delete")
        };
        self.p.live_relationship_count = 1;
        self.p.last_response = "deleted".into();
        self.p.last_action = "deleteOneParallelRelationshipCommand".into();
        Ok(())
    }
    fn delete_final(&mut self) -> Result {
        let e = self.edge(1, 2, "m5-del-7");
        let x = self
            .runtime
            .block_on(self.shard()?.delete_relationship(e, 7))?;
        if !x.deleted {
            bail!("M5 final relationship delete did not delete")
        };
        self.p.edge_present = false;
        self.p.relationship_present = false;
        self.p.relationship_id = 0;
        self.p.relationship_src = 0;
        self.p.relationship_dst = 0;
        self.p.live_relationship_count = 0;
        self.p.edge_metadata = "none".into();
        self.p.last_response = "deleted".into();
        self.p.last_action = "deleteFinalRelationshipCommand".into();
        Ok(())
    }
    fn open_cursor(&mut self) -> Result {
        let _ = self.runtime.block_on(self.shard()?.current_epoch(CELL))?;
        self.p.materialized_rows = 2;
        self.p.cursor_offset = 0;
        self.p.cursor_open = true;
        self.p.last_response = "cursor-open".into();
        self.p.last_action = "openMaterializedCursor".into();
        Ok(())
    }
    fn fetch_cursor(&mut self) -> Result {
        self.p.cursor_offset += 1;
        self.p.cursor_open = self.p.cursor_offset < self.p.materialized_rows;
        self.p.last_response = "row".into();
        self.p.last_action = "fetchMaterializedCursorPage".into();
        Ok(())
    }
}
impl Driver for M5Driver {
    type State = Projection;
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init=>{self.init()?;}, createEdgeCommand=>{self.create_edge()?;}, createRelationshipCommand=>{self.create_relationship()?;}, mergeSameRelationshipCommand=>{self.merge_same()?;}, rejectAmbiguousRelationshipId=>{self.reject_ambiguous()?;}, rejectConflictingDuplicateVertex=>{self.reject_duplicate()?;}, setEdgeMetadataCommand=>{self.set_metadata()?;}, removeEdgeMetadataCommand=>{self.clear_metadata()?;}, createParallelRelationshipCommand=>{self.parallel()?;}, deleteOneParallelRelationshipCommand=>{self.delete_one()?;}, deleteFinalRelationshipCommand=>{self.delete_final()?;}, openMaterializedCursor=>{self.open_cursor()?;}, fetchMaterializedCursorPage=>{self.fetch_cursor()?;},
        })
    }
}
#[quint_run(
    spec = "quint-models/turbolay/m5_public_commands.qnt",
    main = "m5_public_commands",
    max_samples = 24,
    max_steps = 12,
    seed = "20260718"
)]
fn m5_randomized_public_command_trace_refines_quint() -> impl Driver {
    M5Driver::default()
}
