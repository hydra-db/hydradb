//! Quint Connect refinement adapter for the M3 artifact/GC contract.
//!
//! The driver keeps only the public M3 projection while exercising the real
//! graph APIs for every transition: writes mutate durable topology, builders
//! capture current state in the driver, publication/rejection goes through the
//! checked-current artifact builder, readers are owned snapshots, compaction is
//! gated by a published artifact, and matrix queries are compared with the
//! direct current oracle.

use std::sync::Arc;

use anyhow::{bail, Context};
use quint_connect::{quint_run, switch, Driver, Result, State, Step};
use serde::Deserialize;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb_graph_kernel::{EdgeMutation, GraphError, GraphShard, OwnedGraphSnapshot};

const GRAPH_PATH: &str = "graph/formal-mbt-m3";
const CELL: &str = "formal-cell";
const EDGE_TYPE: &str = "FOLLOWS";
const SRC: u64 = 1;
const DST: u64 = 2;
const TILE_SIZE: u64 = 64;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct M3State {
    epoch: i64,
    previous_epoch: i64,
    canonical_reachable: bool,
    dirty_generation: i64,
    artifact_published: bool,
    artifact_epoch: i64,
    artifact_reachable: bool,
    builder_active: bool,
    builder_base_epoch: i64,
    builder_reachable: bool,
    builder_generation: i64,
    stale_publish_rejected: bool,
    reader_active: bool,
    reader_epoch: i64,
    history_retained: bool,
    query_result: bool,
    last_action: String,
}

impl State<M3Driver> for M3State {
    fn from_driver(driver: &M3Driver) -> Result<Self> {
        Ok(Self {
            epoch: driver.epoch,
            previous_epoch: driver.previous_epoch,
            canonical_reachable: driver.canonical_reachable,
            dirty_generation: driver.dirty_generation,
            artifact_published: driver.artifact_published,
            artifact_epoch: driver.artifact_epoch,
            artifact_reachable: driver.artifact_reachable,
            builder_active: driver.builder_active,
            builder_base_epoch: driver.builder_base_epoch,
            builder_reachable: driver.builder_reachable,
            builder_generation: driver.builder_generation,
            stale_publish_rejected: driver.stale_publish_rejected,
            reader_active: driver.reader_active,
            reader_epoch: driver.reader_epoch,
            history_retained: driver.history_retained,
            query_result: driver.query_result,
            last_action: driver.last_action.clone(),
        })
    }
}

struct M3Driver {
    runtime: tokio::runtime::Runtime,
    shard: Option<Arc<GraphShard>>,
    reader: Option<OwnedGraphSnapshot>,
    reader_reachable: bool,
    write_id: u64,
    epoch: i64,
    previous_epoch: i64,
    canonical_reachable: bool,
    dirty_generation: i64,
    artifact_published: bool,
    artifact_epoch: i64,
    artifact_reachable: bool,
    builder_active: bool,
    builder_base_epoch: i64,
    builder_reachable: bool,
    builder_generation: i64,
    stale_publish_rejected: bool,
    reader_active: bool,
    reader_epoch: i64,
    history_retained: bool,
    query_result: bool,
    last_action: String,
}

impl Default for M3Driver {
    fn default() -> Self {
        Self {
            runtime: tokio::runtime::Runtime::new().expect("M3 MBT runtime"),
            shard: None,
            reader: None,
            reader_reachable: false,
            write_id: 0,
            epoch: 0,
            previous_epoch: 0,
            canonical_reachable: false,
            dirty_generation: 0,
            artifact_published: false,
            artifact_epoch: 0,
            artifact_reachable: false,
            builder_active: false,
            builder_base_epoch: 0,
            builder_reachable: false,
            builder_generation: 0,
            stale_publish_rejected: false,
            reader_active: false,
            reader_epoch: 0,
            history_retained: true,
            query_result: false,
            last_action: String::new(),
        }
    }
}

impl Driver for M3Driver {
    type State = M3State;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => { self.init()?; },
            writeCreate => { self.write_create()?; },
            writeDelete => { self.write_delete()?; },
            startArtifactBuild => { self.start_artifact_build()?; },
            publishCurrentBuild => { self.publish_current_build()?; },
            rejectStalePublish => { self.reject_stale_publish()?; },
            beginRead => { self.begin_read()?; },
            endRead => { self.end_read()?; },
            gcHistory => { self.gc_history()?; },
            queryMatrix => { self.query_matrix()?; },
        })
    }
}

impl M3Driver {
    fn shard(&self) -> Result<&Arc<GraphShard>> {
        self.shard.as_ref().context("M3 shard is not open")
    }

    fn init(&mut self) -> Result {
        self.reader = None;
        if let Some(shard) = self.shard.take() {
            self.runtime.block_on(shard.close())?;
        }

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shard = Arc::new(
            self.runtime
                .block_on(GraphShard::open_standalone_writer(GRAPH_PATH, store))?,
        );
        self.shard = Some(shard);
        let (epoch, reachable) = self.observe_current()?;
        if epoch != 0 || reachable {
            bail!("M3 init did not start from an empty graph projection");
        }

        self.reader_reachable = false;
        self.write_id = 0;
        self.epoch = 0;
        self.previous_epoch = 0;
        self.canonical_reachable = false;
        self.dirty_generation = 0;
        self.artifact_published = false;
        self.artifact_epoch = 0;
        self.artifact_reachable = false;
        self.builder_active = false;
        self.builder_base_epoch = 0;
        self.builder_reachable = false;
        self.builder_generation = 0;
        self.stale_publish_rejected = false;
        self.reader_active = false;
        self.reader_epoch = 0;
        self.history_retained = true;
        self.query_result = false;
        self.last_action = "init".to_string();
        Ok(())
    }

    fn write_create(&mut self) -> Result {
        self.begin_non_write_action("writeCreate")?;
        let mutation = self.next_mutation("create");
        let result = self.runtime.block_on(self.shard()?.write_edge(mutation))?;
        if result.already_existed {
            bail!("M3 create unexpectedly reported an existing edge");
        }
        self.refresh_after_topology_write(result.epoch, true)
    }

    fn write_delete(&mut self) -> Result {
        self.begin_non_write_action("writeDelete")?;
        let mutation = self.next_mutation("delete");
        let result = self.runtime.block_on(self.shard()?.delete_edge(mutation))?;
        if !result.deleted {
            bail!("M3 delete unexpectedly reported an absent edge");
        }
        self.refresh_after_topology_write(result.epoch, false)
    }

    fn start_artifact_build(&mut self) -> Result {
        self.begin_non_write_action("startArtifactBuild")?;
        let (epoch, reachable) = self.expect_current_projection()?;
        self.builder_active = true;
        self.builder_base_epoch = epoch;
        self.builder_reachable = reachable;
        self.builder_generation = self.dirty_generation;
        Ok(())
    }

    fn publish_current_build(&mut self) -> Result {
        self.begin_non_write_action("publishCurrentBuild")?;
        let base_epoch = u64::try_from(self.builder_base_epoch)?;
        let artifact = self.runtime.block_on(
            self.shard()?
                .build_matrix_tiles_checked_current(CELL, EDGE_TYPE, base_epoch, TILE_SIZE),
        )?;
        if artifact.base_epoch != base_epoch {
            bail!(
                "M3 artifact published at epoch {}, expected {}",
                artifact.base_epoch,
                base_epoch
            );
        }
        let expected_edges = u64::from(self.builder_reachable);
        if artifact.edge_count != expected_edges {
            bail!(
                "M3 artifact edge count {}, expected {}",
                artifact.edge_count,
                expected_edges
            );
        }
        let latest = self
            .runtime
            .block_on(
                self.shard()?
                    .latest_matrix_artifact(CELL, EDGE_TYPE, base_epoch),
            )?
            .context("M3 checked-current build did not publish a readable matrix artifact")?;
        if latest.base_epoch != base_epoch {
            bail!("M3 latest published artifact does not match the builder base epoch");
        }
        self.expect_current_projection()?;
        self.artifact_published = true;
        self.artifact_epoch = self.builder_base_epoch;
        self.artifact_reachable = self.builder_reachable;
        self.builder_active = false;
        Ok(())
    }

    fn reject_stale_publish(&mut self) -> Result {
        self.begin_non_write_action("rejectStalePublish")?;
        let stale_epoch = u64::try_from(self.builder_base_epoch)?;
        let error = self
            .runtime
            .block_on(self.shard()?.build_matrix_tiles_checked_current(
                CELL,
                EDGE_TYPE,
                stale_epoch,
                TILE_SIZE,
            ))
            .expect_err("M3 stale checked-current build must be rejected");
        if !matches!(
            error,
            GraphError::SnapshotChanged {
                operation: "build_matrix_tiles",
                ref cell_id,
                ref edge_type,
                read_epoch,
                current_epoch,
            } if cell_id == CELL
                && edge_type == EDGE_TYPE
                && read_epoch == stale_epoch
                && current_epoch == self.epoch as u64
        ) {
            bail!("M3 stale build returned unexpected error: {error}");
        }
        self.expect_current_projection()?;
        self.builder_active = false;
        self.stale_publish_rejected = true;
        Ok(())
    }

    fn begin_read(&mut self) -> Result {
        self.begin_non_write_action("beginRead")?;
        let snapshot = self.runtime.block_on(self.shard()?.owned_snapshot(CELL))?;
        if snapshot.read_epoch() != self.epoch as u64 {
            bail!(
                "M3 owned snapshot opened at epoch {}, expected {}",
                snapshot.read_epoch(),
                self.epoch
            );
        }
        let snapshot_reachable = self
            .runtime
            .block_on(snapshot.edge_exists(EDGE_TYPE, SRC, DST))?;
        if snapshot_reachable != self.canonical_reachable {
            bail!("M3 owned snapshot did not capture the current reachable projection");
        }
        self.reader_reachable = snapshot_reachable;
        self.reader = Some(snapshot);
        self.reader_active = true;
        self.reader_epoch = self.epoch;
        self.history_retained = true;
        Ok(())
    }

    fn end_read(&mut self) -> Result {
        self.begin_non_write_action("endRead")?;
        self.verify_owned_reader()?;
        self.reader = None;
        self.reader_active = false;
        Ok(())
    }

    fn gc_history(&mut self) -> Result {
        self.begin_non_write_action("gcHistory")?;
        let compact_epoch = u64::try_from(self.artifact_epoch)?;
        let result = self
            .runtime
            .block_on(
                self.shard()?
                    .delete_deltas_through_matrix(CELL, EDGE_TYPE, compact_epoch),
            )?;
        if result.compacted_through_epoch != compact_epoch {
            bail!(
                "M3 compacted through epoch {}, expected {}",
                result.compacted_through_epoch,
                compact_epoch
            );
        }
        self.expect_current_projection()?;
        self.history_retained = false;
        Ok(())
    }

    fn query_matrix(&mut self) -> Result {
        self.begin_non_write_action("queryMatrix")?;
        let read_epoch = u64::try_from(self.epoch)?;
        let (direct, matrix, current_edge) = self.runtime.block_on(async {
            let shard = self.shard()?;
            let direct = shard
                .direct_snapshot_reachable(CELL, EDGE_TYPE, &[SRC], 1, read_epoch)
                .await?;
            let matrix = shard
                .matrix_reachable(CELL, EDGE_TYPE, &[SRC], 1, read_epoch)
                .await?;
            let current_edge = shard.edge_exists(CELL, EDGE_TYPE, SRC, DST).await?;
            Ok::<_, anyhow::Error>((direct, matrix, current_edge))
        })?;
        if direct.vertices != matrix.vertices {
            bail!(
                "M3 matrix oracle mismatch: direct {:?}, matrix {:?}",
                direct.vertices,
                matrix.vertices
            );
        }
        let reachable = direct.vertices.contains(&DST);
        if reachable != current_edge || reachable != self.canonical_reachable {
            bail!("M3 query result did not match the current canonical projection");
        }
        if self.reader_active {
            self.verify_owned_reader()?;
        }
        self.query_result = reachable;
        Ok(())
    }

    fn begin_non_write_action(&mut self, action: &str) -> Result {
        self.previous_epoch = self.epoch;
        self.last_action = action.to_string();
        Ok(())
    }

    fn refresh_after_topology_write(
        &mut self,
        committed_epoch: u64,
        expected_reachable: bool,
    ) -> Result {
        let (epoch, reachable) = self.observe_current()?;
        if epoch as u64 != committed_epoch {
            bail!("M3 committed at epoch {committed_epoch}, but current epoch is {epoch}");
        }
        if reachable != expected_reachable {
            bail!("M3 durable topology projection did not match the requested write");
        }
        self.epoch = epoch;
        self.canonical_reachable = reachable;
        self.dirty_generation = epoch;
        self.history_retained = true;
        Ok(())
    }

    fn expect_current_projection(&self) -> Result<(i64, bool)> {
        let (epoch, reachable) = self.observe_current()?;
        if epoch != self.epoch || reachable != self.canonical_reachable {
            bail!(
                "M3 current projection drifted: epoch/reachable = {epoch}/{reachable}, expected {}/{}",
                self.epoch,
                self.canonical_reachable
            );
        }
        Ok((epoch, reachable))
    }

    fn observe_current(&self) -> Result<(i64, bool)> {
        let (epoch, reachable) = self.runtime.block_on(async {
            let shard = self.shard()?;
            let epoch = shard.current_epoch(CELL).await?;
            let reachable = shard.edge_exists(CELL, EDGE_TYPE, SRC, DST).await?;
            Ok::<_, anyhow::Error>((epoch, reachable))
        })?;
        Ok((i64::try_from(epoch)?, reachable))
    }

    fn verify_owned_reader(&self) -> Result {
        let Some(snapshot) = self.reader.as_ref() else {
            return Ok(());
        };
        let reachable = self
            .runtime
            .block_on(snapshot.edge_exists(EDGE_TYPE, SRC, DST))?;
        if reachable != self.reader_reachable {
            bail!("M3 owned snapshot did not retain its captured edge view");
        }
        let neighbors = self
            .runtime
            .block_on(snapshot.out_neighbors(EDGE_TYPE, SRC))?;
        let expected_neighbors = if self.reader_reachable {
            vec![DST]
        } else {
            vec![]
        };
        if neighbors != expected_neighbors {
            bail!(
                "M3 owned snapshot retained neighbors {:?}, expected {:?}",
                neighbors,
                expected_neighbors
            );
        }
        Ok(())
    }

    fn next_mutation(&mut self, kind: &str) -> EdgeMutation {
        self.write_id = self.write_id.saturating_add(1);
        EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: EDGE_TYPE.to_string(),
            src: SRC,
            dst: DST,
            idempotency_key: format!("m3-{kind}-{}", self.write_id),
        }
    }
}

#[quint_run(
    spec = "quint-models/turbolay/m3_artifact_gc.qnt",
    main = "m3_artifact_gc",
    max_samples = 24,
    max_steps = 12,
    seed = "20260718"
)]
fn m3_randomized_artifact_gc_trace_refines_quint() -> impl Driver {
    M3Driver::default()
}
