//! Rust model-based tests driven by executable Quint traces.
//!
//! The state below deliberately mirrors the public, durable write projection in
//! `m1_cell_write.qnt`.  Every non-crash action reads the real graph state after
//! executing its public API call; a crash preserves the last observed durable
//! projection until its successor opens the same S3-compatible store.

mod support;

use std::sync::Arc;

use anyhow::{bail, Context};
use quint_connect::{quint_run, switch, Driver, Result, State, Step};
use serde::Deserialize;
use slatedb::object_store::ObjectStore;
use slatedb_graph_kernel::{CommitResult, EdgeMutation, GraphError, GraphShard};
use support::mbt_backend::MbtBackend;
const CELL: &str = "formal-cell";
const EDGE_TYPE: &str = "FOLLOWS";

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct M1State {
    epoch: i64,
    previous_epoch: i64,
    edge_present: bool,
    out_degree: i64,
    delta_epoch: i64,
    create_recorded: bool,
    delete_recorded: bool,
    create_outcome_epoch: i64,
    delete_outcome_epoch: i64,
    unknown_reply: bool,
    active_writer: i64,
    writer1_live: bool,
    writer2_live: bool,
    last_action: String,
}

impl State<M1Driver> for M1State {
    fn from_driver(driver: &M1Driver) -> Result<Self> {
        Ok(Self {
            epoch: driver.last_epoch,
            previous_epoch: driver.previous_epoch,
            edge_present: driver.last_edge_present,
            out_degree: driver.last_out_degree,
            delta_epoch: driver.delta_epoch,
            create_recorded: driver.create_recorded,
            delete_recorded: driver.delete_recorded,
            create_outcome_epoch: driver.create_outcome_epoch,
            delete_outcome_epoch: driver.delete_outcome_epoch,
            unknown_reply: driver.unknown_reply,
            active_writer: driver.active_writer,
            writer1_live: driver.writer1_live,
            writer2_live: driver.writer2_live,
            last_action: driver.last_action.clone(),
        })
    }
}

struct M1Driver {
    runtime: tokio::runtime::Runtime,
    store: Option<Arc<dyn ObjectStore>>,
    graph_path: String,
    writer: Option<GraphShard>,
    // The closed shard is retained solely to prove that its next write is
    // rejected after a replacement writer has acquired the storage.
    zombie_writer: Option<GraphShard>,
    last_epoch: i64,
    previous_epoch: i64,
    last_edge_present: bool,
    last_out_degree: i64,
    delta_epoch: i64,
    create_recorded: bool,
    delete_recorded: bool,
    create_outcome_epoch: i64,
    delete_outcome_epoch: i64,
    unknown_reply: bool,
    active_writer: i64,
    writer1_live: bool,
    writer2_live: bool,
    last_action: String,
}

impl Default for M1Driver {
    fn default() -> Self {
        Self {
            runtime: tokio::runtime::Runtime::new().expect("model-based test runtime"),
            store: None,
            graph_path: String::new(),
            writer: None,
            zombie_writer: None,
            last_epoch: 0,
            previous_epoch: 0,
            last_edge_present: false,
            last_out_degree: 0,
            delta_epoch: 0,
            create_recorded: false,
            delete_recorded: false,
            create_outcome_epoch: 0,
            delete_outcome_epoch: 0,
            unknown_reply: false,
            active_writer: 0,
            writer1_live: false,
            writer2_live: false,
            last_action: String::new(),
        }
    }
}

impl Driver for M1Driver {
    type State = M1State;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => { self.init()?; },
            openWriter1 => { self.open_writer1()?; },
            createEdge => { self.create_edge(false)?; },
            commitThenLoseReply => { self.create_edge(true)?; },
            retryCreate => { self.retry_create()?; },
            rejectConflictingRetry => { self.reject_conflicting_retry()?; },
            deleteEdge => { self.delete_edge()?; },
            retryDelete => { self.retry_delete()?; },
            crashWriter1 => { self.crash_writer1()?; },
            takeOverWriter2 => { self.take_over_writer2()?; },
            rejectZombieWrite => { self.reject_zombie_write()?; },
        })
    }
}

impl M1Driver {
    fn init(&mut self) -> Result {
        if let Some(writer) = self.writer.take() {
            let _ = self.runtime.block_on(writer.close());
        }
        if let Some(writer) = self.zombie_writer.take() {
            let _ = self.runtime.block_on(writer.close());
        }

        let replay = MbtBackend::from_env()?.new_replay("m1")?;
        self.store = Some(replay.object_store);
        self.graph_path = replay.graph_path;
        self.last_epoch = 0;
        self.previous_epoch = 0;
        self.last_edge_present = false;
        self.last_out_degree = 0;
        self.delta_epoch = 0;
        self.create_recorded = false;
        self.delete_recorded = false;
        self.create_outcome_epoch = 0;
        self.delete_outcome_epoch = 0;
        self.unknown_reply = false;
        self.active_writer = 0;
        self.writer1_live = false;
        self.writer2_live = false;
        self.last_action = "init".to_string();
        Ok(())
    }

    fn open_writer1(&mut self) -> Result {
        self.begin_action("openWriter1");
        self.open_writer()?;
        self.active_writer = 1;
        self.writer1_live = true;
        self.refresh_projection()
    }

    fn create_edge(&mut self, lose_reply: bool) -> Result {
        self.begin_action(if lose_reply {
            "commitThenLoseReply"
        } else {
            "createEdge"
        });
        let result = self.write(self.create_mutation())?;
        if result.already_existed {
            bail!("first M1 create unexpectedly reported an existing edge");
        }
        self.create_recorded = true;
        self.create_outcome_epoch = i64::try_from(result.epoch)?;
        self.unknown_reply = lose_reply;
        self.refresh_projection()
    }

    fn retry_create(&mut self) -> Result {
        self.begin_action("retryCreate");
        let result = self.write(self.create_mutation())?;
        if result.epoch != self.create_outcome_epoch as u64 {
            bail!("M1 create retry did not return its original idempotent outcome");
        }
        self.unknown_reply = false;
        self.refresh_projection()
    }

    fn reject_conflicting_retry(&mut self) -> Result {
        self.begin_action("rejectConflictingRetry");
        let conflicting = EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: EDGE_TYPE.to_string(),
            src: 1,
            dst: 3,
            idempotency_key: "m1-create".to_string(),
        };
        let writer = self.writer()?;
        match self.runtime.block_on(writer.write_edge(conflicting)) {
            Err(GraphError::IdempotencyConflict { .. }) => {}
            Ok(result) => bail!(
                "M1 conflicting idempotency retry committed at epoch {}",
                result.epoch
            ),
            Err(error) => return Err(error.into()),
        }
        self.refresh_projection()
    }

    fn delete_edge(&mut self) -> Result {
        self.begin_action("deleteEdge");
        let writer = self.writer()?;
        let result = self
            .runtime
            .block_on(writer.delete_edge(self.delete_mutation()))?;
        if !result.deleted {
            bail!("first M1 delete unexpectedly reported an absent edge");
        }
        self.delete_recorded = true;
        self.delete_outcome_epoch = i64::try_from(result.epoch)?;
        self.unknown_reply = false;
        self.refresh_projection()
    }

    fn retry_delete(&mut self) -> Result {
        self.begin_action("retryDelete");
        let writer = self.writer()?;
        let result = self
            .runtime
            .block_on(writer.delete_edge(self.delete_mutation()))?;
        if !result.deleted || result.epoch != self.delete_outcome_epoch as u64 {
            bail!("M1 delete retry did not return its original idempotent outcome");
        }
        self.unknown_reply = false;
        self.refresh_projection()
    }

    fn crash_writer1(&mut self) -> Result {
        self.begin_action("crashWriter1");
        let writer = self.writer.take().context("M1 writer 1 is not open")?;
        self.runtime.block_on(writer.close())?;
        self.zombie_writer = Some(writer);
        self.active_writer = 0;
        self.writer1_live = false;
        Ok(())
    }

    fn take_over_writer2(&mut self) -> Result {
        self.begin_action("takeOverWriter2");
        self.open_writer()?;
        self.active_writer = 2;
        self.writer1_live = false;
        self.writer2_live = true;
        self.refresh_projection()
    }

    fn reject_zombie_write(&mut self) -> Result {
        self.begin_action("rejectZombieWrite");
        let zombie = self
            .zombie_writer
            .as_ref()
            .context("M1 has no former writer to fence")?;
        let result = self.runtime.block_on(zombie.write_edge(EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: EDGE_TYPE.to_string(),
            src: 9,
            dst: 10,
            idempotency_key: "m1-zombie".to_string(),
        }));
        if result.is_ok() {
            bail!("M1 former writer committed after replacement writer takeover");
        }
        self.refresh_projection()
    }

    fn begin_action(&mut self, action: &str) {
        self.previous_epoch = self.last_epoch;
        self.last_action = action.to_string();
    }

    fn open_writer(&mut self) -> Result {
        let store = Arc::clone(self.store.as_ref().context("M1 store is not initialized")?);
        self.writer = Some(self.runtime.block_on(GraphShard::open_standalone_writer(
            self.graph_path.as_str(),
            store,
        ))?);
        Ok(())
    }

    fn writer(&self) -> Result<&GraphShard> {
        self.writer.as_ref().context("M1 active writer is not open")
    }

    fn write(&self, mutation: EdgeMutation) -> Result<CommitResult> {
        Ok(self.runtime.block_on(self.writer()?.write_edge(mutation))?)
    }

    fn refresh_projection(&mut self) -> Result {
        let writer = self.writer()?;
        let (epoch, edge_present, out_degree) = self.runtime.block_on(async {
            Ok::<_, GraphError>((
                writer.current_epoch(CELL).await?,
                writer.edge_exists(CELL, EDGE_TYPE, 1, 2).await?,
                writer.out_degree(CELL, EDGE_TYPE, 1).await?,
            ))
        })?;
        self.last_epoch = i64::try_from(epoch)?;
        self.last_edge_present = edge_present;
        self.last_out_degree = i64::try_from(out_degree)?;
        self.delta_epoch = self.last_epoch;
        Ok(())
    }

    fn create_mutation(&self) -> EdgeMutation {
        EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: EDGE_TYPE.to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "m1-create".to_string(),
        }
    }

    fn delete_mutation(&self) -> EdgeMutation {
        EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: EDGE_TYPE.to_string(),
            src: 1,
            dst: 2,
            idempotency_key: "m1-delete".to_string(),
        }
    }
}

#[quint_run(
    spec = "quint-models/turbolay/m1_cell_write.qnt",
    main = "m1_cell_write",
    max_samples = 24,
    max_steps = 10,
    seed = "20260718"
)]
fn m1_randomized_public_write_trace_refines_quint() -> impl Driver {
    M1Driver::default()
}
