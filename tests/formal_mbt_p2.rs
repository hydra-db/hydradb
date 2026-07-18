//! Public-API Quint Connect adapters for the P2 contracts.
//!
//! These drivers deliberately retain only normalized public observations. They
//! do not inspect private object-store keys: the API result/error, snapshot
//! rows, and structural degrees are the refinement oracle after each action.

mod support;

use anyhow::{bail, Context};
use quint_connect::{quint_run, switch, Driver, Result, State, Step};
use serde::Deserialize;
use slatedb_graph_kernel::{EdgeMutation, GraphError, GraphShard, VertexMetadata};
use support::mbt_backend::MbtBackend;

const CELL: &str = "formal-cell";
const EDGE_TYPE: &str = "FOLLOWS";

fn edge(edge_type: &str, src: u64, dst: u64, idempotency_key: &str) -> EdgeMutation {
    EdgeMutation {
        cell_id: CELL.to_string(),
        edge_type: edge_type.to_string(),
        src,
        dst,
        idempotency_key: idempotency_key.to_string(),
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct M5bState {
    vertex_present: bool,
    incident_edge_count: i64,
    vertex_deleted: bool,
    cell_dropped: bool,
    delete_rejected: bool,
    post_drop_write_rejected: bool,
    last_action: String,
}

impl State<M5bDriver> for M5bState {
    fn from_driver(driver: &M5bDriver) -> Result<Self> {
        Ok(Self {
            vertex_present: driver.vertex_present,
            incident_edge_count: driver.incident_edge_count,
            vertex_deleted: driver.vertex_deleted,
            cell_dropped: driver.cell_dropped,
            delete_rejected: driver.delete_rejected,
            post_drop_write_rejected: driver.post_drop_write_rejected,
            last_action: driver.last_action.clone(),
        })
    }
}

struct M5bDriver {
    runtime: tokio::runtime::Runtime,
    shard: Option<GraphShard>,
    vertex_present: bool,
    incident_edge_count: i64,
    vertex_deleted: bool,
    cell_dropped: bool,
    delete_rejected: bool,
    post_drop_write_rejected: bool,
    last_action: String,
}

impl Default for M5bDriver {
    fn default() -> Self {
        Self {
            runtime: tokio::runtime::Runtime::new().expect("P2 MBT runtime"),
            shard: None,
            vertex_present: false,
            incident_edge_count: 0,
            vertex_deleted: false,
            cell_dropped: false,
            delete_rejected: false,
            post_drop_write_rejected: false,
            last_action: String::new(),
        }
    }
}

impl Driver for M5bDriver {
    type State = M5bState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => { self.init()?; },
            rejectDeleteWithIncidentEdges => { self.reject_delete()?; },
            detachDeleteVertex => { self.detach_delete()?; },
            dropCell => { self.drop_cell()?; },
            rejectWriteAfterDrop => { self.reject_post_drop_write()?; },
        })
    }
}

impl M5bDriver {
    fn shard(&self) -> Result<&GraphShard> {
        self.shard.as_ref().context("M5b shard is not open")
    }

    fn init(&mut self) -> Result {
        if let Some(shard) = self.shard.take() {
            self.runtime.block_on(shard.close())?;
        }
        let replay = MbtBackend::from_env()?.new_replay("p2-m5b")?;
        let shard = self.runtime.block_on(GraphShard::open_standalone_writer(
            replay.graph_path,
            replay.object_store,
        ))?;
        self.runtime.block_on(async {
            shard
                .set_vertex_metadata(CELL, 1, VertexMetadata::default().with_label("User"))
                .await?;
            shard.write_edge(edge(EDGE_TYPE, 1, 2, "m5b-out")).await?;
            shard.write_edge(edge("LIKES", 3, 1, "m5b-in")).await?;
            Ok::<_, GraphError>(())
        })?;
        self.shard = Some(shard);
        self.vertex_present = true;
        self.incident_edge_count = 2;
        self.vertex_deleted = false;
        self.cell_dropped = false;
        self.delete_rejected = false;
        self.post_drop_write_rejected = false;
        self.last_action = "init".to_string();
        self.refresh_incident_projection()?;
        Ok(())
    }

    fn reject_delete(&mut self) -> Result {
        let error = self
            .runtime
            .block_on(self.shard()?.delete_vertex(CELL, 1, "m5b-delete"))
            .expect_err("plain delete with incident edges must fail");
        if !matches!(
            error,
            GraphError::UnsupportedQuery { ref dialect, ref feature }
                if *dialect == "Graph" && feature.contains("requires DETACH")
        ) {
            bail!("M5b plain delete returned unexpected error: {error}");
        }
        self.refresh_incident_projection()?;
        self.delete_rejected = true;
        self.last_action = "rejectDeleteWithIncidentEdges".to_string();
        Ok(())
    }

    fn detach_delete(&mut self) -> Result {
        let result = self.runtime.block_on(self.shard()?.detach_delete_vertex(
            CELL,
            1,
            "m5b-detach-delete",
        ))?;
        if !result.vertex_deleted || result.incident_edges_deleted != 2 {
            bail!("M5b detach delete did not remove the expected incident graph state");
        }
        self.vertex_present = false;
        self.vertex_deleted = true;
        self.delete_rejected = false;
        self.post_drop_write_rejected = false;
        self.last_action = "detachDeleteVertex".to_string();
        self.refresh_incident_projection()?;
        Ok(())
    }

    fn drop_cell(&mut self) -> Result {
        let result = self
            .runtime
            .block_on(self.shard()?.drop_cell(CELL, "m5b-drop"))?;
        if result.already_dropped {
            bail!("M5b first drop unexpectedly reported an existing drop marker");
        }
        self.vertex_present = false;
        self.incident_edge_count = 0;
        self.cell_dropped = true;
        self.delete_rejected = false;
        self.post_drop_write_rejected = false;
        self.last_action = "dropCell".to_string();
        Ok(())
    }

    fn reject_post_drop_write(&mut self) -> Result {
        let error = self
            .runtime
            .block_on(
                self.shard()?
                    .write_edge(edge(EDGE_TYPE, 1, 4, "m5b-post-drop")),
            )
            .expect_err("post-drop write must be fenced");
        if !matches!(
            error,
            GraphError::CellDropped { operation: "write_edge", ref cell_id } if cell_id == CELL
        ) {
            bail!("M5b post-drop write returned unexpected error: {error}");
        }
        self.post_drop_write_rejected = true;
        self.last_action = "rejectWriteAfterDrop".to_string();
        Ok(())
    }

    fn refresh_incident_projection(&mut self) -> Result {
        let shard = self.shard()?;
        let count = self.runtime.block_on(async {
            Ok::<_, GraphError>(
                shard.out_degree(CELL, EDGE_TYPE, 1).await?
                    + shard.out_degree(CELL, "LIKES", 3).await?,
            )
        })?;
        self.incident_edge_count = i64::try_from(count)?;
        Ok(())
    }
}

#[quint_run(
    spec = "quint-models/turbolay/m5_destructive_lifecycle.qnt",
    main = "m5_destructive_lifecycle",
    max_samples = 24,
    max_steps = 8,
    seed = "20260718"
)]
fn m5b_randomized_destructive_trace_refines_quint() -> impl Driver {
    M5bDriver::default()
}

#[cfg(feature = "opencypher")]
mod snapshot_lifecycle {
    use super::*;
    use slatedb_graph_kernel::{QueryCancellationToken, QueryContext};

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct M2bState {
        current_epoch: i64,
        snapshot_open: bool,
        snapshot_epoch: i64,
        future_rejected: bool,
        historical_rejected: bool,
        cursor_open: bool,
        cursor_cancelled: bool,
        page_returned: bool,
        last_action: String,
    }

    impl State<M2bDriver> for M2bState {
        fn from_driver(driver: &M2bDriver) -> Result<Self> {
            Ok(Self {
                current_epoch: driver.current_epoch,
                snapshot_open: driver.snapshot_open,
                snapshot_epoch: driver.snapshot_epoch,
                future_rejected: driver.future_rejected,
                historical_rejected: driver.historical_rejected,
                cursor_open: driver.cursor_open,
                cursor_cancelled: driver.cursor_cancelled,
                page_returned: driver.page_returned,
                last_action: driver.last_action.clone(),
            })
        }
    }

    struct M2bDriver {
        runtime: tokio::runtime::Runtime,
        shard: Option<GraphShard>,
        current_epoch: i64,
        snapshot_open: bool,
        snapshot_epoch: i64,
        future_rejected: bool,
        historical_rejected: bool,
        cursor_open: bool,
        cursor_cancelled: bool,
        page_returned: bool,
        last_action: String,
    }

    impl Default for M2bDriver {
        fn default() -> Self {
            Self {
                runtime: tokio::runtime::Runtime::new().expect("P2 MBT runtime"),
                shard: None,
                current_epoch: 0,
                snapshot_open: false,
                snapshot_epoch: 0,
                future_rejected: false,
                historical_rejected: false,
                cursor_open: false,
                cursor_cancelled: false,
                page_returned: false,
                last_action: String::new(),
            }
        }
    }

    impl Driver for M2bDriver {
        type State = M2bState;

        fn step(&mut self, step: &Step) -> Result {
            switch!(step {
                init => { self.init()?; },
                openCurrentSnapshot => { self.open_current_snapshot()?; },
                rejectFutureSnapshot => { self.reject_future_snapshot()?; },
                rejectHistoricalSnapshot => { self.reject_historical_snapshot()?; },
                openCursor => { self.open_cursor()?; },
                cancelCursor => { self.cancel_cursor(); },
                rejectFetchAfterCancellation => { self.reject_cancelled_page()?; },
                fetchCursorPage => { self.fetch_page()?; },
            })
        }
    }

    impl M2bDriver {
        fn shard(&self) -> Result<&GraphShard> {
            self.shard.as_ref().context("M2b shard is not open")
        }

        fn init(&mut self) -> Result {
            if let Some(shard) = self.shard.take() {
                self.runtime.block_on(shard.close())?;
            }
            let replay = MbtBackend::from_env()?.new_replay("p2-m2b")?;
            let shard = self.runtime.block_on(GraphShard::open_standalone_writer(
                replay.graph_path,
                replay.object_store,
            ))?;
            self.runtime
                .block_on(shard.write_edge(edge(EDGE_TYPE, 1, 2, "m2b-seed")))?;
            self.current_epoch = i64::try_from(self.runtime.block_on(shard.current_epoch(CELL))?)?;
            if self.current_epoch != 1 {
                bail!("M2b setup did not create exactly one graph epoch");
            }
            self.shard = Some(shard);
            self.snapshot_open = false;
            self.snapshot_epoch = 0;
            self.future_rejected = false;
            self.historical_rejected = false;
            self.cursor_open = false;
            self.cursor_cancelled = false;
            self.page_returned = false;
            self.last_action = "init".to_string();
            Ok(())
        }

        fn open_current_snapshot(&mut self) -> Result {
            let epoch = u64::try_from(self.current_epoch)?;
            let shard = self.shard()?;
            let neighbors = self.runtime.block_on(async {
                let snapshot = shard.snapshot_at(CELL, epoch).await?;
                snapshot.out_neighbors(EDGE_TYPE, 1).await
            })?;
            if neighbors != vec![2] {
                bail!("M2b current snapshot did not expose the seeded row");
            }
            self.snapshot_open = true;
            self.snapshot_epoch = self.current_epoch;
            self.future_rejected = false;
            self.historical_rejected = false;
            self.page_returned = false;
            self.last_action = "openCurrentSnapshot".to_string();
            Ok(())
        }

        fn reject_future_snapshot(&mut self) -> Result {
            let error = match self.runtime.block_on(
                self.shard()?
                    .snapshot_at(CELL, u64::try_from(self.current_epoch + 1)?),
            ) {
                Ok(_) => bail!("M2b future epoch unexpectedly opened a snapshot"),
                Err(error) => error,
            };
            if !matches!(error, GraphError::SnapshotAhead { .. }) {
                bail!("M2b future epoch returned unexpected error: {error}");
            }
            self.future_rejected = true;
            self.historical_rejected = false;
            self.page_returned = false;
            self.last_action = "rejectFutureSnapshot".to_string();
            Ok(())
        }

        fn reject_historical_snapshot(&mut self) -> Result {
            let error = match self.runtime.block_on(self.shard()?.snapshot_at(CELL, 0)) {
                Ok(_) => bail!("M2b historical epoch unexpectedly opened a snapshot"),
                Err(error) => error,
            };
            if !matches!(
                error,
                GraphError::UnsupportedQuery { ref dialect, .. } if *dialect == "GraphSnapshot"
            ) {
                bail!("M2b historical epoch returned unexpected error: {error}");
            }
            self.future_rejected = false;
            self.historical_rejected = true;
            self.page_returned = false;
            self.last_action = "rejectHistoricalSnapshot".to_string();
            Ok(())
        }

        fn open_cursor(&mut self) -> Result {
            let epoch = u64::try_from(self.snapshot_epoch)?;
            self.runtime
                .block_on(self.shard()?.snapshot_at(CELL, epoch))?;
            self.cursor_open = true;
            self.cursor_cancelled = false;
            self.page_returned = false;
            self.last_action = "openCursor".to_string();
            Ok(())
        }

        fn cancel_cursor(&mut self) {
            self.cursor_open = false;
            self.cursor_cancelled = true;
            self.page_returned = false;
            self.last_action = "cancelCursor".to_string();
        }

        fn reject_cancelled_page(&mut self) -> Result {
            let token = QueryCancellationToken::new();
            token.cancel();
            let error = self
                .runtime
                .block_on(self.shard()?.execute_cypher_rows_page(
                    QueryContext::new(CELL, "m2b-cancelled-page").with_cancellation_token(token),
                    "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id",
                    None,
                    1,
                ));
            match error {
                Err(error) if error.to_string().contains("query_cancelled") => {}
                Ok(_) => bail!("M2b cancelled request unexpectedly returned a page"),
                Err(error) => bail!("M2b cancellation returned unexpected error: {error}"),
            }
            self.cursor_open = false;
            self.page_returned = false;
            self.last_action = "rejectFetchAfterCancellation".to_string();
            Ok(())
        }

        fn fetch_page(&mut self) -> Result {
            let page = self
                .runtime
                .block_on(self.shard()?.execute_cypher_rows_page(
                    QueryContext::new(CELL, "m2b-page"),
                    "MATCH (u {id: 1})-[:FOLLOWS]->(v) RETURN v.id",
                    None,
                    1,
                ))?;
            if page.rows.is_empty() {
                bail!("M2b cursor page unexpectedly returned no seeded rows");
            }
            self.page_returned = true;
            self.last_action = "fetchCursorPage".to_string();
            Ok(())
        }
    }

    #[quint_run(
        spec = "quint-models/turbolay/m2_snapshot_lifecycle.qnt",
        main = "m2_snapshot_lifecycle",
        max_samples = 24,
        max_steps = 8,
        seed = "20260718"
    )]
    fn m2b_randomized_snapshot_lifecycle_trace_refines_quint() -> impl Driver {
        M2bDriver::default()
    }
}
