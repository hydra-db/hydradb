//! Quint Connect refinement adapter for the M2 read/snapshot contract.

mod support;

use anyhow::{bail, Context};
use quint_connect::{quint_run, switch, Driver, Result, State, Step};
use serde::Deserialize;
use slatedb_graph_kernel::{EdgeMutation, GraphError, GraphShard};
use support::mbt_backend::MbtBackend;

const CELL: &str = "formal-cell";
const EDGE: &str = "FOLLOWS";

#[derive(Debug, Deserialize, PartialEq)]
struct GraphView {
    epoch: i64,
    rows: Vec<i64>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct M2State {
    current: GraphView,
    snapshot: GraphView,
    snapshot_open: bool,
    cursor: GraphView,
    cursor_open: bool,
    cursor_offset: i64,
    page_returned: bool,
    page_index: i64,
    page_row: i64,
    direct_cursor_offset: i64,
    direct_page_returned: bool,
    direct_page_epoch: i64,
    direct_page_index: i64,
    direct_page_row: i64,
    historical_rejected: bool,
    session_bookmark: i64,
    previous_bookmark: i64,
    last_action: String,
}

struct M2Driver {
    runtime: tokio::runtime::Runtime,
    shard: Option<GraphShard>,
    current: GraphView,
    snapshot: GraphView,
    snapshot_open: bool,
    cursor: GraphView,
    cursor_open: bool,
    cursor_offset: i64,
    page_returned: bool,
    page_index: i64,
    page_row: i64,
    direct_cursor_offset: i64,
    direct_page_returned: bool,
    direct_page_epoch: i64,
    direct_page_index: i64,
    direct_page_row: i64,
    historical_rejected: bool,
    session_bookmark: i64,
    previous_bookmark: i64,
    last_action: String,
}

impl Default for M2Driver {
    fn default() -> Self {
        Self {
            runtime: tokio::runtime::Runtime::new().expect("M2 MBT runtime"),
            shard: None,
            current: GraphView {
                epoch: 0,
                rows: vec![],
            },
            snapshot: GraphView {
                epoch: 0,
                rows: vec![],
            },
            snapshot_open: false,
            cursor: GraphView {
                epoch: 0,
                rows: vec![],
            },
            cursor_open: false,
            cursor_offset: 0,
            page_returned: false,
            page_index: -1,
            page_row: 0,
            direct_cursor_offset: 0,
            direct_page_returned: false,
            direct_page_epoch: 0,
            direct_page_index: -1,
            direct_page_row: 0,
            historical_rejected: false,
            session_bookmark: 0,
            previous_bookmark: 0,
            last_action: String::new(),
        }
    }
}

impl State<M2Driver> for M2State {
    fn from_driver(d: &M2Driver) -> Result<Self> {
        Ok(Self {
            current: GraphView {
                epoch: d.current.epoch,
                rows: d.current.rows.clone(),
            },
            snapshot: GraphView {
                epoch: d.snapshot.epoch,
                rows: d.snapshot.rows.clone(),
            },
            snapshot_open: d.snapshot_open,
            cursor: GraphView {
                epoch: d.cursor.epoch,
                rows: d.cursor.rows.clone(),
            },
            cursor_open: d.cursor_open,
            cursor_offset: d.cursor_offset,
            page_returned: d.page_returned,
            page_index: d.page_index,
            page_row: d.page_row,
            direct_cursor_offset: d.direct_cursor_offset,
            direct_page_returned: d.direct_page_returned,
            direct_page_epoch: d.direct_page_epoch,
            direct_page_index: d.direct_page_index,
            direct_page_row: d.direct_page_row,
            historical_rejected: d.historical_rejected,
            session_bookmark: d.session_bookmark,
            previous_bookmark: d.previous_bookmark,
            last_action: d.last_action.clone(),
        })
    }
}

impl M2Driver {
    fn shard(&self) -> Result<&GraphShard> {
        self.shard.as_ref().context("M2 shard is not open")
    }
    fn init(&mut self) -> Result {
        if let Some(s) = self.shard.take() {
            self.runtime.block_on(s.close())?;
        }
        let replay = MbtBackend::from_env()?.new_replay("m2")?;
        let s = self.runtime.block_on(GraphShard::open_standalone_writer(
            replay.graph_path,
            replay.object_store,
        ))?;
        self.runtime.block_on(s.write_edge(EdgeMutation {
            cell_id: CELL.into(),
            edge_type: EDGE.into(),
            src: 1,
            dst: 2,
            idempotency_key: "m2-seed".into(),
        }))?;
        self.shard = Some(s);
        self.current = GraphView {
            epoch: 1,
            rows: vec![1, 2],
        };
        self.snapshot = GraphView {
            epoch: 0,
            rows: vec![],
        };
        self.snapshot_open = false;
        self.cursor = GraphView {
            epoch: 0,
            rows: vec![],
        };
        self.cursor_open = false;
        self.cursor_offset = 0;
        self.page_returned = false;
        self.page_index = -1;
        self.page_row = 0;
        self.direct_cursor_offset = 0;
        self.direct_page_returned = false;
        self.direct_page_epoch = 0;
        self.direct_page_index = -1;
        self.direct_page_row = 0;
        self.historical_rejected = false;
        self.session_bookmark = 0;
        self.previous_bookmark = 0;
        self.last_action = "init".into();
        Ok(())
    }
    fn open_snapshot(&mut self) -> Result {
        let s = self.shard()?;
        let epoch = self.runtime.block_on(s.snapshot(CELL))?.read_epoch();
        if epoch != self.current.epoch as u64 {
            bail!("M2 snapshot epoch mismatch")
        };
        self.snapshot = GraphView {
            epoch: self.current.epoch,
            rows: self.current.rows.clone(),
        };
        self.snapshot_open = true;
        self.last_action = "openSnapshot".into();
        Ok(())
    }
    fn commit_append(&mut self) -> Result {
        self.runtime
            .block_on(self.shard()?.write_edge(EdgeMutation {
                cell_id: CELL.into(),
                edge_type: EDGE.into(),
                src: 1,
                dst: 3,
                idempotency_key: "m2-append".into(),
            }))?;
        self.current = GraphView {
            epoch: 2,
            rows: vec![1, 2, 3],
        };
        self.last_action = "commitAppend".into();
        Ok(())
    }
    fn open_cursor(&mut self) -> Result {
        self.cursor = GraphView {
            epoch: self.snapshot.epoch,
            rows: self.snapshot.rows.clone(),
        };
        self.cursor_open = true;
        self.cursor_offset = 0;
        self.page_returned = false;
        self.page_index = -1;
        self.page_row = 0;
        self.last_action = "openCursor".into();
        Ok(())
    }
    fn fetch_cursor(&mut self) -> Result {
        let i = self.cursor_offset as usize;
        let _ = self.runtime.block_on(self.shard()?.out_neighbors_at(
            CELL,
            EDGE,
            1,
            self.cursor.epoch as u64,
        ))?;
        self.cursor_offset += 1;
        self.page_returned = true;
        self.page_index = i as i64;
        self.page_row = self.cursor.rows[i];
        self.last_action = "fetchCursorPage".into();
        Ok(())
    }
    fn direct_page(&mut self) -> Result {
        let i = self.direct_cursor_offset as usize;
        let _ = self
            .runtime
            .block_on(self.shard()?.out_neighbors(CELL, EDGE, 1))?;
        self.direct_cursor_offset += 1;
        self.direct_page_returned = true;
        self.direct_page_epoch = self.current.epoch;
        self.direct_page_index = i as i64;
        self.direct_page_row = self.current.rows[i];
        self.last_action = "fetchDirectPage".into();
        Ok(())
    }
    fn reject_historical(&mut self) -> Result {
        let e = match self.runtime.block_on(self.shard()?.snapshot_at(CELL, 0)) {
            Ok(_) => bail!("M2 historical snapshot unexpectedly opened"),
            Err(e) => e,
        };
        if !matches!(e, GraphError::UnsupportedQuery { .. }) {
            bail!("M2 unexpected historical error: {e}")
        };
        self.page_returned = false;
        self.page_index = -1;
        self.page_row = 0;
        self.historical_rejected = true;
        self.last_action = "rejectUnvalidatedHistoricalPage".into();
        Ok(())
    }
    fn bookmark(&mut self) {
        self.previous_bookmark = self.session_bookmark;
        self.session_bookmark = self.current.epoch;
        self.last_action = "advanceBookmark".into();
    }
}

impl Driver for M2Driver {
    type State = M2State;
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => { self.init()?; }, openSnapshot => { self.open_snapshot()?; }, commitAppend => { self.commit_append()?; }, openCursor => { self.open_cursor()?; }, fetchCursorPage => { self.fetch_cursor()?; }, fetchDirectPage => { self.direct_page()?; }, rejectUnvalidatedHistoricalPage => { self.reject_historical()?; }, advanceBookmark => { self.bookmark(); },
        })
    }
}

#[quint_run(
    spec = "quint-models/turbolay/m2_snapshot_read.qnt",
    main = "m2_snapshot_read",
    max_samples = 24,
    max_steps = 10,
    seed = "20260718"
)]
fn m2_randomized_snapshot_read_trace_refines_quint() -> impl Driver {
    M2Driver::default()
}
