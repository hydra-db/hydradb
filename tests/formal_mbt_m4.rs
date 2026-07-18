//! Quint Connect refinement adapter for the M4 placement/fence contract.
//!
//! The Quint model exposes only a normalized placement/fence projection.  The
//! driver keeps that projection while exercising the public routed-cluster APIs:
//! independent local placement views, fenced owned opens, live old handles during
//! a reachability-only partition, replacement takeover on the same object-store
//! path, prefix extension by the replacement, and typed rejection of stale writes.

use std::sync::Arc;

use anyhow::{bail, Context};
use quint_connect::{quint_run, switch, Driver, Result, State, Step};
use serde::Deserialize;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::ErrorKind;
use slatedb_graph_kernel::{EdgeMutation, GraphError, RoutedGraphCluster, ShardPlacement};

const GRAPH_PATH: &str = "graph/formal-mbt-m4";
const CELL: &str = "formal-cell";
const EDGE_TYPE: &str = "FOLLOWS";
const NODE1: &str = "node-1";
const NODE2: &str = "node-2";

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct M4State {
    durable_fence: i64,
    previous_fence: i64,
    node1_candidate: bool,
    node2_candidate: bool,
    node1_active: bool,
    node2_active: bool,
    node1_reachable: bool,
    node2_reachable: bool,
    node1_fenced_by_node2: bool,
    committed_prefix: i64,
    previous_prefix: i64,
    last_action: String,
}

struct M4Driver {
    runtime: tokio::runtime::Runtime,
    store: Option<Arc<dyn ObjectStore>>,
    node1_placement: Option<ShardPlacement>,
    node2_placement: Option<ShardPlacement>,
    node1_cluster: Option<RoutedGraphCluster>,
    node2_cluster: Option<RoutedGraphCluster>,
    p: M4State,
}

impl Default for M4Driver {
    fn default() -> Self {
        Self {
            runtime: tokio::runtime::Runtime::new().expect("M4 MBT runtime"),
            store: None,
            node1_placement: None,
            node2_placement: None,
            node1_cluster: None,
            node2_cluster: None,
            p: M4State {
                durable_fence: 0,
                previous_fence: 0,
                node1_candidate: false,
                node2_candidate: false,
                node1_active: false,
                node2_active: false,
                node1_reachable: true,
                node2_reachable: true,
                node1_fenced_by_node2: false,
                committed_prefix: 0,
                previous_prefix: 0,
                last_action: String::new(),
            },
        }
    }
}

impl State<M4Driver> for M4State {
    fn from_driver(driver: &M4Driver) -> Result<Self> {
        Ok(driver.p.clone())
    }
}

impl Driver for M4Driver {
    type State = M4State;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => { self.init()?; },
            chooseNode1 => { self.choose_node1()?; },
            chooseNode2 => { self.choose_node2()?; },
            acquireFenceNode1 => { self.acquire_fence_node1()?; },
            commitNode1 => { self.commit_node1()?; },
            partitionNode1 => { self.partition_node1()?; },
            takeOverFenceNode2 => { self.take_over_fence_node2()?; },
            commitNode2 => { self.commit_node2()?; },
            rejectZombieNode1Commit => { self.reject_zombie_node1_commit()?; },
        })
    }
}

impl M4Driver {
    fn init(&mut self) -> Result {
        if let Some(cluster) = self.node1_cluster.take() {
            let _ = self.runtime.block_on(cluster.close());
        }
        if let Some(cluster) = self.node2_cluster.take() {
            let _ = self.runtime.block_on(cluster.close());
        }
        self.store = Some(Arc::new(InMemory::new()));
        self.node1_placement = None;
        self.node2_placement = None;
        self.p = M4State {
            durable_fence: 0,
            previous_fence: 0,
            node1_candidate: false,
            node2_candidate: false,
            node1_active: false,
            node2_active: false,
            node1_reachable: true,
            node2_reachable: true,
            node1_fenced_by_node2: false,
            committed_prefix: 0,
            previous_prefix: 0,
            last_action: "init".to_string(),
        };
        Ok(())
    }

    fn choose_node1(&mut self) -> Result {
        self.begin_action("chooseNode1");
        let placement = ShardPlacement::fixed([(CELL, NODE1)])?;
        placement.ensure_local_owner(NODE1, CELL)?;
        match placement.ensure_local_owner(NODE2, CELL) {
            Err(GraphError::ShardNotOwned {
                cell_id,
                owner_node_id,
                local_node_id,
            }) if cell_id == CELL && owner_node_id == NODE1 && local_node_id == NODE2 => {}
            Ok(()) => bail!("M4 node-1 placement unexpectedly allowed node-2 ownership"),
            Err(error) => return Err(error.into()),
        }
        self.node1_placement = Some(placement);
        self.p.node1_candidate = true;
        self.verify_candidate_disagreement()?;
        Ok(())
    }

    fn choose_node2(&mut self) -> Result {
        self.begin_action("chooseNode2");
        let placement = ShardPlacement::fixed([(CELL, NODE2)])?;
        placement.ensure_local_owner(NODE2, CELL)?;
        match placement.ensure_local_owner(NODE1, CELL) {
            Err(GraphError::ShardNotOwned {
                cell_id,
                owner_node_id,
                local_node_id,
            }) if cell_id == CELL && owner_node_id == NODE2 && local_node_id == NODE1 => {}
            Ok(()) => bail!("M4 node-2 placement unexpectedly allowed node-1 ownership"),
            Err(error) => return Err(error.into()),
        }
        self.node2_placement = Some(placement);
        self.p.node2_candidate = true;
        self.verify_candidate_disagreement()?;
        Ok(())
    }

    fn acquire_fence_node1(&mut self) -> Result {
        self.begin_action("acquireFenceNode1");
        if !self.p.node1_reachable || self.p.node1_active {
            bail!("M4 node-1 fence acquisition requires a reachable inactive node-1 candidate");
        }
        let placement = self
            .node1_placement
            .clone()
            .context("M4 node-1 placement was not chosen")?;
        let cluster = self.open_fenced_owned(NODE1, placement)?;
        self.expect_local_cells(&cluster)?;
        self.expect_cluster_prefix(&cluster, self.p.committed_prefix)?;
        self.node1_cluster = Some(cluster);
        self.p.durable_fence = 1;
        self.p.node1_active = true;
        self.p.node2_active = false;
        self.p.node1_fenced_by_node2 = false;
        Ok(())
    }

    fn commit_node1(&mut self) -> Result {
        self.begin_action("commitNode1");
        if !self.p.node1_reachable {
            bail!("M4 node-1 commit attempted while driver marked node-1 unreachable");
        }
        if self.p.durable_fence != 1 {
            bail!("M4 node-1 commit attempted without durable fence 1");
        }
        let next_prefix = self.p.committed_prefix + 1;
        let mutation = self.mutation(1, 100 + next_prefix as u64, "node1", next_prefix);
        let cluster = self
            .node1_cluster
            .as_ref()
            .context("M4 node-1 fenced cluster is not open")?;
        self.expect_cluster_prefix(cluster, self.p.committed_prefix)?;
        let result = self.runtime.block_on(cluster.write_edge(mutation))?;
        if result.epoch != next_prefix as u64 {
            bail!(
                "M4 node-1 write committed at epoch {}, expected {}",
                result.epoch,
                next_prefix
            );
        }
        self.expect_cluster_prefix(cluster, next_prefix)?;
        self.p.committed_prefix = next_prefix;
        Ok(())
    }

    fn partition_node1(&mut self) -> Result {
        self.begin_action("partitionNode1");
        if !self.p.node1_active || self.p.durable_fence != 1 || self.node1_cluster.is_none() {
            bail!("M4 node-1 partition requires a live node-1 writer");
        }
        self.p.node1_reachable = false;
        Ok(())
    }

    fn take_over_fence_node2(&mut self) -> Result {
        self.begin_action("takeOverFenceNode2");
        if !self.p.node1_active || self.p.durable_fence != 1 || self.node1_cluster.is_none() {
            bail!("M4 node-2 takeover requires a live node-1 writer to fence");
        }
        let placement = self
            .node2_placement
            .clone()
            .context("M4 node-2 placement was not chosen")?;
        let cluster = self.open_fenced_owned(NODE2, placement)?;
        self.expect_local_cells(&cluster)?;
        self.expect_cluster_prefix(&cluster, self.p.committed_prefix)?;
        self.node2_cluster = Some(cluster);
        self.p.durable_fence = 2;
        self.p.node1_active = false;
        self.p.node2_active = true;
        self.p.node1_fenced_by_node2 = true;
        Ok(())
    }

    fn commit_node2(&mut self) -> Result {
        self.begin_action("commitNode2");
        if !self.p.node2_reachable {
            bail!("M4 node-2 commit attempted while driver marked node-2 unreachable");
        }
        if self.p.durable_fence != 2 {
            bail!("M4 node-2 commit attempted without durable fence 2");
        }
        let next_prefix = self.p.committed_prefix + 1;
        let mutation = self.mutation(2, 200 + next_prefix as u64, "node2", next_prefix);
        let cluster = self
            .node2_cluster
            .as_ref()
            .context("M4 node-2 fenced cluster is not open")?;
        self.expect_cluster_prefix(cluster, self.p.committed_prefix)?;
        let result = self.runtime.block_on(cluster.write_edge(mutation))?;
        if result.epoch != next_prefix as u64 {
            bail!(
                "M4 node-2 write committed at epoch {}, expected {}",
                result.epoch,
                next_prefix
            );
        }
        self.expect_cluster_prefix(cluster, next_prefix)?;
        self.p.committed_prefix = next_prefix;
        Ok(())
    }

    fn reject_zombie_node1_commit(&mut self) -> Result {
        self.begin_action("rejectZombieNode1Commit");
        let prefix_before = self.p.committed_prefix;
        let mutation = self.mutation(9, 900 + prefix_before as u64, "node1-zombie", prefix_before);

        if !self.p.node1_fenced_by_node2 {
            bail!("M4 zombie rejection requires node-2 to have fenced a live node-1 writer");
        }
        let cluster = self
            .node1_cluster
            .as_ref()
            .context("M4 old node-1 writer handle is not available for stale-fence evidence")?;
        let error = self
            .runtime
            .block_on(cluster.write_edge(mutation))
            .expect_err("M4 old node-1 writer must be fenced after node-2 takeover");
        if !matches!(
            error,
            GraphError::Slate(ref slate_error)
                if matches!(
                    slate_error.kind(),
                    ErrorKind::Closed(slatedb::CloseReason::Fenced)
                )
        ) {
            bail!("M4 stale node-1 write returned unexpected error: {error}");
        }

        if let Some(cluster) = self.node2_cluster.as_ref() {
            self.expect_cluster_prefix(cluster, prefix_before)?;
            let exists = self.runtime.block_on(cluster.shard(CELL)?.edge_exists(
                CELL,
                EDGE_TYPE,
                9,
                900 + prefix_before as u64,
            ))?;
            if exists {
                bail!("M4 zombie write became visible in the replacement writer");
            }
        }
        self.p.committed_prefix = prefix_before;
        Ok(())
    }

    fn begin_action(&mut self, action: &str) {
        self.p.previous_fence = self.p.durable_fence;
        self.p.previous_prefix = self.p.committed_prefix;
        self.p.last_action = action.to_string();
    }

    fn open_fenced_owned(
        &mut self,
        local_node_id: &str,
        placement: ShardPlacement,
    ) -> Result<RoutedGraphCluster> {
        let store = Arc::clone(self.store.as_ref().context("M4 store is not initialized")?);
        Ok(self
            .runtime
            .block_on(RoutedGraphCluster::open_fenced_owned(
                GRAPH_PATH,
                local_node_id,
                placement,
                store,
            ))?)
    }

    fn verify_candidate_disagreement(&self) -> Result {
        if let (Some(node1), Some(node2)) = (&self.node1_placement, &self.node2_placement) {
            let owner1 = node1.owner(CELL)?;
            let owner2 = node2.owner(CELL)?;
            if owner1 == owner2 || owner1 != NODE1 || owner2 != NODE2 {
                bail!("M4 placement views did not disagree on {CELL}: {owner1} vs {owner2}");
            }
        }
        Ok(())
    }

    fn expect_local_cells(&self, cluster: &RoutedGraphCluster) -> Result {
        let cells = cluster.local_cells();
        if cells != [CELL] {
            bail!(
                "M4 routed cluster opened local cells {:?}, expected [{CELL}]",
                cells
            );
        }
        Ok(())
    }

    fn expect_cluster_prefix(&self, cluster: &RoutedGraphCluster, expected: i64) -> Result {
        let epoch = self
            .runtime
            .block_on(cluster.shard(CELL)?.current_epoch(CELL))?;
        if epoch != expected as u64 {
            bail!("M4 cluster prefix is {epoch}, expected {expected}");
        }
        Ok(())
    }

    fn mutation(&self, src: u64, dst: u64, writer: &str, prefix: i64) -> EdgeMutation {
        EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: EDGE_TYPE.to_string(),
            src,
            dst,
            idempotency_key: format!("m4-{writer}-{prefix}"),
        }
    }
}

#[quint_run(
    spec = "quint-models/turbolay/m4_placement_fence.qnt",
    main = "m4_placement_fence",
    max_samples = 24,
    max_steps = 8,
    seed = "20260718"
)]
fn m4_randomized_placement_fence_trace_refines_quint() -> impl Driver {
    M4Driver::default()
}
