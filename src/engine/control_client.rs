use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::*;

#[async_trait]
pub trait GraphControlClient: Send + Sync {
    fn scope(&self) -> &GraphScope;

    async fn load_placement(&self) -> Result<ShardPlacement>;

    async fn acquire_lease(
        &self,
        cell_id: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<ShardLease>;

    async fn renew_lease(&self, lease: &ShardLease, ttl: Duration) -> Result<ShardLease>;

    async fn release_lease(&self, lease: &ShardLease) -> Result<bool>;

    async fn drop_cell_control_state(
        &self,
        cell_id: &str,
        expected_lease: Option<&ShardLease>,
    ) -> Result<()>;

    async fn publish_node_heartbeat(
        &self,
        node_id: &str,
        state: GraphNodeHealthState,
    ) -> Result<GraphNodeHeartbeat>;

    async fn load_node_heartbeats(&self) -> Result<Vec<GraphNodeHeartbeat>>;

    async fn current_lease(&self, cell_id: &str) -> Result<Option<ShardLease>>;

    async fn metrics(&self) -> Result<GraphControlMetricsSnapshot>;
}

#[async_trait]
impl GraphControlClient for GraphControlPlane {
    fn scope(&self) -> &GraphScope {
        GraphControlPlane::scope(self)
    }

    async fn load_placement(&self) -> Result<ShardPlacement> {
        GraphControlPlane::load_placement(self).await
    }

    async fn acquire_lease(
        &self,
        cell_id: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<ShardLease> {
        GraphControlPlane::acquire_lease(self, cell_id, node_id, ttl).await
    }

    async fn renew_lease(&self, lease: &ShardLease, ttl: Duration) -> Result<ShardLease> {
        GraphControlPlane::renew_lease(self, lease, ttl).await
    }

    async fn release_lease(&self, lease: &ShardLease) -> Result<bool> {
        GraphControlPlane::release_lease(self, lease).await
    }

    async fn drop_cell_control_state(
        &self,
        cell_id: &str,
        expected_lease: Option<&ShardLease>,
    ) -> Result<()> {
        GraphControlPlane::drop_cell_control_state(self, cell_id, expected_lease)
            .await
            .map(|_| ())
    }

    async fn publish_node_heartbeat(
        &self,
        node_id: &str,
        state: GraphNodeHealthState,
    ) -> Result<GraphNodeHeartbeat> {
        GraphControlPlane::publish_node_heartbeat(self, node_id, state).await
    }

    async fn load_node_heartbeats(&self) -> Result<Vec<GraphNodeHeartbeat>> {
        GraphControlPlane::load_node_heartbeats(self).await
    }

    async fn current_lease(&self, cell_id: &str) -> Result<Option<ShardLease>> {
        GraphControlPlane::current_lease(self, cell_id).await
    }

    async fn metrics(&self) -> Result<GraphControlMetricsSnapshot> {
        Ok(GraphControlPlane::graph_control_metrics(self))
    }
}

#[async_trait]
impl<T> GraphControlClient for Arc<T>
where
    T: GraphControlClient + ?Sized,
{
    fn scope(&self) -> &GraphScope {
        self.as_ref().scope()
    }
    async fn load_placement(&self) -> Result<ShardPlacement> {
        self.as_ref().load_placement().await
    }
    async fn acquire_lease(
        &self,
        cell_id: &str,
        node_id: &str,
        ttl: Duration,
    ) -> Result<ShardLease> {
        self.as_ref().acquire_lease(cell_id, node_id, ttl).await
    }
    async fn renew_lease(&self, lease: &ShardLease, ttl: Duration) -> Result<ShardLease> {
        self.as_ref().renew_lease(lease, ttl).await
    }
    async fn release_lease(&self, lease: &ShardLease) -> Result<bool> {
        self.as_ref().release_lease(lease).await
    }
    async fn drop_cell_control_state(
        &self,
        cell_id: &str,
        expected_lease: Option<&ShardLease>,
    ) -> Result<()> {
        self.as_ref()
            .drop_cell_control_state(cell_id, expected_lease)
            .await
    }
    async fn publish_node_heartbeat(
        &self,
        node_id: &str,
        state: GraphNodeHealthState,
    ) -> Result<GraphNodeHeartbeat> {
        self.as_ref().publish_node_heartbeat(node_id, state).await
    }
    async fn load_node_heartbeats(&self) -> Result<Vec<GraphNodeHeartbeat>> {
        self.as_ref().load_node_heartbeats().await
    }
    async fn current_lease(&self, cell_id: &str) -> Result<Option<ShardLease>> {
        self.as_ref().current_lease(cell_id).await
    }
    async fn metrics(&self) -> Result<GraphControlMetricsSnapshot> {
        self.as_ref().metrics().await
    }
}

pub(crate) async fn start_control_client_heartbeat(
    control: Arc<dyn GraphControlClient>,
    node_id: String,
    initial_state: GraphNodeHealthState,
    interval: Duration,
) -> Result<NodeHeartbeatHandle> {
    if interval.is_zero() {
        return Err(GraphError::CorruptValue {
            key: "control/node_heartbeat_interval".to_string(),
            reason: "node heartbeat interval must be greater than zero".to_string(),
        });
    }
    validate_component("node_id", &node_id)?;
    control
        .publish_node_heartbeat(&node_id, initial_state)
        .await?;
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let (state_tx, mut state_rx) = watch::channel(initial_state);
    let task = tokio::spawn(async move {
        loop {
            let state = *state_rx.borrow();
            if let Err(error) = control.publish_node_heartbeat(&node_id, state).await {
                tracing::warn!(node_id, error = %error, "node heartbeat publish failed");
            }
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return Ok(());
                    }
                }
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                }
                _ = tokio::time::sleep(interval) => {}
            }
        }
    });
    Ok(NodeHeartbeatHandle {
        stop_tx,
        state_tx,
        task,
    })
}
