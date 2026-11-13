//! Per node admission: permits, the in-flight gauge and per node counters.
//!
//! Each node has a budget of concurrent units across the whole proxy fleet.
//! A replica holds `budget / replicas` permits for it, so the replicas together
//! stay within the budget, and a replica joining or leaving resizes everyone's
//! share on the next unit. A replica never holds fewer than one permit, since
//! with none it could serve nothing; with more replicas than the budget, the
//! fleet holds one per replica. Both adapters admit through the
//! same registry, so Bolt and HTTP cannot double book a node.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use turbolay_proxy_types::{Access, NodeId, Protocol, RouteSnapshot};

use crate::metrics::{NodeCounters, NodeMetrics, RouteOutcome};

/// Why a unit was not admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmitError {
    /// Every permit for the node stayed taken for the whole wait.
    Timeout,
    /// The node is not in the current snapshot.
    UnknownNode,
}

/// One backend node's admission state.
pub struct NodeState {
    /// This replica's free permits for the node.
    semaphore: Arc<Semaphore>,
    /// Permits granted to this replica.
    share: AtomicUsize,
    /// Permits a shrink could not take back because they were in use. Each is
    /// retired when it comes back instead of being returned to the semaphore.
    debt: AtomicUsize,
    pub id: NodeId,
    pub counters: NodeCounters,
}

impl NodeState {
    fn new(id: NodeId, share: usize, metrics: &NodeMetrics) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(share)),
            share: AtomicUsize::new(share),
            debt: AtomicUsize::new(0),
            counters: metrics.node(&id),
            id,
        }
    }

    /// Units admitted to this node and not yet finished, from this replica.
    pub fn in_flight(&self) -> usize {
        let issued = self.share.load(Ordering::Relaxed) + self.debt.load(Ordering::Relaxed);
        issued.saturating_sub(self.semaphore.available_permits())
    }

    /// This replica's permit share for the node.
    pub fn share(&self) -> usize {
        self.share.load(Ordering::Relaxed)
    }

    /// Take up to `count` permits off the debt; returns how many.
    fn pay_debt(&self, count: usize) -> usize {
        let before = self.debt.update(Ordering::AcqRel, Ordering::Acquire, |debt| debt - debt.min(count));
        before.min(count)
    }

    fn resize(&self, target: usize) {
        let current = self.share.swap(target, Ordering::AcqRel);
        if target > current {
            let grow = target - current;
            self.semaphore.add_permits(grow - self.pay_debt(grow));
        } else if current > target {
            // Owe the whole shrink first, so a permit returned meanwhile is
            // retired rather than freed. Free permits then pay the debt down;
            // any taken past what is still owed go back.
            let shrink = current - target;
            self.debt.fetch_add(shrink, Ordering::AcqRel);
            let taken = self.semaphore.forget_permits(shrink);
            self.semaphore.add_permits(taken - self.pay_debt(taken));
        }
    }

    pub fn record(&self, protocol: Protocol, access: Access, outcome: RouteOutcome) {
        self.counters.record(protocol, access, outcome);
    }
}

/// A unit's hold on a node. Dropping it frees the permit.
pub struct Permit {
    node: Arc<NodeState>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Permit {
    fn new(node: Arc<NodeState>, permit: OwnedSemaphorePermit) -> Self {
        Self {
            node,
            permit: Some(permit),
        }
    }

    pub fn node(&self) -> &Arc<NodeState> {
        &self.node
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            if self.node.pay_debt(1) == 1 {
                permit.forget();
            }
        }
    }
}

struct Registry {
    replicas: usize,
    /// Sorted by id. A handful of nodes: a binary search beats hashing.
    nodes: Vec<Arc<NodeState>>,
}

impl Registry {
    fn find(&self, id: &str) -> Option<&Arc<NodeState>> {
        self.nodes
            .binary_search_by(|node| node.id.as_str().cmp(id))
            .ok()
            .map(|index| &self.nodes[index])
    }
}

/// Admission for every node in the snapshot.
pub struct NodeRegistry {
    budget: usize,
    wait: Duration,
    registry: ArcSwap<Registry>,
    metrics: NodeMetrics,
}

impl NodeRegistry {
    /// `budget` is the fleet-wide concurrent units per node; `wait` how long a
    /// unit queues for a permit before it is refused.
    pub(crate) fn new(budget: usize, wait: Duration, metrics: NodeMetrics) -> Self {
        Self {
            budget,
            wait,
            registry: ArcSwap::from_pointee(Registry {
                replicas: 0,
                nodes: Vec::new(),
            }),
            metrics,
        }
    }

    fn share(&self, replicas: usize) -> usize {
        (self.budget / replicas.max(1)).max(1)
    }

    /// Bring the registry in line with `snapshot`: add new nodes, drop gone
    /// ones, resize every share when the replica count moved. Only the
    /// router's one writer calls this: two at once could each build their own
    /// state for a new node.
    pub(crate) fn sync(&self, snapshot: &RouteSnapshot) {
        let current = self.registry.load();
        let replicas = snapshot.replica_count();
        let share = self.share(replicas);
        let mut nodes = Vec::with_capacity(snapshot.nodes.len());
        for node in &snapshot.nodes {
            let state = match current.find(node.id.as_str()) {
                Some(state) => {
                    if replicas != current.replicas {
                        state.resize(share);
                    }
                    Arc::clone(state)
                }
                None => Arc::new(NodeState::new(node.id.clone(), share, &self.metrics)),
            };
            nodes.push(state);
        }
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        for gone in &current.nodes {
            if snapshot.node(gone.id.as_str()).is_none() {
                self.metrics.forget(&gone.id);
            }
        }
        self.registry.store(Arc::new(Registry { replicas, nodes }));
    }

    pub fn get(&self, id: &NodeId) -> Option<Arc<NodeState>> {
        self.registry.load().find(id.as_str()).cloned()
    }

    /// Units in flight to `id` from this replica, without taking a reference
    /// on the node's state.
    pub fn in_flight(&self, id: &NodeId) -> usize {
        self.registry.load().find(id.as_str()).map_or(0, |node| node.in_flight())
    }

    /// Record an attempt's outcome against `id`.
    pub fn record(&self, id: &NodeId, protocol: Protocol, access: Access, outcome: RouteOutcome) {
        if let Some(node) = self.registry.load().find(id.as_str()) {
            node.record(protocol, access, outcome);
        }
    }

    /// Every node's state, sorted by id.
    pub fn all(&self) -> Vec<Arc<NodeState>> {
        self.registry.load().nodes.clone()
    }

    /// Take a permit for `id`, queueing up to the configured wait.
    pub async fn admit(&self, id: &NodeId) -> Result<Permit, AdmitError> {
        let node = self.get(id).ok_or(AdmitError::UnknownNode)?;
        if let Ok(permit) = Arc::clone(&node.semaphore).try_acquire_owned() {
            return Ok(Permit::new(node, permit));
        }
        let started = Instant::now();
        self.metrics.admit_queued.inc();
        let acquired = tokio::time::timeout(self.wait, Arc::clone(&node.semaphore).acquire_owned()).await;
        self.metrics.admit_queued_nanos.inc_by(started.elapsed().as_nanos() as u64);
        match acquired {
            Ok(permit) => Ok(Permit::new(node, permit.expect("the semaphore is never closed"))),
            Err(_) => {
                self.metrics.admit_timeouts.inc();
                Err(AdmitError::Timeout)
            }
        }
    }
}
