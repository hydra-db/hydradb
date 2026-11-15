use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use moka::sync::Cache;
use tokio::sync::watch;
use turbolay_proxy_types::{classify, Access, NodeId, Operation, Outcome, RetryBudget, RetryDecision, RouteKey, RouteSnapshot};

use crate::metrics::Metrics;
use crate::nodes::{AdmitError, NodeRegistry, Permit};
use crate::placement::{Placement, RendezvousPlacement};

/// Where reads go, the same `GRAPH_READ_ROUTING` knob graph-node reads. Behind
/// the proxy the nodes' own setting does nothing, since the proxy answers
/// ROUTE, so the proxy carries it.
///
/// `gds.*` calls carry catalog state keyed by graph name, so a read can land
/// on any node after the project call that named it. graph-node makes that
/// work by keeping each projection as a durable record in the cell: project
/// writes it, drop deletes it, and every node reads it. Routing that looked at
/// the query could pin a graph name to one node instead, and then that record,
/// with its write on project and its delete on drop, could go.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadPolicy {
    /// Any node, least units in flight first. Default, as on graph-node.
    #[default]
    Fleet,
    /// The cell's owner, like a write.
    Owner,
}

impl ReadPolicy {
    /// Parse with graph-node's spelling and default: trimmed, case-insensitive,
    /// absent or empty means `fleet`.
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        match value.map(|value| value.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("fleet") => Ok(Self::Fleet),
            Some("owner") => Ok(Self::Owner),
            Some(_) => Err(format!(
                "invalid GRAPH_READ_ROUTING={}; expected owner or fleet",
                value.unwrap_or_default().trim()
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fleet => "fleet",
            Self::Owner => "owner",
        }
    }
}

/// Router settings.
#[derive(Clone, Debug)]
pub struct RouterConfig {
    pub read_policy: ReadPolicy,
    /// Concurrent units per node across the whole proxy fleet.
    pub node_budget: usize,
    /// How long a unit queues for a node permit.
    pub admit_wait: Duration,
    pub owner_cache_capacity: usize,
    pub owner_cache_ttl: Duration,
    pub retry: RetryBudget,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            read_policy: ReadPolicy::Fleet,
            node_budget: 256,
            admit_wait: Duration::from_secs(5),
            owner_cache_capacity: 65_536,
            owner_cache_ttl: Duration::from_secs(60),
            retry: RetryBudget::default(),
        }
    }
}

/// Where a unit may go, best first. Addresses are looked up per attempt from
/// the live snapshot, so a route holds no reference on it.
#[derive(Clone, Debug)]
pub struct Route {
    pub candidates: Vec<NodeId>,
}

/// Routing cannot name any node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteError {
    NoNodes,
}

/// The core both adapters call into.
pub struct Router<P: Placement = RendezvousPlacement> {
    /// Written only through the [`RouteWriter`].
    snapshot: ArcSwap<RouteSnapshot>,
    placement: P,
    /// Owners learned from NotALeader / 421 hints, per route key. A hint can
    /// name the writer lease holder before rendezvous does, during a handoff.
    /// Bounded and time-limited: an entry only saves one refused round trip,
    /// so losing one is cheap and a stale one must age out on its own.
    owners: Cache<RouteKey, NodeId>,
    /// Unknown owner names logged in the last minute.
    unknown_hints: Cache<String, ()>,
    nodes: NodeRegistry,
    metrics: Metrics,
    config: RouterConfig,
}

/// The router's one writer. [`Router::new`] hands out exactly one, it is not
/// `Clone`, and applying takes `&mut self`, so two snapshots are never applied
/// at once.
pub struct RouteWriter<P: Placement = RendezvousPlacement> {
    router: Arc<Router<P>>,
}

impl<P: Placement> RouteWriter<P> {
    /// Route by `next` from now on.
    pub fn apply(&mut self, next: Arc<RouteSnapshot>) {
        self.router.install(next);
        self.router.metrics.snapshot_changes.inc();
    }

    /// Apply every snapshot `updates` publishes, until its sender is dropped.
    pub async fn follow(mut self, mut updates: watch::Receiver<Arc<RouteSnapshot>>) {
        while updates.changed().await.is_ok() {
            let next = Arc::clone(&updates.borrow_and_update());
            self.apply(next);
        }
    }
}

impl<P: Placement> Router<P> {
    /// A router routing by `initial`, and the one writer that can change that.
    pub fn new(initial: Arc<RouteSnapshot>, placement: P, config: RouterConfig) -> (Arc<Self>, RouteWriter<P>) {
        let metrics = Metrics::default();
        let router = Self {
            snapshot: ArcSwap::new(Arc::clone(&initial)),
            placement,
            owners: Cache::builder()
                .max_capacity(config.owner_cache_capacity as u64)
                .time_to_live(config.owner_cache_ttl)
                .build(),
            unknown_hints: Cache::builder().max_capacity(256).time_to_live(Duration::from_secs(60)).build(),
            nodes: NodeRegistry::new(config.node_budget, config.admit_wait, metrics.nodes.clone()),
            metrics,
            config,
        };
        router.install(initial);
        let router = Arc::new(router);
        (Arc::clone(&router), RouteWriter { router })
    }

    /// Admission learns the nodes before any unit can see them, so a routable
    /// node is always admissible.
    fn install(&self, next: Arc<RouteSnapshot>) {
        self.nodes.sync(&next);
        self.snapshot.store(next);
    }

    /// The current snapshot. Takes a reference on it; the per-unit path uses
    /// [`Self::snapshot`] instead.
    pub fn current(&self) -> Arc<RouteSnapshot> {
        self.snapshot.load_full()
    }

    /// A short-lived view of the snapshot. No reference count is touched, so
    /// every worker can call this on every unit without sharing a cache line.
    /// Do not hold it across an await.
    pub fn snapshot(&self) -> arc_swap::Guard<Arc<RouteSnapshot>> {
        self.snapshot.load()
    }

    pub fn config(&self) -> &RouterConfig {
        &self.config
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn nodes(&self) -> &NodeRegistry {
        &self.nodes
    }

    /// Candidates for a unit. Writes, and reads under `owner`, get exactly one:
    /// the owner from the cache, else the rendezvous winner. It is never
    /// skipped for health: advertising anyone else builds a refusal loop.
    /// Reads under `fleet` get every node that serves reads, fewest units in
    /// flight first, rendezvous order breaking ties.
    pub async fn route(&self, key: &RouteKey, operation: Operation) -> Result<Route, RouteError> {
        let snapshot = self.snapshot();
        let candidates = match (operation.access, self.config.read_policy) {
            (Access::Write, _) | (Access::Read, ReadPolicy::Owner) => {
                if operation.access == Access::Read {
                    self.metrics.read_routed(ReadPolicy::Owner);
                }
                match self.cached_owner(key, &snapshot) {
                    Some(owner) => vec![owner],
                    None => self.placement.owner(key, Access::Write, &snapshot).await.into_iter().collect(),
                }
            }
            (Access::Read, ReadPolicy::Fleet) => {
                self.metrics.read_routed(ReadPolicy::Fleet);
                // Each load is read once: other workers change it while the
                // sort runs, and a key that moves mid-sort breaks the order
                // the sort relies on. The sort is stable, so rendezvous order
                // breaks ties.
                let mut ranked = self.placement.resolve(key, Access::Read, &snapshot).await;
                ranked.sort_by_cached_key(|id| self.nodes.in_flight(id));
                ranked
            }
        };
        if candidates.is_empty() {
            return Err(RouteError::NoNodes);
        }
        Ok(Route { candidates })
    }

    fn cached_owner(&self, key: &RouteKey, snapshot: &RouteSnapshot) -> Option<NodeId> {
        let owner = self.owners.get(key)?;
        if snapshot.node(owner.as_str()).is_none() {
            self.owners.invalidate(key);
            return None;
        }
        self.metrics.owner_cache_hits.inc();
        Some(owner)
    }

    /// Learn from `refused_by` refusing a write for `key` as not its owner.
    /// A hint naming a node in the snapshot becomes the key's owner and is
    /// returned. A refusal without a hint forgets the cached owner.
    ///
    /// A hint naming a node outside the snapshot means the fleet disagrees on
    /// membership: a node configured with a list the proxy has not loaded yet,
    /// or a writer lease still held by a node that was removed. It is counted,
    /// and logged once a minute per name so a rollout cannot flood the log.
    pub fn learn_refusal(&self, key: &RouteKey, refused_by: &NodeId, hint: Option<&str>) -> Option<NodeId> {
        let Some(hint) = hint else {
            self.owners.invalidate(key);
            return None;
        };
        let snapshot = self.snapshot();
        if let Some((_, node)) = snapshot.node(hint) {
            self.owners.insert(key.clone(), node.id.clone());
            self.metrics.owner_cache_updates.inc();
            return Some(node.id.clone());
        }
        self.metrics.owner_hints_unknown_node.inc();
        if self.unknown_hints.entry(hint.to_string()).or_insert(()).is_fresh() {
            tracing::warn!(%refused_by, owner = hint, scope = key.scope(), "owner hint names a node outside the fleet");
        }
        None
    }

    pub async fn admit(&self, node: &NodeId) -> Result<Permit, AdmitError> {
        self.nodes.admit(node).await
    }

    /// The retry policy with this router's budget. Under the `owner` read
    /// policy a read has one candidate, like a write, so it retries like one.
    pub fn classify(&self, operation: Operation, outcome: &Outcome, attempt: u32) -> RetryDecision {
        let operation = match self.config.read_policy {
            ReadPolicy::Owner => Operation {
                access: Access::Write,
                ..operation
            },
            ReadPolicy::Fleet => operation,
        };
        classify(operation, outcome, attempt, self.config.retry)
    }

    /// Prometheus text for `/metrics`.
    pub fn render_metrics(&self) -> String {
        self.metrics.render(&self.nodes, self.snapshot().replica_count())
    }
}
