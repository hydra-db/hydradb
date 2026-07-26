//! Whether this node may serve: the one answer `/readyz` reports and the
//! heartbeat publisher acts on.
//!
//! Two conditions, and both are necessary:
//!
//! - **Lifecycle.** False until every listener is bound, false again from the
//!   first sign of shutdown. This is the condition `/readyz` has always
//!   reported.
//! - **Placement (decision 7 of
//!   `docs/plans/2026-07-25-rendezvous-placement.md`).** A node whose heartbeat
//!   LIST has been failing for longer than the grace window has **shed** its
//!   view of the fleet. It knows nothing about who is live, so it refuses every
//!   promotion with `NotCellWriter` and no owner hint — and it must withdraw
//!   its own heartbeat.
//!
//! The withdrawal is the part that is easy to leave out, and leaving it out is
//! the nastiest failure available in this design. In a partial failure — LIST
//! throttled while PUTs still land — a node that sheds but keeps publishing
//! stays the *computed owner for every one of its peers*. They route every
//! write to it; it refuses every one, because it has shed; and nothing times
//! out, because its heartbeat is still fresh. The cell is permanently
//! unwritable and every node's view of the fleet is self-consistent.
//! Withdrawing is what lets peers age this node out and take over.
//!
//! # Why both consumers read one value
//!
//! `/readyz` and the publisher could each derive readiness for themselves. They
//! must not. Decision 4 deleted the client-side `/readyz` fan-out precisely
//! because two liveness signals that can disagree are worse than one, and
//! presence of the heartbeat object *is* the readiness signal that replaced it
//! — so a node reporting ready while publishing nothing, or the reverse, would
//! reintroduce the inconsistency that deletion removed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use slatedb_graph_kernel::PlacementView;
use turbolay_placement::liveness::HeartbeatAction;

/// The node's readiness, shared by the admin server and the heartbeat
/// publisher.
///
/// Cheap to clone: an `Arc` around the lifecycle flag plus a
/// [`PlacementView`] handle, which is itself a shared snapshot.
#[derive(Clone, Debug)]
pub struct NodeReadiness {
    lifecycle: Arc<AtomicBool>,
    placement: PlacementView,
}

impl NodeReadiness {
    /// A node that is not yet ready. The lifecycle flag starts false and is
    /// raised by [`NodeReadiness::mark_ready`] once the listeners are up.
    pub fn new(placement: PlacementView) -> Self {
        Self {
            lifecycle: Arc::new(AtomicBool::new(false)),
            placement,
        }
    }

    /// The listeners are bound and this node can serve.
    pub fn mark_ready(&self) {
        self.lifecycle.store(true, Ordering::Release);
    }

    /// Shutting down, or otherwise unable to serve.
    pub fn mark_unready(&self) {
        self.lifecycle.store(false, Ordering::Release);
    }

    /// What the publisher must do with this node's heartbeat object right now.
    ///
    /// [`HeartbeatAction::Publish`] only while both conditions hold. Either one
    /// failing means DELETE, not "skip the PUT": presence of the object is the
    /// readiness signal, so a stale object left behind would keep advertising a
    /// node that is not serving until it aged out.
    pub fn heartbeat_action(&self) -> HeartbeatAction {
        if self.lifecycle.load(Ordering::Acquire) {
            self.placement.heartbeat_action()
        } else {
            HeartbeatAction::Withdraw
        }
    }

    /// Whether `/readyz` should answer 200 — exactly when the publisher would
    /// publish.
    pub fn is_ready(&self) -> bool {
        matches!(self.heartbeat_action(), HeartbeatAction::Publish)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use slatedb::object_store::{
        self as object_store, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use slatedb_graph_kernel::PlacementConfig;
    use turbolay_placement::liveness::LiveView;

    const FLEET: &[&str] = &["graph-node-0", "graph-node-1"];

    fn readiness() -> NodeReadiness {
        NodeReadiness::new(
            PlacementView::new(
                "graph-node-0",
                FLEET.iter().copied(),
                PlacementConfig::default(),
            )
            .expect("a valid fleet"),
        )
    }

    #[test]
    fn a_node_is_unready_until_its_listeners_are_up() {
        let readiness = readiness();
        assert!(!readiness.is_ready());
        assert_eq!(readiness.heartbeat_action(), HeartbeatAction::Withdraw);

        readiness.mark_ready();
        assert!(readiness.is_ready());
        assert_eq!(readiness.heartbeat_action(), HeartbeatAction::Publish);

        readiness.mark_unready();
        assert!(!readiness.is_ready());
        assert_eq!(readiness.heartbeat_action(), HeartbeatAction::Withdraw);
    }

    /// Decision 7's obligation, at the seam where it is discharged. A node that
    /// has shed its view must stop publishing even though its lifecycle is
    /// perfectly healthy — otherwise it stays the computed owner for every peer
    /// while refusing every write they send it.
    ///
    /// The shed view is produced the way production produces it: a LIST that
    /// keeps failing past the grace window. `PlacementView` seeds its grace
    /// clock at process start, so a node whose LIST has never worked sheds one
    /// `heartbeat_timeout` in — which is why this needs no clock control.
    #[tokio::test]
    async fn a_shed_placement_view_withdraws_the_heartbeat_of_a_healthy_node() {
        let store = InMemory::new();
        let placement = PlacementView::new(
            "graph-node-0",
            FLEET.iter().copied(),
            PlacementConfig {
                // Compressed so the grace window closes inside the test. The
                // rule is `since_last_success >= heartbeat_timeout`, not a
                // fixed 15s.
                heartbeat_interval: Duration::from_millis(1),
                heartbeat_timeout: Duration::from_millis(20),
            },
        )
        .expect("a valid fleet");
        let readiness = NodeReadiness::new(placement.clone());
        readiness.mark_ready();

        // A store that answers LIST normally: the view is fresh and the node
        // publishes.
        let _ = placement.refresh(&store, &Path::from("graph/data")).await;
        assert!(readiness.is_ready());
        assert_eq!(readiness.heartbeat_action(), HeartbeatAction::Publish);

        // Now nothing succeeds. Past the grace window the view sheds, and the
        // publisher's answer flips even though nothing about the process
        // changed.
        tokio::time::sleep(Duration::from_millis(30)).await;
        let shed = placement
            .refresh(&FailingStore::new(), &Path::from("graph/data"))
            .await;
        assert!(matches!(*shed, LiveView::Shed));
        assert_eq!(readiness.heartbeat_action(), HeartbeatAction::Withdraw);
        assert!(
            !readiness.is_ready(),
            "a node that cannot read the live set is not ready"
        );
    }

    /// The smallest store that fails everything. `crates/placement`'s
    /// `FaultStore` is `#[cfg(test)]` inside that crate and cannot be imported
    /// here; this needs only enough to make one LIST fail.
    #[derive(Debug)]
    struct FailingStore;

    impl FailingStore {
        fn new() -> Self {
            Self
        }

        fn error() -> object_store::Error {
            object_store::Error::Generic {
                store: "FailingStore",
                source: "injected LIST fault".into(),
            }
        }
    }

    impl std::fmt::Display for FailingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FailingStore {
        async fn put_opts(
            &self,
            _location: &Path,
            _payload: PutPayload,
            _opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            Err(Self::error())
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            Err(Self::error())
        }

        async fn get_opts(
            &self,
            _location: &Path,
            _options: GetOptions,
        ) -> object_store::Result<GetResult> {
            Err(Self::error())
        }

        fn delete_stream(
            &self,
            _locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            futures::stream::once(async { Err(Self::error()) }).boxed()
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            futures::stream::once(async { Err(Self::error()) }).boxed()
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            Err(Self::error())
        }

        async fn copy_opts(
            &self,
            _from: &Path,
            _to: &Path,
            _options: CopyOptions,
        ) -> object_store::Result<()> {
            Err(Self::error())
        }
    }
}
