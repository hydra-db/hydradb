//! Every decision the turbolay proxy makes, shared by the Bolt and HTTP
//! adapters so they cannot route, admit or retry differently.
//!
//! - [`Placement`]: who serves a route key. Rendezvous over the snapshot is the
//!   only implementation; the trait is async so a future page-server lookup
//!   fits behind it
//! - [`Router`]: writes go to the owner, always; reads follow [`ReadPolicy`].
//!   Owners learned from NotALeader / 421 hints are cached
//! - [`NodeRegistry`]: per node permits, each node's budget divided by the
//!   replica count, and the units in flight that `fleet` reads balance on
//! - [`Metrics`]: the Prometheus registry the admin endpoint renders

#![forbid(unsafe_code)]

mod lifecycle;
mod metrics;
mod nodes;
mod placement;
mod router;

pub use lifecycle::{Lifecycle, LifecycleState};
pub use metrics::{Metrics, RetryClass, RouteOutcome};
pub use nodes::{AdmitError, NodeRegistry, Permit};
pub use placement::{Placement, RendezvousPlacement};
pub use router::{ReadPolicy, Route, RouteError, RouteWriter, Router, RouterConfig};
pub use turbolay_proxy_types as types;
