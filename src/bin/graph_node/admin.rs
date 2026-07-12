use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use slatedb_graph_kernel::{ClientQueryService, GraphControlClient, Result};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Clone)]
struct AdminState {
    ready: Arc<AtomicBool>,
    control: Option<Arc<dyn GraphControlClient>>,
    query: Option<ClientQueryService>,
}

pub struct AdminServer {
    local_addr: SocketAddr,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl AdminServer {
    pub async fn bind<C>(
        addr: SocketAddr,
        ready: Arc<AtomicBool>,
        control: Arc<C>,
        query: Option<ClientQueryService>,
    ) -> Result<Self>
    where
        C: GraphControlClient + 'static,
    {
        let control: Arc<dyn GraphControlClient> = control;
        Self::bind_inner(addr, ready, Some(control), query).await
    }

    pub async fn bind_without_control(addr: SocketAddr, ready: Arc<AtomicBool>) -> Result<Self> {
        Self::bind_inner(addr, ready, None, None).await
    }

    async fn bind_inner(
        addr: SocketAddr,
        ready: Arc<AtomicBool>,
        control: Option<Arc<dyn GraphControlClient>>,
        query: Option<ClientQueryService>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await.map_err(admin_io_error)?;
        let local_addr = listener.local_addr().map_err(admin_io_error)?;
        let state = AdminState {
            ready,
            control,
            query,
        };
        let router = Router::new()
            .route("/livez", get(live))
            .route("/readyz", get(readiness))
            .route("/metrics", get(metrics))
            .with_state(state);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    while stop_rx.changed().await.is_ok() {
                        if *stop_rx.borrow() {
                            return;
                        }
                    }
                })
                .await
                .map_err(admin_io_error)
        });
        Ok(Self {
            local_addr,
            stop_tx,
            task,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn stop(self) -> Result<()> {
        let _ = self.stop_tx.send(true);
        self.task
            .await
            .map_err(|err| slatedb_graph_kernel::GraphError::CorruptValue {
                key: "runtime/admin".to_string(),
                reason: err.to_string(),
            })?
    }
}

async fn live() -> StatusCode {
    StatusCode::OK
}

async fn readiness(State(state): State<AdminState>) -> StatusCode {
    if state.ready.load(Ordering::Acquire) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics(State(state): State<AdminState>) -> Response {
    let mut output = format!(
        "# TYPE graph_runtime_ready gauge\ngraph_runtime_ready {}\n",
        u8::from(state.ready.load(Ordering::Acquire)),
    );
    if let Some(control) = state.control {
        let control = match control.metrics().await {
            Ok(metrics) => metrics,
            Err(error) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("control metrics unavailable: {error}\n"),
                )
                    .into_response();
            }
        };
        output.push_str(&format!(
            concat!(
                "# TYPE graph_control_controller_runs counter\n",
                "graph_control_controller_runs {}\n",
                "# TYPE graph_control_controller_failures counter\n",
                "graph_control_controller_failures {}\n",
                "# TYPE graph_control_lease_renew_failures counter\n",
                "graph_control_lease_renew_failures {}\n",
                "# TYPE graph_control_lease_renew_lost counter\n",
                "graph_control_lease_renew_lost {}\n",
                ""
            ),
            control.controller_runs,
            control.controller_failures,
            control.lease_renew_failures,
            control.lease_renew_lost,
        ));
    }
    if let Some(query) = state.query {
        let query = query.metrics();
        output.push_str(&format!(
            concat!(
                "# TYPE graph_query_started counter\n",
                "graph_query_started {}\n",
                "# TYPE graph_query_completed counter\n",
                "graph_query_completed {}\n",
                "# TYPE graph_query_failed counter\n",
                "graph_query_failed {}\n",
                "# TYPE graph_query_auth_failures counter\n",
                "graph_query_auth_failures {}\n",
                "# TYPE graph_query_scope_denials counter\n",
                "graph_query_scope_denials {}\n"
            ),
            query.queries_started,
            query.queries_completed,
            query.queries_failed,
            query.auth_failures,
            query.scope_denials,
        ));
    }
    (
        [
            ("content-type", "text/plain; version=0.0.4; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        output,
    )
        .into_response()
}

fn admin_io_error(error: std::io::Error) -> slatedb_graph_kernel::GraphError {
    slatedb_graph_kernel::GraphError::CorruptValue {
        key: "runtime/admin".to_string(),
        reason: error.to_string(),
    }
}
