//! The owner's routes: every writer a node holds as one stream of server-sent
//! events, and confirmation of one writer for a replica's strong read.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt as _, StreamExt as _};

use crate::{node_feed, BuddyWalMetrics, ConfirmServed, WriterDirectory, CONFIRM_ROUTE, FEED_EVENT, FEED_ROUTE};

/// Decides whether a request may subscribe. Supplied by the host, so this
/// crate has no opinion on credentials.
pub type Authorizer = Arc<dyn Fn(&HeaderMap) -> bool + Send + Sync>;

/// How often an idle stream sends a comment, so proxies keep it open and a
/// subscriber can tell a quiet node from a dead connection.
const KEEP_ALIVE: Duration = Duration::from_secs(10);

/// The most feed events sent in one SSE event. Events that are ready together
/// go out together, so a burst across many writers costs one frame, not one
/// per writer.
const MAX_BATCH: usize = 512;

#[derive(Clone)]
struct FeedState {
    directory: Arc<dyn WriterDirectory>,
    authorize: Authorizer,
    metrics: Arc<BuddyWalMetrics>,
    stopping: Shared<BoxFuture<'static, ()>>,
}

/// The feed and confirm routes, ready to merge into a host router.
///
/// `stopping` resolves when the host listener begins to shut down. Every open
/// feed ends then: a feed never finishes on its own, so a server that waits
/// for in-flight responses before stopping would otherwise wait forever.
pub fn feed_router(
    directory: Arc<dyn WriterDirectory>,
    authorize: Authorizer,
    metrics: Arc<BuddyWalMetrics>,
    stopping: impl Future<Output = ()> + Send + 'static,
) -> Router {
    let stopping = stopping.boxed().shared();
    Router::new()
        .route(FEED_ROUTE, get(feed))
        .route(CONFIRM_ROUTE, get(confirm))
        .with_state(FeedState {
            directory,
            authorize,
            metrics,
            stopping,
        })
}

fn refuse(status: StatusCode, reason: &'static str, detail: impl Into<String>) -> Response {
    (status, Json(serde_json::json!({ "error": reason, "detail": detail.into() }))).into_response()
}

fn unauthorized() -> Response {
    refuse(StatusCode::UNAUTHORIZED, "unauthorized", "a bearer token is required")
}

#[derive(serde::Deserialize)]
struct ConfirmQuery {
    db: String,
}

async fn confirm(State(state): State<FeedState>, headers: HeaderMap, Query(query): Query<ConfirmQuery>) -> Response {
    if !(state.authorize)(&headers) {
        return unauthorized();
    }
    match state.directory.confirm(&query.db).await {
        Ok(Some(confirmed)) => {
            state.metrics.confirm_served(ConfirmServed::Confirmed);
            Json(confirmed).into_response()
        }
        Ok(None) => {
            state.metrics.confirm_served(ConfirmServed::NotOwner);
            refuse(
                StatusCode::CONFLICT,
                "not_owner",
                format!("this node holds no current writer for {}", query.db),
            )
        }
        Err(detail) => {
            state.metrics.confirm_served(ConfirmServed::Unavailable);
            refuse(StatusCode::SERVICE_UNAVAILABLE, "unavailable", detail)
        }
    }
}

async fn feed(State(state): State<FeedState>, headers: HeaderMap) -> Response {
    if !(state.authorize)(&headers) {
        return unauthorized();
    }
    // Counted for as long as the response streams.
    let subscription = state.metrics.feed_subscription();
    let batches = node_feed(state.directory)
        .take_until(state.stopping)
        .ready_chunks(MAX_BATCH)
        .map(move |events| {
            let _subscription = &subscription;
            Ok::<_, Infallible>(
                Event::default()
                    .event(FEED_EVENT)
                    .json_data(events)
                    .unwrap_or_else(|_| Event::default().comment("unserializable feed batch")),
            )
        });
    Sse::new(batches).keep_alive(KeepAlive::new().interval(KEEP_ALIVE)).into_response()
}
