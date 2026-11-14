//! A unit of work: one RUN through the final PULL summary.
//!
//! The unit is routed by the core, admitted, and run on one pooled backend
//! connection, pinned from RUN to the final summary. Records are forwarded
//! whole without decoding; only summaries are decoded.

use std::time::{Duration, Instant};

use hydradb_protocol::bolt::{decode_summary, failure, run_extra, sig, success, ClientMessage, Message, ServerMessage, Summary};
use hydradb_protocol::boltr::error::BoltError;
use hydradb_protocol::boltr::types::{BoltDict, BoltValue};
use hydradb_protocol::{codes, owner::parse_owner_hint};
use tokio::io::{AsyncRead, AsyncWrite};
use turbolay_proxy_core::types::{Access, NodeId, Operation, Outcome, Protocol, RetryDecision, RouteKey};
use turbolay_proxy_core::{AdmitError, Permit, Placement, RetryClass, Route, RouteOutcome};

use crate::pool::{release, PooledConn};
use crate::session::Session;
use crate::BoltProxy;

/// How the session continues after a unit.
pub(crate) enum UnitEnd {
    /// The result reached its final summary: the statement committed.
    Completed(Completed),
    /// The client RESET with the result still open.
    Reset,
    Failed,
    Closed,
}

/// What a transaction needs to know about a unit that committed.
pub(crate) struct Completed {
    /// The node's final summary says the statement only read (`type: "r"`).
    /// Not the declared mode, which a node does not enforce; a summary
    /// without `type` counts as a write, so a ROLLBACK is never answered
    /// SUCCESS on a guess.
    pub(crate) read: bool,
    /// The final summary's `bookmark`, if the node sent one.
    pub(crate) bookmark: Option<String>,
}

impl<P: Placement> BoltProxy<P> {
    pub(crate) async fn run_unit<S>(&self, session: &mut Session<S>, run: Message) -> Result<UnitEnd, BoltError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let extra = match run_extra(run.body()) {
            Ok(extra) => extra,
            Err(error) => return refuse(session, codes::REQUEST_INVALID, &error.to_string()).await,
        };
        let key = match self.resolver.resolve(extra.db.as_deref()) {
            Ok(key) => key,
            Err(error) => return refuse(session, codes::STATEMENT_INVALID_SYNTAX, &error.to_string()).await,
        };
        let operation = Operation {
            access: if extra.read { Access::Read } else { Access::Write },
            protocol: Protocol::Bolt,
        };
        self.router.metrics().unit(operation.protocol, operation.access);
        let Ok(route) = self.router.route(&key, operation).await else {
            return refuse(session, codes::DATABASE_UNAVAILABLE, "no graph node is configured").await;
        };
        Unit {
            proxy: self,
            session,
            run,
            pull: None,
            answered: Answered::default(),
            tally: Tally::default(),
            key,
            operation,
        }
        .drive(route)
        .await
    }
}

/// Answer the RUN with a FAILURE before the unit reached any node.
async fn refuse<S>(session: &mut Session<S>, code: &str, message: &str) -> Result<UnitEnd, BoltError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    session.writer.send(&failure(code, message)).await?;
    Ok(UnitEnd::Failed)
}

/// One unit in flight, across every attempt at it.
///
/// Every client request gets exactly one answer: the RUN a SUCCESS or FAILURE,
/// a PULL its records and a summary, or IGNORED once the unit has failed. A
/// retry is invisible to the client: a second RUN SUCCESS is swallowed, and a
/// retry is only taken before any record reached the client.
struct Unit<'a, P: Placement, S> {
    proxy: &'a BoltProxy<P>,
    session: &'a mut Session<S>,
    run: Message,
    /// The client's PULL, once read, so a retried attempt can send it with
    /// the RUN. The first attempt sends the RUN alone: drivers write RUN and
    /// PULL together, but the reader cannot tell whether the PULL is already
    /// buffered without waiting for it (see `hydradb_protocol::bolt::frame`).
    pull: Option<Message>,
    answered: Answered,
    /// What the current attempt measured, recorded when it ends.
    tally: Tally,
    key: RouteKey,
    operation: Operation,
}

/// What the client has been told so far in this unit.
#[derive(Default)]
struct Answered {
    /// The RUN has its SUCCESS or FAILURE.
    run: bool,
    /// A PULL or DISCARD was taken from the client and not yet answered.
    pull_pending: bool,
    /// A record or a PULL summary reached the client. No retry after this.
    stream_started: bool,
}

/// Measurements of one attempt, for the node's metrics.
#[derive(Default)]
struct Tally {
    checkout: Option<Duration>,
    bytes: u64,
}

/// One attempt's result.
struct Attempt {
    outcome: Outcome,
    /// The node's own FAILURE, forwarded if the unit gives up on it.
    failure: Option<Message>,
    end: UnitEnd,
    /// The metric label when it is not derived from `outcome`.
    label: Option<RouteOutcome>,
}

impl Attempt {
    fn of(outcome: Outcome) -> Self {
        Self::new(outcome, None, UnitEnd::Failed)
    }

    fn new(outcome: Outcome, failure: Option<Message>, end: UnitEnd) -> Self {
        Self {
            outcome,
            failure,
            end,
            label: None,
        }
    }

    /// The node refused with NotALeader; its FAILURE may name the owner.
    fn refused(summary: &ServerMessage, frame: Message) -> Self {
        let owner = summary.message().and_then(parse_owner_hint).map(NodeId::new);
        Self::new(Outcome::RefusedNotOwner { owner }, Some(frame), UnitEnd::Failed)
    }
}

/// Why an attempt stopped: it reached an outcome, or writing to the client
/// failed, which ends the session.
enum Exit {
    Attempt(Attempt),
    Client(BoltError),
}

impl From<Attempt> for Exit {
    fn from(attempt: Attempt) -> Self {
        Self::Attempt(attempt)
    }
}

impl From<BoltError> for Exit {
    fn from(error: BoltError) -> Self {
        Self::Client(error)
    }
}

/// What follows a failed attempt.
enum Next {
    Retry { target: NodeId, delay: Option<Duration> },
    GiveUp,
}

/// Where the retry loop is: which attempt, at which candidate.
struct Tries {
    attempt: u32,
    candidate: usize,
}

impl<P: Placement, S> Unit<'_, P, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Attempt the route's candidates until one finishes the unit or the
    /// retry policy gives up.
    async fn drive(mut self, route: Route) -> Result<UnitEnd, BoltError> {
        let mut tries = Tries { attempt: 1, candidate: 0 };
        let mut target = route.candidates[0].clone();
        loop {
            let result = self.attempt(&target).await?;
            self.record(&target, Some(&result));
            if matches!(result.outcome, Outcome::Completed | Outcome::NodeError) {
                return Ok(result.end);
            }
            match self.next(&result, &target, &route, &mut tries) {
                Next::Retry { target: next, delay } => {
                    if let Some(delay) = delay {
                        tokio::time::sleep(delay).await;
                    }
                    target = next;
                }
                Next::GiveUp => {
                    self.give_up(&result, &target).await?;
                    return Ok(UnitEnd::Failed);
                }
            }
        }
    }

    /// The retry policy's answer to a failed attempt at `target`.
    fn next(&self, result: &Attempt, target: &NodeId, route: &Route, tries: &mut Tries) -> Next {
        let router = &self.proxy.router;
        let owner = match &result.outcome {
            Outcome::RefusedNotOwner { owner } => router.learn_refusal(&self.key, target, owner.as_ref().map(NodeId::as_str)),
            _ => None,
        };
        // `classify` gives up on `RowsSent`; this covers any other outcome
        // reported after a record already reached the client.
        let decision = if self.answered.stream_started && result.outcome != Outcome::RowsSent {
            RetryDecision::GiveUp
        } else {
            router.classify(self.operation, &result.outcome, tries.attempt)
        };
        let (target, delay) = match decision {
            // A hint naming a node outside the snapshot cannot be reached; the
            // client gets the node's own refusal.
            RetryDecision::RetryAt(_) => match owner {
                Some(owner) => (owner, None),
                None => return Next::GiveUp,
            },
            RetryDecision::RetrySame => (target.clone(), Some(Duration::from_millis(20 * u64::from(tries.attempt)))),
            RetryDecision::RetryNext if tries.candidate + 1 < route.candidates.len() => {
                tries.candidate += 1;
                (route.candidates[tries.candidate].clone(), None)
            }
            RetryDecision::RetryNext | RetryDecision::GiveUp => return Next::GiveUp,
        };
        router.metrics().retry(retry_class(&result.outcome));
        tries.attempt += 1;
        Next::Retry { target, delay }
    }

    /// The node's metrics for one attempt, all written here. `result` is
    /// `None` when the client went away mid-attempt: what was measured still
    /// counts, but there is no outcome.
    fn record(&mut self, target: &NodeId, result: Option<&Attempt>) {
        let tally = std::mem::take(&mut self.tally);
        let Some(node) = self.proxy.router.nodes().get(target) else {
            return;
        };
        if let Some(result) = result {
            let label = result.label.unwrap_or_else(|| route_outcome(&result.outcome));
            node.record(self.operation.protocol, self.operation.access, label);
        }
        if let Some(checkout) = tally.checkout {
            node.counters.pool_waits.inc();
            node.counters.pool_wait_nanos.inc_by(checkout.as_nanos() as u64);
        }
        node.counters.bytes_relayed.inc_by(tally.bytes);
    }

    /// One attempt at `target`: connect, RUN, then PULL pages until the
    /// result ends.
    async fn attempt(&mut self, target: &NodeId) -> Result<Attempt, BoltError> {
        match self.attempt_steps(target).await {
            Err(Exit::Attempt(attempt)) => Ok(attempt),
            Err(Exit::Client(error)) => {
                self.record(target, None);
                Err(error)
            }
            Ok(never) => match never {},
        }
    }

    /// Each step hands the connection on, or ends the attempt having pooled
    /// or dropped it.
    async fn attempt_steps(&mut self, target: &NodeId) -> Result<std::convert::Infallible, Exit> {
        let (conn, _permit) = self.connect(target).await?;
        let mut pull_sent = self.pull.is_some();
        let mut conn = self.send_run(conn, pull_sent).await?;
        loop {
            if !pull_sent {
                conn = self.take_pull(conn).await?;
            }
            conn = self.relay_page(conn).await?;
            pull_sent = false;
        }
    }

    /// Admit the unit to `target` and check out a connection to it.
    async fn connect(&mut self, target: &NodeId) -> Result<(PooledConn, Permit), Exit> {
        // Address lookup under a short guard: no reference taken on the
        // snapshot, nothing held across the awaits below.
        let pool = {
            let snapshot = self.proxy.router.snapshot();
            let (_, node) = snapshot.node(target.as_str()).ok_or(Attempt::of(Outcome::ConnectFailed))?;
            self.proxy.pools.pool(target, &node.bolt_addr)
        };
        let permit = match self.proxy.router.admit(target).await {
            Ok(permit) => permit,
            // Admission refusals are not retried: the caller should back off.
            Err(AdmitError::Timeout) => {
                let budget = failure(
                    codes::RESOURCE_EXHAUSTED,
                    &format!("graph node {target} is at its concurrency budget"),
                );
                self.answer_failure(&Message::encode(&budget)).await?;
                return Err(Attempt {
                    label: Some(RouteOutcome::AdmitTimeout),
                    ..Attempt::of(Outcome::NodeError)
                }
                .into());
            }
            Err(AdmitError::UnknownNode) => return Err(Attempt::of(Outcome::ConnectFailed).into()),
        };
        let checkout = Instant::now();
        let mut conn = pool.checkout().await.map_err(|error| {
            tracing::warn!(node = %target, %error, "backend connection failed");
            Attempt::of(Outcome::ConnectFailed)
        })?;
        self.tally.checkout = Some(checkout.elapsed());
        // Not READY until this unit ends in READY: a return that skips
        // `release`, such as a client write failing, drops the connection
        // instead of pooling one left mid-result.
        conn.ready = false;
        Ok((conn, permit))
    }

    /// Send the RUN, with the kept PULL on a retry, and answer it from the
    /// node's summary.
    async fn send_run(&mut self, mut conn: PooledConn, pull_sent: bool) -> Result<PooledConn, Exit> {
        let lost = if pull_sent {
            Outcome::LostAfterPull
        } else {
            Outcome::LostBeforePull
        };
        if send_backend(&mut conn, &self.run, self.pull.as_ref()).await.is_err() {
            return Err(drop_conn(conn, lost));
        }
        let Some(frame) = next_backend(&mut conn).await else {
            return Err(drop_conn(conn, lost));
        };
        let Ok(summary) = decode_summary(frame.body()) else {
            return Err(drop_conn(conn, lost));
        };
        match summary {
            ServerMessage::Success { .. } => {
                // A retry's RUN SUCCESS is swallowed: the client has one.
                if !self.answered.run {
                    self.session.writer.send(&frame).await?;
                    self.answered.run = true;
                }
                Ok(conn)
            }
            ServerMessage::Failure { .. } => {
                // A PULL sent with the RUN is IGNORED by the node; drain it,
                // then RESET so the connection can go back to the pool.
                self.reset_and_release(conn, usize::from(pull_sent)).await;
                if summary.code() == Some(codes::NOT_A_LEADER) {
                    return Err(Attempt::refused(&summary, frame).into());
                }
                self.answer_failure(&frame).await?;
                Err(Attempt::of(Outcome::NodeError).into())
            }
            ServerMessage::Ignored | ServerMessage::Record { .. } => Err(drop_conn(conn, lost)),
        }
    }

    /// Read the client's next request while the result is open: a PULL or
    /// DISCARD goes to the node; RESET, GOODBYE or anything else ends the unit.
    async fn take_pull(&mut self, mut conn: PooledConn) -> Result<PooledConn, Exit> {
        self.session.writer.flush().await?;
        let request = self.session.reader.next().await?;
        match request.tag {
            sig::PULL | sig::DISCARD => {
                let sent = send_backend(&mut conn, &request, None).await;
                self.pull = Some(request);
                self.answered.pull_pending = true;
                if sent.is_err() {
                    return Err(drop_conn(conn, Outcome::LostAfterPull));
                }
                Ok(conn)
            }
            sig::RESET => {
                self.reset_and_release(conn, 0).await;
                self.session.writer.send(&success(BoltDict::new())).await?;
                Err(Attempt::new(Outcome::Completed, None, UnitEnd::Reset).into())
            }
            sig::GOODBYE => {
                release(conn, false);
                Err(Attempt::new(Outcome::Completed, None, UnitEnd::Closed).into())
            }
            _ => {
                self.reset_and_release(conn, 0).await;
                let invalid = failure(codes::REQUEST_INVALID, "message is invalid while a result is open");
                self.session.writer.send(&invalid).await?;
                Err(Attempt::of(Outcome::NodeError).into())
            }
        }
    }

    /// Relay one page: records until a summary. Returns when the node has
    /// more to send; every other summary ends the attempt.
    async fn relay_page(&mut self, mut conn: PooledConn) -> Result<PooledConn, Exit> {
        let frame = loop {
            let Some(frame) = next_backend(&mut conn).await else {
                return Err(drop_conn(conn, self.stream_lost()));
            };
            if !frame.is_record() {
                break frame;
            }
            self.answered.stream_started = true;
            self.tally.bytes += frame.body().len() as u64;
            // The writer reaches the client once its buffer fills, so a large
            // result streams instead of accumulating.
            self.session.writer.send(&frame).await?;
        };
        let Ok(summary) = decode_summary(frame.body()) else {
            return Err(drop_conn(conn, self.stream_lost()));
        };
        match summary {
            ServerMessage::Success { ref metadata } => {
                self.session.writer.send(&frame).await?;
                self.answered.pull_pending = false;
                self.answered.stream_started = true;
                if summary.has_more() {
                    return Ok(conn);
                }
                release(conn, true);
                let completed = Completed {
                    read: matches!(metadata.get("type").and_then(BoltValue::as_str), Some("r")),
                    bookmark: metadata.get("bookmark").and_then(BoltValue::as_str).map(str::to_string),
                };
                Err(Attempt::new(Outcome::Completed, None, UnitEnd::Completed(completed)).into())
            }
            // Refused before anything ran: the PULL is where a node finds out
            // it is not the owner. Still a refusal, still retryable.
            ServerMessage::Failure { .. } if summary.code() == Some(codes::NOT_A_LEADER) && !self.answered.stream_started => {
                self.reset_and_release(conn, 0).await;
                Err(Attempt::refused(&summary, frame).into())
            }
            ServerMessage::Failure { .. } | ServerMessage::Ignored | ServerMessage::Record { .. } => {
                self.session.writer.send(&frame).await?;
                self.answered.pull_pending = false;
                self.reset_and_release(conn, 0).await;
                Err(Attempt::of(Outcome::NodeError).into())
            }
        }
    }

    /// A connection lost mid-stream is retryable only if nothing reached the
    /// client.
    fn stream_lost(&self) -> Outcome {
        if self.answered.stream_started {
            Outcome::RowsSent
        } else {
            Outcome::LostAfterPull
        }
    }

    /// Tell the client the unit failed: the node's own FAILURE when there is
    /// one, else one naming the node.
    async fn give_up(&mut self, result: &Attempt, target: &NodeId) -> Result<(), BoltError> {
        let answer = match (&result.failure, &result.outcome) {
            (Some(failure), _) => failure.clone(),
            (None, Outcome::ConnectFailed) => Message::encode(&failure(
                codes::DATABASE_UNAVAILABLE,
                &format!("graph node {target} is unreachable"),
            )),
            (None, Outcome::RefusedNotOwner { .. }) => {
                Message::encode(&failure(codes::NOT_A_LEADER, &format!("graph node {target} refused the write")))
            }
            (None, _) => Message::encode(&failure(
                codes::DATABASE_UNAVAILABLE,
                &format!("connection to graph node {target} was lost"),
            )),
        };
        self.answer_failure(&answer).await
    }

    /// Answer whatever the client still waits on with `answer`: the RUN, and
    /// IGNORED for a PULL behind it; or, with the RUN answered, the PULL.
    async fn answer_failure(&mut self, answer: &Message) -> Result<(), BoltError> {
        let writer = &mut self.session.writer;
        if !self.answered.run {
            writer.send(answer).await?;
            self.answered.run = true;
            if self.answered.pull_pending {
                writer.send(&ServerMessage::Ignored).await?;
            }
        } else if self.answered.pull_pending {
            writer.send(answer).await?;
        }
        self.answered.pull_pending = false;
        Ok(())
    }

    /// RESET the connection, then pool it if it came back READY. A node that
    /// does not answer in time loses the connection, so cleanup never holds
    /// up the client's answer.
    async fn reset_and_release(&self, mut conn: PooledConn, outstanding: usize) {
        let reset = reset_backend(&mut conn, outstanding);
        let ready = tokio::time::timeout(self.proxy.config.pool.reset_timeout, reset)
            .await
            .unwrap_or(false);
        release(conn, ready);
    }
}

/// Drop a connection in an unknown state and end the attempt with `outcome`.
fn drop_conn(conn: PooledConn, outcome: Outcome) -> Exit {
    release(conn, false);
    Attempt::of(outcome).into()
}

async fn next_backend(conn: &mut PooledConn) -> Option<Message> {
    conn.reader.next().await.ok()
}

/// Send `first`, then `second` if any, to the backend in one write.
async fn send_backend(conn: &mut PooledConn, first: &Message, second: Option<&Message>) -> Result<(), BoltError> {
    conn.writer.send(first).await?;
    if let Some(second) = second {
        conn.writer.send(second).await?;
    }
    conn.writer.flush().await
}

/// Send RESET and read past the answers still owed: `outstanding` requests
/// already sent, then the RESET's own SUCCESS. True when the connection ended
/// up READY.
async fn reset_backend(conn: &mut PooledConn, outstanding: usize) -> bool {
    let backend = &mut **conn;
    if backend.writer.send(&ClientMessage::Reset).await.is_err() || backend.writer.flush().await.is_err() {
        return false;
    }
    let mut owed = outstanding + 1;
    loop {
        let Ok(frame) = backend.reader.next().await else {
            return false;
        };
        if frame.is_record() {
            continue;
        }
        owed -= 1;
        if owed == 0 {
            return frame.tag == sig::SUCCESS;
        }
    }
}

fn route_outcome(outcome: &Outcome) -> RouteOutcome {
    match outcome {
        Outcome::Completed => RouteOutcome::Ok,
        Outcome::NodeError => RouteOutcome::NodeError,
        Outcome::RefusedNotOwner { .. } => RouteOutcome::RefusedNotOwner,
        Outcome::ConnectFailed => RouteOutcome::ConnectFailed,
        Outcome::LostBeforePull => RouteOutcome::LostBeforePull,
        Outcome::LostAfterPull | Outcome::RowsSent => RouteOutcome::LostAfterPull,
    }
}

fn retry_class(outcome: &Outcome) -> RetryClass {
    match outcome {
        Outcome::ConnectFailed => RetryClass::ConnectFailed,
        Outcome::RefusedNotOwner { .. } => RetryClass::NotOwner,
        _ => RetryClass::LostBeforePull,
    }
}
