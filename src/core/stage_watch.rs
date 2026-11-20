//! Name the stage a stuck write is waiting in.
//!
//! A repair write on a few prod scopes ran to the 120 s client runtime limit
//! on every retry, while the same statement against a copy of the same scope
//! finished in milliseconds. Its traces held `client.mutate`, `query.page` and
//! `query.admission` and nothing else, so nothing said where the 120 s went.
//!
//! [`watched`] wraps one stage of the write path. While the stage keeps
//! waiting past [`FIRST_REPORT`] it logs one WARN, repeated every
//! [`REPEAT_REPORT`], and if it is then dropped (the client's runtime limit
//! cancelling it) it logs that too. A log line is not subject to span
//! sampling, and it carries the trace id and the span stack's fields (scope,
//! tenant, fingerprint), so the next stuck write names its stage. A stage that
//! finishes before [`FIRST_REPORT`] logs nothing.
//!
//! The wrapper and the stage it wraps live in one heap allocation: a caller's
//! future grows by a pointer, not by the stage's future or the timer. Prod
//! tokio workers run on a 2 MiB stack, and #549 crash-looped every node by
//! growing the write path's futures inline.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tokio::time::Instant;

/// How long a stage may wait before it is reported.
pub(crate) const FIRST_REPORT: Duration = Duration::from_secs(10);

/// How often a stage that keeps waiting is reported again.
pub(crate) const REPEAT_REPORT: Duration = Duration::from_secs(30);

/// One write-path stage, boxed with its watch.
pub(crate) type WatchedStage<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What the watch reports about a stage that waited past [`FIRST_REPORT`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StageReport {
    /// Still waiting after `waited`.
    Waiting { stage: &'static str, waited: Duration },
    /// Finished after `waited`, having been reported as waiting.
    Finished { stage: &'static str, waited: Duration },
    /// Dropped after `waited` without finishing.
    Dropped { stage: &'static str, waited: Duration },
}

/// `future` as the write-path stage `stage`, logged while it waits past
/// [`FIRST_REPORT`] and when it is dropped after that.
pub(crate) fn watched<'a, T, F>(stage: &'static str, future: F) -> WatchedStage<'a, T>
where
    T: Send + 'a,
    F: Future<Output = T> + Send + 'a,
{
    watched_with(stage, future, FIRST_REPORT, REPEAT_REPORT, log_report)
}

fn watched_with<'a, T, F, R>(stage: &'static str, future: F, first: Duration, repeat: Duration, report: R) -> WatchedStage<'a, T>
where
    T: Send + 'a,
    F: Future<Output = T> + Send + 'a,
    R: Fn(StageReport) + Send + Sync + 'a,
{
    Box::pin(async move {
        let started = Instant::now();
        let mut guard = DropReport {
            stage,
            started,
            first,
            report: &report,
            armed: true,
        };
        let mut future = std::pin::pin!(future);
        let mut next = started + first;
        let mut reported = false;
        loop {
            tokio::select! {
                biased;
                output = &mut future => {
                    guard.armed = false;
                    if reported {
                        report(StageReport::Finished { stage, waited: started.elapsed() });
                    }
                    return output;
                }
                () = tokio::time::sleep_until(next) => {
                    reported = true;
                    report(StageReport::Waiting { stage, waited: started.elapsed() });
                    next += repeat;
                }
            }
        }
    })
}

/// Reports a stage dropped after it waited past the first report.
struct DropReport<'r, R: Fn(StageReport)> {
    stage: &'static str,
    started: Instant,
    first: Duration,
    report: &'r R,
    armed: bool,
}

impl<R: Fn(StageReport)> Drop for DropReport<'_, R> {
    fn drop(&mut self) {
        let waited = self.started.elapsed();
        if self.armed && waited >= self.first {
            (self.report)(StageReport::Dropped { stage: self.stage, waited });
        }
    }
}

fn log_report(report: StageReport) {
    match report {
        StageReport::Waiting { stage, waited } => tracing::warn!(
            hydradb.write.stage = stage,
            waited_ms = waited.as_millis() as u64,
            "write stage still waiting"
        ),
        StageReport::Finished { stage, waited } => tracing::warn!(
            hydradb.write.stage = stage,
            waited_ms = waited.as_millis() as u64,
            "write stage finished after a long wait"
        ),
        StageReport::Dropped { stage, waited } => tracing::warn!(
            hydradb.write.stage = stage,
            waited_ms = waited.as_millis() as u64,
            "write stage dropped while waiting"
        ),
    }
}
