//! One process-wide window for both traffic and timings. In-flight requests
//! stay attached to their original window, which is frozen before collection.
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::core::histogram::{AtomicDurationHistogram, DurationHistogramSnapshot};
use arc_swap::ArcSwapOption;

use super::{RemoteObjectStoreCounters, RemoteObjectStoreSnapshot};

#[derive(Default)]
pub(super) struct Timings {
    closed: RwLock<bool>,
    counters: RemoteObjectStoreCounters,
    pub(super) get_response: AtomicDurationHistogram,
    pub(super) get_body_chunk: AtomicDurationHistogram,
    pub(super) put: AtomicDurationHistogram,
    pub(super) upload_part: AtomicDurationHistogram,
}

impl Timings {
    fn record<T>(&self, record: impl FnOnce(&Self) -> T) -> Option<T> {
        let closed = self.closed.read().unwrap();
        (!*closed).then(|| record(self))
    }

    pub(super) fn record_chunk(&self, duration: Duration) {
        self.record(|p| p.get_body_chunk.record(duration));
    }

    fn start(self: Arc<Self>, request: Request) -> Option<RequestTimer> {
        let started = self.record(|p| {
            match request {
                Request::Get { ranged } => p.counters.note_get(ranged),
                Request::Put { bytes } | Request::UploadPart { bytes } => p.counters.note_put(bytes),
            }
            Instant::now()
        })?;
        Some(RequestTimer {
            timings: self,
            request,
            started,
        })
    }

    fn finish(&self) -> (RemoteObjectStoreSnapshot, [(&'static str, DurationHistogramSnapshot); 4]) {
        let mut closed = self.closed.write().unwrap();
        *closed = true;
        (
            self.counters.snapshot(),
            [
                ("get_response", self.get_response.snapshot()),
                ("get_body_chunk", self.get_body_chunk.snapshot()),
                ("put", self.put.snapshot()),
                ("upload_part", self.upload_part.snapshot()),
            ],
        )
    }
}

static ACTIVE: ArcSwapOption<Timings> = ArcSwapOption::const_empty();

#[derive(Clone, Copy)]
pub(super) enum Request {
    Get { ranged: bool },
    Put { bytes: usize },
    UploadPart { bytes: usize },
}

pub(super) struct RequestTimer {
    pub(super) timings: Arc<Timings>,
    request: Request,
    started: Instant,
}

pub(super) fn start(request: Request) -> Option<RequestTimer> {
    ACTIVE.load_full()?.start(request)
}

pub(super) fn note_get_bytes(bytes: usize) {
    if let Some(timings) = &*ACTIVE.load() {
        timings.record(|p| p.counters.get_bytes.fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed));
    }
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        self.timings.record(|p| {
            let histogram = match self.request {
                Request::Get { .. } => &p.get_response,
                Request::Put { .. } => &p.put,
                Request::UploadPart { .. } => &p.upload_part,
            };
            histogram.record(self.started.elapsed());
        });
    }
}

/// Holds the exclusive resource-profiling window until dropped.
pub struct RemoteStorageProfile(Arc<Timings>);

impl RemoteStorageProfile {
    pub fn try_start() -> Option<Self> {
        let timings = Arc::new(Timings::default());
        let previous = ACTIVE.compare_and_swap(&None::<Arc<Timings>>, Some(Arc::clone(&timings)));
        if previous.is_some() {
            None
        } else {
            Some(Self(timings))
        }
    }

    /// Freeze counts and timings at the same boundary, then release the slot.
    /// Timings for requests that have not finished are omitted.
    pub fn finish(self) -> (RemoteObjectStoreSnapshot, [(&'static str, DurationHistogramSnapshot); 4]) {
        self.0.finish()
    }
}

impl Drop for RemoteStorageProfile {
    fn drop(&mut self) {
        *self.0.closed.write().unwrap() = true;
        ACTIVE.store(None);
    }
}
