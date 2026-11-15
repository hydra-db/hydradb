use slatedb::object_store::{memory::InMemory, ObjectStore};
use std::sync::{atomic::Ordering, Arc};
struct WalPayloadPause {
    started: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
    completed: tokio::sync::oneshot::Sender<()>,
}

pub struct ReadCountingObjectStore {
    pub inner: Arc<dyn ObjectStore>,
    reads: std::sync::atomic::AtomicU64,
    pub compacted_reads: std::sync::atomic::AtomicU64,
    pub max_compacted_range: std::sync::atomic::AtomicU64,
    pub get_delay_ms: std::sync::atomic::AtomicU64,
    pub wal_put_delay_ms: std::sync::atomic::AtomicU64,
    pub wal_puts: std::sync::atomic::AtomicU64,
    active_gets: std::sync::atomic::AtomicU64,
    pub peak_gets: std::sync::atomic::AtomicU64,
    next_wal_pause: std::sync::Mutex<Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>>,
    #[cfg(feature = "query-transport")]
    pub scope_puts: std::sync::atomic::AtomicU64,
    #[cfg(feature = "query-transport")]
    pub fail_scope_put: std::sync::atomic::AtomicBool,
    next_get_pause: std::sync::Mutex<Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>>,
    wal_payload_pauses: std::sync::Mutex<std::collections::BTreeMap<String, WalPayloadPause>>,
    wal_put_queue: std::sync::Mutex<std::collections::VecDeque<(tokio::sync::oneshot::Sender<String>, tokio::sync::oneshot::Receiver<()>)>>,
    pub ignore_wal_create_only: std::sync::atomic::AtomicBool,
    lose_next_wal_response: std::sync::atomic::AtomicBool,
    lost_response_pause: std::sync::Mutex<Option<(tokio::sync::oneshot::Sender<String>, tokio::sync::oneshot::Receiver<()>)>>,
    strip_object_metadata: std::sync::atomic::AtomicBool,
    conflict_next_wal_put: std::sync::atomic::AtomicBool,
    pub failing_wal_puts: std::sync::atomic::AtomicU64,
    compacted_ranges: std::sync::Mutex<Vec<String>>,
}

impl ReadCountingObjectStore {
    pub fn new() -> Arc<Self> {
        Self::with_inner(Arc::new(InMemory::new()))
    }

    /// The same hooks over another store, so a scenario can run against a real
    /// S3-compatible endpoint as well as `InMemory`.
    pub fn with_inner(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            reads: std::sync::atomic::AtomicU64::new(0),
            compacted_reads: std::sync::atomic::AtomicU64::new(0),
            max_compacted_range: std::sync::atomic::AtomicU64::new(0),
            get_delay_ms: std::sync::atomic::AtomicU64::new(0),
            wal_put_delay_ms: std::sync::atomic::AtomicU64::new(0),
            wal_puts: std::sync::atomic::AtomicU64::new(0),
            active_gets: std::sync::atomic::AtomicU64::new(0),
            peak_gets: std::sync::atomic::AtomicU64::new(0),
            next_wal_pause: std::sync::Mutex::new(None),
            #[cfg(feature = "query-transport")]
            scope_puts: std::sync::atomic::AtomicU64::new(0),
            #[cfg(feature = "query-transport")]
            fail_scope_put: std::sync::atomic::AtomicBool::new(false),
            next_get_pause: std::sync::Mutex::new(None),
            wal_payload_pauses: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            wal_put_queue: std::sync::Mutex::new(std::collections::VecDeque::new()),
            ignore_wal_create_only: std::sync::atomic::AtomicBool::new(false),
            lose_next_wal_response: std::sync::atomic::AtomicBool::new(false),
            lost_response_pause: std::sync::Mutex::new(None),
            strip_object_metadata: std::sync::atomic::AtomicBool::new(false),
            conflict_next_wal_put: std::sync::atomic::AtomicBool::new(false),
            failing_wal_puts: std::sync::atomic::AtomicU64::new(0),
            compacted_ranges: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// GETs and LISTs served so far. Manifest refresh is both, so counting the
    /// two together is what makes a zero mean "never went to the store".
    pub fn reads(&self) -> u64 {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(all(feature = "opencypher", feature = "query-transport"))]
    pub fn compacted_reads(&self) -> u64 {
        self.compacted_reads.load(Ordering::Relaxed)
    }

    #[cfg(all(feature = "opencypher", feature = "query-transport"))]
    pub fn delay_reads_and_reset_peak(&self, millis: u64) {
        self.get_delay_ms.store(millis, Ordering::Relaxed);
        self.peak_gets.store(0, Ordering::Relaxed);
    }

    #[cfg(all(feature = "opencypher", feature = "query-transport"))]
    pub fn peak_reads(&self) -> u64 {
        self.peak_gets.load(Ordering::Relaxed)
    }

    /// Returns and clears the compacted SST ranges read since the last call,
    /// each as "path range".
    pub fn take_compacted_ranges(&self) -> Vec<String> {
        std::mem::take(&mut *self.compacted_ranges.lock().unwrap())
    }

    pub fn pause_next_wal(&self) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *self.next_wal_pause.lock().unwrap() = Some((started_tx, release_rx));
        (started_rx, release_tx)
    }

    /// Hold WAL PUTs in arrival order, one per call; `started` yields the WAL
    /// object path being written. Independent of `pause_next_wal`.
    pub fn queue_wal_put_pause(&self) -> (tokio::sync::oneshot::Receiver<String>, tokio::sync::oneshot::Sender<()>) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        self.wal_put_queue.lock().unwrap().push_back((started_tx, release_rx));
        (started_rx, release_tx)
    }

    /// Negative control: let WAL PUTs overwrite instead of failing when the
    /// object exists, as a store without create-only semantics would.
    pub fn set_ignore_wal_create_only(&self, ignore: bool) {
        self.ignore_wal_create_only.store(ignore, Ordering::SeqCst);
    }

    /// The next WAL PUT lands in the store, but its caller sees a timeout.
    pub fn lose_next_wal_response(&self) {
        self.lose_next_wal_response.store(true, Ordering::SeqCst);
    }

    /// As `lose_next_wal_response`, holding the lost reply after the object
    /// landed; `started` yields the WAL object path.
    pub fn lose_next_wal_response_and_pause(&self) -> (tokio::sync::oneshot::Receiver<String>, tokio::sync::oneshot::Sender<()>) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *self.lost_response_pause.lock().unwrap() = Some((started_tx, release_rx));
        self.lose_next_wal_response();
        (started_rx, release_tx)
    }

    /// Drop user metadata from GET and HEAD responses, as an S3-compatible
    /// layer that does not return `x-amz-meta-*` would.
    pub fn set_strip_object_metadata(&self, strip: bool) {
        self.strip_object_metadata.store(strip, Ordering::SeqCst);
    }

    /// Reject the next WAL PUT as S3 rejects a conditional PUT that races
    /// another in-flight one (409): object_store reports `AlreadyExists`
    /// although no object is visible yet.
    pub fn conflict_next_wal_put(&self) {
        self.conflict_next_wal_put.store(true, Ordering::SeqCst);
    }

    /// Fail the next `count` WAL PUTs with a retryable error before they reach
    /// the store; `u64::MAX` fails them until reset to 0.
    pub fn fail_wal_puts(&self, count: u64) {
        self.failing_wal_puts.store(count, Ordering::SeqCst);
    }

    pub fn pause_next_get(&self) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *self.next_get_pause.lock().unwrap() = Some((started_tx, release_rx));
        (started_rx, release_tx)
    }

    pub fn pause_wal_payload(
        &self,
        key: &str,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (started, begun) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        let (completed, done) = tokio::sync::oneshot::channel();
        assert!(self
            .wal_payload_pauses
            .lock()
            .unwrap()
            .insert(
                key.to_string(),
                WalPayloadPause {
                    started,
                    release: wait,
                    completed
                }
            )
            .is_none());
        (begun, release, done)
    }

    fn note_read(&self) {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl std::fmt::Display for ReadCountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadCountingObjectStore({})", self.inner)
    }
}

impl std::fmt::Debug for ReadCountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadCountingObjectStore({:?})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ReadCountingObjectStore {
    async fn put_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        payload: slatedb::object_store::PutPayload,
        opts: slatedb::object_store::PutOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::PutResult> {
        if location.as_ref().contains("/wal/") && self.conflict_next_wal_put.swap(false, Ordering::SeqCst) {
            return Err(slatedb::object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: Box::new(std::io::Error::other("409 ConditionalRequestConflict")),
            });
        }
        if location.as_ref().contains("/wal/")
            && self
                .failing_wal_puts
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| match left {
                    0 => None,
                    u64::MAX => Some(u64::MAX),
                    left => Some(left - 1),
                })
                .is_ok()
        {
            return Err(slatedb::object_store::Error::Generic {
                store: "failing-wal-put-test",
                source: Box::new(std::io::Error::other("injected 503 SlowDown")),
            });
        }
        if location.as_ref().contains("/wal/") {
            self.wal_puts.fetch_add(1, Ordering::Relaxed);
            let delay = self.wal_put_delay_ms.load(Ordering::Relaxed);
            if delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
            let pause = self.next_wal_pause.lock().unwrap().take();
            if let Some((started, release)) = pause {
                let _ = started.send(());
                let _ = release.await;
            }
            let queued = self.wal_put_queue.lock().unwrap().pop_front();
            if let Some((started, release)) = queued {
                let _ = started.send(location.to_string());
                let _ = release.await;
            }
        }
        let mut opts = opts;
        if location.as_ref().contains("/wal/") && self.ignore_wal_create_only.load(Ordering::SeqCst) {
            opts.mode = slatedb::object_store::PutMode::Overwrite;
        }
        if location.as_ref().contains("/wal/") && self.lose_next_wal_response.swap(false, Ordering::SeqCst) {
            self.inner.put_opts(location, payload, opts).await?;
            let pause = self.lost_response_pause.lock().unwrap().take();
            if let Some((started, release)) = pause {
                let _ = started.send(location.to_string());
                let _ = release.await;
            }
            return Err(slatedb::object_store::Error::Generic {
                store: "lost-wal-response-test",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "response lost after the PUT landed",
                )),
            });
        }
        #[cfg(feature = "query-transport")]
        if location.filename() == Some("__scope__") {
            self.scope_puts.fetch_add(1, Ordering::Relaxed);
            if self.fail_scope_put.swap(false, Ordering::AcqRel) {
                return Err(slatedb::object_store::Error::Generic {
                    store: "scope-registration-test",
                    source: Box::new(std::io::Error::other("injected registration failure")),
                });
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        opts: slatedb::object_store::PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn slatedb::object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        options: slatedb::object_store::GetOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::GetResult> {
        if location.as_ref().contains("/compacted/") {
            self.compacted_reads.fetch_add(1, Ordering::Relaxed);
            self.compacted_ranges
                .lock()
                .unwrap()
                .push(format!("{location} {:?}", options.range));
        }
        self.note_read();
        let delay = self.get_delay_ms.load(Ordering::Relaxed);
        if delay > 0 {
            let active = self.active_gets.fetch_add(1, Ordering::Relaxed) + 1;
            self.peak_gets.fetch_max(active, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            self.active_gets.fetch_sub(1, Ordering::Relaxed);
        }
        let pause = self.next_get_pause.lock().unwrap().take();
        if let Some((started, release)) = pause {
            let _ = started.send(());
            let _ = release.await;
        }
        let payload_pause = if matches!(&options.range, Some(slatedb::object_store::GetRange::Bounded(range)) if range.start == 0) {
            self.wal_payload_pauses.lock().unwrap().remove(location.as_ref())
        } else {
            None
        };
        let completed = if let Some(WalPayloadPause {
            started,
            release,
            completed,
        }) = payload_pause
        {
            let _ = started.send(());
            let _ = release.await;
            Some(completed)
        } else {
            None
        };
        let mut result = self.inner.get_opts(location, options).await?;
        if self.strip_object_metadata.load(Ordering::SeqCst) {
            result.attributes = Default::default();
        }
        if let Some(completed) = completed {
            let _ = completed.send(());
        }
        if location.as_ref().contains("/compacted/") {
            self.max_compacted_range
                .fetch_max(result.range.end - result.range.start, Ordering::Relaxed);
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, slatedb::object_store::Result<slatedb::object_store::path::Path>>,
    ) -> futures::stream::BoxStream<'static, slatedb::object_store::Result<slatedb::object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&slatedb::object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, slatedb::object_store::Result<slatedb::object_store::ObjectMeta>> {
        self.note_read();
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&slatedb::object_store::path::Path>,
    ) -> slatedb::object_store::Result<slatedb::object_store::ListResult> {
        self.note_read();
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &slatedb::object_store::path::Path,
        to: &slatedb::object_store::path::Path,
        options: slatedb::object_store::CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
