//! Process-wide counters of the requests this process sends to a remote object
//! store.
//!
//! [`CountingObjectStore`] wraps the store [`super::object_store_from_env`]
//! builds when that store is remote (S3, GCS or Azure), so it sits below
//! everything that reads through that store. A local filesystem or in-memory
//! store is left unwrapped: its traffic never leaves the machine and is not
//! counted. In particular the wrapper sits below SlateDB's on-disk object store
//! cache: `DbBuilder::build` hands the caller's store to
//! `CachedObjectStore::from_config` and wraps the result in its retry and
//! instrumentation layers ("the cache sits under the retry and instrumentation
//! layers", slatedb `src/db/builder.rs`, `build`, at the pinned rev), and
//! `DbReaderBuilder::build` does the same. A GET that the disk cache answers
//! therefore never reaches this wrapper, which is what makes these counters a
//! count of remote traffic rather than of SlateDB reads.
//!
//! What a count means, and what it leaves out:
//!
//! - A request is a call into the provider's client. A retry from above this
//!   wrapper, such as SlateDB's own retry layer, is a second call and counts
//!   again. A retry inside the provider's client, which the object_store S3,
//!   GCS and Azure clients do for transient HTTP failures, is invisible here,
//!   so one counted request can be several network attempts.
//! - GET bytes are the body chunks delivered to the caller, counted as they
//!   arrive. A body dropped unread counts nothing. They are payload bytes, not
//!   wire bytes: headers, TLS framing and retried transfers are not included.
//! - PUT bytes are the payload handed to the provider when a request is
//!   attempted, counted whether or not the upload then succeeds. A multipart
//!   part counts when its upload future is first polled, so a part that is
//!   created and dropped unpolled counts nothing.
//! - HEAD, LIST, COPY and DELETE requests are not counted.
//!
//! The counters are process-global rather than per-store, because every store
//! the node opens comes from the same constructor and a span's window has to be
//! readable from code that holds no store handle. The cost per request is a
//! couple of relaxed atomic adds.

mod profile;
pub use profile::RemoteStorageProfile;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt as _;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, RenameOptions, Result, UploadPart,
};

/// The counters every remote store from [`super::object_store_from_env`] adds to.
static PROCESS_COUNTERS: RemoteObjectStoreCounters = RemoteObjectStoreCounters::new();

/// Cumulative remote object store traffic since the process started.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RemoteObjectStoreSnapshot {
    /// GET requests, ranged or not.
    pub gets: u64,
    /// The subset of [`Self::gets`] that asked for a byte range.
    pub ranged_gets: u64,
    /// GET body bytes delivered to callers, counted as chunks arrive.
    pub get_bytes: u64,
    /// PUT requests attempted, counting each multipart part as one.
    pub puts: u64,
    /// Payload bytes of the PUT requests attempted, not bytes confirmed
    /// transferred.
    pub put_bytes: u64,
}

impl RemoteObjectStoreSnapshot {
    /// The traffic between `earlier` and `self`.
    ///
    /// Saturating, so a pair taken out of order reads as zero rather than as a
    /// wrapped near-`u64::MAX` value on a span.
    pub fn since(self, earlier: Self) -> Self {
        Self {
            gets: self.gets.saturating_sub(earlier.gets),
            ranged_gets: self.ranged_gets.saturating_sub(earlier.ranged_gets),
            get_bytes: self.get_bytes.saturating_sub(earlier.get_bytes),
            puts: self.puts.saturating_sub(earlier.puts),
            put_bytes: self.put_bytes.saturating_sub(earlier.put_bytes),
        }
    }
}

/// The process totals. Five relaxed loads, so the fields are not one atomic
/// cut, which a counter that only grows does not need.
pub fn remote_object_store_snapshot() -> RemoteObjectStoreSnapshot {
    PROCESS_COUNTERS.snapshot()
}

/// Wrap `store` so its traffic adds to the process totals when `cloud_provider`,
/// SlateDB's `CLOUD_PROVIDER` value, names a remote backend. A local
/// filesystem or in-memory store comes back as it is.
pub(crate) fn count_if_remote(cloud_provider: &str, store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    if !is_remote_provider(cloud_provider) {
        return store;
    }
    Arc::new(CountingObjectStore {
        inner: store,
        counters: &PROCESS_COUNTERS,
    })
}

/// The `CLOUD_PROVIDER` values `slatedb::admin::load_object_store_from_env`
/// maps to a network store, matched case-insensitively as it does. Anything
/// else it accepts, `local` and `memory`, stays on the machine.
fn is_remote_provider(cloud_provider: &str) -> bool {
    matches!(cloud_provider.to_ascii_lowercase().as_str(), "aws" | "gcp" | "azure")
}

#[derive(Debug, Default)]
struct RemoteObjectStoreCounters {
    gets: AtomicU64,
    ranged_gets: AtomicU64,
    get_bytes: AtomicU64,
    puts: AtomicU64,
    put_bytes: AtomicU64,
}

impl RemoteObjectStoreCounters {
    const fn new() -> Self {
        Self {
            gets: AtomicU64::new(0),
            ranged_gets: AtomicU64::new(0),
            get_bytes: AtomicU64::new(0),
            puts: AtomicU64::new(0),
            put_bytes: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> RemoteObjectStoreSnapshot {
        RemoteObjectStoreSnapshot {
            gets: self.gets.load(Ordering::Relaxed),
            ranged_gets: self.ranged_gets.load(Ordering::Relaxed),
            get_bytes: self.get_bytes.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            put_bytes: self.put_bytes.load(Ordering::Relaxed),
        }
    }

    fn note_get(&self, ranged: bool) {
        self.gets.fetch_add(1, Ordering::Relaxed);
        if ranged {
            self.ranged_gets.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn note_put(&self, bytes: usize) {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.put_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

/// The process's remote object store traffic while one query span was open:
/// the change in the process totals between [`Self::open`] and
/// [`Self::record`].
///
/// A process-window figure, not I/O attributable to the query. Everything the
/// process sent in the window lands in it: overlapping queries, and SlateDB's
/// background flushes, manifest polling and prefetch. Attributing I/O to one
/// query would need a task-local accumulator, and that would miss the fetches
/// SlateDB runs on tasks it spawns itself.
#[cfg(feature = "opencypher")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct RemoteIoWindow {
    opened: RemoteObjectStoreSnapshot,
}

#[cfg(feature = "opencypher")]
impl RemoteIoWindow {
    pub(crate) const REMOTE_GETS: &'static str = "hydradb.storage.remote_gets";
    pub(crate) const REMOTE_GET_BYTES: &'static str = "hydradb.storage.remote_get_bytes";
    pub(crate) const RANGED_GETS: &'static str = "hydradb.storage.ranged_gets";
    pub(crate) const PUTS: &'static str = "hydradb.storage.puts";
    pub(crate) const PUT_BYTES: &'static str = "hydradb.storage.put_bytes";

    pub(crate) fn open() -> Self {
        Self {
            opened: remote_object_store_snapshot(),
        }
    }

    /// Record the traffic since [`Self::open`] on `span`, which must declare the
    /// five field names above: `tracing` drops a record for an undeclared field.
    pub(crate) fn record(self, span: &tracing::Span) {
        let delta = remote_object_store_snapshot().since(self.opened);
        span.record(Self::REMOTE_GETS, delta.gets);
        span.record(Self::REMOTE_GET_BYTES, delta.get_bytes);
        span.record(Self::RANGED_GETS, delta.ranged_gets);
        span.record(Self::PUTS, delta.puts);
        span.record(Self::PUT_BYTES, delta.put_bytes);
    }
}

/// Forwards every call to `inner` and counts its GETs and PUTs.
///
/// `get_ranges` is left to the trait default, which coalesces the ranges and
/// issues each merged range through [`ObjectStore::get_opts`] on this wrapper,
/// so every request it sends is counted once. That is the same request pattern
/// the S3, GCS and Azure stores produce, since none of them override it.
#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: &'static RemoteObjectStoreCounters,
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.counters.note_put(payload.content_length());
        let _timer = profile::start(profile::Request::Put {
            bytes: payload.content_length(),
        });
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        let upload = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingUpload {
            inner: upload,
            counters: self.counters,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.head {
            return self.inner.get_opts(location, options).await;
        }
        self.counters.note_get(options.range.is_some());
        let timer = profile::start(profile::Request::Get {
            ranged: options.range.is_some(),
        });
        let mut result = self.inner.get_opts(location, options).await?;
        result.payload = count_streamed_bytes(result.payload, self.counters, timer.as_ref().map(|t| &t.timings));
        Ok(result)
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Count GET bytes as the body's chunks arrive, so a body that fails partway
/// or is dropped unread counts what crossed the wire rather than the range the
/// response announced.
///
/// A `File` payload is passed through uncounted: only the local filesystem
/// store returns one, and a read from local disk is not remote traffic.
fn count_streamed_bytes(
    payload: GetResultPayload,
    counters: &'static RemoteObjectStoreCounters,
    profile: Option<&Arc<profile::Timings>>,
) -> GetResultPayload {
    let GetResultPayload::Stream(stream) = payload else {
        return payload;
    };
    let stream = stream.inspect(move |chunk| {
        if let Ok(bytes) = chunk {
            counters.get_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            profile::note_get_bytes(bytes.len());
        }
    });
    let Some(profile) = profile else {
        return GetResultPayload::Stream(stream.boxed());
    };
    let weak = Arc::downgrade(profile);
    let timed = futures::stream::unfold(stream, move |mut stream| {
        let profile = weak.upgrade();
        async move {
            let started = profile.as_ref().map(|_| std::time::Instant::now());
            let chunk = stream.next().await?;
            if let (Some(p), Some(started)) = (profile, started) {
                p.record_chunk(started.elapsed());
            }
            Some((chunk, stream))
        }
    });
    GetResultPayload::Stream(timed.boxed())
}

/// Counts each part of a multipart upload as one PUT, since each part is its
/// own request to the store.
#[derive(Debug)]
struct CountingUpload {
    inner: Box<dyn MultipartUpload>,
    counters: &'static RemoteObjectStoreCounters,
}

#[async_trait::async_trait]
impl MultipartUpload for CountingUpload {
    /// Counted on the future's first poll rather than here, so a part that is
    /// created and dropped unpolled is not counted as attempted.
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let counters = self.counters;
        let bytes = data.content_length();
        let upload = self.inner.put_part(data);
        Box::pin(async move {
            counters.note_put(bytes);
            let _timer = profile::start(profile::Request::UploadPart { bytes });
            upload.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
