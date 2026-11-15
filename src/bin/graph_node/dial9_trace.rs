//! The node's Tokio runtime, optionally recorded by dial9.
//!
//! dial9 records worker timelines, polls, parks and wakes as rotated binary
//! segments that its viewer opens. It is off unless `DIAL9_ENABLED` is set,
//! and then the runtime is built exactly as `#[tokio::main]` builds one, so a
//! node that does not ask for traces runs the runtime it always has.
//!
//! The build does not set `--cfg tokio_unstable`, which would change how every
//! build compiles for the sake of a diagnostic. Without it dial9 keeps worker
//! timelines and wake causality but records no spawn or terminate events, no
//! per-worker queue depth, and polls only of tasks started through dial9's own
//! spawners.
//!
//! dial9 writes segments under `DIAL9_TRACE_DIR`, in a directory per process,
//! and renames each one from `.bin.active` to `.bin` when it seals it. With
//! `GRAPH_TRACE_UPLOAD_URI` set, sealed segments are uploaded there every
//! [`UPLOAD_INTERVAL`] and deleted locally, and whatever is left is uploaded at
//! shutdown. That uses the `object_store` client the node already has rather
//! than dial9's own S3 writer, which would add the AWS SDK to every build. An
//! upload failure is logged and retried on the next pass; it never stops the
//! node.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use slatedb::object_store::{aws::AmazonS3Builder, local::LocalFileSystem, path::Path, ObjectStore, ObjectStoreExt, PutPayload};

/// How often sealed segments are shipped. dial9 seals a segment every 60s by
/// default, so this ships each one within a pass of it being sealed.
const UPLOAD_INTERVAL: Duration = Duration::from_secs(30);

/// How long shutdown waits for dial9 to seal the last segment, and then for
/// that segment to upload. A pod gets 30s to exit by default, and the node's
/// own shutdown has already spent some of it.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(10);

/// dial9's default for `DIAL9_TRACE_DIR`, which the uploader must agree on.
const DEFAULT_TRACE_DIR: &str = "/tmp/dial9-traces";

/// The node's runtime, with dial9's recorder and the segment uploader when
/// traces are on.
pub(crate) struct NodeRuntime {
    runtime: tokio::runtime::Runtime,
    recorder: Option<dial9::Recorder>,
    /// The uploader and its periodic task, stopped before the final pass so
    /// the two never ship the same segment at once.
    upload: Option<(TraceUpload, tokio::task::JoinHandle<()>)>,
}

impl NodeRuntime {
    /// A plain multi-thread runtime, or a dial9-recorded one when
    /// `DIAL9_ENABLED` asks for it.
    pub(crate) fn from_env() -> std::io::Result<Self> {
        if !dial9_enabled(std::env::var("DIAL9_ENABLED").ok().as_deref()) {
            let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
            return Ok(Self {
                runtime,
                recorder: None,
                upload: None,
            });
        }
        let (recorder, runtime) = dial9::recorder_from_env()?;
        let upload = TraceUpload::from_env()?.map(|upload| {
            let periodic = runtime.spawn(upload.clone().ship_forever());
            (upload, periodic)
        });
        Ok(Self {
            runtime,
            recorder: Some(recorder),
            upload,
        })
    }

    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    /// Drop the runtime, seal the last segment, then ship what is left.
    ///
    /// dial9 needs the recorded runtime dropped before `graceful_shutdown`, so
    /// its workers flush their thread-local buffers into the final segment.
    /// The final upload therefore runs on a separate, unrecorded runtime.
    pub(crate) fn finish(self) {
        let Self { runtime, recorder, upload } = self;
        // The periodic pass stops before the final one starts, so the two
        // never ship the same segment at once. A segment the stopped pass was
        // midway through is still on disk, so the final pass ships it.
        let upload = upload.map(|(upload, periodic)| {
            periodic.abort();
            let _ = runtime.block_on(periodic);
            upload
        });
        drop(runtime);
        if let Some(recorder) = recorder {
            recorder.graceful_shutdown(SHUTDOWN_DRAIN);
        }
        let Some(upload) = upload else { return };
        let drained = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            // The timeout's timer needs the runtime, so it is made inside it.
            .map(|runtime| {
                runtime
                    .block_on(async { tokio::time::timeout(SHUTDOWN_DRAIN, upload.ship_sealed()).await })
                    .is_ok()
            });
        match drained {
            Ok(true) => {}
            Ok(false) => tracing::warn!(
                destination = %upload.destination,
                "dial9 segments were still uploading at shutdown; the rest stay unshipped"
            ),
            Err(error) => tracing::warn!(
                destination = %upload.destination,
                %error,
                "no runtime for the final dial9 upload; the last segments stay unshipped"
            ),
        }
    }
}

/// The values dial9 itself reads as true for its boolean variables, so the
/// node agrees with dial9 on whether traces are on.
fn dial9_enabled(value: Option<&str>) -> bool {
    value.is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "t" | "true" | "1" | "y" | "yes" | "on"))
}

/// Where `GRAPH_TRACE_UPLOAD_URI` points.
#[derive(Debug, PartialEq, Eq)]
enum Destination {
    /// `s3://bucket/prefix`, with credentials from the standard AWS variables,
    /// which include the web identity a Kubernetes service account provides.
    S3 { bucket: String, prefix: String },
    /// `file:///absolute/dir`, for running a node by hand.
    Local { dir: PathBuf },
}

fn parse_destination(uri: &str) -> std::io::Result<Destination> {
    let invalid = |why: &str| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("GRAPH_TRACE_UPLOAD_URI {uri:?}: {why}"));
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(invalid("no bucket"));
        }
        return Ok(Destination::S3 {
            bucket: bucket.to_string(),
            prefix: prefix.trim_matches('/').to_string(),
        });
    }
    if let Some(dir) = uri.strip_prefix("file://") {
        if !dir.starts_with('/') {
            return Err(invalid("a file URI needs an absolute path"));
        }
        return Ok(Destination::Local { dir: PathBuf::from(dir) });
    }
    Err(invalid("expected s3://bucket/prefix or file:///dir"))
}

/// Ships sealed dial9 segments from the trace dir to the destination.
#[derive(Clone)]
struct TraceUpload {
    trace_dir: PathBuf,
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    destination: String,
}

impl TraceUpload {
    /// Off when `GRAPH_TRACE_UPLOAD_URI` is unset or empty: the segments then
    /// stay in the trace dir, under dial9's own disk cap.
    fn from_env() -> std::io::Result<Option<Self>> {
        let Some(uri) = std::env::var("GRAPH_TRACE_UPLOAD_URI").ok().filter(|uri| !uri.trim().is_empty()) else {
            return Ok(None);
        };
        let trace_dir = std::env::var("DIAL9_TRACE_DIR")
            .ok()
            .filter(|dir| !dir.trim().is_empty())
            .map_or_else(|| PathBuf::from(DEFAULT_TRACE_DIR), PathBuf::from);
        let (store, prefix): (Arc<dyn ObjectStore>, Path) = match parse_destination(uri.trim())? {
            // A store of its own rather than the graph's, so segment uploads
            // never count toward the graph's remote object-store traffic.
            Destination::S3 { bucket, prefix } => (
                Arc::new(
                    AmazonS3Builder::from_env()
                        .with_bucket_name(bucket)
                        .build()
                        .map_err(std::io::Error::other)?,
                ),
                Path::from(prefix),
            ),
            Destination::Local { dir } => {
                std::fs::create_dir_all(&dir)?;
                (
                    Arc::new(LocalFileSystem::new_with_prefix(&dir).map_err(std::io::Error::other)?),
                    Path::default(),
                )
            }
        };
        Ok(Some(Self {
            trace_dir,
            store,
            prefix,
            destination: uri,
        }))
    }

    async fn ship_forever(self) {
        let mut interval = tokio::time::interval(UPLOAD_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            self.ship_sealed().await;
        }
    }

    /// Upload every sealed segment, `{trace_dir}/{process dir}/{name}.bin`, to
    /// `{prefix}/{process dir}/{name}.bin`, and delete it once it is stored. A
    /// segment still being written ends in `.bin.active` and is left alone. A
    /// segment that fails stays put for the next pass. Returns how many shipped.
    async fn ship_sealed(&self) -> usize {
        let trace_dir = self.trace_dir.clone();
        let segments = match tokio::task::spawn_blocking(move || sealed_segments(&trace_dir)).await {
            Ok(Ok(segments)) => segments,
            Ok(Err(error)) => {
                // The dir appears with dial9's first segment, so it can be
                // missing for the first few seconds of a run.
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(%error, trace_dir = %self.trace_dir.display(), "could not list dial9 segments");
                }
                return 0;
            }
            Err(error) => {
                tracing::warn!(%error, "listing dial9 segments did not finish");
                return 0;
            }
        };
        let mut shipped = 0;
        for (process_dir, name, path) in segments {
            let key = self.prefix.clone().join(process_dir.as_str()).join(name.as_str());
            let read_path = path.clone();
            let bytes = match tokio::task::spawn_blocking(move || std::fs::read(read_path)).await {
                Ok(Ok(bytes)) => bytes,
                // dial9's own disk cap can evict a segment between the
                // listing and the read; it is gone either way.
                Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Ok(Err(error)) => {
                    tracing::warn!(%error, segment = %path.display(), "could not read dial9 segment");
                    continue;
                }
                Err(error) => {
                    tracing::warn!(%error, segment = %path.display(), "reading dial9 segment did not finish");
                    continue;
                }
            };
            if let Err(error) = self.store.put(&key, PutPayload::from(bytes)).await {
                tracing::warn!(%error, %key, destination = %self.destination, "dial9 segment upload failed, retrying next pass");
                continue;
            }
            if let Err(error) = std::fs::remove_file(&path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(%error, segment = %path.display(), "uploaded dial9 segment could not be removed");
                }
            }
            shipped += 1;
        }
        shipped
    }
}

/// Sealed segments one directory down from `trace_dir`, as
/// `(process dir, file name, path)`, oldest name first within each dir.
fn sealed_segments(trace_dir: &std::path::Path) -> std::io::Result<Vec<(String, String, PathBuf)>> {
    let mut segments = Vec::new();
    for process in std::fs::read_dir(trace_dir)? {
        let process = process?;
        if !process.file_type()?.is_dir() {
            continue;
        }
        let Some(process_dir) = process.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let mut names = Vec::new();
        for entry in std::fs::read_dir(process.path())? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str().filter(|name| name.ends_with(".bin")) {
                names.push(name.to_string());
            }
        }
        names.sort();
        segments.extend(names.into_iter().map(|name| {
            let path = process.path().join(&name);
            (process_dir.clone(), name, path)
        }));
    }
    Ok(segments)
}
