//! Conditional updates on a local directory store for a single writer.
//!
//! `object_store`'s `LocalFileSystem` implements `PutMode::Create` (a hard
//! link) but not `PutMode::Update`, so an import into a local directory could
//! not publish its planner statistics: the publication claims, publishes and
//! releases through conditional updates of its control record. An import is
//! the only process writing its store, so this wrapper performs the update
//! itself, comparing the object's current ETag with the expected one and
//! overwriting it under a lock that every conditional write of the same path
//! takes. Writes from other processes are not serialized: use it only for a
//! store no other process writes while the import runs.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tokio::sync::Mutex;

/// Conditional writes of different paths rarely share a stripe; a stripe
/// held by one only delays the other, never breaks it.
const LOCK_STRIPES: usize = 64;

#[derive(Debug)]
pub struct SingleWriterLocalStore {
    inner: Arc<dyn ObjectStore>,
    locks: Vec<Mutex<()>>,
}

impl SingleWriterLocalStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            locks: (0..LOCK_STRIPES).map(|_| Mutex::new(())).collect(),
        }
    }

    fn lock(&self, location: &Path) -> &Mutex<()> {
        let mut hasher = DefaultHasher::new();
        location.hash(&mut hasher);
        &self.locks[(hasher.finish() % LOCK_STRIPES as u64) as usize]
    }
}

impl std::fmt::Display for SingleWriterLocalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SingleWriterLocalStore({})", self.inner)
    }
}

fn precondition(location: &Path, reason: &str) -> Error {
    Error::Precondition {
        path: location.to_string(),
        source: reason.to_string().into(),
    }
}

#[async_trait]
impl ObjectStore for SingleWriterLocalStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, mut opts: PutOptions) -> Result<PutResult> {
        let PutMode::Update(expected) = &opts.mode else {
            // Create is atomic on its own; it takes the lock so an update of
            // the same path never interleaves with it.
            let _guard = self.lock(location).lock().await;
            return self.inner.put_opts(location, payload, opts).await;
        };
        let Some(expected) = expected.e_tag.clone() else {
            return Err(precondition(location, "an update needs the ETag it expects"));
        };
        let _guard = self.lock(location).lock().await;
        let current = match self.inner.head(location).await {
            Ok(meta) => meta,
            Err(Error::NotFound { .. }) => return Err(precondition(location, "the object to update does not exist")),
            Err(error) => return Err(error),
        };
        if current.e_tag.as_deref() != Some(expected.as_str()) {
            return Err(precondition(location, "the object changed since it was read"));
        }
        opts.mode = PutMode::Overwrite;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
