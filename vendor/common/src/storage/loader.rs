use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::{StorageError, StorageRead, StorageResult};

/// Trait for types that can be loaded from storage.
///
/// This trait abstracts the loading logic for any type that can be loaded from
/// storage.
#[async_trait]
pub trait Loadable<S>: Sized + Send + Sync {
    /// Load an instance from storage for the given scope.
    async fn load(storage: &dyn StorageRead, scope: &S) -> StorageResult<Self>;
}

/// Synchronization metadata for coordinating with the ingest side.
pub struct LoadMetadata {
    pub load_id: Uuid,
    pub sequence_number: u64,
}

/// Result of loading a bucket from storage.
///
/// Contains the loaded data along with synchronization metadata
/// to coordinate with the ingest side.
pub struct LoadResult<T> {
    /// The loaded data
    pub data: T,
    /// Synchronization metadata from the ingest side
    pub metadata: LoadMetadata,
}

/// Specification for loading a bucket from storage.
///
/// Contains all the information needed to load a bucket, including
/// the storage snapshot to load from and optional synchronization metadata.
///
/// The generic parameter `T` specifies which type to load (e.g., IngestTsdb or QueryTsdb).
pub struct LoadSpec<T: Loadable<S>, S> {
    /// The storage snapshot to load from (could be current storage or a point-in-time snapshot)
    pub storage: Arc<dyn StorageRead>,
    /// Optional synchronization metadata from the ingest side
    /// If present, the loader will return these values in the LoadResult
    pub metadata: Option<LoadMetadata>,
    /// Phantom data to carry the type information
    _phantom: std::marker::PhantomData<(T, S)>,
}

/// Generic loader that manages concurrent loading of Loadable types.
///
/// Uses a semaphore to limit the number of concurrent load operations,
/// preventing resource exhaustion while still allowing some parallelism.
///
/// This loader is not generic itself, but its `load()` method is generic,
/// allowing a single Loader instance to load different types and scopes.
pub struct Loader {
    /// Semaphore to limit concurrent load operations
    semaphore: Arc<Semaphore>,
}

impl Loader {
    /// Creates a new Loader with a specified maximum number of concurrent loads.
    ///
    /// # Arguments
    ///
    /// * `max_concurrent_loads` - Maximum number of load operations that can run concurrently
    pub fn new(max_concurrent_loads: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent_loads)),
        }
    }

    /// Load a bucket from storage.
    ///
    /// Acquires a semaphore permit to limit concurrent loads, then spawns
    /// an async task to perform the actual loading operation.
    ///
    /// If the LoadSpec does not include metadata, a new load_id (UUID) will be
    /// generated and sequence_number will be set to 0.
    ///
    /// Returns a LoadResult containing the loaded data and synchronization metadata.
    pub async fn load<T: Loadable<S>, S>(
        &self,
        spec: LoadSpec<T, S>,
        scope: &S,
    ) -> StorageResult<LoadResult<T>> {
        // Acquire permit (blocks if at capacity)
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| StorageError::from_storage("Semaphore closed"))?;

        // Generate metadata if not provided
        let metadata = spec.metadata.unwrap_or_else(|| LoadMetadata {
            load_id: Uuid::new_v4(),
            sequence_number: 0,
        });

        // Load the data
        let data = T::load(&*spec.storage, scope).await?;

        Ok(LoadResult { data, metadata })
    }
}
