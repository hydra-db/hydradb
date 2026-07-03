//! Storage utilities.
//!
//! This module provides utility functions for storage operations,
//! such as deleting data from object stores after benchmarks or tests.

use futures::StreamExt;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, path::Path};

use super::config::{SlateDbStorageConfig, StorageConfig};
use super::factory::create_object_store;
use super::{StorageError, StorageResult};

/// Deletes all data for the given storage configuration.
///
/// For SlateDB storage, this deletes all objects under the configured path prefix.
/// For InMemory storage, this is a no-op (memory is released when storage is dropped).
///
/// Call this after the storage/database has been closed.
pub async fn delete(config: &StorageConfig) -> StorageResult<()> {
    match config {
        StorageConfig::InMemory => {
            // Nothing to delete for in-memory storage
            Ok(())
        }
        StorageConfig::SlateDb(slate_config) => delete_slatedb(slate_config).await,
    }
}

/// Deletes all objects under a SlateDB path prefix in the object store.
///
/// This is useful for cleaning up after benchmarks or tests. Call this after
/// the database has been closed.
///
/// # Arguments
///
/// * `config` - The SlateDB storage configuration (contains path and object store config)
pub async fn delete_slatedb(config: &SlateDbStorageConfig) -> StorageResult<()> {
    let object_store = create_object_store(&config.object_store)?;
    let prefix = Path::from(config.path.as_str());

    delete_prefix(&*object_store, &prefix).await
}

/// Deletes all objects under a given prefix in an object store.
///
/// This is a lower-level function for cases where you already have an object store.
pub async fn delete_prefix(object_store: &dyn ObjectStore, prefix: &Path) -> StorageResult<()> {
    let mut list_stream = object_store.list(Some(prefix));

    while let Some(result) = list_stream.next().await {
        let meta =
            result.map_err(|e| StorageError::Storage(format!("Failed to list objects: {}", e)))?;

        object_store.delete(&meta.location).await.map_err(|e| {
            StorageError::Storage(format!(
                "Failed to delete object {:?}: {}",
                meta.location, e
            ))
        })?;
    }

    Ok(())
}
