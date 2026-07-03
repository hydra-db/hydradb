//! Storage configuration types.
//!
//! This module provides configuration structures for different storage backends,
//! allowing services to configure storage type (InMemory or SlateDB) via config files
//! or environment variables.

use serde::{Deserialize, Serialize};

/// Top-level storage configuration.
///
/// Defaults to `SlateDb` with a local `/tmp/opendata-storage` directory.
// The `SlateDb` variant is large (it carries object-store and two cache
// configs), but `StorageConfig` is constructed rarely at startup and never
// moved in a hot path, so the size asymmetry with `InMemory` doesn't matter.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum StorageConfig {
    InMemory,
    SlateDb(SlateDbStorageConfig),
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: ".data".to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        })
    }
}

/// SlateDB-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SlateDbStorageConfig {
    /// Path prefix for SlateDB data in the object store.
    pub path: String,

    /// Object store provider configuration.
    pub object_store: ObjectStoreConfig,

    /// Optional path to SlateDB settings file (TOML/YAML/JSON).
    ///
    /// If not provided, uses SlateDB's `Settings::load()` which checks for
    /// `SlateDb.toml`, `SlateDb.json`, `SlateDb.yaml` in the working directory
    /// and merges any `SLATEDB_` prefixed environment variables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings_path: Option<String>,

    /// Optional cache for SST *data* block lookups.
    ///
    /// When configured, reduces object store reads by caching hot data blocks
    /// in memory and/or on local disk. Maps to the `block_cache` side of
    /// SlateDB's [`slatedb::db_cache::SplitCache`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_cache: Option<BlockCacheConfig>,

    /// Optional cache for SST *metadata* — indexes, filters, and stats blocks.
    ///
    /// Maps to the `meta_cache` side of SlateDB's
    /// [`slatedb::db_cache::SplitCache`]. Metadata blocks are small but
    /// consulted on every read, so a dedicated cache keeps them resident
    /// instead of competing with large data blocks for capacity. An in-memory
    /// [`BlockCacheConfig::FoyerMemory`] is usually the right choice: size it
    /// to comfortably hold the live index + filter set and it effectively
    /// never evicts.
    ///
    /// When either `block_cache` or `meta_cache` is set, the two are combined
    /// into a `SplitCache` that routes data blocks to `block_cache` and
    /// index/filter/stats blocks to `meta_cache`. A side left unset simply
    /// caches nothing for that class of block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta_cache: Option<BlockCacheConfig>,
}

/// Cache configuration for SlateDB. Used for both the data-block cache
/// (`block_cache`) and the metadata cache (`meta_cache`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum BlockCacheConfig {
    /// Two-tier cache using foyer: in-memory + on-disk (ideally NVMe).
    FoyerHybrid(FoyerHybridCacheConfig),
    /// Single-tier in-memory cache using foyer. Lowest-latency option, with
    /// no durable tier — contents are rebuilt on restart (cheap when paired
    /// with cache warming). A good fit for the metadata cache.
    FoyerMemory(FoyerMemoryCacheConfig),
}

/// Configuration for foyer's in-memory (single-tier) cache.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FoyerMemoryCacheConfig {
    /// In-memory cache capacity in bytes.
    pub capacity: u64,
    /// Number of shards. When absent, foyer derives a default from the
    /// available CPU count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shards: Option<usize>,
}

/// Write policy for foyer's hybrid cache.
///
/// Controls when entries are written to the disk tier.
#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FoyerWritePolicy {
    /// Write to disk when an entry is inserted into the memory cache.
    /// Ensures every cached block is also persisted to the disk tier.
    #[default]
    WriteOnInsertion,
    /// Write to disk only when an entry is evicted from the memory cache.
    /// This is foyer's default policy.
    WriteOnEviction,
}

/// Configuration for foyer's hybrid (memory + disk) block cache.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FoyerHybridCacheConfig {
    /// In-memory cache capacity in bytes.
    pub memory_capacity: u64,
    /// On-disk cache capacity in bytes.
    pub disk_capacity: u64,
    /// Path for the on-disk cache directory.
    pub disk_path: String,
    /// Write policy for the hybrid cache. Default: `WriteOnInsertion`.
    #[serde(default)]
    pub write_policy: FoyerWritePolicy,
    /// Number of flush threads for the large engine. Default: 4.
    #[serde(default = "default_flushers")]
    pub flushers: usize,
    /// Buffer pool size in bytes for the large engine flush pipeline.
    /// Each flusher double-buffers, so actual allocation is ~2x this value.
    /// Default: `memory_capacity / 32` (computed at build time when absent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_pool_size: Option<u64>,
    /// Submit queue size threshold in bytes. Entries are dropped when
    /// the queue exceeds this limit. Default: 1 GiB.
    #[serde(default = "default_submit_queue_size_threshold")]
    pub submit_queue_size_threshold: u64,
}

fn default_flushers() -> usize {
    4
}

fn default_submit_queue_size_threshold() -> u64 {
    1024 * 1024 * 1024 // 1 GiB
}

impl FoyerHybridCacheConfig {
    /// Returns the effective buffer pool size: explicit value if set,
    /// otherwise `memory_capacity / 32`.
    pub fn effective_buffer_pool_size(&self) -> u64 {
        self.buffer_pool_size.unwrap_or(self.memory_capacity / 32)
    }
}

impl Default for SlateDbStorageConfig {
    fn default() -> Self {
        Self {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::default(),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        }
    }
}

impl StorageConfig {
    /// Returns a new config with the path modified by appending a suffix.
    ///
    /// For SlateDB storage, appends the suffix to the path (e.g., "data" -> "data/0").
    /// For InMemory storage, returns a clone unchanged.
    pub fn with_path_suffix(&self, suffix: &str) -> Self {
        match self {
            StorageConfig::InMemory => StorageConfig::InMemory,
            StorageConfig::SlateDb(config) => StorageConfig::SlateDb(SlateDbStorageConfig {
                path: format!("{}/{}", config.path, suffix),
                object_store: config.object_store.clone(),
                settings_path: config.settings_path.clone(),
                block_cache: config.block_cache.clone(),
                meta_cache: config.meta_cache.clone(),
            }),
        }
    }
}

/// Object store provider configuration for SlateDB.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    /// In-memory object store (useful for testing and development).
    #[default]
    InMemory,

    /// AWS S3 object store.
    Aws(AwsObjectStoreConfig),

    /// Local filesystem object store.
    Local(LocalObjectStoreConfig),
}

/// AWS S3 object store configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AwsObjectStoreConfig {
    /// AWS region (e.g., "us-west-2").
    pub region: String,

    /// S3 bucket name.
    pub bucket: String,
}

/// Local filesystem object store configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalObjectStoreConfig {
    /// Path to the local directory for storage.
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_default_to_slatedb_with_local_data_dir() {
        // given/when
        let config = StorageConfig::default();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "data");
                assert_eq!(
                    slate_config.object_store,
                    ObjectStoreConfig::Local(LocalObjectStoreConfig {
                        path: ".data".to_string()
                    })
                );
            }
            _ => panic!("Expected SlateDb config as default"),
        }
    }

    #[test]
    fn should_deserialize_in_memory_config() {
        // given
        let yaml = r#"type: InMemory"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        assert_eq!(config, StorageConfig::InMemory);
    }

    #[test]
    fn should_deserialize_slatedb_config_with_local_object_store() {
        // given
        let yaml = r#"
type: SlateDb
path: my-data
object_store:
  type: Local
  path: /tmp/slatedb
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "my-data");
                assert_eq!(
                    slate_config.object_store,
                    ObjectStoreConfig::Local(LocalObjectStoreConfig {
                        path: "/tmp/slatedb".to_string()
                    })
                );
                assert!(slate_config.settings_path.is_none());
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_deserialize_slatedb_config_with_aws_object_store() {
        // given
        let yaml = r#"
type: SlateDb
path: my-data
object_store:
  type: Aws
  region: us-west-2
  bucket: my-bucket
settings_path: slatedb.toml
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "my-data");
                assert_eq!(
                    slate_config.object_store,
                    ObjectStoreConfig::Aws(AwsObjectStoreConfig {
                        region: "us-west-2".to_string(),
                        bucket: "my-bucket".to_string()
                    })
                );
                assert_eq!(slate_config.settings_path, Some("slatedb.toml".to_string()));
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_deserialize_slatedb_config_with_in_memory_object_store() {
        // given
        let yaml = r#"
type: SlateDb
path: test-data
object_store:
  type: InMemory
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "test-data");
                assert_eq!(slate_config.object_store, ObjectStoreConfig::InMemory);
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_serialize_slatedb_config() {
        // given
        let config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "my-data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: "/tmp/slatedb".to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        });

        // when
        let yaml = serde_yaml::to_string(&config).unwrap();

        // then
        assert!(yaml.contains("type: SlateDb"));
        assert!(yaml.contains("path: my-data"));
        assert!(yaml.contains("type: Local"));
        // settings_path and caches should be omitted when None
        assert!(!yaml.contains("settings_path"));
        assert!(!yaml.contains("block_cache"));
        assert!(!yaml.contains("meta_cache"));
    }

    #[test]
    fn should_deserialize_block_cache_config() {
        let yaml = r#"
type: SlateDb
path: data
object_store:
  type: InMemory
block_cache:
  type: FoyerHybrid
  memory_capacity: 8589934592
  disk_capacity: 150323855360
  disk_path: /mnt/nvme/block-cache
"#;
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageConfig::SlateDb(slate_config) => {
                let cache = slate_config.block_cache.expect("block_cache should be set");
                match cache {
                    BlockCacheConfig::FoyerHybrid(foyer) => {
                        assert_eq!(foyer.memory_capacity, 8589934592);
                        assert_eq!(foyer.disk_capacity, 150323855360);
                        assert_eq!(foyer.disk_path, "/mnt/nvme/block-cache");
                        // new fields should get defaults
                        assert_eq!(foyer.write_policy, FoyerWritePolicy::WriteOnInsertion);
                        assert_eq!(foyer.flushers, 4);
                        assert!(foyer.buffer_pool_size.is_none());
                        assert_eq!(foyer.submit_queue_size_threshold, 1024 * 1024 * 1024);
                        // effective buffer pool = memory_capacity / 32
                        assert_eq!(foyer.effective_buffer_pool_size(), 8589934592 / 32);
                    }
                    other => panic!("expected FoyerHybrid, got {other:?}"),
                }
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_deserialize_block_cache_with_explicit_engine_options() {
        // given
        let yaml = r#"
type: SlateDb
path: data
object_store:
  type: InMemory
block_cache:
  type: FoyerHybrid
  memory_capacity: 4294967296
  disk_capacity: 10737418240
  disk_path: /mnt/nvme/cache
  write_policy: WriteOnEviction
  flushers: 2
  buffer_pool_size: 134217728
  submit_queue_size_threshold: 536870912
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                let cache = slate_config.block_cache.expect("block_cache should be set");
                match cache {
                    BlockCacheConfig::FoyerHybrid(foyer) => {
                        assert_eq!(foyer.write_policy, FoyerWritePolicy::WriteOnEviction);
                        assert_eq!(foyer.flushers, 2);
                        assert_eq!(foyer.buffer_pool_size, Some(134217728));
                        assert_eq!(foyer.submit_queue_size_threshold, 536870912);
                        // explicit value overrides derivation
                        assert_eq!(foyer.effective_buffer_pool_size(), 134217728);
                    }
                    other => panic!("expected FoyerHybrid, got {other:?}"),
                }
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_derive_buffer_pool_size_from_memory_capacity() {
        // given
        let config = FoyerHybridCacheConfig {
            memory_capacity: 8 * 1024 * 1024 * 1024, // 8 GiB
            disk_capacity: 100 * 1024 * 1024 * 1024,
            disk_path: "/tmp/cache".to_string(),
            write_policy: FoyerWritePolicy::default(),
            flushers: 4,
            buffer_pool_size: None,
            submit_queue_size_threshold: 1024 * 1024 * 1024,
        };

        // when/then
        assert_eq!(
            config.effective_buffer_pool_size(),
            256 * 1024 * 1024 // 256 MiB = 8 GiB / 32
        );
    }

    #[test]
    fn should_default_block_cache_to_none() {
        let yaml = r#"
type: SlateDb
path: data
object_store:
  type: InMemory
"#;
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert!(slate_config.block_cache.is_none());
                assert!(slate_config.meta_cache.is_none());
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_deserialize_split_cache_with_hybrid_data_and_memory_meta() {
        // given: a data block_cache on disk-backed hybrid, plus a small
        // in-memory meta_cache for index/filter/stats blocks.
        let yaml = r#"
type: SlateDb
path: data
object_store:
  type: InMemory
block_cache:
  type: FoyerHybrid
  memory_capacity: 536870912
  disk_capacity: 10737418240
  disk_path: /mnt/nvme/data-cache
meta_cache:
  type: FoyerMemory
  capacity: 134217728
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        let StorageConfig::SlateDb(slate_config) = config else {
            panic!("Expected SlateDb config");
        };
        match slate_config.block_cache.expect("block_cache should be set") {
            BlockCacheConfig::FoyerHybrid(foyer) => {
                assert_eq!(foyer.memory_capacity, 536870912);
                assert_eq!(foyer.disk_path, "/mnt/nvme/data-cache");
            }
            other => panic!("expected FoyerHybrid data cache, got {other:?}"),
        }
        match slate_config.meta_cache.expect("meta_cache should be set") {
            BlockCacheConfig::FoyerMemory(mem) => {
                assert_eq!(mem.capacity, 134217728);
                assert!(mem.shards.is_none());
            }
            other => panic!("expected FoyerMemory meta cache, got {other:?}"),
        }
    }

    #[test]
    fn should_roundtrip_split_cache_config() {
        // given
        let config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::InMemory,
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity: 64 * 1024 * 1024,
                shards: Some(8),
            })),
            meta_cache: Some(BlockCacheConfig::FoyerMemory(FoyerMemoryCacheConfig {
                capacity: 16 * 1024 * 1024,
                shards: None,
            })),
        });

        // when
        let yaml = serde_yaml::to_string(&config).unwrap();
        let parsed: StorageConfig = serde_yaml::from_str(&yaml).unwrap();

        // then
        assert_eq!(config, parsed);
    }
}
