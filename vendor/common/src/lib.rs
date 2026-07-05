pub mod bytes;
pub mod clock;
pub mod coordinator;
pub mod display;
pub mod sequence;
pub mod serde;
pub mod storage;

pub use bytes::BytesRange;
pub use clock::Clock;
pub use sequence::{DEFAULT_BLOCK_SIZE, SequenceAllocator, SequenceError, SequenceResult};
pub use serde::seq_block::SeqBlock;
pub use storage::config::{
    BlockCacheConfig, FoyerHybridCacheConfig, ObjectStoreConfig, StorageConfig,
};
pub use storage::factory::{
    CompactorBuilder, DbBuilder, StorageBuilder, StorageReaderRuntime, StorageSemantics,
    create_object_store, create_storage_read, new_slatedb_compactor_builder,
};
// The whole `object_store` crate (re-exported from `slatedb::object_store`),
// at the exact version SlateDB itself depends on — so a downstream crate can
// implement `ObjectStore` (or name its associated types) without taking its
// own direct `slatedb`/`object_store` dependency and risking a version skew
// that SlateDB's `DbBuilder`/`DbReader` would then reject. RFC 0017 §3.1's
// instrumented-`ObjectStore`-wrapper seam.
pub use storage::factory::object_store;
pub use storage::loader::{LoadMetadata, LoadResult, LoadSpec, Loadable, Loader};
pub use storage::slate::SlateReadHandle;
pub use storage::sst_blocks::{
    BlockOpCounts, CountResult, L0Stats, SortedRunStats, WalkStats, count_in_range,
};
pub use storage::{
    CheckpointInfo, MergeRecordOp, PutRecordOp, Record, Storage, StorageError, StorageIterator,
    StorageRead, StorageResult, Ttl, WriteOptions, WriteResult,
};
