//! turbolay — an object-native property-graph database on SlateDB.
//!
//! This crate is the **M0 foundation**: the substrate wrapper over
//! [`opendata-common`](common), the graph keyspace + order-preserving key
//! encodings (RFC 0003), and crash-safe UID / interned-id / changelog-seq
//! allocation (RFC 0004 §UID).
//!
//! Everything here is storage-model plumbing — the write path (M1), index
//! framework (M2) and openCypher read path (M3) build on top of the keyspace
//! and allocators defined in this crate. No graph semantics or query yet: M0
//! is "we can encode/decode every key and value correctly and allocate ids
//! crash-safely" (RFC 0003 acceptance).
//!
//! ## Module map
//!
//! - [`serde`] — the graph keyspace. `SUBSYSTEM GRAPH = 0x05`, the record-type
//!   tags ([`serde::record_tag`]), every key builder/parser ([`serde::keys`]),
//!   and the order-preserving index-token encodings ([`serde::token`]). All
//!   ordering is baked into the *bytes* because SlateDB has no custom
//!   comparators.
//! - [`schema`] — name interning model (label/predicate/property → `u32` id),
//!   the durable `SchemaEntry` value format, and the in-memory schema cache.
//! - [`ids`] — [`ids::GraphAllocators`], block-reserved monotonic id spaces
//!   built on [`common::SequenceAllocator`], plus `xid → uid` resolution.
//! - [`merge`] — [`merge::GraphMergeOperator`], the record-tag-routed merge
//!   operator (RFC 0003 dispatch table). **M0 stub** — the associative roaring
//!   / counter merges land in M1 (RFC 0004/0005).
//! - [`posting`] — [`posting::PostingValue`], the posting-list value type
//!   (RFC 0005) stored at every adjacency/index/count key: its on-wire codec
//!   and read-side set-algebra surface. M1 deliverable 1.
//! - [`posting_ops`] — add/delete/split/rollup/neighbors: the operations that
//!   turn adjacency keys into live, updatable posting lists (RFC 0005
//!   §"Add/delete mechanics", §"Splitting supernodes"). M1 deliverable 4.
//! - [`value`] — [`value::NodeRecord`]/[`value::TypedValue`] (RFC 0004
//!   §"Node record") and [`value::ChangeRecord`] (§"ChangeRecord schema"):
//!   the node blob and changelog value types, each with a fail-closed codec.
//!   M1 deliverable 2.
//! - [`storage`] — [`storage::GraphStorage`], the per-namespace substrate
//!   wrapper that opens SlateDB via [`common::StorageBuilder`].
//! - [`telemetry`] — `tracing` subscriber initialization.

pub mod error;
pub mod ids;
pub mod merge;
pub mod posting;
pub mod posting_ops;
pub mod schema;
pub mod serde;
pub mod storage;
pub mod telemetry;
pub mod value;

pub use error::{Error, Result};
pub use serde::record_tag::RecordType;
pub use serde::{Direction, LabelId, PredId, PropId, SchemaKind, Uid};
