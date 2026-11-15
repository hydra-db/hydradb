//! Backend-neutral graph identities and Cypher values.
//!
//! This crate is the semantic boundary shared by graph providers and query
//! execution. It deliberately does not depend on storage or DataFusion.

mod arrow;
mod codec;
mod identity;
mod semantics;
mod value;

pub use arrow::{
    canonical_expansion_schema, canonical_node_binding_schema, canonical_path_field, canonical_relationship_binding_schema,
    canonical_value_data_type, canonical_value_field, value_array, value_from_array, ArrowValueError,
};
pub use codec::{decode_value, encode_value, encode_value_stable, CodecError, CypherValueTag, VALUE_CODEC_VERSION};
pub use identity::{
    NodeId, Path, PathError, PathRelationship, RelationshipId, RelationshipIdentity, RelationshipRef, TraversalOrientation,
};
pub use semantics::{hash_semantic, semantic_cmp, semantic_eq, semantic_eq_three_valued, semantic_list_membership};
pub use value::{
    CypherTemporal, CypherValue, DateTimeValue, DateValue, DurationValue, ExtensionValue, LocalDateTimeValue, LocalTimeValue, MapValue,
    NodeEntity, PointValue, RelationshipEntity, TimeValue,
};

/// Version used for canonical Arrow schemas and metadata.
pub const GRAPH_SCHEMA_VERSION: u16 = 1;
