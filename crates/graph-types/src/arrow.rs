use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, StringBuilder, UInt8Builder};
use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array, StringArray, StructArray, UInt8Array};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, SchemaRef};
use thiserror::Error;

use crate::codec::{tag, value_tag};
use crate::{decode_value, encode_value, CodecError, CypherValue, GRAPH_SCHEMA_VERSION, VALUE_CODEC_VERSION};

const SCHEMA_VERSION_KEY: &str = "hydradb.graph.schema_version";
const VALUE_VERSION_KEY: &str = "hydradb.graph.value_codec_version";

/// Canonical versioned representation of a heterogeneous Cypher value.
/// Primitive values use their native child field; complex and extensible
/// values carry their deterministic versioned codec bytes in `payload`.
pub fn canonical_value_data_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("tag", DataType::UInt8, false)),
        Arc::new(Field::new("boolean", DataType::Boolean, true)),
        Arc::new(Field::new("integer_i128_be", DataType::FixedSizeBinary(16), true)),
        Arc::new(Field::new("float64", DataType::Float64, true)),
        Arc::new(Field::new("string", DataType::Utf8, true)),
        Arc::new(Field::new("payload", DataType::Binary, true)),
    ]))
}

/// A value field annotated with the schema and codec versions required to
/// interpret its tagged payload.
pub fn canonical_value_field(name: impl Into<String>, nullable: bool) -> Field {
    Field::new(name, canonical_value_data_type(), nullable).with_metadata(version_metadata())
}

/// Canonical node provider output. Nullable `node_id` represents an absent
/// binding; `exists` separately distinguishes an absent record from a null
/// property or an unbound optional pattern.
pub fn canonical_node_binding_schema() -> SchemaRef {
    let labels = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
    graph_schema(vec![
        Field::new("node_id", DataType::UInt64, true)
            .with_metadata(HashMap::from([("hydradb.graph.kind".to_owned(), "node-id".to_owned())])),
        Field::new("exists", DataType::Boolean, false),
        Field::new("labels", labels, false),
        Field::new("properties", property_map_data_type(), false),
    ])
}

/// Canonical nullable relationship binding. Structural relationships have a
/// null `record_id`; identified relationships have a non-null record ID and a
/// distinct `kind` discriminator.
pub fn canonical_relationship_binding_schema() -> SchemaRef {
    graph_schema(vec![
        Field::new("relationship", relationship_data_type(), true)
            .with_metadata(HashMap::from([("hydradb.graph.kind".to_owned(), "relationship".to_owned())])),
        Field::new("exists", DataType::Boolean, false),
    ])
}

/// Canonical expansion output. Input ordinals preserve duplicate seed rows;
/// every result has the source/destination, relationship identity and optional
/// oriented path state required by traversal semantics.
pub fn canonical_expansion_schema() -> SchemaRef {
    graph_schema(vec![
        Field::new("input_ordinal", DataType::UInt64, false),
        Field::new("source", DataType::UInt64, false),
        Field::new("destination", DataType::UInt64, false),
        Field::new("relationship", relationship_data_type(), true),
        canonical_path_field("path", true),
    ])
}

/// Canonical ordered path representation, including each relationship's
/// structural/identified kind and its traversal orientation.
pub fn canonical_path_field(name: impl Into<String>, nullable: bool) -> Field {
    let nodes = DataType::List(Arc::new(Field::new("item", DataType::UInt64, false)));
    let relationship = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("identity", relationship_identity_data_type(), false)),
        Arc::new(Field::new("orientation", DataType::UInt8, false)),
    ]));
    let relationships = DataType::List(Arc::new(Field::new("item", relationship, false)));
    Field::new(
        name,
        DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("nodes", nodes, false)),
            Arc::new(Field::new("relationships", relationships, false)),
        ])),
        nullable,
    )
    .with_metadata(HashMap::from([("hydradb.graph.kind".to_owned(), "oriented-path".to_owned())]))
}

/// Encode values into a canonical tagged Arrow struct array. Every struct row
/// is valid, including `Null` and `Missing`; the tags preserve their distinction.
pub fn value_array(values: &[CypherValue]) -> Result<StructArray, ArrowValueError> {
    let fields = match canonical_value_data_type() {
        DataType::Struct(fields) => fields,
        _ => unreachable!("canonical value type is always a struct"),
    };
    let mut tags = UInt8Builder::with_capacity(values.len());
    let mut booleans = BooleanBuilder::with_capacity(values.len());
    // The default builder reserves 1024 slots (16 KiB), even for an empty
    // property map. Size this child to the actual batch so small, bounded
    // provider batches do not exhaust their byte budgets on unused capacity.
    let mut integers = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    let mut floats = Float64Builder::with_capacity(values.len());
    let mut strings = StringBuilder::with_capacity(values.len(), values.iter().map(string_size).sum());
    let mut payloads = BinaryBuilder::with_capacity(values.len(), 0);

    for value in values {
        tags.append_value(value_tag(value) as u8);
        match value {
            CypherValue::Boolean(value) => {
                booleans.append_value(*value);
                integers.append_null();
                floats.append_null();
                strings.append_null();
                payloads.append_null();
            }
            CypherValue::Integer(value) => {
                booleans.append_null();
                integers.append_value(value.to_be_bytes())?;
                floats.append_null();
                strings.append_null();
                payloads.append_null();
            }
            CypherValue::Float(value) => {
                booleans.append_null();
                integers.append_null();
                floats.append_value(*value);
                strings.append_null();
                payloads.append_null();
            }
            CypherValue::String(value) => {
                booleans.append_null();
                integers.append_null();
                floats.append_null();
                strings.append_value(value);
                payloads.append_null();
            }
            CypherValue::Null | CypherValue::Missing => {
                booleans.append_null();
                integers.append_null();
                floats.append_null();
                strings.append_null();
                payloads.append_null();
            }
            _ => {
                booleans.append_null();
                integers.append_null();
                floats.append_null();
                strings.append_null();
                payloads.append_value(encode_value(value)?);
            }
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(tags.finish()),
        Arc::new(booleans.finish()),
        Arc::new(integers.finish()),
        Arc::new(floats.finish()),
        Arc::new(strings.finish()),
        Arc::new(payloads.finish()),
    ];
    Ok(StructArray::new(fields, arrays, None))
}

/// Decode one row from a canonical tagged value array. The full payload is
/// checked, including codec version and trailing bytes.
pub fn value_from_array(array: &dyn Array, row: usize) -> Result<CypherValue, ArrowValueError> {
    if row >= array.len() {
        return Err(ArrowValueError::RowOutOfBounds { row, len: array.len() });
    }
    let values = array.as_any().downcast_ref::<StructArray>().ok_or(ArrowValueError::NotStruct)?;
    if values.data_type() != &canonical_value_data_type() {
        return Err(ArrowValueError::NotCanonicalValueType);
    }
    if values.is_null(row) {
        return Ok(CypherValue::Null);
    }
    let tags = child::<UInt8Array>(values, 0, "tag")?;
    check_child(tags, row, "tag")?;
    let tag = tags.value(row);
    match tag {
        tag::NULL => Ok(CypherValue::Null),
        tag::MISSING => Ok(CypherValue::Missing),
        tag::BOOLEAN => {
            let values = child::<BooleanArray>(values, 1, "boolean")?;
            check_child(values, row, "boolean")?;
            Ok(CypherValue::Boolean(values.value(row)))
        }
        tag::INTEGER => {
            let values = child::<FixedSizeBinaryArray>(values, 2, "integer_i128_be")?;
            check_child(values, row, "integer_i128_be")?;
            let bytes: [u8; 16] = values.value(row).try_into().map_err(|_| ArrowValueError::InvalidIntegerWidth)?;
            Ok(CypherValue::Integer(i128::from_be_bytes(bytes)))
        }
        tag::FLOAT => {
            let values = child::<Float64Array>(values, 3, "float64")?;
            check_child(values, row, "float64")?;
            Ok(CypherValue::Float(values.value(row)))
        }
        tag::STRING => {
            let values = child::<StringArray>(values, 4, "string")?;
            check_child(values, row, "string")?;
            Ok(CypherValue::String(values.value(row).to_owned()))
        }
        tag @ (tag::LIST..=tag::TEMPORAL) => {
            let values = child::<BinaryArray>(values, 5, "payload")?;
            check_child(values, row, "payload")?;
            let decoded = decode_value(values.value(row))?;
            let payload_tag = crate::codec::value_tag(&decoded) as u8;
            if payload_tag != tag {
                return Err(ArrowValueError::PayloadTagMismatch {
                    column_tag: tag,
                    payload_tag,
                });
            }
            Ok(decoded)
        }
        other => Err(ArrowValueError::UnknownTag(other)),
    }
}

#[derive(Debug, Error)]
pub enum ArrowValueError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("value array row {row} is out of bounds for length {len}")]
    RowOutOfBounds { row: usize, len: usize },
    #[error("canonical value input is not a struct array")]
    NotStruct,
    #[error("Arrow struct does not use the canonical Cypher value type")]
    NotCanonicalValueType,
    #[error("canonical value child {0} has an unexpected Arrow type")]
    InvalidChildType(&'static str),
    #[error("canonical value child {0} is null for a non-null tagged value")]
    NullChild(&'static str),
    #[error("canonical i128 child is not exactly 16 bytes")]
    InvalidIntegerWidth,
    #[error("unknown canonical Cypher value tag {0}")]
    UnknownTag(u8),
    #[error("Arrow value tag {column_tag} does not match payload tag {payload_tag}")]
    PayloadTagMismatch { column_tag: u8, payload_tag: u8 },
}

fn child<'a, T: Array + 'static>(values: &'a StructArray, index: usize, name: &'static str) -> Result<&'a T, ArrowValueError> {
    values
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or(ArrowValueError::InvalidChildType(name))
}

fn check_child(array: &dyn Array, row: usize, name: &'static str) -> Result<(), ArrowValueError> {
    if array.is_null(row) {
        Err(ArrowValueError::NullChild(name))
    } else {
        Ok(())
    }
}

fn graph_schema(fields: Vec<Field>) -> SchemaRef {
    let metadata = HashMap::from([
        (SCHEMA_VERSION_KEY.to_owned(), GRAPH_SCHEMA_VERSION.to_string()),
        (VALUE_VERSION_KEY.to_owned(), VALUE_CODEC_VERSION.to_string()),
    ]);
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

fn version_metadata() -> HashMap<String, String> {
    HashMap::from([
        (SCHEMA_VERSION_KEY.to_owned(), GRAPH_SCHEMA_VERSION.to_string()),
        (VALUE_VERSION_KEY.to_owned(), VALUE_CODEC_VERSION.to_string()),
        ("hydradb.graph.kind".to_owned(), "cypher-value".to_owned()),
    ])
}

fn relationship_data_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("source", DataType::UInt64, false)),
        Arc::new(Field::new("destination", DataType::UInt64, false)),
        Arc::new(Field::new("relationship_type", DataType::Utf8, false)),
        Arc::new(Field::new("kind", DataType::Utf8, false)),
        Arc::new(Field::new("record_id", DataType::UInt64, true)),
        Arc::new(Field::new("properties", property_map_data_type(), false)),
    ]))
}

fn property_map_data_type() -> DataType {
    let entries = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("key", DataType::Utf8, false)),
        Arc::new(canonical_value_field("value", true)),
    ]));
    DataType::Map(Arc::new(Field::new("entries", entries, false)), true)
}

fn relationship_identity_data_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("kind", DataType::UInt8, false)),
        Arc::new(Field::new("source", DataType::UInt64, true)),
        Arc::new(Field::new("destination", DataType::UInt64, true)),
        Arc::new(Field::new("relationship_type", DataType::Utf8, true)),
        Arc::new(Field::new("record_id", DataType::UInt64, true)),
    ]))
}

fn string_size(value: &CypherValue) -> usize {
    match value {
        CypherValue::String(value) => value.len(),
        _ => 0,
    }
}
