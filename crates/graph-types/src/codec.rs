use std::collections::BTreeMap;

use thiserror::Error;

use crate::{
    CypherTemporal, CypherValue, DateTimeValue, DateValue, DurationValue, ExtensionValue, LocalDateTimeValue, LocalTimeValue, NodeEntity,
    NodeId, Path, PathRelationship, PointValue, RelationshipEntity, RelationshipId, RelationshipIdentity, RelationshipRef, TimeValue,
    TraversalOrientation,
};

const MAGIC: &[u8; 3] = b"CYV";
/// Maximum nesting depth accepted by the canonical value codec.
pub const MAX_VALUE_DEPTH: usize = 128;
/// Version of the canonical value byte encoding.
pub const VALUE_CODEC_VERSION: u8 = 4;
const LEGACY_VALUE_CODEC_VERSION: u8 = 1;
const ENTITY_VALUE_CODEC_VERSION: u8 = 2;
const EXTENDED_VALUE_CODEC_VERSION: u8 = 3;

/// Stable tag discriminants used by the canonical Arrow value struct and the
/// versioned byte codec. New tags require a compatible codec version change.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CypherValueTag {
    Null = 0,
    Missing = 1,
    Boolean = 2,
    Integer = 3,
    Float = 4,
    String = 5,
    List = 6,
    Map = 7,
    Node = 8,
    Relationship = 9,
    Path = 10,
    Date = 11,
    LocalTime = 12,
    Time = 13,
    LocalDateTime = 14,
    DateTime = 15,
    Duration = 16,
    Point = 17,
    Extension = 18,
    NodeEntity = 19,
    RelationshipEntity = 20,
    Decimal = 21,
    Bytes = 22,
    Record = 23,
    Temporal = 24,
}

impl TryFrom<u8> for CypherValueTag {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        use CypherValueTag::*;
        Ok(match value {
            0 => Null,
            1 => Missing,
            2 => Boolean,
            3 => Integer,
            4 => Float,
            5 => String,
            6 => List,
            7 => Map,
            8 => Node,
            9 => Relationship,
            10 => Path,
            11 => Date,
            12 => LocalTime,
            13 => Time,
            14 => LocalDateTime,
            15 => DateTime,
            16 => Duration,
            17 => Point,
            18 => Extension,
            19 => NodeEntity,
            20 => RelationshipEntity,
            21 => Decimal,
            22 => Bytes,
            23 => Record,
            24 => Temporal,
            other => return Err(other),
        })
    }
}

pub(crate) mod tag {
    use super::CypherValueTag;

    pub const NULL: u8 = CypherValueTag::Null as u8;
    pub const MISSING: u8 = CypherValueTag::Missing as u8;
    pub const BOOLEAN: u8 = CypherValueTag::Boolean as u8;
    pub const INTEGER: u8 = CypherValueTag::Integer as u8;
    pub const FLOAT: u8 = CypherValueTag::Float as u8;
    pub const STRING: u8 = CypherValueTag::String as u8;
    pub const LIST: u8 = CypherValueTag::List as u8;
    pub const MAP: u8 = CypherValueTag::Map as u8;
    pub const NODE: u8 = CypherValueTag::Node as u8;
    pub const RELATIONSHIP: u8 = CypherValueTag::Relationship as u8;
    pub const PATH: u8 = CypherValueTag::Path as u8;
    pub const DATE: u8 = CypherValueTag::Date as u8;
    pub const LOCAL_TIME: u8 = CypherValueTag::LocalTime as u8;
    pub const TIME: u8 = CypherValueTag::Time as u8;
    pub const LOCAL_DATE_TIME: u8 = CypherValueTag::LocalDateTime as u8;
    pub const DATE_TIME: u8 = CypherValueTag::DateTime as u8;
    pub const DURATION: u8 = CypherValueTag::Duration as u8;
    pub const POINT: u8 = CypherValueTag::Point as u8;
    pub const EXTENSION: u8 = CypherValueTag::Extension as u8;
    pub const NODE_ENTITY: u8 = CypherValueTag::NodeEntity as u8;
    pub const RELATIONSHIP_ENTITY: u8 = CypherValueTag::RelationshipEntity as u8;
    pub const DECIMAL: u8 = CypherValueTag::Decimal as u8;
    pub const BYTES: u8 = CypherValueTag::Bytes as u8;
    pub const RECORD: u8 = CypherValueTag::Record as u8;
    pub const TEMPORAL: u8 = CypherValueTag::Temporal as u8;
}

/// Errors produced while encoding or decoding canonical values.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CodecError {
    #[error("invalid Cypher value codec header")]
    InvalidHeader,
    #[error("unsupported Cypher value codec version {0}")]
    UnsupportedVersion(u8),
    #[error("unexpected end of Cypher value payload")]
    UnexpectedEof,
    #[error("unknown Cypher value tag {0}")]
    UnknownTag(u8),
    #[error("Cypher value tag {tag} is not supported by codec version {version}")]
    ValueNotSupportedInVersion { version: u8, tag: u8 },
    #[error("invalid boolean byte {0}")]
    InvalidBoolean(u8),
    #[error("invalid enum discriminant {0}")]
    InvalidDiscriminant(u8),
    #[error("invalid UTF-8 in Cypher value payload")]
    InvalidUtf8,
    #[error("length cannot be represented on this platform")]
    LengthOverflow,
    #[error("declared length exceeds remaining payload")]
    LengthOutOfBounds,
    #[error("value nesting exceeds the maximum depth of {MAX_VALUE_DEPTH}")]
    ExcessiveDepth,
    #[error("trailing bytes after encoded Cypher value")]
    TrailingBytes,
    #[error("invalid encoded relationship identity")]
    InvalidRelationship,
    #[error("duplicate key in encoded map")]
    DuplicateMapKey,
    #[error("invalid encoded path: {0}")]
    InvalidPath(String),
}

/// Encode a value as a deterministic, versioned byte sequence.
///
/// Map entries are emitted in `BTreeMap` key order. Integers use all 16 signed
/// bytes, floats retain their exact IEEE-754 bit patterns, and no value is
/// stringified or narrowed.
pub fn encode_value(value: &CypherValue) -> Result<Vec<u8>, CodecError> {
    encode_value_version(value, VALUE_CODEC_VERSION)
}

/// Encode with the earliest supported version at or above the v2 fingerprint
/// baseline. Values representable in v2 retain their v2 encoding.
pub fn encode_value_stable(value: &CypherValue) -> Result<Vec<u8>, CodecError> {
    for version in ENTITY_VALUE_CODEC_VERSION..=VALUE_CODEC_VERSION {
        match encode_value_version(value, version) {
            Ok(encoded) => return Ok(encoded),
            Err(CodecError::ValueNotSupportedInVersion { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    unreachable!("the current codec version supports every canonical value")
}

fn encode_value_version(value: &CypherValue, version: u8) -> Result<Vec<u8>, CodecError> {
    let mut output = Vec::new();
    output.extend_from_slice(MAGIC);
    output.push(version);
    encode_inner(value, &mut output, 0, version)?;
    Ok(output)
}

/// Decode one complete versioned value, rejecting truncation, unsupported
/// versions, malformed entities and trailing bytes.
pub fn decode_value(bytes: &[u8]) -> Result<CypherValue, CodecError> {
    if bytes.len() < MAGIC.len() + 1 || &bytes[..MAGIC.len()] != MAGIC {
        return Err(CodecError::InvalidHeader);
    }
    let version = bytes[MAGIC.len()];
    if !(LEGACY_VALUE_CODEC_VERSION..=VALUE_CODEC_VERSION).contains(&version) {
        return Err(CodecError::UnsupportedVersion(version));
    }
    let mut decoder = Decoder {
        bytes: &bytes[MAGIC.len() + 1..],
        position: 0,
        version,
    };
    let value = decoder.value(0)?;
    if decoder.position != decoder.bytes.len() {
        return Err(CodecError::TrailingBytes);
    }
    Ok(value)
}

pub(crate) fn value_tag(value: &CypherValue) -> CypherValueTag {
    use CypherValue::*;
    match value {
        Null => CypherValueTag::Null,
        Missing => CypherValueTag::Missing,
        Boolean(_) => CypherValueTag::Boolean,
        Integer(_) => CypherValueTag::Integer,
        Float(_) => CypherValueTag::Float,
        String(_) => CypherValueTag::String,
        List(_) => CypherValueTag::List,
        Map(_) => CypherValueTag::Map,
        Record(_) => CypherValueTag::Record,
        Node(_) => CypherValueTag::Node,
        Relationship(_) => CypherValueTag::Relationship,
        NodeEntity(_) => CypherValueTag::NodeEntity,
        RelationshipEntity(_) => CypherValueTag::RelationshipEntity,
        Path(_) => CypherValueTag::Path,
        Date(_) => CypherValueTag::Date,
        LocalTime(_) => CypherValueTag::LocalTime,
        Time(_) => CypherValueTag::Time,
        LocalDateTime(_) => CypherValueTag::LocalDateTime,
        DateTime(_) => CypherValueTag::DateTime,
        Duration(_) => CypherValueTag::Duration,
        Point(_) => CypherValueTag::Point,
        Extension(_) => CypherValueTag::Extension,
        Decimal(_) => CypherValueTag::Decimal,
        Bytes(_) => CypherValueTag::Bytes,
        Temporal(_) => CypherValueTag::Temporal,
    }
}

fn encode_inner(value: &CypherValue, output: &mut Vec<u8>, depth: usize, version: u8) -> Result<(), CodecError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(CodecError::ExcessiveDepth);
    }
    output.push(value_tag(value) as u8);
    match value {
        CypherValue::Null | CypherValue::Missing => {}
        CypherValue::Boolean(value) => output.push(u8::from(*value)),
        CypherValue::Integer(value) => output.extend_from_slice(&value.to_be_bytes()),
        CypherValue::Float(value) => output.extend_from_slice(&value.to_bits().to_be_bytes()),
        CypherValue::String(value) => write_string(output, value),
        CypherValue::List(values) => {
            write_len(output, values.len());
            for value in values {
                encode_inner(value, output, depth + 1, version)?;
            }
        }
        CypherValue::Map(values) => {
            write_len(output, values.len());
            for (key, value) in values {
                write_string(output, key);
                encode_inner(value, output, depth + 1, version)?;
            }
        }
        CypherValue::Record(values) => {
            if version < VALUE_CODEC_VERSION {
                return Err(CodecError::ValueNotSupportedInVersion {
                    version,
                    tag: CypherValueTag::Record as u8,
                });
            }
            write_len(output, values.len());
            for (key, value) in values {
                write_string(output, key);
                encode_inner(value, output, depth + 1, version)?;
            }
        }
        CypherValue::Node(value) => output.extend_from_slice(&value.0.to_be_bytes()),
        CypherValue::Relationship(value) => encode_relationship(value, output),
        CypherValue::NodeEntity(value) => {
            output.extend_from_slice(&value.id.0.to_be_bytes());
            write_len(output, value.labels.len());
            for label in &value.labels {
                write_string(output, label);
            }
            write_len(output, value.properties.len());
            for (key, property) in &value.properties {
                write_string(output, key);
                encode_inner(property, output, depth + 1, version)?;
            }
        }
        CypherValue::RelationshipEntity(value) => {
            encode_relationship(&value.identity, output);
            write_len(output, value.properties.len());
            for (key, property) in &value.properties {
                write_string(output, key);
                encode_inner(property, output, depth + 1, version)?;
            }
        }
        CypherValue::Path(value) => {
            write_len(output, value.nodes().len());
            for node in value.nodes() {
                output.extend_from_slice(&node.0.to_be_bytes());
            }
            write_len(output, value.relationships().len());
            for relationship in value.relationships() {
                encode_relationship_identity(relationship.identity(), output);
                output.push(match relationship.orientation() {
                    TraversalOrientation::Forward => 0,
                    TraversalOrientation::Reverse => 1,
                });
            }
        }
        CypherValue::Date(value) => output.extend_from_slice(&value.days_since_epoch.to_be_bytes()),
        CypherValue::LocalTime(value) => output.extend_from_slice(&value.nanos_since_midnight.to_be_bytes()),
        CypherValue::Time(value) => {
            output.extend_from_slice(&value.local.nanos_since_midnight.to_be_bytes());
            output.extend_from_slice(&value.offset_seconds.to_be_bytes());
        }
        CypherValue::LocalDateTime(value) => {
            output.extend_from_slice(&value.date.days_since_epoch.to_be_bytes());
            output.extend_from_slice(&value.time.nanos_since_midnight.to_be_bytes());
        }
        CypherValue::DateTime(value) => {
            output.extend_from_slice(&value.unix_nanos.to_be_bytes());
            output.extend_from_slice(&value.offset_seconds.to_be_bytes());
            write_option_string(output, value.timezone.as_deref());
        }
        CypherValue::Duration(value) => {
            output.extend_from_slice(&value.months.to_be_bytes());
            output.extend_from_slice(&value.days.to_be_bytes());
            output.extend_from_slice(&value.nanos.to_be_bytes());
        }
        CypherValue::Point(value) => {
            output.extend_from_slice(&value.srid.to_be_bytes());
            output.extend_from_slice(&value.x.to_bits().to_be_bytes());
            output.extend_from_slice(&value.y.to_bits().to_be_bytes());
            match value.z {
                Some(z) => {
                    output.push(1);
                    output.extend_from_slice(&z.to_bits().to_be_bytes());
                }
                None => output.push(0),
            }
        }
        CypherValue::Extension(value) => {
            write_string(output, &value.type_name);
            output.extend_from_slice(&value.version.to_be_bytes());
            write_bytes(output, &value.payload);
        }
        CypherValue::Decimal(value) => {
            if version < EXTENDED_VALUE_CODEC_VERSION {
                return Err(CodecError::ValueNotSupportedInVersion {
                    version,
                    tag: CypherValueTag::Decimal as u8,
                });
            }
            write_string(output, value);
        }
        CypherValue::Bytes(value) => {
            if version < EXTENDED_VALUE_CODEC_VERSION {
                return Err(CodecError::ValueNotSupportedInVersion {
                    version,
                    tag: CypherValueTag::Bytes as u8,
                });
            }
            write_bytes(output, value);
        }
        CypherValue::Temporal(value) => {
            if version < VALUE_CODEC_VERSION {
                return Err(CodecError::ValueNotSupportedInVersion {
                    version,
                    tag: CypherValueTag::Temporal as u8,
                });
            }
            encode_temporal(value, output);
        }
    }
    Ok(())
}

fn encode_temporal(value: &CypherTemporal, output: &mut Vec<u8>) {
    match value {
        CypherTemporal::Date { days, text } => {
            output.push(0);
            output.extend_from_slice(&days.to_be_bytes());
            write_string(output, text);
        }
        CypherTemporal::LocalTime { nanoseconds, text } => {
            output.push(1);
            output.extend_from_slice(&nanoseconds.to_be_bytes());
            write_string(output, text);
        }
        CypherTemporal::ZonedTime {
            nanoseconds,
            offset_seconds,
            text,
        } => {
            output.push(2);
            output.extend_from_slice(&nanoseconds.to_be_bytes());
            output.extend_from_slice(&offset_seconds.to_be_bytes());
            write_string(output, text);
        }
        CypherTemporal::LocalDateTime {
            seconds,
            nanoseconds,
            text,
        } => {
            output.push(3);
            output.extend_from_slice(&seconds.to_be_bytes());
            output.extend_from_slice(&nanoseconds.to_be_bytes());
            write_string(output, text);
        }
        CypherTemporal::ZonedDateTime {
            seconds,
            nanoseconds,
            offset_seconds,
            text,
        } => {
            output.push(4);
            output.extend_from_slice(&seconds.to_be_bytes());
            output.extend_from_slice(&nanoseconds.to_be_bytes());
            output.extend_from_slice(&offset_seconds.to_be_bytes());
            write_string(output, text);
        }
        CypherTemporal::Duration {
            months,
            days,
            seconds,
            nanoseconds,
            text,
        } => {
            output.push(5);
            output.extend_from_slice(&months.to_be_bytes());
            output.extend_from_slice(&days.to_be_bytes());
            output.extend_from_slice(&seconds.to_be_bytes());
            output.extend_from_slice(&nanoseconds.to_be_bytes());
            write_string(output, text);
        }
    }
}

fn encode_relationship(value: &RelationshipRef, output: &mut Vec<u8>) {
    encode_relationship_identity(value.identity(), output);
    output.extend_from_slice(&value.source().0.to_be_bytes());
    output.extend_from_slice(&value.destination().0.to_be_bytes());
    write_string(output, value.relationship_type());
}

fn encode_relationship_identity(identity: &RelationshipIdentity, output: &mut Vec<u8>) {
    match identity {
        RelationshipIdentity::Structural {
            source,
            destination,
            relationship_type,
        } => {
            output.push(0);
            output.extend_from_slice(&source.0.to_be_bytes());
            output.extend_from_slice(&destination.0.to_be_bytes());
            write_string(output, relationship_type);
        }
        RelationshipIdentity::Identified(id) => {
            output.push(1);
            output.extend_from_slice(&id.0.to_be_bytes());
        }
    }
}

fn write_option_string(output: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            output.push(1);
            write_string(output, value);
        }
        None => output.push(0),
    }
}

fn write_string(output: &mut Vec<u8>, value: &str) {
    write_bytes(output, value.as_bytes());
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) {
    write_len(output, value.len());
    output.extend_from_slice(value);
}

fn write_len(output: &mut Vec<u8>, value: usize) {
    output.extend_from_slice(&(value as u64).to_be_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
    version: u8,
}

impl Decoder<'_> {
    fn value(&mut self, depth: usize) -> Result<CypherValue, CodecError> {
        if depth > MAX_VALUE_DEPTH {
            return Err(CodecError::ExcessiveDepth);
        }
        let tag = self.read_u8()?;
        if (self.version == LEGACY_VALUE_CODEC_VERSION && tag > CypherValueTag::Extension as u8)
            || (self.version == ENTITY_VALUE_CODEC_VERSION && tag > CypherValueTag::RelationshipEntity as u8)
            || (self.version == EXTENDED_VALUE_CODEC_VERSION && tag > CypherValueTag::Bytes as u8)
        {
            return Err(CodecError::UnknownTag(tag));
        }
        use tag::*;
        Ok(match tag {
            NULL => CypherValue::Null,
            MISSING => CypherValue::Missing,
            BOOLEAN => match self.read_u8()? {
                0 => CypherValue::Boolean(false),
                1 => CypherValue::Boolean(true),
                value => return Err(CodecError::InvalidBoolean(value)),
            },
            INTEGER => CypherValue::Integer(i128::from_be_bytes(self.read_array()?)),
            FLOAT => CypherValue::Float(f64::from_bits(u64::from_be_bytes(self.read_array()?))),
            STRING => CypherValue::String(self.read_string()?),
            LIST => {
                let count = self.read_count(1)?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.value(depth + 1)?);
                }
                CypherValue::List(values)
            }
            MAP => CypherValue::Map(self.read_map_values(depth + 1)?),
            RECORD => CypherValue::Record(self.read_map_values(depth + 1)?),
            NODE => CypherValue::Node(NodeId(self.read_u64()?)),
            RELATIONSHIP => CypherValue::Relationship(self.read_relationship()?),
            NODE_ENTITY => {
                let id = NodeId(self.read_u64()?);
                let count = self.read_count(8)?;
                let mut labels = Vec::with_capacity(count);
                for _ in 0..count {
                    labels.push(self.read_string()?);
                }
                CypherValue::NodeEntity(NodeEntity {
                    id,
                    labels,
                    properties: self.read_map_values(depth + 1)?,
                })
            }
            RELATIONSHIP_ENTITY => CypherValue::RelationshipEntity(RelationshipEntity {
                identity: self.read_relationship()?,
                properties: self.read_map_values(depth + 1)?,
            }),
            PATH => {
                let node_count = self.read_count(8)?;
                let mut nodes = Vec::with_capacity(node_count);
                for _ in 0..node_count {
                    nodes.push(NodeId(self.read_u64()?));
                }
                let relationship_count = self.read_count(2)?;
                let mut relationships = Vec::with_capacity(relationship_count);
                for _ in 0..relationship_count {
                    let identity = self.read_relationship_identity()?;
                    let orientation = match self.read_u8()? {
                        0 => TraversalOrientation::Forward,
                        1 => TraversalOrientation::Reverse,
                        value => return Err(CodecError::InvalidDiscriminant(value)),
                    };
                    relationships.push(PathRelationship::new(identity, orientation));
                }
                CypherValue::Path(Path::new(nodes, relationships).map_err(|error| CodecError::InvalidPath(error.to_string()))?)
            }
            DATE => CypherValue::Date(DateValue {
                days_since_epoch: i32::from_be_bytes(self.read_array()?),
            }),
            LOCAL_TIME => CypherValue::LocalTime(LocalTimeValue {
                nanos_since_midnight: self.read_u64()?,
            }),
            TIME => CypherValue::Time(TimeValue {
                local: LocalTimeValue {
                    nanos_since_midnight: self.read_u64()?,
                },
                offset_seconds: i32::from_be_bytes(self.read_array()?),
            }),
            LOCAL_DATE_TIME => CypherValue::LocalDateTime(LocalDateTimeValue {
                date: DateValue {
                    days_since_epoch: i32::from_be_bytes(self.read_array()?),
                },
                time: LocalTimeValue {
                    nanos_since_midnight: self.read_u64()?,
                },
            }),
            DATE_TIME => CypherValue::DateTime(DateTimeValue {
                unix_nanos: i128::from_be_bytes(self.read_array()?),
                offset_seconds: i32::from_be_bytes(self.read_array()?),
                timezone: self.read_option_string()?,
            }),
            DURATION => CypherValue::Duration(DurationValue {
                months: i64::from_be_bytes(self.read_array()?),
                days: i64::from_be_bytes(self.read_array()?),
                nanos: i128::from_be_bytes(self.read_array()?),
            }),
            POINT => {
                let srid = self.read_u32()?;
                let x = f64::from_bits(self.read_u64()?);
                let y = f64::from_bits(self.read_u64()?);
                let z = match self.read_u8()? {
                    0 => None,
                    1 => Some(f64::from_bits(self.read_u64()?)),
                    value => return Err(CodecError::InvalidDiscriminant(value)),
                };
                CypherValue::Point(PointValue { srid, x, y, z })
            }
            EXTENSION => CypherValue::Extension(ExtensionValue {
                type_name: self.read_string()?,
                version: self.read_u32()?,
                payload: self.read_bytes()?.to_vec(),
            }),
            DECIMAL => CypherValue::Decimal(self.read_string()?),
            BYTES => CypherValue::Bytes(self.read_bytes()?.to_vec()),
            TEMPORAL => CypherValue::Temporal(self.read_temporal()?),
            value => return Err(CodecError::UnknownTag(value)),
        })
    }

    fn read_temporal(&mut self) -> Result<CypherTemporal, CodecError> {
        Ok(match self.read_u8()? {
            0 => CypherTemporal::Date {
                days: i64::from_be_bytes(self.read_array()?),
                text: self.read_string()?,
            },
            1 => CypherTemporal::LocalTime {
                nanoseconds: i64::from_be_bytes(self.read_array()?),
                text: self.read_string()?,
            },
            2 => CypherTemporal::ZonedTime {
                nanoseconds: i64::from_be_bytes(self.read_array()?),
                offset_seconds: i32::from_be_bytes(self.read_array()?),
                text: self.read_string()?,
            },
            3 => CypherTemporal::LocalDateTime {
                seconds: i64::from_be_bytes(self.read_array()?),
                nanoseconds: u32::from_be_bytes(self.read_array()?),
                text: self.read_string()?,
            },
            4 => CypherTemporal::ZonedDateTime {
                seconds: i64::from_be_bytes(self.read_array()?),
                nanoseconds: u32::from_be_bytes(self.read_array()?),
                offset_seconds: i32::from_be_bytes(self.read_array()?),
                text: self.read_string()?,
            },
            5 => CypherTemporal::Duration {
                months: i64::from_be_bytes(self.read_array()?),
                days: i64::from_be_bytes(self.read_array()?),
                seconds: i64::from_be_bytes(self.read_array()?),
                nanoseconds: i32::from_be_bytes(self.read_array()?),
                text: self.read_string()?,
            },
            value => return Err(CodecError::InvalidDiscriminant(value)),
        })
    }

    fn read_relationship(&mut self) -> Result<RelationshipRef, CodecError> {
        let identity = self.read_relationship_identity()?;
        let source = NodeId(self.read_u64()?);
        let destination = NodeId(self.read_u64()?);
        let relationship_type = self.read_string()?;
        match identity {
            RelationshipIdentity::Structural {
                source: identity_source,
                destination: identity_destination,
                relationship_type: identity_type,
            } if identity_source == source && identity_destination == destination && identity_type == relationship_type => {
                Ok(RelationshipRef::structural(source, destination, relationship_type))
            }
            RelationshipIdentity::Identified(id) => Ok(RelationshipRef::identified(id, source, destination, relationship_type)),
            RelationshipIdentity::Structural { .. } => Err(CodecError::InvalidRelationship),
        }
    }

    fn read_map_values(&mut self, depth: usize) -> Result<BTreeMap<String, CypherValue>, CodecError> {
        let count = self.read_count(9)?;
        let mut values = BTreeMap::new();
        for _ in 0..count {
            let key = self.read_string()?;
            let value = self.value(depth)?;
            if values.insert(key, value).is_some() {
                return Err(CodecError::DuplicateMapKey);
            }
        }
        Ok(values)
    }

    fn read_relationship_identity(&mut self) -> Result<RelationshipIdentity, CodecError> {
        match self.read_u8()? {
            0 => Ok(RelationshipIdentity::Structural {
                source: NodeId(self.read_u64()?),
                destination: NodeId(self.read_u64()?),
                relationship_type: self.read_string()?,
            }),
            1 => Ok(RelationshipIdentity::Identified(RelationshipId(self.read_u64()?))),
            value => Err(CodecError::InvalidDiscriminant(value)),
        }
    }

    fn read_option_string(&mut self) -> Result<Option<String>, CodecError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_string().map(Some),
            value => Err(CodecError::InvalidDiscriminant(value)),
        }
    }

    fn read_string(&mut self) -> Result<String, CodecError> {
        String::from_utf8(self.read_bytes()?.to_vec()).map_err(|_| CodecError::InvalidUtf8)
    }

    fn read_bytes(&mut self) -> Result<&[u8], CodecError> {
        let length = self.read_len()?;
        let end = self.position.checked_add(length).ok_or(CodecError::LengthOverflow)?;
        let value = self.bytes.get(self.position..end).ok_or(CodecError::LengthOutOfBounds)?;
        self.position = end;
        Ok(value)
    }

    fn read_count(&mut self, minimum_item_bytes: usize) -> Result<usize, CodecError> {
        let count = self.read_len()?;
        if count > self.bytes.len().saturating_sub(self.position) / minimum_item_bytes {
            return Err(CodecError::LengthOutOfBounds);
        }
        Ok(count)
    }

    fn read_len(&mut self) -> Result<usize, CodecError> {
        usize::try_from(self.read_u64()?).map_err(|_| CodecError::LengthOverflow)
    }

    fn read_u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let end = self.position.checked_add(N).ok_or(CodecError::LengthOverflow)?;
        let bytes = self.bytes.get(self.position..end).ok_or(CodecError::UnexpectedEof)?;
        self.position = end;
        let mut result = [0; N];
        result.copy_from_slice(bytes);
        Ok(result)
    }
}
