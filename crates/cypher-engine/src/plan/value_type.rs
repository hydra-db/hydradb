//! The value types (GQL §18.9) the engine can test a value against (§19.6)
//! and cast a value to (§20.8), each name resolved to what it means here.
//!
//! The implementation-defined choices (ID028, ID037, IL011): INTEGER, INT and
//! BIGINT are 64-bit, SMALLINT 16-bit, USMALLINT 16-bit, UBIGINT 64-bit;
//! `INT(p)` and `UINT(p)` have exactly `p` bits of precision, up to 256.
//! FLOAT and DOUBLE are binary64, REAL binary32, and `FLOAT(p, s)` the
//! narrowest of FLOAT16 to FLOAT256 with at least that precision and scale.

use hydradb_cypher_ast::{FloatSize, IntegerSize, QueryFailureReason, ValueType, ValueTypeKind};

use super::{GraphPlanError, GraphPlanResult};

/// IL011: the largest precision a numeric type may give.
pub const MAXIMUM_PRECISION: u64 = 256;

/// The greatest length a character string type may declare here (deviations
/// L21). A `CHAR(n)` value is n characters long, so this bounds what a CAST
/// pads to. A string value itself has no maximum length (IL013).
pub const MAXIMUM_STRING_LENGTH: u64 = 1 << 24;

/// The greatest length a byte string type may declare here (deviations
/// L21), for the same reason: a `BINARY(n)` value is n bytes long. A byte
/// string value itself has no maximum length.
pub const MAXIMUM_BYTES_LENGTH: u64 = 1 << 24;

/// A value type: its values, and whether it includes the null value.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct GqlType {
    pub kind: GqlTypeKind,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum GqlTypeKind {
    /// The empty type, and the null type: no material value.
    Immaterial,
    Boolean,
    /// Character strings of `min` to `max` characters, both at most
    /// [`MAXIMUM_STRING_LENGTH`] (IL013: no maximum).
    String {
        min: u64,
        max: Option<u64>,
    },
    /// A binary exact numeric type: signed, the integers from `-2^p` to
    /// `2^p - 1`; unsigned, from 0 to `2^p - 1`, `p` being the precision.
    Integer {
        signed: bool,
        precision: u32,
    },
    Float(FloatFormat),
    /// A decimal exact numeric type (GV17): the exact numbers with at most
    /// `precision` digits, `scale` of them after the point.
    Decimal {
        precision: u32,
        scale: u32,
    },
    /// Byte strings of `min` to `max` bytes (GV35 to GV38), both at most
    /// [`MAXIMUM_BYTES_LENGTH`].
    Bytes {
        min: u64,
        max: Option<u64>,
    },
    Node,
    Edge,
    Path,
    Date,
    LocalTime,
    ZonedTime,
    LocalDatetime,
    ZonedDatetime,
    /// A temporal duration type: year and month-based (`Some(true)`), day
    /// and time-based (`Some(false)`), or, Cypher's bare `DURATION`, either.
    Duration(Option<bool>),
    /// A list of up to `max` elements of `element` (IL015: no maximum).
    List {
        element: Box<GqlType>,
        max: Option<u64>,
    },
    /// A record type: open (`None`), any record; closed, the records with
    /// exactly these fields, ordered by name, each value of its type.
    Record(Option<Vec<(Box<str>, GqlType)>>),
    /// The open dynamic union type: every value.
    Any,
    /// The dynamic property value type: the values a property may hold here,
    /// booleans, integers, floats and strings.
    PropertyValue,
    /// A closed dynamic union type: the values of any component.
    Union(Vec<GqlType>),
}

/// An approximate numeric type's binary format.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FloatFormat {
    Binary16,
    Binary32,
    Binary64,
    /// FLOAT128 and FLOAT256 hold every binary64 value exactly.
    Binary128,
    Binary256,
}

impl GqlType {
    /// Whether the type includes the null value.
    pub fn includes_null(&self) -> bool {
        self.nullable
            && match &self.kind {
                GqlTypeKind::Union(components) => components.iter().any(Self::includes_null),
                _ => true,
            }
    }

    /// Whether the type has no material value: `NULL` and `NOTHING`.
    pub fn is_immaterial(&self) -> bool {
        matches!(self.kind, GqlTypeKind::Immaterial)
    }

    /// Whether a CAST to the type may need the current date: a datetime
    /// target, alone or anywhere inside a list, record or union, takes a
    /// time onto it (§20.30 General Rule 4).
    pub fn reads_current_date(&self) -> bool {
        match &self.kind {
            GqlTypeKind::LocalDatetime | GqlTypeKind::ZonedDatetime => true,
            GqlTypeKind::List { element, .. } => element.reads_current_date(),
            GqlTypeKind::Record(Some(fields)) => fields.iter().any(|(_, field)| field.reads_current_date()),
            GqlTypeKind::Union(components) => components.iter().any(Self::reads_current_date),
            _ => false,
        }
    }
}

/// The type's preferred name, its normal form's (§18.9 Syntax Rule 44).
impl std::fmt::Display for GqlType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            GqlTypeKind::Immaterial => return formatter.write_str(if self.nullable { "NULL" } else { "NOTHING" }),
            GqlTypeKind::Boolean => formatter.write_str("BOOLEAN")?,
            GqlTypeKind::String { min, max: Some(max) } if min == max => write!(formatter, "CHAR({max})")?,
            GqlTypeKind::String { min: 0, max: None } => formatter.write_str("STRING")?,
            GqlTypeKind::String { min, max: Some(max) } => write!(formatter, "STRING({min}, {max})")?,
            GqlTypeKind::String { min, max: None } => write!(formatter, "STRING({min}, )")?,
            GqlTypeKind::Integer { signed: true, precision } => match precision {
                7 | 15 | 31 | 63 | 127 | 255 => write!(formatter, "INT{}", precision + 1)?,
                _ => write!(formatter, "INT({precision})")?,
            },
            GqlTypeKind::Integer { signed: false, precision } => match precision {
                8 | 16 | 32 | 64 | 128 | 256 => write!(formatter, "UINT{precision}")?,
                _ => write!(formatter, "UINT({precision})")?,
            },
            GqlTypeKind::Float(format) => formatter.write_str(match format {
                FloatFormat::Binary16 => "FLOAT16",
                FloatFormat::Binary32 => "FLOAT32",
                FloatFormat::Binary64 => "FLOAT64",
                FloatFormat::Binary128 => "FLOAT128",
                FloatFormat::Binary256 => "FLOAT256",
            })?,
            GqlTypeKind::Decimal { precision, scale } => write!(formatter, "DECIMAL({precision}, {scale})")?,
            GqlTypeKind::Bytes { min, max: Some(max) } if min == max => write!(formatter, "BINARY({max})")?,
            GqlTypeKind::Bytes { min: 0, max: None } => formatter.write_str("BYTES")?,
            GqlTypeKind::Bytes { min, max: Some(max) } => write!(formatter, "BYTES({min}, {max})")?,
            GqlTypeKind::Bytes { min, max: None } => write!(formatter, "BYTES({min}, )")?,
            GqlTypeKind::Node => formatter.write_str("NODE")?,
            GqlTypeKind::Edge => formatter.write_str("EDGE")?,
            GqlTypeKind::Path => formatter.write_str("PATH")?,
            GqlTypeKind::Date => formatter.write_str("DATE")?,
            GqlTypeKind::LocalTime => formatter.write_str("LOCAL TIME")?,
            GqlTypeKind::ZonedTime => formatter.write_str("ZONED TIME")?,
            GqlTypeKind::LocalDatetime => formatter.write_str("LOCAL DATETIME")?,
            GqlTypeKind::ZonedDatetime => formatter.write_str("ZONED DATETIME")?,
            GqlTypeKind::Duration(Some(true)) => formatter.write_str("DURATION(YEAR TO MONTH)")?,
            GqlTypeKind::Duration(Some(false)) => formatter.write_str("DURATION(DAY TO SECOND)")?,
            GqlTypeKind::Duration(None) => formatter.write_str("DURATION")?,
            GqlTypeKind::List { element, max } => {
                write!(formatter, "LIST<{element}>")?;
                if let Some(max) = max {
                    write!(formatter, "[{max}]")?;
                }
            }
            GqlTypeKind::Record(None) => formatter.write_str("RECORD")?,
            GqlTypeKind::Record(Some(fields)) => {
                let fields = fields
                    .iter()
                    .map(|(name, value_type)| format!("{name} {value_type}"))
                    .collect::<Vec<_>>();
                write!(formatter, "RECORD {{{}}}", fields.join(", "))?;
            }
            GqlTypeKind::Any => formatter.write_str("ANY")?,
            GqlTypeKind::PropertyValue => formatter.write_str("PROPERTY VALUE")?,
            GqlTypeKind::Union(components) => {
                let names = components.iter().map(ToString::to_string).collect::<Vec<_>>();
                return write!(formatter, "ANY<{}>", names.join(" | "));
            }
        }
        if !self.nullable {
            formatter.write_str(" NOT NULL")?;
        }
        Ok(())
    }
}

fn syntax(message: impl Into<String>) -> GraphPlanError {
    GraphPlanError::syntax(QueryFailureReason::Other, message.into())
}

fn unsupported(message: &str) -> GraphPlanError {
    GraphPlanError::UnsupportedCypher(QueryFailureReason::Other, message.to_string())
}

/// What a written <value type> means here, its Syntax Rules checked.
pub fn resolve_value_type(written: &ValueType) -> GraphPlanResult<GqlType> {
    let nullable = !written.not_null;
    let kind = match &written.kind {
        ValueTypeKind::Nothing | ValueTypeKind::Null => GqlTypeKind::Immaterial,
        ValueTypeKind::Boolean => GqlTypeKind::Boolean,
        ValueTypeKind::CharacterString { min_length, max_length } => {
            // §18.9 Syntax Rules 7, 9, 10 and 13.
            if *max_length == Some(0) {
                return Err(syntax("a string type's maximum or fixed length is at least 1"));
            }
            let min = min_length.unwrap_or(0);
            if max_length.is_some_and(|max| min > max) {
                return Err(syntax("a string type's minimum length is at most its maximum"));
            }
            if min > MAXIMUM_STRING_LENGTH || max_length.is_some_and(|max| max > MAXIMUM_STRING_LENGTH) {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Other,
                    format!("a string type declares lengths of at most {MAXIMUM_STRING_LENGTH} here"),
                ));
            }
            GqlTypeKind::String { min, max: *max_length }
        }
        ValueTypeKind::Integer { signed, size } => {
            let precision = match (size, signed) {
                (IntegerSize::Bits(bits), true) => bits - 1,
                (IntegerSize::Bits(bits), false) => *bits,
                (IntegerSize::Small, true) => 15,
                (IntegerSize::Small, false) => 16,
                (IntegerSize::Regular | IntegerSize::Big, true) => 63,
                // §18.9 Syntax Rule 36 g): UINT is UINT32.
                (IntegerSize::Regular, false) => 32,
                (IntegerSize::Big, false) => 64,
                (IntegerSize::Precision(precision), _) => {
                    // Syntax Rules 29 and 32 a).
                    if *precision == 0 {
                        return Err(syntax("a numeric type's precision is at least 1"));
                    }
                    if *precision > MAXIMUM_PRECISION {
                        return Err(syntax(format!("a numeric type's precision is at most {MAXIMUM_PRECISION}")));
                    }
                    *precision as u32
                }
            };
            GqlTypeKind::Integer {
                signed: *signed,
                precision,
            }
        }
        ValueTypeKind::Float(size) => GqlTypeKind::Float(match size {
            FloatSize::Bits(16) => FloatFormat::Binary16,
            FloatSize::Bits(32) | FloatSize::Real => FloatFormat::Binary32,
            FloatSize::Bits(64) | FloatSize::Regular | FloatSize::Double => FloatFormat::Binary64,
            FloatSize::Bits(128) => FloatFormat::Binary128,
            FloatSize::Bits(_) => FloatFormat::Binary256,
            FloatSize::Precision { precision, scale } => {
                // Syntax Rules 40 and 42.
                if *precision < 2 {
                    return Err(syntax("an approximate numeric type's precision is at least 2"));
                }
                let scale = scale.unwrap_or(0);
                [
                    (10, 4, FloatFormat::Binary16),
                    (23, 7, FloatFormat::Binary32),
                    (52, 10, FloatFormat::Binary64),
                    (112, 14, FloatFormat::Binary128),
                    (236, 18, FloatFormat::Binary256),
                ]
                .into_iter()
                .find(|(most, scaled, _)| *precision <= *most && scale <= *scaled)
                .map(|(_, _, format)| format)
                .ok_or_else(|| syntax("an approximate numeric type's precision is at most 236 and its scale at most 18"))?
            }
        }),
        ValueTypeKind::Node => GqlTypeKind::Node,
        ValueTypeKind::Edge => GqlTypeKind::Edge,
        ValueTypeKind::Path => GqlTypeKind::Path,
        ValueTypeKind::List { element, max_length } => {
            if *max_length == Some(0) {
                return Err(syntax("a list type's maximum length is at least 1"));
            }
            // §18.9 Syntax Rule 73 a) ii): an open list holds any value.
            let element = match element {
                Some(element) => resolve_value_type(element)?,
                None => GqlType {
                    kind: GqlTypeKind::Any,
                    nullable: true,
                },
            };
            GqlTypeKind::List {
                element: Box::new(element),
                max: *max_length,
            }
        }
        ValueTypeKind::Record { fields: None } => GqlTypeKind::Record(None),
        ValueTypeKind::Record { fields: Some(fields) } => {
            let mut resolved = Vec::new();
            for (name, value_type) in fields {
                // §18.10 Syntax Rule 2: field type names are distinct.
                if resolved
                    .iter()
                    .any(|(existing, _): &(Box<str>, GqlType)| existing.as_ref() == name.as_str())
                {
                    return Err(syntax(format!("a record type names the field {name} twice")));
                }
                resolved.push((Box::from(name.as_str()), resolve_value_type(value_type)?));
            }
            resolved.sort_by(|left, right| left.0.cmp(&right.0));
            GqlTypeKind::Record(Some(resolved))
        }
        // Cypher's MAP is GQL's open record type.
        ValueTypeKind::Cypher(name) if name.as_str() == "MAP" => GqlTypeKind::Record(None),
        ValueTypeKind::Any => GqlTypeKind::Any,
        ValueTypeKind::PropertyValue => GqlTypeKind::PropertyValue,
        ValueTypeKind::Union(components) => {
            // Syntax Rule 88: the components are all nullable or all not.
            if components.iter().any(|component| component.not_null) && components.iter().any(|component| !component.not_null) {
                return Err(syntax("a union type's components are all nullable or all NOT NULL"));
            }
            let components = components.iter().map(resolve_value_type).collect::<GraphPlanResult<Vec<_>>>()?;
            // Syntax Rule 86: its nullability is its components'.
            let nullable = nullable && components.iter().any(|component| component.nullable);
            return Ok(match <[GqlType; 1]>::try_from(components) {
                Ok([one]) => GqlType {
                    nullable: nullable && one.nullable,
                    ..one
                },
                Err(components) => GqlType {
                    kind: GqlTypeKind::Union(components),
                    nullable,
                },
            });
        }
        // ID034: a decimal holds 38 digits; §18.9 Syntax Rules 29, 32, 37
        // and 38.
        ValueTypeKind::Decimal { precision, scale } => {
            let precision = precision.unwrap_or(u64::from(crate::decimal::MAXIMUM_DIGITS));
            let scale = scale.unwrap_or(0);
            if precision == 0 {
                return Err(syntax("a numeric type's precision is at least 1"));
            }
            if precision > u64::from(crate::decimal::MAXIMUM_DIGITS) {
                return Err(syntax(format!(
                    "a decimal's precision is at most {}",
                    crate::decimal::MAXIMUM_DIGITS
                )));
            }
            if scale > precision {
                return Err(syntax("a decimal's scale is at most its precision"));
            }
            GqlTypeKind::Decimal {
                precision: precision as u32,
                scale: scale as u32,
            }
        }
        // Syntax Rules 7, 8, 10, 21 and 28.
        ValueTypeKind::ByteString { min_length, max_length } => {
            if *max_length == Some(0) {
                return Err(syntax("a byte string type's maximum or fixed length is at least 1"));
            }
            let min = min_length.unwrap_or(0);
            if max_length.is_some_and(|max| min > max) {
                return Err(syntax("a byte string type's minimum length is at most its maximum"));
            }
            if min > MAXIMUM_BYTES_LENGTH || max_length.is_some_and(|max| max > MAXIMUM_BYTES_LENGTH) {
                return Err(GraphPlanError::UnsupportedCypher(
                    QueryFailureReason::Other,
                    format!("a byte string type declares lengths of at most {MAXIMUM_BYTES_LENGTH} here"),
                ));
            }
            GqlTypeKind::Bytes { min, max: *max_length }
        }
        ValueTypeKind::Date => GqlTypeKind::Date,
        ValueTypeKind::LocalTime => GqlTypeKind::LocalTime,
        ValueTypeKind::ZonedTime => GqlTypeKind::ZonedTime,
        ValueTypeKind::LocalDatetime => GqlTypeKind::LocalDatetime,
        ValueTypeKind::ZonedDatetime => GqlTypeKind::ZonedDatetime,
        ValueTypeKind::Duration { year_month } => GqlTypeKind::Duration(*year_month),
        ValueTypeKind::Graph => return Err(unsupported("graph reference value types are not supported")),
        ValueTypeKind::Cypher(name) => return Err(unsupported(&format!("the type {name} is not supported"))),
    };
    // NULL NOT NULL is the empty type, and NOTHING never includes null.
    let nullable = nullable && !matches!(written.kind, ValueTypeKind::Nothing);
    Ok(GqlType { kind, nullable })
}
