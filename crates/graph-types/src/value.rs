use std::collections::BTreeMap;

use crate::{NodeId, Path, RelationshipRef};

/// A date represented as days from the Unix epoch using the proleptic
/// Gregorian calendar.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DateValue {
    pub days_since_epoch: i32,
}

impl DateValue {
    /// Parses a Cypher date string with the parser every engine shares
    /// (`hydradb_cypher_date::parse_date`), so a date accepted here is
    /// accepted by `date('...')` on every route and the other way round.
    pub fn parse_iso(text: &str) -> Option<Self> {
        use chrono::NaiveDate;

        let date = hydradb_cypher_date::parse_date(text)?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
        i32::try_from(date.signed_duration_since(epoch).num_days())
            .ok()
            .map(|days_since_epoch| Self { days_since_epoch })
    }

    pub fn to_iso_string(self) -> Option<String> {
        use chrono::{Days, NaiveDate};

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
        let date = if self.days_since_epoch >= 0 {
            epoch.checked_add_days(Days::new(self.days_since_epoch as u64))?
        } else {
            epoch.checked_sub_days(Days::new(self.days_since_epoch.unsigned_abs() as u64))?
        };
        Some(date.format("%Y-%m-%d").to_string())
    }
}

/// A wall-clock time represented as nanoseconds from midnight.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LocalTimeValue {
    pub nanos_since_midnight: u64,
}

/// A wall-clock time and UTC offset. Offset is retained independently of any
/// optional named-zone metadata.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimeValue {
    pub local: LocalTimeValue,
    pub offset_seconds: i32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LocalDateTimeValue {
    pub date: DateValue,
    pub time: LocalTimeValue,
}

/// An instant at nanosecond precision plus the source zone information needed
/// to round-trip a zoned Cypher datetime.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DateTimeValue {
    pub unix_nanos: i128,
    pub offset_seconds: i32,
    pub timezone: Option<String>,
}

/// Cypher durations retain all three independently meaningful components.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DurationValue {
    pub months: i64,
    pub days: i64,
    pub nanos: i128,
}

/// A two- or three-dimensional point, preserving the coordinate reference
/// system and the exact IEEE-754 coordinate bits in codecs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PointValue {
    pub srid: u32,
    pub x: f64,
    pub y: f64,
    pub z: Option<f64>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExtensionValue {
    pub type_name: String,
    pub version: u32,
    pub payload: Vec<u8>,
}

pub type MapValue = BTreeMap<String, CypherValue>;

/// A GQL temporal value and its original display text.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CypherTemporal {
    Date {
        days: i64,
        text: String,
    },
    LocalTime {
        nanoseconds: i64,
        text: String,
    },
    ZonedTime {
        nanoseconds: i64,
        offset_seconds: i32,
        text: String,
    },
    LocalDateTime {
        seconds: i64,
        nanoseconds: u32,
        text: String,
    },
    ZonedDateTime {
        seconds: i64,
        nanoseconds: u32,
        offset_seconds: i32,
        text: String,
    },
    Duration {
        months: i64,
        days: i64,
        seconds: i64,
        nanoseconds: i32,
        text: String,
    },
}

/// A node binding materialized for a public result, including the complete
/// metadata fetched from the query's pinned read view.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeEntity {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: MapValue,
}

/// A relationship binding materialized for a public result, including its
/// properties in addition to the identity and topology reference.
#[derive(Clone, Debug, PartialEq)]
pub struct RelationshipEntity {
    pub identity: RelationshipRef,
    pub properties: MapValue,
}

/// Complete value algebra for the graph query boundary.
///
/// `PartialEq` is the representation-level comparison. Cypher equality (which
/// equates numerically equal integers/floats and recursively compares values)
/// is provided separately by [`crate::semantic_eq`].
#[derive(Clone, Debug, PartialEq)]
pub enum CypherValue {
    Null,
    /// Absence is not Cypher `null`; providers use it for an unbound or
    /// missing property result.
    Missing,
    Boolean(bool),
    Integer(i128),
    Float(f64),
    String(String),
    List(Vec<CypherValue>),
    Map(MapValue),
    Record(MapValue),
    Node(NodeId),
    Relationship(RelationshipRef),
    NodeEntity(NodeEntity),
    RelationshipEntity(RelationshipEntity),
    Path(Path),
    Date(DateValue),
    LocalTime(LocalTimeValue),
    Time(TimeValue),
    LocalDateTime(LocalDateTimeValue),
    DateTime(DateTimeValue),
    Duration(DurationValue),
    Point(PointValue),
    /// Forward-compatible value supplied by a backend or extension codec.
    Extension(ExtensionValue),
    /// An exact decimal represented by its literal text.
    Decimal(String),
    Bytes(Vec<u8>),
    Temporal(CypherTemporal),
}
