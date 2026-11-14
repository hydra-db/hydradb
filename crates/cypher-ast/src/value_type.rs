//! GQL's `<value type>` (§18.9), as written: the planner decides what each
//! name means, including the implementation-defined precisions and lengths.

use crate::Identifier;

/// A <value type>: its kind, and whether `NOT NULL` makes it material.
#[derive(Clone, Debug, PartialEq)]
pub struct ValueType {
    pub kind: ValueTypeKind,
    pub not_null: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ValueTypeKind {
    /// `NOTHING`, the empty type (GV72).
    Nothing,
    /// `NULL`, the null type (GV71); with `NOT NULL`, the empty type.
    Null,
    Boolean,
    /// `STRING [(min,] max)]`, `VARCHAR [(max)]` and `CHAR [(fixed)]`: the
    /// lengths written, a fixed length being both.
    CharacterString {
        min_length: Option<u64>,
        max_length: Option<u64>,
    },
    /// `BYTES`, `BINARY` and `VARBINARY` (GV35 to GV38).
    ByteString {
        min_length: Option<u64>,
        max_length: Option<u64>,
    },
    /// A binary exact numeric type (GV01 to GV19).
    Integer {
        signed: bool,
        size: IntegerSize,
    },
    /// `DECIMAL [(precision [, scale])]` (GV17).
    Decimal {
        precision: Option<u64>,
        scale: Option<u64>,
    },
    /// An approximate numeric type (GV20 to GV26).
    Float(FloatSize),
    Date,
    LocalTime,
    ZonedTime,
    LocalDatetime,
    ZonedDatetime,
    /// `DURATION(YEAR TO MONTH)` or `DURATION(DAY TO SECOND)`; Cypher's
    /// bare `DURATION` gives no unit group.
    Duration {
        year_month: Option<bool>,
    },
    /// `[ANY] NODE` or `VERTEX`.
    Node,
    /// `[ANY] EDGE` or `RELATIONSHIP`.
    Edge,
    Path,
    /// `[ANY] [PROPERTY] GRAPH` (GV60).
    Graph,
    /// `LIST<T>`, `T LIST` or a bare `LIST` (GV50, GV51).
    List {
        element: Option<Box<ValueType>>,
        max_length: Option<u64>,
    },
    /// `[ANY] RECORD` (GV47), or `[RECORD] {name T, ...}` with its field
    /// types (GV46).
    Record {
        fields: Option<Vec<(Identifier, ValueType)>>,
    },
    /// `ANY [VALUE]`, the open dynamic union type (GV66).
    Any,
    /// `[ANY] PROPERTY VALUE` (GV68).
    PropertyValue,
    /// `A | B` or `ANY<A | B>`, a closed dynamic union type (GV67).
    Union(Vec<ValueType>),
    /// A Cypher type name GQL does not have: `MAP`, `POINT`, `VECTOR`,
    /// `UUID`.
    Cypher(Identifier),
}

/// Which binary exact numeric type a name gives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegerSize {
    /// `INT8` to `INT256`, `UINT8` to `UINT256` and their verbose spellings:
    /// the bit width.
    Bits(u32),
    /// `SMALLINT`, `USMALLINT`, `SMALL INTEGER`.
    Small,
    /// `INT`, `INTEGER`, `UINT`.
    Regular,
    /// `BIGINT`, `UBIGINT`, `BIG INTEGER`.
    Big,
    /// `INT(p)`, `INTEGER(p)`, `UINT(p)` (GV09): the precision in bits.
    Precision(u64),
}

/// Which approximate numeric type a name gives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FloatSize {
    /// `FLOAT16` to `FLOAT256`: the bit width.
    Bits(u32),
    /// `FLOAT`.
    Regular,
    /// `REAL`.
    Real,
    /// `DOUBLE [PRECISION]`.
    Double,
    /// `FLOAT(p [, s])` (GV22).
    Precision { precision: u64, scale: Option<u64> },
}
