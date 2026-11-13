use crate::Symbol;

/// A storage-independent scalar whose exact KV encoding is chosen by the backend.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Integer(i128),
    Float(Box<str>),
    String(Box<str>),
    /// An exact decimal number (GQL §4.16.5, GV17).
    Decimal(crate::Decimal),
    /// A byte string (GQL §4.16.4, GV35).
    Bytes(Box<[u8]>),
    Date(i32),
    List(Vec<ScalarValue>),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ValueOrigin {
    Literal,
    Parameter(Symbol),
}

/// A planning-time value with provenance retained for safe EXPLAIN output.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BoundValue {
    pub value: ScalarValue,
    pub origin: ValueOrigin,
}
