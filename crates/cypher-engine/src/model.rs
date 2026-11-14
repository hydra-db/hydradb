use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::{ExpandDirection, ScalarValue};

pub type VertexId = u64;
pub type RelationshipId = u64;
pub type ParameterMap = BTreeMap<String, ScalarValue>;

/// List-valued query parameters. Separate from `ParameterMap` because
/// `ScalarValue` is also the stored-property type and must not gain a list
/// variant. Only `IN` reads these.
pub type ListParameterMap = BTreeMap<String, Vec<ScalarValue>>;

/// Cypher scalar equality used by both residual expressions and semantic
/// property seeks. Numeric types compare by value, so integer `1` equals float
/// `1.0`; `NULL` remains unknown and is handled by expression evaluation.
pub fn scalar_values_equal(left: &ScalarValue, right: &ScalarValue) -> bool {
    match (left, right) {
        (ScalarValue::Integer(left), ScalarValue::Float(right)) | (ScalarValue::Float(right), ScalarValue::Integer(left)) => {
            right.parse::<f64>().ok().is_some_and(|right| integer_float_equal(*left, right))
        }
        (ScalarValue::Float(left), ScalarValue::Float(right)) => match (left.parse::<f64>(), right.parse::<f64>()) {
            (Ok(left), Ok(right)) => left == right,
            _ => false,
        },
        // Numbers by their algebraic values (§19.3): a decimal against an
        // integer, or against a float's exact binary value, so 0.5 equals
        // 0.5M but 0.1, a little more than a tenth, does not equal 0.1M.
        (ScalarValue::Integer(left), ScalarValue::Decimal(right)) | (ScalarValue::Decimal(right), ScalarValue::Integer(left)) => {
            right.cmp_integer(*left) == Ordering::Equal
        }
        (ScalarValue::Float(left), ScalarValue::Decimal(right)) | (ScalarValue::Decimal(right), ScalarValue::Float(left)) => {
            left.parse::<f64>().is_ok_and(|left| right.cmp_f64(left) == Some(Ordering::Equal))
        }
        _ => left == right,
    }
}

fn integer_float_equal(integer: i128, float: f64) -> bool {
    const I128_INCLUSIVE_LOWER: f64 = -170141183460469231731687303715884105728.0;
    const I128_EXCLUSIVE_UPPER: f64 = 170141183460469231731687303715884105728.0;
    float.is_finite() && float.fract() == 0.0 && (I128_INCLUSIVE_LOWER..I128_EXCLUSIVE_UPPER).contains(&float) && (float as i128) == integer
}

pub(crate) fn scalar_value_rank(value: &ScalarValue) -> u8 {
    match value {
        ScalarValue::Boolean(_) => 0,
        ScalarValue::Integer(_) | ScalarValue::Float(_) | ScalarValue::Decimal(_) => 1,
        ScalarValue::String(_) => 2,
        ScalarValue::Bytes(_) => 3,
        ScalarValue::Date(_) => 4,
        ScalarValue::List(_) => 5,
        ScalarValue::Null => u8::MAX,
    }
}

/// The total order Cypher sorts scalar values by, and the order an ordered
/// property index walk must produce: booleans, then numbers (integers and
/// floats compared by value, so `1` ties `1.0` and `-0.0` ties `0.0`, with NaN
/// above every other number), then strings; `NULL` sorts last. Comparisons in
/// expressions use the same order, so an index bound on a stored value decides
/// exactly what the equivalent predicate would.
pub fn compare_property_values(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    match (left, right) {
        (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
        (ScalarValue::Null, _) => Ordering::Greater,
        (_, ScalarValue::Null) => Ordering::Less,
        (ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => left.cmp(right),
        (ScalarValue::Integer(left), ScalarValue::Integer(right)) => left.cmp(right),
        (ScalarValue::Float(left), ScalarValue::Float(right)) => compare_float_spellings(left, right),
        (ScalarValue::Integer(left), ScalarValue::Float(right)) => right
            .parse::<f64>()
            .ok()
            .map(|right| compare_integer_float(*left, right))
            .unwrap_or_else(|| left.to_string().cmp(&right.to_string())),
        (ScalarValue::Float(left), ScalarValue::Integer(right)) => left
            .parse::<f64>()
            .ok()
            .map(|left| compare_integer_float(*right, left).reverse())
            .unwrap_or_else(|| left.to_string().cmp(&right.to_string())),
        (ScalarValue::String(left), ScalarValue::String(right)) => left.cmp(right),
        (ScalarValue::Decimal(left), ScalarValue::Decimal(right)) => left.cmp(right),
        (ScalarValue::Integer(left), ScalarValue::Decimal(right)) => compare_integer_decimal(*left, *right),
        (ScalarValue::Decimal(left), ScalarValue::Integer(right)) => compare_integer_decimal(*right, *left).reverse(),
        (ScalarValue::Float(left), ScalarValue::Decimal(right)) => compare_float_decimal(left, *right).reverse(),
        (ScalarValue::Decimal(left), ScalarValue::Float(right)) => compare_float_decimal(right, *left),
        // Byte strings byte by byte, a prefix first.
        (ScalarValue::Bytes(left), ScalarValue::Bytes(right)) => left.cmp(right),
        (ScalarValue::Date(left), ScalarValue::Date(right)) => left.cmp(right),
        (ScalarValue::List(left), ScalarValue::List(right)) => {
            for (left, right) in left.iter().zip(right) {
                let ordering = compare_property_values(left, right);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            left.len().cmp(&right.len())
        }
        _ => scalar_value_rank(left).cmp(&scalar_value_rank(right)),
    }
}

fn compare_integer_decimal(integer: i128, decimal: crate::Decimal) -> Ordering {
    decimal.cmp_integer(integer).reverse()
}

/// How `decimal` orders against the float `float` spells, exactly; a NaN
/// sorts as it does against any number.
fn compare_float_decimal(float: &str, decimal: crate::Decimal) -> Ordering {
    match float.parse::<f64>().ok().and_then(|float| decimal.cmp_f64(float)) {
        Some(order) => order,
        None => compare_float_spellings(&format!("{:?}", decimal.to_f64()), float),
    }
}

fn compare_float_spellings(left: &str, right: &str) -> Ordering {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(left), Ok(right)) => left.partial_cmp(&right).unwrap_or_else(|| left.total_cmp(&right)),
        _ => left.cmp(right),
    }
}

fn compare_integer_float(integer: i128, float: f64) -> Ordering {
    const I128_INCLUSIVE_LOWER: f64 = -170141183460469231731687303715884105728.0;
    const I128_EXCLUSIVE_UPPER: f64 = 170141183460469231731687303715884105728.0;
    // Float-to-float comparison places NaN above every number (and a
    // negative NaN below), as legacy does for both comparisons. Integers must
    // agree, or the order would not be transitive.
    if float.is_nan() {
        return if float.is_sign_negative() {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    if float < I128_INCLUSIVE_LOWER {
        return Ordering::Greater;
    }
    if float >= I128_EXCLUSIVE_UPPER {
        return Ordering::Less;
    }
    let truncated = float as i128;
    match integer.cmp(&truncated) {
        Ordering::Equal if float.fract().is_sign_positive() && float.fract() > 0.0 => Ordering::Less,
        Ordering::Equal if float.fract().is_sign_negative() && float.fract() < 0.0 => Ordering::Greater,
        ordering => ordering,
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReadRequest {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpandRequest {
    pub input_vertex_ids: Vec<VertexId>,
    pub direction: ExpandDirection,
    /// Empty means every relationship type.
    pub relationship_types: Vec<String>,
}

/// One neighbour an expansion reaches, listed before any relationship record
/// is read: `input` has at least one `relationship_type` relationship to
/// `neighbor` in the direction of the [`ExpandRequest`] that listed it.
///
/// Listing neighbours is the cheap half of an expansion -- an adjacency read
/// per input -- and reading their relationship records the expensive half.
/// Keeping them apart is what lets a `LIMIT` stop after the records it needs.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExpandNeighbor {
    pub input: VertexId,
    pub neighbor: VertexId,
    pub relationship_type: String,
}

/// One equality restriction offered to a runtime access-path probe.
///
/// `values` is the complete set a vertex may hold for `property` and still
/// satisfy the restriction, so the vertices matching any of them are a
/// superset of the query's answer. A candidate is only ever built from a
/// conjunct of the predicate; a disjunct over two different properties is not
/// a candidate, because neither side alone covers the result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropertyEqualityCandidate {
    pub property: String,
    pub values: Vec<ScalarValue>,
}

/// Which kind of access a statistics-chosen operator ran.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessPathKind {
    Equality,
    Range,
    LabelScan,
}

impl AccessPathKind {
    /// A closed vocabulary, safe as a span attribute or metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Equality => "equality",
            Self::Range => "range",
            Self::LabelScan => "label_scan",
        }
    }
}

/// What one statistics-chosen access did, as reported to
/// [`crate::GraphRead::record_statistics_access`]. Names a property, never a
/// value, so it may reach a span.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsAccessReport {
    pub kind: AccessPathKind,
    pub property: Option<String>,
    /// The planner's estimate for the chosen candidate.
    pub estimated_rows: u64,
    /// Index entries the access really read before it finished or declined.
    pub entries_read: u64,
    /// The access read past its limit and handed over to its fallback.
    pub declined: bool,
}

/// What a guarded access returned: its vertices, or `None` once it declined,
/// with the index entries it really read either way. A backend may decline
/// without reading anything, and that must not be reported as a full read.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BoundedVertexIds {
    pub ids: Option<Vec<VertexId>>,
    pub index_entries: u64,
}

impl BoundedVertexIds {
    /// A decline after reading `index_entries` entries.
    pub fn declined(index_entries: u64) -> Self {
        Self { ids: None, index_entries }
    }

    /// `ids` read one index entry each, or a decline once there are more than
    /// `max_index_entries` of them.
    pub fn within(ids: Vec<VertexId>, max_index_entries: usize) -> Self {
        let index_entries = ids.len() as u64;
        Self {
            ids: (ids.len() <= max_index_entries).then_some(ids),
            index_entries,
        }
    }
}

/// One side of a range restriction on an ordered property scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropertyBound {
    pub value: ScalarValue,
    pub inclusive: bool,
}

/// A position in an ordered walk, naming the last entry a page returned.
///
/// The index orders entries by `(value, vertex_id)`, so both are needed: many
/// vertices share a value, and a page may end part-way through them. `value`
/// is the property's own stored value, never a backend's key encoding of it:
/// the engine compares it with the values on the rows it holds, under
/// [`compare_property_values`]. Values of different types are ordered by that
/// same function, so a position is meaningful across the whole index, not
/// only within one type's run.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderedScanPosition {
    pub value: ScalarValue,
    pub vertex_id: VertexId,
}

/// One page of a bounded, ordered walk over a vertex property index.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OrderedPropertyScanPage {
    pub vertices: Vec<VertexRecord>,
    /// The last entry this page reached, to resume after.
    pub last: Option<OrderedScanPosition>,
    /// The index holds nothing further within the window. Reported rather
    /// than inferred from a short page: the caller cannot tell a page the
    /// backend chose to end from one the index ran out of, and guessing wrong
    /// either loops forever or silently returns fewer rows than were asked
    /// for.
    pub exhausted: bool,
    /// Index entries the page read to produce its rows, entries of other
    /// labels and outside the restrictions included, when the backend counts
    /// them. A bounded read declines on this, not on the rows returned: a
    /// property shared by many labels can cost far more than it yields.
    pub index_entries: Option<u64>,
}

/// One page of the same walk as [`OrderedPropertyScanPage`], as index
/// entries instead of records: each qualifying vertex's id with the value
/// its index entry holds, which is the property's stored value.
///
/// For a caller that reads nothing of the vertex beyond its id, the walk's
/// labels and the walked property, the record is pure cost, and on a backend
/// whose record read is a point get plus an overlay it is most of the walk.
/// The backend still proves each entry names an existing vertex carrying
/// the labels; what it skips is reading the record.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OrderedPropertyEntryPage {
    /// `(vertex id, value)` per qualifying entry, in the page's order.
    pub entries: Vec<(VertexId, ScalarValue)>,
    pub last: Option<OrderedScanPosition>,
    pub exhausted: bool,
    pub index_entries: Option<u64>,
}

/// A bounded, ordered walk over one vertex property index.
///
/// The backend must return only vertices that carry every label in `labels`
/// and whose stored `property` is a non-null value satisfying `prefix`,
/// `lower` and `upper` (see [`OrderedPropertyScanRequest::accepts`]).
///
/// Index order is the property value under [`compare_property_values`] --
/// booleans, then integers and floats merged by value, then strings -- in the
/// `ascending` direction, then the vertex ID, using `id_ascending` for ties.
/// Values that compare equal, such as `1` and `1.0`, are one tie. It may
/// return a superset; the engine defensively sorts and truncates.
///
/// `required` is a page size, not an answer size. The caller applies a
/// residual the backend knows nothing about, so it cannot say how many of the
/// rows it returns will survive -- it returns that many and reports whether
/// more exist. The caller resumes with `after` until enough rows survive.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderedPropertyScanRequest {
    pub property: String,
    pub labels: Vec<String>,
    pub ascending: bool,
    pub id_ascending: bool,
    pub prefix: Option<String>,
    pub lower: Option<PropertyBound>,
    pub upper: Option<PropertyBound>,
    /// How many entries to return in this page.
    pub required: usize,
    /// Resume strictly after this position. `None` starts at the window's
    /// beginning.
    pub after: Option<OrderedScanPosition>,
    /// Return entries in the order the backend walks them and stop at exactly
    /// `required`, dropping none, rather than finishing the last value's run
    /// and keeping the best `required` of it by the backend's own ranking.
    ///
    /// That ranking is correct only while it is the caller's and nothing
    /// downstream can reject a row it kept. Neither holds once the caller
    /// orders ties by a key the index does not store, or applies a residual
    /// the backend cannot see: an entry the backend dropped may rank ahead of
    /// one it kept, or be the only survivor of the filter, and the cursor has
    /// moved past it for good. Handing back the whole run instead would be
    /// unbounded -- a tie group can be the entire index -- so the page stops
    /// at `required` and `after` resumes inside the run; the caller reads the
    /// run in pages and keeps only what its window needs.
    pub index_order: bool,
    /// Sorted vertex IDs the caller will accept. A backend may skip any other
    /// entry without loading it; ignoring this is still correct.
    pub allowed: Option<std::sync::Arc<Vec<VertexId>>>,
    /// End the page, not exhausted, once it has judged this many index
    /// entries, whether or not they qualified. `None` bounds the page by
    /// `required` rows alone. A backend that ignores it stays correct but
    /// lets a bounded read cost more than its limit.
    ///
    /// A caller that sets it bounds the whole read by the `index_entries`
    /// each page reports, so a backend need not also charge those entries to
    /// a query-wide allowance: the read declines past its own limit instead
    /// of failing, and what it leaves on such an allowance would be taken
    /// from whatever runs after the decline.
    pub max_index_entries: Option<usize>,
}

impl OrderedPropertyScanRequest {
    /// Whether a stored, non-null value satisfies every restriction carried
    /// by this request, under [`compare_property_values`]. A prefix admits
    /// only strings. Backends without a native seek can filter with it.
    pub fn accepts(&self, value: &ScalarValue) -> bool {
        restrictions_accept(self.prefix.as_deref(), self.lower.as_ref(), self.upper.as_ref(), value)
    }
}

/// One or more property-index point spans. `Multiple` stays distinct even when
/// it contains one value because operation attribution and backend batching are
/// properties of the planned access, not of its bound cardinality.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PropertyPointSet<T> {
    Single(T),
    Multiple(Vec<T>),
}

impl<T> PropertyPointSet<T> {
    pub fn values(&self) -> &[T] {
        match self {
            Self::Single(value) => std::slice::from_ref(value),
            Self::Multiple(values) => values,
        }
    }
}

/// One runtime vertex-storage request produced by a provider-prepared `VertexScan`.
///
/// The variants are typed rather than optional fields: every request names one
/// storage family and only the span, projection and paging data that family
/// accepts. Ordered property pages retain their established cursor contract.
#[derive(Clone, Debug, PartialEq)]
pub enum VertexStorageScanRequest {
    /// Vertex-record point spans. The executor hydrates these IDs through its
    /// request-local identity map.
    IdPoints { ids: Vec<VertexId> },
    /// One or more equality spans over a property index.
    PropertyPoints {
        property: String,
        points: PropertyPointSet<ScalarValue>,
    },
    /// The complete prefix for one property index.
    PropertyPresence { property: String },
    /// A cursor-based page from an ordered property index.
    OrderedProperty(Box<OrderedPropertyScanRequest>),
    /// One label-index prefix, optionally under a proven semantic result cap.
    LabelPrefix { label: String, max_results: Option<usize> },
    /// The complete vertex inventory.
    AllVertices,
}

/// The typed output of one [`VertexStorageScanRequest`]. Ordinary point and
/// prefix scans return IDs for identity-map hydration. Ordered scans retain
/// hydrated records and their cursor because pagination depends on both.
#[derive(Clone, Debug, PartialEq)]
pub enum VertexStorageScanPage {
    Ids(Vec<VertexId>),
    /// Hydrated point records returned by providers that can combine the
    /// identity lookup and metadata read in one storage operation.
    Records(Vec<VertexRecord>),
    Ordered(OrderedPropertyScanPage),
}

fn restrictions_accept(prefix: Option<&str>, lower: Option<&PropertyBound>, upper: Option<&PropertyBound>, value: &ScalarValue) -> bool {
    if matches!(value, ScalarValue::Null) {
        return false;
    }
    if let Some(prefix) = prefix {
        if !matches!(value, ScalarValue::String(value) if value.starts_with(prefix)) {
            return false;
        }
    }
    if let Some(lower) = lower {
        let ordering = compare_property_values(value, &lower.value);
        if ordering.is_lt() || (ordering.is_eq() && !lower.inclusive) {
            return false;
        }
    }
    if let Some(upper) = upper {
        let ordering = compare_property_values(value, &upper.value);
        if ordering.is_gt() || (ordering.is_eq() && !upper.inclusive) {
            return false;
        }
    }
    true
}

/// One page of an ordered walk over one relationship property index: the
/// relationship counterpart of [`OrderedPropertyScanRequest`].
///
/// The backend returns, in index order, relationships of `relationship_type`
/// whose stored `property` is a non-null value satisfying `prefix`, `lower`
/// and `upper` (see [`Self::accepts`]), with the same properties expansion
/// would report for them. Index order is the property value under
/// [`compare_property_values`] in the `ascending` direction, then the
/// relationship's `(source, target)` in the same direction; the order of
/// parallel relationships sharing a value and both endpoints is the backend's,
/// but must be the same on every page. A page holds the relationships of at
/// most `required` index entries (one entry may carry several parallel
/// relationships), may hold fewer, including none, while `next` is set, and
/// the engine keeps pulling pages until its window is decided.
///
/// Restrictions are sound, not complete, exactly as for vertex walks: the
/// engine re-checks every row and evaluates any residual predicate. A backend
/// that cannot honour a restriction, or has no ordered relationship property
/// index at all, must fail the request rather than return a wider walk.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderedRelationshipPropertyScanRequest {
    pub relationship_type: String,
    pub property: String,
    pub ascending: bool,
    pub prefix: Option<String>,
    pub lower: Option<PropertyBound>,
    pub upper: Option<PropertyBound>,
    /// Maximum index entries in this page.
    pub required: usize,
    /// Resume strictly after this cursor. `None` starts at the window's
    /// beginning.
    pub after: Option<OrderedScanCursor>,
}

impl OrderedRelationshipPropertyScanRequest {
    /// Whether a stored, non-null value satisfies every restriction carried
    /// by this request; see [`OrderedPropertyScanRequest::accepts`].
    pub fn accepts(&self, value: &ScalarValue) -> bool {
        restrictions_accept(self.prefix.as_deref(), self.lower.as_ref(), self.upper.as_ref(), value)
    }
}

/// An opaque continuation for an ordered relationship walk.
///
/// Unlike [`OrderedScanPosition`], nothing in the engine reads this: the
/// relationship walk settles its boundary tie from the rows a page returns and
/// hands the cursor straight back to the backend. Keeping it opaque is what
/// lets the backend record which of the two relationship indexes it was
/// reading, which a value-and-ID position could not express.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedScanCursor(pub Vec<u8>);

/// One page of an ordered relationship property scan.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OrderedRelationshipPropertyScanPage {
    /// Relationships in index order; see
    /// [`OrderedRelationshipPropertyScanRequest`].
    pub relationships: Vec<RelationshipRecord>,
    /// The last entry this page reached, to resume after.
    pub last: Option<OrderedScanCursor>,
    /// The index holds nothing further within the window, reported for the
    /// same reason [`OrderedPropertyScanPage::exhausted`] is.
    pub exhausted: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct VertexRecord {
    pub id: VertexId,
    pub labels: BTreeSet<String>,
    pub properties: BTreeMap<String, ScalarValue>,
}

impl VertexRecord {
    pub fn new(id: VertexId) -> Self {
        Self {
            id,
            labels: BTreeSet::new(),
            properties: BTreeMap::new(),
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.labels.insert(label.into());
        self
    }

    pub fn with_property(mut self, name: impl Into<String>, value: ScalarValue) -> Self {
        self.properties.insert(name.into(), value);
        self
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RelationshipRecord {
    pub id: RelationshipId,
    /// Canonical stored direction, independent of the query pattern direction.
    pub source: VertexId,
    pub target: VertexId,
    pub relationship_type: String,
    pub properties: BTreeMap<String, ScalarValue>,
}

impl RelationshipRecord {
    pub fn new(id: RelationshipId, source: VertexId, target: VertexId, relationship_type: impl Into<String>) -> Self {
        Self {
            id,
            source,
            target,
            relationship_type: relationship_type.into(),
            properties: BTreeMap::new(),
        }
    }

    pub fn with_property(mut self, name: impl Into<String>, value: ScalarValue) -> Self {
        self.properties.insert(name.into(), value);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryColumn {
    pub name: String,
}

/// Result values compare and hash structurally: `1` and `1.0` are different
/// values, as are `0.0` and `-0.0`, while two `NaN`s with the same spelling
/// and two `NULL`s are the same value. This preserves typed result assertions.
/// DISTINCT, UNION and grouping instead use Cypher semantic keys in the
/// executor: numeric representations compare by value, recursively in lists.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum QueryValue {
    Null,
    VertexId(VertexId),
    RelationshipId(RelationshipId),
    Count(u64),
    Scalar(ScalarValue),
    List(Vec<QueryValue>),
    /// A path (GQL §4.15.2), bound to a path or subpath variable (G004,
    /// G048).
    Path(Box<PathValue>),
    /// A record (GQL §4.15.4, GV45): its fields, ordered by name, each name
    /// once.
    Record(Vec<(Box<str>, QueryValue)>),
    /// A temporal instant or duration (GQL §4.16.6, GV39 to GV41).
    Temporal(crate::TemporalValue),
}

/// A path value: nodes and edges alternating, from its first node to its
/// last. `relationships[i]` connects `nodes[i]` and `nodes[i + 1]`, in either
/// direction.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PathValue {
    pub nodes: Vec<VertexRecord>,
    pub relationships: Vec<RelationshipRecord>,
}

impl QueryValue {
    /// Both public null representations denote the same absent value.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null | Self::Scalar(ScalarValue::Null))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct QueryRow {
    pub values: Vec<QueryValue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<QueryColumn>,
    pub rows: Vec<QueryRow>,
}
