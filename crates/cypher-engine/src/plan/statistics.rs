use crate::{PatternDirection, ScalarValue, Symbol};

/// Persisted property facts available to physical planning.
///
/// A storage adapter may populate any subset. Missing values mean "unknown",
/// never zero. Scalar spellings remain lossless so the planner does not invent
/// precision while comparing min/max or percentile bounds.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PropertyStatistics {
    pub non_null_count: Option<u64>,
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub average: Option<ScalarValue>,
    pub p95: Option<ScalarValue>,
    pub bloom: Option<BloomFilterStatistics>,
}

/// The numeric domain of one property's index entries: integers and floats
/// merged by value into one `f64` range, as the ordered index walk merges
/// them. `entries` counts only the numeric entries.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PropertyNumericRange {
    pub min: f64,
    pub max: f64,
    pub entries: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BloomFilterStatistics {
    pub bit_count: u64,
    pub hash_count: u32,
    pub inserted_count: u64,
    pub false_positive_rate_ppm: u32,
}

/// Immutable statistics view consumed only by physical planning.
///
/// Implementations may be backed by a snapshot loaded from disk, but these
/// methods must not perform I/O. That keeps logical lowering deterministic and
/// makes one planning attempt observe one coherent statistics generation.
pub trait StatisticsProvider: Send + Sync {
    /// True only for a provider whose counts may rank a vertex's equality,
    /// range and label candidates against each other. Every older provider
    /// stays `false`, which is what keeps their plans byte-identical: the
    /// planner then takes the first equality exactly as it always has.
    fn ranks_access_candidates(&self) -> bool {
        false
    }

    /// The numeric domain of a vertex property's index entries, for
    /// estimating the share of entries a range keeps. `None` when unknown, or
    /// when the property holds no number.
    fn property_numeric_range(&self, _labels: &[Symbol], _property: &Symbol) -> Option<PropertyNumericRange> {
        None
    }

    fn vertex_count(&self, _labels: &[Symbol]) -> Option<u64> {
        None
    }

    fn property_statistics(&self, _labels: &[Symbol], _property: &Symbol) -> Option<PropertyStatistics> {
        None
    }

    /// Exact or estimated matches for one property value. Unlike
    /// `property_statistics`, this can consume a persisted point-frequency
    /// record and avoids assuming a uniform histogram when a hot value is
    /// known explicitly.
    fn property_value_count(&self, _labels: &[Symbol], _property: &Symbol, _value: &ScalarValue) -> Option<u64> {
        None
    }

    /// Number of relationships matching any of `relationship_types`.
    /// Implementations return `None` for an untyped relationship pattern when
    /// they do not persist an all-relationship count.
    fn relationship_count(&self, _relationship_types: &[Symbol]) -> Option<u64> {
        None
    }

    /// Relationships the storage expansion will inspect for a source-label
    /// population. This is deliberately separate from the database-wide type
    /// count: label cardinality alone cannot reveal whether rare sources are
    /// low-degree vertices or hubs.
    fn relationship_expansion_count(
        &self,
        _source_labels: &[Symbol],
        _relationship_types: &[Symbol],
        _direction: PatternDirection,
    ) -> Option<u64> {
        None
    }

    fn relationship_property_statistics(&self, _relationship_types: &[Symbol], _property: &Symbol) -> Option<PropertyStatistics> {
        None
    }

    fn relationship_property_value_count(&self, _relationship_types: &[Symbol], _property: &Symbol, _value: &ScalarValue) -> Option<u64> {
        None
    }

    fn relationship_bloom_may_contain(&self, _relationship_types: &[Symbol], _property: &Symbol, _value: &ScalarValue) -> Option<bool> {
        None
    }

    /// `Some(false)` means definitely absent; `Some(true)` means possibly
    /// present. `None` means no compatible bloom filter was loaded.
    fn bloom_may_contain(&self, _labels: &[Symbol], _property: &Symbol, _value: &ScalarValue) -> Option<bool> {
        None
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UnknownStatistics;

impl StatisticsProvider for UnknownStatistics {}
