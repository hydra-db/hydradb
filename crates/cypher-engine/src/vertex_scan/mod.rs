//! Provider-owned vertex access preparation over the graph storage contract.
//!
//! The query planner supplies requirements. The provider keeps access selection,
//! execution, estimates and diagnostics together in a prepared scan.

mod explain;
mod slatedb;

use crate::{BoundValue, PhysicalExpression, PhysicalSort, PropertyPointSet, StatisticsProvider, Symbol};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VertexOutput {
    Record,
    /// Nothing above the scan reads the binding beyond its id, the request's
    /// labels and the access's property, so a property-range access may bind
    /// what each index entry establishes instead of reading the record. Set
    /// on a prepared plan by `mark_index_only_range_seeks`, never from
    /// statistics; every other access reads records whatever this says, and
    /// so does a range access's fallback.
    IndexEntries,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexWindow {
    pub ordering: Vec<PhysicalSort>,
    pub required: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexScanRequest {
    pub binding: Symbol,
    pub labels: Vec<Symbol>,
    /// Predicates admitted by the engine without crossing an evaluation barrier.
    /// Ordinary requests put the innermost filter first for the unranked fallback;
    /// ordered requests preserve the window's predicate evaluation order.
    pub predicates: Vec<PhysicalExpression>,
    pub output: VertexOutput,
    pub window: Option<VertexWindow>,
}

impl VertexScanRequest {
    pub fn new(binding: Symbol, labels: Vec<Symbol>) -> Self {
        Self {
            binding,
            labels,
            predicates: Vec::new(),
            output: VertexOutput::Record,
            window: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexScanProperties {
    pub estimated_rows: Option<u64>,
    pub ordering: Vec<PhysicalSort>,
    pub residual_predicates: Vec<PhysicalExpression>,
    pub window_accepted: bool,
    pub may_scan_inventory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedVertexScan {
    request: VertexScanRequest,
    pub(crate) plan: VertexAccess,
    properties: VertexScanProperties,
}

impl PreparedVertexScan {
    fn new(request: VertexScanRequest, plan: VertexAccess, statistics: &dyn StatisticsProvider) -> Self {
        let estimated_rows = match &plan {
            VertexAccess::IdPoint { .. } => Some(1),
            VertexAccess::IdPoints { values } => Some(values.len() as u64),
            VertexAccess::PropertyPoints { property, points } => {
                crate::plan::access::property_values_estimate(&request.labels, property, points.values(), statistics)
            }
            VertexAccess::PropertyPresence { property } => statistics
                .property_statistics(&request.labels, property)
                .and_then(|stats| stats.non_null_count),
            VertexAccess::OrderedProperty(ordered) => Some(
                statistics
                    .property_statistics(&request.labels, &ordered.property)
                    .and_then(|stats| stats.non_null_count)
                    .map_or(ordered.required as u64, |rows| rows.min(ordered.required as u64)),
            ),
            VertexAccess::LabelPrefix { label } => statistics
                .vertex_count(&request.labels)
                .or_else(|| statistics.vertex_count(std::slice::from_ref(label))),
            VertexAccess::AllVertices => statistics.vertex_count(&[]),
            VertexAccess::Guarded { estimated_rows, .. } | VertexAccess::Range { estimated_rows, .. } => Some(*estimated_rows),
        };
        let may_scan_inventory = match &plan {
            VertexAccess::AllVertices => true,
            VertexAccess::Guarded { fallback, .. } | VertexAccess::Range { fallback, .. } => fallback.properties.may_scan_inventory,
            _ => false,
        };
        let (ordering, residual_predicates, window_accepted) = match &plan {
            VertexAccess::OrderedProperty(ordered) => (ordered.items.clone(), Vec::new(), true),
            _ => (Vec::new(), request.predicates.clone(), false),
        };
        let properties = VertexScanProperties {
            estimated_rows,
            ordering,
            residual_predicates,
            window_accepted,
            may_scan_inventory,
        };
        Self { request, plan, properties }
    }

    pub fn request(&self) -> &VertexScanRequest {
        &self.request
    }
    pub fn binding(&self) -> &Symbol {
        &self.request.binding
    }
    pub fn labels(&self) -> &[Symbol] {
        &self.request.labels
    }
    pub fn properties(&self) -> &VertexScanProperties {
        &self.properties
    }
    /// The property a statistics-chosen range access walks, when that is the
    /// access.
    pub(crate) fn range_property(&self) -> Option<&Symbol> {
        match &self.plan {
            VertexAccess::Range { property, .. } => Some(property),
            _ => None,
        }
    }
    /// Let a range access bind index entries rather than records. Only the
    /// whole-plan pass may call this, once it has proven nothing reads more.
    pub(crate) fn set_index_only(&mut self) {
        self.request.output = VertexOutput::IndexEntries;
    }
    pub(crate) fn index_only(&self) -> bool {
        self.request.output == VertexOutput::IndexEntries
    }
    pub(crate) fn is_unrestricted_population(&self) -> bool {
        matches!(self.plan, VertexAccess::LabelPrefix { .. } | VertexAccess::AllVertices)
    }
    pub fn population_labels(&self) -> Option<&[Symbol]> {
        (!matches!(self.plan, VertexAccess::AllVertices)).then_some(self.labels())
    }
    pub fn access_kind(&self) -> &'static str {
        match &self.plan {
            VertexAccess::IdPoint { .. } => "id-point",
            VertexAccess::IdPoints { .. } => "id-points",
            VertexAccess::PropertyPoints {
                points: PropertyPointSet::Single(_),
                ..
            } => "property-point",
            VertexAccess::PropertyPoints { .. } => "property-points",
            VertexAccess::PropertyPresence { .. } => "property-presence",
            VertexAccess::OrderedProperty(_) => "ordered-property",
            VertexAccess::LabelPrefix { .. } => "label-prefix",
            VertexAccess::AllVertices => "inventory",
            VertexAccess::Guarded {
                access: GuardedAccess::PropertySeek { .. },
                ..
            } => "guarded-property",
            VertexAccess::Guarded {
                access: GuardedAccess::LabelScan { .. },
                ..
            } => "guarded-label",
            VertexAccess::Range { .. } => "property-range",
        }
    }
}

/// SlateDB index policy over `GraphRead`. Preparation needs statistics, not a
/// live reader, so EXPLAIN and capability checks do not open a read session.
pub struct SlateDbDataSource<'a> {
    statistics: &'a dyn StatisticsProvider,
}

impl<'a> SlateDbDataSource<'a> {
    pub fn new(statistics: &'a dyn StatisticsProvider) -> Self {
        Self { statistics }
    }
    pub fn statistics(&self) -> &dyn StatisticsProvider {
        self.statistics
    }
    /// Check ordering eligibility before a speculative rewrite binds its filters.
    pub fn supports_ordering(&self, binding: &Symbol, ordering: &[PhysicalSort]) -> bool {
        slatedb::ordered_property(binding, ordering).is_some()
    }
    pub fn prepare_vertex_scan(&self, request: VertexScanRequest) -> PreparedVertexScan {
        let plan = slatedb::prepare_access(&request, self.statistics);
        PreparedVertexScan::new(request, plan, self.statistics)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GuardedAccess {
    PropertySeek { property: Symbol, values: Vec<BoundValue> },
    LabelScan { label: Symbol },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VertexAccess {
    IdPoint {
        value: BoundValue,
    },
    IdPoints {
        values: Vec<BoundValue>,
    },
    PropertyPoints {
        property: Symbol,
        points: PropertyPointSet<BoundValue>,
    },
    PropertyPresence {
        property: Symbol,
    },
    OrderedProperty(Box<OrderedVertexStorageAccess>),
    LabelPrefix {
        label: Symbol,
    },
    AllVertices,
    Guarded {
        access: GuardedAccess,
        estimated_rows: u64,
        read_limit: usize,
        fallback: Box<PreparedVertexScan>,
    },
    Range {
        property: Symbol,
        lower: Option<(BoundValue, bool)>,
        upper: Option<(BoundValue, bool)>,
        estimated_rows: u64,
        read_limit: usize,
        fallback: Box<PreparedVertexScan>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderedVertexStorageAccess {
    pub property: Symbol,
    pub direction: crate::SortDirection,
    pub prefix: Option<BoundValue>,
    pub lower: Option<(BoundValue, bool)>,
    pub upper: Option<(BoundValue, bool)>,
    pub required: usize,
    pub items: Vec<PhysicalSort>,
    pub residual: Option<PhysicalExpression>,
}
