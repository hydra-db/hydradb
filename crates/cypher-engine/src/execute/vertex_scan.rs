//! Execution policy for prepared SlateDB vertex scans, including guarded
//! fallbacks, equality probes and ordered-page hydration.

use super::*;
use crate::plan::CountOnlyRangeSeek;
use crate::vertex_scan::{GuardedAccess, OrderedVertexStorageAccess, PreparedVertexScan, VertexAccess};
use crate::{
    AccessPathKind, OrderedPropertyScanPage, OrderedPropertyScanRequest, OrderedScanPosition, PropertyEqualityCandidate, PropertyPointSet,
    StatisticsAccessReport, VertexStorageScanPage, VertexStorageScanRequest,
};

pub(super) fn execute<'a>(
    read: &'a mut dyn GraphRead,
    scan: &'a PreparedVertexScan,
    row_limit: Option<usize>,
    seed: Option<&'a BindingRow>,
) -> Pin<Box<dyn Future<Output = EngineResult<Vec<BindingRow>>> + Send + 'a>> {
    Box::pin(async move {
        if row_limit == Some(0) {
            return Ok(Vec::new());
        }
        read.checkpoint("cypher_vertex_scan")?;
        let binding = scan.binding();
        let labels = scan.labels();
        match &scan.plan {
            VertexAccess::IdPoint { value } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let id = vertex_id_value(value)?;
                let rows = scan_vertex_point_rows(read, binding, labels, vec![id], row_limit).await?;
                merge_seed_rows(rows, seed)
            }
            VertexAccess::IdPoints { values } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let ids = values
                    .iter()
                    .map(vertex_id_value)
                    .collect::<EngineResult<BTreeSet<_>>>()?
                    .into_iter()
                    .collect();
                let rows = scan_vertex_point_rows(read, binding, labels, ids, row_limit).await?;
                merge_seed_rows(rows, seed)
            }
            VertexAccess::PropertyPoints { property, points } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let points = match points {
                    PropertyPointSet::Single(value) => PropertyPointSet::Single(seek_value(value)?.clone()),
                    PropertyPointSet::Multiple(values) => PropertyPointSet::Multiple(
                        values
                            .iter()
                            .map(seek_value)
                            .collect::<EngineResult<BTreeSet<_>>>()?
                            .into_iter()
                            .cloned()
                            .collect::<Vec<_>>(),
                    ),
                };
                let ids = scan_vertex_ids(
                    read,
                    VertexStorageScanRequest::PropertyPoints {
                        property: property.to_string(),
                        points,
                    },
                )
                .await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            VertexAccess::PropertyPresence { property } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                let ids = scan_vertex_ids(
                    read,
                    VertexStorageScanRequest::PropertyPresence {
                        property: property.to_string(),
                    },
                )
                .await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            VertexAccess::OrderedProperty(ordered) => {
                let OrderedVertexStorageAccess {
                    property,
                    direction,
                    prefix,
                    lower,
                    upper,
                    required,
                    items,
                    residual,
                } = ordered.as_ref();
                // A tie key the index stores is satisfied by arrival order, so
                // the walk can stop the moment it has enough rows. Any other
                // tie key reorders within a primary value, so the walk must
                // first read that value out -- and must not let the backend
                // drop any of it.
                let ties_by_other_key = items
                    .get(1)
                    .is_some_and(|item| !matches!(&item.expression, PhysicalExpression::Identity(tie) if tie == binding));
                let mut request = OrderedPropertyScanRequest {
                    property: property.to_string(),
                    labels: labels.iter().map(ToString::to_string).collect(),
                    ascending: *direction == SortDirection::Ascending,
                    // With no explicit tie key preserve the native scan's
                    // stable ID order, including descending primary scans.
                    id_ascending: items.get(1).map_or(*direction == SortDirection::Ascending, |item| {
                        item.direction == SortDirection::Ascending
                    }),
                    prefix: prefix.as_ref().map(|value| bound_string(value, "STARTS WITH")).transpose()?,
                    lower: lower
                        .as_ref()
                        .map(|(value, inclusive)| {
                            Ok::<_, EngineError>(PropertyBound {
                                value: seek_value(value)?.clone(),
                                inclusive: *inclusive,
                            })
                        })
                        .transpose()?,
                    upper: upper
                        .as_ref()
                        .map(|(value, inclusive)| {
                            Ok::<_, EngineError>(PropertyBound {
                                value: seek_value(value)?.clone(),
                                inclusive: *inclusive,
                            })
                        })
                        .transpose()?,
                    // The enclosing window fixed `required`; an outer limit can
                    // only make it smaller.
                    required: row_limit.map_or(*required, |limit| limit.min(*required)),
                    after: None,
                    // Both reasons the backend's own top-of-page selection
                    // would be wrong for this operator: it cannot see the
                    // residual that may reject everything it kept, and it
                    // orders ties by vertex ID rather than by the tie key
                    // this walk was allowed to carry.
                    index_order: residual.is_some() || ties_by_other_key,
                    allowed: None,
                    max_index_entries: None,
                };
                // Whether this walk has to read a primary value's whole run
                // before it can rank within it. An `index_order` page arrives
                // in the index's own order, which ranks ties by vertex ID in
                // the scan's direction; any other tie order -- another key, or
                // IDs the other way round -- needs the run read out first.
                // Retention stays bounded regardless: the walk keeps only the
                // best `target` survivors as it goes.
                let drains_ties = request.index_order && (ties_by_other_key || request.id_ascending != request.ascending);
                // A correlated seed already holds the vertex: the removed
                // Filter must still be honored, so check it here instead of
                // trusting the label-only seed shortcut.
                if seed.is_some_and(|seed| seed.is_null(binding)) {
                    return Ok(Vec::new());
                }
                if let Some(vertex) = seed.and_then(|seed| seed.vertices.get(binding)) {
                    let accepted = labels.iter().all(|label| vertex.labels.contains(label.as_str()))
                        && matches!(
                            vertex.properties.get(request.property.as_str()),
                            Some(value) if request.accepts(value)
                        )
                        && residual_accepts(residual.as_ref(), binding, vertex)?;
                    return Ok(accepted.then(|| seed.cloned()).flatten().into_iter().collect());
                }
                if request.required == 0 {
                    return Ok(Vec::new());
                }
                // A residual rejects rows the index happily returned, and a
                // rejected row does not count against `required`. So one page
                // can come back with too few survivors, and stopping there
                // would answer with fewer rows than the query asked for --
                // quietly, since nothing about it is an error. Keep asking.
                let target = request.required;
                let mut rows: Vec<BindingRow> = Vec::new();
                // While draining, `rows` is kept ranked by `items`, and these
                // are its sort keys, index for index.
                let mut ranked_keys: Vec<Vec<EvaluatedValue>> = Vec::new();
                let mut page = request.required;
                let mut examined = 0usize;
                let mut probed = false;
                let mut seed_walk: Option<SeedWalk> = None;
                loop {
                    request.required = page;
                    // A seed walk's pages are bounded by what it has left, and
                    // the backend leaves their entries off the query's
                    // candidate tally: this walk is bounded by its own
                    // allowance and gives way to the seed past it, rather
                    // than failing on a tally it would otherwise exhaust.
                    request.max_index_entries = seed_walk.as_ref().map(|walk| walk.remaining.max(1));
                    let scanned = scan_ordered_vertices(read, request.clone()).await?;
                    let exhausted = scanned.exhausted;
                    request.after = scanned.last.clone();
                    let produced = scanned.vertices.len();
                    if let Some(walk) = seed_walk.as_mut() {
                        // A backend that does not count its entries is charged
                        // the whole page it was allowed, so the walk still ends.
                        let read_entries = scanned
                            .index_entries
                            .map_or(walk.remaining, |entries| usize::try_from(entries).unwrap_or(usize::MAX));
                        walk.remaining = walk.remaining.saturating_sub(read_entries.max(1));
                    }
                    let kept_before = rows.len();
                    for vertex in scanned.vertices {
                        // Rows arrive in rank order unless the walk is
                        // draining, so once the window is full nothing later
                        // in the page can enter it.
                        if !drains_ties && rows.len() >= target {
                            break;
                        }
                        read.checkpoint("cypher_ordered_scan_vertex")?;
                        // Defensive re-check: a backend that returns a superset is
                        // still correct, one that returns a wrong row is not.
                        if !labels.iter().all(|label| vertex.labels.contains(label.as_str()))
                            || !matches!(
                                vertex.properties.get(request.property.as_str()),
                                Some(value) if request.accepts(value)
                            )
                        {
                            continue;
                        }
                        // The scan replaced a Filter, so this is what keeps the
                        // result correct. A rejected row is not a row: it must not
                        // count against `required`, which is why the walk has to
                        // keep reading rather than stop at a full page.
                        if !residual_accepts(residual.as_ref(), binding, &vertex)? {
                            continue;
                        }
                        let row = BindingRow {
                            vertices: BTreeMap::from([(binding.clone(), vertex)]),
                            relationships: BTreeMap::new(),
                            nulls: BTreeSet::new(),
                            values: BTreeMap::new(),
                            relationship_lists: BTreeMap::new(),
                            deferred: BTreeMap::new(),
                            vertex_records: BTreeMap::new(),
                            conditions: BTreeMap::new(),
                        };
                        // Draining a run ranks within it, so only the best
                        // `target` survivors can matter. Each one either takes
                        // its place among them or is dropped on arrival: the
                        // run can be the whole index, and even one row past the
                        // window would fail a window sized at the row budget.
                        if drains_ties {
                            insert_ranked(&mut rows, &mut ranked_keys, row, items, target)?;
                        } else {
                            rows.push(row);
                        }
                        read.check_intermediate_rows("cypher_ordered_scan_intermediate_rows", rows.len())?;
                    }
                    examined = examined.saturating_add(produced);
                    // A bounded page of a seed walk can end with nothing in
                    // the seed and the index not spent, so only the index
                    // running out ends that walk here.
                    if exhausted || (produced == 0 && seed_walk.is_none()) {
                        break;
                    }
                    let window_full = rows.len() >= target;
                    if window_full
                        && (!drains_ties || tie_boundary_passed(&rows, binding, request.property.as_str(), target, request.after.as_ref()))
                    {
                        break;
                    }
                    // The seed walk spent its allowance before the window
                    // settled -- short of it, or full with its boundary tie
                    // still being read out. The seed's members are spread thin
                    // across what is left of the index, so sorting the seed
                    // itself is now the cheaper way to the answer. It is
                    // complete on its own, as the small-seed answer below is.
                    if let (Some(walk), Some(residual)) = (seed_walk.as_ref(), residual.as_ref()) {
                        if walk.remaining == 0 {
                            read.checkpoint("cypher_ordered_scan_seed_sort")?;
                            let answer = ordered_seed_rows(read, binding, labels, residual, &request, &walk.ids, items, target).await?;
                            return merge_seed_rows(answer, seed);
                        }
                    }
                    if window_full {
                        // Only rows still sharing the boundary value can change
                        // the answer now, and on a distinct property there are
                        // none. Ask for the smallest page rather than letting
                        // the miss-rate growth below widen one.
                        page = target;
                        continue;
                    }
                    // The walk has read far past what it was asked for and is
                    // still short, so the residual rather than the window is
                    // deciding this query. One of its equalities may name only
                    // a handful of vertices, and seeking those beats walking
                    // whatever is left. Ask the storage probe, which answers
                    // `None` once every equality overflows its cap -- the same
                    // evidence that no seek is narrow enough to be worth
                    // preferring. That answer cannot change as the walk goes
                    // on, so ask once.
                    if !probed && examined >= target.saturating_mul(ORDERED_PROBE_TRIGGER) {
                        probed = true;
                        if let Some(residual) = residual.as_ref() {
                            let candidates = equality_candidates(residual, binding);
                            if !candidates.is_empty() {
                                read.checkpoint("cypher_ordered_scan_probe")?;
                                if let Some(ids) = read.probe_selective_equality(&candidates).await? {
                                    // A small seed is cheapest loaded whole and
                                    // sorted. A large one is not: keep walking
                                    // in order and let the backend skip every
                                    // entry outside it without loading it --
                                    // for a bounded number of entries, since
                                    // a seed whose newest members sit behind
                                    // most of the index would otherwise be
                                    // walked past entry by entry.
                                    if ids.len() > ORDERED_SEED_HYDRATE_MAX {
                                        let mut ids = ids;
                                        ids.sort_unstable();
                                        ids.dedup();
                                        let ids = std::sync::Arc::new(ids);
                                        seed_walk = Some(SeedWalk {
                                            remaining: ids.len().saturating_mul(ORDERED_SEED_WALK_FACTOR),
                                            ids: std::sync::Arc::clone(&ids),
                                        });
                                        request.allowed = Some(ids);
                                    } else {
                                        // The probe answers for a whole conjunct,
                                        // so its vertices are a superset of every
                                        // row the residual can accept, including
                                        // the ones already collected. Answering
                                        // from it alone is therefore complete, and
                                        // simpler than reconciling two partial
                                        // walks of the same population.
                                        let mut ids = ids;
                                        ids.sort_unstable();
                                        ids.dedup();
                                        let answer =
                                            ordered_seed_rows(read, binding, labels, residual, &request, &ids, items, target).await?;
                                        return merge_seed_rows(answer, seed);
                                    }
                                }
                            }
                        }
                    }
                    // A window the residual barely matches would otherwise cost
                    // a round trip per surviving row. Grow with the miss rate,
                    // and cap it so a window it never matches cannot ask for
                    // the whole index in one page.
                    let kept = rows.len() - kept_before;
                    page = if kept == 0 {
                        page.saturating_mul(ORDERED_PAGE_GROWTH)
                    } else {
                        page.saturating_mul(produced).saturating_div(kept.max(1))
                    }
                    .clamp(target, target.saturating_mul(ORDERED_PAGE_CEILING));
                }
                let mut rows = sort_rows(rows, items)?;
                truncate_to_limit(&mut rows, Some(target));
                merge_seed_rows(rows, seed)
            }
            VertexAccess::LabelPrefix { label } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                // A limit cannot stop the anchor scan early when additional
                // labels still need in-memory validation: the first N anchor
                // rows may all fail the remaining labels.
                let scan_limit = (labels.len() == 1).then_some(row_limit).flatten();
                let ids = scan_vertex_ids(
                    read,
                    VertexStorageScanRequest::LabelPrefix {
                        label: label.to_string(),
                        max_results: scan_limit,
                    },
                )
                .await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            VertexAccess::AllVertices => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                read.record_full_scan();
                let ids = scan_vertex_ids(read, VertexStorageScanRequest::AllVertices).await?;
                let rows = hydrate_rows(read, binding, labels, ids, row_limit, None).await?;
                merge_seed_rows(rows, seed)
            }
            VertexAccess::Guarded { fallback, .. } | VertexAccess::Range { fallback, .. } => {
                if let Some(row) = bound_seed_row(seed, binding, labels) {
                    return Ok(row);
                }
                // Correlated runs retain the unranked path: a guard estimates
                // one population, not repeated work for every outer row.
                if seed.is_some() {
                    return execute(read, fallback, row_limit, seed).await;
                }
                match statistics_access_rows(read, scan).await? {
                    Some(mut rows) => {
                        truncate_to_limit(&mut rows, row_limit);
                        Ok(rows)
                    }
                    None => execute(read, fallback, row_limit, None).await,
                }
            }
        }
    })
}

pub(super) enum FilterInput {
    Candidates(Vec<BindingRow>),
    PredicateEvaluated(Vec<BindingRow>),
}

impl FilterInput {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Candidates(rows) | Self::PredicateEvaluated(rows) => rows.len(),
        }
    }

    pub(super) fn filter(
        self,
        read: &mut dyn GraphRead,
        predicate: &PhysicalExpression,
        row_limit: Option<usize>,
    ) -> EngineResult<Vec<BindingRow>> {
        match self {
            Self::Candidates(rows) => filter_rows(read, rows, predicate, row_limit),
            Self::PredicateEvaluated(rows) => Ok(rows),
        }
    }
}

pub(super) async fn execute_for_filter(
    read: &mut dyn GraphRead,
    scan: &PreparedVertexScan,
    predicate: &PhysicalExpression,
    row_limit: Option<usize>,
    seed: Option<&BindingRow>,
) -> EngineResult<FilterInput> {
    let mut access = scan;
    if seed.is_none() {
        // Successful statistics-chosen access skips the speculative equality
        // probe. A declined guard uses the same probe as an unranked plan.
        while let VertexAccess::Guarded { fallback, .. } | VertexAccess::Range { fallback, .. } = &access.plan {
            match statistics_access_rows(read, access).await? {
                Some(rows) => return Ok(FilterInput::Candidates(rows)),
                None => access = fallback,
            }
        }
        if matches!(
            &access.plan,
            VertexAccess::PropertyPoints { .. }
                | VertexAccess::PropertyPresence { .. }
                | VertexAccess::LabelPrefix { .. }
                | VertexAccess::AllVertices
        ) {
            let candidates = equality_candidates(predicate, access.binding());
            if distinct_properties(&candidates) >= 2 {
                read.checkpoint("cypher_equality_probe")?;
                if let Some(ids) = read.probe_selective_equality(&candidates).await? {
                    return hydrate_rows(read, access.binding(), access.labels(), ids, row_limit, Some(predicate))
                        .await
                        .map(FilterInput::PredicateEvaluated);
                }
            }
        }
    }
    let rows = execute(read, access, None, seed).await?;
    Ok(FilterInput::Candidates(rows))
}

async fn statistics_access_rows(read: &mut dyn GraphRead, scan: &PreparedVertexScan) -> EngineResult<Option<Vec<BindingRow>>> {
    match &scan.plan {
        VertexAccess::Range {
            property,
            lower,
            upper,
            estimated_rows,
            read_limit,
            ..
        } => {
            range_seek_rows(
                read,
                RangeSeek {
                    binding: scan.binding(),
                    labels: scan.labels(),
                    property,
                    lower: lower.as_ref(),
                    upper: upper.as_ref(),
                    estimated_rows: *estimated_rows,
                    read_limit: *read_limit,
                    index_only: scan.index_only(),
                },
            )
            .await
        }
        VertexAccess::Guarded {
            access,
            estimated_rows,
            read_limit,
            ..
        } => guarded_rows(read, scan.binding(), scan.labels(), access, *estimated_rows, *read_limit).await,
        _ => Ok(None),
    }
}

pub(super) async fn scan_vertex_ids(read: &mut dyn GraphRead, request: VertexStorageScanRequest) -> EngineResult<Vec<VertexId>> {
    match read.scan_vertex_storage(&request).await? {
        VertexStorageScanPage::Ids(ids) => Ok(ids),
        VertexStorageScanPage::Records(records) => Ok(records.into_iter().map(|record| record.id).collect()),
        VertexStorageScanPage::Ordered(_) => Err(EngineError::InvalidStorageResponse(
            "ordinary vertex storage scan returned an ordered page".to_string(),
        )),
    }
}

async fn scan_vertex_point_rows(
    read: &mut dyn GraphRead,
    binding: &Symbol,
    labels: &[Symbol],
    ids: Vec<VertexId>,
    row_limit: Option<usize>,
) -> EngineResult<Vec<BindingRow>> {
    match read.scan_vertex_storage(&VertexStorageScanRequest::IdPoints { ids }).await? {
        VertexStorageScanPage::Ids(ids) => hydrate_rows(read, binding, labels, ids, row_limit, None).await,
        VertexStorageScanPage::Records(mut records) => {
            records.sort_by_key(|record| record.id);
            records.dedup_by_key(|record| record.id);
            let mut rows = Vec::new();
            for record in records {
                read.checkpoint("cypher_point_scan_vertex")?;
                if !labels.iter().all(|label| record.labels.contains(label.as_str())) {
                    continue;
                }
                let mut row = BindingRow::default();
                row.vertices.insert(binding.clone(), record);
                rows.push(row);
                read.check_intermediate_rows("cypher_point_scan_intermediate_rows", rows.len())?;
                if row_limit.is_some_and(|limit| rows.len() >= limit) {
                    break;
                }
            }
            Ok(rows)
        }
        VertexStorageScanPage::Ordered(_) => Err(EngineError::InvalidStorageResponse(
            "vertex point scan returned an ordered page".to_string(),
        )),
    }
}

pub(super) async fn scan_ordered_vertices(
    read: &mut dyn GraphRead,
    request: OrderedPropertyScanRequest,
) -> EngineResult<OrderedPropertyScanPage> {
    match read
        .scan_vertex_storage(&VertexStorageScanRequest::OrderedProperty(Box::new(request)))
        .await?
    {
        VertexStorageScanPage::Ordered(page) => Ok(page),
        VertexStorageScanPage::Ids(_) | VertexStorageScanPage::Records(_) => Err(EngineError::InvalidStorageResponse(
            "ordered vertex storage scan returned vertex IDs or records".to_string(),
        )),
    }
}

/// Entries one page of a range seek may read. Small enough that a decline is
/// noticed soon after the limit, large enough that a narrow range is one page.
const RANGE_SEEK_PAGE_ENTRIES: usize = 512;

/// Bounds and budgets of a prepared range access.
struct RangeSeek<'a> {
    binding: &'a Symbol,
    labels: &'a [Symbol],
    property: &'a Symbol,
    lower: Option<&'a (BoundValue, bool)>,
    upper: Option<&'a (BoundValue, bool)>,
    estimated_rows: u64,
    read_limit: usize,
    index_only: bool,
}

/// Read every vertex of `range.property` between the bounds that carries the
/// labels, or `None` once more than `read_limit` index entries have been read
/// without reaching the end of the range.
///
/// Rows are a candidate superset of what the enclosing Filter accepts: only
/// labels and the bounds are checked here, and the Filter applies the whole
/// predicate. They come back in vertex ID order, the order every other
/// vertex access hydrates in, so the access path cannot reorder results.
///
/// An `index_only` seek binds a record holding exactly what the index entry
/// and the walk established: the id, the seek's labels and the seek's
/// property. The flag is set on the prepared query only after proving nothing
/// in the plan reads more of the binding, so no operator can tell the
/// difference, and the Filter's residual on the property is exact: the index
/// value is the stored value.
async fn range_seek_rows(read: &mut dyn GraphRead, range: RangeSeek<'_>) -> EngineResult<Option<Vec<BindingRow>>> {
    let bound = |bound: Option<&(BoundValue, bool)>| {
        bound
            .map(|(value, inclusive)| {
                Ok::<_, EngineError>(PropertyBound {
                    value: seek_value(value)?.clone(),
                    inclusive: *inclusive,
                })
            })
            .transpose()
    };
    let mut request = OrderedPropertyScanRequest {
        property: range.property.to_string(),
        labels: range.labels.iter().map(ToString::to_string).collect(),
        ascending: true,
        id_ascending: true,
        prefix: None,
        lower: bound(range.lower)?,
        upper: bound(range.upper)?,
        required: RANGE_SEEK_PAGE_ENTRIES,
        after: None,
        // Every entry is wanted, so the backend must not rank within a page.
        index_order: true,
        allowed: None,
        max_index_entries: None,
    };
    let limit = capped_read_limit(&*read, range.read_limit) as u64;
    let mut entries_read = 0_u64;
    let mut vertices: Vec<VertexRecord> = Vec::new();
    let declined = loop {
        // Under a cap of zero or one the seek may keep no entry, and even the
        // first would fail the row check before the guard declined, so it
        // declines before reading.
        if limit == 0 {
            break true;
        }
        // One entry past the limit is what proves the range is wider than it.
        let remaining = limit.saturating_add(1).saturating_sub(entries_read);
        let page = usize::try_from(remaining).unwrap_or(usize::MAX).clamp(1, RANGE_SEEK_PAGE_ENTRIES);
        request.required = page;
        request.max_index_entries = Some(page);
        let scanned = if range.index_only {
            let page = read.scan_vertex_entries_by_property_ordered(&request).await?;
            OrderedPropertyScanPage {
                vertices: page
                    .entries
                    .into_iter()
                    .map(|(id, value)| VertexRecord {
                        id,
                        labels: range.labels.iter().map(ToString::to_string).collect(),
                        properties: BTreeMap::from([(request.property.clone(), value)]),
                    })
                    .collect(),
                last: page.last,
                exhausted: page.exhausted,
                index_entries: page.index_entries,
            }
        } else {
            read.scan_vertices_by_property_ordered(&request).await?
        };
        let page_entries = scanned.index_entries.unwrap_or(scanned.vertices.len() as u64);
        entries_read = entries_read.saturating_add(page_entries);
        for vertex in scanned.vertices {
            read.checkpoint("cypher_range_seek_vertex")?;
            // Defensive re-check, as for the ordered walk: a backend that
            // returns a superset is still correct, one that returns a wrong
            // row is not.
            if !range.labels.iter().all(|label| vertex.labels.contains(label.as_str()))
                || !matches!(
                    vertex.properties.get(request.property.as_str()),
                    Some(value) if request.accepts(value)
                )
            {
                continue;
            }
            vertices.push(vertex);
            read.check_intermediate_rows("cypher_range_seek_intermediate_rows", vertices.len())?;
        }
        if scanned.exhausted {
            break false;
        }
        if entries_read > limit {
            break true;
        }
        // A page that neither read anything nor moved the cursor would be
        // asked for again forever. Declining is always safe.
        if page_entries == 0 || scanned.last.is_none() {
            break true;
        }
        request.after = scanned.last;
    };
    read.record_statistics_access(&StatisticsAccessReport {
        kind: AccessPathKind::Range,
        property: Some(range.property.to_string()),
        estimated_rows: range.estimated_rows,
        entries_read,
        declined,
    });
    if declined {
        return Ok(None);
    }
    vertices.sort_by_key(|vertex| vertex.id);
    vertices.dedup_by_key(|vertex| vertex.id);
    Ok(Some(
        vertices
            .into_iter()
            .map(|vertex| BindingRow {
                vertices: BTreeMap::from([(range.binding.clone(), vertex)]),
                relationships: BTreeMap::new(),
                nulls: BTreeSet::new(),
                values: BTreeMap::new(),
                relationship_lists: BTreeMap::new(),
                deferred: BTreeMap::new(),
                vertex_records: BTreeMap::new(),
                conditions: BTreeMap::new(),
            })
            .collect(),
    ))
}

/// Entries one page of a counting walk may read. The walk holds one page at
/// a time, so this trades that page's memory against one seek per page.
const COUNT_PAGE_ENTRIES: usize = 4_096;

/// How many rows of an index-only range seek its Filter accepts, read a page
/// at a time with no row kept: the count-only form of [`range_seek_rows`].
/// `None` when the backend has no counting walk, and the caller then runs the
/// plan as it stands.
///
/// The seek's rows bind a record holding the id, the labels and the
/// property's value, and the index-only marking proved nothing reads more,
/// so the predicate sees here what it would see over those rows; one row is
/// reused for every entry. The backend charges what the walk reads to its
/// scan allowance, which bounds a count over any number of entries. The read
/// limit and candidate cap that bound a seek keeping its rows do not apply,
/// and nor does the fallback such a seek declines to past them.
///
/// The index holds one entry per vertex and property, so unlike the seek
/// this does not dedupe by id: counting entries counts vertices.
pub(super) async fn count_range_seek(read: &mut dyn GraphRead, count: &CountOnlyRangeSeek<'_>) -> EngineResult<Option<u64>> {
    let VertexAccess::Range {
        property,
        lower,
        upper,
        estimated_rows,
        ..
    } = &count.prepared.plan
    else {
        return Ok(None);
    };
    let bound = |bound: Option<&(BoundValue, bool)>| {
        bound
            .map(|(value, inclusive)| {
                Ok::<_, EngineError>(PropertyBound {
                    value: seek_value(value)?.clone(),
                    inclusive: *inclusive,
                })
            })
            .transpose()
    };
    let binding = count.prepared.binding();
    let labels = count.prepared.labels();
    let mut request = OrderedPropertyScanRequest {
        property: property.to_string(),
        labels: labels.iter().map(ToString::to_string).collect(),
        ascending: true,
        id_ascending: true,
        prefix: None,
        lower: bound(lower.as_ref())?,
        upper: bound(upper.as_ref())?,
        required: COUNT_PAGE_ENTRIES,
        after: None,
        // Every entry is wanted, so the backend must not rank within a page.
        index_order: true,
        allowed: None,
        max_index_entries: Some(COUNT_PAGE_ENTRIES),
    };
    // Bracketed before the first page, so every page's reads are charged to
    // the scan. A backend without the walk closes both scopes having read
    // nothing, and the plan that runs instead opens its own.
    read.operator_started(count.filter);
    read.operator_started(count.scan);
    let mut row = BindingRow::default();
    row.vertices.insert(
        binding.clone(),
        VertexRecord {
            id: 0,
            labels: labels.iter().map(ToString::to_string).collect(),
            properties: BTreeMap::new(),
        },
    );
    let mut entries_read = 0_u64;
    let mut walked = 0_usize;
    let mut counted = 0_u64;
    let result = loop {
        let page = match read.scan_vertex_entries_by_property_for_count(&request).await {
            Ok(Some(page)) => page,
            Ok(None) if request.after.is_none() => break Ok(None),
            Ok(None) => {
                break Err(EngineError::InvalidStorageResponse(format!(
                    "counting walk over {} was refused part way",
                    request.property
                )))
            }
            Err(error) => break Err(error.into()),
        };
        let page_entries = page.index_entries.unwrap_or(page.entries.len() as u64);
        entries_read = entries_read.saturating_add(page_entries);
        walked = walked.saturating_add(page.entries.len());
        let accepted = page.entries.into_iter().try_fold(0_u64, |accepted, (id, value)| {
            read.checkpoint("cypher_range_count_entry")?;
            // Defensive, as for the seek: a backend may return a superset.
            if !request.accepts(&value) {
                return Ok::<_, EngineError>(accepted);
            }
            let vertex = row.vertices.get_mut(binding).expect("the counted binding is bound");
            vertex.id = id;
            vertex.properties.insert(request.property.clone(), value);
            let holds = predicate_holds(
                evaluate_expression(count.predicate, &row)?,
                "Filter predicate did not evaluate to a boolean",
            )?;
            Ok(accepted + u64::from(holds))
        });
        match accepted {
            Ok(accepted) => counted = counted.saturating_add(accepted),
            Err(error) => break Err(error),
        }
        if page.exhausted {
            break Ok(Some(counted));
        }
        // A page that neither read anything nor moved the cursor would be
        // asked for again forever.
        if page_entries == 0 || page.last.is_none() {
            break Err(EngineError::InvalidStorageResponse(format!(
                "counting walk over {} stopped without reaching the end of the range",
                request.property
            )));
        }
        request.after = page.last;
    };
    read.operator_finished(count.scan, walked);
    read.operator_finished(
        count.filter,
        match &result {
            Ok(Some(counted)) => usize::try_from(*counted).unwrap_or(usize::MAX),
            _ => 0,
        },
    );
    let Some(counted) = result.map_err(|error| error.in_operation(count.filter.operation()))? else {
        return Ok(None);
    };
    read.record_statistics_access(&StatisticsAccessReport {
        kind: AccessPathKind::Range,
        property: Some(property.to_string()),
        estimated_rows: *estimated_rows,
        entries_read,
        declined: false,
    });
    Ok(Some(counted))
}

/// Read a statistics-chosen equality seek or label scan, or `None` once it
/// holds more than `read_limit` index entries. The backend may decline
/// sooner, at a ceiling of its own; `None` is always safe.
///
/// Rows are a candidate superset, as for the range seek: labels are checked
/// here and the enclosing Filter applies the whole predicate.
/// A statistics-chosen access's read limit, kept under the query's index
/// candidate and intermediate row caps.
///
/// The access is an optional shortcut: reaching a cap proves only that it was
/// a poor choice, so it must decline to its fallback rather than fail a query
/// the fallback may answer. Its reads are not charged to the shared candidate
/// tally, and every row it keeps came from one entry it read, so holding its
/// entries under both caps holds its rows under them too. It reads one entry
/// past its limit to prove the access is wider, hence the `- 1`.
fn capped_read_limit(read: &dyn GraphRead, read_limit: usize) -> usize {
    [read.max_query_index_candidates(), read.max_intermediate_rows()]
        .into_iter()
        .flatten()
        .fold(read_limit, |limit, cap| limit.min(cap.saturating_sub(1)))
}

async fn guarded_rows(
    read: &mut dyn GraphRead,
    binding: &Symbol,
    labels: &[Symbol],
    access: &GuardedAccess,
    estimated_rows: u64,
    read_limit: usize,
) -> EngineResult<Option<Vec<BindingRow>>> {
    let read_limit = capped_read_limit(&*read, read_limit);
    let (kind, property, bounded) = match access {
        GuardedAccess::PropertySeek { property, values } => {
            let unique_values = values
                .iter()
                .map(seek_value)
                .collect::<EngineResult<BTreeSet<_>>>()?
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let bounded = read
                .seek_vertices_by_property_values_bounded(property.as_str(), &unique_values, read_limit)
                .await?;
            (AccessPathKind::Equality, Some(property.to_string()), bounded)
        }
        GuardedAccess::LabelScan { label } => {
            let bounded = read.scan_vertices_by_label_bounded(label.as_str(), read_limit).await?;
            (AccessPathKind::LabelScan, None, bounded)
        }
    };
    read.record_statistics_access(&StatisticsAccessReport {
        kind,
        property,
        estimated_rows,
        entries_read: bounded.index_entries,
        declined: bounded.ids.is_none(),
    });
    let Some(ids) = bounded.ids else {
        return Ok(None);
    };
    hydrate_rows(read, binding, labels, ids, None, None).await.map(Some)
}

/// The equality restrictions on `binding` that are conjuncts of `predicate`.
///
/// Only conjuncts qualify. The vertices satisfying one conjunct are a superset
/// of the rows the whole predicate accepts, which is what makes a probe result
/// usable as the complete candidate set. One side of a disjunct is not, unless
/// both sides restrict the same property, in which case their union still is.
fn equality_candidates(predicate: &PhysicalExpression, binding: &Symbol) -> Vec<PropertyEqualityCandidate> {
    let mut candidates = Vec::new();
    collect_equality_candidates(predicate, binding, &mut candidates);
    candidates.dedup();
    candidates
}

fn collect_equality_candidates(predicate: &PhysicalExpression, binding: &Symbol, candidates: &mut Vec<PropertyEqualityCandidate>) {
    if let PhysicalExpression::Binary {
        left,
        operator: PhysicalBinaryOperator::And,
        right,
    } = predicate
    {
        collect_equality_candidates(left, binding, candidates);
        collect_equality_candidates(right, binding, candidates);
        return;
    }
    if let Some(candidate) = equality_candidate(predicate, binding) {
        candidates.push(candidate);
    }
}

fn equality_candidate(predicate: &PhysicalExpression, binding: &Symbol) -> Option<PropertyEqualityCandidate> {
    match predicate {
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Equal,
            right,
        } => {
            let (property, value) =
                candidate_property_value(left, right, binding).or_else(|| candidate_property_value(right, left, binding))?;
            Some(PropertyEqualityCandidate {
                property,
                values: vec![value],
            })
        }
        PhysicalExpression::Binary {
            left,
            operator: PhysicalBinaryOperator::Or,
            right,
        } => {
            let left = equality_candidate(left, binding)?;
            let right = equality_candidate(right, binding)?;
            if left.property != right.property {
                return None;
            }
            let mut values = left.values;
            for value in right.values {
                if !values.contains(&value) {
                    values.push(value);
                }
            }
            Some(PropertyEqualityCandidate {
                property: left.property,
                values,
            })
        }
        PhysicalExpression::InList { expression, values } => {
            let property = candidate_property(expression, binding)?;
            // NULL is not a seekable key, and Cypher's three-valued IN already
            // resolves it through the predicate itself.
            let values = values
                .iter()
                .filter(|value| !matches!(value.value, ScalarValue::Null))
                .map(|value| value.value.clone())
                .collect::<Vec<_>>();
            (!values.is_empty()).then_some(PropertyEqualityCandidate { property, values })
        }
        _ => None,
    }
}

fn candidate_property_value(property: &PhysicalExpression, value: &PhysicalExpression, binding: &Symbol) -> Option<(String, ScalarValue)> {
    let property = candidate_property(property, binding)?;
    let PhysicalExpression::Value(value) = value else {
        return None;
    };
    if matches!(value.value, ScalarValue::Null) {
        return None;
    }
    Some((property, value.value.clone()))
}

fn candidate_property(expression: &PhysicalExpression, binding: &Symbol) -> Option<String> {
    let PhysicalExpression::Property {
        binding: candidate_binding,
        property,
    } = expression
    else {
        return None;
    };
    (candidate_binding == binding).then(|| property.to_string())
}

/// How many different properties the candidates restrict. One is the property
/// the planner already anchored on, so a probe can only confirm it; two or more
/// means there is a genuine choice the planner never costed.
fn distinct_properties(candidates: &[PropertyEqualityCandidate]) -> usize {
    candidates
        .iter()
        .map(|candidate| candidate.property.as_str())
        .collect::<BTreeSet<_>>()
        .len()
}

/// Evaluate a scan's residual against one vertex.
///
/// `None` accepts everything, which is the case where the index proved the
/// whole predicate and no Filter was removed.
fn residual_accepts(residual: Option<&PhysicalExpression>, binding: &Symbol, vertex: &VertexRecord) -> EngineResult<bool> {
    let Some(residual) = residual else {
        return Ok(true);
    };
    let row = BindingRow {
        vertices: BTreeMap::from([(binding.clone(), vertex.clone())]),
        relationships: BTreeMap::new(),
        nulls: BTreeSet::new(),
        values: BTreeMap::new(),
        relationship_lists: BTreeMap::new(),
        deferred: BTreeMap::new(),
        vertex_records: BTreeMap::new(),
        conditions: BTreeMap::new(),
    };
    predicate_holds(
        evaluate_expression(residual, &row)?,
        "ordered scan residual did not evaluate to a boolean",
    )
}

/// How much wider a page grows when a residual rejected every row of the last
/// one, leaving no miss rate to scale by.
const ORDERED_PAGE_GROWTH: usize = 4;

/// The most a page may exceed the rows still wanted. A residual that matches
/// almost nothing would otherwise walk the whole index in one request, and the
/// walk is supposed to be bounded work.
const ORDERED_PAGE_CEILING: usize = 64;

/// How far past its target an ordered walk reads before asking whether an
/// equality in its residual could be seeked instead.
///
/// The planner cannot tell a selective equality from a broad one, so the choice
/// between walking the sorted index and seeking an equality is settled here,
/// where both populations can be measured. This is the price of asking: low
/// enough that a query the walk cannot serve gives up after bounded work,
/// high enough that a window a residual thins by an ordinary fraction reaches
/// its target first and never pays for the probe at all.
const ORDERED_PROBE_TRIGGER: usize = 1;

/// Largest probe seed answered by loading it whole instead of filtering the
/// ordered walk with it. Matches legacy's speculative probe slice.
const ORDERED_SEED_HYDRATE_MAX: usize = 256;

/// Index entries a seed-filtered ordered walk may read per seed vertex before
/// it sorts the seed instead.
///
/// The walk finds the window quickly when the seed is dense among the entries
/// it reaches first, and the seed sort costs one vertex load per seed member
/// however the seed is spread. Reading an index entry is a key in a range
/// scan, far cheaper than a point load, so the walk is given several entries
/// per seed vertex before it gives way. Past that the seed is spread thinly
/// enough that walking on would cost more than loading it, and a walk left
/// unbounded is what reached the candidate limit for a rare type whose newest
/// members sit behind most of a scope. Together the two never read more than
/// a small multiple of the seed, which the probe has already bounded.
const ORDERED_SEED_WALK_FACTOR: usize = 8;

/// Seed vertices loaded per batch while sorting a seed, so only the window and
/// one batch are held at a time however large the seed is.
const ORDERED_SEED_SORT_BATCH: usize = 1_024;

/// An ordered walk restricted to a probe's seed, and what it may still read.
struct SeedWalk {
    ids: std::sync::Arc<Vec<VertexId>>,
    remaining: usize,
}

/// The window answered from a probe's seed: every seed vertex loaded in
/// batches, checked against the labels, the residual and the window's own
/// bounds, and the best `target` kept under the query's order.
///
/// The probe answers for a whole conjunct of the residual, so the seed is a
/// superset of every row the window can hold, and this answer is complete
/// without whatever the walk collected before it. `ids` is sorted and
/// distinct, and each batch is ranked together with the rows already kept,
/// so truncating per batch keeps exactly what one sort of every survivor
/// would.
#[allow(clippy::too_many_arguments)]
async fn ordered_seed_rows(
    read: &mut dyn GraphRead,
    binding: &Symbol,
    labels: &[Symbol],
    residual: &PhysicalExpression,
    request: &OrderedPropertyScanRequest,
    ids: &[VertexId],
    items: &[PhysicalSort],
    target: usize,
) -> EngineResult<Vec<BindingRow>> {
    let mut kept = Vec::new();
    for batch in ids.chunks(ORDERED_SEED_SORT_BATCH) {
        let hydrated = hydrate_rows(read, binding, labels, batch.to_vec(), None, Some(residual)).await?;
        // Hydration applied the labels and the residual; the window bounds
        // the scan would have enforced are still owed.
        kept.extend(hydrated.into_iter().filter(|row| {
            row.vertices.get(binding).is_some_and(|vertex| {
                matches!(
                    vertex.properties.get(request.property.as_str()),
                    Some(value) if request.accepts(value)
                )
            })
        }));
        kept = rank_seed_rows(kept, binding, items, request.ascending)?;
        truncate_to_limit(&mut kept, Some(target));
    }
    Ok(kept)
}

/// Seed rows in the order the ordered walk would have returned them: the
/// query's sort keys, then the vertex ID in the direction the walk reads.
///
/// A seed walk's pages come in `index_order`, so rows every sort key ties
/// arrive by vertex ID in the scan's own direction -- descending under a
/// descending scan, whatever direction a tie key asks for -- and the walk
/// keeps the first of them. The seed arrives in ascending ID order, and a
/// stable sort would keep that instead. Ranking by the ID in the scan's
/// direction here is what makes a window answered from the seed the one the
/// walk would have answered.
fn rank_seed_rows(rows: Vec<BindingRow>, binding: &Symbol, items: &[PhysicalSort], scan_ascending: bool) -> EngineResult<Vec<BindingRow>> {
    let mut keyed = rows
        .into_iter()
        .map(|row| {
            let keys = items
                .iter()
                .map(|item| evaluate_expression(&item.expression, &row))
                .collect::<EngineResult<Vec<_>>>()?;
            let id = row.vertices.get(binding).map(|vertex| vertex.id);
            Ok((row, keys, id))
        })
        .collect::<EngineResult<Vec<_>>>()?;
    keyed.sort_by(|(_, left, left_id), (_, right, right_id)| {
        compare_sort_keys(left, right, items).then_with(|| {
            if scan_ascending {
                left_id.cmp(right_id)
            } else {
                right_id.cmp(left_id)
            }
        })
    });
    Ok(keyed.into_iter().map(|(row, _, _)| row).collect())
}

/// Whether the walk has read past the primary value that decides the answer's
/// edge, and so has seen every row a tie key could reorder into it.
///
/// Rows arrive in primary-key order, which puts the row at `target - 1` on the
/// boundary. Rows sharing its value may sort ahead of it under a tie key the
/// index does not store, so all of them are candidates; rows past it lose on
/// the primary key, which no tie key can undo. `position` is where the index
/// left off rather than where the survivors did -- the residual rejecting a row
/// says nothing about how far the scan reached.
fn tie_boundary_passed(
    rows: &[BindingRow],
    binding: &Symbol,
    property: &str,
    target: usize,
    position: Option<&OrderedScanPosition>,
) -> bool {
    let Some(position) = position else {
        return false;
    };
    let boundary = rows
        .get(target.saturating_sub(1))
        .and_then(|row| row.vertices.get(binding))
        .and_then(|vertex| vertex.properties.get(property))
        .filter(|value| !matches!(value, ScalarValue::Null));
    // Compared under the index's own order, so a boundary of any stored type
    // is decided the same way -- and `1` and `1.0`, which compare equal, stay
    // one tie rather than two.
    boundary.is_some_and(|boundary| compare_property_values(&position.value, boundary).is_ne())
}

async fn hydrate_rows(
    read: &mut dyn GraphRead,
    binding: &Symbol,
    labels: &[Symbol],
    ids: Vec<VertexId>,
    row_limit: Option<usize>,
    residual: Option<&PhysicalExpression>,
) -> EngineResult<Vec<BindingRow>> {
    let unique_ids = ids.into_iter().collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
    if row_limit == Some(0) {
        return Ok(Vec::new());
    }

    let batch_size = row_limit.unwrap_or(unique_ids.len()).max(1);
    let mut rows = Vec::new();
    for ids in unique_ids.chunks(batch_size) {
        read.checkpoint("cypher_hydrate_rows")?;
        let mut vertices = read.hydrate_vertices(ids).await?;
        vertices.sort_by_key(|vertex| vertex.id);
        vertices.dedup_by_key(|vertex| vertex.id);
        for vertex in vertices {
            read.checkpoint("cypher_hydrate_vertex")?;
            if !labels.iter().all(|label| vertex.labels.contains(label.as_str())) {
                continue;
            }
            let row = BindingRow {
                vertices: BTreeMap::from([(binding.clone(), vertex)]),
                relationships: BTreeMap::new(),
                nulls: BTreeSet::new(),
                values: BTreeMap::new(),
                relationship_lists: BTreeMap::new(),
                deferred: BTreeMap::new(),
                vertex_records: BTreeMap::new(),
                conditions: BTreeMap::new(),
            };
            // A residual is only present when the caller removed the Filter
            // that would otherwise have applied it, so rejecting here is the
            // only thing keeping the result correct.
            if let Some(residual) = residual {
                if !predicate_holds(
                    evaluate_expression(residual, &row)?,
                    "Filter predicate did not evaluate to a boolean",
                )? {
                    continue;
                }
            }
            rows.push(row);
            read.check_intermediate_rows("cypher_hydrate_intermediate_rows", rows.len())?;
            if row_limit.is_some_and(|limit| rows.len() >= limit) {
                return Ok(rows);
            }
        }
    }
    Ok(rows)
}
