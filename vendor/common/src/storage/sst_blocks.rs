//! Precise key-range record counts over a SlateDB manifest.
//!
//! Built on SlateDB RFC 0020 primitives — [`slatedb::Db::manifest`],
//! [`slatedb::SstReader`], [`slatedb::SstFile::stats`],
//! [`slatedb::SstFile::index`] — plus [`slatedb::SstFile::read_block`] for
//! per-row precision on boundary blocks.
//!
//! The primary entry point is [`count_in_range`], which walks the manifest
//! and returns the exact number of physical write operations (puts,
//! deletes, merges) recorded within a query range.
//!
//! ## Walk structure
//!
//! For each SST overlapping the query:
//!
//! 1. **Back-scan** — walk blocks high-to-low, reading each, until we find
//!    one with actual in-query rows. That "witness" block fixes
//!    [`CountResult::covered_to`] from the highest in-query row key. If the
//!    witness is the SST's last data block AND fully contained in the
//!    query, [`slatedb::SstFile::info`]'s `last_entry` is a free witness
//!    (no row read needed). Blocks scanned above the witness contributed
//!    nothing in query, so we just keep walking.
//! 2. **Forward fill** — for blocks below the witness, use the cheap stats
//!    path when the block is fully contained in the query (no per-row I/O),
//!    and read rows when the block straddles `query.start` (to filter out
//!    below-query keys).
//!
//! ## Why the back-scan
//!
//! SlateDB's index stores separators, not first keys. A separator is the
//! shortest prefix of `first_key(block i)` that is still strictly greater
//! than `last_key(block i-1)` — so `sep[i] <= first_key(block i)`, often
//! strict (e.g. for blocks ending at `k230` and starting at `k280`, the
//! separator is `k28`). A block's separator can therefore place it
//! "inside" the query while every real key in the block sits above
//! `query.end`. Picking the highest *overlapping-by-separator* block as
//! the witness can yield a block with zero in-query rows. The back-scan
//! tolerates this by continuing downward until a block actually
//! contributes.
//!
//! ## Known gap
//!
//! `SsTableView::visible_range` projection is not applied. Callers using
//! view-projected SSTs may over-count rows outside the visible range.
//! Fixable with the same `read_block` primitive (clip rows to
//! `visible_range ∩ query`) — left as follow-up.
//!
//! ## Count semantics
//!
//! Counts are **physical write operations**, not visible rows. A key
//! written twice contributes 2 to `num_puts`; a tombstone counts as a
//! delete even if its put has compacted away. This matches the LogDb
//! append-only model where physical ops equal logical records. If an
//! update or delete API is added to LogDb, revisit.

use std::collections::VecDeque;
use std::ops::Bound;

use bytes::Bytes;
use slatedb::manifest::{SortedRun, SsTableView, VersionedManifest};
use slatedb::{RowEntry, SstReader, SstStats, ValueDeletable};

use crate::{BytesRange, StorageError, StorageResult};

/// Counts of physical write operations within a query range.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockOpCounts {
    pub num_puts: u64,
    pub num_deletes: u64,
    pub num_merges: u64,
}

impl BlockOpCounts {
    pub fn num_rows(&self) -> u64 {
        self.num_puts + self.num_deletes + self.num_merges
    }

    pub fn add(&mut self, other: BlockOpCounts) {
        self.num_puts += other.num_puts;
        self.num_deletes += other.num_deletes;
        self.num_merges += other.num_merges;
    }
}

/// Distribution of a key's in-query records across the **L0 tier** of the
/// walked tree(s). L0 SSTs are not range-partitioned, so a read must consult
/// every L0 SST — `ssts_total` therefore counts all of them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct L0Stats {
    /// L0 SSTs in the tier — all of them, since L0 isn't range-partitioned.
    pub ssts_total: u32,
    /// Of those, how many actually held an in-query record for the key
    /// (data locality: fewer ⇒ better-localized).
    pub ssts_with_data: u32,
    /// Across the SSTs that held data, how many data blocks the in-query
    /// records span — block-level locality. Divided by `ssts_with_data` it
    /// gives the average blocks-per-SST a read must touch.
    pub blocks_with_data: u32,
    /// In-query records (physical puts) found in L0.
    pub records: u64,
}

/// Distribution of a key's in-query records across the **sorted-run tier**.
/// Sorted runs are range-partitioned, so only the SST views covering the
/// query are checked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SortedRunStats {
    /// Sorted runs overlapping the query (a read merges across these).
    pub runs: u32,
    /// SST views across those runs that cover the query.
    pub ssts_total: u32,
    /// Of those, how many actually held an in-query record for the key.
    pub ssts_with_data: u32,
    /// Across the SSTs that held data, how many data blocks the in-query
    /// records span — block-level locality.
    pub blocks_with_data: u32,
    /// In-query records (physical puts) found in the sorted runs.
    pub records: u64,
}

/// LSM data-distribution statistics for a single [`count_in_range`] walk.
///
/// A *tier* here is one of the two kinds of level a SlateDB tree holds: the
/// **L0 tier** — the freshly-flushed SSTs, which are not range-partitioned
/// and may overlap each other in key space — and the **sorted-run tier** —
/// the compacted sorted runs, each a range-partitioned, non-overlapping run
/// of SSTs. A read consults both.
///
/// These stats characterize the *shape of the data* the query touched — how
/// the key's records are split between those two tiers — rather than the
/// cost of the count that produced it. Both tiers are summed over the trees
/// the walk visited: the unsegmented default tree plus every configured
/// segment whose prefix interval overlaps `query` (see [`count_in_range`]).
/// For a query confined to one segment's prefix — the LogDb case — they
/// describe that one segment's tree.
///
/// `l0.records + sorted_runs.records` is the count contributed by persisted
/// SSTs; writes not yet flushed are accounted separately by the caller.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalkStats {
    /// The L0 tier's contribution; see [`L0Stats`].
    pub l0: L0Stats,
    /// The sorted-run tier's contribution; see [`SortedRunStats`].
    pub sorted_runs: SortedRunStats,
}

/// Private accumulator for one tier (L0 or sorted-run — see [`WalkStats`])
/// that the walk threads through `count_view`. The public [`L0Stats`] /
/// [`SortedRunStats`] are assembled from these.
#[derive(Default)]
struct TierWalk {
    ssts_total: u32,
    ssts_with_data: u32,
    blocks_with_data: u32,
    records: u64,
}

impl TierWalk {
    /// Records one block's in-query contribution: its records, and — when it
    /// holds at least one in-query row — that it's a data-spanning block.
    fn add_block(&mut self, counts: BlockOpCounts) {
        self.records += counts.num_puts;
        if counts.num_rows() > 0 {
            self.blocks_with_data += 1;
        }
    }
}

/// Result of a [`count_in_range`] walk: aggregate counts plus a witness
/// of the highest key actually observed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CountResult {
    /// Per-variant counts of physical write operations in `range`.
    pub counts: BlockOpCounts,
    /// Inclusive upper bound on the keys reflected in `counts`. If
    /// `Some(k)`, every persisted entry in `[range.start, k]` is included.
    /// `None` if no SSTs contributed to the count (no overlap, empty
    /// manifest, etc.).
    ///
    /// Callers that need exact counts including not-yet-persisted writes
    /// can combine this with a scan over `(covered_to, range.end)`.
    pub covered_to: Option<Bytes>,
    /// Per-tier record distribution for this walk; see [`WalkStats`].
    pub stats: WalkStats,
}

/// Counts every physical write operation in the manifest whose key falls
/// in `query`, and returns a witness of how far up the walk observed data.
///
/// SlateDB stores data in two places: the unsegmented *default* tree
/// ([`VersionedManifest::l0`] / [`compacted`](VersionedManifest::compacted))
/// and, when a `PrefixExtractor` is configured, one independent LSM tree per
/// *segment* ([`VersionedManifest::segments`]) — each owning the key
/// interval `[prefix, prefix++)`. With an extractor every write routes to a
/// segment and the default tree is empty by construction, so a walk that
/// only consulted the default tree would miss all the data. This walks the
/// default tree and every segment whose interval overlaps `query`; for a
/// query confined to one routing prefix (the LogDb case) that is exactly one
/// segment.
///
/// Within each tree it covers L0 SSTs and the views returned by each
/// compacted sorted run's `tables_covering_range(query)`. SSTs without a
/// stats block (predating RFC 0020) are skipped with a tracing warning, so
/// the result undercounts in that case.
pub async fn count_in_range(
    manifest: &VersionedManifest,
    sst_reader: &SstReader,
    query: &BytesRange,
) -> StorageResult<CountResult> {
    let mut result = CountResult::default();

    // The unsegmented default tree. Empty when an extractor is configured,
    // but walking it keeps the no-extractor case correct.
    walk_tree(
        sst_reader,
        manifest.l0(),
        manifest.compacted(),
        query,
        &mut result,
    )
    .await?;

    // Each configured segment owns a disjoint prefix interval; walk only
    // those the query touches. This is where the data lives once a segment
    // extractor routes writes away from the default tree.
    for segment in manifest.segments() {
        if ranges_overlap(query, &prefix_range(segment.prefix())) {
            walk_tree(
                sst_reader,
                segment.l0(),
                segment.compacted(),
                query,
                &mut result,
            )
            .await?;
        }
    }
    Ok(result)
}

/// Walks one LSM tree (a default tree or a single segment's tree): every L0
/// SST plus the covering views of each sorted run. Accumulates counts,
/// `covered_to`, and per-tier data-distribution stats into `result`.
async fn walk_tree(
    sst_reader: &SstReader,
    l0: &VecDeque<SsTableView>,
    compacted: &[SortedRun],
    query: &BytesRange,
    result: &mut CountResult,
) -> StorageResult<()> {
    // L0: not range-partitioned, so every SST is checked.
    let mut l0_walk = TierWalk::default();
    for view in l0 {
        count_view(sst_reader, view, query, result, &mut l0_walk).await?;
    }
    result.stats.l0.ssts_total += l0_walk.ssts_total;
    result.stats.l0.ssts_with_data += l0_walk.ssts_with_data;
    result.stats.l0.blocks_with_data += l0_walk.blocks_with_data;
    result.stats.l0.records += l0_walk.records;

    // Sorted runs: range-partitioned, so only the views covering the query
    // are checked. A run counts as overlapping if it yields any such view.
    let mut sr_walk = TierWalk::default();
    for run in compacted {
        let mut overlaps = false;
        for view in run.tables_covering_range::<BytesRange>(query.clone()) {
            overlaps = true;
            count_view(sst_reader, view, query, result, &mut sr_walk).await?;
        }
        if overlaps {
            result.stats.sorted_runs.runs += 1;
        }
    }
    result.stats.sorted_runs.ssts_total += sr_walk.ssts_total;
    result.stats.sorted_runs.ssts_with_data += sr_walk.ssts_with_data;
    result.stats.sorted_runs.blocks_with_data += sr_walk.blocks_with_data;
    result.stats.sorted_runs.records += sr_walk.records;
    Ok(())
}

/// The key interval a segment owns: `[prefix, prefix++)`, where `prefix++`
/// is the smallest key strictly greater than every key beginning with
/// `prefix` (increment the last non-`0xFF` byte, dropping trailing `0xFF`s).
/// An all-`0xFF` or empty prefix has no upper bound.
fn prefix_range(prefix: &[u8]) -> BytesRange {
    let start = Bound::Included(Bytes::copy_from_slice(prefix));
    let mut end = prefix.to_vec();
    loop {
        match end.last().copied() {
            None => return BytesRange::new(start, Bound::Unbounded),
            Some(0xFF) => {
                end.pop();
            }
            Some(b) => {
                let last = end.len() - 1;
                end[last] = b + 1;
                return BytesRange::new(start, Bound::Excluded(Bytes::from(end)));
            }
        }
    }
}

async fn count_view(
    sst_reader: &SstReader,
    view: &SsTableView,
    query: &BytesRange,
    result: &mut CountResult,
    tier: &mut TierWalk,
) -> StorageResult<()> {
    let sst_file = sst_reader
        .open_with_handle(view.sst.clone())
        .map_err(StorageError::from_storage)?;
    let sst_id = sst_file.id();
    // This SST belongs to the tier (its index/stats footer were read) even
    // if no block ends up overlapping the query.
    tier.ssts_total += 1;

    let Some(stats) = sst_file.stats().await.map_err(StorageError::from_storage)? else {
        tracing::warn!(?sst_id, "SST has no stats block; skipping");
        return Ok(());
    };

    let index = sst_file.index().await.map_err(StorageError::from_storage)?;
    let last_entry = sst_file.info().last_entry.clone();

    let n = index.len();
    let overlapping: Vec<usize> = (0..n)
        .filter(|i| ranges_overlap(query, &block_key_range(&index, *i, last_entry.as_ref())))
        .collect();

    // Back-scan: walk overlapping blocks high-to-low, reading each, until
    // we find one with in-query rows (see the module-level "Why the
    // back-scan" section for the rationale). That witness pins down
    // `covered_to`; blocks above contributed nothing (counted as zero),
    // blocks below get the cheap stats path on the second pass.
    let mut witness_pos: Option<usize> = None;
    for pos in (0..overlapping.len()).rev() {
        let i = overlapping[pos];
        let key_range = block_key_range(&index, i, last_entry.as_ref());
        let is_sst_last = i + 1 == n;
        let contained = range_contains(query, &key_range);

        // Shortcut: SST's last data block is contained in the query — its
        // last_entry is a free witness (already in SsTableInfo, no read).
        if is_sst_last
            && contained
            && let Some(last) = last_entry.as_ref()
        {
            let block_counts = block_counts_from_stats(&stats, i, sst_id);
            result.counts.add(block_counts);
            tier.add_block(block_counts);
            bump_covered_to(&mut result.covered_to, last.clone());
            witness_pos = Some(pos);
            break;
        }

        let rows = sst_file
            .read_block(i)
            .await
            .map_err(StorageError::from_storage)?;
        let (block_counts, block_max) = count_rows_in_range_with_max(&rows, query);
        if let Some(max) = block_max {
            result.counts.add(block_counts);
            tier.add_block(block_counts);
            bump_covered_to(&mut result.covered_to, max);
            witness_pos = Some(pos);
            break;
        }
        // No in-query rows in this block; the separator was misleading.
        // Continue scanning down. We've already added zero counts.
    }

    let Some(witness_pos) = witness_pos else {
        // No SST contents in query.
        return Ok(());
    };
    tier.ssts_with_data += 1;

    // Forward pass for blocks below the witness. Cheap stats path when
    // contained; read for boundary blocks (needed to filter rows by query).
    for &i in &overlapping[..witness_pos] {
        let key_range = block_key_range(&index, i, last_entry.as_ref());
        let block_counts = if range_contains(query, &key_range) {
            block_counts_from_stats(&stats, i, sst_id)
        } else {
            let rows = sst_file
                .read_block(i)
                .await
                .map_err(StorageError::from_storage)?;
            count_rows_in_range_with_max(&rows, query).0
        };
        result.counts.add(block_counts);
        tier.add_block(block_counts);
    }

    Ok(())
}

fn bump_covered_to(covered_to: &mut Option<Bytes>, candidate: Bytes) {
    match covered_to {
        None => *covered_to = Some(candidate),
        Some(existing) if *existing < candidate => *covered_to = Some(candidate),
        _ => {}
    }
}

fn block_counts_from_stats(stats: &SstStats, i: usize, sst_id: ulid::Ulid) -> BlockOpCounts {
    match stats.block_stats.get(i) {
        Some(bs) => BlockOpCounts {
            num_puts: bs.num_puts as u64,
            num_deletes: bs.num_deletes as u64,
            num_merges: bs.num_merges as u64,
        },
        None => {
            // Index and stats disagree on block count — shouldn't happen
            // for an RFC-0020 SST. Skip the block rather than crash.
            tracing::warn!(?sst_id, block_index = i, "missing block_stats entry");
            BlockOpCounts::default()
        }
    }
}

fn count_rows_in_range_with_max(
    rows: &[RowEntry],
    query: &BytesRange,
) -> (BlockOpCounts, Option<Bytes>) {
    let mut counts = BlockOpCounts::default();
    let mut max_key: Option<Bytes> = None;
    for row in rows {
        if !query.contains(&row.key) {
            continue;
        }
        match row.value {
            ValueDeletable::Value(_) => counts.num_puts += 1,
            ValueDeletable::Merge(_) => counts.num_merges += 1,
            ValueDeletable::Tombstone => counts.num_deletes += 1,
        }
        match &max_key {
            None => max_key = Some(row.key.clone()),
            Some(m) if *m < row.key => max_key = Some(row.key.clone()),
            _ => {}
        }
    }
    (counts, max_key)
}

fn block_key_range(index: &[(u64, Bytes)], i: usize, sst_last_entry: Option<&Bytes>) -> BytesRange {
    let start = Bound::Included(index[i].1.clone());
    let end = if i + 1 < index.len() {
        Bound::Excluded(index[i + 1].1.clone())
    } else {
        match sst_last_entry {
            Some(last) => Bound::Included(last.clone()),
            None => Bound::Unbounded,
        }
    };
    BytesRange::new(start, end)
}

fn ranges_overlap(a: &BytesRange, b: &BytesRange) -> bool {
    lower_lt_upper(&a.start, &b.end) && lower_lt_upper(&b.start, &a.end)
}

fn lower_lt_upper(lower: &Bound<Bytes>, upper: &Bound<Bytes>) -> bool {
    match (lower, upper) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Included(l), Bound::Included(u)) => l <= u,
        (Bound::Included(l), Bound::Excluded(u)) => l < u,
        (Bound::Excluded(l), Bound::Included(u)) => l < u,
        (Bound::Excluded(l), Bound::Excluded(u)) => l < u,
    }
}

fn range_contains(outer: &BytesRange, inner: &BytesRange) -> bool {
    lower_le_lower(&outer.start, &inner.start) && upper_ge_upper(&outer.end, &inner.end)
}

fn lower_le_lower(a: &Bound<Bytes>, b: &Bound<Bytes>) -> bool {
    use Bound::*;
    match (a, b) {
        (Unbounded, _) => true,
        (_, Unbounded) => false,
        (Included(ak), Included(bk)) => ak <= bk,
        (Included(ak), Excluded(bk)) => ak <= bk,
        (Excluded(ak), Included(bk)) => ak < bk,
        (Excluded(ak), Excluded(bk)) => ak <= bk,
    }
}

fn upper_ge_upper(a: &Bound<Bytes>, b: &Bound<Bytes>) -> bool {
    use Bound::*;
    match (a, b) {
        (Unbounded, _) => true,
        (_, Unbounded) => false,
        (Included(ak), Included(bk)) => ak >= bk,
        (Included(ak), Excluded(bk)) => ak >= bk,
        (Excluded(ak), Included(bk)) => ak > bk,
        (Excluded(ak), Excluded(bk)) => ak >= bk,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MergeOperator;
    use slatedb::config::{FlushOptions, FlushType, PutOptions, SstBlockSize, WriteOptions};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, DbBuilder};
    use std::sync::Arc;

    const PATH: &str = "/test";

    /// Trivial merge operator: concatenate operands in batch order.
    struct ConcatMerger;

    impl MergeOperator for ConcatMerger {
        fn merge_batch(&self, _key: &Bytes, existing: Option<Bytes>, operands: &[Bytes]) -> Bytes {
            let mut out = existing.map(|b| b.to_vec()).unwrap_or_default();
            for op in operands {
                out.extend_from_slice(op);
            }
            Bytes::from(out)
        }
    }

    async fn build_db_with_entries(entries: &[(&[u8], &[u8])]) -> (Arc<Db>, Arc<InMemory>) {
        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let db = DbBuilder::new(PATH, object_store.clone())
            .with_sst_block_size(SstBlockSize::Block1Kib)
            .build()
            .await
            .unwrap();
        for (k, v) in entries {
            db.put_with_options(*k, *v, &PutOptions::default(), &WriteOptions::default())
                .await
                .unwrap();
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        (Arc::new(db), object_store)
    }

    fn sst_reader(object_store: Arc<InMemory>) -> SstReader {
        SstReader::new(PATH, object_store, None, None)
    }

    fn mk_range(lo: &[u8], hi: &[u8]) -> BytesRange {
        BytesRange::new(
            Bound::Included(Bytes::copy_from_slice(lo)),
            Bound::Excluded(Bytes::copy_from_slice(hi)),
        )
    }

    /// Builds ~250 entries with keys `kNNN` so an SST with 1 KiB blocks
    /// produces several blocks. Each entry is ~30 bytes after encoding,
    /// so ~30 entries/block → ~8+ blocks per SST.
    fn many_entries(count: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("k{:03}", i).into_bytes();
                let val = format!("v{:03}aaaaaaaaaaaaaa", i).into_bytes();
                (key, val)
            })
            .collect()
    }

    #[tokio::test]
    async fn empty_manifest_counts_zero() {
        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let db = DbBuilder::new(PATH, object_store.clone())
            .build()
            .await
            .unwrap();
        let result = count_in_range(
            &db.manifest(),
            &sst_reader(object_store),
            &BytesRange::unbounded(),
        )
        .await
        .unwrap();
        assert_eq!(result.counts, BlockOpCounts::default());
        assert!(result.covered_to.is_none());
    }

    #[tokio::test]
    async fn unbounded_query_counts_every_put() {
        let entries = many_entries(250);
        let entries_ref: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let (db, object_store) = build_db_with_entries(&entries_ref).await;

        let result = count_in_range(
            &db.manifest(),
            &sst_reader(object_store),
            &BytesRange::unbounded(),
        )
        .await
        .unwrap();
        assert_eq!(result.counts.num_puts, 250);
        assert_eq!(result.counts.num_deletes, 0);
        assert_eq!(result.counts.num_merges, 0);
        // Unbounded query → highest SST key is the witness. Last inserted key
        // is `k249`; the SST's last_entry is exactly that.
        assert_eq!(result.covered_to.as_deref(), Some(b"k249" as &[u8]));
    }

    /// The boundary-block path: query slices through interior blocks, so the
    /// contained-path stats counts would over-count. Asserts the exact answer.
    #[tokio::test]
    async fn subrange_query_counts_exactly() {
        let entries = many_entries(250);
        let entries_ref: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let (db, object_store) = build_db_with_entries(&entries_ref).await;

        let result = count_in_range(
            &db.manifest(),
            &sst_reader(object_store),
            &mk_range(b"k100", b"k150"),
        )
        .await
        .unwrap();
        assert_eq!(result.counts.num_puts, 50, "exactly the 50 keys k100..k150");
        // Boundary block at the top of query — last counted row is k149.
        assert_eq!(result.covered_to.as_deref(), Some(b"k149" as &[u8]));
    }

    #[tokio::test]
    async fn query_outside_keyspace_counts_zero() {
        let entries = many_entries(50);
        let entries_ref: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let (db, object_store) = build_db_with_entries(&entries_ref).await;

        let result = count_in_range(
            &db.manifest(),
            &sst_reader(object_store),
            &mk_range(b"z", b"z\xff"),
        )
        .await
        .unwrap();
        assert_eq!(result.counts, BlockOpCounts::default());
        assert!(result.covered_to.is_none());
    }

    #[tokio::test]
    async fn counts_across_multiple_l0_ssts() {
        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let db = DbBuilder::new(PATH, object_store.clone())
            .with_sst_block_size(SstBlockSize::Block1Kib)
            .build()
            .await
            .unwrap();

        for i in 0u8..5 {
            db.put_with_options(
                &[b'k', i],
                &[b'v', i],
                &PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        for i in 5u8..10 {
            db.put_with_options(
                &[b'k', i],
                &[b'v', i],
                &PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        let manifest = db.manifest();
        assert!(manifest.l0().len() >= 2);

        let result = count_in_range(
            &manifest,
            &sst_reader(object_store),
            &BytesRange::unbounded(),
        )
        .await
        .unwrap();
        assert_eq!(result.counts.num_puts, 10);
        // covered_to comes from the SST with the highest last_entry — `k\x09`.
        assert_eq!(result.covered_to.as_deref(), Some(&b"k\x09"[..]));
    }

    /// Flushes `batches` separately so each becomes its own L0 SST, then
    /// returns the DB + object store. Keys are taken verbatim from each
    /// batch so callers control which SST a key lands in.
    async fn build_db_with_l0_batches(batches: &[&[(&[u8], &[u8])]]) -> (Arc<Db>, Arc<InMemory>) {
        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let db = DbBuilder::new(PATH, object_store.clone())
            .with_sst_block_size(SstBlockSize::Block1Kib)
            .build()
            .await
            .unwrap();
        for batch in batches {
            for (k, v) in *batch {
                db.put_with_options(*k, *v, &PutOptions::default(), &WriteOptions::default())
                    .await
                    .unwrap();
            }
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        }
        (Arc::new(db), object_store)
    }

    #[tokio::test]
    async fn walk_stats_report_manifest_shape() {
        let a: &[(&[u8], &[u8])] = &[(b"k000", b"v0"), (b"k001", b"v1")];
        let b: &[(&[u8], &[u8])] = &[(b"k100", b"v2"), (b"k101", b"v3")];
        let (db, object_store) = build_db_with_l0_batches(&[a, b]).await;

        let result = count_in_range(
            &db.manifest(),
            &sst_reader(object_store),
            &BytesRange::unbounded(),
        )
        .await
        .unwrap();

        // Two flushes, no compaction → all data in two L0 SSTs, no runs.
        assert_eq!(result.stats.l0.ssts_total, 2);
        assert_eq!(result.stats.l0.ssts_with_data, 2, "both SSTs contribute");
        assert_eq!(
            result.stats.l0.blocks_with_data, 2,
            "two small SSTs, one block of data each"
        );
        assert_eq!(result.stats.l0.records, 4, "two records per batch");
        assert_eq!(result.stats.sorted_runs, SortedRunStats::default());
    }

    #[tokio::test]
    async fn walk_stats_check_every_l0_but_attribute_data_to_one() {
        // L0 is not range-partitioned: a query matching only the second SST
        // must still check the first to know it has nothing in range — but
        // only the second holds data.
        let a: &[(&[u8], &[u8])] = &[(b"k000", b"v0"), (b"k001", b"v1")];
        let b: &[(&[u8], &[u8])] = &[(b"k100", b"v2"), (b"k101", b"v3")];
        let (db, object_store) = build_db_with_l0_batches(&[a, b]).await;

        let result = count_in_range(
            &db.manifest(),
            &sst_reader(object_store),
            &mk_range(b"k100", b"k200"),
        )
        .await
        .unwrap();

        assert_eq!(result.counts.num_puts, 2, "only the k1xx batch matches");
        assert_eq!(result.stats.l0.ssts_total, 2, "both L0 SSTs are present");
        assert_eq!(
            result.stats.l0.ssts_with_data, 1,
            "only the second SST holds in-range rows"
        );
        assert_eq!(result.stats.l0.records, 2);
    }

    #[tokio::test]
    async fn walk_stats_attribute_records_on_boundary_slice() {
        // A subrange that slices interior blocks still attributes the exact
        // record count to the (single) L0 SST it lives in.
        let entries = many_entries(250);
        let entries_ref: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let (db, object_store) = build_db_with_entries(&entries_ref).await;

        let result = count_in_range(
            &db.manifest(),
            &sst_reader(object_store),
            &mk_range(b"k100", b"k150"),
        )
        .await
        .unwrap();

        assert_eq!(result.counts.num_puts, 50);
        assert_eq!(result.stats.l0.ssts_total, 1);
        assert_eq!(result.stats.l0.ssts_with_data, 1);
        // 50 records of ~30 bytes over 1 KiB blocks span multiple blocks.
        assert!(
            result.stats.l0.blocks_with_data >= 2,
            "a 50-record slice should span several blocks, got {}",
            result.stats.l0.blocks_with_data
        );
        assert_eq!(result.stats.l0.records, 50);
        assert_eq!(result.stats.sorted_runs, SortedRunStats::default());
    }

    /// Routes writes into per-segment trees by a fixed-length key prefix,
    /// mirroring how LogDb's `LogSegmentExtractor` partitions the keyspace.
    #[derive(Debug)]
    struct FixedPrefixExtractor(usize);

    impl slatedb::PrefixExtractor for FixedPrefixExtractor {
        fn name(&self) -> &str {
            "test/fixed-prefix"
        }

        fn prefix_len(&self, target: &slatedb::PrefixTarget) -> Option<usize> {
            let len = match target {
                slatedb::PrefixTarget::Point(b) => b.as_ref().len(),
                slatedb::PrefixTarget::Prefix(b) => b.as_ref().len(),
            };
            (len >= self.0).then_some(self.0)
        }
    }

    /// Builds a DB whose writes are routed into per-segment trees by their
    /// first `prefix_len` bytes, then flushed to L0.
    async fn build_segmented_db(
        entries: &[(&[u8], &[u8])],
        prefix_len: usize,
    ) -> (Arc<Db>, Arc<InMemory>) {
        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let db = DbBuilder::new(PATH, object_store.clone())
            .with_sst_block_size(SstBlockSize::Block1Kib)
            .with_segment_extractor(Arc::new(FixedPrefixExtractor(prefix_len)))
            .build()
            .await
            .unwrap();
        for (k, v) in entries {
            db.put_with_options(*k, *v, &PutOptions::default(), &WriteOptions::default())
                .await
                .unwrap();
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        (Arc::new(db), object_store)
    }

    #[tokio::test]
    async fn walks_per_segment_trees_when_extractor_configured() {
        // Regression: with a segment extractor, writes route into per-segment
        // trees and the default tree is empty. A walk that consulted only the
        // default tree (manifest.l0()/compacted()) would count zero — this is
        // the production LogDb layout, since LogDb always installs an
        // extractor.
        let entries: &[(&[u8], &[u8])] = &[
            (b"aa-1", b"v"),
            (b"aa-2", b"v"),
            (b"aa-3", b"v"),
            (b"bb-1", b"v"),
        ];
        let (db, object_store) = build_segmented_db(entries, 2).await;
        let manifest = db.manifest();

        // Precondition: data lives in segments, not the default tree.
        assert!(
            manifest.l0().is_empty() && manifest.compacted().is_empty(),
            "default tree must be empty under a segment extractor"
        );
        assert_eq!(manifest.segments().len(), 2, "one segment per prefix");

        // A query confined to the `aa` prefix must find exactly its three
        // rows and walk only that segment's tree (not the `bb` segment).
        let result = count_in_range(
            &manifest,
            &sst_reader(object_store),
            &mk_range(b"aa", b"ab"),
        )
        .await
        .unwrap();
        assert_eq!(result.counts.num_puts, 3, "the three aa-* rows");
        assert_eq!(result.covered_to.as_deref(), Some(b"aa-3" as &[u8]));
        // The data and its stats come from the walked segment's tree, not
        // the empty default tree.
        assert!(
            result.stats.l0.ssts_total >= 1,
            "must check the aa segment's L0 SST"
        );
        assert_eq!(result.stats.l0.ssts_with_data, 1);
        assert_eq!(result.stats.l0.blocks_with_data, 1, "three rows, one block");
        assert_eq!(result.stats.l0.records, 3);
    }

    #[tokio::test]
    async fn counts_tombstones_and_merges() {
        let object_store: Arc<InMemory> = Arc::new(InMemory::new());
        let merge_op: Arc<dyn MergeOperator> = Arc::new(ConcatMerger);
        let db = DbBuilder::new(PATH, object_store.clone())
            .with_merge_operator(Arc::new(
                crate::storage::slate::SlateDbStorage::merge_operator_adapter(merge_op),
            ))
            .build()
            .await
            .unwrap();

        // Use distinct keys: slatedb collapses same-key ops in the memtable
        // before flush (put+delete -> tombstone, put+merge -> value), so we
        // need one key per ValueDeletable variant to land in the SST.
        db.put_with_options(
            b"k1",
            b"v1",
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
        db.put_with_options(
            b"k2",
            b"v2",
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
        db.delete(b"k3").await.unwrap();
        let mut batch = slatedb::WriteBatch::new();
        batch.merge(b"k4", b"operand");
        db.write_with_options(batch, &WriteOptions::default())
            .await
            .unwrap();

        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        let result = count_in_range(
            &db.manifest(),
            &sst_reader(object_store),
            &BytesRange::unbounded(),
        )
        .await
        .unwrap();
        assert_eq!(result.counts.num_puts, 2);
        assert_eq!(result.counts.num_deletes, 1);
        assert_eq!(result.counts.num_merges, 1);
    }

    /// The contained-interior-at-top case: query.end falls exactly on a
    /// block separator. The block just below is contained AND interior
    /// (the SST has more data above). count must still produce an exact
    /// answer by reading that block.
    #[tokio::test]
    async fn subrange_query_ending_at_block_boundary() {
        // Build a DB with enough entries that 1 KiB blocks split frequently,
        // then query `[k000, k100)` which is unlikely to align perfectly
        // with a block boundary BUT exercises the "highest-overlapping is
        // contained, SST has more data above" path for the SST containing
        // entries below k100.
        let entries = many_entries(250);
        let entries_ref: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let (db, object_store) = build_db_with_entries(&entries_ref).await;

        let result = count_in_range(
            &db.manifest(),
            &sst_reader(object_store),
            &mk_range(b"k000", b"k100"),
        )
        .await
        .unwrap();
        // Whether the boundary lands inside a block or on a separator, the
        // contract is the same: exactly 100 puts, and the witness key is
        // the highest one observed in `[k000, k100)`.
        assert_eq!(result.counts.num_puts, 100);
        assert_eq!(result.covered_to.as_deref(), Some(b"k099" as &[u8]));
    }

    #[test]
    fn range_contains_cases() {
        use Bound::*;
        let b = |s: &[u8]| Bytes::copy_from_slice(s);
        let r = |lo, hi| BytesRange::new(lo, hi);

        // Unbounded outer contains anything
        assert!(range_contains(
            &BytesRange::unbounded(),
            &r(Included(b(b"x")), Excluded(b(b"y"))),
        ));
        // Equal ranges contain each other
        let eq = r(Included(b(b"a")), Excluded(b(b"z")));
        assert!(range_contains(&eq, &eq));
        // Strict containment
        assert!(range_contains(
            &r(Included(b(b"a")), Excluded(b(b"z"))),
            &r(Included(b(b"c")), Excluded(b(b"d"))),
        ));
        // Inner extends below outer
        assert!(!range_contains(
            &r(Included(b(b"b")), Excluded(b(b"z"))),
            &r(Included(b(b"a")), Excluded(b(b"d"))),
        ));
        // Inner extends above outer
        assert!(!range_contains(
            &r(Included(b(b"a")), Excluded(b(b"d"))),
            &r(Included(b(b"a")), Excluded(b(b"z"))),
        ));
        // Outer Included(k) vs inner Excluded(k) — outer's lower allows points inner doesn't need
        assert!(range_contains(
            &r(Included(b(b"a")), Included(b(b"z"))),
            &r(Excluded(b(b"a")), Excluded(b(b"z"))),
        ));
        // Outer Excluded(k) vs inner Included(k) at start — outer starts past k, inner needs k
        assert!(!range_contains(
            &r(Excluded(b(b"a")), Unbounded),
            &r(Included(b(b"a")), Unbounded),
        ));
    }

    #[test]
    fn ranges_overlap_all_combinations() {
        use Bound::*;
        let b = |s: &[u8]| Bytes::copy_from_slice(s);

        // Disjoint
        assert!(!ranges_overlap(
            &BytesRange::new(Included(b(b"a")), Excluded(b(b"b"))),
            &BytesRange::new(Included(b(b"c")), Excluded(b(b"d"))),
        ));
        // Touching at exclusive boundary — no overlap
        assert!(!ranges_overlap(
            &BytesRange::new(Included(b(b"a")), Excluded(b(b"b"))),
            &BytesRange::new(Included(b(b"b")), Excluded(b(b"c"))),
        ));
        // Overlapping
        assert!(ranges_overlap(
            &BytesRange::new(Included(b(b"a")), Excluded(b(b"c"))),
            &BytesRange::new(Included(b(b"b")), Excluded(b(b"d"))),
        ));
        // Contained
        assert!(ranges_overlap(
            &BytesRange::new(Included(b(b"a")), Excluded(b(b"z"))),
            &BytesRange::new(Included(b(b"b")), Excluded(b(b"c"))),
        ));
        // Unbounded
        assert!(ranges_overlap(
            &BytesRange::unbounded(),
            &BytesRange::new(Included(b(b"a")), Excluded(b(b"b"))),
        ));
        // Inclusive upper meets inclusive lower — overlap at the shared point
        assert!(ranges_overlap(
            &BytesRange::new(Included(b(b"a")), Included(b(b"b"))),
            &BytesRange::new(Included(b(b"b")), Included(b(b"c"))),
        ));
    }
}
