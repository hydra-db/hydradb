//! The edge changelog (xlog): the delta, stored as data at commit time.
//!
//! Every topology mutation writes one entry per changed edge under
//! `cell/{cell}/xlog/{edge_type}/{seq:020}/{src:020}/{dst:020}` in the same
//! transaction as the mutation itself (`mark_topology_change_txn` in
//! `write.rs`), so "which edges of type T changed in `(B_prev, B]`" is a
//! single bounded range scan instead of a WAL-file walk priced per round
//! trip. The 1-byte value is the edge's existence *after* the commit —
//! final state, not operation — so within a scanned range the last entry in
//! sequence order wins per `(src, dst)` and re-scanning an overlapping range
//! after a crash is harmless.
//!
//! The design and its verification live in the cost note
//! (`interactive/incremental-build-cost.html`, §11) and the benchmark
//! record (`docs/benchmarks/2026-08-07-xlog-incremental-index.md`).

use super::*;
use crate::shard::topology_tail::GraphTopologyOverlay;

/// Soft cap on xlog entries deleted per GC pass so a long-idle builder's
/// backlog cannot balloon one write transaction.
///
/// **Soft, and it has to be.** The floor this pass publishes names a
/// *sequence*, while the cap counts *entries*, and one sequence is one commit
/// with one entry per changed edge — so a hard cap would routinely land inside
/// a sequence. A pass that deleted half of sequence `S` and then published `S`
/// as the retention floor would be claiming coverage for entries it had just
/// removed, and the next incremental build over a range starting at `S` would
/// take the `Complete` arm with a silently truncated delta instead of
/// bootstrapping. So the pass stops at the first sequence *boundary* at or
/// after the cap: a sequence is always collected whole, and the floor is
/// always one past the last sequence that no longer exists. The overshoot is
/// bounded by one commit's edge count, which the write path already had to
/// hold in a single transaction.
const XLOG_GC_MAX_DELETES: usize = 100_000;

/// Outcome of deriving the delta for `(previous.base_sequence, base_sequence]`
/// from the xlog. Both non-`Complete` arms mean "run the bootstrap full
/// build" — they are one-time coverage conditions, not cost verdicts.
pub(crate) enum XlogDelta {
    /// The range is fully retained; the overlay holds the resolved final
    /// state per changed `(src, dst)`.
    Complete(GraphTopologyOverlay),
    /// No low-water key: nothing has ever been logged for this edge type
    /// (xlog not yet deployed when the previous generation was built, or the
    /// cell has never mutated this edge type since deployment).
    Uninitialized,
    /// The low-water mark sits above `previous.base_sequence + 1`: part of
    /// the needed range was GC'd or never logged, so the delta cannot be
    /// trusted. Only reachable on first enablement or after a manual purge —
    /// the builder GCs strictly below its own published base.
    CoverageGap { low_water: StorageSequence },
}

pub(crate) fn parse_xlog_entry_key(
    prefix: &str,
    key: &str,
) -> Result<(StorageSequence, VertexId, VertexId)> {
    let corrupt = |reason: &str| GraphError::CorruptValue {
        key: key.to_string(),
        reason: reason.to_string(),
    };
    let suffix = key
        .strip_prefix(prefix)
        .ok_or_else(|| corrupt("xlog key does not match scan prefix"))?;
    let mut components = suffix.split('/');
    let mut component = |name: &str| {
        components
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| corrupt(&format!("xlog key missing numeric {name}")))
    };
    let sequence = component("sequence")?;
    let src = component("src")?;
    let dst = component("dst")?;
    if components.next().is_some() {
        return Err(corrupt("xlog key has trailing components"));
    }
    Ok((sequence, src, dst))
}

/// What one GC pass does with the entry it is looking at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum XlogGcStep {
    /// Add it to the delete batch.
    Take,
    /// End the pass without it.
    Stop,
}

/// The retention decision a `gc_topology_changelog_to` pass makes as it walks
/// the xlog in ascending sequence order.
///
/// Split out of the scan loop so both boundary rules — where the cap may stop,
/// and what floor the pass then publishes — are exercisable from a table test
/// instead of only through a backlog of [`XLOG_GC_MAX_DELETES`] real entries.
/// The two are one rule seen twice, and reading them side by side is the point:
/// the floor names a *sequence*, so the pass may only stop where a sequence
/// ends.
pub(crate) struct XlogGcPass {
    gc_through: StorageSequence,
    max_deletes: usize,
    taken: usize,
    last_sequence: StorageSequence,
    capped: bool,
}

impl XlogGcPass {
    /// A pass resuming at `low_water` and collecting entries stamped at or
    /// below `gc_through` — the published generation's base, above which
    /// entries are the next build's delta rather than dead weight.
    pub(crate) fn new(
        low_water: StorageSequence,
        gc_through: StorageSequence,
        max_deletes: usize,
    ) -> Self {
        Self {
            gc_through,
            max_deletes,
            taken: 0,
            last_sequence: low_water,
            capped: false,
        }
    }

    /// Offer the pass the entry stamped `sequence`. Entries must arrive in
    /// ascending sequence order, which the key encoding's zero padding
    /// guarantees.
    pub(crate) fn step(&mut self, sequence: StorageSequence) -> XlogGcStep {
        if sequence > self.gc_through {
            return XlogGcStep::Stop;
        }
        // The cap is consulted before the entry is taken, and only where the
        // sequence changes. A pass that stopped *inside* a sequence would
        // delete part of one commit and then publish a floor claiming that
        // commit is still retained, and the next incremental build over a
        // range starting there would take `XlogDelta::Complete` with a
        // silently truncated delta instead of bootstrapping.
        if self.taken >= self.max_deletes && sequence != self.last_sequence {
            self.capped = true;
            return XlogGcStep::Stop;
        }
        self.last_sequence = sequence;
        self.taken = self.taken.saturating_add(1);
        XlogGcStep::Take
    }

    /// Whether the pass stopped at the cap rather than at `gc_through`.
    pub(crate) fn capped(&self) -> bool {
        self.capped
    }

    /// The retention floor to publish: one past the last sequence this pass
    /// emptied, i.e. the lowest sequence whose entries still exist. Only
    /// meaningful once at least one entry was taken.
    pub(crate) fn next_low_water(&self) -> StorageSequence {
        if self.capped {
            self.last_sequence.saturating_add(1)
        } else {
            self.gc_through.saturating_add(1)
        }
    }
}

pub(crate) fn decode_xlog_exists(key: &str, value: &[u8]) -> Result<bool> {
    match value {
        [0] => Ok(false),
        [1] => Ok(true),
        _ => Err(GraphError::CorruptValue {
            key: key.to_string(),
            reason: "xlog value must be a single 0/1 existence byte".to_string(),
        }),
    }
}

impl GraphShard {
    /// Derive the incremental build's delta from the xlog: one bounded range
    /// scan over `(previous.base_sequence, base_sequence]`, resolved
    /// last-wins per `(src, dst)`. Reads only through the pinned snapshot, so
    /// an incremental and a full build at the same snapshot see the same
    /// changes — the byte-identical-payload oracle in `src/tests.rs` depends
    /// on exactly this.
    pub(crate) async fn xlog_delta_since(
        &self,
        snapshot: &GraphStorageSnapshot,
        previous: &crate::GraphIndexGeneration,
        base_sequence: StorageSequence,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<XlogDelta> {
        let low_key = keys::xlog_low_water(cell_id, edge_type);
        let low_water = match snapshot
            .get_with_options(low_key.as_bytes(), &remote_read_options())
            .await?
        {
            Some(value) => decode_u64(&low_key, &value)?,
            None => return Ok(XlogDelta::Uninitialized),
        };
        let from = previous.base_sequence.saturating_add(1);
        if from < low_water {
            return Ok(XlogDelta::CoverageGap { low_water });
        }

        let prefix = keys::xlog_type_prefix(cell_id, edge_type);
        // The subrange is relative to the prefix, and the sequence is the
        // first zero-padded component after it, so plain string bounds give
        // exactly `from <= seq <= base_sequence`. The snapshot would exclude
        // later commits anyway; the upper bound keeps the scan from touching
        // their blocks at all.
        let upper = base_sequence.saturating_add(1);
        let mut iter = snapshot
            .scan_prefix_with_options(
                prefix.as_bytes(),
                format!("{from:020}").into_bytes()..format!("{upper:020}").into_bytes(),
                &remote_scan_options(),
            )
            .await?;
        let mut overlay = GraphTopologyOverlay::default();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_, src, dst) = parse_xlog_entry_key(&prefix, &key)?;
            let exists = decode_xlog_exists(&key, &kv.value)?;
            // Entries arrive in ascending sequence order, so a plain
            // overwrite is last-in-sequence-wins.
            overlay.set(src, dst, exists);
        }
        Ok(XlogDelta::Complete(overlay))
    }

    /// Delete xlog entries the current published generation has made dead and
    /// advance the low-water mark. Best-effort by design: a shard without the
    /// SlateDB writer (the production indexer reads through a `DbReader`)
    /// returns `Ok(0)` untouched, leaving retention to whichever process
    /// holds the writer. Never called from inside a build — GC is a write,
    /// and a build that writes would advance the very sequence it just
    /// published against (and break the byte-identical build oracle, which
    /// compares an incremental and a full build at the same sequence).
    pub async fn gc_topology_changelog(&self, cell_id: &str, edge_type: &str) -> Result<u64> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        if self.db.writer().is_err() {
            return Ok(0);
        }
        let Some(generation) = self.current_graph_index(cell_id, edge_type).await? else {
            return Ok(0);
        };
        self.gc_topology_changelog_to(&generation).await
    }

    /// GC against a generation the caller already holds — the builder calls
    /// this with the manifest it just published, so retention is one build
    /// cycle wherever the builder holds the writer.
    pub(crate) async fn gc_topology_changelog_to(
        &self,
        generation: &crate::GraphIndexGeneration,
    ) -> Result<u64> {
        let cell_id = generation.cell_id.as_str();
        let edge_type = generation.edge_type.as_str();
        self.ensure_write_authority(cell_id, "gc_topology_changelog")?;
        let _permit = self.acquire_gc_permit("gc_topology_changelog").await?;
        let _writer = self.writer_lane(cell_id).lock().await;

        let low_key = keys::xlog_low_water(cell_id, edge_type);
        let low_water = match self.read_remote(&low_key).await? {
            Some(value) => decode_u64(&low_key, &value)?,
            None => {
                // Nothing to collect — but an absent floor also means the
                // incremental build bootstraps forever, and the write path's
                // floor cache may believe the floor still exists (a manual
                // purge deletes the key out from under it). GC holds writer
                // authority and reads storage, not the cache, so it repairs
                // the floor here: claiming coverage only from the current
                // epoch onward is conservative — anything older bootstraps
                // exactly once more — and converges instead of degrading
                // silently.
                let repaired = self.current_epoch(cell_id).await?;
                let mut batch = GraphWriteBatch::new();
                batch.put(low_key.as_bytes(), encode_u64(repaired));
                self.write_graph_batch_strict_with_cell_lock(
                    cell_id,
                    "gc_topology_changelog",
                    batch,
                )
                .await?;
                tracing::info!(
                    cell_id,
                    edge_type,
                    repaired,
                    "xlog low-water mark was absent; repaired at the current epoch"
                );
                return Ok(0);
            }
        };
        // Entries stamped <= the published base are dead: the next build
        // scans `(base, B']`. Entries above it are the next build's delta.
        let gc_through = generation.base_sequence;
        if low_water > gc_through {
            return Ok(0);
        }

        let prefix = keys::xlog_type_prefix(cell_id, edge_type);
        let mut iter = self
            .db
            .scan_prefix_with_options(
                prefix.as_bytes(),
                Some(format!("{low_water:020}").into_bytes()),
                &remote_scan_options(),
            )
            .await?;
        let mut dead_keys: Vec<String> = Vec::new();
        let mut pass = XlogGcPass::new(low_water, gc_through, XLOG_GC_MAX_DELETES);
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (sequence, _, _) = parse_xlog_entry_key(&prefix, &key)?;
            match pass.step(sequence) {
                XlogGcStep::Take => dead_keys.push(key),
                XlogGcStep::Stop => break,
            }
        }

        // Nothing dead: write nothing, not even a low-water advance. The
        // floor being lower than strictly necessary never breaks coverage,
        // and an empty pass that still commits would advance the durable
        // sequence every cycle — perpetual churn for zero reclaimed bytes.
        if dead_keys.is_empty() {
            return Ok(0);
        }
        let capped = pass.capped();
        let next_low = pass.next_low_water();
        let mut batch = GraphWriteBatch::new();
        for key in &dead_keys {
            batch.delete(key.as_bytes());
        }
        if next_low > low_water {
            batch.put(low_key.as_bytes(), encode_u64(next_low));
        }
        let deleted = dead_keys.len() as u64;
        self.write_graph_batch_strict_with_cell_lock(cell_id, "gc_topology_changelog", batch)
            .await?;
        tracing::debug!(
            cell_id,
            edge_type,
            deleted,
            next_low,
            capped,
            "xlog GC advanced the low-water mark"
        );
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive a pass over `sequences` and report what it collected and what
    /// floor it would publish. One entry per element, in the order the key
    /// encoding delivers them.
    fn run(
        sequences: &[StorageSequence],
        low_water: StorageSequence,
        gc_through: StorageSequence,
        max_deletes: usize,
    ) -> (Vec<StorageSequence>, StorageSequence, bool) {
        let mut pass = XlogGcPass::new(low_water, gc_through, max_deletes);
        let mut taken = Vec::new();
        for sequence in sequences {
            match pass.step(*sequence) {
                XlogGcStep::Take => taken.push(*sequence),
                XlogGcStep::Stop => break,
            }
        }
        (taken, pass.next_low_water(), pass.capped())
    }

    /// The invariant every other test here is a corollary of, stated once:
    /// `xlog_low_water` is the lowest sequence whose entries are *still
    /// retained* (`keys::xlog_low_water`), so no sequence may be left
    /// half-collected below the floor the pass publishes. `xlog_delta_since`
    /// reads that floor and answers `Complete` for any range starting at or
    /// above it; a half-collected sequence there is a silently truncated
    /// delta, not an error.
    #[test]
    fn a_capped_pass_never_leaves_a_sequence_half_collected() {
        // Sequence 7 is one commit that changed four edges. The cap falls in
        // the middle of it.
        let (taken, next_low, capped) = run(&[5, 6, 7, 7, 7, 7, 8, 9], 5, 9, 3);

        assert!(capped, "the cap must have stopped this pass");
        assert_eq!(
            taken,
            [5, 6, 7, 7, 7, 7],
            "the pass must finish sequence 7 rather than stop inside it"
        );
        assert_eq!(
            next_low, 8,
            "the floor must name the lowest sequence that still exists"
        );
        assert!(
            !taken.contains(&next_low),
            "the published floor must not name a sequence this pass deleted from"
        );
    }

    /// The regression this file's fix is for, phrased as the builder sees it:
    /// after a capped pass, a delta whose range opens at the floor must find
    /// every entry that floor claims is retained.
    #[test]
    fn the_published_floor_only_covers_sequences_that_were_left_alone() {
        let all: Vec<StorageSequence> = vec![5, 6, 7, 7, 7, 7, 8, 9];
        let (taken, next_low, _) = run(&all, 5, 9, 3);

        let survivors: Vec<StorageSequence> = all
            .iter()
            .copied()
            .skip(taken.len())
            .filter(|sequence| *sequence <= 9)
            .collect();
        // Everything the next build would scan from `next_low` is still there,
        // and nothing below `next_low` survived to be missed.
        assert!(
            survivors.iter().all(|sequence| *sequence >= next_low),
            "a surviving entry sits below the published floor: {survivors:?}"
        );
        assert!(
            taken.iter().all(|sequence| *sequence < next_low),
            "a deleted entry sits at or above the published floor: {taken:?}"
        );
    }

    /// A single commit larger than the cap must still be collected whole, or
    /// GC would never make progress past it. The cap is a soft one precisely
    /// so this terminates.
    #[test]
    fn one_commit_bigger_than_the_cap_is_still_collected_whole() {
        let (taken, next_low, capped) = run(&[4, 4, 4, 4, 4, 4, 5], 4, 5, 2);

        assert!(capped);
        assert_eq!(taken, [4, 4, 4, 4, 4, 4], "sequence 4 must come out whole");
        assert_eq!(next_low, 5, "and the floor must advance past it");
    }

    /// An uncapped pass covered everything through the published base, so the
    /// floor goes one past that base and not merely past the last entry it
    /// happened to see.
    #[test]
    fn an_uncapped_pass_publishes_one_past_the_generation_base() {
        let (taken, next_low, capped) = run(&[5, 6, 7], 5, 9, 100);

        assert!(!capped);
        assert_eq!(taken, [5, 6, 7]);
        assert_eq!(next_low, 10);
    }

    /// Entries above the published base are the *next* build's delta, not
    /// dead weight, and the pass stops at them whatever the cap says.
    #[test]
    fn entries_above_the_generation_base_are_never_collected() {
        let (taken, next_low, capped) = run(&[5, 6, 10, 11], 5, 6, 100);

        assert!(!capped);
        assert_eq!(taken, [5, 6]);
        assert_eq!(next_low, 7);
    }

    /// The cap alone does not stop a pass: it arms it, and the next sequence
    /// boundary fires it. A run that reaches the cap on its final sequence
    /// finishes uncapped.
    #[test]
    fn reaching_the_cap_on_the_last_sequence_is_not_a_capped_pass() {
        let (taken, next_low, capped) = run(&[5, 5, 5], 5, 5, 2);

        assert!(!capped, "there was no later sequence to stop at");
        assert_eq!(taken, [5, 5, 5]);
        assert_eq!(next_low, 6);
    }
}
