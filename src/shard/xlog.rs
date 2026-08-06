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
//! The design and its verification live in
//! `docs/plans/2026-08-05-edge-changelog-incremental-index.md`.

use super::*;
use crate::shard::topology_tail::GraphTopologyOverlay;

/// Cap on xlog entries deleted per GC pass so a long-idle builder's backlog
/// cannot balloon one write transaction. A capped pass advances the low-water
/// mark to the last sequence it touched; the next pass continues from there.
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
        let mut last_sequence = low_water;
        let mut capped = false;
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (sequence, _, _) = parse_xlog_entry_key(&prefix, &key)?;
            if sequence > gc_through {
                break;
            }
            last_sequence = sequence;
            dead_keys.push(key);
            if dead_keys.len() >= XLOG_GC_MAX_DELETES {
                capped = true;
                break;
            }
        }

        // Nothing dead: write nothing, not even a low-water advance. The
        // floor being lower than strictly necessary never breaks coverage,
        // and an empty pass that still commits would advance the durable
        // sequence every cycle — perpetual churn for zero reclaimed bytes.
        if dead_keys.is_empty() {
            return Ok(0);
        }
        // A capped pass may leave entries at `last_sequence` behind, so the
        // floor stays *at* that sequence (still retained), not past it; the
        // next pass resumes there. An uncapped pass covered everything
        // through `gc_through`.
        let next_low = if capped {
            last_sequence
        } else {
            gc_through.saturating_add(1)
        };
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
