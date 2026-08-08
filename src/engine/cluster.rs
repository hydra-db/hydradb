use super::*;
use crate::keys;

use chrono::Utc;
use tracing::Instrument as _;
use turbolay_placement::cell_writer::{self, CellWriterRecord};

/// Stamp a failure onto the span that raised it.
///
/// §6 of `docs/plans/2026-07-26-otel-telemetry-crate.md`: a `fencing` error seen
/// at `client.mutate` says a write failed, and the same error on
/// `placement.resolve` says why. So each site records its own class rather than
/// letting an outer span report one it did not produce.
///
/// `contention` and `fencing` are expected classes. Nothing here logs — the
/// attribute exists so a dashboard can chart the *rate*, not so each occurrence
/// pages someone.
fn record_error_class(error: &GraphError) {
    tracing::Span::current().record("error.class", error.class());
}

impl GraphCluster {
    pub async fn open_cells(
        base_path: impl Into<String>,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_cells_scoped(base_path, GraphScope::default(), cell_ids, object_store).await
    }

    pub async fn open_cells_scoped(
        base_path: impl Into<String>,
        scope: GraphScope,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_cells_scoped_with_options(
            base_path,
            scope,
            cell_ids,
            object_store,
            GraphOpenOptions::default(),
        )
        .await
    }

    pub async fn open_cells_scoped_with_options(
        base_path: impl Into<String>,
        scope: GraphScope,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        let store_path = scope.scoped_store_path(&base_path.into());
        Self::open_cells_at_path(store_path, scope, cell_ids, object_store, false, options).await
    }

    pub async fn open_cells_standalone_writers(
        base_path: impl Into<String>,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_cells_standalone_writers_scoped(
            base_path,
            GraphScope::default(),
            cell_ids,
            object_store,
        )
        .await
    }

    pub async fn open_cells_standalone_writers_scoped(
        base_path: impl Into<String>,
        scope: GraphScope,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let store_path = scope.scoped_store_path(&base_path.into());
        Self::open_cells_at_path(
            store_path,
            scope,
            cell_ids,
            object_store,
            true,
            GraphOpenOptions::default(),
        )
        .await
    }

    async fn open_cells_at_path(
        base_path: String,
        scope: GraphScope,
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        object_store: Arc<dyn ObjectStore>,
        writable: bool,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        let cell_ids = cell_ids
            .into_iter()
            .map(Into::into)
            .collect::<Vec<String>>();
        for cell_id in &cell_ids {
            validate_component("cell_id", cell_id)?;
        }
        let mut shards = BTreeMap::new();
        for cell_id in cell_ids {
            let path = format!("{base_path}/{cell_id}");
            let opened = if writable {
                GraphShard::open_standalone_writer_with_options(
                    path,
                    Arc::clone(&object_store),
                    options.clone(),
                )
                .await
            } else {
                GraphShard::open_with_options(path, Arc::clone(&object_store), options.clone())
                    .await
            };
            let shard = match opened {
                Ok(shard) => shard,
                Err(err) => {
                    close_shards_best_effort(shards).await;
                    return Err(err);
                }
            };
            shards.insert(cell_id, shard);
        }
        Ok(Self { scope, shards })
    }

    pub fn scope(&self) -> &GraphScope {
        &self.scope
    }

    pub fn shard(&self, cell_id: &str) -> Option<&GraphShard> {
        self.shards.get(cell_id)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub async fn close(&self) -> Result<()> {
        for shard in self.shards.values() {
            shard.close().await?;
        }
        Ok(())
    }
}

impl ObjectStoreNodeDirectory {
    pub fn new(
        cell_ids: impl IntoIterator<Item = impl Into<String>>,
        node_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let mut cells = BTreeSet::new();
        for cell_id in cell_ids {
            let cell_id = cell_id.into();
            validate_component("cell_id", &cell_id)?;
            if !cells.insert(cell_id.clone()) {
                return Err(GraphError::CorruptValue {
                    key: format!("directory/cell/{cell_id}"),
                    reason: "duplicate cell id".to_string(),
                });
            }
        }
        if cells.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "directory/cells".to_string(),
                reason: "at least one cell id is required".to_string(),
            });
        }
        let mut nodes = Vec::new();
        for node_id in node_ids {
            let node_id = node_id.into();
            validate_component("node_id", &node_id)?;
            nodes.push(node_id);
        }
        nodes.sort();
        nodes.dedup();
        if nodes.is_empty() {
            return Err(GraphError::CorruptValue {
                key: "directory/nodes".to_string(),
                reason: "at least one node id is required".to_string(),
            });
        }
        Ok(Self {
            cells,
            nodes: nodes.into_iter().collect(),
        })
    }

    pub fn cells(&self) -> impl Iterator<Item = &str> {
        self.cells.iter().map(String::as_str)
    }

    pub fn node_ids(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(String::as_str)
    }

    pub fn contains_cell(&self, cell_id: &str) -> Result<bool> {
        validate_component("cell_id", cell_id)?;
        Ok(self.cells.contains(cell_id))
    }

    pub fn contains_node(&self, node_id: &str) -> Result<bool> {
        validate_component("node_id", node_id)?;
        Ok(self.nodes.contains(node_id))
    }
}

struct RoutedClusterOpenConfig {
    base_path: String,
    scope: GraphScope,
    local_node_id: String,
    directory: ObjectStoreNodeDirectory,
    placement: PlacementView,
    object_store: Arc<dyn ObjectStore>,
    writer_registry: Option<Arc<ProcessWriterRegistry>>,
    promotable: bool,
    options: GraphOpenOptions,
    memory: GraphMemoryConfig,
}

impl RoutedGraphCluster {
    pub async fn open_readers(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_readers_scoped(
            base_path,
            GraphScope::default(),
            local_node_id,
            directory,
            placement,
            object_store,
        )
        .await
    }

    pub async fn open_promotable(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Self::open_promotable_scoped_with_memory_options(
            base_path,
            GraphScope::default(),
            local_node_id,
            directory,
            placement,
            object_store,
            GraphOpenOptions::default(),
            GraphMemoryConfig::default(),
        )
        .await
    }

    pub async fn open_promotable_with_options(
        base_path: impl Into<String>,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
    ) -> Result<Self> {
        Self::open_promotable_scoped_with_memory_options(
            base_path,
            GraphScope::default(),
            local_node_id,
            directory,
            placement,
            object_store,
            options,
            GraphMemoryConfig::default(),
        )
        .await
    }

    pub async fn open_readers_scoped(
        base_path: impl Into<String>,
        scope: GraphScope,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let base_path = scope.scoped_store_path(&base_path.into());
        Self::open_at_path(RoutedClusterOpenConfig {
            base_path,
            scope,
            local_node_id: local_node_id.into(),
            directory,
            placement,
            object_store,
            writer_registry: None,
            promotable: false,
            options: GraphOpenOptions::default(),
            memory: GraphMemoryConfig::default(),
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open_promotable_scoped_with_memory_options(
        base_path: impl Into<String>,
        scope: GraphScope,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
    ) -> Result<Self> {
        let writer_registry = process_writer_registry(&object_store);
        Self::open_promotable_scoped_with_writer_registry(
            base_path,
            scope,
            local_node_id,
            directory,
            placement,
            object_store,
            writer_registry,
            options,
            memory,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_promotable_scoped_with_writer_registry(
        base_path: impl Into<String>,
        scope: GraphScope,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
        writer_registry: Arc<ProcessWriterRegistry>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
    ) -> Result<Self> {
        let base_path = scope.scoped_store_path(&base_path.into());
        Self::open_at_path(RoutedClusterOpenConfig {
            base_path,
            scope,
            local_node_id: local_node_id.into(),
            directory,
            placement,
            object_store,
            writer_registry: Some(writer_registry),
            promotable: true,
            options,
            memory,
        })
        .await
    }

    async fn open_at_path(config: RoutedClusterOpenConfig) -> Result<Self> {
        let RoutedClusterOpenConfig {
            base_path,
            scope,
            local_node_id,
            directory,
            placement,
            object_store,
            writer_registry,
            promotable,
            options,
            memory,
        } = config;
        validate_component("node_id", &local_node_id)?;
        if !directory.contains_node(&local_node_id)? {
            return Err(GraphError::CorruptValue {
                key: format!("directory/node/{local_node_id}"),
                reason: "local node is not present in the object-store node directory".to_string(),
            });
        }
        // A placement view answers `Local` or `Remote` relative to *its* node
        // id, so a handle built for another node would have this one refuse
        // every write it owns and promote itself for every cell it does not —
        // and nothing about either symptom points at the mismatch.
        if placement.local_node_id() != local_node_id {
            return Err(GraphError::CorruptValue {
                key: format!("directory/node/{local_node_id}"),
                reason: format!(
                    "placement view belongs to node {}, not to this cluster's node",
                    placement.local_node_id()
                ),
            });
        }
        for cell_id in directory.cells() {
            validate_component("cell_id", cell_id)?;
        }
        let mut shards = BTreeMap::new();
        for cell_id in directory.cells().map(str::to_string) {
            let path = format!("{base_path}/{cell_id}");
            let opened = if promotable {
                GraphShard::open_promotable_for_node_with_memory_options(
                    path,
                    Arc::clone(&object_store),
                    options.clone(),
                    memory.clone(),
                    &local_node_id,
                    Arc::clone(
                        writer_registry
                            .as_ref()
                            .expect("promotable cluster must retain a writer registry"),
                    ),
                )
                .await
            } else {
                GraphShard::open_with_memory_options(
                    path,
                    Arc::clone(&object_store),
                    options.clone(),
                    memory.clone(),
                )
                .await
            };
            let shard = match opened {
                Ok(shard) => shard,
                Err(err) => {
                    close_routed_shards_best_effort(shards).await;
                    return Err(err);
                }
            };
            if !promotable
                && shard
                    .read_remote(&keys::cell_drop_marker(&cell_id))
                    .await?
                    .is_some()
            {
                shard.close().await?;
                continue;
            }
            shards.insert(cell_id, Arc::new(shard));
        }
        Ok(Self {
            scope,
            local_node_id,
            directory,
            placement,
            shards,
            promotable,
        })
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }
    /// The shared placement handle, for a caller that must hand the same live
    /// set to another reader (the Bolt routing provider) rather than build a
    /// second one.
    pub fn placement(&self) -> &PlacementView {
        &self.placement
    }
    pub fn scope(&self) -> &GraphScope {
        &self.scope
    }
    pub fn directory(&self) -> &ObjectStoreNodeDirectory {
        &self.directory
    }
    pub fn local_cells(&self) -> Vec<&str> {
        self.shards.keys().map(String::as_str).collect()
    }
    pub async fn local_shard_runtime_metrics(&self) -> Vec<GraphShardRuntimeMetrics> {
        let mut metrics = Vec::with_capacity(self.shards.len());
        for (cell_id, shard) in &self.shards {
            metrics.push(GraphShardRuntimeMetrics {
                cell_id: cell_id.clone(),
                operational: shard.graph_operational_metrics(),
                cache: shard.graph_cache_metrics(),
                cache_entries: shard.graph_cache_entry_counts().await,
                cache_resident_bytes: shard.graph_cache_resident_bytes().await,
            });
        }
        metrics
    }

    pub fn shard(&self, cell_id: &str) -> Result<&GraphShard> {
        validate_component("cell_id", cell_id)?;
        self.shards
            .get(cell_id)
            .map(Arc::as_ref)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })
    }

    #[cfg(feature = "query-transport")]
    fn holds_unowned_writer(&self) -> bool {
        let view = self.placement.view();
        let scope = self.scope.to_string();
        self.shards.iter().any(|(cell_id, shard)| {
            shard.db.writer_epoch().is_some()
                && !self
                    .placement
                    .ownership_in(&view, &scope, cell_id)
                    .may_promote()
        })
    }

    /// The gate every routed write passes through, and the one branch that ends
    /// the writer duel.
    ///
    /// > A node must not promote itself for a cell it does not own, unless the
    /// > computed owner is not live.
    ///
    /// Touch point (b) of `docs/plans/2026-07-25-rendezvous-placement.md`.
    /// Before this existed, any node opened the SlateDB writer on demand, which
    /// fenced whoever held it; the fenced node's next write took it straight
    /// back, and three nodes traded one cell's epoch forever. Rendezvous decides
    /// which of them has a *reason* to hold it, and the other two decline here.
    ///
    /// # Ownership first, pacing second
    ///
    /// The order is deliberate. A node that does not own the cell must be
    /// refused outright, not merely asked to wait — a wait it would spend and
    /// then promote anyway is the duel with extra latency. Only once ownership
    /// is settled does the fence backoff (touch point (d)) apply.
    ///
    /// [`CellOwnership::Unowned`] and [`CellOwnership::Unknown`] both mean
    /// "rendezvous named nobody" and have opposite answers, which is why they
    /// are matched separately here rather than through an `Option`: an empty
    /// live set licenses promotion, a *shed* view forbids it.
    ///
    /// # What the span is for
    ///
    /// `writer.acquire` is the span the whole of §4 exists to produce. The
    /// `cell-writer-fencing-pingpong` incident — three nodes trading one cell's
    /// epoch — is invisible as scattered `Fenced` errors and obvious as a span
    /// tree: three traces, three `turbolay.node_id`s, one `turbolay.cell_id`,
    /// and `turbolay.writer.epoch` climbing on every acquisition. The epoch is
    /// recorded after the promotion because that is the only moment it is known.
    pub(crate) async fn ensure_local_writer(&self, cell_id: &str) -> Result<()> {
        let span = tracing::info_span!(
            "writer.acquire",
            turbolay.scope = %self.scope,
            turbolay.cell_id = %cell_id,
            turbolay.node_id = %self.local_node_id,
            turbolay.writer.epoch = tracing::field::Empty,
            error.class = tracing::field::Empty,
        );
        self.ensure_local_writer_traced(cell_id)
            .instrument(span)
            .await
    }

    /// The body of [`Self::ensure_local_writer`], running inside its span.
    ///
    /// Split out only so the span can wrap an `async fn` without an inline
    /// `async` block swallowing the borrow checker's diagnostics; the control
    /// flow is unchanged.
    async fn ensure_local_writer_traced(&self, cell_id: &str) -> Result<()> {
        validate_component("cell_id", cell_id).inspect_err(record_error_class)?;
        if !self.promotable {
            let error = GraphError::WriteRequiresWriter {
                operation: "routed_write",
                cell_id: cell_id.to_string(),
            };
            record_error_class(&error);
            return Err(error);
        }
        let shard = self
            .shards
            .get(cell_id)
            .ok_or_else(|| GraphError::UnknownShard {
                cell_id: cell_id.to_string(),
            })
            .inspect_err(record_error_class)?;

        self.resolve_placement(cell_id)?;

        self.await_writer_reopen(shard, cell_id).await?;
        // Whether this shard already held a writer, taken *before* the call.
        // `promote_to_writer` is idempotent and says nothing about which of the
        // two it did, and the difference is the whole of "after a successful
        // promotion, and only then": a shard that was already writing claimed no
        // epoch, so there is nothing new to record and no PUT to make. Without
        // this check the record would be rewritten on every single write, which
        // is precisely the object-store request on the write path that decision
        // 3 refuses to add.
        let already_writing = shard.db.writer_epoch().is_some();
        shard.promote_to_writer(cell_id, "routed_write").await?;
        if let Some(epoch) = shard.db.writer_epoch() {
            // The climbing value in the ping-pong query. Taken from the
            // manifest, which is the authority; the advisory record written
            // below only ever repeats it.
            tracing::Span::current().record("turbolay.writer.epoch", epoch);
        }
        if !already_writing {
            self.record_cell_writer(shard, cell_id).await;
        }
        Ok(())
    }

    /// Ask rendezvous who owns the cell, as its own span.
    ///
    /// Synchronous and I/O-free by decision 10, so the span costs a timestamp
    /// pair. It earns that because the refusal it may return is the single most
    /// common write-path `fencing` error, and a class recorded here says
    /// "rendezvous named somebody else" rather than the far vaguer "a write
    /// failed" that the same error carries at the root.
    fn resolve_placement(&self, cell_id: &str) -> Result<()> {
        let span = tracing::info_span!(
            "placement.resolve",
            turbolay.scope = %self.scope,
            turbolay.cell_id = %cell_id,
            turbolay.node_id = %self.local_node_id,
            turbolay.placement.ownership = tracing::field::Empty,
            error.class = tracing::field::Empty,
        );
        let _entered = span.enter();
        let ownership = self.placement.ownership(&self.scope.to_string(), cell_id);
        // Recorded for all four answers, not just the refusals. "How often does
        // a write reach a node that does not own the cell" is the question the
        // write-routing work exists to answer, and it cannot be answered from
        // failures alone — a rate needs its denominator, and the successful
        // `local` case is the denominator.
        span.record(
            "turbolay.placement.ownership",
            match &ownership {
                CellOwnership::Local => "local",
                CellOwnership::Remote { .. } => "remote",
                CellOwnership::Unowned => "unowned",
                CellOwnership::Unknown => "unknown",
            },
        );
        match ownership {
            // The rendezvous owner, or a known-empty fleet with no owner to
            // defer to. Either way this node may hold the writer.
            CellOwnership::Local | CellOwnership::Unowned => Ok(()),
            // A live peer owns the cell. Name it, so the driver re-routes
            // instead of retrying into the same wrong node.
            CellOwnership::Remote { node_id } => Err(GraphError::NotCellWriter {
                cell_id: cell_id.to_string(),
                owner: Some(node_id),
            })
            .inspect_err(record_error_class),
            // This node has shed its view (decision 7) and knows nothing about
            // the fleet. Refuse with no hint: a stale guess would point at a
            // node that may itself have shed, and promoting here is the
            // unbounded duel — a partitioned node can never learn it lost.
            CellOwnership::Unknown => Err(GraphError::NotCellWriter {
                cell_id: cell_id.to_string(),
                owner: None,
            })
            .inspect_err(record_error_class),
        }
    }

    /// Write down that this node just took `cell_id`'s writer, for whoever reads
    /// the next incident's logs.
    ///
    /// Option 3a of decision 3 in
    /// `docs/plans/2026-07-25-rendezvous-placement.md`, and the only place the
    /// record is written. Three properties are load-bearing, and each is
    /// deliberate rather than incidental:
    ///
    /// - **It returns nothing.** A failed PUT cannot fail the promotion, because
    ///   the promotion has already succeeded — the epoch is claimed and the
    ///   write is about to proceed. Turning a diagnostics write into an
    ///   availability dependency on a second object-store request would be
    ///   strictly worse than keeping no record at all, so the error is logged at
    ///   `warn!` and dropped.
    /// - **It runs after the promotion, never before.** Written first it would
    ///   be a claim, and a claim that is read back is option 3b — the
    ///   read-then-act race decision 3 rejects.
    /// - **Nothing reads it here.** The promote path performs no GET. Ownership
    ///   was settled above by rendezvous, synchronously and from the shared live
    ///   set; this record is never an input to that decision. The epoch comes
    ///   from the manifest of the writer just opened, which remains the only
    ///   authority on who holds it.
    async fn record_cell_writer(&self, shard: &GraphShard, cell_id: &str) {
        let Some(epoch) = shard.db.writer_epoch() else {
            // Racing a fence between the promotion and this line. The record
            // would name an epoch this node no longer holds, so write nothing.
            tracing::debug!(
                node_id = %self.local_node_id,
                cell_id,
                "promoted writer was gone before it could be recorded"
            );
            return;
        };
        let Some((base, _)) = shard.db.cell_location() else {
            tracing::warn!(
                node_id = %self.local_node_id,
                cell_id,
                path = %shard.db.store_path(),
                "no base path to write the advisory cell-writer record under"
            );
            return;
        };
        // The one clock read on this path, and it is here rather than inside
        // `CellWriterRecord` by decision 10.
        let record = CellWriterRecord::new(&self.local_node_id, epoch, Utc::now());
        if let Err(error) =
            cell_writer::put_cell_writer(shard.db.object_store().as_ref(), &base, cell_id, &record)
                .await
        {
            // Advisory: the write path is already past this point.
            tracing::warn!(
                node_id = %self.local_node_id,
                cell_id,
                epoch,
                %error,
                "could not record the advisory cell writer; the promotion stands"
            );
        }
    }

    /// Touch point (d)'s backoff, enforced where the promotion happens.
    ///
    /// `refresh_writer_fence` records the wait and returns; nothing re-opens a
    /// writer until it comes back through here, so this is the one place the
    /// pacing can be applied without walking past the ownership check above.
    ///
    /// # Waiting rather than refusing, and the cap on it
    ///
    /// This node is the owner, so there is nowhere better to send the write:
    /// refusing would hand the driver an error it can only answer by retrying
    /// into this same node, turning a bounded local wait into an unbounded
    /// client-side one. So the common case — a fence, which arms exactly one
    /// `heartbeat_interval` sized to let the rival refresh its view and stand
    /// down — is waited out and the caller's write then succeeds on its
    /// original request.
    ///
    /// The wait is capped at that same interval because the gate is also armed
    /// by *plain* re-open failures, whose ladder climbs to a minute. Holding a
    /// Bolt request open for a minute is worse for the client than telling it
    /// to come back, so past the cap this refuses with a retryable
    /// `AdmissionRejected` instead. The cap costs nothing in the case decision
    /// 6 actually sizes: a fence never asks for longer than one interval.
    async fn await_writer_reopen(&self, shard: &GraphShard, cell_id: &str) -> Result<()> {
        // A shard that already holds its writer is not re-opening anything, so
        // the gate does not apply to it — only a promotion that would really
        // open a writer is paced.
        if shard.db.writer().is_ok() {
            return Ok(());
        }
        let Some(delay) = shard.db.writer_reopen_delay() else {
            return Ok(());
        };
        let cap = self.placement.config().heartbeat_interval;
        let span = tracing::info_span!(
            "writer.reopen_wait",
            turbolay.node_id = %self.local_node_id,
            turbolay.cell_id = %cell_id,
            turbolay.writer.reopen_delay_ms = delay.as_millis(),
            turbolay.writer.reopen_cap_ms = cap.as_millis(),
            turbolay.outcome = tracing::field::Empty,
            error.class = tracing::field::Empty,
        );
        let result = async {
            if delay > cap {
                let error = GraphError::AdmissionRejected {
                    operation: "writer_reopen",
                    actual: delay.as_millis() as u64,
                    limit: cap.as_millis() as u64,
                };
                record_error_class(&error);
                tracing::Span::current().record("turbolay.outcome", "failed");
                tracing::warn!(
                    operation = "writer_reopen",
                    delay_ms = delay.as_millis(),
                    cap_ms = cap.as_millis(),
                    error.class = error.class(),
                    "writer re-open delay exceeded the request wait cap"
                );
                return Err(error);
            }
            tracing::info!(
                operation = "writer_reopen",
                delay_ms = delay.as_millis(),
                cap_ms = cap.as_millis(),
                "pacing a writer re-open"
            );
            tokio::time::sleep(delay).await;
            tracing::Span::current().record("turbolay.outcome", "success");
            Ok(())
        }
        .instrument(span)
        .await;
        if let Err(error) = &result {
            // The nested span says where the rejection arose; the enclosing
            // `writer.acquire` span must still report that the acquisition
            // itself failed.
            record_error_class(error);
        }
        result
    }

    pub async fn write_edge(&self, mutation: crate::EdgeMutation) -> Result<crate::CommitResult> {
        let shard = self.shard(&mutation.cell_id)?;
        self.ensure_local_writer(&mutation.cell_id).await?;
        shard.write_edge(mutation).await
    }

    pub async fn delete_edge(&self, mutation: crate::EdgeMutation) -> Result<crate::DeleteResult> {
        let shard = self.shard(&mutation.cell_id)?;
        self.ensure_local_writer(&mutation.cell_id).await?;
        shard.delete_edge(mutation).await
    }

    pub async fn delete_edges_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<crate::EdgeDeleteBatchResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .delete_edges_batch(cell_id, edge_type, edges, idempotency_key)
            .await
    }

    pub async fn delete_edges_batch_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<crate::EdgeDeleteBatchResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .delete_edges_batch_chunked(cell_id, edge_type, edges, idempotency_key, chunk_size)
            .await
    }

    pub async fn delete_edge_mutations_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = crate::EdgeMutation>,
    ) -> Result<crate::EdgeDeleteBatchResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.delete_edge_mutations_batch(cell_id, mutations).await
    }

    pub async fn delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: crate::VertexId,
        idempotency_key: &str,
    ) -> Result<crate::VertexDeleteResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .delete_vertex(cell_id, vertex_id, idempotency_key)
            .await
    }

    pub async fn detach_delete_vertex(
        &self,
        cell_id: &str,
        vertex_id: crate::VertexId,
        idempotency_key: &str,
    ) -> Result<crate::VertexDeleteResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .detach_delete_vertex(cell_id, vertex_id, idempotency_key)
            .await
    }

    pub async fn drop_cell(
        &self,
        cell_id: &str,
        idempotency_key: &str,
    ) -> Result<crate::GraphCellDropResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.drop_cell(cell_id, idempotency_key).await
    }

    pub async fn ingest_edge_mutations(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = crate::EdgeMutation>,
        options: crate::EdgeIngestOptions,
    ) -> Result<crate::EdgeIngestResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .ingest_edge_mutations(cell_id, mutations, options)
            .await
    }

    pub async fn bulk_import_edges(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<crate::BulkImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .bulk_import_edges(cell_id, edge_type, edges, idempotency_key)
            .await
    }

    pub async fn write_edges_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
    ) -> Result<crate::BulkImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .write_edges_batch(cell_id, edge_type, edges, idempotency_key)
            .await
    }

    pub async fn write_edges_batch_chunked(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
        idempotency_key: &str,
        chunk_size: usize,
    ) -> Result<crate::BulkImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .write_edges_batch_chunked(cell_id, edge_type, edges, idempotency_key, chunk_size)
            .await
    }

    pub async fn set_vertex_metadata(
        &self,
        cell_id: &str,
        vertex_id: crate::VertexId,
        metadata: crate::VertexMetadata,
    ) -> Result<()> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .set_vertex_metadata(cell_id, vertex_id, metadata)
            .await
    }

    pub async fn set_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (crate::VertexId, crate::VertexMetadata)>,
    ) -> Result<usize> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.set_vertex_metadata_batch(cell_id, updates).await
    }

    pub async fn import_vertex_metadata_batch(
        &self,
        cell_id: &str,
        updates: impl IntoIterator<Item = (crate::VertexId, crate::VertexMetadata)>,
    ) -> Result<usize> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.import_vertex_metadata_batch(cell_id, updates).await
    }

    pub async fn set_edge_metadata(
        &self,
        cell_id: &str,
        edge_type: &str,
        src: crate::VertexId,
        dst: crate::VertexId,
        metadata: crate::EdgeMetadata,
    ) -> Result<bool> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .set_edge_metadata(cell_id, edge_type, src, dst, metadata)
            .await
    }

    pub async fn set_edge_metadata_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        updates: impl IntoIterator<Item = (crate::VertexId, crate::VertexId, crate::EdgeMetadata)>,
    ) -> Result<usize> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .set_edge_metadata_batch(cell_id, edge_type, updates)
            .await
    }

    pub async fn import_relationships_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        relationships: impl IntoIterator<Item = crate::RelationshipMutation>,
        idempotency_key: &str,
    ) -> Result<crate::RelationshipImportResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard
            .import_relationships_batch(cell_id, edge_type, relationships, idempotency_key)
            .await
    }

    pub async fn write_edge_mutations_batch(
        &self,
        cell_id: &str,
        mutations: impl IntoIterator<Item = crate::EdgeMutation>,
    ) -> Result<crate::EdgeMutationBatchResult> {
        let shard = self.shard(cell_id)?;
        self.ensure_local_writer(cell_id).await?;
        shard.write_edge_mutations_batch(cell_id, mutations).await
    }

    pub async fn out_neighbors_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        sources: impl IntoIterator<Item = VertexId>,
    ) -> Result<Vec<crate::NeighborBatchEntry>> {
        self.shard(cell_id)?
            .out_neighbors_batch(cell_id, edge_type, sources)
            .await
    }

    pub async fn in_neighbors_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        destinations: impl IntoIterator<Item = VertexId>,
    ) -> Result<Vec<crate::NeighborBatchEntry>> {
        self.shard(cell_id)?
            .in_neighbors_batch(cell_id, edge_type, destinations)
            .await
    }

    pub async fn edge_exists_batch(
        &self,
        cell_id: &str,
        edge_type: &str,
        edges: impl IntoIterator<Item = (VertexId, VertexId)>,
    ) -> Result<Vec<crate::EdgeExistenceBatchEntry>> {
        self.shard(cell_id)?
            .edge_exists_batch(cell_id, edge_type, edges)
            .await
    }

    pub async fn execute_query_statement(
        &self,
        context: crate::QueryContext,
        statement: crate::QueryStatement,
    ) -> Result<crate::QueryOutput> {
        self.ensure_query_scope(&context.scope)?;
        let shard = self.shard(&context.cell_id)?;
        let plan = shard.plan_query_statement(context, statement)?;
        if plan.is_write() {
            self.ensure_local_writer(&plan.cell_id).await?;
        }
        shard.execute_query_plan(plan).await
    }

    pub async fn execute_query_plan(&self, plan: crate::QueryPlan) -> Result<crate::QueryOutput> {
        let shard = self.shard(&plan.cell_id)?;
        if plan.is_write() {
            self.ensure_local_writer(&plan.cell_id).await?;
        }
        shard.execute_query_plan(plan).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher(
        &self,
        context: crate::QueryContext,
        query: &str,
    ) -> Result<crate::QueryOutput> {
        self.ensure_query_scope(&context.scope)?;
        let shard = self.shard(&context.cell_id)?;
        let requires_writer = matches!(
            crate::query::opencypher::classify_opencypher_query_access(query)?,
            crate::query::opencypher::OpenCypherQueryAccess::Write
        );
        if requires_writer {
            self.ensure_local_writer(&context.cell_id).await?;
        }
        shard.execute_cypher(context, query).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher_rows(
        &self,
        context: crate::QueryContext,
        query: &str,
    ) -> Result<crate::QueryResultSet> {
        self.ensure_query_scope(&context.scope)?;
        let shard = self.shard(&context.cell_id)?;
        shard.execute_cypher_rows(context, query).await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher_rows_page(
        &self,
        context: crate::QueryContext,
        query: &str,
        cursor: Option<crate::QueryCursorToken>,
        page_size: usize,
    ) -> Result<crate::QueryResultPage> {
        self.ensure_query_scope(&context.scope)?;
        let shard = self.shard(&context.cell_id)?;
        shard
            .execute_cypher_rows_page(context, query, cursor, page_size)
            .await
    }

    #[cfg(feature = "opencypher")]
    pub async fn execute_cypher_rows_many(
        &self,
        contexts: impl IntoIterator<Item = crate::QueryContext>,
        query: &str,
    ) -> Result<BTreeMap<String, crate::QueryResultSet>> {
        let mut result_sets = BTreeMap::new();
        for context in contexts {
            self.ensure_query_scope(&context.scope)?;
            validate_component("cell_id", &context.cell_id)?;
            if result_sets.contains_key(&context.cell_id) {
                return Err(GraphError::CorruptValue {
                    key: format!("query/cell/{}", context.cell_id),
                    reason: "duplicate cell in routed query request".to_string(),
                });
            }
            let cell_id = context.cell_id.clone();
            let shard = self.shard(&cell_id)?;
            let result_set = shard.execute_cypher_rows(context, query).await?;
            result_sets.insert(cell_id, result_set);
        }
        Ok(result_sets)
    }

    fn ensure_query_scope(&self, scope: &GraphScope) -> Result<()> {
        if scope == &self.scope {
            return Ok(());
        }
        Err(GraphError::GraphScopeMismatch {
            expected: self.scope.to_string(),
            actual: scope.to_string(),
        })
    }

    pub async fn close(&self) -> Result<()> {
        let mut failures = Vec::new();
        for (cell_id, shard) in &self.shards {
            if let Err(error) = shard.close().await {
                failures.push(format!("{cell_id}: {error}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(GraphError::CorruptValue {
                key: format!("routed-cluster/{}/close", self.scope),
                reason: failures.join("; "),
            })
        }
    }
}

#[cfg(feature = "query-transport")]
impl ScopedRoutedGraphCluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_path: impl Into<String>,
        root_namespace: NamespacePath,
        graph_id: GraphId,
        local_node_id: impl Into<String>,
        directory: ObjectStoreNodeDirectory,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
        options: GraphOpenOptions,
        memory: GraphMemoryConfig,
        max_open_scopes: usize,
    ) -> Result<Self> {
        let base_path = base_path.into();
        let local_node_id = local_node_id.into();
        validate_component("node_id", &local_node_id)?;
        if !directory.contains_node(&local_node_id)? {
            return Err(GraphError::CorruptValue {
                key: format!("directory/node/{local_node_id}"),
                reason: "local node is not present in the object-store node directory".to_string(),
            });
        }
        if placement.local_node_id() != local_node_id {
            return Err(GraphError::CorruptValue {
                key: format!("directory/node/{local_node_id}"),
                reason: format!(
                    "placement view belongs to node {}, not to this runtime's node",
                    placement.local_node_id()
                ),
            });
        }
        if max_open_scopes == 0 {
            return Err(GraphError::AdmissionRejected {
                operation: "max_open_scopes",
                actual: 0,
                limit: 1,
            });
        }
        let writer_registry = process_writer_registry(&object_store);
        Ok(Self {
            base_path: base_path.clone(),
            root_namespace: root_namespace.clone(),
            graph_id: graph_id.clone(),
            local_node_id,
            directory,
            placement,
            scope_directory: ObjectStoreGraphScopeDirectory::new(
                base_path,
                root_namespace,
                graph_id,
                Arc::clone(&object_store),
            ),
            object_store,
            writer_registry,
            options,
            memory,
            max_open_scopes,
            access_clock: AtomicU64::new(0),
            clusters: tokio::sync::Mutex::new(BTreeMap::new()),
            scope_open_gates: tokio::sync::Mutex::new(BTreeMap::new()),
            scope_closures: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            scope_capacity_gate: tokio::sync::Mutex::new(()),
            scope_open_reservations: AtomicUsize::new(0),
        })
    }

    /// Use explicit writer ownership for equivalent handles to one physical store.
    ///
    /// Call this immediately after construction when multiple runtimes wrap the
    /// same backend with distinct [`ObjectStore`] handles. Independent physical
    /// stores must use distinct registries, regardless of their display strings.
    pub fn with_writer_registry(mut self, writer_registry: Arc<ProcessWriterRegistry>) -> Self {
        self.writer_registry = writer_registry;
        self
    }

    pub fn root_scope(&self) -> GraphScope {
        GraphScope::new(self.root_namespace.clone(), self.graph_id.clone())
    }

    fn validate_scope(&self, scope: &GraphScope) -> Result<()> {
        if scope.graph_id == self.graph_id && scope.namespace.is_descendant_of(&self.root_namespace)
        {
            return Ok(());
        }
        Err(GraphError::GraphScopeMismatch {
            expected: format!(
                "{}/graphs/{} and descendants",
                self.root_namespace, self.graph_id
            ),
            actual: scope.to_string(),
        })
    }

    fn options_for_scope(&self, scope: &GraphScope) -> GraphOpenOptions {
        let mut options = self.options.clone();
        if let Some(root) = &options.cache.object_store_cache_dir {
            let mut scoped_root = root.join("scopes");
            for segment in scope.namespace.segments() {
                scoped_root.push(segment.as_str());
            }
            scoped_root.push("graphs");
            scoped_root.push(scope.graph_id.as_str());
            options.cache.object_store_cache_dir = Some(scoped_root);
        }
        if let Some(bytes) = options.cache.object_store_cache_bytes {
            options.cache.object_store_cache_bytes = Some((bytes / self.max_open_scopes).max(1));
        }
        options
    }

    pub(crate) async fn cluster_for_scope(
        &self,
        scope: &GraphScope,
    ) -> Result<Arc<RoutedGraphCluster>> {
        self.validate_scope(scope)?;
        loop {
            let access = self.access_clock.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(cluster) = self.cached_scope(scope, access).await {
                return Ok(cluster);
            }

            let scope_gate = {
                let mut gates = self.scope_open_gates.lock().await;
                gates.retain(|_, gate| gate.strong_count() > 0);
                if let Some(gate) = gates.get(scope).and_then(std::sync::Weak::upgrade) {
                    gate
                } else {
                    let gate = Arc::new(tokio::sync::Mutex::new(()));
                    gates.insert(scope.clone(), Arc::downgrade(&gate));
                    gate
                }
            };
            let scope_open_guard = scope_gate.lock().await;
            if let Some(cluster) = self.cached_scope(scope, access).await {
                return Ok(cluster);
            }

            // Reserve capacity without holding either async lock across
            // close/open I/O. A scope removed for eviction is first published
            // in `scope_closures`, so a concurrent reopen waits for its old
            // SlateDB writer to finish closing rather than fencing it.
            let capacity_guard = self.scope_capacity_gate.lock().await;
            if let Some(cluster) = self.cached_scope(scope, access).await {
                return Ok(cluster);
            }
            if let Some(mut closing) = self.scope_closure(scope) {
                drop(capacity_guard);
                drop(scope_open_guard);
                let _ = closing.wait_for(|closed| *closed).await;
                continue;
            }
            let (evicted, reservation) = {
                let mut clusters = self.clusters.lock().await;
                let occupied = clusters
                    .len()
                    .saturating_add(self.scope_open_reservations.load(Ordering::Acquire));
                let evicted = if occupied >= self.max_open_scopes {
                    let candidate = clusters
                        .iter()
                        .filter(|(_, entry)| Arc::strong_count(&entry.cluster) == 1)
                        .min_by_key(|(_, entry)| entry.last_used)
                        .map(|(scope, _)| scope.clone())
                        .ok_or(GraphError::AdmissionRejected {
                            operation: "open_graph_scopes",
                            actual: occupied.saturating_add(1) as u64,
                            limit: self.max_open_scopes as u64,
                        })?;
                    let entry = clusters
                        .remove(&candidate)
                        .expect("selected scoped cluster must still exist");
                    let close = ScopeCloseReservation::new(
                        Arc::clone(&self.scope_closures),
                        candidate.clone(),
                    );
                    Some((candidate, entry.cluster, close))
                } else {
                    None
                };
                self.scope_open_reservations.fetch_add(1, Ordering::AcqRel);
                (
                    evicted,
                    ScopedOpenReservation::new(&self.scope_open_reservations),
                )
            };
            drop(capacity_guard);
            if let Some((candidate, cluster, close)) = evicted {
                let cluster = Arc::try_unwrap(cluster).map_err(|_| GraphError::CorruptValue {
                    key: format!("scoped-cluster/{candidate}"),
                    reason: "idle scoped cluster acquired a concurrent owner during eviction"
                        .to_string(),
                })?;
                // Closing outlives the request that triggered eviction. If the
                // request is cancelled, this task still drains the old writer
                // and only then releases same-scope reopeners.
                tokio::spawn(async move {
                    let result = cluster.close().await;
                    drop(cluster);
                    close.release();
                    result
                })
                .await
                .map_err(|error| GraphError::CorruptValue {
                    key: format!("scoped-cluster/{candidate}"),
                    reason: format!("scope close task failed: {error}"),
                })??;
            }

            let cluster = Arc::new(
                RoutedGraphCluster::open_promotable_scoped_with_writer_registry(
                    self.base_path.clone(),
                    scope.clone(),
                    self.local_node_id.clone(),
                    self.directory.clone(),
                    // Cloned, never rebuilt: every scope's cluster and the routing
                    // provider must answer from one live set.
                    self.placement.clone(),
                    Arc::clone(&self.object_store),
                    Arc::clone(&self.writer_registry),
                    self.options_for_scope(scope),
                    self.memory.clone(),
                )
                .await?,
            );
            let _capacity_guard = self.scope_capacity_gate.lock().await;
            reservation.release();
            self.clusters.lock().await.insert(
                scope.clone(),
                ScopedRoutedClusterEntry {
                    cluster: Arc::clone(&cluster),
                    last_used: access,
                },
            );
            return Ok(cluster);
        }
    }

    fn scope_closure(&self, scope: &GraphScope) -> Option<tokio::sync::watch::Receiver<bool>> {
        self.scope_closures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(scope)
            .cloned()
    }

    async fn cached_scope(
        &self,
        scope: &GraphScope,
        access: u64,
    ) -> Option<Arc<RoutedGraphCluster>> {
        let mut clusters = self.clusters.lock().await;
        let entry = clusters.get_mut(scope)?;
        entry.last_used = access;
        Some(Arc::clone(&entry.cluster))
    }

    pub(crate) async fn cluster_for_scope_write(
        &self,
        scope: &GraphScope,
        cell_id: &str,
    ) -> Result<Arc<RoutedGraphCluster>> {
        let cluster = self.cluster_for_scope(scope).await?;
        cluster.ensure_local_writer(cell_id).await?;
        self.scope_directory.register(scope).await?;
        Ok(cluster)
    }

    /// Close idle writers that the current placement view assigns to another node.
    ///
    /// Placement can change without another request ever reaching the previous
    /// owner. Keeping that node's SlateDB writer open lets its background tasks
    /// race the replacement writer indefinitely. Removing the idle scoped
    /// cluster completes the handoff; a later read can reopen the scope, while a
    /// write still has to pass `ensure_local_writer` against the latest view.
    pub async fn retire_unowned_writers(&self) -> Result<usize> {
        let _capacity_guard = self.scope_capacity_gate.lock().await;
        let candidates = {
            let clusters = self.clusters.lock().await;
            clusters
                .iter()
                .filter(|(_, entry)| {
                    Arc::strong_count(&entry.cluster) == 1 && entry.cluster.holds_unowned_writer()
                })
                .map(|(scope, _)| scope.clone())
                .collect::<Vec<_>>()
        };

        let mut retiring = Vec::with_capacity(candidates.len());
        {
            let mut clusters = self.clusters.lock().await;
            for scope in candidates {
                let Some(entry) = clusters.get(&scope) else {
                    continue;
                };
                if Arc::strong_count(&entry.cluster) != 1 || !entry.cluster.holds_unowned_writer() {
                    continue;
                }
                let Some(entry) = clusters.remove(&scope) else {
                    continue;
                };
                let close =
                    ScopeCloseReservation::new(Arc::clone(&self.scope_closures), scope.clone());
                match Arc::try_unwrap(entry.cluster) {
                    Ok(cluster) => retiring.push((scope, cluster, close)),
                    Err(cluster) => {
                        close.release();
                        clusters.insert(
                            scope,
                            ScopedRoutedClusterEntry {
                                cluster,
                                last_used: entry.last_used,
                            },
                        );
                    }
                }
            }
        }
        drop(_capacity_guard);

        let mut retired = 0;
        let mut failures = Vec::new();
        for (scope, cluster, close) in retiring {
            let result = cluster.close().await;
            drop(cluster);
            close.release();
            match result {
                Ok(()) => retired += 1,
                Err(error) => failures.push(format!("{scope}: {error}")),
            }
        }
        if failures.is_empty() {
            Ok(retired)
        } else {
            Err(GraphError::CorruptValue {
                key: "scoped-clusters/placement-retire".to_string(),
                reason: failures.join("; "),
            })
        }
    }

    pub async fn loaded_scopes(&self) -> Vec<GraphScope> {
        self.clusters.lock().await.keys().cloned().collect()
    }

    /// Non-owning handles to every open scope's cluster.
    ///
    /// `Weak`, and the return type is the whole point. Eviction's candidate
    /// filter is `Arc::strong_count(&entry.cluster) == 1` (see
    /// `cluster_for_scope`), so a strong clone a caller keeps makes that scope
    /// un-evictable for as long as it keeps it — and at `max_open_scopes` a
    /// query for a not-yet-open scope then fails with `AdmissionRejected`
    /// rather than waiting. The index-discovery loop iterated a `Vec` of strong
    /// clones across per-cell object-store I/O, which turned a background
    /// interval task into a client-visible error rate.
    ///
    /// Callers must upgrade **one at a time** and drop each `Arc` before
    /// upgrading the next. An upgrade returning `None` means the scope was
    /// evicted since the snapshot, which is a skip, not an error.
    ///
    /// This shrinks the un-evictable window rather than eliminating it: while a
    /// caller holds one upgraded `Arc`, that one scope is still un-evictable.
    /// Closing the window entirely needs an in-use count separate from the
    /// `Arc` count, which is more machinery than the residual risk justifies.
    pub async fn loaded_clusters(&self) -> Vec<std::sync::Weak<RoutedGraphCluster>> {
        self.clusters
            .lock()
            .await
            .values()
            .map(|entry| Arc::downgrade(&entry.cluster))
            .collect()
    }

    /// Per-scope shard runtime metrics, collected one scope at a time.
    ///
    /// The `clusters` mutex is taken only to snapshot the map, and what is
    /// snapshotted is `Weak` rather than `Arc` deliberately. Eviction's
    /// candidate filter is `Arc::strong_count(&entry.cluster) == 1`, so
    /// retaining a strong clone of every open scope for the length of the
    /// collection — which is what this did — made *every* scope un-evictable
    /// for that whole span. At `max_open_scopes` a query for a not-yet-open
    /// scope then found no eviction candidate and got
    /// `AdmissionRejected { operation: "open_graph_scopes", .. }` straight back:
    /// a hard client error with no retry and no wait, produced by a scrape.
    ///
    /// Upgrading one at a time and dropping each `Arc` before the next
    /// **shrinks** that window from "every open scope, for the whole
    /// collection" to "one scope, for one cluster's worth of cache-lock reads".
    /// It does not eliminate it — the scope currently being read is still
    /// un-evictable. Eliminating it needs an explicit in-use count separate
    /// from the `Arc` count, which is more machinery than the residual risk
    /// justifies at `max_open_scopes = 8` and a 60s export interval.
    ///
    /// A `Weak` that fails to upgrade means the scope was evicted between the
    /// snapshot and its turn. That is a skip, not an error: the metrics are
    /// advisory and a closed scope has nothing left to report.
    pub async fn local_shard_runtime_metrics(&self) -> Vec<ScopedGraphShardRuntimeMetrics> {
        let clusters = self
            .clusters
            .lock()
            .await
            .iter()
            .map(|(scope, entry)| (scope.clone(), Arc::downgrade(&entry.cluster)))
            .collect::<Vec<_>>();
        let mut metrics = Vec::new();
        for (scope, cluster) in clusters {
            let Some(cluster) = cluster.upgrade() else {
                continue;
            };
            metrics.extend(
                cluster
                    .local_shard_runtime_metrics()
                    .await
                    .into_iter()
                    .map(|shard| ScopedGraphShardRuntimeMetrics {
                        scope: scope.clone(),
                        shard,
                    }),
            );
            // `cluster` drops here, before the next upgrade, so at most one
            // scope is pinned at a time.
        }
        metrics
    }

    pub async fn close(&self) -> Result<()> {
        let entries = std::mem::take(&mut *self.clusters.lock().await);
        let mut failures = Vec::new();
        for (scope, entry) in entries {
            match Arc::try_unwrap(entry.cluster) {
                Ok(cluster) => {
                    if let Err(error) = cluster.close().await {
                        failures.push(format!("{scope}: {error}"));
                    }
                }
                Err(_) => failures.push(format!("{scope}: scoped cluster is still in use")),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(GraphError::CorruptValue {
                key: "runtime/scoped_clusters/close".to_string(),
                reason: failures.join("; "),
            })
        }
    }
}

#[cfg(feature = "query-transport")]
struct ScopedOpenReservation<'a> {
    counter: &'a AtomicUsize,
    active: bool,
}

#[cfg(feature = "query-transport")]
impl<'a> ScopedOpenReservation<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        Self {
            counter,
            active: true,
        }
    }

    fn release(mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
        self.active = false;
    }
}

#[cfg(feature = "query-transport")]
impl Drop for ScopedOpenReservation<'_> {
    fn drop(&mut self) {
        if self.active {
            self.counter.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(feature = "query-transport")]
struct ScopeCloseReservation {
    closures: Arc<std::sync::Mutex<BTreeMap<GraphScope, tokio::sync::watch::Receiver<bool>>>>,
    scope: GraphScope,
    completed: tokio::sync::watch::Sender<bool>,
    active: bool,
}

#[cfg(feature = "query-transport")]
impl ScopeCloseReservation {
    fn new(
        closures: Arc<std::sync::Mutex<BTreeMap<GraphScope, tokio::sync::watch::Receiver<bool>>>>,
        scope: GraphScope,
    ) -> Self {
        let (completed, receiver) = tokio::sync::watch::channel(false);
        closures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(scope.clone(), receiver);
        Self {
            closures,
            scope,
            completed,
            active: true,
        }
    }

    fn release(mut self) {
        self.finish();
    }

    fn finish(&mut self) {
        if !self.active {
            return;
        }
        let _ = self.completed.send(true);
        self.closures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.scope);
        self.active = false;
    }
}

#[cfg(feature = "query-transport")]
impl Drop for ScopeCloseReservation {
    fn drop(&mut self) {
        self.finish();
    }
}

async fn close_shards_best_effort(shards: BTreeMap<String, GraphShard>) {
    for shard in shards.into_values() {
        let _ = shard.close().await;
    }
}

async fn close_routed_shards_best_effort(shards: BTreeMap<String, Arc<GraphShard>>) {
    for shard in shards.into_values() {
        let _ = shard.close().await;
    }
}

#[cfg(all(test, feature = "query-transport"))]
mod scoped_cluster_tests {
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use slatedb::object_store::prefix::PrefixStore;
    use turbolay_placement::heartbeat::{self, Heartbeat};

    use super::*;
    use crate::NamespaceId;

    #[test]
    fn scoped_clusters_partition_the_local_slate_cache_budget() {
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let graph_id = GraphId::new("hydradb").unwrap();
        let runtime = ScopedRoutedGraphCluster::new(
            "graph/data",
            root.clone(),
            graph_id.clone(),
            "node-a",
            ObjectStoreNodeDirectory::new(["cell-0"], ["node-a"]).unwrap(),
            PlacementView::new("node-a", ["node-a"], PlacementConfig::default()).unwrap(),
            Arc::new(InMemory::new()),
            GraphOpenOptions {
                cache: crate::GraphCacheConfig::disk_cache("/cache", 800),
                ..GraphOpenOptions::default()
            },
            GraphMemoryConfig::default(),
            8,
        )
        .unwrap();
        let first = GraphScope::new(
            root.child(NamespaceId::new("tenant-a").unwrap()).unwrap(),
            graph_id.clone(),
        );
        let second = GraphScope::new(
            runtime
                .root_scope()
                .namespace
                .child(NamespaceId::new("tenant-b").unwrap())
                .unwrap(),
            graph_id,
        );

        let first_options = runtime.options_for_scope(&first);
        let second_options = runtime.options_for_scope(&second);
        assert_eq!(first_options.cache.object_store_cache_bytes, Some(100));
        assert_eq!(second_options.cache.object_store_cache_bytes, Some(100));
        assert_ne!(
            first_options.cache.object_store_cache_dir,
            second_options.cache.object_store_cache_dir
        );
        assert!(first_options
            .cache
            .object_store_cache_dir
            .unwrap()
            .ends_with("production/tenant-a/graphs/hydradb"));
    }

    /// A runtime whose scope map is exactly `max_open_scopes` deep, so the next
    /// miss must evict, and eviction only considers entries nobody else holds.
    fn runtime_at_capacity(base_path: &str, max_open_scopes: usize) -> ScopedRoutedGraphCluster {
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        ScopedRoutedGraphCluster::new(
            base_path,
            root,
            GraphId::new("hydradb").unwrap(),
            "node-a",
            ObjectStoreNodeDirectory::new(["cell-0"], ["node-a"]).unwrap(),
            PlacementView::new("node-a", ["node-a"], PlacementConfig::default()).unwrap(),
            Arc::new(InMemory::new()),
            GraphOpenOptions::default(),
            GraphMemoryConfig::default(),
            max_open_scopes,
        )
        .unwrap()
    }

    fn tenant_scope(runtime: &ScopedRoutedGraphCluster, tenant: &str) -> GraphScope {
        GraphScope::new(
            runtime
                .root_scope()
                .namespace
                .child(NamespaceId::new(tenant).unwrap())
                .unwrap(),
            runtime.root_scope().graph_id,
        )
    }

    /// Fill the map to `max_open_scopes`, returning the scopes in map order.
    ///
    /// The returned `Arc`s are dropped, so every entry is back to
    /// `strong_count == 1` and therefore an eviction candidate. Anything that
    /// makes them un-evictable afterwards is the thing under test.
    async fn open_scopes_to_capacity(
        runtime: &ScopedRoutedGraphCluster,
        tenants: &[&str],
    ) -> Vec<GraphScope> {
        let mut scopes = Vec::new();
        for tenant in tenants {
            let scope = tenant_scope(runtime, tenant);
            drop(
                runtime
                    .cluster_for_scope(&scope)
                    .await
                    .expect("scope opens"),
            );
            scopes.push(scope);
        }
        assert_eq!(runtime.loaded_scopes().await.len(), tenants.len());
        scopes
    }

    fn scoped_edge(key: &str) -> crate::EdgeMutation {
        crate::EdgeMutation {
            cell_id: "cell-0".to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: key.to_string(),
        }
    }

    #[tokio::test]
    async fn evicting_a_scope_drains_its_writer_with_an_active_reader_snapshot() {
        let runtime = runtime_at_capacity("graph/active-reader-eviction", 1);
        let first_scope = tenant_scope(&runtime, "tenant-first");
        let first = runtime
            .cluster_for_scope_write(&first_scope, "cell-0")
            .await
            .expect("the first scope opens for writes");
        first
            .write_edge(scoped_edge("first-write"))
            .await
            .expect("the first writer commits");

        let shard = first.shard("cell-0").expect("the first shard exists");
        let old_writer = shard.db.writer().expect("the first writer is installed");
        let reader = shard
            .db
            .open_reader()
            .await
            .expect("the reader opens")
            .expect("the initialized scope has a reader");
        let snapshot = reader.snapshot().await.expect("the reader snapshot opens");
        drop(first);

        let second_scope = tenant_scope(&runtime, "tenant-second");
        drop(
            runtime
                .cluster_for_scope(&second_scope)
                .await
                .expect("an active reader snapshot must not prevent writer retirement"),
        );
        assert_eq!(
            old_writer.status().close_reason,
            Some(slatedb::CloseReason::Clean),
            "eviction must cleanly close the old writer before another scope can open"
        );

        drop(snapshot);
        drop(reader);
        let reopened = runtime
            .cluster_for_scope_write(&first_scope, "cell-0")
            .await
            .expect("the evicted scope reopens for writes");
        reopened
            .write_edge(scoped_edge("reopened-write"))
            .await
            .expect("the replacement writer commits without fencing the old handle");
        drop(reopened);
        runtime.close().await.expect("the runtime drains");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn repeated_scope_eviction_reopens_one_process_writer() {
        let runtime = runtime_at_capacity("graph/repeated-writer-eviction", 2);
        let hot = tenant_scope(&runtime, "tenant-hot");

        for round in 0..8 {
            let hot_clusters = futures::future::join_all(
                (0..16).map(|_| runtime.cluster_for_scope_write(&hot, "cell-0")),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .expect("concurrent hot-scope acquisitions succeed");
            let first = hot_clusters.first().expect("the hot scope was returned");
            assert!(hot_clusters
                .iter()
                .all(|cluster| Arc::ptr_eq(first, cluster)));
            first
                .write_edge(scoped_edge(&format!("hot-write-{round}")))
                .await
                .expect("the hot writer commits without fencing itself");
            drop(hot_clusters);

            for suffix in ["a", "b"] {
                let cold = tenant_scope(&runtime, &format!("tenant-cold-{round}-{suffix}"));
                drop(
                    runtime
                        .cluster_for_scope(&cold)
                        .await
                        .expect("cold scope opens and advances eviction"),
                );
            }
        }

        runtime.close().await.expect("the runtime drains");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_scope_churn_does_not_fence_the_hot_writer() {
        let runtime = Arc::new(runtime_at_capacity("graph/concurrent-writer-churn", 4));
        let hot = tenant_scope(&runtime, "tenant-hot");

        let writers = (0..16).map(|worker| {
            let runtime = Arc::clone(&runtime);
            let hot = hot.clone();
            tokio::spawn(async move {
                for round in 0..32 {
                    loop {
                        match runtime.cluster_for_scope_write(&hot, "cell-0").await {
                            Ok(cluster) => {
                                cluster
                                    .write_edge(scoped_edge(&format!(
                                        "hot-churn-write-{worker}-{round}"
                                    )))
                                    .await
                                    .expect("scope churn must not fence the hot writer");
                                break;
                            }
                            Err(GraphError::AdmissionRejected { .. }) => {
                                tokio::task::yield_now().await;
                            }
                            Err(error) => panic!("hot writer acquisition failed: {error}"),
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
        });

        let churners = (0..8).map(|worker| {
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                for round in 0..64 {
                    let scope =
                        tenant_scope(&runtime, &format!("tenant-cold-{worker}-{}", round % 16));
                    loop {
                        match runtime.cluster_for_scope(&scope).await {
                            Ok(cluster) => {
                                drop(cluster);
                                break;
                            }
                            Err(GraphError::AdmissionRejected { .. }) => {
                                tokio::task::yield_now().await;
                            }
                            Err(error) => panic!("cold scope acquisition failed: {error}"),
                        }
                    }
                    tokio::task::yield_now().await;
                }
            })
        });

        for writer in writers {
            writer.await.expect("writer task completes");
        }
        for churner in churners {
            churner.await.expect("scope churn task completes");
        }
        runtime.close().await.expect("the runtime drains");
    }

    #[tokio::test]
    async fn placement_handoff_retires_the_previous_nodes_writer() {
        let base_path = "graph/placement-handoff";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = PlacementConfig {
            heartbeat_interval: Duration::from_millis(2),
            heartbeat_timeout: Duration::from_millis(10),
        };
        let placement = PlacementView::new("node-a", ["node-a", "node-b"], config)
            .expect("the placement view is valid");
        let root = NamespacePath::root(NamespaceId::new("production").unwrap());
        let graph_id = GraphId::new("hydradb").unwrap();
        let runtime = ScopedRoutedGraphCluster::new(
            base_path,
            root.clone(),
            graph_id.clone(),
            "node-a",
            ObjectStoreNodeDirectory::new(["cell-0"], ["node-a", "node-b"]).unwrap(),
            placement.clone(),
            Arc::clone(&store),
            GraphOpenOptions::default(),
            GraphMemoryConfig::default(),
            4,
        )
        .unwrap();
        let scope = (0..1_000)
            .map(|index| tenant_scope(&runtime, &format!("tenant-{index}")))
            .find(|scope| {
                turbolay_placement::hash::owner(&scope.to_string(), "cell-0", &["node-a", "node-b"])
                    == Some("node-b")
            })
            .expect("one test scope moves from node-a to node-b");

        tokio::time::sleep(config.heartbeat_timeout * 2).await;
        let _ = placement
            .refresh(store.as_ref(), &Path::from(base_path))
            .await;
        assert_eq!(
            placement.ownership(&scope.to_string(), "cell-0"),
            CellOwnership::Local,
            "without a peer heartbeat node-a owns the scope"
        );
        let cluster = runtime
            .cluster_for_scope_write(&scope, "cell-0")
            .await
            .expect("node-a opens the initial writer");
        cluster
            .write_edge(scoped_edge("before-handoff"))
            .await
            .expect("the initial owner commits");
        let old_writer = cluster.shard("cell-0").unwrap().db.writer().unwrap();
        drop(cluster);

        let now = Utc::now();
        heartbeat::put_heartbeat(
            store.as_ref(),
            &Path::from(base_path),
            &Heartbeat::new("node-b", "test", now, now, vec!["cell-0".to_string()]),
        )
        .await
        .expect("node-b publishes its heartbeat");
        let _ = placement
            .refresh(store.as_ref(), &Path::from(base_path))
            .await;
        assert!(matches!(
            placement.ownership(&scope.to_string(), "cell-0"),
            CellOwnership::Remote { ref node_id } if node_id == "node-b"
        ));

        assert_eq!(runtime.retire_unowned_writers().await.unwrap(), 1);
        assert_eq!(
            old_writer.status().close_reason,
            Some(slatedb::CloseReason::Clean),
            "the previous owner must close before its background tasks can fence the replacement"
        );
        assert!(runtime.loaded_scopes().await.is_empty());

        let replacement_placement = PlacementView::new("node-b", ["node-a", "node-b"], config)
            .expect("the replacement placement view is valid");
        let _ = replacement_placement
            .refresh(store.as_ref(), &Path::from(base_path))
            .await;
        let replacement = ScopedRoutedGraphCluster::new(
            base_path,
            root,
            graph_id,
            "node-b",
            ObjectStoreNodeDirectory::new(["cell-0"], ["node-a", "node-b"]).unwrap(),
            replacement_placement,
            Arc::clone(&store),
            GraphOpenOptions::default(),
            GraphMemoryConfig::default(),
            4,
        )
        .unwrap();
        let replacement_cluster = replacement
            .cluster_for_scope_write(&scope, "cell-0")
            .await
            .expect("the replacement owner opens after the old writer closes");
        replacement_cluster
            .write_edge(scoped_edge("after-handoff"))
            .await
            .expect("the replacement owner commits without another fence");
        drop(replacement_cluster);

        runtime.close().await.expect("the runtime drains");
        replacement.close().await.expect("the replacement drains");
    }

    #[tokio::test]
    async fn placement_reconciliation_retires_every_selected_scope() {
        let base_path = "graph/placement-batch-handoff";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = PlacementConfig {
            heartbeat_interval: Duration::from_millis(2),
            heartbeat_timeout: Duration::from_millis(10),
        };
        let placement = PlacementView::new("node-a", ["node-a", "node-b"], config)
            .expect("the placement view is valid");
        let runtime = ScopedRoutedGraphCluster::new(
            base_path,
            NamespacePath::root(NamespaceId::new("production").unwrap()),
            GraphId::new("hydradb").unwrap(),
            "node-a",
            ObjectStoreNodeDirectory::new(["cell-0"], ["node-a", "node-b"]).unwrap(),
            placement.clone(),
            Arc::clone(&store),
            GraphOpenOptions::default(),
            GraphMemoryConfig::default(),
            4,
        )
        .unwrap();
        let scopes = (0..2_000)
            .map(|index| tenant_scope(&runtime, &format!("tenant-{index}")))
            .filter(|scope| {
                turbolay_placement::hash::owner(&scope.to_string(), "cell-0", &["node-a", "node-b"])
                    == Some("node-b")
            })
            .take(2)
            .collect::<Vec<_>>();
        assert_eq!(scopes.len(), 2);

        tokio::time::sleep(config.heartbeat_timeout * 2).await;
        let _ = placement
            .refresh(store.as_ref(), &Path::from(base_path))
            .await;
        let mut writers = Vec::new();
        for (index, scope) in scopes.iter().enumerate() {
            let cluster = runtime
                .cluster_for_scope_write(scope, "cell-0")
                .await
                .expect("node-a opens the initial writer");
            cluster
                .write_edge(scoped_edge(&format!("before-batch-handoff-{index}")))
                .await
                .expect("the initial owner commits");
            writers.push(cluster.shard("cell-0").unwrap().db.writer().unwrap());
        }

        let now = Utc::now();
        heartbeat::put_heartbeat(
            store.as_ref(),
            &Path::from(base_path),
            &Heartbeat::new("node-b", "test", now, now, vec!["cell-0".to_string()]),
        )
        .await
        .expect("node-b publishes its heartbeat");
        let _ = placement
            .refresh(store.as_ref(), &Path::from(base_path))
            .await;

        assert_eq!(runtime.retire_unowned_writers().await.unwrap(), 2);
        for writer in writers {
            assert_eq!(
                writer.status().close_reason,
                Some(slatedb::CloseReason::Clean),
                "every selected stale writer must be explicitly closed"
            );
        }
        assert!(runtime.loaded_scopes().await.is_empty());
        runtime.close().await.expect("the runtime drains");
    }

    #[tokio::test]
    async fn concurrent_writes_to_one_scope_share_one_cluster_and_writer() {
        let runtime = runtime_at_capacity("graph/concurrent-scope-writes", 16);
        let scope = tenant_scope(&runtime, "tenant-hot");

        let clusters = futures::future::join_all(
            (0..64).map(|_| runtime.cluster_for_scope_write(&scope, "cell-0")),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .expect("concurrent write acquisitions succeed");

        let first = clusters.first().expect("at least one cluster was returned");
        assert!(
            clusters.iter().all(|cluster| Arc::ptr_eq(first, cluster)),
            "one process must never construct two routed clusters for the same scope"
        );
        let epoch = first
            .shard("cell-0")
            .expect("the shard exists")
            .db
            .writer_epoch()
            .expect("the shared writer is installed");
        assert!(clusters.iter().all(|cluster| {
            cluster
                .shard("cell-0")
                .ok()
                .and_then(|shard| shard.db.writer_epoch())
                == Some(epoch)
        }));

        drop(clusters);
        runtime.close().await.expect("the runtime drains");
    }

    #[tokio::test]
    async fn duplicate_routed_runtimes_share_one_process_writer() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let make_runtime = || {
            let root = NamespacePath::root(NamespaceId::new("production").unwrap());
            ScopedRoutedGraphCluster::new(
                "graph/duplicate-runtime",
                root,
                GraphId::new("hydradb").unwrap(),
                "node-a",
                ObjectStoreNodeDirectory::new(["cell-0"], ["node-a"]).unwrap(),
                PlacementView::new("node-a", ["node-a"], PlacementConfig::default()).unwrap(),
                Arc::clone(&object_store),
                GraphOpenOptions::default(),
                GraphMemoryConfig::default(),
                16,
            )
            .unwrap()
        };
        let first_runtime = make_runtime();
        let second_runtime = make_runtime();
        let scope = tenant_scope(&first_runtime, "tenant-hot");

        let first = first_runtime
            .cluster_for_scope_write(&scope, "cell-0")
            .await
            .expect("the first runtime acquires the writer");
        let first_writer = first
            .shard("cell-0")
            .expect("the first shard exists")
            .db
            .writer()
            .expect("the first writer is installed");
        let second = second_runtime
            .cluster_for_scope_write(&scope, "cell-0")
            .await
            .expect("the duplicate runtime reuses the writer");

        first_writer
            .refresh_manifest()
            .await
            .expect("the duplicate runtime must not fence the first handle");
        assert_eq!(
            first
                .shard("cell-0")
                .ok()
                .and_then(|shard| shard.db.writer_epoch()),
            second
                .shard("cell-0")
                .ok()
                .and_then(|shard| shard.db.writer_epoch())
        );

        drop(first);
        first_runtime
            .close()
            .await
            .expect("closing one runtime preserves the shared writer");
        second
            .write_edge(scoped_edge("second-runtime-write"))
            .await
            .expect("the remaining runtime can still write");
        drop(second);
        second_runtime
            .close()
            .await
            .expect("the final runtime drains");
    }

    #[tokio::test]
    async fn equivalent_object_store_handles_share_one_process_writer() {
        let physical_store = Arc::new(InMemory::new());
        let first_store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(
            Arc::clone(&physical_store),
            "shared-physical-store",
        ));
        let second_store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(
            Arc::clone(&physical_store),
            "shared-physical-store",
        ));
        assert_ne!(
            Arc::as_ptr(&first_store) as *const () as usize,
            Arc::as_ptr(&second_store) as *const () as usize,
            "the regression requires distinct object-store wrappers"
        );
        assert_eq!(first_store.to_string(), second_store.to_string());

        let writer_registry = Arc::new(ProcessWriterRegistry::new());
        let make_runtime = |object_store| {
            let root = NamespacePath::root(NamespaceId::new("production").unwrap());
            ScopedRoutedGraphCluster::new(
                "graph/equivalent-object-store-handles",
                root,
                GraphId::new("hydradb").unwrap(),
                "node-a",
                ObjectStoreNodeDirectory::new(["cell-0"], ["node-a"]).unwrap(),
                PlacementView::new("node-a", ["node-a"], PlacementConfig::default()).unwrap(),
                object_store,
                GraphOpenOptions::default(),
                GraphMemoryConfig::default(),
                16,
            )
            .unwrap()
            .with_writer_registry(Arc::clone(&writer_registry))
        };
        let first_runtime = make_runtime(first_store);
        let second_runtime = make_runtime(second_store);
        let scope = tenant_scope(&first_runtime, "tenant-hot");

        let first = first_runtime
            .cluster_for_scope_write(&scope, "cell-0")
            .await
            .expect("the first handle acquires the writer");
        let first_writer = first
            .shard("cell-0")
            .expect("the first shard exists")
            .db
            .writer()
            .expect("the first writer is installed");
        let second = second_runtime
            .cluster_for_scope_write(&scope, "cell-0")
            .await
            .expect("the equivalent handle reuses the writer");

        first_writer
            .refresh_manifest()
            .await
            .expect("an equivalent store handle must not fence the first writer");
        assert_eq!(
            first
                .shard("cell-0")
                .ok()
                .and_then(|shard| shard.db.writer_epoch()),
            second
                .shard("cell-0")
                .ok()
                .and_then(|shard| shard.db.writer_epoch())
        );

        drop(first);
        first_runtime
            .close()
            .await
            .expect("the first runtime closes");
        drop(second);
        second_runtime
            .close()
            .await
            .expect("the second runtime closes");
    }

    #[tokio::test]
    async fn equal_wrapper_displays_do_not_share_across_independent_stores() {
        let first_store: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(Arc::new(InMemory::new()), "equal-prefix"));
        let second_store: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(Arc::new(InMemory::new()), "equal-prefix"));
        assert_eq!(first_store.to_string(), second_store.to_string());

        let make_runtime = |object_store| {
            let root = NamespacePath::root(NamespaceId::new("production").unwrap());
            ScopedRoutedGraphCluster::new(
                "graph/independent-object-stores",
                root,
                GraphId::new("hydradb").unwrap(),
                "node-a",
                ObjectStoreNodeDirectory::new(["cell-0"], ["node-a"]).unwrap(),
                PlacementView::new("node-a", ["node-a"], PlacementConfig::default()).unwrap(),
                object_store,
                GraphOpenOptions::default(),
                GraphMemoryConfig::default(),
                16,
            )
            .unwrap()
        };
        let first_runtime = make_runtime(first_store);
        let second_runtime = make_runtime(second_store);
        assert!(!Arc::ptr_eq(
            &first_runtime.writer_registry,
            &second_runtime.writer_registry
        ));
        let scope = tenant_scope(&first_runtime, "tenant-hot");

        let first = first_runtime
            .cluster_for_scope_write(&scope, "cell-0")
            .await
            .expect("the first physical store acquires its writer");
        let second = second_runtime
            .cluster_for_scope_write(&scope, "cell-0")
            .await
            .expect("the independent physical store acquires its own writer");
        first
            .write_edge(scoped_edge("first-store-write"))
            .await
            .expect("the first store remains writable");
        second
            .write_edge(scoped_edge("second-store-write"))
            .await
            .expect("the second store remains writable");

        drop(first);
        first_runtime.close().await.expect("close first runtime");
        drop(second);
        second_runtime.close().await.expect("close second runtime");
    }

    #[tokio::test]
    async fn duplicate_routed_runtimes_reject_mismatched_fence_backoff() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let make_runtime = |fence_backoff_interval| {
            let root = NamespacePath::root(NamespaceId::new("production").unwrap());
            ScopedRoutedGraphCluster::new(
                "graph/duplicate-runtime-config",
                root,
                GraphId::new("hydradb").unwrap(),
                "node-a",
                ObjectStoreNodeDirectory::new(["cell-0"], ["node-a"]).unwrap(),
                PlacementView::new("node-a", ["node-a"], PlacementConfig::default()).unwrap(),
                Arc::clone(&object_store),
                GraphOpenOptions {
                    fence_backoff_interval,
                    ..GraphOpenOptions::default()
                },
                GraphMemoryConfig::default(),
                16,
            )
            .unwrap()
        };
        let first_interval = Duration::from_secs(5);
        let second_interval = Duration::from_secs(9);
        let first_runtime = make_runtime(first_interval);
        let second_runtime = make_runtime(second_interval);
        let scope = tenant_scope(&first_runtime, "tenant-hot");

        let first = first_runtime
            .cluster_for_scope_write(&scope, "cell-0")
            .await
            .expect("the first runtime acquires the writer");
        let error = match second_runtime
            .cluster_for_scope_write(&scope, "cell-0")
            .await
        {
            Ok(_) => panic!("a duplicate runtime must not inherit another fencing interval"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            GraphError::RoutedWriterConfigMismatch {
                existing,
                requested,
                ..
            } if existing == first_interval && requested == second_interval
        ));

        drop(first);
        first_runtime
            .close()
            .await
            .expect("the first runtime drains");
        second_runtime
            .close()
            .await
            .expect("the rejected runtime drains");
    }

    #[tokio::test]
    async fn a_cold_scope_open_does_not_lock_out_cached_scopes() {
        let runtime = runtime_at_capacity("graph/cold-open", 2);
        let hot = tenant_scope(&runtime, "tenant-hot");
        drop(
            runtime
                .cluster_for_scope(&hot)
                .await
                .expect("hot scope opens"),
        );

        let capacity_guard = runtime.scope_capacity_gate.lock().await;
        let cold = tenant_scope(&runtime, "tenant-cold");
        let mut cold_open = Box::pin(runtime.cluster_for_scope(&cold));
        assert!(
            futures::poll!(cold_open.as_mut()).is_pending(),
            "the held capacity gate should park the cold scope open"
        );

        let cached = runtime.cluster_for_scope(&hot).await;
        assert!(
            cached.is_ok(),
            "a cold scope open must not hold the cache-map lock needed by hot scopes"
        );
        drop(cached);

        drop(capacity_guard);
        drop(
            cold_open
                .await
                .expect("cold scope opens after capacity resumes"),
        );
        runtime.close().await.expect("the runtime drains");
    }

    #[tokio::test]
    async fn cancelling_a_cold_scope_open_does_not_poison_that_scope() {
        let runtime = runtime_at_capacity("graph/cancelled-open", 2);
        let capacity_guard = runtime.scope_capacity_gate.lock().await;
        let scope = tenant_scope(&runtime, "tenant-cancelled");
        let mut cancelled_open = Box::pin(runtime.cluster_for_scope(&scope));
        assert!(
            futures::poll!(cancelled_open.as_mut()).is_pending(),
            "the held capacity gate should park the first open"
        );
        drop(cancelled_open);
        drop(capacity_guard);

        drop(
            runtime
                .cluster_for_scope(&scope)
                .await
                .expect("a later request can reopen the cancelled scope"),
        );
        runtime.close().await.expect("the runtime drains");
    }

    #[tokio::test]
    async fn reopening_an_evicted_scope_waits_for_its_writer_to_close() {
        let runtime = runtime_at_capacity("graph/closing-scope", 2);
        let scope = tenant_scope(&runtime, "tenant-closing");
        let close = ScopeCloseReservation::new(Arc::clone(&runtime.scope_closures), scope.clone());

        let mut reopen = Box::pin(runtime.cluster_for_scope(&scope));
        assert!(
            futures::poll!(reopen.as_mut()).is_pending(),
            "a scope must not reopen while its previous SlateDB writer is closing"
        );
        assert!(runtime.loaded_scopes().await.is_empty());

        close.release();
        drop(
            reopen
                .await
                .expect("the scope reopens after its old writer has closed"),
        );
        runtime.close().await.expect("the runtime drains");
    }

    #[tokio::test]
    async fn cancelled_scope_close_releases_waiting_reopeners() {
        let runtime = runtime_at_capacity("graph/cancelled-close", 2);
        let scope = tenant_scope(&runtime, "tenant-cancelled-close");
        let close = ScopeCloseReservation::new(Arc::clone(&runtime.scope_closures), scope.clone());

        let mut reopen = Box::pin(runtime.cluster_for_scope(&scope));
        assert!(futures::poll!(reopen.as_mut()).is_pending());
        drop(close);

        drop(
            reopen
                .await
                .expect("dropping the close reservation wakes the reopener"),
        );
        runtime.close().await.expect("the runtime drains");
    }

    /// A metrics collection in flight must not make every open scope
    /// un-evictable.
    ///
    /// The collector snapshots the `clusters` map and then reads each cluster's
    /// cache gauges outside the map lock. Snapshotting `Arc`s rather than
    /// `Weak`s meant it held a strong reference to *every* open scope for the
    /// whole collection, and eviction's candidate filter is
    /// `Arc::strong_count(&entry.cluster) == 1` — so at `max_open_scopes` a
    /// query for a not-yet-open scope found no candidate and got
    /// `AdmissionRejected` back. A scrape moving a client-visible failure rate.
    ///
    /// Driven by hand in the style of `08e78df`: hold one shard's first cache
    /// mutex, poll the collection exactly once so it is suspended partway
    /// through the *first* cluster, and then open a new scope. No second task
    /// and no timer, so it cannot flake.
    #[tokio::test]
    async fn a_parked_metrics_collection_does_not_block_opening_a_new_scope() {
        let runtime = runtime_at_capacity("graph/parked-metrics", 3);
        let scopes = open_scopes_to_capacity(&runtime, &["tenant-a", "tenant-b", "tenant-c"]).await;

        // Park the collection on the first scope in map order. Holding the lock
        // needs the cluster alive, so this scope is pinned by the test itself --
        // which is the point: the other two must still be evictable.
        let first = runtime
            .cluster_for_scope(&scopes[0])
            .await
            .expect("the first scope is open");
        let blocker = first
            .shard("cell-0")
            .expect("the directory has one cell")
            .matrix_artifact_cache
            .lock()
            .await;

        let mut collection = std::pin::pin!(runtime.local_shard_runtime_metrics());
        assert!(
            futures::poll!(collection.as_mut()).is_pending(),
            "the held matrix_artifact_cache lock should have parked the collection \
             inside the first cluster"
        );

        let fresh = tenant_scope(&runtime, "tenant-d");
        let opened = runtime.cluster_for_scope(&fresh).await;
        assert!(
            opened.is_ok(),
            "a collection parked on one scope must leave the others evictable; \
             opening a new scope failed with {:?}",
            opened.err()
        );
        drop(opened);

        drop(blocker);
        drop(first);
        let metrics = collection.await;
        // tenant-b was the least recently used evictable entry, so it went to make
        // room for tenant-d. Its `Weak` no longer upgrades, and the collection
        // skips it rather than failing: one shard each for tenant-a and tenant-c.
        assert_eq!(
            metrics.len(),
            2,
            "a scope evicted mid-collection is a skip, not an error, and not a row"
        );

        runtime.close().await.expect("the runtime drains");
    }

    /// `loaded_clusters` must hand back non-owning handles.
    ///
    /// Same failure as above with a wider window: the index-discovery loop holds
    /// its snapshot across `dirty_graph_index_edge_types` and
    /// `discover_graph_index` per cell, both of which do object-store I/O. While
    /// the snapshot was `Vec<Arc<..>>`, merely holding it blocked every eviction.
    #[tokio::test]
    async fn a_held_loaded_clusters_snapshot_does_not_block_opening_a_new_scope() {
        let runtime = runtime_at_capacity("graph/loaded-clusters", 2);
        open_scopes_to_capacity(&runtime, &["tenant-a", "tenant-b"]).await;

        let snapshot = runtime.loaded_clusters().await;
        assert_eq!(snapshot.len(), 2);

        let fresh = tenant_scope(&runtime, "tenant-c");
        let opened = runtime.cluster_for_scope(&fresh).await;
        assert!(
            opened.is_ok(),
            "holding a loaded_clusters snapshot must not pin the open scopes; \
             opening a new scope failed with {:?}",
            opened.err()
        );
        drop(opened);

        // One scope was evicted to make room, so one handle no longer upgrades.
        // That is the skip the consumer has to tolerate.
        let live = snapshot
            .iter()
            .filter(|weak| weak.upgrade().is_some())
            .count();
        assert_eq!(
            live, 1,
            "exactly one of the two snapshotted scopes survived"
        );

        runtime.close().await.expect("the runtime drains");
    }
}

/// Touch point (b): the promotion gate, in all the states this layer can reach.
#[cfg(test)]
mod placement_gate_tests {
    use super::*;

    use std::fmt;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use slatedb::object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };

    const CELL: &str = "cell-a";
    const FLEET: &[&str] = &["node-a", "node-b", "node-c"];

    fn scope_key() -> String {
        GraphScope::default().to_string()
    }

    /// The node rendezvous names for `CELL`, and one that it does not.
    fn owner_and_peer() -> (&'static str, &'static str) {
        let owner = turbolay_placement::hash::owner(&scope_key(), CELL, FLEET)
            .expect("a non-empty fleet has an owner");
        let peer = FLEET
            .iter()
            .copied()
            .find(|node_id| *node_id != owner)
            .expect("three nodes cannot all be the owner");
        (owner, peer)
    }

    fn fleet_view(local_node_id: &str, config: PlacementConfig) -> PlacementView {
        PlacementView::new(local_node_id, FLEET.iter().copied(), config).expect("a valid fleet")
    }

    async fn open_cluster(
        base_path: &str,
        local_node_id: &str,
        placement: PlacementView,
        object_store: Arc<dyn ObjectStore>,
    ) -> RoutedGraphCluster {
        RoutedGraphCluster::open_promotable(
            base_path,
            local_node_id,
            ObjectStoreNodeDirectory::new([CELL], FLEET.iter().copied())
                .expect("a valid directory"),
            placement,
            object_store,
        )
        .await
        .expect("the cluster opens")
    }

    fn edge(key: &str) -> crate::EdgeMutation {
        crate::EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: key.to_string(),
        }
    }

    /// Whether the shard is actually holding the SlateDB writer, which is what
    /// "refused rather than promoted" has to mean to be worth anything.
    fn holds_the_writer(cluster: &RoutedGraphCluster) -> bool {
        cluster
            .shard(CELL)
            .expect("the shard is open")
            .db
            .writer()
            .is_ok()
    }

    /// The owner promotes and the write lands.
    #[tokio::test]
    async fn the_rendezvous_owner_opens_the_writer() {
        let (owner, _) = owner_and_peer();
        let cluster = open_cluster(
            "gate/owner",
            owner,
            fleet_view(owner, PlacementConfig::default()),
            Arc::new(InMemory::new()),
        )
        .await;

        cluster.write_edge(edge("owner-write")).await.unwrap();
        assert!(holds_the_writer(&cluster));
        cluster.close().await.unwrap();
    }

    /// The branch that ends the duel: a node that is not the owner refuses, and
    /// — the part that matters — leaves the writer alone rather than taking the
    /// epoch from whoever holds it. The peer is named so the driver can
    /// re-route instead of retrying into the same wrong node.
    #[tokio::test]
    async fn a_non_owner_refuses_the_write_and_does_not_promote() {
        let (owner, peer) = owner_and_peer();
        let cluster = open_cluster(
            "gate/non-owner",
            peer,
            fleet_view(peer, PlacementConfig::default()),
            Arc::new(InMemory::new()),
        )
        .await;

        let error = cluster
            .write_edge(edge("non-owner-write"))
            .await
            .unwrap_err();
        assert!(
            matches!(
                &error,
                GraphError::NotCellWriter { cell_id, owner: Some(hint) }
                    if cell_id == CELL && hint == owner
            ),
            "expected a hinted refusal, got {error:?}"
        );
        assert!(
            !holds_the_writer(&cluster),
            "a refusal that still opened the writer is the duel"
        );
        cluster.close().await.unwrap();
    }

    /// Decision 7 at the gate: a node past its LIST grace knows nothing about
    /// the fleet, so it refuses **with no hint** — a stale guess would point at
    /// a node that may itself have shed — and it must not promote, because a
    /// partitioned node that promotes can never learn it lost.
    #[tokio::test]
    async fn a_shed_view_refuses_with_no_hint_and_does_not_promote() {
        let store = ListFailingStore::new();
        // Milliseconds rather than the 5s/15s defaults, so grace expires within
        // the test instead of being slept through. Decision 10 gives the crate's
        // own rules this property for free; this is the cheapest way to extend
        // it to a layer that measures real elapsed time.
        let config = PlacementConfig {
            heartbeat_interval: Duration::from_millis(1),
            heartbeat_timeout: Duration::from_millis(5),
        };
        let (_, peer) = owner_and_peer();
        let placement = fleet_view(peer, config);
        let cluster = open_cluster(
            "gate/shed",
            peer,
            placement.clone(),
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        )
        .await;

        store.break_list();
        tokio::time::sleep(config.heartbeat_timeout * 2).await;
        let view = placement
            .refresh(store.as_ref(), &Path::from("gate/shed"))
            .await;
        assert_eq!(view.candidates(), None, "the view must have shed");

        let error = cluster.write_edge(edge("shed-write")).await.unwrap_err();
        assert!(
            matches!(&error, GraphError::NotCellWriter { cell_id, owner: None } if cell_id == CELL),
            "expected an unhinted refusal, got {error:?}"
        );
        assert!(!holds_the_writer(&cluster));
        cluster.close().await.unwrap();
    }

    /// The fourth state, and the one this layer cannot reach on its own:
    /// `LiveNodeSet` always counts the local node live, so a *successful* view
    /// never has an empty candidate list. The seam's own tests build one by
    /// hand; what is pinned here is the rule the gate's first arm implements —
    /// `Unowned` and `Unknown` are both "nobody was named" and have opposite
    /// answers, and folding them together is how the permanent-refusal trap
    /// gets built.
    #[test]
    fn a_known_empty_fleet_licenses_promotion_where_a_shed_view_does_not() {
        assert!(CellOwnership::Unowned.may_promote());
        assert_eq!(CellOwnership::Unowned.owner_hint(), None);
        assert!(!CellOwnership::Unknown.may_promote());
        assert_eq!(CellOwnership::Unknown.owner_hint(), None);
    }

    /// Ownership is checked *before* the fence pacing, so a non-owner is
    /// refused outright instead of being made to wait first: a wait it would
    /// spend and then be refused anyway is latency bought for nothing.
    #[tokio::test]
    async fn ownership_is_refused_without_first_serving_the_reopen_delay() {
        let (_, peer) = owner_and_peer();
        let cluster = open_cluster(
            "gate/order",
            peer,
            fleet_view(peer, PlacementConfig::default()),
            Arc::new(InMemory::new()),
        )
        .await;

        let started = Instant::now();
        let error = cluster.write_edge(edge("ordered")).await.unwrap_err();
        assert!(matches!(error, GraphError::NotCellWriter { .. }));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the refusal must not wait out anything"
        );
        cluster.close().await.unwrap();
    }

    /// An `ObjectStore` whose LIST can be switched off. The kernel has no
    /// shared test double for this — `crates/placement`'s is `#[cfg(test)]`
    /// inside that crate and cannot be imported — and decision 7's state is
    /// only reachable through a failing LIST.
    struct ListFailingStore {
        inner: Arc<dyn ObjectStore>,
        fail_list: AtomicBool,
    }

    impl ListFailingStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(InMemory::new()),
                fail_list: AtomicBool::new(false),
            })
        }

        fn break_list(&self) {
            self.fail_list.store(true, AtomicOrdering::SeqCst);
        }

        fn injected() -> slatedb::object_store::Error {
            slatedb::object_store::Error::Generic {
                store: "ListFailingStore",
                source: "injected LIST fault".into(),
            }
        }
    }

    impl fmt::Display for ListFailingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "ListFailingStore({})", self.inner)
        }
    }

    impl fmt::Debug for ListFailingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "ListFailingStore({:?})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for ListFailingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> slatedb::object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> slatedb::object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, slatedb::object_store::Result<Path>>,
        ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
            if self.fail_list.load(AtomicOrdering::SeqCst) {
                return futures::stream::once(async { Err(Self::injected()) }).boxed();
            }
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> slatedb::object_store::Result<ListResult> {
            if self.fail_list.load(AtomicOrdering::SeqCst) {
                return Err(Self::injected());
            }
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> slatedb::object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }
}

/// Commit 6 of §7.8: the advisory cell-writer record, and the three properties
/// that keep it advisory.
///
/// The whole risk in a "who owns the writer" object is that somebody eventually
/// believes it. These tests exist to make that a build failure: the record is
/// written only by a promotion that really claimed an epoch, a store that
/// refuses to take it cannot fail that promotion, and the promote path issues no
/// GET for it at all — asserted on the object-store call counters rather than on
/// the code's shape, so it stays true when the code moves.
#[cfg(test)]
mod cell_writer_record_tests {
    use super::*;

    use std::fmt;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use slatedb::object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use turbolay_placement::cell_writer::{read_cell_writer, CellWriterRecord, CELL_WRITER_PREFIX};

    const CELL: &str = "cell-a";
    const FLEET: &[&str] = &["node-a", "node-b", "node-c"];

    /// The node rendezvous names for `CELL`, and one that it does not.
    fn owner_and_peer() -> (&'static str, &'static str) {
        let scope = GraphScope::default().to_string();
        let owner = turbolay_placement::hash::owner(&scope, CELL, FLEET)
            .expect("a non-empty fleet has an owner");
        let peer = FLEET
            .iter()
            .copied()
            .find(|node_id| *node_id != owner)
            .expect("three nodes cannot all be the owner");
        (owner, peer)
    }

    async fn open_cluster(
        base_path: &str,
        local_node_id: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> RoutedGraphCluster {
        RoutedGraphCluster::open_promotable(
            base_path,
            local_node_id,
            ObjectStoreNodeDirectory::new([CELL], FLEET.iter().copied())
                .expect("a valid directory"),
            PlacementView::new(
                local_node_id,
                FLEET.iter().copied(),
                PlacementConfig::default(),
            )
            .expect("a valid fleet"),
            object_store,
        )
        .await
        .expect("the cluster opens")
    }

    fn edge(key: &str) -> crate::EdgeMutation {
        crate::EdgeMutation {
            cell_id: CELL.to_string(),
            edge_type: "FOLLOWS".to_string(),
            src: 1,
            dst: 2,
            idempotency_key: key.to_string(),
        }
    }

    /// Where the record for `CELL` lives, taken from the shard's own store path
    /// rather than reconstructed — the same derivation the production write
    /// uses, so a test cannot pass against a path nothing writes to.
    fn record_base(cluster: &RoutedGraphCluster) -> Path {
        cluster
            .shard(CELL)
            .expect("the shard is open")
            .db
            .cell_location()
            .expect("a shard path always has a base")
            .0
    }

    fn writer_epoch(cluster: &RoutedGraphCluster) -> Option<u64> {
        cluster
            .shard(CELL)
            .expect("the shard is open")
            .db
            .writer_epoch()
    }

    /// A promotion that claimed an epoch writes exactly one record, and it names
    /// this node and the epoch the manifest actually gave it.
    #[tokio::test]
    async fn a_successful_promotion_records_the_advisory_cell_writer() {
        let (owner, _) = owner_and_peer();
        let store = RecordStore::new();
        let cluster = open_cluster(
            "record/owner",
            owner,
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        )
        .await;

        cluster.write_edge(edge("owner-write")).await.unwrap();

        assert_eq!(store.record_puts(), 1, "the promotion wrote no record");
        let epoch = writer_epoch(&cluster).expect("the owner holds the writer");
        let record = read_cell_writer(store.as_ref(), &record_base(&cluster), CELL)
            .await
            .expect("the record reads back")
            .expect("a successful promotion wrote one");
        assert_eq!(record.node_id, owner);
        assert_eq!(
            record.epoch, epoch,
            "the record must carry the epoch the manifest gave the writer"
        );
        assert!(
            epoch > 0,
            "a zero epoch would make the assertion above vacuous"
        );
        cluster.close().await.unwrap();
    }

    /// **The PUT is per epoch claimed, not per write.** `promote_to_writer` is
    /// called on every routed write and is idempotent; if the record were
    /// written on each of those calls, decision 3's "no object-store request on
    /// the write path" would be quietly false.
    #[tokio::test]
    async fn the_record_is_written_once_per_promotion_and_not_once_per_write() {
        let (owner, _) = owner_and_peer();
        let store = RecordStore::new();
        let cluster = open_cluster(
            "record/once",
            owner,
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        )
        .await;

        for i in 0..5 {
            cluster
                .write_edge(edge(&format!("write-{i}")))
                .await
                .unwrap();
        }

        assert_eq!(
            store.record_puts(),
            1,
            "the record was rewritten by writes that promoted nothing"
        );
        cluster.close().await.unwrap();
    }

    /// A refusal is not a promotion, so there is nothing to record. Writing one
    /// here would be the worst version of this object: an attribution naming a
    /// node that never held the writer.
    #[tokio::test]
    async fn a_refused_promotion_records_nothing() {
        let (_, peer) = owner_and_peer();
        let store = RecordStore::new();
        let cluster = open_cluster(
            "record/refused",
            peer,
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        )
        .await;

        let error = cluster.write_edge(edge("refused")).await.unwrap_err();
        assert!(
            matches!(error, GraphError::NotCellWriter { .. }),
            "expected a refusal, got {error:?}"
        );

        assert_eq!(store.record_puts(), 0, "a refusal wrote a record");
        assert!(
            read_cell_writer(store.as_ref(), &record_base(&cluster), CELL)
                .await
                .expect("readable")
                .is_none(),
            "a node that never promoted named itself the writer"
        );
        cluster.close().await.unwrap();
    }

    /// Rule 3 of the module docs on `turbolay_placement::cell_writer`, at the
    /// only place that can honour it: the epoch is already claimed and the write
    /// is already committed by the time the record is attempted, so a store that
    /// refuses it must cost nothing but a `warn!`. The alternative — a write
    /// path that fails because its diagnostics could not be written — is
    /// strictly worse than keeping no record at all.
    #[tokio::test]
    async fn a_failing_record_put_does_not_fail_the_promotion() {
        let (owner, _) = owner_and_peer();
        let store = RecordStore::new();
        // Only the record's own PUTs. Failing every PUT would fail SlateDB's
        // writer open too, and the test would prove nothing about the record.
        store.break_record_puts();
        let cluster = open_cluster(
            "record/failing-put",
            owner,
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        )
        .await;

        cluster
            .write_edge(edge("survives"))
            .await
            .expect("an unwritable record must not fail the write");

        assert!(store.record_puts() > 0, "the record was never attempted");
        assert!(
            writer_epoch(&cluster).is_some(),
            "the promotion itself must have stood"
        );
        store.heal_record_puts();
        assert!(
            read_cell_writer(store.as_ref(), &record_base(&cluster), CELL)
                .await
                .expect("readable")
                .is_none(),
            "the failed PUT left something behind"
        );
        cluster.close().await.unwrap();
    }

    /// **Option 3b, refused in a test.** A record naming another node with an
    /// absurd epoch is sitting in the store; the rendezvous owner promotes
    /// straight over it, because ownership comes from the live set and this
    /// object is not an input to it. The GET counter is the proof: the promote
    /// path never even looks.
    #[tokio::test]
    async fn the_record_is_not_read_on_the_promote_path() {
        let (owner, peer) = owner_and_peer();
        let store = RecordStore::new();
        let cluster = open_cluster(
            "record/not-consulted",
            owner,
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        )
        .await;

        let base = record_base(&cluster);
        turbolay_placement::cell_writer::put_cell_writer(
            store.as_ref(),
            &base,
            CELL,
            &CellWriterRecord::new(peer, u64::MAX, chrono::Utc::now()),
        )
        .await
        .expect("seeded");
        store.reset_counters();

        cluster
            .write_edge(edge("promotes-anyway"))
            .await
            .expect("a record naming a peer must not stop the owner promoting");

        assert_eq!(
            store.record_gets(),
            0,
            "the promote path read the advisory record; that is option 3b"
        );
        assert_eq!(
            store.record_puts(),
            1,
            "the promotion should have overwritten the seeded record exactly once"
        );
        let record = read_cell_writer(store.as_ref(), &base, CELL)
            .await
            .expect("readable")
            .expect("written");
        assert_eq!(record.node_id, owner);
        assert_ne!(record.epoch, u64::MAX);
        cluster.close().await.unwrap();
    }

    /// An `ObjectStore` that counts and can break requests **against the
    /// cell-writer prefix alone**.
    ///
    /// The narrowness is the point twice over. Counting everything would drown
    /// the record's one PUT in SlateDB's manifest and SST traffic, so the
    /// "never read on the promote path" assertion could not be written at all.
    /// And failing every PUT would fail the writer open, so the "a failed PUT
    /// does not fail the promotion" test would be asserting that a promotion
    /// which never happened did not happen.
    ///
    /// `crates/placement`'s richer `FaultStore` is `#[cfg(test)]` inside that
    /// crate and cannot be imported here; it also has no notion of which prefix
    /// a request is for, which is the one thing this needs.
    struct RecordStore {
        inner: Arc<dyn ObjectStore>,
        record_puts: AtomicU64,
        record_gets: AtomicU64,
        fail_record_puts: AtomicBool,
    }

    impl RecordStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(InMemory::new()),
                record_puts: AtomicU64::new(0),
                record_gets: AtomicU64::new(0),
                fail_record_puts: AtomicBool::new(false),
            })
        }

        /// Whether a path is under `_cell_writers/…`, by the prefix constant's
        /// own first segment rather than a copy of the string.
        fn is_record(location: &Path) -> bool {
            let head = CELL_WRITER_PREFIX
                .split('/')
                .next()
                .expect("the prefix is not empty");
            location.parts().any(|part| part.as_ref() == head)
        }

        /// Counted **including** requests the fault refuses: the thing being
        /// asserted is that the attempt was made and survived.
        fn record_puts(&self) -> u64 {
            self.record_puts.load(AtomicOrdering::SeqCst)
        }

        fn record_gets(&self) -> u64 {
            self.record_gets.load(AtomicOrdering::SeqCst)
        }

        fn reset_counters(&self) {
            self.record_puts.store(0, AtomicOrdering::SeqCst);
            self.record_gets.store(0, AtomicOrdering::SeqCst);
        }

        fn break_record_puts(&self) {
            self.fail_record_puts.store(true, AtomicOrdering::SeqCst);
        }

        fn heal_record_puts(&self) {
            self.fail_record_puts.store(false, AtomicOrdering::SeqCst);
        }

        /// `Error::Generic`, because that is what a throttled or refused request
        /// arrives as once the SDK has given up, and it is not one of the
        /// variants `read_cell_writer` special-cases.
        fn injected() -> slatedb::object_store::Error {
            slatedb::object_store::Error::Generic {
                store: "RecordStore",
                source: "injected cell-writer PUT fault".into(),
            }
        }
    }

    impl fmt::Display for RecordStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "RecordStore({})", self.inner)
        }
    }

    impl fmt::Debug for RecordStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "RecordStore({:?})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for RecordStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> slatedb::object_store::Result<PutResult> {
            if Self::is_record(location) {
                self.record_puts.fetch_add(1, AtomicOrdering::SeqCst);
                if self.fail_record_puts.load(AtomicOrdering::SeqCst) {
                    return Err(Self::injected());
                }
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> slatedb::object_store::Result<GetResult> {
            if Self::is_record(location) {
                self.record_gets.fetch_add(1, AtomicOrdering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, slatedb::object_store::Result<Path>>,
        ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> slatedb::object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> slatedb::object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }
}
