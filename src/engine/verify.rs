use super::*;

impl GraphShard {
    pub async fn export_live_graph_digest(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
    ) -> Result<GraphExportDigest> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let edges = self.edges_at(cell_id, edge_type, read_epoch).await?;
        Ok(graph_export_digest(cell_id, edge_type, read_epoch, &edges))
    }

    pub async fn verify_current_graph(
        &self,
        cell_id: &str,
        edge_type: &str,
        max_hops: u8,
        traversal_root_limit: usize,
    ) -> Result<GraphCorrectnessReport> {
        validate_component("cell_id", cell_id)?;
        validate_component("edge_type", edge_type)?;
        let started = Instant::now();
        let read_epoch = self.current_epoch(cell_id).await?;
        let edges = self.edges_at(cell_id, edge_type, read_epoch).await?;
        let expected_edges = edge_set(&edges);
        let (expected_out, expected_in) = degree_maps(&edges);
        let mut report = GraphCorrectnessReport {
            cell_id: cell_id.to_string(),
            edge_type: edge_type.to_string(),
            read_epoch,
            delta_gc_watermark: self.delta_gc_watermark(cell_id, edge_type).await?,
            digest: graph_export_digest(cell_id, edge_type, read_epoch, &edges),
            ..GraphCorrectnessReport::default()
        };

        let canonical = self
            .scan_edge_index_pairs(
                &crate::keys::out_edge_type_prefix(cell_id, edge_type),
                cell_id,
                edge_type,
                read_epoch,
                "canonical",
                &mut report,
            )
            .await?;
        report.canonical_edges = canonical.len() as u64;

        let mut out_index = canonical.clone();
        out_index.extend(
            self.out_segment_edge_pairs_at(cell_id, edge_type, read_epoch)
                .await?,
        );
        report.out_index_edges = out_index.len() as u64;
        compare_edge_sets("out_index", &expected_edges, &out_index, &mut report);

        let in_index = self
            .scan_edge_index_pairs(
                &crate::keys::in_edge_type_prefix(cell_id, edge_type),
                cell_id,
                edge_type,
                read_epoch,
                "in_index",
                &mut report,
            )
            .await?;
        report.in_index_edges = in_index.len() as u64;
        if self.writes_reverse_index() {
            compare_edge_sets("in_index", &expected_edges, &in_index, &mut report);
        } else if !in_index.is_empty() {
            record_mismatch(
                &mut report,
                format!(
                    "in_index:unexpected-under-outbound-only count={}",
                    in_index.len()
                ),
            );
        }

        let actual_out = self
            .scan_degree_counters(&crate::keys::degree_out_prefix(cell_id, edge_type))
            .await?;
        let actual_in = self
            .scan_degree_counters(&crate::keys::degree_in_prefix(cell_id, edge_type))
            .await?;
        report.degree_counters = actual_out.len().saturating_add(actual_in.len()) as u64;
        compare_degree_maps("out_degree", &expected_out, &actual_out, &mut report);
        if self.writes_reverse_index() {
            compare_degree_maps("in_degree", &expected_in, &actual_in, &mut report);
        } else if !actual_in.is_empty() {
            record_mismatch(
                &mut report,
                format!(
                    "in_degree:unexpected-under-outbound-only count={}",
                    actual_in.len()
                ),
            );
        }

        self.verify_rollup_and_artifacts(cell_id, edge_type, read_epoch, &mut report)
            .await?;
        self.verify_traversals(
            TraversalVerifyRequest {
                cell_id,
                edge_type,
                read_epoch,
                max_hops,
                root_limit: traversal_root_limit,
                edges: &edges,
            },
            &mut report,
        )
        .await?;
        self.record_verifier_completed(report.mismatch_count, started.elapsed());
        Ok(report)
    }

    async fn scan_edge_index_pairs(
        &self,
        prefix: &str,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
        index_name: &'static str,
        report: &mut GraphCorrectnessReport,
    ) -> Result<BTreeSet<(VertexId, VertexId)>> {
        let mut iter = self.scan_remote_prefix(prefix).await?;
        let mut pairs = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let edge = decode_edge_record(&key, &kv.value)?;
            if edge.cell_id != cell_id || edge.edge_type != edge_type {
                record_mismatch(
                    report,
                    format!(
                        "{index_name}:record-identity key={key} got={}/{}",
                        edge.cell_id, edge.edge_type
                    ),
                );
            }
            if edge.epoch > read_epoch {
                record_mismatch(
                    report,
                    format!(
                        "{index_name}:future-edge key={key} edge_epoch={} read_epoch={read_epoch}",
                        edge.epoch
                    ),
                );
            }
            pairs.insert((edge.src, edge.dst));
        }
        Ok(pairs)
    }

    async fn scan_degree_counters(&self, prefix: &str) -> Result<BTreeMap<VertexId, u64>> {
        let mut iter = self.scan_remote_prefix(prefix).await?;
        let mut counters = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let vertex = parse_last_key_component(&key, "degree_vertex")?;
            let degree = decode_u64(&key, &kv.value)?;
            counters.insert(vertex, degree);
        }
        Ok(counters)
    }

    async fn verify_rollup_and_artifacts(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
        report: &mut GraphCorrectnessReport,
    ) -> Result<()> {
        if let Some(artifact) = self
            .latest_matrix_artifact(cell_id, edge_type, read_epoch)
            .await?
        {
            let adjacency = self
                .cached_matrix_adjacency(cell_id, edge_type, artifact.base_epoch)
                .await?;
            report.matrix_edges_checked = adjacency_edge_count(adjacency.as_ref());
            if report.matrix_edges_checked != artifact.edge_count {
                record_mismatch(
                    report,
                    format!(
                        "matrix:manifest-edge-count base_epoch={} expected={} actual={}",
                        artifact.base_epoch, artifact.edge_count, report.matrix_edges_checked
                    ),
                );
            }
            if let Some(csc) = self
                .graphblas_csc(cell_id, edge_type, artifact.base_epoch)
                .await?
            {
                if csc.indices.len() as u64 != artifact.edge_count {
                    record_mismatch(
                        report,
                        format!(
                            "graphblas_csc:edge-count base_epoch={} expected={} actual={}",
                            artifact.base_epoch,
                            artifact.edge_count,
                            csc.indices.len()
                        ),
                    );
                }
            }
        }

        let mut posting_count = 0_u64;
        let mut posting_iter = self
            .scan_remote_prefix(&format!("cell/{cell_id}/artifact/posting/{edge_type}/"))
            .await?;
        while let Some(kv) = posting_iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let chunk = decode_posting_chunk(&key, &kv.value)?;
            if chunk.cell_id != cell_id || chunk.edge_type != edge_type {
                record_mismatch(
                    report,
                    format!(
                        "posting:identity key={key} got={}/{}",
                        chunk.cell_id, chunk.edge_type
                    ),
                );
            }
            if !chunk
                .vertices
                .windows(2)
                .all(|window| window[0] < window[1])
            {
                record_mismatch(report, format!("posting:unsorted key={key}"));
            }
            posting_count = posting_count.saturating_add(1);
        }
        report.posting_chunks_checked = posting_count;

        let mut group_count = 0_u64;
        let mut group_iter = self
            .scan_remote_prefix(&format!("cell/{cell_id}/artifact/supernode/{edge_type}/"))
            .await?;
        while let Some(kv) = group_iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let group = decode_supernode_group(&key, &kv.value)?;
            group_count = group_count.saturating_add(1);
            self.verify_supernode_group_chunks(&group, report).await?;
        }
        report.supernode_groups_checked = group_count;

        if let Some(rollup) = self.latest_rollup(cell_id, edge_type, read_epoch).await? {
            if let Some(artifact) = self
                .latest_matrix_artifact(cell_id, edge_type, rollup.base_epoch)
                .await?
                .filter(|artifact| artifact.base_epoch == rollup.base_epoch)
            {
                if artifact.edge_count != rollup.matrix_edge_count {
                    record_mismatch(
                        report,
                        format!(
                            "rollup:matrix-edge-count base_epoch={} expected={} actual={}",
                            rollup.base_epoch, rollup.matrix_edge_count, artifact.edge_count
                        ),
                    );
                }
            } else {
                record_mismatch(
                    report,
                    format!("rollup:missing-matrix base_epoch={}", rollup.base_epoch),
                );
            }
        }
        Ok(())
    }

    async fn verify_supernode_group_chunks(
        &self,
        group: &SupernodeGroup,
        report: &mut GraphCorrectnessReport,
    ) -> Result<()> {
        let mut degree = 0_u64;
        for chunk_id in 0..group.chunk_count {
            let Some(chunk) = self.posting_chunk(group, chunk_id).await? else {
                record_mismatch(
                    report,
                    format!(
                        "supernode:missing-chunk vertex={} direction={} base_epoch={} chunk={chunk_id}",
                        group.vertex_id,
                        direction_str(group.direction),
                        group.base_epoch
                    ),
                );
                continue;
            };
            degree = degree.saturating_add(chunk.vertices.len() as u64);
            if !chunk
                .vertices
                .windows(2)
                .all(|window| window[0] < window[1])
            {
                record_mismatch(
                    report,
                    format!(
                        "supernode:unsorted-chunk vertex={} direction={} base_epoch={} chunk={chunk_id}",
                        group.vertex_id,
                        direction_str(group.direction),
                        group.base_epoch
                    ),
                );
            }
            if let Some(bound) = group
                .chunk_bounds
                .iter()
                .find(|bound| bound.chunk_id == chunk_id)
            {
                if chunk.vertices.first().copied() != Some(bound.first)
                    || chunk.vertices.last().copied() != Some(bound.last)
                {
                    record_mismatch(
                        report,
                        format!(
                            "supernode:chunk-bound vertex={} direction={} base_epoch={} chunk={chunk_id}",
                            group.vertex_id,
                            direction_str(group.direction),
                            group.base_epoch
                        ),
                    );
                }
            }
        }
        if degree != group.degree {
            record_mismatch(
                report,
                format!(
                    "supernode:degree vertex={} direction={} base_epoch={} expected={} actual={degree}",
                    group.vertex_id,
                    direction_str(group.direction),
                    group.base_epoch,
                    group.degree
                ),
            );
        }
        Ok(())
    }

    async fn verify_traversals(
        &self,
        request: TraversalVerifyRequest<'_>,
        report: &mut GraphCorrectnessReport,
    ) -> Result<()> {
        if request.max_hops == 0 || request.root_limit == 0 {
            return Ok(());
        }
        let adjacency = adjacency_from_edges(request.edges);
        let roots = adjacency
            .keys()
            .copied()
            .take(request.root_limit)
            .collect::<Vec<_>>();
        for root in roots {
            for hops in 1..=request.max_hops {
                let expected = naive_reachable(&adjacency, root, hops);
                let posting = self
                    .posting_reachable(
                        request.cell_id,
                        request.edge_type,
                        &[root],
                        hops,
                        request.read_epoch,
                    )
                    .await?
                    .vertices;
                let matrix = self
                    .matrix_reachable_with_kernel(
                        request.cell_id,
                        request.edge_type,
                        &[root],
                        hops,
                        request.read_epoch,
                        default_matrix_kernel(),
                    )
                    .await?
                    .vertices;
                if posting != expected {
                    record_mismatch(
                        report,
                        format!(
                            "traversal:posting root={root} hops={hops} expected={} actual={}",
                            expected.len(),
                            posting.len()
                        ),
                    );
                }
                if matrix != expected {
                    record_mismatch(
                        report,
                        format!(
                            "traversal:matrix root={root} hops={hops} expected={} actual={}",
                            expected.len(),
                            matrix.len()
                        ),
                    );
                }
                report.traversal_roots_checked = report.traversal_roots_checked.saturating_add(1);
            }
        }
        Ok(())
    }
}
