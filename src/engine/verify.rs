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

        self.verify_relationship_indexes(
            cell_id,
            edge_type,
            read_epoch,
            &expected_edges,
            &mut report,
        )
        .await?;
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

    async fn verify_relationship_indexes(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
        expected_edges: &BTreeSet<(VertexId, VertexId)>,
        report: &mut GraphCorrectnessReport,
    ) -> Result<()> {
        let relationships = self
            .scan_live_relationship_records(cell_id, edge_type, read_epoch, report)
            .await?;
        report.relationship_records = relationships.len() as u64;

        let mut expected_counts = BTreeMap::<(VertexId, VertexId), u64>::new();
        let mut expected_property_indexes = BTreeSet::<RelationshipPropertyIndexEntry>::new();
        for record in &relationships {
            if !expected_edges.contains(&(record.src, record.dst)) {
                record_mismatch(
                    report,
                    format!(
                        "relationship:missing-structural-edge relationship_id={} src={} dst={}",
                        record.relationship_id, record.src, record.dst
                    ),
                );
            }
            *expected_counts.entry((record.src, record.dst)).or_insert(0) += 1;
            for (property, value) in &record.metadata.properties {
                expected_property_indexes.insert((
                    property.clone(),
                    encode_vertex_property_value_key(value),
                    record.src,
                    record.dst,
                    record.relationship_id,
                ));
            }
        }

        let actual_counts = self
            .scan_relationship_count_counters(cell_id, edge_type)
            .await?;
        report.relationship_count_counters = actual_counts.len() as u64;
        compare_relationship_count_maps(
            "relationship_count",
            &expected_counts,
            &actual_counts,
            report,
        );

        let actual_property_indexes = self
            .scan_relationship_property_index_entries(cell_id, edge_type)
            .await?;
        report.relationship_property_indexes = actual_property_indexes.len() as u64;
        compare_relationship_property_index_sets(
            "relationship_property_index",
            &expected_property_indexes,
            &actual_property_indexes,
            report,
        );
        Ok(())
    }

    async fn scan_live_relationship_records(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: GraphEpoch,
        report: &mut GraphCorrectnessReport,
    ) -> Result<Vec<RelationshipRecord>> {
        let mut iter = self
            .scan_remote_prefix(&crate::keys::relationship_cell_prefix(cell_id))
            .await?;
        let mut records = Vec::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let record = decode_relationship_record(&key, &kv.value)?;
            if record.edge_type != edge_type {
                continue;
            }
            if record.cell_id != cell_id {
                record_mismatch(
                    report,
                    format!(
                        "relationship:record-identity key={key} got={}/{}",
                        record.cell_id, record.edge_type
                    ),
                );
            }
            if record.epoch > read_epoch {
                record_mismatch(
                    report,
                    format!(
                        "relationship:future-record key={key} relationship_epoch={} read_epoch={read_epoch}",
                        record.epoch
                    ),
                );
                continue;
            }
            let tombstone_key = crate::keys::relationship_tombstone(
                cell_id,
                &record.edge_type,
                record.src,
                record.dst,
                record.relationship_id,
            );
            if let Some(value) = self.read_remote(&tombstone_key).await? {
                let tombstone_epoch = decode_u64(&tombstone_key, &value)?;
                if record.epoch <= tombstone_epoch && tombstone_epoch <= read_epoch {
                    continue;
                }
            }
            records.push(record);
        }
        Ok(records)
    }

    async fn scan_relationship_count_counters(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<BTreeMap<(VertexId, VertexId), u64>> {
        let mut iter = self
            .scan_remote_prefix(&format!("cell/{cell_id}/rel_count/{edge_type}/"))
            .await?;
        let mut counters = BTreeMap::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (src, dst) = parse_relationship_count_key(&key)?;
            let count = decode_u64(&key, &kv.value)?;
            if count > 0 {
                counters.insert((src, dst), count);
            }
        }
        Ok(counters)
    }

    async fn scan_relationship_property_index_entries(
        &self,
        cell_id: &str,
        edge_type: &str,
    ) -> Result<BTreeSet<RelationshipPropertyIndexEntry>> {
        let mut iter = self
            .scan_remote_prefix(&format!("cell/{cell_id}/rprop_idx/{edge_type}/"))
            .await?;
        let mut entries = BTreeSet::new();
        while let Some(kv) = iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let (_cell_id, _edge_type, property, encoded, src, dst, relationship_id) =
                parse_relationship_property_index_key_for_verify(&key)?;
            entries.insert((property, encoded, src, dst, relationship_id));
        }
        Ok(entries)
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
            if !self
                .posting_chunk_is_published_or_supernode_referenced(&chunk)
                .await?
            {
                record_mismatch(report, format!("posting:unpublished-chunk key={key}"));
            }
            posting_count = posting_count.saturating_add(1);
        }
        report.posting_chunks_checked = posting_count;

        let mut group_count = 0_u64;
        let mut groups_by_epoch = BTreeMap::<GraphEpoch, Vec<SupernodeGroup>>::new();
        let mut group_iter = self
            .scan_remote_prefix(&format!("cell/{cell_id}/artifact/supernode/{edge_type}/"))
            .await?;
        while let Some(kv) = group_iter.next().await? {
            let key = String::from_utf8_lossy(&kv.key).into_owned();
            let group = decode_supernode_group(&key, &kv.value)?;
            if self
                .supernode_artifact_manifest(cell_id, edge_type, group.base_epoch)
                .await?
                .is_none()
            {
                record_mismatch(report, format!("supernode:unpublished-group key={key}"));
            }
            group_count = group_count.saturating_add(1);
            groups_by_epoch
                .entry(group.base_epoch)
                .or_default()
                .push(group.clone());
            self.verify_supernode_group_chunks(&group, report).await?;
        }
        report.supernode_groups_checked = group_count;
        for (base_epoch, groups) in groups_by_epoch {
            if let Some(actual) = self
                .supernode_artifact_manifest(cell_id, edge_type, base_epoch)
                .await?
            {
                match supernode_artifact_manifest_from_groups(
                    cell_id, edge_type, base_epoch, &groups,
                )? {
                    Some(expected) if expected == actual => {}
                    Some(expected) => record_mismatch(
                        report,
                        format!(
                            "supernode:manifest-mismatch base_epoch={base_epoch} expected_groups={} actual_groups={}",
                            expected.group_count, actual.group_count
                        ),
                    ),
                    None => record_mismatch(
                        report,
                        format!("supernode:empty-groups-with-manifest base_epoch={base_epoch}"),
                    ),
                }
            }
        }

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

    async fn posting_chunk_is_published_or_supernode_referenced(
        &self,
        chunk: &PostingChunk,
    ) -> Result<bool> {
        let posting_manifest_key =
            posting_artifact_manifest_key(&chunk.cell_id, &chunk.edge_type, chunk.base_epoch);
        if let Some(value) = self.read_remote(&posting_manifest_key).await? {
            decode_posting_artifact_manifest(&posting_manifest_key, &value)?;
            return Ok(true);
        }
        let group_key = format!(
            "cell/{}/artifact/supernode/{}/{}/{:020}/{:020}",
            chunk.cell_id,
            chunk.edge_type,
            direction_str(chunk.direction),
            chunk.owner,
            chunk.base_epoch
        );
        if self.read_remote(&group_key).await?.is_none() {
            return Ok(false);
        }
        Ok(self
            .supernode_artifact_manifest(&chunk.cell_id, &chunk.edge_type, chunk.base_epoch)
            .await?
            .is_some())
    }

    async fn supernode_artifact_manifest(
        &self,
        cell_id: &str,
        edge_type: &str,
        base_epoch: GraphEpoch,
    ) -> Result<Option<SupernodeArtifactManifest>> {
        let key = supernode_artifact_manifest_key(cell_id, edge_type, base_epoch);
        self.read_remote(&key)
            .await?
            .map(|value| decode_supernode_artifact_manifest(&key, &value))
            .transpose()
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
