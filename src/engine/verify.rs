use super::*;

impl GraphShard {
    pub async fn export_live_graph_digest(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
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
        self.verify_matrix_artifact(cell_id, edge_type, read_epoch, &mut report)
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
        _read_epoch: StorageSequence,
        expected_edges: &BTreeSet<(VertexId, VertexId)>,
        report: &mut GraphCorrectnessReport,
    ) -> Result<()> {
        let relationships = self
            .scan_live_relationship_records(cell_id, edge_type, report)
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
        _read_epoch: StorageSequence,
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

    async fn verify_matrix_artifact(
        &self,
        cell_id: &str,
        edge_type: &str,
        read_epoch: StorageSequence,
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
                let direct = self
                    .direct_snapshot_reachable(
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
                        default_matrix_kernel(&self.cache_policy),
                    )
                    .await?
                    .vertices;
                if direct != expected {
                    record_mismatch(
                        report,
                        format!(
                            "traversal:direct root={root} hops={hops} expected={} actual={}",
                            expected.len(),
                            direct.len()
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
