//! Skip SSTs that cannot contain the requested incident records or relationship pair.
use std::sync::Arc;

use slatedb::{BloomFilterPolicy, FilterPolicy, PrefixExtractor, PrefixTarget};

/// The SST filters every graph store is opened with. Anything that writes a
/// store's SSTs outside the runtime (an offline image) writes these too, or
/// its SSTs lack the filters the runtime's reads consult.
pub fn graph_filter_policies() -> Vec<Arc<dyn FilterPolicy>> {
    vec![
        // Keep the existing policy name so old SSTs and old readers retain
        // their whole-key Bloom filter during a rolling upgrade or rollback.
        Arc::new(BloomFilterPolicy::new(10)),
        Arc::new(
            BloomFilterPolicy::new(10)
                .with_whole_key_filtering(false)
                .with_prefix_extractor(Arc::new(IncidentPrefixExtractor)),
        ),
        Arc::new(
            BloomFilterPolicy::new(10)
                .with_whole_key_filtering(false)
                .with_prefix_extractor(Arc::new(RelationshipPairPrefixExtractor)),
        ),
    ]
}

struct IncidentPrefixExtractor;

impl PrefixExtractor for IncidentPrefixExtractor {
    fn name(&self) -> &str {
        // Persisted in SST metadata. Change the name if extraction changes.
        "hydradb-incident-prefix-v1"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let input = match target {
            PrefixTarget::Point(input) | PrefixTarget::Prefix(input) => input.as_ref(),
        };
        let cell_len = crate::locality::locality_cell_prefix_len(input)?;
        let suffix = &input[cell_len..];
        let family = [b"e/out/".as_slice(), b"e/in/", b"seg/out/", b"seg/tomb/out/", b"rel/"]
            .into_iter()
            .find(|family| suffix.starts_with(family))?;
        let start = cell_len + family.len();
        // Exactly two complete components: edge type and source/destination.
        // The same prefix is hashed for every possible key extension. Broader
        // scans return None and must continue scanning conservatively.
        let type_end = input[start..].iter().position(|byte| *byte == b'/')? + start;
        let vertex_start = type_end + 1;
        let vertex_end = input[vertex_start..].iter().position(|byte| *byte == b'/')? + vertex_start;
        Some(vertex_end + 1)
    }
}

/// A source can occur in many SSTs while a single `(type, src, dst)` pair
/// occurs in only one. Incident filters cannot prune the other destinations
/// of that source during the per-pair relationship hydration used by LIMIT
/// and incoming expansions. Keep a separate policy (and the old policies)
/// so source scans, old SSTs and rollback readers remain compatible.
struct RelationshipPairPrefixExtractor;

impl PrefixExtractor for RelationshipPairPrefixExtractor {
    fn name(&self) -> &str {
        "hydradb-relationship-pair-prefix-v1"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let input = match target {
            PrefixTarget::Point(input) | PrefixTarget::Prefix(input) => input.as_ref(),
        };
        let cell_len = crate::locality::locality_cell_prefix_len(input)?;
        input[cell_len..].strip_prefix(b"rel/")?;
        let mut end = cell_len + b"rel/".len();
        // Require complete type, source AND destination components. A
        // shorter source/type scan cannot safely use this filter.
        for _ in 0..3 {
            end += input[end..].iter().position(|byte| *byte == b'/')? + 1;
        }
        Some(end)
    }
}
