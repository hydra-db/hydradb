use std::collections::BTreeMap;

use slatedb::bytes::Bytes;
use slatedb::{PrefixExtractor, PrefixTarget};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageLayout {
    OneDbPerLocalityCell,
    SegmentedLocalityKeyspace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalityLayoutExperiment {
    pub total_keys: usize,
    pub segmentable_keys: usize,
    pub cells: BTreeMap<String, usize>,
    pub unsegmentable_keys: Vec<String>,
    pub recommended_layout: StorageLayout,
}

impl LocalityLayoutExperiment {
    pub fn segment_extractor_safe(&self) -> bool {
        self.total_keys == self.segmentable_keys && self.unsegmentable_keys.is_empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct LocalityCellExtractor;

impl LocalityCellExtractor {
    pub fn new() -> Self {
        Self
    }

    pub fn prefix<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        locality_cell_prefix_len(key).map(|len| &key[..len])
    }
}

impl PrefixExtractor for LocalityCellExtractor {
    fn name(&self) -> &str {
        "graph-locality-cell-v1"
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        match target {
            PrefixTarget::Point(key) => locality_cell_prefix_len(key),
            PrefixTarget::Prefix(prefix) => locality_cell_prefix_len(prefix),
        }
    }
}

pub fn compare_locality_layouts(
    keys: impl IntoIterator<Item = impl AsRef<str>>,
) -> LocalityLayoutExperiment {
    let mut total_keys = 0_usize;
    let mut segmentable_keys = 0_usize;
    let mut cells = BTreeMap::new();
    let mut unsegmentable_keys = Vec::new();

    for key in keys {
        let key = key.as_ref();
        total_keys += 1;
        if let Some(cell_id) = locality_cell_id(key.as_bytes()) {
            segmentable_keys += 1;
            *cells.entry(cell_id.to_string()).or_insert(0) += 1;
        } else {
            unsegmentable_keys.push(key.to_string());
        }
    }

    LocalityLayoutExperiment {
        total_keys,
        segmentable_keys,
        cells,
        unsegmentable_keys,
        recommended_layout: StorageLayout::OneDbPerLocalityCell,
    }
}

pub fn locality_cell_prefix_len(key: &[u8]) -> Option<usize> {
    const PREFIX: &[u8] = b"cell/";
    if !key.starts_with(PREFIX) {
        return None;
    }
    let cell_start = PREFIX.len();
    let relative_end = key[cell_start..].iter().position(|byte| *byte == b'/')?;
    if relative_end == 0 {
        return None;
    }
    Some(cell_start + relative_end + 1)
}

pub fn locality_cell_id(key: &[u8]) -> Option<&str> {
    let prefix_len = locality_cell_prefix_len(key)?;
    let cell_bytes = &key[b"cell/".len()..prefix_len - 1];
    std::str::from_utf8(cell_bytes).ok()
}

pub fn locality_cell_prefix(cell_id: &str) -> Bytes {
    Bytes::from(format!("cell/{cell_id}/"))
}
