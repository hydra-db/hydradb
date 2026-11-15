use super::adapter::HydraAdapter;
use hydradb::CypherEngineMode;
use std::path::PathBuf;

pub fn adapter(corpus_root: impl Into<PathBuf>) -> HydraAdapter {
    HydraAdapter::new(CypherEngineMode::Legacy, corpus_root)
}
