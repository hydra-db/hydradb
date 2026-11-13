use super::*;
pub(crate) use hydradb_graph_slate::write_pipeline::*;
use std::future::Future;

impl GraphShard {
    pub(crate) async fn run_write_pipeline<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        self.run_write_pipeline_with(operation, false).await
    }

    /// For families whose local guard skips the manifest read: a committing
    /// operation is fenced by its create-only WAL upload, and one that commits
    /// nothing refreshes the writer fence after the permit is released.
    pub(crate) async fn run_write_pipeline_fencing_noops<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        self.run_write_pipeline_with(operation, true).await
    }

    async fn run_write_pipeline_with<T>(&self, operation: impl Future<Output = Result<T>>, fence_noops: bool) -> Result<T> {
        self.storage.run_write_pipeline_with(operation, fence_noops).await
    }

    pub(crate) async fn await_prior_write_durability(&self) -> Result<()> {
        self.storage.await_prior_write_durability().await
    }
}
