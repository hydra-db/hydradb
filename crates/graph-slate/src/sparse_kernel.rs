#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum SparseKernelBackend {
    /// Kernel 1: adjacency BFS over `BTreeMap`/`BTreeSet`. Never compiled.
    Adjacency,
    /// Kernel 2: BFS over the compiled flat CSC. No SuiteSparse involved.
    CompactCsc,
    /// Kernel 3: masked `GrB_mxv` over a compiled `GrB_Matrix`.
    #[default]
    SuiteSparse,
}

pub fn env_default_kernel() -> SparseKernelBackend {
    static RESOLVED: std::sync::OnceLock<SparseKernelBackend> = std::sync::OnceLock::new();
    *RESOLVED.get_or_init(|| {
        if std::env::var("GRAPH_COMPILED_KERNEL").is_ok_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "compact" | "csc" | "compact-csc" | "rust-csc"
            )
        }) {
            SparseKernelBackend::CompactCsc
        } else {
            SparseKernelBackend::SuiteSparse
        }
    })
}
