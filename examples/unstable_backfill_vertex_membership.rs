use hydradb::{
    object_store_from_env, GraphOpenOptions, GraphShard, VertexMembershipBackfillOptions,
};

/// Run with `just unstable-backfill-vertex-membership <store-path> <cell>`.
/// Object-store credentials/configuration use the usual CLOUD_PROVIDER env.
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() < 2 || args.len() > 3 || args.get(2).is_some_and(|arg| arg != "--restart") {
        return Err(
            "usage: unstable_backfill_vertex_membership <store-path> <cell> [--restart]".into(),
        );
    }
    let mut options = GraphOpenOptions::default();
    options.unstable_canonical_vertex_membership = true;
    let shard = GraphShard::open_standalone_writer_with_options(
        args[0].clone(),
        object_store_from_env(None)?,
        options,
    )
    .await?;
    let mut work = VertexMembershipBackfillOptions::default();
    work.max_commits = 1;
    work.restart = args.len() == 3;
    let result = async {
        loop {
            let report = shard.unstable_backfill_vertex_membership(&args[1], work.clone()).await?;
            println!("complete={} records_examined={} vertices_created={} storage_sequence={} legacy_isolated_vertices_may_be_missing={}", report.complete, report.records_examined, report.vertices_created, report.storage_sequence, report.legacy_isolated_vertices_may_be_missing);
            work.restart = false;
            if report.complete { return hydradb::Result::Ok(()); }
        }
    }.await;
    // Release the writer on both success and a recoverable maintenance error.
    let closed = shard.close().await;
    result?;
    closed?;
    Ok(())
}
