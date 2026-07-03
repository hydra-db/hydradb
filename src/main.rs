//! turbolay binary — M0 smoke entrypoint.
//!
//! There is no service yet (the HTTP fleet is M3, RFC 0008). For now `main`
//! initializes tracing and exercises the M0 foundation end-to-end against an
//! in-memory namespace: open storage, allocate a uid, resolve an xid, and read
//! it back. This is a liveness check that the substrate wiring holds together.

use turbolay::ids::{GraphAllocators, resolve_or_create_xid};
use turbolay::serde::keys;
use turbolay::storage::GraphStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    turbolay::telemetry::init();

    let storage = GraphStorage::in_memory().await?;
    let mut allocs = GraphAllocators::load(storage.inner().as_ref()).await?;

    let (uid, block) = allocs.allocate_uid();
    if let Some(record) = block {
        storage.put(vec![record.into()]).await?;
    }
    tracing::info!(uid = uid.get(), "allocated first uid");

    let resolved =
        resolve_or_create_xid(storage.inner().as_ref(), &mut allocs, b"doc:hello").await?;
    let again = resolve_or_create_xid(storage.inner().as_ref(), &mut allocs, b"doc:hello").await?;
    assert_eq!(resolved, again, "xid resolution must be idempotent");
    tracing::info!(xid = "doc:hello", uid = resolved.get(), "resolved xid");

    // Confirm the mapping is durable and readable through the keyspace.
    let stored = storage.get(keys::xid_key(b"doc:hello")).await?;
    tracing::info!(present = stored.is_some(), "xid mapping readable");

    println!(
        "turbolay M0 smoke ok: uid={}, xid doc:hello -> {}",
        uid.get(),
        resolved.get()
    );
    Ok(())
}
