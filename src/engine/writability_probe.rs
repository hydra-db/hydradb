use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::{GraphError, Result};

const PROBE_PAYLOAD: &str = "hydradb-writability-probe\n";

/// Check that an object store accepts a write, before anything durable is
/// staged on top of it.
///
/// A container that cannot write to its store directory does not fail at
/// startup: reads work, `/readyz` is green, and the first symptom is every
/// write failing with a generic execution error deep inside the commit path —
/// exactly what happens when a Docker named volume is mounted root-owned
/// under a non-root image, since Docker creates named volumes root-owned by
/// default and only bind mounts inherit the host directory's ownership.
///
/// This turns that into an explicit, actionable boot-time failure instead.
/// The probe writes a small object under `_coordination/v1/`, beside the
/// conditional-put capability probe, and removes it again; cleanup is
/// best-effort, since a leftover probe object cannot change the answer on the
/// next start and a failed delete is not worth failing a healthy boot over.
pub async fn probe_store_writable(object_store: &dyn ObjectStore, base_path: &str) -> Result<()> {
    let path = Path::from(format!(
        "{}/_coordination/v1/writability-probe",
        base_path.trim_end_matches('/')
    ));
    object_store
        .put(&path, PutPayload::from(PROBE_PAYLOAD.to_string()))
        .await
        .map_err(|error| GraphError::StoreNotWritable {
            store: object_store.to_string(),
            reason: error.to_string(),
        })?;
    let _ = object_store.delete(&path).await;
    Ok(())
}
