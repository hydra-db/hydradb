use slatedb::object_store::path::Path;
use slatedb::object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};

use crate::Result;

const PROBE_PAYLOAD: &str = "hydradb-conditional-put-probe\n";

/// Whether an object store implements the conditional put that SlateDB's
/// manifest compare-and-swap is built on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConditionalPutSupport {
    /// The store evaluated `put_opts` with `PutMode::Update`.
    Supported,
    /// The store answered `NotImplemented`. `store` is its own description of
    /// itself — `LocalFileSystem(file:///data/store)` and the like — taken from
    /// the store rather than from the configuration that selected it, so the
    /// report names what is actually mounted.
    Unsupported { store: String },
}

impl ConditionalPutSupport {
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Supported)
    }
}

/// Ask an object store whether it can perform a conditional put.
///
/// SlateDB updates its manifest by compare-and-swap, so a store that answers
/// `NotImplemented` to `PutMode::Update` cannot collect its own garbage: every
/// GC cycle fails and reclaims nothing, and the store grows without bound while
/// reads, writes and the readiness endpoint all stay healthy. `LocalFileSystem`
/// — the backend behind `CLOUD_PROVIDER=local` — is such a store, and its first
/// failure arrives minutes into a write load, long after anyone is still
/// watching the log.
///
/// The question is put to the store rather than inferred from a backend name,
/// so a store that gains the capability stops being reported the day it does,
/// and one that lacks it is caught whatever it is called.
///
/// The probe object is written under `_coordination/v1/`, beside the writer
/// lease's clock probe, and deleted again. Cleanup is best-effort: the probe
/// overwrites unconditionally, so a leftover cannot change the next answer and
/// a failed delete is not worth failing a startup over.
pub async fn probe_conditional_put(
    object_store: &dyn ObjectStore,
    base_path: &str,
) -> Result<ConditionalPutSupport> {
    let path = Path::from(format!(
        "{}/_coordination/v1/conditional-put-probe",
        base_path.trim_end_matches('/')
    ));
    let created = object_store
        .put(&path, PutPayload::from(PROBE_PAYLOAD.to_string()))
        .await?;
    let version = UpdateVersion {
        e_tag: created.e_tag,
        version: created.version,
    };
    let conditional = object_store
        .put_opts(
            &path,
            PutPayload::from(PROBE_PAYLOAD.to_string()),
            PutOptions::from(PutMode::Update(version)),
        )
        .await;
    let _ = object_store.delete(&path).await;
    match conditional {
        Ok(_) => Ok(ConditionalPutSupport::Supported),
        Err(ObjectStoreError::NotImplemented { .. }) => Ok(ConditionalPutSupport::Unsupported {
            store: object_store.to_string(),
        }),
        // The store weighed the precondition and refused it, which is the
        // capability working — something else simply wrote first. Only
        // `NotImplemented` means it cannot answer the question at all.
        Err(ObjectStoreError::Precondition { .. } | ObjectStoreError::AlreadyExists { .. }) => {
            Ok(ConditionalPutSupport::Supported)
        }
        Err(error) => Err(error.into()),
    }
}
