//! Whether an object store honours create-only PUTs.
//!
//! WAL fencing rests on them: a takeover claims the next WAL id with a
//! create-only PUT, and a superseded writer's upload to that id must fail.
//! object_store sends `If-None-Match: *` and maps 412/304 (and S3's 409
//! conflict) to `AlreadyExists`. A proxy or S3-compatible layer that ignores
//! the header turns both PUTs into overwrites, and the claim silently clobbers
//! a WAL object whose write was already acknowledged.
//!
//! SlateDB's retrying store also needs the store to return user metadata on a
//! HEAD: after a lost PUT response it re-sends, gets `AlreadyExists`, and
//! checks the object's `slatedbputid` to tell its own landed PUT from a newer
//! writer's. Without the echo a healthy writer fences itself.

use slatedb::object_store::path::Path;
use slatedb::object_store::{Attribute, AttributeValue, Attributes, GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions};

/// The metadata key SlateDB's retrying object store tags every create-only PUT
/// with (`retrying_object_store.rs`).
const SLATEDB_PUT_ID_ATTRIBUTE: &str = "slatedbputid";

#[derive(Debug, thiserror::Error)]
pub enum CreateOnlyProbeError {
    #[error("{contenders} concurrent create-only PUTs to {key} produced {winners} successes; the store does not honour If-None-Match: *")]
    NotExclusive { key: String, contenders: usize, winners: usize },
    #[error(
        "a HEAD of {key} returned {SLATEDB_PUT_ID_ATTRIBUTE} {returned:?}, expected {expected:?}; the store does not return user metadata"
    )]
    MetadataNotReturned {
        key: String,
        expected: String,
        returned: Option<String>,
    },
    #[error("create-only probe of {key} failed: {source}")]
    Store {
        key: String,
        #[source]
        source: slatedb::object_store::Error,
    },
}

/// Race `contenders` create-only PUTs, each tagged with its own put id, to one
/// key under `prefix`. Exactly one must win, every other must be rejected as
/// `AlreadyExists` or `Precondition`, and a HEAD must return the winner's put
/// id. A store that does not support PUT attributes (`LocalFileSystem`) gets
/// untagged PUTs and no metadata check, as SlateDB's retrying store falls
/// back the same way. The probe object is deleted afterwards, best effort.
pub async fn probe_create_only_puts(store: &dyn ObjectStore, prefix: &Path, contenders: usize) -> Result<(), CreateOnlyProbeError> {
    let location = prefix.clone().join("probe");
    let result = match race_create_only_puts(store, &location, contenders, true).await {
        Err(CreateOnlyProbeError::Store {
            source: slatedb::object_store::Error::NotImplemented { .. } | slatedb::object_store::Error::NotSupported { .. },
            ..
        }) => race_create_only_puts(store, &location, contenders, false).await,
        result => result,
    };
    let _ = store.delete(&location).await;
    result
}

async fn race_create_only_puts(
    store: &dyn ObjectStore,
    location: &Path,
    contenders: usize,
    tag_put_ids: bool,
) -> Result<(), CreateOnlyProbeError> {
    let key = location.to_string();
    let puts = (0..contenders).map(|contender| async move {
        let put_id = format!("create-only-probe-{contender}");
        let mut attributes = Attributes::new();
        if tag_put_ids {
            attributes.insert(
                Attribute::Metadata(SLATEDB_PUT_ID_ATTRIBUTE.into()),
                AttributeValue::from(put_id.clone()),
            );
        }
        let options = PutOptions {
            mode: PutMode::Create,
            attributes,
            ..Default::default()
        };
        (
            put_id,
            store.put_opts(location, format!("contender {contender}").into(), options).await,
        )
    });
    let mut winners = Vec::new();
    for (put_id, result) in futures::future::join_all(puts).await {
        match result {
            Ok(_) => winners.push(put_id),
            Err(slatedb::object_store::Error::AlreadyExists { .. } | slatedb::object_store::Error::Precondition { .. }) => {}
            Err(source) => return Err(CreateOnlyProbeError::Store { key, source }),
        }
    }
    let [winner] = winners.as_slice() else {
        return Err(CreateOnlyProbeError::NotExclusive {
            key,
            contenders,
            winners: winners.len(),
        });
    };
    if !tag_put_ids {
        return Ok(());
    }

    let head = store
        .get_opts(
            location,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .map_err(|source| CreateOnlyProbeError::Store { key: key.clone(), source })?;
    let returned = head
        .attributes
        .get(&Attribute::Metadata(SLATEDB_PUT_ID_ATTRIBUTE.into()))
        .map(|value| value.as_ref().to_string());
    if returned.as_deref() != Some(winner.as_str()) {
        return Err(CreateOnlyProbeError::MetadataNotReturned {
            key,
            expected: winner.clone(),
            returned,
        });
    }
    Ok(())
}

/// Whether this process may take any cell's writer.
///
/// A node whose object store has not passed [`probe_create_only_puts`] still
/// serves reads, but must not write: WAL fencing on such a store cannot stop a
/// superseded writer, and the writer lease is stored the same way. graph-node
/// closes the gate when the boot probe fails and opens it once a retry passes;
/// `/readyz` and the heartbeat read the same gate, so a node that refuses
/// writes is never advertised as ready. Open by default, so every other
/// caller writes as before.
#[derive(Clone, Debug)]
pub struct WriterGate(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Default for WriterGate {
    fn default() -> Self {
        Self(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)))
    }
}

impl WriterGate {
    pub fn is_open(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn set_open(&self, open: bool) {
        self.0.store(open, std::sync::atomic::Ordering::Release);
    }
}
