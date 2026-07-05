//! SlateDB-backed acceptance tier (M1 Wave 4 Workstream B) for the three RFC
//! 0004 acceptance items the in-memory backend cannot exercise:
//!
//! 1. **crash recovery** (acceptance #1) — dropping a [`Writer`] without
//!    calling `close()` and reopening over the same durable state;
//! 2. **zombie-writer fencing** (acceptance #2) — a second writer opening
//!    the same path fences the first, which must fail cleanly on its next
//!    commit rather than silently diverging;
//! 3. **durable gate / read-your-writes** (acceptance #3) — the durable
//!    watermark ([`GraphStorage::subscribe_durable`]) actually advances on
//!    SlateDB (unlike the in-memory backend, which never drives it — see
//!    `src/write.rs`'s own `should_read_a_committed_write_through_a_separate_reader_handle`
//!    test).
//!
//! `common::create_object_store`'s `ObjectStoreConfig::InMemory` branch builds
//! a fresh, disconnected `object_store::memory::InMemory` on every call, so
//! two `StorageConfig::InMemory` opens never share state — that backend is
//! structurally unable to stand in for "two writers/a writer-then-reopen
//! against the same namespace". Real SlateDB semantics need a real SlateDB.
//! We get one cheaply and hermetically with `StorageConfig::SlateDb` over
//! `ObjectStoreConfig::Local` on a `tempfile` tempdir: `LocalFileSystem`
//! opened twice against the same directory *does* share the on-disk state,
//! which is exactly what these three acceptance items need. This is **not**
//! LocalStack/S3 and does not reopen the RFC 0017 D12 door — D12 is about not
//! benchmarking against a simulated S3; this is SlateDB-on-local-disk
//! correctness, the same substrate `vendor/common`'s own `slate.rs` unit
//! tests already use (in-memory object store there; local disk here only
//! because fencing/crash-recovery need a filesystem two independent
//! `StorageBuilder::new` calls can actually share).
//!
//! Every SlateDB `.await` in this file is wrapped in [`with_timeout`] so a
//! regression that causes a hang fails the test suite instead of hanging CI.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use common::storage::RecordOp;
use common::storage::config::{LocalObjectStoreConfig, SlateDbStorageConfig};
use common::{ObjectStoreConfig, Record, StorageConfig, StorageError, WriteOptions};
use tempfile::TempDir;
use turbolay::serde::keys::log_key;
use turbolay::write::Writer;

/// Every SlateDB op in this file is awaited through here: a hang becomes a
/// panic (test failure) after 10s instead of wedging the test binary.
async fn with_timeout<F: Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), fut)
        .await
        .expect("SlateDB operation exceeded 10s timeout — likely hang")
}

/// Builds a `StorageConfig::SlateDb` over `ObjectStoreConfig::Local` rooted at
/// `dir`, with `path` as the namespace prefix inside it. Two configs built
/// from the *same* `dir` with the same `path` describe the same durable
/// namespace — reopening it (or opening a second writer against it) is a
/// real, shared-state SlateDB operation, not a fresh disconnected backend.
fn slatedb_config(dir: &TempDir, path: &str) -> StorageConfig {
    StorageConfig::SlateDb(SlateDbStorageConfig {
        path: path.to_string(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: dir.path().to_str().unwrap().to_string(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    })
}

// ---------------------------------------------------------------------------
// RFC 0004 acceptance #3: durable gate / read-your-writes
// ---------------------------------------------------------------------------

/// RFC 0004 acceptance #3 (durable gate / RYW), on the one backend where
/// `subscribe_durable()`'s watch channel actually advances: unlike
/// `InMemoryStorage` (which never drives `durable_tx` — see the note in
/// `src/write.rs`'s in-memory RYW test), `SlateDbStorage::new` bridges
/// SlateDB's own durable-seq status into the watch channel on every change
/// (`vendor/common/src/storage/slate.rs`). `Writer::commit` always uses
/// `await_durable: true` with an injected `seqnum` equal to the returned
/// token (M1 integration point #2), so the token *is* the durable seq the
/// subscriber must observe — this test asserts that identity holds for a
/// real subscriber, which is the actual reader-freshness gate (`durable_seq
/// >= token`) a caller would drive.
#[tokio::test(flavor = "multi_thread")]
async fn should_advance_durable_watermark_and_be_visible_to_a_subscriber() {
    let dir = tempfile::tempdir().unwrap();
    let config = slatedb_config(&dir, "ns-durable-gate");

    let mut writer = with_timeout(Writer::open(&config)).await.unwrap();
    let mut durable_rx = writer.storage().subscribe_durable();
    assert_eq!(
        *durable_rx.borrow(),
        0,
        "a fresh namespace starts at durable seq 0"
    );

    let token = with_timeout(writer.upsert_edge(b"user:a", "knows", b"user:b"))
        .await
        .unwrap();
    assert_eq!(token, 1, "first commit on a fresh writer is logical seq 1");

    // The subscriber's view is the real freshness gate a reader drives.
    with_timeout(durable_rx.changed()).await.unwrap();
    assert!(
        *durable_rx.borrow() >= token,
        "durable watermark ({}) must have advanced to at least the returned token ({})",
        *durable_rx.borrow(),
        token
    );

    // And the write itself is visible through the writer's own handle
    // (RYW is trivially true here since `commit` already awaited durability
    // before returning `token`).
    let uid = with_timeout(writer.lookup_uid(b"user:a"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        with_timeout(writer.get_node(uid)).await.unwrap().is_some(),
        "just-committed node must be visible once the durable token is observed"
    );
}

// ---------------------------------------------------------------------------
// RFC 0004 acceptance #2: zombie-writer fencing
// ---------------------------------------------------------------------------

/// RFC 0004 acceptance #2 (zombie-writer fencing). SlateDB is
/// single-writer-per-path: opening a second `Db` (`writer2`) against the same
/// path bumps the writer epoch and fences the first (`writer1`), per
/// `slatedb::fence::WriterFencer` — any parallel old writer fails with
/// `SlateDBError::Fenced` the next time it tries to durably write (its next
/// WAL SST collides with the new epoch's barrier). `vendor/common`'s
/// `slate.rs::map_write_error` (added alongside this test, part B2) maps that
/// to the typed `StorageError::Fenced` instead of a generic stringified
/// `StorageError::Storage`.
///
/// `turbolay::Error::Storage` still flattens `StorageError` to a `String`
/// (`src/error.rs`'s `From<StorageError> for Error` calls `.to_string()`), so
/// `writer1.upsert_edge(...)` through the public turbolay API can only prove
/// the failure *surfaces* (non-hang, non-silent-divergence), not that it's
/// typed. To assert the typed variant we drop to the `common` layer directly
/// via `writer1.storage().inner()` (bypassing `GraphStorage`'s `?`-based
/// conversion to `turbolay::Error`, which would erase the type the same way).
#[tokio::test(flavor = "multi_thread")]
async fn should_fence_a_zombie_writer_on_next_commit() {
    let dir = tempfile::tempdir().unwrap();
    let config = slatedb_config(&dir, "ns-fencing");

    let mut writer1 = with_timeout(Writer::open(&config)).await.unwrap();
    let seq1 = with_timeout(writer1.upsert_edge(b"user:a", "knows", b"user:b"))
        .await
        .unwrap();
    assert_eq!(seq1, 1);

    // Opening writer2 over the SAME config/path bumps the epoch — writer1 is
    // now a zombie, even though nothing has told it so yet.
    let mut writer2 = with_timeout(Writer::open(&config)).await.unwrap();

    // writer1's next commit must fail — not hang, not silently succeed while
    // diverging from writer2's view of the namespace.
    let public_result = with_timeout(writer1.upsert_edge(b"user:c", "knows", b"user:d")).await;
    assert!(
        public_result.is_err(),
        "a fenced writer's next commit must surface as an error through the public API"
    );

    // Direct assertion at the `common` layer: the raw error is the typed
    // `StorageError::Fenced`, not a generic string.
    let probe_op = RecordOp::Put(
        Record::new(
            Bytes::from_static(b"__fencing_probe__"),
            Bytes::from_static(b"v"),
        )
        .into(),
    );
    let raw_result = with_timeout(writer1.storage().inner().apply_with_options(
        vec![probe_op],
        WriteOptions {
            await_durable: true,
            seqnum: 0,
        },
    ))
    .await;
    match raw_result {
        Err(StorageError::Fenced(msg)) => {
            assert!(
                !msg.is_empty(),
                "Fenced error should carry the underlying slatedb message"
            );
        }
        other => panic!(
            "expected the fenced writer's raw apply_with_options to fail with \
             StorageError::Fenced, got: {other:?}"
        ),
    }

    // writer2 (the current epoch's writer) continues the seq lineage from
    // the persisted `latest_seq` — recovery-on-open seeded it at `seq1`, and
    // the next commit strictly advances past it.
    assert_eq!(
        writer2.latest_seq(),
        seq1,
        "writer2's recovery-on-open must resume from writer1's durably-committed latest_seq"
    );
    let seq2 = with_timeout(writer2.upsert_edge(b"user:e", "knows", b"user:f"))
        .await
        .unwrap();
    assert!(
        seq2 > seq1,
        "writer2's first write must yield a seq strictly greater than writer1's last ({seq1}), got {seq2}"
    );
}

// ---------------------------------------------------------------------------
// RFC 0004 acceptance #1: crash recovery
// ---------------------------------------------------------------------------

/// RFC 0004 acceptance #1 (crash recovery). `Writer::commit` always uses
/// `await_durable: true` (`src/write.rs`), so by the time `upsert_edge`
/// returns, the write is already durable (WAL-flushed and acknowledged) —
/// dropping the `Writer` afterwards **without** calling
/// `GraphStorage`'s underlying `close()` is a faithful crash simulation (the
/// process dies; nothing gets a chance to run a graceful-shutdown path) that
/// must not lose anything already committed.
///
/// This adds real WAL replay + manifest + allocator recovery on top of the
/// existing in-memory `FailingStorage` mid-batch-truncation test
/// (`src/write.rs::should_leave_no_partial_state_when_a_batch_fails_to_apply`),
/// which only proves atomicity of a single failed `apply` — it can't exercise
/// "does a *closed* process's durable state come back after a real reopen"
/// because `InMemoryStorage` has no on-disk WAL/manifest to replay.
#[tokio::test(flavor = "multi_thread")]
async fn should_recover_committed_state_after_drop_without_close() {
    let dir = tempfile::tempdir().unwrap();
    let config = slatedb_config(&dir, "ns-crash-recovery");

    let (alice_uid, bob_uid, seq1) = {
        let mut writer = with_timeout(Writer::open(&config)).await.unwrap();
        let seq = with_timeout(writer.upsert_edge(b"user:alice", "knows", b"user:bob"))
            .await
            .unwrap();
        let alice = with_timeout(writer.lookup_uid(b"user:alice"))
            .await
            .unwrap()
            .unwrap();
        let bob = with_timeout(writer.lookup_uid(b"user:bob"))
            .await
            .unwrap()
            .unwrap();
        (alice, bob, seq)
        // `writer` (and its `GraphStorage`/`Arc<dyn Storage>`) is dropped
        // here at end of scope — deliberately WITHOUT an explicit `close()`.
        // No `.await` follows the drop in this scope, so there is nothing to
        // yield for; the reopen below is the real test of whether anything
        // was lost.
    };

    // Reopen a fresh Writer over the same config/path — the "process
    // restart" half of the simulation.
    let mut writer2 = with_timeout(Writer::open(&config)).await.unwrap();

    assert_eq!(
        writer2.latest_seq(),
        seq1,
        "latest_seq must recover to the last durably-committed token"
    );
    assert_eq!(
        with_timeout(writer2.lookup_uid(b"user:alice"))
            .await
            .unwrap(),
        Some(alice_uid),
        "xid -> uid mapping must survive the crash"
    );
    assert_eq!(
        with_timeout(writer2.lookup_uid(b"user:bob")).await.unwrap(),
        Some(bob_uid)
    );
    assert!(
        with_timeout(writer2.get_node(alice_uid))
            .await
            .unwrap()
            .is_some(),
        "the node record itself must survive the crash"
    );
    let log = with_timeout(writer2.storage().get(log_key(seq1)))
        .await
        .unwrap();
    assert!(
        log.is_some(),
        "the changelog entry for the last committed seq must survive the crash"
    );

    // Allocators + seq lineage recovered: the next write strictly advances.
    let seq2 = with_timeout(writer2.upsert_edge(b"user:carol", "knows", b"user:alice"))
        .await
        .unwrap();
    assert!(
        seq2 > seq1,
        "seq must be strictly monotonic across the crash, got {seq2} after {seq1}"
    );
    let carol_uid = with_timeout(writer2.lookup_uid(b"user:carol"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        carol_uid.get() > alice_uid.get() && carol_uid.get() > bob_uid.get(),
        "post-crash uid must exceed every pre-crash uid (no reuse)"
    );
}
