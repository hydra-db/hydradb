//! `FaultStore`: an `ObjectStore` decorator that fails on command, counts what
//! it was asked to do, and lets a test backdate an object's `LastModified`.
//!
//! Test-only, and deliberately so — decision 11 of
//! `docs/plans/2026-07-25-rendezvous-placement.md`. Nothing in the tree
//! implements `ObjectStore`, so decision 7 needed one built from scratch; it
//! lives here under `#[cfg(test)]` rather than in a `test-support` member
//! because the only production code a failing LIST matters to is
//! [`crate::liveness`]. Routing and cluster tests inject a live set directly and
//! never need a broken store. If kernel tests ever want one too, this graduates
//! to `crates/test-support`; until then it ships in no binary.
//!
//! # Why faults are per-operation, and not one global switch
//!
//! **[`FaultStore::fail_all`] on [`Op::List`] must leave [`Op::Put`] working.**
//! That is the whole reason this file exists, and it is not a nicety.
//!
//! Decision 7 sheds a node's ownership when its LIST has been failing for
//! longer than the grace window, and the part that is easy to get wrong is that
//! shedding must *also* withdraw the heartbeat. In a partial failure — LIST
//! throttled by the store while PUTs still land — a node that sheds while it
//! keeps publishing stays the *computed owner for every one of its peers*. They
//! all route writes to it; it refuses every one, because it has shed; and
//! nothing times out, because its heartbeat is still fresh. The cell is
//! permanently unwritable, and every node's view is self-consistent. It is the
//! nastiest failure in the whole design, and a double that can only fail
//! everything at once cannot reproduce it: kill PUTs too and the heartbeat
//! expires on its own, which is exactly the case that *does* recover.
//!
//! So LIST and PUT get independent knobs, and
//! [`list_can_fail_while_puts_still_land`](tests::list_can_fail_while_puts_still_land)
//! pins that they really are independent.
//!
//! # Why backdating exists
//!
//! `InMemory` stamps `LastModified` from the real wall clock and offers no way
//! to override it. Liveness is measured from that stamp (never a local clock),
//! so without [`FaultStore::set_modified`] the only way to test "this heartbeat
//! is 16 seconds old" is to sleep for 16 seconds — four such rules, at 5s/15s
//! defaults, is a minute of sleeping per run and a flake under CI load.
//! `tokio::time::pause()` does not help: it moves tokio's timers, not the
//! store's timestamps.
//!
//! Most liveness tests should not need this at all — decision 10 collapses
//! timestamps to a `Duration` age at the LIST boundary, so the rules below it
//! are pure functions over hand-built entries. Backdating is for the boundary
//! itself: the tests that exercise `list_heartbeats` reading a real store.
//!
//! Ported from `sleet/src/testing.rs`, which is the proven version of this.
//! Sleet ships it as a public `pub mod testing` and drives its stamps from an
//! injected `TestClock`; ours is `#[cfg(test)]` and takes the timestamp
//! directly, because decision 10 rejected the clock trait.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// The operation classes a fault or a counter applies to.
///
/// Coarse on purpose: these are the classes the placement code distinguishes,
/// not the ten-odd trait methods. Every read path counts as [`Op::Get`], every
/// listing variant as [`Op::List`], and so on — see the [`ObjectStore`] impl for
/// which method maps where.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    /// GET, including ranged reads and `head`.
    Get,
    /// PUT, including multipart uploads.
    Put,
    /// LIST, in all its variants.
    List,
    /// DELETE.
    Delete,
    /// Server-side copy.
    Copy,
}

/// Per-operation call counters.
///
/// Separate atomics rather than a locked map so counting never contends with
/// fault configuration, and so a counter read cannot deadlock against a call in
/// flight.
#[derive(Default)]
struct Counters {
    get: AtomicU64,
    put: AtomicU64,
    list: AtomicU64,
    delete: AtomicU64,
    copy: AtomicU64,
}

impl Counters {
    fn slot(&self, op: Op) -> &AtomicU64 {
        match op {
            Op::Get => &self.get,
            Op::Put => &self.put,
            Op::List => &self.list,
            Op::Delete => &self.delete,
            Op::Copy => &self.copy,
        }
    }
}

/// The configured faults. Empty means healthy.
#[derive(Default)]
struct Faults {
    /// Fail the next N calls of this op, then stop.
    next: HashMap<Op, u64>,
    /// Fail every call of this op until [`FaultStore::heal`].
    always: HashSet<Op>,
}

/// An [`ObjectStore`] decorator with per-operation faults, per-operation call
/// counters, and per-object `LastModified` overrides.
///
/// Wraps any store; [`FaultStore::in_memory`] wraps a fresh `InMemory`, which is
/// what every test here wants. All state is interior-mutable behind `Arc`, so a
/// test can hold the concrete `Arc<FaultStore>` to configure faults while
/// handing an `Arc<dyn ObjectStore>` clone of the same object to the code under
/// test.
pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    counters: Counters,
    faults: Mutex<Faults>,
    /// `LastModified` overrides, applied on the way out of every metadata path.
    /// Behind an `Arc` because `delete_stream` returns a `'static` stream that
    /// has to keep reaching them after the borrow of `self` is gone.
    modified: Arc<Mutex<HashMap<Path, DateTime<Utc>>>>,
}

impl FaultStore {
    /// Decorate an existing store.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            counters: Counters::default(),
            faults: Mutex::default(),
            modified: Arc::default(),
        })
    }

    /// A healthy decorator over a fresh empty `InMemory`.
    pub fn in_memory() -> Arc<Self> {
        Self::new(Arc::new(InMemory::new()))
    }

    /// Fail the next `n` calls of `op`, then behave normally again.
    ///
    /// Additive: two `fail_next(Op::List, 1)` calls fail two LISTs. That is what
    /// a test building up a scenario in stages means by it.
    pub fn fail_next(&self, op: Op, n: u64) {
        *self.lock_faults().next.entry(op).or_default() += n;
    }

    /// Fail every call of `op` until [`FaultStore::heal`].
    ///
    /// Independent per op — see the module docs. `fail_all(Op::List)` with PUTs
    /// left healthy is decision 7's partial failure.
    pub fn fail_all(&self, op: Op) {
        self.lock_faults().always.insert(op);
    }

    /// Clear every fault, for every op, of both kinds.
    ///
    /// Counters and `LastModified` overrides deliberately survive. Neither is a
    /// fault: a test that heals mid-scenario is asserting that the store
    /// recovered, and it still wants the calls made while it was broken to show
    /// up in [`FaultStore::count`], and its backdated heartbeats to stay
    /// backdated.
    pub fn heal(&self) {
        *self.lock_faults() = Faults::default();
    }

    /// Calls of `op` so far, **including calls that were failed by injection**.
    ///
    /// Counting before the fault check, not after, is deliberate and copied from
    /// sleet. The thing these counters are for is retry cadence — "the node
    /// retried the LIST four times inside the grace window" — and a counter that
    /// skipped the failures would report zero for exactly the scenario being
    /// asserted. An assertion that quietly measures nothing is worse than no
    /// assertion.
    pub fn count(&self, op: Op) -> u64 {
        self.counters.slot(op).load(Ordering::SeqCst)
    }

    /// Report `location` as having been last modified at `time`.
    ///
    /// Applied on the way out of `list`, `list_with_offset`,
    /// `list_with_delimiter`, `get_opts` and therefore `head` — see the
    /// [`ObjectStore`] impl for the two paths that are not covered and why.
    ///
    /// The override is dropped as soon as the object is written again, so a
    /// backdated heartbeat that its node re-publishes goes fresh, exactly as it
    /// would in production. Without that, a test that ages a node out and then
    /// watches it recover would find it stale forever.
    pub fn set_modified(&self, location: &Path, time: DateTime<Utc>) {
        self.lock_modified().insert(location.clone(), time);
    }

    fn lock_faults(&self) -> std::sync::MutexGuard<'_, Faults> {
        self.faults.lock().expect("fault state is not poisoned")
    }

    fn lock_modified(&self) -> std::sync::MutexGuard<'_, HashMap<Path, DateTime<Utc>>> {
        self.modified
            .lock()
            .expect("modified overrides are not poisoned")
    }

    /// Count the call, then decide whether to fail it. Order matters — see
    /// [`FaultStore::count`].
    fn check(&self, op: Op) -> Result<(), slatedb::object_store::Error> {
        self.counters.slot(op).fetch_add(1, Ordering::SeqCst);
        let mut faults = self.lock_faults();
        if faults.always.contains(&op) {
            return Err(injected(op));
        }
        if let Some(remaining) = faults.next.get_mut(&op) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(injected(op));
            }
        }
        Ok(())
    }

    /// Forget any override for `location`, so its real `LastModified` shows
    /// through again. Called after anything that rewrites or removes the object.
    fn clear_modified(&self, location: &Path) {
        self.lock_modified().remove(location);
    }

    fn restamp(&self, mut meta: ObjectMeta) -> ObjectMeta {
        if let Some(time) = self.lock_modified().get(&meta.location) {
            meta.last_modified = *time;
        }
        meta
    }
}

/// The error a fault surfaces as.
///
/// `Error::Generic` rather than a bespoke type so callers run their real error
/// path: it is what a throttled or refused request from a real store arrives as
/// once the SDK has given up, and it is not one of the variants
/// (`NotFound`, `Precondition`, …) that callers special-case.
fn injected(op: Op) -> slatedb::object_store::Error {
    slatedb::object_store::Error::Generic {
        store: "FaultStore",
        source: format!("injected {op:?} fault").into(),
    }
}

impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

impl fmt::Debug for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({:?})", self.inner)
    }
}

/// Every method counts first, then faults, then delegates.
///
/// # Why `#[async_trait]`
///
/// `ObjectStore` is an `#[async_trait]` trait, so its `async fn`s are really
/// `fn(..) -> Pin<Box<dyn Future + Send>>` with early-bound lifetimes, and an
/// impl must match that shape exactly — writing the bounds out by hand is the
/// only alternative, and their lifetimes cannot be elided (E0195, late-bound vs
/// early-bound), so it costs ~30 lines of `'life0`/`'async_trait` noise. The
/// macro is a **dev-dependency** and this whole file is `#[cfg(test)]`, so
/// nothing reaches the shipped crate either way.
///
/// # What is not overridden, on purpose
///
/// `list_with_offset`, `get_ranges`, `rename_opts`, `head`, `get`, `delete` and
/// friends are left to their default implementations, which route through the
/// methods below. That is strictly better than intercepting them: they pick up
/// both fault injection and restamping for free and cannot drift out of sync.
/// Sleet overrode `list_with_offset` and lost restamping on it as a result.
///
/// Two consequences worth knowing before writing an assertion:
///
/// - **`get_ranges` bumps [`Op::Get`] once per coalesced range**, not once per
///   call, because the default implementation fans out into `get_range`. It
///   cannot be overridden here without naming `bytes::Bytes`, which is not a
///   dependency of this crate. Placement never reads object bodies, so this
///   costs nothing on the paths that matter.
/// - **`rename_opts` bumps [`Op::Copy`] and [`Op::Delete`]**, since that is what
///   its default implementation performs.
///
/// # What cannot be restamped
///
/// `put_opts` and `copy_opts` return a `PutResult`, which carries no
/// `ObjectMeta`, so there is nothing to restamp on the write path — the
/// override is instead *cleared* there, so the next read shows the real, fresh
/// timestamp. And `MultipartUpload` handles hand out no metadata either; the
/// override for a multipart target is cleared when the upload is *started*
/// rather than when it completes, which is a small lie no placement test can
/// observe, since heartbeats are single-shot PUTs of a few dozen bytes.
#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        self.check(Op::Put)?;
        let result = self.inner.put_opts(location, payload, opts).await?;
        // A rewritten object is fresh again, whatever a test said earlier.
        self.clear_modified(location);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        self.check(Op::Put)?;
        let upload = self.inner.put_multipart_opts(location, opts).await?;
        self.clear_modified(location);
        Ok(upload)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        self.check(Op::Get)?;
        let mut result = self.inner.get_opts(location, options).await?;
        // Also covers `head`, which is `get_opts` with `head(true)` and
        // returns exactly this `meta`.
        result.meta = self.restamp(result.meta);
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, slatedb::object_store::Result<Path>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
        if let Err(error) = self.check(Op::Delete) {
            return futures::stream::once(async move { Err(error) }).boxed();
        }
        // One `check` per stream, not per location: a bulk delete is one
        // request to a real store, and `Op::Delete` counts requests.
        let modified = Arc::clone(&self.modified);
        self.inner
            .delete_stream(locations)
            .map_ok(move |path| {
                modified
                    .lock()
                    .expect("modified overrides are not poisoned")
                    .remove(&path);
                path
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        if let Err(error) = self.check(Op::List) {
            return futures::stream::once(async move { Err(error) }).boxed();
        }
        // The overrides are snapshotted here, when `list` is called, rather than
        // read as each item is yielded. The stream is `'static` and cannot
        // borrow `self`, and copying the handful of entries a placement test
        // registers is cheaper than sharing a lock across the stream's life.
        // The visible consequence: a `set_modified` issued after `list` returns
        // but before the stream is drained is not reflected in that stream. No
        // placement test wants that, and the alternative — a stream that changes
        // its answers while being read — is worse.
        let modified = self.lock_modified().clone();
        self.inner
            .list(prefix)
            .map_ok(move |mut meta| {
                if let Some(time) = modified.get(&meta.location) {
                    meta.last_modified = *time;
                }
                meta
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> slatedb::object_store::Result<ListResult> {
        self.check(Op::List)?;
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result.objects = result
            .objects
            .into_iter()
            .map(|meta| self.restamp(meta))
            .collect();
        // `common_prefixes` are paths, not objects: no timestamp to restamp.
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.check(Op::Copy)?;
        self.inner.copy_opts(from, to, options).await?;
        self.clear_modified(to);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::ObjectStoreExt;

    const PREFIX: &str = "_graph_nodes/v1";

    fn heartbeat(node: &str) -> Path {
        Path::from(format!("{PREFIX}/{node}"))
    }

    async fn publish(store: &FaultStore, node: &str) {
        store
            .put(&heartbeat(node), PutPayload::from("{}"))
            .await
            .expect("a healthy PUT lands");
    }

    async fn list_all(store: &FaultStore) -> slatedb::object_store::Result<Vec<ObjectMeta>> {
        store
            .list(Some(&Path::from(PREFIX)))
            .try_collect::<Vec<_>>()
            .await
    }

    async fn age_of(store: &FaultStore, node: &str, now: DateTime<Utc>) -> chrono::Duration {
        let path = heartbeat(node);
        let metas = list_all(store).await.expect("the LIST is healthy");
        let meta = metas
            .into_iter()
            .find(|m| m.location == path)
            .expect("the heartbeat is present");
        now - meta.last_modified
    }

    #[tokio::test]
    async fn fail_next_fails_exactly_that_many_calls_and_then_recovers() {
        let store = FaultStore::in_memory();
        publish(&store, "graph-node-0").await;

        store.fail_next(Op::List, 1);
        assert!(
            list_all(&store).await.is_err(),
            "the first LIST is the injected failure"
        );
        assert_eq!(
            list_all(&store).await.expect("recovered").len(),
            1,
            "the second LIST is healthy again"
        );
    }

    #[tokio::test]
    async fn fail_next_is_additive_so_a_scenario_can_be_built_up_in_stages() {
        let store = FaultStore::in_memory();
        store.fail_next(Op::List, 1);
        store.fail_next(Op::List, 2);

        for attempt in 0..3 {
            assert!(list_all(&store).await.is_err(), "attempt {attempt} fails");
        }
        assert!(list_all(&store).await.is_ok(), "and the fourth recovers");
    }

    /// Decision 7's partial failure, and the reason this file exists. A node
    /// whose LIST is throttled while its PUTs still land sheds ownership but
    /// stays the computed owner for every peer, so every write routed to it is
    /// refused and nothing ever times out. The double has to be able to express
    /// that, which means the two faults must be genuinely independent.
    #[tokio::test]
    async fn list_can_fail_while_puts_still_land() {
        let store = FaultStore::in_memory();
        store.fail_all(Op::List);

        // The node cannot read the live set...
        assert!(list_all(&store).await.is_err());
        // ...but it can still publish, which is precisely the trap.
        publish(&store, "graph-node-0").await;
        publish(&store, "graph-node-0").await;
        assert_eq!(store.count(Op::Put), 2);
        assert!(list_all(&store).await.is_err(), "still shut out of LIST");

        // And the peers, whose LIST works, keep seeing it as live.
        let peer = FaultStore::new(Arc::clone(&store.inner));
        assert_eq!(
            list_all(&peer)
                .await
                .expect("the peer's LIST is healthy")
                .len(),
            1,
            "the heartbeat the shedding node published is visible to peers"
        );
    }

    /// The converse direction, so the test above cannot pass by accident on a
    /// store that simply never fails PUTs.
    #[tokio::test]
    async fn faults_on_one_operation_do_not_leak_into_another() {
        let store = FaultStore::in_memory();
        store.fail_all(Op::Put);

        assert!(
            store
                .put(&heartbeat("graph-node-0"), PutPayload::from("{}"))
                .await
                .is_err(),
            "PUT is the faulted op"
        );
        assert!(list_all(&store).await.is_ok(), "LIST is untouched");
        assert!(
            store.get(&heartbeat("graph-node-0")).await.is_err(),
            "GET is untouched by the PUT fault — this one fails as NotFound"
        );
    }

    #[tokio::test]
    async fn backdating_ages_one_object_and_leaves_its_neighbours_alone() {
        let store = FaultStore::in_memory();
        publish(&store, "graph-node-0").await;
        publish(&store, "graph-node-1").await;
        publish(&store, "graph-node-2").await;

        let now = Utc::now();
        store.set_modified(
            &heartbeat("graph-node-1"),
            now - chrono::Duration::seconds(16),
        );

        // 16s is past the 15s default timeout; its neighbours are seconds old.
        assert!(
            age_of(&store, "graph-node-1", now).await >= chrono::Duration::seconds(16),
            "the backdated node reads stale"
        );
        for fresh in ["graph-node-0", "graph-node-2"] {
            assert!(
                age_of(&store, fresh, now).await < chrono::Duration::seconds(5),
                "{fresh} stays fresh"
            );
        }
    }

    /// A heartbeat that its node re-publishes must go fresh again, or a test
    /// that ages a node out could never watch it come back.
    #[tokio::test]
    async fn republishing_a_backdated_object_restores_its_real_timestamp() {
        let store = FaultStore::in_memory();
        publish(&store, "graph-node-0").await;
        let now = Utc::now();
        store.set_modified(
            &heartbeat("graph-node-0"),
            now - chrono::Duration::seconds(600),
        );
        assert!(age_of(&store, "graph-node-0", now).await >= chrono::Duration::seconds(600));

        publish(&store, "graph-node-0").await;
        assert!(
            age_of(&store, "graph-node-0", Utc::now()).await < chrono::Duration::seconds(5),
            "the re-published heartbeat is fresh"
        );
    }

    /// The override has to reach every path that hands out an `ObjectMeta`, or a
    /// test would get different answers depending on which one it happened to
    /// call. `head` is covered transitively: it is `get_opts` with `head(true)`.
    #[tokio::test]
    async fn every_metadata_path_reports_the_backdated_timestamp() {
        let store = FaultStore::in_memory();
        let path = heartbeat("graph-node-0");
        publish(&store, "graph-node-0").await;
        publish(&store, "graph-node-1").await;
        let stale = Utc::now() - chrono::Duration::seconds(16);
        store.set_modified(&path, stale);

        let prefix = Path::from(PREFIX);

        let listed = list_all(&store).await.expect("healthy");
        let found = listed.iter().find(|m| m.location == path).expect("present");
        assert_eq!(found.last_modified, stale, "list");

        let offset: Vec<ObjectMeta> = store
            .list_with_offset(Some(&prefix), &Path::from(format!("{PREFIX}/graph-node")))
            .try_collect()
            .await
            .expect("healthy");
        let found = offset.iter().find(|m| m.location == path).expect("present");
        assert_eq!(found.last_modified, stale, "list_with_offset");

        let delimited = store
            .list_with_delimiter(Some(&prefix))
            .await
            .expect("healthy");
        let found = delimited
            .objects
            .iter()
            .find(|m| m.location == path)
            .expect("present");
        assert_eq!(found.last_modified, stale, "list_with_delimiter");

        assert_eq!(
            store.head(&path).await.expect("healthy").last_modified,
            stale,
            "head"
        );
        assert_eq!(
            store.get(&path).await.expect("healthy").meta.last_modified,
            stale,
            "get_opts"
        );
    }

    /// Counting before the fault check, not after. These counters exist to
    /// assert retry cadence inside decision 7's grace window, and a counter that
    /// skipped the failures would read zero for exactly that scenario.
    #[tokio::test]
    async fn counters_include_the_calls_that_were_failed() {
        let store = FaultStore::in_memory();
        store.fail_all(Op::List);
        for _ in 0..4 {
            assert!(list_all(&store).await.is_err());
        }
        assert_eq!(store.count(Op::List), 4, "four attempts, four failures");

        store.heal();
        assert!(list_all(&store).await.is_ok());
        assert_eq!(store.count(Op::List), 5, "healing does not reset the count");
    }

    #[tokio::test]
    async fn counters_are_per_operation_and_start_at_zero() {
        let store = FaultStore::in_memory();
        for op in [Op::Get, Op::Put, Op::List, Op::Delete, Op::Copy] {
            assert_eq!(store.count(op), 0, "{op:?} starts at zero");
        }

        let from = heartbeat("graph-node-0");
        let to = heartbeat("graph-node-1");
        publish(&store, "graph-node-0").await;
        store.copy(&from, &to).await.expect("healthy copy");
        store.delete(&to).await.expect("healthy delete");
        store.get(&from).await.expect("healthy get");
        let _ = list_all(&store).await.expect("healthy list");

        for op in [Op::Get, Op::Put, Op::List, Op::Delete, Op::Copy] {
            assert_eq!(store.count(op), 1, "{op:?} counted exactly once");
        }
    }

    #[tokio::test]
    async fn heal_clears_both_kinds_of_fault_on_every_operation() {
        let store = FaultStore::in_memory();
        store.fail_all(Op::List);
        store.fail_next(Op::Put, 3);
        store.fail_all(Op::Get);
        store.fail_next(Op::Delete, 1);
        store.fail_next(Op::Copy, 1);

        store.heal();

        let from = heartbeat("graph-node-0");
        let to = heartbeat("graph-node-1");
        publish(&store, "graph-node-0").await;
        assert!(list_all(&store).await.is_ok(), "list healed");
        assert!(store.get(&from).await.is_ok(), "get healed");
        assert!(store.copy(&from, &to).await.is_ok(), "copy healed");
        assert!(store.delete(&to).await.is_ok(), "delete healed");
    }

    /// Deleting an object drops its override too, so a path that is re-used by a
    /// later test phase does not inherit a timestamp from the object that used
    /// to live there.
    #[tokio::test]
    async fn deleting_an_object_forgets_its_backdated_timestamp() {
        let store = FaultStore::in_memory();
        let path = heartbeat("graph-node-0");
        publish(&store, "graph-node-0").await;
        store.set_modified(&path, Utc::now() - chrono::Duration::seconds(600));

        store.delete(&path).await.expect("healthy delete");
        publish(&store, "graph-node-0").await;

        assert!(
            age_of(&store, "graph-node-0", Utc::now()).await < chrono::Duration::seconds(5),
            "the replacement object is fresh"
        );
    }

    #[tokio::test]
    async fn a_failed_list_surfaces_as_a_generic_store_error() {
        let store = FaultStore::in_memory();
        store.fail_all(Op::List);
        let error = list_all(&store).await.expect_err("faulted");
        assert!(
            matches!(error, slatedb::object_store::Error::Generic { .. }),
            "callers must exercise their real error path, not a bespoke variant: {error}"
        );
        assert!(error.to_string().contains("injected List fault"));
    }
}
