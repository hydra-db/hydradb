use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};

use hydradb_cypher_ast::GqlStatus;

use crate::GraphRead;

/// Where deferred conditions land once the value holding them is read: one
/// per execution scope.
///
/// A branch of a forked execution has its own, so a sibling running beside it
/// can never drain what it has published. When the branch finishes, its sink
/// is drained and, in the same step, starts forwarding to its parent's: a
/// value the branch returned and its parent reads later still reports to the
/// caller.
#[derive(Clone, Debug, Default)]
pub(super) struct ConditionSink(Arc<Mutex<SinkState>>);

#[derive(Debug, Default)]
struct SinkState {
    conditions: Vec<GqlStatus>,
    forward: Option<ConditionSink>,
}

impl ConditionSink {
    fn lock(&self) -> MutexGuard<'_, SinkState> {
        self.0.lock().expect("completion condition lock poisoned")
    }

    fn publish(&self, conditions: &[GqlStatus]) {
        let mut state = self.lock();
        if let Some(parent) = state.forward.clone() {
            // A child's lock is never held while its parent's is taken
            // and released in the other order, so forwarding cannot deadlock.
            drop(state);
            return parent.publish(conditions);
        }
        for condition in conditions {
            if !state.conditions.contains(condition) {
                state.conditions.push(*condition);
            }
        }
    }

    fn take(&self) -> Vec<GqlStatus> {
        std::mem::take(&mut self.lock().conditions)
    }

    /// Drain this sink and forward everything published to it from now on to
    /// `parent`, as one step: nothing published in between is lost.
    fn close_into(&self, parent: &ConditionSink) -> Vec<GqlStatus> {
        let mut state = self.lock();
        state.forward = Some(parent.clone());
        std::mem::take(&mut state.conditions)
    }
}

/// Completion conditions from a hoisted expression belong to its value until
/// that value is read. The sink is shared by one execution, not by storage.
#[derive(Clone, Debug)]
pub(super) struct DeferredConditions {
    conditions: Vec<GqlStatus>,
    published: ConditionSink,
}

impl PartialEq for DeferredConditions {
    fn eq(&self, other: &Self) -> bool {
        self.conditions == other.conditions
    }
}

impl Eq for DeferredConditions {}

impl Hash for DeferredConditions {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.conditions.hash(state);
    }
}

impl DeferredConditions {
    pub(super) fn publish(&self) {
        self.published.publish(&self.conditions);
    }
}

pub(super) struct ExecutionRead<'a> {
    storage: &'a mut dyn GraphRead,
    published: Option<ConditionSink>,
    /// The parent's sink, for a branch of a forked execution.
    parent: Option<ConditionSink>,
}

impl<'a> ExecutionRead<'a> {
    pub(super) fn new(storage: &'a mut dyn GraphRead) -> Self {
        Self {
            storage,
            published: None,
            parent: None,
        }
    }

    /// A read for one branch of this execution on `storage`. `parent` is
    /// [`Self::sink`] of the read the branch was forked from; the branch keeps
    /// its own sink until [`Self::finish`] hands it over.
    pub(super) fn branch(storage: &'a mut dyn GraphRead, parent: ConditionSink) -> Self {
        Self {
            storage,
            published: None,
            parent: Some(parent),
        }
    }

    /// This execution's sink for deferred conditions.
    pub(super) fn sink(&mut self) -> ConditionSink {
        self.published.get_or_insert_with(Default::default).clone()
    }

    /// A sibling read for a concurrently executed branch; see
    /// [`GraphRead::fork_read`].
    pub(super) fn fork(&self) -> Option<Box<dyn GraphRead + '_>> {
        self.storage.fork_read()
    }

    /// Re-raise conditions a forked branch collected, in their order.
    pub(super) fn raise_conditions(&mut self, conditions: Vec<GqlStatus>) {
        for condition in conditions {
            self.storage.raise_completion_condition(condition);
        }
    }

    pub(super) fn take_conditions(&mut self) -> Vec<GqlStatus> {
        let mut conditions = self.storage.take_completion_conditions();
        if let Some(published) = &self.published {
            conditions.extend(published.take());
        }
        conditions
    }

    /// End a branch: everything it raised and published, and from now on its
    /// returned values publish to the parent.
    pub(super) fn finish(mut self) -> Vec<GqlStatus> {
        let mut conditions = self.storage.take_completion_conditions();
        match (&self.published, &self.parent) {
            (Some(published), Some(parent)) => conditions.extend(published.close_into(parent)),
            (Some(published), None) => conditions.extend(published.take()),
            (None, _) => {}
        }
        self.published = None;
        conditions
    }

    /// Close a mark's condition scope and restore conditions raised before it.
    pub(super) fn defer_conditions(&mut self, outer: Vec<GqlStatus>) -> Option<DeferredConditions> {
        let conditions = self.take_conditions();
        for condition in outer {
            self.storage.raise_completion_condition(condition);
        }
        if conditions.is_empty() {
            return None;
        }
        Some(DeferredConditions {
            conditions,
            published: self.sink(),
        })
    }
}

impl<'a> Deref for ExecutionRead<'a> {
    type Target = dyn GraphRead + 'a;

    fn deref(&self) -> &Self::Target {
        self.storage
    }
}

impl DerefMut for ExecutionRead<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.storage
    }
}
