//! In-memory capability implementations: a channel-backed transport pair
//! and a map-backed store. The reference implementations for tests and
//! the conformance baseline for platform adapters.

use core::cell::RefCell;

use async_channel::{Receiver, Sender};
use future_form::{future_form, FutureForm, Local, Sendable};
use futures::future::LocalBoxFuture;
use sedimentree_core::{
    collections::Map,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};
use subduction_crypto::signed::Signed;
use subduction_protocol::storage::{Provenance, StorageFailure};
use thiserror::Error;

use crate::storage::{FetchedItems, Policy, Storage, StorageAction, Verdict};
use crate::transport::Transport;

/// The peer end of a memory transport is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("memory transport closed")]
pub struct MemoryTransportClosed;

/// One end of an in-memory framed connection.
#[derive(Debug, Clone)]
pub struct MemoryTransport {
    tx: Sender<Vec<u8>>,
    rx: Receiver<Vec<u8>>,
}

impl MemoryTransport {
    /// A connected pair of transport ends.
    #[must_use]
    pub fn pair() -> (Self, Self) {
        let (a_tx, a_rx) = async_channel::unbounded();
        let (b_tx, b_rx) = async_channel::unbounded();
        (Self { tx: a_tx, rx: b_rx }, Self { tx: b_tx, rx: a_rx })
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> Transport<Async> for MemoryTransport {
    type Error = MemoryTransportClosed;

    fn send_bytes(&self, bytes: Vec<u8>) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(
            async move { self.tx.send(bytes).await.map_err(|_| MemoryTransportClosed) },
        )
    }

    fn recv_bytes(&self) -> Async::Future<'_, Result<Option<Vec<u8>>, Self::Error>> {
        Async::from_future(async move {
            // A closed channel is a clean close, not an error.
            Ok(self.rx.recv().await.ok())
        })
    }

    fn disconnect(&self) -> Async::Future<'_, ()> {
        Async::from_future(async move {
            let _was_open = self.tx.close();
            let _was_open = self.rx.close();
        })
    }
}

/// Map-backed item storage.
///
/// Interior mutability is a lock-free [`RefCell`]: the driver task is the
/// only accessor. That makes this store [`Local`]-only; a `Sendable`
/// backend needs real shared storage underneath.
#[derive(Debug, Default)]
pub struct MemoryStorage {
    inner: RefCell<Trees>,
}

/// A stored item with its blob bytes.
type Stored<T> = Map<CommitId, (Signed<T>, Vec<u8>)>;

#[derive(Debug, Default)]
struct Trees {
    commits: Map<SedimentreeId, Stored<LooseCommit>>,
    fragments: Map<SedimentreeId, Stored<Fragment>>,
}

impl MemoryStorage {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The stored commit ids for `tree`, sorted (test introspection).
    #[must_use]
    pub fn commit_ids(&self, tree: SedimentreeId) -> Vec<CommitId> {
        let mut ids: Vec<CommitId> = self
            .inner
            .borrow()
            .commits
            .get(&tree)
            .map(|items| items.keys().copied().collect())
            .unwrap_or_default();
        ids.sort_unstable();
        ids
    }
}

impl Storage<Local> for MemoryStorage {
    fn persist_items(
        &self,
        tree: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Vec<u8>)>,
        fragments: Vec<(Signed<Fragment>, Vec<u8>)>,
    ) -> LocalBoxFuture<'_, Result<u32, StorageFailure>> {
        Local::from_future(async move {
            let mut trees = self.inner.borrow_mut();
            let mut stored = 0u32;
            for (signed, blob) in commits {
                let Ok(verified) = signed.try_decode_trusted_payload() else {
                    return Err(StorageFailure::Permanent);
                };
                let _previous = trees
                    .commits
                    .entry(tree)
                    .or_default()
                    .insert(verified.head(), (signed, blob));
                stored += 1;
            }
            for (signed, blob) in fragments {
                let Ok(verified) = signed.try_decode_trusted_payload() else {
                    return Err(StorageFailure::Permanent);
                };
                let _previous = trees
                    .fragments
                    .entry(tree)
                    .or_default()
                    .insert(verified.head(), (signed, blob));
                stored += 1;
            }
            Ok(stored)
        })
    }

    fn fetch_items(
        &self,
        tree: SedimentreeId,
        commit_ids: Vec<CommitId>,
        fragment_heads: Vec<CommitId>,
    ) -> LocalBoxFuture<'_, Result<Option<FetchedItems>, StorageFailure>> {
        Local::from_future(async move {
            let trees = self.inner.borrow();
            let commits = trees.commits.get(&tree);
            let fragments = trees.fragments.get(&tree);
            if commits.is_none() && fragments.is_none() {
                return Ok(None);
            }
            let mut items = FetchedItems::default();
            for id in commit_ids {
                if let Some(found) = commits.and_then(|c| c.get(&id)) {
                    items.commits.push(found.clone());
                }
            }
            for head in fragment_heads {
                if let Some(found) = fragments.and_then(|f| f.get(&head)) {
                    items.fragments.push(found.clone());
                }
            }
            Ok(Some(items))
        })
    }

    fn delete_tree(&self, tree: SedimentreeId) -> LocalBoxFuture<'_, Result<(), StorageFailure>> {
        Local::from_future(async move {
            let mut trees = self.inner.borrow_mut();
            let _commits = trees.commits.remove(&tree);
            let _fragments = trees.fragments.remove(&tree);
            Ok(())
        })
    }
}

/// Allows everything: the default policy for tests and trusted setups.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

#[future_form(Sendable, Local)]
impl<Async: FutureForm> Policy<Async> for AllowAll {
    fn authorize(
        &self,
        _provenance: &Provenance,
        _tree: SedimentreeId,
        _action: StorageAction,
    ) -> Async::Future<'_, Verdict> {
        Async::ready(Verdict::Allow)
    }
}
