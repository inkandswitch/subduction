//! Shared, per-sedimentree tracking of in-flight upstream fetches.
//!
//! When a subscribing request arrives for a document this node holds nothing
//! for, the response is held (bounded) until an upstream attempt completes, so
//! the first reply carries whatever upstream had rather than an empty local
//! snapshot. See `design/sync/subscriptions.md#missing-document-responses`.
//!
//! Concurrent requests for the same document must share one attempt: both to
//! avoid duplicate upstream traffic, and so a late joiner — one arriving after
//! a *partial* result from one of several upstreams has already been ingested
//! — still waits for the rest instead of answering with the partial state.
//!
//! `InFlightFetches` is that registry. The fetch's owner holds a
//! [`FetchToken`]; everyone else holds a [`FetchDone`]. Completion is signalled
//! by closing the channel behind them (no value is ever sent), which wakes
//! every waiter at once. A token dropped without `finish` (an aborted task)
//! leaves a closed entry behind; lookups treat closed entries as absent, so
//! the next request simply claims afresh.
//!
//! The module is `pub` only so that [`FetchDone`] and [`FetchToken`] can appear
//! in the `#[future_form]`-split trait signatures that build the per-form
//! futures; the registry itself is crate-private.

use async_channel::{Receiver, Sender};
use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{collections::Map, id::SedimentreeId};

/// Registry of upstream fetches currently in flight, keyed by sedimentree.
#[derive(Debug, Default)]
pub(crate) struct InFlightFetches {
    fetches: Mutex<Map<SedimentreeId, Receiver<()>>>,
}

/// A waiter's handle on an in-flight fetch; resolves when the fetch finishes.
#[derive(Debug, Clone)]
pub struct FetchDone(Receiver<()>);

impl FetchDone {
    /// Wait for the fetch to finish. Never yields a value: the channel is only
    /// ever closed, never sent on.
    pub async fn wait(self) {
        let _ = self.0.recv().await;
    }
}

/// Owned by the task running a fetch. Dropping it marks the fetch finished;
/// hand it back via `InFlightFetches::finish` so the registry entry is
/// removed first.
#[derive(Debug)]
pub struct FetchToken {
    _finished: Sender<()>,
}

/// Lifts a [`FetchDone`] into the form's future type so generic code can
/// bound the wait with a [`Timeout`](crate::timeout::Timeout) without naming
/// `Send`. Mirrors [`SendPushes`](super::peers::SendPushes).
pub trait AwaitFetch: FutureForm + Sized {
    /// Resolve once `done`'s fetch has finished.
    ///
    /// Generic in `'a` (like `from_future`) so the result can be handed to a
    /// `Timeout` whose lifetime is tied to a borrowed timer.
    fn fetch_done<'a>(done: FetchDone) -> Self::Future<'a, ()>;
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> AwaitFetch for Async {
    fn fetch_done<'a>(done: FetchDone) -> Self::Future<'a, ()> {
        Async::from_future(done.wait())
    }
}

/// Outcome of [`InFlightFetches::join_or_claim`].
#[derive(Debug)]
pub(crate) enum FetchSlot {
    /// Another task owns the fetch; wait on it.
    Joined(FetchDone),
    /// The caller now owns the fetch: run it, then [`finish`] with the token.
    ///
    /// [`finish`]: InFlightFetches::finish
    Claimed(FetchDone, FetchToken),
    /// No fetch is in flight, and the caller asked not to start one.
    NothingInFlight,
}

impl InFlightFetches {
    /// Join the fetch for `id` if one is in flight.
    pub(crate) async fn join(&self, id: SedimentreeId) -> Option<FetchDone> {
        let fetches = self.fetches.lock().await;
        Self::open(&fetches, id)
    }

    /// Join the fetch for `id` if one is in flight, otherwise start one.
    ///
    /// If there is an in flight fetch we join it, if not then the behavior
    /// depends on `claim_if_vacant`. If that is `true` then we start a fetch
    /// otherwise we just return [`FetchSlot::NothingInFlight`]. The usecase
    /// here is to avoid starting an in flight fetch for a document we have
    /// in storage already.
    pub(crate) async fn join_or_claim(
        &self,
        id: SedimentreeId,
        claim_if_vacant: bool,
    ) -> FetchSlot {
        let mut fetches = self.fetches.lock().await;
        if let Some(done) = Self::open(&fetches, id) {
            return FetchSlot::Joined(done);
        }
        if !claim_if_vacant {
            return FetchSlot::NothingInFlight;
        }
        let (finished, done) = async_channel::bounded::<()>(1);
        fetches.insert(id, done.clone());
        FetchSlot::Claimed(
            FetchDone(done),
            FetchToken {
                _finished: finished,
            },
        )
    }

    /// Mark the fetch for `id` finished: forget it, then wake its waiters.
    ///
    /// The entry is removed *before* the token is dropped so that a waiter
    /// woken by the close cannot observe (and join) the stale entry.
    pub(crate) async fn finish(&self, id: SedimentreeId, token: FetchToken) {
        self.fetches.lock().await.remove(&id);
        drop(token);
    }

    fn open(fetches: &Map<SedimentreeId, Receiver<()>>, id: SedimentreeId) -> Option<FetchDone> {
        fetches
            .get(&id)
            .filter(|done| !done.is_closed())
            .map(|done| FetchDone(done.clone()))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn id(n: u8) -> SedimentreeId {
        SedimentreeId::new([n; 32])
    }

    #[test]
    fn first_caller_claims_and_second_joins() {
        futures::executor::block_on(async {
            let fetches = InFlightFetches::default();
            assert!(fetches.join(id(1)).await.is_none());

            let FetchSlot::Claimed(_done, token) = fetches.join_or_claim(id(1), true).await else {
                panic!("first caller should claim");
            };
            assert!(matches!(
                fetches.join_or_claim(id(1), true).await,
                FetchSlot::Joined(_)
            ));
            assert!(fetches.join(id(1)).await.is_some());
            assert!(fetches.join(id(2)).await.is_none());

            fetches.finish(id(1), token).await;
            assert!(fetches.join(id(1)).await.is_none());
        });
    }

    #[test]
    fn vacant_slot_is_only_claimed_on_request() {
        futures::executor::block_on(async {
            let fetches = InFlightFetches::default();
            assert!(matches!(
                fetches.join_or_claim(id(1), false).await,
                FetchSlot::NothingInFlight
            ));
            let FetchSlot::Claimed(_done, _token) = fetches.join_or_claim(id(1), true).await else {
                panic!("first caller should claim");
            };
            // In flight now: joined regardless of the caller's own verdict.
            assert!(matches!(
                fetches.join_or_claim(id(1), false).await,
                FetchSlot::Joined(_)
            ));
        });
    }

    #[test]
    fn finish_wakes_every_waiter() {
        futures::executor::block_on(async {
            let fetches = InFlightFetches::default();
            let FetchSlot::Claimed(a, token) = fetches.join_or_claim(id(1), true).await else {
                panic!("first caller should claim");
            };
            let b = fetches.join(id(1)).await.expect("in flight");
            fetches.finish(id(1), token).await;
            // Both resolve; a hung waiter would block the executor here.
            a.wait().await;
            b.wait().await;
        });
    }

    #[test]
    fn dropped_token_counts_as_finished() {
        futures::executor::block_on(async {
            let fetches = InFlightFetches::default();
            let FetchSlot::Claimed(done, token) = fetches.join_or_claim(id(1), true).await else {
                panic!("first caller should claim");
            };
            // An aborted owner never calls `finish`; the stale entry must not
            // trap later callers behind a fetch nobody is running.
            drop(token);
            done.wait().await;
            assert!(fetches.join(id(1)).await.is_none());
            assert!(matches!(
                fetches.join_or_claim(id(1), true).await,
                FetchSlot::Claimed(..)
            ));
        });
    }
}
