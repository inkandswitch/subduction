//! Heads notifications must quiesce once two peers have converged.
//!
//! An application that reacts to `on_remote_heads` by syncing (the obvious
//! reading of the callback) will otherwise loop forever: every sync round
//! answers with `responder_heads`, every `responder_heads` is reported, and
//! every report triggers another sync. The heads never change, so nothing
//! makes it stop.
//!
//! Regression guard for heads notifications repeating unchanged heads.

#![allow(clippy::expect_used, clippy::panic)]

use core::time::Duration;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::{RemoteHeads, RemoteHeadsObserver},
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<ChannelTransport>;

type Node<R> = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<
            Sendable,
            MemoryStorage,
            Conn,
            OpenPolicy,
            CountLeadingZeroBytes,
            TokioSpawn,
            256,
            R,
        >,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

/// Counts notifications and remembers the heads reported each time.
#[derive(Clone, Debug, Default)]
struct CountingObserver {
    calls: Arc<AtomicUsize>,
    reported: Arc<Mutex<Vec<RemoteHeads>>>,
}

impl CountingObserver {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn reported(&self) -> Vec<RemoteHeads> {
        self.reported.lock().expect("poisoned").clone()
    }
}

impl RemoteHeadsObserver for CountingObserver {
    fn on_remote_heads(&self, _id: SedimentreeId, _peer: PeerId, heads: RemoteHeads) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.reported.lock().expect("poisoned").push(heads);
    }
}

fn signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn node<R: RemoteHeadsObserver + Send + Sync + 'static>(seed: u8, observer: R) -> Node<R> {
    let (sd, _handler, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
        .signer(signer(seed))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .heads_observer(observer)
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

async fn connect<A, B>(a: &Node<A>, a_seed: u8, b: &Node<B>, b_seed: u8) -> TestResult
where
    A: RemoteHeadsObserver + Send + Sync + 'static,
    B: RemoteHeadsObserver + Send + Sync + 'static,
{
    let (ta, tb) = ChannelTransport::pair();
    let peer_a = PeerId::from(signer(a_seed).verifying_key());
    let peer_b = PeerId::from(signer(b_seed).verifying_key());

    a.add_connection(Authenticated::new_for_test(
        MessageTransport::new(ta),
        peer_b,
    ))
    .await?;
    b.add_connection(Authenticated::new_for_test(
        MessageTransport::new(tb),
        peer_a,
    ))
    .await?;
    Ok(())
}

/// Two converged peers, repeatedly synced. The heads never change, so the
/// observer should be told once and then left alone.
#[tokio::test]
async fn repeated_sync_of_converged_trees_stops_notifying() -> TestResult {
    let observer = CountingObserver::default();
    let a = node(1, observer.clone());
    let b = node(2, CountingObserver::default());
    connect(&a, 1, &b, 2).await?;

    let doc = SedimentreeId::new([9u8; 32]);
    let peer_b = PeerId::from(signer(2).verifying_key());

    b.add_commit(
        doc,
        CommitId::new([1u8; 32]),
        BTreeSet::new(),
        Blob::new(b"only commit".to_vec()),
    )
    .await?;

    // First sync: A learns about the commit. A notification here is correct.
    a.sync_with_peer(&peer_b, doc, true, CallTimeout::TimeoutMillis(500))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after_first = observer.calls();
    assert!(
        after_first >= 1,
        "the first sync should report B's heads at least once"
    );

    // Nothing changes from here on. Five more no-op sync rounds.
    for _ in 0..5 {
        a.sync_with_peer(&peer_b, doc, true, CallTimeout::TimeoutMillis(500))
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let after_repeats = observer.calls();
    let reported = observer.reported();
    let distinct: BTreeSet<Vec<CommitId>> = reported.iter().map(|h| h.heads.clone()).collect();

    assert_eq!(
        distinct.len(),
        1,
        "expected one distinct heads value across all notifications, got {}: {reported:?}",
        distinct.len()
    );
    assert_eq!(
        after_repeats,
        after_first,
        "syncing a converged tree notified the observer {} more times with unchanged heads; \
         an app that syncs in response to a notification never quiesces",
        after_repeats - after_first
    );

    Ok(())
}

/// Filtering must not swallow real news: when the peer's heads actually move,
/// the observer hears about it.
#[tokio::test]
async fn changed_heads_still_notify() -> TestResult {
    let observer = CountingObserver::default();
    let a = node(3, observer.clone());
    let b = node(4, CountingObserver::default());
    connect(&a, 3, &b, 4).await?;

    let doc = SedimentreeId::new([8u8; 32]);
    let peer_b = PeerId::from(signer(4).verifying_key());

    let mut distinct_heads = BTreeSet::new();
    for i in 1..=3u8 {
        b.add_commit(
            doc,
            CommitId::new([i; 32]),
            BTreeSet::new(),
            Blob::new(vec![i; 16]),
        )
        .await?;

        a.sync_with_peer(&peer_b, doc, true, CallTimeout::TimeoutMillis(500))
            .await?;
        tokio::time::sleep(Duration::from_millis(50)).await;

        distinct_heads.insert(
            observer
                .reported()
                .last()
                .expect("a new commit should have been reported")
                .heads
                .clone(),
        );
    }

    assert_eq!(
        distinct_heads.len(),
        3,
        "each new commit should have produced a distinct heads report, got {distinct_heads:?}"
    );
    Ok(())
}

/// Peers stamp one counter sequence across every tree, so a message about
/// tree Y must not mask a fresh, lower-counter message about tree X. Driven
/// directly, because the in-process channel transport will not reorder on
/// demand.
#[tokio::test]
async fn a_later_tree_does_not_mask_an_earlier_one() {
    use subduction_core::remote_heads::FilteredHeadsNotifier;

    let observer = CountingObserver::default();
    let notifier = FilteredHeadsNotifier::new(observer.clone());

    let peer = PeerId::new([1u8; 32]);
    let tree_x = SedimentreeId::new([b'x'; 32]);
    let tree_y = SedimentreeId::new([b'y'; 32]);

    // Y's message (counter 6) overtakes X's (counter 5) in flight.
    notifier
        .notify(
            tree_y,
            peer,
            RemoteHeads {
                counter: 6,
                heads: vec![CommitId::new([0xBB; 32])],
            },
        )
        .await;
    notifier
        .notify(
            tree_x,
            peer,
            RemoteHeads {
                counter: 5,
                heads: vec![CommitId::new([0xAA; 32])],
            },
        )
        .await;

    assert_eq!(
        observer.calls(),
        2,
        "X's update was dropped because an unrelated tree had a higher counter"
    );
}

/// A reconnecting peer is a new session: the first heads it reports should
/// reach the observer again, even though they are unchanged.
#[tokio::test]
async fn reconnecting_peer_reports_again() {
    use subduction_core::remote_heads::FilteredHeadsNotifier;

    let observer = CountingObserver::default();
    let notifier = FilteredHeadsNotifier::new(observer.clone());

    let peer = PeerId::new([2u8; 32]);
    let other = PeerId::new([3u8; 32]);
    let doc = SedimentreeId::new([7u8; 32]);
    let heads = RemoteHeads {
        counter: 10,
        heads: vec![CommitId::new([0xCC; 32])],
    };

    notifier.notify(doc, peer, heads.clone()).await;
    notifier.notify(doc, other, heads.clone()).await;
    assert_eq!(observer.calls(), 2, "each peer reports once");

    // Unchanged heads from a live peer stay filtered.
    notifier
        .notify(
            doc,
            peer,
            RemoteHeads {
                counter: 11,
                ..heads.clone()
            },
        )
        .await;
    assert_eq!(observer.calls(), 2);

    notifier.remove_peer(peer).await;

    notifier
        .notify(
            doc,
            peer,
            RemoteHeads {
                counter: 12,
                ..heads.clone()
            },
        )
        .await;
    assert_eq!(observer.calls(), 3, "a new session should report once");

    // Forgetting one peer must not forget the others.
    notifier
        .notify(
            doc,
            other,
            RemoteHeads {
                counter: 13,
                ..heads
            },
        )
        .await;
    assert_eq!(observer.calls(), 3, "the other peer's state survived");
}

/// `RemoteHeads::default()` is what `NotFound` and `Unauthorized` responses
/// carry: counter zero and no heads. It says nothing about the peer's state,
/// so it must not reach the observer — nor record anything that would make a
/// later, real update look stale.
#[tokio::test]
async fn counterless_updates_are_ignored() {
    use subduction_core::remote_heads::FilteredHeadsNotifier;

    let observer = CountingObserver::default();
    let notifier = FilteredHeadsNotifier::new(observer.clone());

    let peer = PeerId::new([4u8; 32]);
    let doc = SedimentreeId::new([5u8; 32]);

    notifier.notify(doc, peer, RemoteHeads::default()).await;
    assert_eq!(observer.calls(), 0, "a counterless update said nothing");

    notifier
        .notify(
            doc,
            peer,
            RemoteHeads {
                counter: 1,
                heads: vec![CommitId::new([1u8; 32])],
            },
        )
        .await;
    assert_eq!(
        observer.calls(),
        1,
        "the counterless update must not have recorded state that filters a real one"
    );
}

/// `remove_peer` has to be reached through `Handler::on_peer_disconnect` for
/// any of this to hold in a running node. Tearing a peer down through the
/// public API and reconnecting must report its heads again — the direct
/// `remove_peer` test cannot see whether the hook is wired, and proactive
/// disconnects historically skipped it.
#[tokio::test]
async fn disconnect_through_the_api_clears_filter_state() -> TestResult {
    let observer = CountingObserver::default();
    let a = node(5, observer.clone());
    let b = node(6, CountingObserver::default());
    connect(&a, 5, &b, 6).await?;

    let doc = SedimentreeId::new([3u8; 32]);
    let peer_b = PeerId::from(signer(6).verifying_key());

    b.add_commit(
        doc,
        CommitId::new([1u8; 32]),
        BTreeSet::new(),
        Blob::new(b"before".to_vec()),
    )
    .await?;
    a.sync_with_peer(&peer_b, doc, true, CallTimeout::TimeoutMillis(500))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = observer.calls();
    assert!(before >= 1);

    // Proactive teardown, the path an embedder drives.
    a.disconnect_from_peer(&peer_b).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Reconnect and sync the same, unchanged tree.
    connect(&a, 5, &b, 6).await?;
    a.sync_with_peer(&peer_b, doc, true, CallTimeout::TimeoutMillis(500))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        observer.calls() > before,
        "after a disconnect the peer's heads are news again; \
         got {} notifications before and {} after",
        before,
        observer.calls()
    );
    Ok(())
}

/// Concurrent notifications for one `(peer, tree)` must not leave the observer
/// holding older heads than the filter has recorded: the recorded state is
/// what suppresses future reports, so a stale last delivery is permanent.
#[tokio::test]
async fn concurrent_updates_deliver_in_recorded_order() {
    use std::sync::Arc as StdArc;
    use subduction_core::remote_heads::FilteredHeadsNotifier;

    let observer = CountingObserver::default();
    let notifier = StdArc::new(FilteredHeadsNotifier::new(observer.clone()));

    let peer = PeerId::new([9u8; 32]);
    let doc = SedimentreeId::new([9u8; 32]);

    let mut tasks = Vec::new();
    for counter in 1..=64u64 {
        let notifier = StdArc::clone(&notifier);
        tasks.push(tokio::spawn(async move {
            notifier
                .notify(
                    doc,
                    peer,
                    RemoteHeads {
                        counter,
                        heads: vec![CommitId::new([u8::try_from(counter).unwrap_or(0); 32])],
                    },
                )
                .await;
        }));
    }
    for task in tasks {
        task.await.expect("notify task panicked");
    }

    let reported = observer.reported();
    let last = reported.last().expect("something was reported");
    let highest = reported
        .iter()
        .map(|h| h.counter)
        .max()
        .expect("at least one");
    assert_eq!(
        last.counter,
        highest,
        "the observer's final view is stale: last delivered {} but saw {} \
         (sequence: {:?})",
        last.counter,
        highest,
        reported.iter().map(|h| h.counter).collect::<Vec<_>>()
    );
}
