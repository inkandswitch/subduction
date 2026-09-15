//! Laws for subscription propagation and requester-side push.
//!
//! - `upstream_peers` selects exactly the non-originator peers with a dialed
//!   connection, regardless of how many other connections they have.
//! - Requester-side ingest reports each head once: repeats within a response
//!   and items already in the tree are not pushed.
//!
//! The `build_pushes` law lives as a unit test in `subduction::peers`, where
//! the crate-private types are visible.

#![cfg(feature = "bolero")]
#![allow(clippy::expect_used, clippy::panic, clippy::indexing_slicing)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{
        message::{RequestedData, SyncDiff, SyncMessage},
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder, upstream_peers},
    test_utils::subscribe_request,
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};

const fn peer(seed: u8) -> PeerId {
    PeerId::new([seed; 32])
}

/// `upstream_peers(peers, originator) == { p ≠ originator : Dialed ∈ dirs(p) }`.
#[test]
fn prop_upstream_peers_is_dialed_minus_originator() {
    bolero::check!()
        .with_arbitrary::<(BTreeMap<u8, Vec<bool>>, u8)>()
        .for_each(|(table, originator)| {
            let originator = peer(*originator);
            let input = table.iter().map(|(seed, dialed)| {
                (
                    peer(*seed),
                    dialed.iter().map(|d| {
                        if *d {
                            Direction::Dialed
                        } else {
                            Direction::Accepted
                        }
                    }),
                )
            });
            let got: BTreeSet<PeerId> = upstream_peers(input, originator).into_iter().collect();

            let want: BTreeSet<PeerId> = table
                .iter()
                .filter(|(seed, dialed)| peer(**seed) != originator && dialed.contains(&true))
                .map(|(seed, _)| peer(*seed))
                .collect();
            assert_eq!(got, want);
        });
}

type Conn = ChannelMockConnection<SyncMessage>;

type Node = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

fn signed_commit(id: SedimentreeId, head: u8) -> (Signed<LooseCommit>, Blob) {
    let blob = Blob::new(vec![head; 8]);
    let commit = LooseCommit::new(
        id,
        CommitId::new([head; 32]),
        BTreeSet::new(),
        BlobMeta::new(&blob),
    );
    let signed = futures::executor::block_on(Signed::seal::<Sendable, _>(
        &MemorySigner::from_bytes(&[7u8; 32]),
        commit,
    ))
    .into_signed();
    (signed, blob)
}

/// Requester-side ingest reports `unique(response) \ already_present`, so a
/// subscriber receives exactly one frame per genuinely new head.
///
/// Fewer iterations than the pure laws: each case spins up a node.
#[test]
fn prop_ingest_pushes_each_new_head_once() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let id = SedimentreeId::new([2u8; 32]);
    let pool: Vec<_> = (0..6u8).map(|h| signed_commit(id, h)).collect();

    bolero::check!()
        .with_iterations(60)
        .with_arbitrary::<(Vec<u8>, Vec<u8>)>()
        .for_each(|(present, response)| {
            let present: BTreeSet<usize> = present.iter().map(|h| usize::from(*h % 6)).collect();
            let response: Vec<usize> = response
                .iter()
                .take(12)
                .map(|h| usize::from(*h % 6))
                .collect();
            let expected = response
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .difference(&present)
                .count();

            rt.block_on(async {
                let (node, _h, listener, manager): (Node, _, _, _) =
                    SubductionBuilder::<_, _, _, _, _, _, 256>::new()
                        .signer(MemorySigner::from_bytes(&[3u8; 32]))
                        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
                        .spawner(TokioSpawn)
                        .timer(InstantTimeout)
                        .build::<Sendable, Conn>();
                let listener = tokio::spawn(listener);
                let manager = tokio::spawn(manager);

                for h in &present {
                    let (_, blob) = &pool[*h];
                    let head = u8::try_from(*h).expect("pool index fits in u8");
                    node.store_commit(id, CommitId::new([head; 32]), BTreeSet::new(), blob.clone())
                        .await
                        .expect("store");
                }

                // A subscriber, established over the wire; its outbound side
                // is where we count pushes. The subscribe's own response is
                // drained first.
                let sub_peer = peer(100);
                let (sub_conn, sub) = Conn::new_with_handle(sub_peer);
                node.add_connection(Authenticated::new_for_test(
                    sub_conn,
                    sub_peer,
                    Direction::Accepted,
                ))
                .await
                .expect("add subscriber");
                sub.inbound_tx
                    .send(subscribe_request(sub_peer, id))
                    .await
                    .expect("subscribe");
                while node.get_subscribers(id).await.is_empty() {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                while tokio::time::timeout(Duration::from_millis(30), sub.outbound_rx.recv())
                    .await
                    .is_ok()
                {}

                let diff = SyncDiff {
                    missing_commits: response.iter().map(|h| pool[*h].clone()).collect(),
                    missing_fragments: Vec::new(),
                    requesting: RequestedData::default(),
                };
                node.recv_batch_sync_response(&peer(101), id, diff)
                    .await
                    .expect("ingest");

                let mut frames = 0;
                while let Ok(Ok(msg)) =
                    tokio::time::timeout(Duration::from_millis(50), sub.outbound_rx.recv()).await
                {
                    if matches!(msg, SyncMessage::LooseCommit { .. }) {
                        frames += 1;
                    }
                }
                assert_eq!(
                    frames, expected,
                    "present={present:?} response={response:?}"
                );
                listener.abort();
                manager.abort();
            });
        });
}
