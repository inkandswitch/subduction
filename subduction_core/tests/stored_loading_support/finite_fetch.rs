//! Finite loading must not queue responsive peers behind stalled requests.
use super::{connect, node};
use future_form::Sendable;
use futures::StreamExt;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use subduction_core::{
    connection::{
        message::{BatchSyncResponse, RequestedData, SyncDiff, SyncMessage, SyncResult},
        test_utils::{ChannelMockConnection, TokioSpawn, TokioTimeout},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    stored::{LoadError, StoredEvent, StoredItem},
    subduction::{NodeTasks, builder::SubductionBuilder},
    timeout::call::CallTimeout,
};
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn finite_fetch_reaches_supplier_despite_four_stalled_candidates() {
    for timeout in [CallTimeout::Uncapped, CallTimeout::TimeoutMillis(1_000)] {
        let (a, _, listener, manager) = SubductionBuilder::new()
            .signer(MemorySigner::from_bytes(&[110; 32]))
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(TokioTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();
        tokio::spawn(listener);
        tokio::spawn(manager);
        let mut handles = Vec::new();
        for seed in 111..116 {
            let peer_id = PeerId::new([seed; 32]);
            let (connection, handle) = ChannelMockConnection::new_with_handle(peer_id);
            a.add_connection(connection.authenticated())
                .await
                .expect("register candidate");
            handles.push((peer_id, handle));
        }
        let id = SedimentreeId::new([110; 32]);
        let mut stream = Sendable::start_stored(a.clone(), id, timeout, false);
        assert!(matches!(
            super::super::next(&mut stream).await,
            StoredEvent::StorageChecked
        ));
        let mut requests = Vec::new();
        // Require all five requests before any response. This detects a cap of
        // four regardless of the candidate hash set's iteration order.
        tokio::time::timeout(Duration::from_secs(3), async {
            for (_, handle) in &handles {
                let SyncMessage::BatchSyncRequest(request) =
                    handle.outbound_rx.recv().await.expect("request")
                else {
                    panic!("expected sync request");
                };
                assert!(!request.subscribe, "finite fetching must not subscribe");
                requests.push(request);
            }
        })
        .await
        .expect("every candidate must be queried without waiting for another to respond");

        // Only one peer responds; the other four stay connected and stalled.
        let (supplier_id, supplier) = handles.last().expect("supplier");
        let request = requests.last().expect("supplier request");
        let mut commits = Vec::new();
        for seed in 1..=3 {
            let verified = VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
                &MemorySigner::from_bytes(&[115; 32]),
                (id, CommitId::new([seed; 32]), BTreeSet::new()),
                VerifiedBlobMeta::new(Blob::new(vec![seed])),
            )
            .await
            .expect("signed data");
            commits.push((verified.signed().clone(), verified.blob().clone()));
        }
        supplier
            .inbound_tx
            .send(SyncMessage::BatchSyncResponse(BatchSyncResponse {
                id,
                req_id: request.req_id,
                responder_heads: RemoteHeads::default(),
                result: SyncResult::Ok(SyncDiff {
                    missing_commits: commits,
                    missing_fragments: vec![],
                    requesting: RequestedData::default(),
                }),
            }))
            .await
            .expect("supplier response");
        let mut blobs = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match stream
                    .next()
                    .await
                    .expect("finite load event")
                    .expect("load")
                {
                    StoredEvent::Item(StoredItem::Commit(commit)) => {
                        blobs.push(commit.blob().as_slice().to_vec());
                    }
                    StoredEvent::PeerSynced { peer_id } => {
                        assert_eq!(peer_id, *supplier_id);
                        break;
                    }
                    event @ (StoredEvent::Item(StoredItem::Fragment(_))
                    | StoredEvent::StorageChecked
                    | StoredEvent::RescanRequired) => panic!("unexpected event: {event:?}"),
                }
            }
        })
        .await
        .expect("supplier must complete while the other four candidates remain stalled");
        blobs.sort();
        assert_eq!(blobs, vec![vec![1], vec![2], vec![3]]);
        assert!(a.get_peer_subscriptions(*supplier_id).await.is_empty());
        stream.close();
        assert!(stream.next().await.is_none());
        a.shutdown();
    }
}

#[tokio::test]
async fn finite_fetch_still_obeys_its_overall_deadline() {
    let a = node(116, MemoryStorage::new());
    let b = node(117, MemoryStorage::new());
    connect(&a, &b).await.pause();
    let mut stream = Sendable::start_stored(
        a.clone(),
        SedimentreeId::new([116; 32]),
        CallTimeout::TimeoutMillis(100),
        false,
    );
    let error = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match stream.next().await.expect("deadline terminates loading") {
                Ok(StoredEvent::StorageChecked) => {}
                Err(error) => break error,
                event => panic!("unexpected event: {event:?}"),
            }
        }
    })
    .await
    .expect("overall deadline must interrupt stalled rounds");
    assert!(matches!(error, LoadError::TimedOut));
    assert!(stream.next().await.is_none());
    a.shutdown();
    b.shutdown();
}
