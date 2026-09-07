//! Regression coverage for duplicate Keyhive sync responses.
#![cfg(feature = "test-utils")]

use subduction_keyhive::{
    SyncStatus,
    test_utils::{create_channel_pair, make_keyhive, make_protocol_with_shared_keyhive},
};

#[tokio::test(flavor = "current_thread")]
async fn duplicate_sync_response_after_request_completion_is_ignored() {
    let (alice_proto, alice_kh, _) = make_protocol_with_shared_keyhive(make_keyhive().await).await;
    let alice_id = alice_proto.peer_id().clone();
    let (bob_proto, bob_kh, _) = make_protocol_with_shared_keyhive(make_keyhive().await).await;
    let bob_id = bob_proto.peer_id().clone();

    let alice_cc = alice_kh.contact_card().await.unwrap();
    let bob_cc = bob_kh.contact_card().await.unwrap();
    alice_kh.receive_contact_card(&bob_cc).await.unwrap();
    bob_kh.receive_contact_card(&alice_cc).await.unwrap();

    let (alice_conn, bob_conn) = create_channel_pair(alice_id.clone(), &bob_id);
    alice_proto
        .add_peer(bob_id.clone(), alice_conn.clone())
        .await;
    bob_proto.add_peer(alice_id.clone(), bob_conn.clone()).await;

    alice_proto.sync_keyhive(Some(&bob_id)).await.unwrap();
    let request = bob_conn.inbound_rx.recv().await.unwrap();
    bob_proto
        .handle_message(&alice_id, request, None)
        .await
        .unwrap();
    let response = alice_conn.inbound_rx.recv().await.unwrap();

    alice_proto
        .handle_message(&bob_id, response.clone(), None)
        .await
        .unwrap();
    let status = alice_proto
        .handle_message(&bob_id, response, None)
        .await
        .unwrap();

    assert!(matches!(status, SyncStatus::Done { changed: false, .. }));
    assert!(alice_conn.inbound_rx.try_recv().is_err());
}
