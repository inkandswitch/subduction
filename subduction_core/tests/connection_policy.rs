//! Tests for connection authorization policy.

#![allow(clippy::expect_used)]

use core::{convert::Infallible, fmt};
use std::sync::Arc;

use async_lock::Mutex;
use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::{collections::Map, commit::CountLeadingZeroBytes, id::SedimentreeId};
use std::vec::Vec;
use subduction_core::{
    connection::{
        nonce_cache::NonceCache,
        test_utils::{MockConnection, TestSpawn, new_test_subduction, test_signer},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    sharded_map::ShardedMap,
    storage::{memory::MemoryStorage, powerbox::StoragePowerbox},
    subduction::{
        Subduction,
        pending_blob_requests::{DEFAULT_MAX_PENDING_BLOB_REQUESTS, PendingBlobRequests},
    },
};
use testresult::TestResult;

#[tokio::test]
async fn test_allowed_to_connect_allows_all_peers() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let peer_id = PeerId::new([1u8; 32]);
    let result = subduction.authorize_connect(peer_id).await;
    assert!(result.is_ok());
}

/// Error returned when a connection is rejected.
#[derive(Debug, Clone, Copy)]
struct ConnectionRejected;

impl fmt::Display for ConnectionRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "connection rejected by policy")
    }
}

impl core::error::Error for ConnectionRejected {}

/// A policy that rejects all connections.
#[derive(Clone, Copy)]
struct RejectConnectionPolicy;

impl ConnectionPolicy<Sendable> for RejectConnectionPolicy {
    type ConnectionDisallowed = ConnectionRejected;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async { Err(ConnectionRejected) }.boxed()
    }
}

impl StoragePolicy<Sendable> for RejectConnectionPolicy {
    type FetchDisallowed = Infallible;
    type PutDisallowed = Infallible;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        async { Ok(()) }.boxed()
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        async { Ok(()) }.boxed()
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async move { ids }.boxed()
    }
}

#[tokio::test]
async fn rejected_connection_is_not_registered() -> TestResult {
    let sedimentrees = Arc::new(ShardedMap::with_key(0, 0));
    let connections = Arc::new(Mutex::new(Map::new()));
    let subscriptions = Arc::new(Mutex::new(Map::new()));
    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(RejectConnectionPolicy));
    let pending = Arc::new(Mutex::new(PendingBlobRequests::new(
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    )));

    let handler = Arc::new(SyncHandler::new(
        sedimentrees.clone(),
        connections.clone(),
        subscriptions.clone(),
        storage.clone(),
        pending.clone(),
        CountLeadingZeroBytes,
    ));

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            handler,
            None,
            test_signer(),
            sedimentrees,
            connections,
            subscriptions,
            storage,
            pending,
            NonceCache::default(),
            CountLeadingZeroBytes,
            TestSpawn,
        );

    let peer_id = PeerId::new([1u8; 32]);
    let conn = MockConnection::with_peer_id(peer_id).authenticated();

    let result = subduction.register(conn).await;
    assert!(result.is_err(), "register should fail when policy rejects");

    let err = result.expect_err("register should have failed");
    let err_string = format!("{err}");
    assert!(
        err_string.contains("connection rejected"),
        "error should indicate connection rejection, got: {err_string}"
    );

    // Verify no connections were registered
    let connected_peers = subduction.connected_peer_ids().await;
    assert!(
        connected_peers.is_empty(),
        "no peers should be connected after rejection"
    );

    Ok(())
}

#[tokio::test]
async fn rejected_connection_does_not_affect_existing_connections() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    // Register an allowed connection first (OpenPolicy allows everything)
    let allowed_peer = PeerId::new([1u8; 32]);
    let allowed_conn = MockConnection::with_peer_id(allowed_peer).authenticated();
    subduction.register(allowed_conn).await?;

    let connected = subduction.connected_peer_ids().await;
    assert_eq!(connected.len(), 1);
    assert!(connected.contains(&allowed_peer));

    // Now create a subduction with reject policy and try to register
    let reject_sedimentrees = Arc::new(ShardedMap::with_key(0, 0));
    let reject_connections = Arc::new(Mutex::new(Map::new()));
    let reject_subscriptions = Arc::new(Mutex::new(Map::new()));
    let reject_storage =
        StoragePowerbox::new(MemoryStorage::new(), Arc::new(RejectConnectionPolicy));
    let reject_pending = Arc::new(Mutex::new(PendingBlobRequests::new(
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    )));

    let reject_handler = Arc::new(SyncHandler::new(
        reject_sedimentrees.clone(),
        reject_connections.clone(),
        reject_subscriptions.clone(),
        reject_storage.clone(),
        reject_pending.clone(),
        CountLeadingZeroBytes,
    ));

    let (reject_subduction, _listener_fut2, _actor_fut2) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            reject_handler,
            None,
            test_signer(),
            reject_sedimentrees,
            reject_connections,
            reject_subscriptions,
            reject_storage,
            reject_pending,
            NonceCache::default(),
            CountLeadingZeroBytes,
            TestSpawn,
        );

    let rejected_peer = PeerId::new([2u8; 32]);
    let rejected_conn = MockConnection::with_peer_id(rejected_peer).authenticated();
    let result = reject_subduction.register(rejected_conn).await;
    assert!(result.is_err());

    // The original subduction's connections are unaffected
    let still_connected = subduction.connected_peer_ids().await;
    assert_eq!(still_connected.len(), 1);
    assert!(still_connected.contains(&allowed_peer));

    Ok(())
}
