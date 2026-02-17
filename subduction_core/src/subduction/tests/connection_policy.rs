//! Tests for connection authorization policy.

#![allow(clippy::expect_used)]

use super::common::{TestSpawn, new_test_subduction, test_signer};
use crate::{
    connection::{nonce_cache::NonceCache, test_utils::MockConnection},
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};
use alloc::vec::Vec;
use core::fmt;
use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::{commit::CountLeadingZeroBytes, id::SedimentreeId};
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
    type FetchDisallowed = core::convert::Infallible;
    type PutDisallowed = core::convert::Infallible;

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
    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            None,
            test_signer(),
            MemoryStorage::new(),
            RejectConnectionPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TestSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    let peer_id = PeerId::new([1u8; 32]);
    let conn = MockConnection::with_peer_id(peer_id);

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
    let allowed_conn = MockConnection::with_peer_id(allowed_peer);
    subduction.register(allowed_conn).await?;

    let connected = subduction.connected_peer_ids().await;
    assert_eq!(connected.len(), 1);
    assert!(connected.contains(&allowed_peer));

    // Now create a subduction with reject policy and try to register
    let (reject_subduction, _listener_fut2, _actor_fut2) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            None,
            test_signer(),
            MemoryStorage::new(),
            RejectConnectionPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TestSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    let rejected_peer = PeerId::new([2u8; 32]);
    let rejected_conn = MockConnection::with_peer_id(rejected_peer);
    let result = reject_subduction.register(rejected_conn).await;
    assert!(result.is_err());

    // The original subduction's connections are unaffected
    let still_connected = subduction.connected_peer_ids().await;
    assert_eq!(still_connected.len(), 1);
    assert!(still_connected.contains(&allowed_peer));

    Ok(())
}
