//! Tests for policy rejection behavior.

use super::common::{TestSpawn, test_signer};
use crate::{
    connection::{nonce_cache::NonceCache, test_utils::MockConnection},
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::Subduction,
};
use alloc::{vec, vec::Vec};
use core::fmt;
use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    crypto::digest::Digest,
    id::SedimentreeId,
    sedimentree::Sedimentree,
};
use testresult::TestResult;

/// A policy that rejects all puts but allows connections and fetches.
#[derive(Clone, Copy)]
struct RejectPutsPolicy;

/// Error returned when a put is rejected.
#[derive(Debug, Clone, Copy)]
struct PutRejected;

impl fmt::Display for PutRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "put rejected by policy")
    }
}

impl core::error::Error for PutRejected {}

impl ConnectionPolicy<Sendable> for RejectPutsPolicy {
    type ConnectionDisallowed = core::convert::Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async { Ok(()) }.boxed()
    }
}

impl StoragePolicy<Sendable> for RejectPutsPolicy {
    type FetchDisallowed = core::convert::Infallible;
    type PutDisallowed = PutRejected;

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
        async { Err(PutRejected) }.boxed()
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async move { ids }.boxed()
    }
}

/// A policy that allows puts only to a specific sedimentree ID.
#[derive(Clone)]
struct AllowSpecificIdPolicy {
    allowed_id: SedimentreeId,
}

impl ConnectionPolicy<Sendable> for AllowSpecificIdPolicy {
    type ConnectionDisallowed = core::convert::Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async { Ok(()) }.boxed()
    }
}

impl StoragePolicy<Sendable> for AllowSpecificIdPolicy {
    type FetchDisallowed = core::convert::Infallible;
    type PutDisallowed = PutRejected;

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
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        let allowed = self.allowed_id;
        async move {
            if sedimentree_id == allowed {
                Ok(())
            } else {
                Err(PutRejected)
            }
        }
        .boxed()
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async move { ids }.boxed()
    }
}

fn make_test_blob(data: &[u8]) -> Blob {
    Blob::new(data.to_vec())
}

fn make_loose_commit(data: &[u8]) -> (sedimentree_core::loose_commit::LooseCommit, Blob) {
    let blob = make_test_blob(data);
    let blob_meta = BlobMeta::new(blob.as_slice());
    let content_digest = Digest::<sedimentree_core::loose_commit::LooseCommit>::hash_bytes(data);
    let commit =
        sedimentree_core::loose_commit::LooseCommit::new(content_digest, vec![], blob_meta);
    (commit, blob)
}

#[tokio::test]
async fn add_sedimentree_rejected_by_policy() {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            RejectPutsPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
        );

    let id = SedimentreeId::new([1u8; 32]);
    let tree = Sedimentree::default();
    let blobs = Vec::new();

    let result = subduction.add_sedimentree(id, tree, blobs).await;

    // Should fail with PutDisallowed
    assert!(result.is_err());
    #[allow(clippy::unwrap_used)]
    let err = result.unwrap_err();
    let err_string = format!("{err}");
    assert!(
        err_string.contains("put disallowed"),
        "Expected 'put disallowed' error, got: {err_string}"
    );
}

#[tokio::test]
async fn add_commit_rejected_by_policy() {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            RejectPutsPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
        );

    let id = SedimentreeId::new([1u8; 32]);
    let (commit, blob) = make_loose_commit(b"test data");

    let result = subduction.add_commit(id, &commit, blob).await;

    // Should fail with PutDisallowed
    assert!(result.is_err());
    #[allow(clippy::unwrap_used)]
    let err = result.unwrap_err();
    let err_string = format!("{err}");
    assert!(
        err_string.contains("put disallowed"),
        "Expected 'put disallowed' error, got: {err_string}"
    );
}

#[tokio::test]
async fn policy_allows_specific_sedimentree_id() -> TestResult {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let allowed_id = SedimentreeId::new([42u8; 32]);
    let disallowed_id = SedimentreeId::new([99u8; 32]);

    let policy = AllowSpecificIdPolicy { allowed_id };

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            policy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
        );

    // Adding to allowed ID should succeed
    let tree = Sedimentree::default();
    let result = subduction
        .add_sedimentree(allowed_id, tree.clone(), Vec::new())
        .await;
    assert!(result.is_ok(), "Should allow adding to allowed ID");

    // Adding to disallowed ID should fail
    let result = subduction
        .add_sedimentree(disallowed_id, tree, Vec::new())
        .await;
    assert!(result.is_err(), "Should reject adding to disallowed ID");

    // Verify only allowed ID is stored
    let ids = subduction.sedimentree_ids().await;
    assert_eq!(ids.len(), 1);
    assert!(ids.contains(&allowed_id));
    assert!(!ids.contains(&disallowed_id));

    Ok(())
}

#[tokio::test]
async fn policy_rejection_does_not_store_data() {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            RejectPutsPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
        );

    let id = SedimentreeId::new([1u8; 32]);
    let tree = Sedimentree::default();

    // Attempt to add (should fail)
    let _ = subduction.add_sedimentree(id, tree, Vec::new()).await;

    // Verify nothing was stored
    let ids = subduction.sedimentree_ids().await;
    assert!(ids.is_empty(), "No sedimentree IDs should be stored");

    let commits = subduction.get_commits(id).await;
    assert!(
        commits.is_none(),
        "No commits should exist for rejected sedimentree"
    );
}

#[tokio::test]
async fn multiple_rejections_all_fail_cleanly() {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            RejectPutsPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
        );

    // Try multiple operations - all should fail
    for i in 0..5u8 {
        let id = SedimentreeId::new([i; 32]);
        let tree = Sedimentree::default();
        let result = subduction.add_sedimentree(id, tree, Vec::new()).await;
        assert!(result.is_err(), "Attempt {i} should be rejected");
    }

    // Verify nothing was stored
    let ids = subduction.sedimentree_ids().await;
    assert!(ids.is_empty(), "No sedimentree IDs should be stored");
}
