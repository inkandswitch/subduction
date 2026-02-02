//! Diagnostic tests to investigate the "one behind" issue.
//!
//! The "one behind" bug manifests as: new sedimentrees don't appear until
//! the NEXT one is created. These tests inject messages through channels
//! to observe timing and storage behavior.
//!
//! Tests run for both `Sendable` and `Local` future kinds to ensure
//! behavior is consistent across native and WASM-like environments.

use super::common::{TokioSpawn, test_signer};
use crate::{
    Subduction,
    connection::{message::Message, nonce_cache::NonceCache, test_utils::ChannelMockConnection},
    crypto::signed::Signed,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
};
use core::time::Duration;
use future_form::{Local, Sendable};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    digest::Digest,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};
use testresult::TestResult;

async fn make_test_commit_with_data(data: &[u8]) -> (Signed<LooseCommit>, Blob) {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(data);
    let digest = Digest::<LooseCommit>::hash_bytes(data);
    let commit = LooseCommit::new(digest, vec![], blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
    (verified.into_signed(), blob)
}

#[tokio::test]
async fn test_sendable_single_commit() -> TestResult {
    let storage = MemoryStorage::new();
    let (subduction, listener_fut, actor_fut) =
        Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TokioSpawn,
        );

    let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
    subduction.register(conn).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let (commit, blob) = make_test_commit_with_data(b"test commit").await;

    handle
        .inbound_tx
        .send(Message::LooseCommit {
            id: sedimentree_id,
            commit,
            blob,
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let ids = subduction.sedimentree_ids().await;
    assert!(
        ids.contains(&sedimentree_id),
        "[SENDABLE] Sedimentree should be visible. Found: {ids:?}"
    );
    assert_eq!(
        subduction
            .get_commits(sedimentree_id)
            .await
            .map(|c| c.len()),
        Some(1)
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn test_sendable_multiple_sequential() -> TestResult {
    let storage = MemoryStorage::new();
    let (subduction, listener_fut, actor_fut) =
        Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TokioSpawn,
        );

    let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
    subduction.register(conn).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    for i in 0..3u8 {
        let sedimentree_id = SedimentreeId::new([i; 32]);
        let (commit, blob) = make_test_commit_with_data(format!("commit {i}").as_bytes()).await;

        handle
            .inbound_tx
            .send(Message::LooseCommit {
                id: sedimentree_id,
                commit,
                blob,
            })
            .await?;

        tokio::time::sleep(Duration::from_millis(20)).await;

        let ids = subduction.sedimentree_ids().await;
        assert_eq!(
            ids.len(),
            (i + 1) as usize,
            "[SENDABLE] After commit {i}: expected {} sedimentrees, found {}. ONE BEHIND BUG!",
            i + 1,
            ids.len()
        );
        assert!(ids.contains(&sedimentree_id));
    }

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn test_sendable_same_sedimentree() -> TestResult {
    let storage = MemoryStorage::new();
    let (subduction, listener_fut, actor_fut) =
        Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TokioSpawn,
        );

    let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
    subduction.register(conn).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([99u8; 32]);

    for i in 0..3usize {
        let (commit, blob) = make_test_commit_with_data(format!("commit {i}").as_bytes()).await;

        handle
            .inbound_tx
            .send(Message::LooseCommit {
                id: sedimentree_id,
                commit,
                blob,
            })
            .await?;

        tokio::time::sleep(Duration::from_millis(20)).await;

        let count = subduction
            .get_commits(sedimentree_id)
            .await
            .map_or(0, |c| c.len());
        assert_eq!(
            count,
            i + 1,
            "[SENDABLE] After commit {i}, expected {} commits but found {}. DELAYED!",
            i + 1,
            count
        );
    }

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn test_local_single_commit() -> TestResult {
    tokio::task::LocalSet::new()
        .run_until(async {
            let storage = MemoryStorage::new();
            let (subduction, listener_fut, actor_fut) =
                Subduction::<'_, Local, _, ChannelMockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    CountLeadingZeroBytes,
                    ShardedMap::with_key(0, 0),
                    TokioSpawn,
                );

            let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
            subduction.register(conn).await?;

            let actor_task = tokio::task::spawn_local(actor_fut);
            let listener_task = tokio::task::spawn_local(listener_fut);
            tokio::time::sleep(Duration::from_millis(10)).await;

            let sedimentree_id = SedimentreeId::new([42u8; 32]);
            let (commit, blob) = make_test_commit_with_data(b"test commit").await;

            handle
                .inbound_tx
                .send(Message::LooseCommit {
                    id: sedimentree_id,
                    commit,
                    blob,
                })
                .await?;

            tokio::time::sleep(Duration::from_millis(50)).await;

            let ids = subduction.sedimentree_ids().await;
            assert!(
                ids.contains(&sedimentree_id),
                "[LOCAL] Sedimentree should be visible. Found: {ids:?}"
            );
            assert_eq!(
                subduction
                    .get_commits(sedimentree_id)
                    .await
                    .map(|c| c.len()),
                Some(1)
            );

            actor_task.abort();
            listener_task.abort();
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_local_multiple_sequential() -> TestResult {
    tokio::task::LocalSet::new()
        .run_until(async {
            let storage = MemoryStorage::new();
            let (subduction, listener_fut, actor_fut) =
                Subduction::<'_, Local, _, ChannelMockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    CountLeadingZeroBytes,
                    ShardedMap::with_key(0, 0),
                    TokioSpawn,
                );

            let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
            subduction.register(conn).await?;

            let actor_task = tokio::task::spawn_local(actor_fut);
            let listener_task = tokio::task::spawn_local(listener_fut);
            tokio::time::sleep(Duration::from_millis(10)).await;

            for i in 0..3u8 {
                let sedimentree_id = SedimentreeId::new([i; 32]);
                let (commit, blob) =
                    make_test_commit_with_data(format!("commit {i}").as_bytes()).await;

                handle
                    .inbound_tx
                    .send(Message::LooseCommit {
                        id: sedimentree_id,
                        commit,
                        blob,
                    })
                    .await?;

                tokio::time::sleep(Duration::from_millis(20)).await;

                let ids = subduction.sedimentree_ids().await;
                assert_eq!(
                    ids.len(),
                    (i + 1) as usize,
                    "[LOCAL] After commit {i}: expected {} sedimentrees, found {}. ONE BEHIND BUG!",
                    i + 1,
                    ids.len()
                );
                assert!(ids.contains(&sedimentree_id));
            }

            actor_task.abort();
            listener_task.abort();
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_local_same_sedimentree() -> TestResult {
    tokio::task::LocalSet::new()
        .run_until(async {
            let storage = MemoryStorage::new();
            let (subduction, listener_fut, actor_fut) =
                Subduction::<'_, Local, _, ChannelMockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    CountLeadingZeroBytes,
                    ShardedMap::with_key(0, 0),
                    TokioSpawn,
                );

            let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
            subduction.register(conn).await?;

            let actor_task = tokio::task::spawn_local(actor_fut);
            let listener_task = tokio::task::spawn_local(listener_fut);
            tokio::time::sleep(Duration::from_millis(10)).await;

            let sedimentree_id = SedimentreeId::new([99u8; 32]);

            for i in 0..3usize {
                let (commit, blob) =
                    make_test_commit_with_data(format!("commit {i}").as_bytes()).await;

                handle
                    .inbound_tx
                    .send(Message::LooseCommit {
                        id: sedimentree_id,
                        commit,
                        blob,
                    })
                    .await?;

                tokio::time::sleep(Duration::from_millis(20)).await;

                let count = subduction
                    .get_commits(sedimentree_id)
                    .await
                    .map_or(0, |c| c.len());
                assert_eq!(
                    count,
                    i + 1,
                    "[LOCAL] After commit {i}, expected {} commits but found {}. DELAYED!",
                    i + 1,
                    count
                );
            }

            actor_task.abort();
            listener_task.abort();
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await?;
    Ok(())
}
