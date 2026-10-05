//! Stored streams participate in the managed client's lifetime.
#![allow(clippy::unwrap_used)]
use futures::StreamExt;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use std::{collections::BTreeSet, time::Duration};
use subduction_core::{policy::open::OpenPolicy, storage::memory::MemoryStorage};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_tokio::{ErrorKind, LoadError, StoredEvent, TokioClient};

#[tokio::test]
async fn local_loading_stream_lives_until_closed_or_client_shutdown() {
    let owner = TokioClient::builder()
        .storage(MemoryStorage::new())
        .policy(OpenPolicy)
        .signer(MemorySigner::from_bytes(&[20; 32]))
        .start()
        .unwrap();
    let client = owner.handle();
    let id = SedimentreeId::new([22; 32]);
    let blob = Blob::new(vec![1]);
    let commit = LooseCommit::new(
        id,
        CommitId::new([23; 32]),
        BTreeSet::new(),
        BlobMeta::new(&blob),
    )
    .unwrap();
    client
        .store_built_batch(id, vec![(commit, blob)], vec![])
        .await
        .unwrap();
    let mut updates = client.load_and_watch(id).await.unwrap();
    assert!(matches!(
        updates.next().await,
        Some(Ok(StoredEvent::Item(_)))
    ));
    assert!(matches!(
        updates.next().await,
        Some(Ok(StoredEvent::StorageChecked))
    ));
    let mut other = client.watch_stored(id).await.unwrap();
    other.close();
    assert!(other.next().await.is_none());
    let report = tokio::time::timeout(Duration::from_secs(2), owner.close())
        .await
        .unwrap();
    assert!(!report.forced);
    assert!(matches!(updates.next().await, Some(Err(LoadError::Closed))));
    assert!(updates.next().await.is_none());
    assert_eq!(
        client.watch_stored(id).await.unwrap_err().kind(),
        ErrorKind::Closed
    );
    assert_eq!(
        client.load_and_watch(id).await.unwrap_err().kind(),
        ErrorKind::Closed
    );
}
