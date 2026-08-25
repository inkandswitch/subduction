//! The first multithreaded driver test: two full stacks on a tokio
//! multi-thread runtime (`Sendable` form throughout — the drivers are
//! `tokio::spawn`ed, so their futures must be `Send`), syncing over a
//! memory transport into durable redb storage.
#![allow(clippy::wildcard_enum_match_arm)] // tests match a growing event enum on purpose

use std::collections::BTreeSet;

use ed25519_dalek::SigningKey;
use future_form::Sendable;
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_protocol::{
    command::NewCommit,
    effect::{AppEvent, SyncStatus},
    event::Direction,
    handshake::audience::Audience,
    node::NodeConfig,
    peer_id::PeerId,
};
use subduction_redb_storage::storage::RedbStorage;
use subduction_runtime::{
    driver::{Driver, handle::Handle},
    memory::{policy::AllowAll, transport::MemoryTransport},
};
use subduction_tokio::clock::TokioClock;
use testresult::TestResult;

type SendableDriver =
    Driver<Sendable, MemoryTransport, RedbStorage, AllowAll, MemorySigner, TokioClock>;

struct Stack {
    handle: Handle<MemoryTransport>,
    storage: RedbStorage,
    peer: PeerId,
}

fn stack(seed: u8, dir: &std::path::Path) -> Result<(SendableDriver, Stack), String> {
    let signing_key = SigningKey::from_bytes(&[seed; 32]);
    let peer = PeerId::from(signing_key.verifying_key());
    let storage =
        RedbStorage::open(dir.join(format!("node-{seed}.redb"))).map_err(|e| e.to_string())?;
    let (driver, handle) = SendableDriver::new(
        NodeConfig::new(peer, [seed ^ 0x55; 32]),
        TokioClock::new(),
        MemorySigner::from_bytes(&[seed; 32]),
        storage.clone(),
        AllowAll,
    );
    Ok((
        driver,
        Stack {
            handle,
            storage,
            peer,
        },
    ))
}

async fn wait_for<F, T>(stack: &Stack, mut matcher: F) -> Result<T, String>
where
    F: FnMut(&AppEvent) -> Option<T>,
{
    for _ in 0..256 {
        let event = stack
            .handle
            .next_app_event()
            .await
            .map_err(|e| e.to_string())?;
        if let Some(found) = matcher(&event) {
            return Ok(found);
        }
    }
    Err("expected app event never arrived".into())
}

async fn stored_commit_ids(storage: &RedbStorage, tree: SedimentreeId) -> Vec<CommitId> {
    use subduction_runtime::storage::Storage;

    let wanted: Vec<CommitId> = (0..=0xFFu8).map(|b| CommitId::new([b; 32])).collect();
    Storage::<Sendable>::fetch_items(storage, tree, wanted, vec![])
        .await
        .ok()
        .flatten()
        .map(|items| {
            let mut ids: Vec<CommitId> = items
                .commits
                .iter()
                .filter_map(|(signed, _)| {
                    signed
                        .try_decode_trusted_payload()
                        .ok()
                        .map(|commit| commit.head())
                })
                .collect();
            ids.sort_unstable();
            ids
        })
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sendable_stacks_sync_into_redb() -> TestResult {
    let dir = tempfile::tempdir()?;
    let tree = SedimentreeId::new([7u8; 32]);

    let (driver_a, a) = stack(1, dir.path())?;
    let (driver_b, b) = stack(2, dir.path())?;
    // The whole point: these futures are Send, so plain tokio::spawn.
    let _driver_a_task = tokio::spawn(driver_a.run());
    let _driver_b_task = tokio::spawn(driver_b.run());

    let (ta, tb) = MemoryTransport::pair();
    let (pending_a, pump_a) = a
        .handle
        .connect::<Sendable>(ta, Direction::Outbound, Some(Audience::known(b.peer)))
        .await?;
    let (pending_b, pump_b) = b
        .handle
        .connect::<Sendable>(tb, Direction::Inbound, None)
        .await?;
    let _pump_a_task = tokio::spawn(pump_a);
    let _pump_b_task = tokio::spawn(pump_b);

    let conn_a = pending_a.authenticated().await?;
    assert_eq!(conn_a.peer(), b.peer);
    let _conn_b = pending_b.authenticated().await?;

    // B writes durably; A syncs it out.
    b.handle
        .add_commits(
            tree,
            vec![NewCommit {
                head: CommitId::new([0xA1; 32]),
                parents: BTreeSet::new(),
                blob: Blob::new(vec![0xA1; 16]),
            }],
        )
        .await?;
    wait_for(&b, |event| match event {
        AppEvent::CommitsStored { tree: t, .. } if *t == tree => Some(()),
        _ => None,
    })
    .await?;

    conn_a.sync_tree(tree, true).await?;
    let status = wait_for(&a, |event| match event {
        AppEvent::SyncFinished {
            tree: t, status, ..
        } if *t == tree => Some(*status),
        _ => None,
    })
    .await?;
    assert_eq!(status, SyncStatus::Completed);
    assert_eq!(
        stored_commit_ids(&a.storage, tree).await,
        vec![CommitId::new([0xA1; 32])],
        "sync persisted b's commit in a's redb"
    );

    // Live push into durable storage.
    b.handle
        .add_commits(
            tree,
            vec![NewCommit {
                head: CommitId::new([0xA2; 32]),
                parents: BTreeSet::new(),
                blob: Blob::new(vec![0xA2; 16]),
            }],
        )
        .await?;
    while !stored_commit_ids(&a.storage, tree)
        .await
        .contains(&CommitId::new([0xA2; 32]))
    {
        wait_for(&a, |event| match event {
            AppEvent::TreeUpdated { tree: t, .. } if *t == tree => Some(()),
            _ => None,
        })
        .await?;
    }

    Ok(())
}
