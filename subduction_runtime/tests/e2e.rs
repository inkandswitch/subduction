//! End-to-end driver test: two full stacks (node + driver + memory
//! transport/storage) handshake, sync, and push — no locks, no tokio,
//! single-threaded `LocalPool`.
#![allow(clippy::wildcard_enum_match_arm)] // tests match a growing event enum on purpose

mod common;

use common::{commit, stack, wait_for};
use future_form::Local;
use futures::{executor::LocalPool, task::LocalSpawnExt as _};
use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};
use subduction_protocol::{
    effect::{AppEvent, SyncStatus},
    event::Direction,
    handshake::audience::Audience,
};
use subduction_runtime::memory::MemoryTransport;
use testresult::TestResult;

#[test]
#[allow(clippy::too_many_lines)] // one linear scenario
fn two_stacks_handshake_sync_and_push() -> TestResult {
    let mut pool = LocalPool::new();
    let spawner = pool.spawner();

    let (driver_a, a) = stack(1);
    let (driver_b, b) = stack(2);
    spawner.spawn_local(driver_a.run())?;
    spawner.spawn_local(driver_b.run())?;

    let tree = SedimentreeId::new([7u8; 32]);

    let result: Result<(), String> = pool.run_until(async {
        // Wire the two stacks together.
        let (ta, tb) = MemoryTransport::pair();
        let (pending_a, pump_a) = a
            .handle
            .connect::<Local>(ta, Direction::Outbound, Some(Audience::known(b.peer)))
            .await
            .map_err(|e| e.to_string())?;
        let (pending_b, pump_b) = b
            .handle
            .connect::<Local>(tb, Direction::Inbound, None)
            .await
            .map_err(|e| e.to_string())?;
        spawner.spawn_local(pump_a).map_err(|e| e.to_string())?;
        spawner.spawn_local(pump_b).map_err(|e| e.to_string())?;

        // Both sides authenticate: the pending capabilities upgrade.
        let conn_a = pending_a.authenticated().await.map_err(|e| e.to_string())?;
        assert_eq!(conn_a.peer(), b.peer, "a authenticated b");
        let conn_b = pending_b.authenticated().await.map_err(|e| e.to_string())?;
        assert_eq!(conn_b.peer(), a.peer, "b authenticated a");

        // B authors a commit locally (sealed + persisted by its driver).
        b.handle
            .add_commits(tree, vec![commit(0xA1)])
            .await
            .map_err(|e| e.to_string())?;
        wait_for(&b, |event| match event {
            AppEvent::CommitsStored { tree: t, .. } if *t == tree => Some(()),
            _ => None,
        })
        .await?;

        // A syncs the tree from B, subscribing — an operation only the
        // authenticated-connection capability can express.
        conn_a
            .sync_tree(tree, true)
            .await
            .map_err(|e| e.to_string())?;
        let status = wait_for(&a, |event| match event {
            AppEvent::SyncFinished {
                tree: t, status, ..
            } if *t == tree => Some(*status),
            _ => None,
        })
        .await?;
        assert_eq!(status, SyncStatus::Completed);

        // The synced commit is durable on A.
        assert_eq!(
            a.storage.commit_ids(tree),
            vec![CommitId::new([0xA1; 32])],
            "sync persisted b's commit on a"
        );

        // A live push: B authors another commit; A receives it via its
        // subscription without asking.
        b.handle
            .add_commits(tree, vec![commit(0xA2)])
            .await
            .map_err(|e| e.to_string())?;
        // The sync above may have queued its own TreeUpdated; wait until
        // the pushed commit is actually durable.
        while !a
            .storage
            .commit_ids(tree)
            .contains(&CommitId::new([0xA2; 32]))
        {
            wait_for(&a, |event| match event {
                AppEvent::TreeUpdated { tree: t, .. } if *t == tree => Some(()),
                _ => None,
            })
            .await?;
        }
        assert_eq!(
            a.storage.commit_ids(tree),
            vec![CommitId::new([0xA1; 32]), CommitId::new([0xA2; 32])],
            "push persisted b's new commit on a"
        );

        // Resident heads agree with storage.
        let heads = a
            .handle
            .tree_heads(tree)
            .await
            .map_err(|e| e.to_string())?
            .unwrap_or_default();
        let mut heads_sorted = heads;
        heads_sorted.sort_unstable();
        assert_eq!(
            heads_sorted,
            vec![CommitId::new([0xA1; 32]), CommitId::new([0xA2; 32])],
        );

        Ok(())
    });
    result?;
    Ok(())
}
