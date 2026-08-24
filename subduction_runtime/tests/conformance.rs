//! Certify the reference backend and pin the adversarial layer
//! boundaries: policy denial keeps remote data out of storage.
#![allow(clippy::wildcard_enum_match_arm)] // tests match a growing event enum on purpose

mod common;

use std::rc::Rc;

use common::{commit, stack, stack_with_policy, wait_for};
use future_form::{FutureForm, Local};
use futures::{executor::LocalPool, future::LocalBoxFuture, task::LocalSpawnExt as _};
use sedimentree_core::id::SedimentreeId;
use subduction_protocol::{
    effect::{AppEvent, SyncStatus},
    event::Direction,
    handshake::audience::Audience,
    storage::Provenance,
};
use subduction_runtime::{
    conformance,
    memory::{MemoryStorage, MemoryTransport},
    storage::{Policy, StorageAction, Verdict},
};
use testresult::TestResult;

/// The reference backend passes its own certification bar.
#[test]
fn memory_storage_conforms() -> TestResult {
    let storage = Rc::new(MemoryStorage::new());
    futures::executor::block_on(conformance::certify::<Local, _>(&storage))?;
    Ok(())
}

/// Deny writes from remote peers; allow everything else.
struct DenyRemoteWrites;

impl Policy<Local> for DenyRemoteWrites {
    fn authorize(
        &self,
        provenance: &Provenance,
        _tree: SedimentreeId,
        action: StorageAction,
    ) -> LocalBoxFuture<'_, Verdict> {
        let verdict = match (provenance, action) {
            (Provenance::Remote(_), StorageAction::Write) => Verdict::Deny,
            _ => Verdict::Allow,
        };
        Local::ready(verdict)
    }
}

/// Adversarial layer boundary: a policy-denied remote write is dropped
/// by the driver's executor — the backend never sees it, and nothing
/// becomes durable, even though the wire exchange itself succeeds.
#[test]
fn policy_denied_remote_writes_never_reach_storage() -> TestResult {
    let mut pool = LocalPool::new();
    let spawner = pool.spawner();

    let (writer_driver, writer) = stack(1);
    let (denier_driver, denier) = stack_with_policy(2, DenyRemoteWrites);
    spawner.spawn_local(writer_driver.run())?;
    spawner.spawn_local(denier_driver.run())?;

    let tree = SedimentreeId::new([7u8; 32]);

    let result: Result<(), String> = pool.run_until(async {
        let (tw, td) = MemoryTransport::pair();
        let (pending_w, pump_w) = writer
            .handle
            .connect::<Local>(tw, Direction::Outbound, Some(Audience::known(denier.peer)))
            .await
            .map_err(|e| e.to_string())?;
        let (pending_d, pump_d) = denier
            .handle
            .connect::<Local>(td, Direction::Inbound, None)
            .await
            .map_err(|e| e.to_string())?;
        spawner.spawn_local(pump_w).map_err(|e| e.to_string())?;
        spawner.spawn_local(pump_d).map_err(|e| e.to_string())?;

        let _conn_w = pending_w.authenticated().await.map_err(|e| e.to_string())?;
        let conn_d = pending_d.authenticated().await.map_err(|e| e.to_string())?;

        // The writer authors a commit (local write: allowed).
        writer
            .handle
            .add_commits(tree, vec![commit(0xA1)])
            .await
            .map_err(|e| e.to_string())?;
        wait_for(&writer, |event| match event {
            AppEvent::CommitsStored { tree: t, .. } if *t == tree => Some(()),
            _ => None,
        })
        .await?;

        // The denier syncs it over the wire; the exchange completes,
        // but its policy denies the remote persist.
        conn_d
            .sync_tree(tree, true)
            .await
            .map_err(|e| e.to_string())?;
        let status = wait_for(&denier, |event| match event {
            AppEvent::SyncFinished {
                tree: t, status, ..
            } if *t == tree => Some(*status),
            _ => None,
        })
        .await?;
        assert_eq!(
            status,
            SyncStatus::Completed,
            "the wire exchange itself succeeds; durability is what policy blocks"
        );

        assert!(
            denier.storage.commit_ids(tree).is_empty(),
            "policy-denied remote write must never become durable"
        );
        assert!(
            writer.storage.commit_ids(tree).len() == 1,
            "the writer's own local write is unaffected"
        );
        Ok(())
    });
    result?;
    Ok(())
}
