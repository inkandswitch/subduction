//! End-to-end driver test: two full stacks (node + driver + memory
//! transport/storage) handshake, sync, and push — no locks, no tokio,
//! single-threaded `LocalPool`.
#![allow(clippy::wildcard_enum_match_arm)] // tests match a growing event enum on purpose

use core::time::Duration;
use std::{
    collections::BTreeSet,
    rc::Rc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::SigningKey;
use future_form::{FutureForm, Local};
use futures::{executor::LocalPool, future, task::LocalSpawnExt as _};
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_protocol::{
    command::{Command, NewCommit},
    effect::{AppEvent, SyncStatus},
    event::Direction,
    handshake::audience::Audience,
    node::NodeConfig,
    peer_id::PeerId,
    timestamp::{Now, Timestamp},
    wall_clock::TimestampSeconds,
};
use subduction_runtime::{
    clock::Clock,
    driver::{Driver, Handle},
    memory::{AllowAll, MemoryStorage, MemoryTransport},
};
use testresult::TestResult;

/// A real wall/monotonic clock whose `sleep` never resolves: fine for
/// happy paths, where no protocol deadline should fire.
#[derive(Clone)]
struct TestClock {
    epoch: Instant,
}

impl TestClock {
    fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }
}

impl Clock<Local> for TestClock {
    fn now(&self) -> Now {
        let monotonic =
            Timestamp::from_millis(u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(0));
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Now {
            monotonic,
            wall: TimestampSeconds::new(wall.as_secs()),
        }
    }

    fn sleep(&self, _duration: Duration) -> <Local as FutureForm>::Future<'_, ()> {
        Local::from_future(future::pending())
    }
}

type TestDriver =
    Driver<Local, MemoryTransport, Rc<MemoryStorage>, AllowAll, MemorySigner, TestClock>;

struct Stack {
    handle: Handle<MemoryTransport>,
    storage: Rc<MemoryStorage>,
    peer: PeerId,
}

fn stack(seed: u8) -> (TestDriver, Stack) {
    let signing_key = SigningKey::from_bytes(&[seed; 32]);
    let peer = PeerId::from(signing_key.verifying_key());
    let storage = Rc::new(MemoryStorage::new());
    let (driver, handle) = TestDriver::new(
        NodeConfig::new(peer, [seed ^ 0x55; 32]),
        TestClock::new(),
        MemorySigner::from_bytes(&[seed; 32]),
        Rc::clone(&storage),
        AllowAll,
    );
    (
        driver,
        Stack {
            handle,
            storage,
            peer,
        },
    )
}

/// Wait for a matching app event, with a bounded number of skips so a
/// wedged driver fails the test instead of hanging it.
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

fn commit(head: u8) -> NewCommit {
    NewCommit {
        head: CommitId::new([head; 32]),
        parents: BTreeSet::new(),
        blob: Blob::new(vec![head; 16]),
    }
}

#[test]
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
        let (conn_a, pump_a) = a
            .handle
            .connect::<Local>(ta, Direction::Outbound, Some(Audience::known(b.peer)))
            .await
            .map_err(|e| e.to_string())?;
        let (_conn_b, pump_b) = b
            .handle
            .connect::<Local>(tb, Direction::Inbound, None)
            .await
            .map_err(|e| e.to_string())?;
        spawner.spawn_local(pump_a).map_err(|e| e.to_string())?;
        spawner.spawn_local(pump_b).map_err(|e| e.to_string())?;

        // Both sides authenticate.
        let peer_seen_by_a = wait_for(&a, |event| match event {
            AppEvent::PeerAuthenticated { peer, .. } => Some(*peer),
            _ => None,
        })
        .await?;
        assert_eq!(peer_seen_by_a, b.peer, "a authenticated b");
        let peer_seen_by_b = wait_for(&b, |event| match event {
            AppEvent::PeerAuthenticated { peer, .. } => Some(*peer),
            _ => None,
        })
        .await?;
        assert_eq!(peer_seen_by_b, a.peer, "b authenticated a");

        // B authors a commit locally (sealed + persisted by its driver).
        b.handle
            .command(Command::AddCommits {
                tree,
                commits: vec![commit(0xA1)],
            })
            .await
            .map_err(|e| e.to_string())?;
        wait_for(&b, |event| match event {
            AppEvent::CommitsStored { tree: t, .. } if *t == tree => Some(()),
            _ => None,
        })
        .await?;

        // A syncs the tree from B, subscribing.
        a.handle
            .command(Command::SyncTree {
                conn: conn_a,
                tree,
                subscribe: true,
            })
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
            .command(Command::AddCommits {
                tree,
                commits: vec![commit(0xA2)],
            })
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
