//! Shared scaffolding for runtime integration tests: a virtual-sleep
//! clock, a full local stack (node + driver + memory storage), and
//! app-event helpers.
#![allow(clippy::allow_attributes, unreachable_pub, dead_code)] // shared across test binaries; not all use every item

use core::time::Duration;
use std::{
    collections::BTreeSet,
    rc::Rc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::SigningKey;
use future_form::{FutureForm, Local};
use futures::future;
use sedimentree_core::{blob::Blob, loose_commit::id::CommitId};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_protocol::{
    command::NewCommit,
    effect::AppEvent,
    node::NodeConfig,
    peer_id::PeerId,
    timestamp::{Now, Timestamp},
    wall_clock::TimestampSeconds,
};
use subduction_runtime::{
    clock::Clock,
    driver::{handle::Handle, Driver},
    memory::{policy::AllowAll, storage::MemoryStorage, transport::MemoryTransport},
    policy::Policy,
};

/// A real wall/monotonic clock whose `sleep` never resolves: fine for
/// happy paths, where no protocol deadline should fire.
#[derive(Clone)]
pub struct TestClock {
    epoch: Instant,
}

impl TestClock {
    #[must_use]
    pub fn new() -> Self {
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

/// A test-stack driver over memory capabilities, generic in policy.
pub type PolicyDriver<P> =
    Driver<Local, MemoryTransport, Rc<MemoryStorage>, P, MemorySigner, TestClock>;

pub type TestDriver = PolicyDriver<AllowAll>;

/// One full local stack's handles.
pub struct Stack {
    pub handle: Handle<MemoryTransport>,
    pub storage: Rc<MemoryStorage>,
    pub peer: PeerId,
}

/// A driver + stack with seed-derived identity, entropy, and storage.
#[must_use]
pub fn stack(seed: u8) -> (TestDriver, Stack) {
    stack_with_policy(seed, AllowAll)
}

/// Like [`stack`], with a custom storage policy.
#[must_use]
pub fn stack_with_policy<P: Policy<Local>>(seed: u8, policy: P) -> (PolicyDriver<P>, Stack) {
    let signing_key = SigningKey::from_bytes(&[seed; 32]);
    let peer = PeerId::from(signing_key.verifying_key());
    let storage = Rc::new(MemoryStorage::new());
    let (driver, handle) = Driver::new(
        NodeConfig::new(peer, [seed ^ 0x55; 32]),
        TestClock::new(),
        MemorySigner::from_bytes(&[seed; 32]),
        Rc::clone(&storage),
        policy,
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
///
/// # Errors
///
/// Fails when the driver closes or the event never arrives.
pub async fn wait_for<F, T>(stack: &Stack, mut matcher: F) -> Result<T, String>
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

/// A test commit whose id and blob repeat `head`.
#[must_use]
pub fn commit(head: u8) -> NewCommit {
    NewCommit {
        head: CommitId::new([head; 32]),
        parents: BTreeSet::new(),
        blob: Blob::new(vec![head; 16]),
    }
}
