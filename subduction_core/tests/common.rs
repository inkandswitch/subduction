//! Shared helpers for stress tests.
//!
//! This module is included as `#[path = "common/mod.rs"] mod common;` from
//! each stress-test integration file. It centralizes the wiring needed to
//! spin up `Subduction` instances against in-memory storage and an in-process
//! channel transport, plus a small set of workload generators.
//!
//! Each helper is generic over `K: FutureForm` so the same scaffolding
//! serves both the multithreaded (`Sendable`) tokio path and the
//! single-threaded (`Local`) path used by the wasm bindings.

#![allow(dead_code)] // Each stress-test file uses a subset of helpers.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::{Local, Sendable};
use sedimentree_core::{
    blob::Blob,
    commit::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TestSpawn, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;

pub(crate) type Conn = MessageTransport<ChannelTransport>;

pub(crate) type SendableNode = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
    >,
>;

pub(crate) type LocalNode = Arc<
    Subduction<
        'static,
        Local,
        MemoryStorage,
        Conn,
        SyncHandler<Local, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
    >,
>;

// ─── Signers / IDs ──────────────────────────────────────────────────────────

/// Deterministic signer keyed by a `u32` seed. Two calls with the same seed
/// produce the same signing key, so peer IDs are reproducible across runs.
#[must_use]
pub(crate) fn signer(seed: u32) -> MemorySigner {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&seed.to_be_bytes());
    MemorySigner::from_bytes(&bytes)
}

/// Build a [`SedimentreeId`] from a `u32` seed.
#[must_use]
pub(crate) fn sed_id(seed: u32) -> SedimentreeId {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&seed.to_be_bytes());
    SedimentreeId::new(bytes)
}

/// Build a [`CommitId`] from two `u32` seeds (typically `(doc_seed, commit_seq)`).
#[must_use]
pub(crate) fn commit_id(doc_seed: u32, seq: u32) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&doc_seed.to_be_bytes());
    bytes[4..8].copy_from_slice(&seq.to_be_bytes());
    CommitId::new(bytes)
}

/// Build a deterministic [`Blob`] of `size` bytes seeded by `seed`.
#[must_use]
pub(crate) fn blob(seed: u32, size: usize) -> Blob {
    let mut data = vec![0u8; size];
    let seed_bytes = seed.to_be_bytes();
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = seed_bytes[i % 4].wrapping_add(u8::try_from(i % 256).unwrap_or(0));
    }
    Blob::new(data)
}

// ─── Sendable (multithreaded tokio) nodes ───────────────────────────────────

/// Spin up a single `Sendable` `Subduction` against `MemoryStorage`. Spawns
/// the listener and connection-manager futures onto the current tokio
/// runtime via [`tokio::spawn`].
#[must_use]
pub(crate) fn make_sendable_node(seed: u32) -> SendableNode {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer(seed))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

/// Connect two `Sendable` nodes via an in-process channel transport pair.
pub(crate) async fn connect_sendable_pair(
    a: &SendableNode,
    a_seed: u32,
    b: &SendableNode,
    b_seed: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let (transport_a, transport_b) = ChannelTransport::pair();
    let conn_a = MessageTransport::new(transport_a);
    let conn_b = MessageTransport::new(transport_b);
    let peer_a = PeerId::from(signer(a_seed).verifying_key());
    let peer_b = PeerId::from(signer(b_seed).verifying_key());
    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);
    a.add_connection(auth_a).await?;
    b.add_connection(auth_b).await?;
    Ok(())
}

// ─── Local (single-threaded) nodes ──────────────────────────────────────────

/// Spin up a single `Local` `Subduction` against `MemoryStorage`. Spawns the
/// listener and connection-manager futures onto the current
/// `tokio::task::LocalSet` via [`tokio::task::spawn_local`]; callers must
/// run their test inside `LocalSet::run_until` for the spawn to be valid.
#[must_use]
pub(crate) fn make_local_node(seed: u32) -> LocalNode {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer(seed))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TestSpawn)
        .timer(InstantTimeout)
        .build::<Local, Conn>();

    tokio::task::spawn_local(listener);
    tokio::task::spawn_local(manager);
    sd
}

/// Connect two `Local` nodes via an in-process channel transport pair.
pub(crate) async fn connect_local_pair(
    a: &LocalNode,
    a_seed: u32,
    b: &LocalNode,
    b_seed: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let (transport_a, transport_b) = ChannelTransport::pair();
    let conn_a = MessageTransport::new(transport_a);
    let conn_b = MessageTransport::new(transport_b);
    let peer_a = PeerId::from(signer(a_seed).verifying_key());
    let peer_b = PeerId::from(signer(b_seed).verifying_key());
    let auth_a: Authenticated<Conn, Local> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Local> = Authenticated::new_for_test(conn_b, peer_a);
    a.add_connection(auth_a).await?;
    b.add_connection(auth_b).await?;
    Ok(())
}

// ─── Workload generators ────────────────────────────────────────────────────

/// Add `count` commits to a `Sendable` node under `id`, with deterministic content.
///
/// Each commit's blob is `blob_size` bytes. Commits form a linear chain —
/// commit `n+1` claims commit `n` as its parent — so the resulting tree
/// has a single head.
pub(crate) async fn populate_linear_chain_sendable(
    node: &SendableNode,
    id: SedimentreeId,
    doc_seed: u32,
    count: u32,
    blob_size: usize,
) -> Result<CommitId, Box<dyn std::error::Error>> {
    let mut head = commit_id(doc_seed, 0);
    let mut parents = BTreeSet::new();
    for seq in 0..count {
        let next = commit_id(doc_seed, seq);
        node.add_commit(
            id,
            next,
            parents.clone(),
            blob(doc_seed.wrapping_add(seq), blob_size),
        )
        .await?;
        head = next;
        parents = BTreeSet::from([next]);
    }
    Ok(head)
}

/// `Local` counterpart of [`populate_linear_chain_sendable`].
pub(crate) async fn populate_linear_chain_local(
    node: &LocalNode,
    id: SedimentreeId,
    doc_seed: u32,
    count: u32,
    blob_size: usize,
) -> Result<CommitId, Box<dyn std::error::Error>> {
    let mut head = commit_id(doc_seed, 0);
    let mut parents = BTreeSet::new();
    for seq in 0..count {
        let next = commit_id(doc_seed, seq);
        node.add_commit(
            id,
            next,
            parents.clone(),
            blob(doc_seed.wrapping_add(seq), blob_size),
        )
        .await?;
        head = next;
        parents = BTreeSet::from([next]);
    }
    Ok(head)
}

// ─── Convenience ────────────────────────────────────────────────────────────

/// A short delay used after `add_connection` to let the in-process channel
/// transport's listener task observe both ends.
///
/// Long enough for `tokio::task::yield_now()` to be insufficient (the
/// connection-manager spawns its own task that needs to run) but short
/// enough not to dominate test wall-clock time.
pub(crate) const SETTLE: Duration = Duration::from_millis(10);
