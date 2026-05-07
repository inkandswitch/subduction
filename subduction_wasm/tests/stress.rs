//! Wasm stress tests for `WasmSubduction`.
//!
//! These run under [`wasm_bindgen_test`] in Node.js (or a headless
//! browser) and exercise the same scenarios as
//! `subduction_core/tests/stress_local.rs`, but with the wasm-side
//! `WasmSpawn` and an in-process channel transport. They validate that
//! the `Local` code path (which is what the JS bindings actually use)
//! survives multi-document and many-commit workloads.
//!
//! The `Sendable`-side `subduction_core::connection::test_utils` is gated
//! on `tokio`, which doesn't compile to `wasm32-unknown-unknown`, so the
//! scaffolding here is intentionally inlined: a wasm-friendly
//! `ChannelTransport` plus a no-op `Timeout` and the existing
//! `WasmSpawn`.
//!
//! Run with:
//!
//! ```sh
//! wasm-pack test --node subduction_wasm
//! ```

#![allow(clippy::expect_used, clippy::panic, missing_docs)]
#![cfg(target_arch = "wasm32")]

use core::{convert::Infallible, time::Duration};
use std::{collections::BTreeSet, sync::Arc};

use future_form::Local;
use futures::future::LocalBoxFuture;
use sedimentree_core::{
    blob::Blob,
    commit::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::Authenticated,
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::{TimedOut, Timeout},
    transport::{Transport, message::MessageTransport},
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_wasm::subduction::WasmSpawn;
use wasm_bindgen_test::wasm_bindgen_test;

// ─── Inlined scaffolding (no tokio) ─────────────────────────────────────────

/// In-process bidirectional byte transport for `Local`.
///
/// Mirrors `subduction_core::connection::test_utils::ChannelTransport`
/// but lives in the wasm-test crate so we don't need to ungate the
/// upstream impl from its `tokio`-bearing `test_utils` feature.
#[derive(Debug, Clone)]
struct WasmChannelTransport {
    tx: async_channel::Sender<Vec<u8>>,
    rx: async_channel::Receiver<Vec<u8>>,
}

impl WasmChannelTransport {
    fn pair() -> (Self, Self) {
        let (tx_a, rx_a) = async_channel::bounded(64);
        let (tx_b, rx_b) = async_channel::bounded(64);
        (Self { tx: tx_a, rx: rx_b }, Self { tx: tx_b, rx: rx_a })
    }
}

impl PartialEq for WasmChannelTransport {
    fn eq(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx) && self.rx.same_channel(&other.rx)
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("channel closed")]
struct ChannelClosed;

impl Transport<Local> for WasmChannelTransport {
    type SendError = ChannelClosed;
    type RecvError = ChannelClosed;
    type DisconnectionError = Infallible;

    fn send_bytes(&self, bytes: &[u8]) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let data = bytes.to_vec();
        let tx = self.tx.clone();
        Box::pin(async move { tx.send(data).await.map_err(|_| ChannelClosed) })
    }

    fn recv_bytes(&self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        let rx = self.rx.clone();
        Box::pin(async move { rx.recv().await.map_err(|_| ChannelClosed) })
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A `Timeout` that never fires — keeps tests deterministic under wasm
/// where `setTimeout` granularity and ordering varies between
/// host runtimes.
#[derive(Debug, Clone, Copy, Default)]
struct InstantTimeout;

impl Timeout<Local> for InstantTimeout {
    fn timeout<'a, T: 'a>(
        &'a self,
        _dur: Duration,
        fut: LocalBoxFuture<'a, T>,
    ) -> LocalBoxFuture<'a, Result<T, TimedOut>> {
        Box::pin(async move { Ok(fut.await) })
    }
}

type Conn = MessageTransport<WasmChannelTransport>;

type LocalNode = Arc<
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

// ─── Helpers ────────────────────────────────────────────────────────────────

fn signer(seed: u32) -> MemorySigner {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&seed.to_be_bytes());
    MemorySigner::from_bytes(&bytes)
}

fn sed_id(seed: u32) -> SedimentreeId {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&seed.to_be_bytes());
    SedimentreeId::new(bytes)
}

fn commit_id(doc_seed: u32, seq: u32) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&doc_seed.to_be_bytes());
    bytes[4..8].copy_from_slice(&seq.to_be_bytes());
    CommitId::new(bytes)
}

fn make_blob(seed: u32, size: usize) -> Blob {
    let mut data = vec![0u8; size];
    let s = seed.to_be_bytes();
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = s[i % 4].wrapping_add(u8::try_from(i % 256).unwrap_or(0));
    }
    Blob::new(data)
}

fn make_node(seed: u32) -> LocalNode {
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(signer(seed))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(WasmSpawn)
        .timer(InstantTimeout)
        .build::<Local, Conn>();
    wasm_bindgen_futures::spawn_local(async move {
        let _ = listener.await;
    });
    wasm_bindgen_futures::spawn_local(async move {
        let _ = manager.await;
    });
    sd
}

async fn connect_pair(a: &LocalNode, a_seed: u32, b: &LocalNode, b_seed: u32) {
    let (transport_a, transport_b) = WasmChannelTransport::pair();
    let conn_a = MessageTransport::new(transport_a);
    let conn_b = MessageTransport::new(transport_b);
    let peer_a = PeerId::from(signer(a_seed).verifying_key());
    let peer_b = PeerId::from(signer(b_seed).verifying_key());
    let auth_a: Authenticated<Conn, Local> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Local> = Authenticated::new_for_test(conn_b, peer_a);
    a.add_connection(auth_a).await.expect("add a");
    b.add_connection(auth_b).await.expect("add b");
}

async fn populate_chain(
    node: &LocalNode,
    id: SedimentreeId,
    doc_seed: u32,
    count: u32,
    blob_size: usize,
) {
    let mut parents = BTreeSet::new();
    for seq in 0..count {
        let head = commit_id(doc_seed, seq);
        node.add_commit(
            id,
            head,
            parents.clone(),
            make_blob(doc_seed.wrapping_add(seq), blob_size),
        )
        .await
        .expect("add_commit");
        parents = BTreeSet::from([head]);
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

/// Many sedimentrees, single chain per tree.
///
/// Wasm counterpart of `stress_local::many_docs_per_peer_local`. Lower
/// counts here because wasm test-runners (`wasm-pack test --node`) have
/// per-test budgets and we don't want flakes from background JS GC.
#[wasm_bindgen_test]
async fn many_docs_per_peer_wasm() {
    const NUM_DOCS: u32 = 30;
    const COMMITS_PER_DOC: u32 = 2;

    let alice = make_node(1);
    let bob = make_node(2);
    connect_pair(&alice, 1, &bob, 2).await;

    for doc in 0..NUM_DOCS {
        populate_chain(&alice, sed_id(doc), doc, COMMITS_PER_DOC, 32).await;
    }

    let bob_id = PeerId::from(signer(2).verifying_key());
    let r = alice
        .full_sync_with_peer(&bob_id, true, Some(Duration::from_secs(30)))
        .await;
    assert!(r.0, "full_sync should succeed");
    assert!(r.2.is_empty(), "no per-conn errors: {:?}", r.2);
    assert!(r.3.is_empty(), "no per-tree IO errors: {:?}", r.3);

    for doc in 0..NUM_DOCS {
        let id = sed_id(doc);
        let bob_commits = bob.get_commits(id).await.expect("bob has tree");
        assert_eq!(
            bob_commits.len() as u32,
            COMMITS_PER_DOC,
            "bob should have all commits for doc {doc}"
        );
    }
}

/// One sedimentree, many commits.
///
/// Wasm counterpart of `stress_local::long_lived_doc_local`. Reduced
/// commit count vs. the native test for runner-budget reasons.
#[wasm_bindgen_test]
async fn long_lived_doc_wasm() {
    const NUM_COMMITS: u32 = 200;

    let alice = make_node(11);
    let bob = make_node(12);
    connect_pair(&alice, 11, &bob, 12).await;

    let id = sed_id(0);
    populate_chain(&alice, id, 0, NUM_COMMITS, 32).await;

    let bob_id = PeerId::from(signer(12).verifying_key());
    let r = alice
        .full_sync_with_peer(&bob_id, true, Some(Duration::from_secs(60)))
        .await;
    assert!(r.0, "full_sync should succeed");
    assert!(r.2.is_empty(), "no per-conn errors: {:?}", r.2);
    assert!(r.3.is_empty(), "no per-tree IO errors: {:?}", r.3);

    let bob_commits = bob.get_commits(id).await.expect("bob has tree");
    assert_eq!(
        bob_commits.len() as u32,
        NUM_COMMITS,
        "bob should have every commit"
    );
}
