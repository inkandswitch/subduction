//! Interop: a new sans-io node (driver + `Node`) syncs with a legacy
//! `Subduction` node over a shared in-memory byte duct, in both
//! directions — the wire-compatibility acceptance test.
//!
//! ```text
//!  new stack                       byte duct                legacy stack
//!  Node + Driver ── MemoryTransport ══════ LegacyDuct ── MessageTransport
//!  (LocalSet task)     Vec<u8> frames, both directions     (tokio tasks)
//! ```
//!
//! The legacy end is real: `SubductionBuilder` node, byte-level
//! handshake (`handshake::initiate`/`respond`), and the `SyncMessage`
//! codec via `MessageTransport`. Nothing is mocked at the message level.
#![allow(clippy::wildcard_enum_match_arm)] // tests match a growing event enum on purpose
#![allow(clippy::expect_used)] // test tasks fail loudly on purpose

mod common;

use std::{collections::BTreeSet, sync::Arc};

use async_channel::{Receiver, Sender};
use common::{commit, stack, wait_for};
use future_form::{FutureForm, Local, Sendable};
use futures::future::BoxFuture;
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use subduction_core::{
    connection::test_utils::{InstantTimeout, TokioSpawn},
    handshake::{self, MAX_PLAUSIBLE_DRIFT},
    nonce_cache::NonceCache,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage as LegacyStorage,
    subduction::builder::SubductionBuilder,
    timeout::call::CallTimeout,
    timestamp::TimestampSeconds,
    transport::{message::MessageTransport, Transport as LegacyTransport},
};
use subduction_crypto::{nonce::Nonce, signer::memory::MemorySigner};
use subduction_protocol::{
    command::Command,
    effect::{AppEvent, SyncStatus},
    event::Direction,
    handshake::audience::Audience,
    peer_id::PeerId,
};
use subduction_runtime::memory::MemoryTransport;
use testresult::TestResult;
use thiserror::Error;

/// The duct closed underneath the legacy end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("duct closed")]
struct DuctClosed;

/// The legacy end of the shared byte duct: implements legacy
/// [`Handshake`](handshake::Handshake) (pre-auth) and legacy
/// [`Transport`](LegacyTransport) (post-auth, under `MessageTransport`).
#[derive(Debug, Clone)]
struct LegacyDuct {
    /// Distinguishes ducts under `PartialEq` (required by legacy
    /// `Connection`); channel handles themselves aren't comparable.
    tag: u64,
    tx: Sender<Vec<u8>>,
    rx: Receiver<Vec<u8>>,
}

impl PartialEq for LegacyDuct {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag
    }
}

impl handshake::Handshake<Sendable> for LegacyDuct {
    type Error = DuctClosed;

    fn send(&mut self, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move { self.tx.send(bytes).await.map_err(|_| DuctClosed) })
    }

    fn recv(&mut self) -> BoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        Sendable::from_future(async move { self.rx.recv().await.map_err(|_| DuctClosed) })
    }
}

impl LegacyTransport<Sendable> for LegacyDuct {
    type SendError = DuctClosed;
    type RecvError = DuctClosed;
    type DisconnectionError = DuctClosed;

    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), Self::SendError>> {
        let bytes = bytes.to_vec();
        Sendable::from_future(async move { self.tx.send(bytes).await.map_err(|_| DuctClosed) })
    }

    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        Sendable::from_future(async move { self.rx.recv().await.map_err(|_| DuctClosed) })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        Sendable::from_future(async move {
            let _was_open = self.tx.close();
            let _was_open = self.rx.close();
            Ok(())
        })
    }
}

/// Wall-clock seconds for the legacy handshake (its `now()` constructor
/// is feature-gated off in this build).
fn legacy_now() -> TimestampSeconds {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    TimestampSeconds::new(secs)
}

/// A byte duct: (new end, legacy end).
fn duct() -> (MemoryTransport, LegacyDuct) {
    let (to_legacy_tx, to_legacy_rx) = async_channel::unbounded();
    let (to_new_tx, to_new_rx) = async_channel::unbounded();
    (
        MemoryTransport::from_channels(to_legacy_tx, to_new_rx),
        LegacyDuct {
            tag: 0,
            tx: to_new_tx,
            rx: to_legacy_rx,
        },
    )
}

type LegacyNode = Arc<
    subduction_core::subduction::Subduction<
        'static,
        Sendable,
        LegacyStorage,
        MessageTransport<LegacyDuct>,
        subduction_core::handler::sync::SyncHandler<
            Sendable,
            LegacyStorage,
            MessageTransport<LegacyDuct>,
            OpenPolicy,
            sedimentree_core::depth::CountLeadingZeroBytes,
            TokioSpawn,
        >,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
        sedimentree_core::depth::CountLeadingZeroBytes,
    >,
>;

/// Build and start a legacy node (listener + manager spawned on tokio).
fn legacy_node(seed: u8) -> (LegacyNode, LegacyStorage, MemorySigner) {
    let signer = MemorySigner::from_bytes(&[seed; 32]);
    let storage = LegacyStorage::new();
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer.clone())
        .storage(storage.clone(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, MessageTransport<LegacyDuct>>();
    drop(tokio::spawn(listener));
    drop(tokio::spawn(manager));
    (sd, storage, signer)
}

/// Poll legacy storage until `head` is present (bounded).
async fn legacy_has_commit(
    storage: &LegacyStorage,
    tree: SedimentreeId,
    head: CommitId,
) -> Result<(), String> {
    for _ in 0u32..50_000 {
        let ids = <LegacyStorage as subduction_core::storage::traits::Storage<Sendable>>::list_commit_ids(storage, tree)
            .await
            .map_err(|e| e.to_string())?;
        if ids.contains(&head) {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    Err("legacy node never stored the pushed commit".into())
}

/// New node initiates the handshake; legacy responds. Then: legacy's
/// data reaches the new node via batch sync, and the new node's data
/// reaches legacy via subscription push.
#[tokio::test(flavor = "current_thread")]
async fn new_node_initiates_and_syncs_both_ways_with_legacy() -> TestResult {
    let local = tokio::task::LocalSet::new();
    let result: Result<(), String> = local
        .run_until(async {
            let tree = SedimentreeId::new([7u8; 32]);
            let (sd, legacy_storage, legacy_signer) = legacy_node(9);
            let legacy_peer = PeerId::new(
                *subduction_core::peer::id::PeerId::from(legacy_signer.verifying_key()).as_bytes(),
            );

            // Legacy authors a commit before we connect.
            let legacy_head = CommitId::new([0xA1; 32]);
            let _fragment_requested = sd
                .add_commit(
                    tree,
                    legacy_head,
                    BTreeSet::new(),
                    Blob::new(vec![0xA1; 16]),
                )
                .await
                .map_err(|e| e.to_string())?;

            // Handshake across the duct: we initiate, legacy responds.
            let (our_transport, legacy_duct) = duct();
            let sd_for_respond = Arc::clone(&sd);
            let respond = tokio::spawn(async move {
                let nonce_cache = NonceCache::default();
                let our_peer_id =
                    subduction_core::peer::id::PeerId::from(legacy_signer.verifying_key());
                let (authenticated, ()) = handshake::respond::<Sendable, _, _, _, _>(
                    legacy_duct,
                    |duct, _peer| (MessageTransport::new(duct), ()),
                    &legacy_signer,
                    &nonce_cache,
                    our_peer_id,
                    None,
                    legacy_now(),
                    MAX_PLAUSIBLE_DRIFT,
                )
                .await
                .expect("legacy handshake respond");
                let _fresh = sd_for_respond
                    .add_connection(authenticated)
                    .await
                    .expect("legacy add_connection");
            });

            let (driver, ours) = stack(1);
            drop(tokio::task::spawn_local(driver.run()));
            let (conn, pump) = ours
                .handle
                .connect::<Local>(
                    our_transport,
                    Direction::Outbound,
                    Some(Audience::known(legacy_peer)),
                )
                .await
                .map_err(|e| e.to_string())?;
            drop(tokio::task::spawn_local(pump));

            let authenticated_peer = wait_for(&ours, |event| match event {
                AppEvent::PeerAuthenticated { peer, .. } => Some(*peer),
                _ => None,
            })
            .await?;
            assert_eq!(authenticated_peer, legacy_peer, "we authenticated legacy");
            respond.await.map_err(|e| e.to_string())?;

            // Legacy → new: batch sync (with subscription).
            ours.handle
                .command(Command::SyncTree {
                    conn,
                    tree,
                    subscribe: true,
                })
                .await
                .map_err(|e| e.to_string())?;
            let status = wait_for(&ours, |event| match event {
                AppEvent::SyncFinished {
                    tree: t, status, ..
                } if *t == tree => Some(*status),
                _ => None,
            })
            .await?;
            assert_eq!(status, SyncStatus::Completed, "sync with legacy completed");
            assert!(
                ours.storage.commit_ids(tree).contains(&legacy_head),
                "legacy's commit is durable on the new node"
            );

            // New → legacy: local write pushed over the mutual
            // subscription; legacy ingests the wire LooseCommit.
            ours.handle
                .command(Command::AddCommits {
                    tree,
                    commits: vec![commit(0xB2)],
                })
                .await
                .map_err(|e| e.to_string())?;
            wait_for(&ours, |event| match event {
                AppEvent::CommitsStored { tree: t, .. } if *t == tree => Some(()),
                _ => None,
            })
            .await?;
            legacy_has_commit(&legacy_storage, tree, CommitId::new([0xB2; 32])).await?;

            Ok(())
        })
        .await;
    result?;
    Ok(())
}

/// Legacy initiates the handshake; the new node responds. Then legacy
/// pulls the new node's data with `full_sync_with_peer` (1.5-RTT
/// bidirectional sync).
#[tokio::test(flavor = "current_thread")]
async fn legacy_initiates_and_pulls_from_new_node() -> TestResult {
    let local = tokio::task::LocalSet::new();
    let result: Result<(), String> = local
        .run_until(async {
            let tree = SedimentreeId::new([8u8; 32]);
            let (sd, legacy_storage, legacy_signer) = legacy_node(9);

            // The new node authors a commit before legacy connects.
            let (driver, ours) = stack(1);
            drop(tokio::task::spawn_local(driver.run()));
            ours.handle
                .command(Command::AddCommits {
                    tree,
                    commits: vec![commit(0xC3)],
                })
                .await
                .map_err(|e| e.to_string())?;
            wait_for(&ours, |event| match event {
                AppEvent::CommitsStored { tree: t, .. } if *t == tree => Some(()),
                _ => None,
            })
            .await?;
            // Legacy must know the tree id to request it.
            let legacy_head = CommitId::new([0xD4; 32]);
            let _fragment_requested = sd
                .add_commit(
                    tree,
                    legacy_head,
                    BTreeSet::new(),
                    Blob::new(vec![0xD4; 16]),
                )
                .await
                .map_err(|e| e.to_string())?;

            // Handshake: legacy initiates, we respond.
            let (our_transport, legacy_duct) = duct();
            let our_peer = ours.peer;
            let sd_for_initiate = Arc::clone(&sd);
            let initiate = tokio::spawn(async move {
                let audience = subduction_core::handshake::audience::Audience::known(
                    subduction_core::peer::id::PeerId::new(*our_peer.as_bytes()),
                );
                let (authenticated, ()) = handshake::initiate::<Sendable, _, _, _, _>(
                    legacy_duct,
                    |duct, _peer| (MessageTransport::new(duct), ()),
                    &legacy_signer,
                    audience,
                    legacy_now(),
                    Nonce::random(),
                )
                .await
                .expect("legacy handshake initiate");
                let _fresh = sd_for_initiate
                    .add_connection(authenticated)
                    .await
                    .expect("legacy add_connection");
            });

            let (_conn, pump) = ours
                .handle
                .connect::<Local>(our_transport, Direction::Inbound, None)
                .await
                .map_err(|e| e.to_string())?;
            drop(tokio::task::spawn_local(pump));

            let _peer = wait_for(&ours, |event| match event {
                AppEvent::PeerAuthenticated { peer, .. } => Some(*peer),
                _ => None,
            })
            .await?;
            initiate.await.map_err(|e| e.to_string())?;

            // Legacy pulls: 1.5-RTT bidirectional sync fetches our
            // commit into legacy storage (and sends theirs back).
            let legacy_view_of_us = subduction_core::peer::id::PeerId::new(*our_peer.as_bytes());
            let (_all_ok, _stats, conn_errors, io_errors) = sd
                .full_sync_with_peer(&legacy_view_of_us, false, CallTimeout::Default)
                .await;
            assert!(
                conn_errors.is_empty(),
                "legacy sync conn errors: {conn_errors:?}"
            );
            assert!(io_errors.is_empty(), "legacy sync io errors: {io_errors:?}");

            legacy_has_commit(&legacy_storage, tree, CommitId::new([0xC3; 32])).await?;

            // The requested-data return leg is fire-and-forget; poll
            // until it lands on our side.
            let mut delivered = false;
            for _ in 0u32..50_000 {
                if ours.storage.commit_ids(tree).contains(&legacy_head) {
                    delivered = true;
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(
                delivered,
                "bidirectional sync delivered legacy's commit to us"
            );

            Ok(())
        })
        .await;
    result?;
    Ok(())
}
