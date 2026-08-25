//! Two full stacks over a real WebSocket on loopback TCP: handshake,
//! batch sync, and subscription push — tokio timers via `TokioClock`,
//! everything spawned by the test (the driver and transport never
//! schedule tasks).
#![allow(clippy::wildcard_enum_match_arm)] // tests match a growing event enum on purpose
#![allow(clippy::expect_used, clippy::too_many_lines)] // test tasks fail loudly; one linear scenario

use std::rc::Rc;

use ed25519_dalek::SigningKey;
use future_form::Local;
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
use subduction_runtime::{
    driver::{handle::Handle, Driver},
    memory::{policy::AllowAll, storage::MemoryStorage},
};
use subduction_tokio::clock::TokioClock;
use subduction_websocket::{client, server, transport::WebSocketTransport};
use testresult::TestResult;

type WsDriver =
    Driver<Local, WebSocketTransport, Rc<MemoryStorage>, AllowAll, MemorySigner, TokioClock>;

struct Stack {
    handle: Handle<WebSocketTransport>,
    storage: Rc<MemoryStorage>,
    peer: PeerId,
}

fn stack(seed: u8) -> (WsDriver, Stack) {
    let signing_key = SigningKey::from_bytes(&[seed; 32]);
    let peer = PeerId::from(signing_key.verifying_key());
    let storage = Rc::new(MemoryStorage::new());
    let (driver, handle) = WsDriver::new(
        NodeConfig::new(peer, [seed ^ 0x55; 32]),
        TokioClock::new(),
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

#[tokio::test(flavor = "current_thread")]
async fn two_stacks_sync_over_a_real_websocket() -> TestResult {
    let local = tokio::task::LocalSet::new();
    let result: Result<(), String> = local
        .run_until(async {
            let tree = SedimentreeId::new([7u8; 32]);
            let (driver_a, a) = stack(1);
            let (driver_b, b) = stack(2);
            let _driver_a_task = tokio::task::spawn_local(driver_a.run());
            let _driver_b_task = tokio::task::spawn_local(driver_b.run());

            // Real loopback TCP + WebSocket handshake.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|e| e.to_string())?;
            let address = listener.local_addr().map_err(|e| e.to_string())?;
            let serve = tokio::spawn(async move {
                let (stream, _peer) = listener.accept().await.expect("tcp accept");
                server::accept(stream).await.expect("ws accept")
            });
            let url = format!("ws://{address}");
            let (client_transport, client_pump) =
                client::connect(&url).await.map_err(|e| e.to_string())?;
            let (server_transport, server_pump) = serve.await.map_err(|e| e.to_string())?;
            let _client_pump_task = tokio::task::spawn_local(client_pump);
            let _server_pump_task = tokio::task::spawn_local(server_pump);

            // Wire the transports into the drivers.
            let (pending_a, read_a) = a
                .handle
                .connect::<Local>(
                    client_transport,
                    Direction::Outbound,
                    Some(Audience::known(b.peer)),
                )
                .await
                .map_err(|e| e.to_string())?;
            let (pending_b, read_b) = b
                .handle
                .connect::<Local>(server_transport, Direction::Inbound, None)
                .await
                .map_err(|e| e.to_string())?;
            let _read_a_task = tokio::task::spawn_local(read_a);
            let _read_b_task = tokio::task::spawn_local(read_b);

            let conn_a = pending_a.authenticated().await.map_err(|e| e.to_string())?;
            assert_eq!(conn_a.peer(), b.peer, "authenticated over the socket");
            let _conn_b = pending_b.authenticated().await.map_err(|e| e.to_string())?;

            // B writes; A syncs with subscription.
            b.handle
                .add_commits(
                    tree,
                    vec![NewCommit {
                        head: CommitId::new([0xA1; 32]),
                        parents: std::collections::BTreeSet::new(),
                        blob: Blob::new(vec![0xA1; 16]),
                    }],
                )
                .await
                .map_err(|e| e.to_string())?;
            wait_for(&b, |event| match event {
                AppEvent::CommitsStored { tree: t, .. } if *t == tree => Some(()),
                _ => None,
            })
            .await?;

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
            assert_eq!(
                a.storage.commit_ids(tree),
                vec![CommitId::new([0xA1; 32])],
                "sync over the socket persisted b's commit on a"
            );

            // Live push over the same socket.
            b.handle
                .add_commits(
                    tree,
                    vec![NewCommit {
                        head: CommitId::new([0xA2; 32]),
                        parents: std::collections::BTreeSet::new(),
                        blob: Blob::new(vec![0xA2; 16]),
                    }],
                )
                .await
                .map_err(|e| e.to_string())?;
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

            Ok(())
        })
        .await;
    result?;
    Ok(())
}
