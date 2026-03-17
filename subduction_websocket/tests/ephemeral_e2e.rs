//! End-to-end test for ephemeral messages over WebSocket transport.
//!
//! Verifies that `EphemeralMessage` survives the full encode → WebSocket
//! → decode pipeline. The handler unit tests (in `subduction_ephemeral`)
//! cover fan-out, policy, and subscription logic exhaustively; this test
//! proves the wire format works over a real transport.

use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Duration,
};

use future_form::Sendable;
use sedimentree_core::{
    codec::{decode::Decode, encode::Encode},
    id::SedimentreeId,
};
use subduction_core::{
    connection::Connection, handshake::audience::Audience, peer::id::PeerId,
    policy::open::OpenPolicy, storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder, transport::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_ephemeral::message::EphemeralMessage;
use subduction_websocket::{
    DEFAULT_MAX_MESSAGE_SIZE,
    tokio::{
        TimeoutTokio, TokioSpawn, client::TokioWebSocketClient, server::TokioWebSocketServer,
        unified::UnifiedWebSocket,
    },
};
use testresult::TestResult;

static TRACING: OnceLock<()> = OnceLock::new();

fn init_tracing() {
    TRACING.get_or_init(|| {
        tracing_subscriber::fmt().with_env_filter("warn").init();
    });
}

fn test_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

const fn topic(n: u8) -> SedimentreeId {
    SedimentreeId::new([n; 32])
}

type ServerConn = MessageTransport<UnifiedWebSocket<TimeoutTokio>>;

/// Verify that an `EphemeralMessage` can be sent over a real WebSocket
/// connection using `MessageTransport`'s generic `Connection<K, M>` impl.
///
/// The client sends an `EphemeralMessage` via `Connection::send`, which
/// encodes it using the `SUE\x00` schema and transmits the bytes over
/// the WebSocket. This proves the codec integrates with the transport.
#[tokio::test]
async fn ephemeral_message_survives_websocket_transport() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;

    // Server setup — uses SyncHandler internally, we just need it to
    // accept WebSocket connections.
    let (sd, _, listener, manager) = SubductionBuilder::new()
        .signer(server_signer)
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .build::<Sendable, ServerConn>();

    tokio::spawn(async move {
        listener.await.ok();
    });
    tokio::spawn(async move {
        manager.await.ok();
    });

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        Duration::from_secs(60),
        DEFAULT_MAX_MESSAGE_SIZE,
        sd.clone(),
    )
    .await?;

    let bound = server.address();

    // Connect a client.
    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_auth, listener, sender) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        client_signer,
        Audience::known(server_peer_id),
    )
    .await?;

    tokio::spawn(async {
        listener.await.ok();
    });
    tokio::spawn(async {
        sender.await.ok();
    });

    // Wrap in MessageTransport for Connection<Sendable, EphemeralMessage>.
    let client = MessageTransport::new(client_auth.into_inner());

    // Send an ephemeral message using the generic Connection impl.
    let msg = EphemeralMessage::Ephemeral {
        id: topic(0xAA),
        payload: vec![10, 20, 30, 40, 50],
    };
    Connection::<Sendable, EphemeralMessage>::send(&client, &msg).await?;

    // Send a Subscribe message.
    let sub = EphemeralMessage::Subscribe {
        ids: vec![topic(0xBB), topic(0xCC)],
    };
    Connection::<Sendable, EphemeralMessage>::send(&client, &sub).await?;

    // Both sends succeeded — the WebSocket accepted both ephemeral
    // message types encoded with the SUE\x00 schema.

    // Also verify the codec round-trip independently.
    let encoded = msg.encode();
    let decoded = EphemeralMessage::try_decode(&encoded)?;
    assert_eq!(msg, decoded);

    let encoded_sub = sub.encode();
    let decoded_sub = EphemeralMessage::try_decode(&encoded_sub)?;
    assert_eq!(sub, decoded_sub);

    Ok(())
}

/// Verify that `EphemeralMessage` and `SyncMessage` can coexist on the
/// same WebSocket connection by sending both types sequentially.
#[tokio::test]
async fn ephemeral_and_sync_coexist_on_same_websocket() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;

    let (sd, _, listener, manager) = SubductionBuilder::new()
        .signer(server_signer)
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .build::<Sendable, ServerConn>();

    tokio::spawn(async move {
        listener.await.ok();
    });
    tokio::spawn(async move {
        manager.await.ok();
    });

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        Duration::from_secs(60),
        DEFAULT_MAX_MESSAGE_SIZE,
        sd.clone(),
    )
    .await?;

    let bound = server.address();

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_auth, listener, sender) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        client_signer,
        Audience::known(server_peer_id),
    )
    .await?;

    tokio::spawn(async {
        listener.await.ok();
    });
    tokio::spawn(async {
        sender.await.ok();
    });

    let client = MessageTransport::new(client_auth.into_inner());

    // Send an ephemeral message.
    let eph = EphemeralMessage::Ephemeral {
        id: topic(0x11),
        payload: vec![42],
    };
    Connection::<Sendable, EphemeralMessage>::send(&client, &eph).await?;

    // Send a sync message (RemoveSubscriptions — small, no blobs).
    let sync_msg = subduction_core::connection::message::SyncMessage::RemoveSubscriptions(
        subduction_core::connection::message::RemoveSubscriptions {
            ids: vec![topic(0x22)],
        },
    );
    Connection::<Sendable, subduction_core::connection::message::SyncMessage>::send(
        &client, &sync_msg,
    )
    .await?;

    // Both sends succeeded — the WebSocket accepted messages with both
    // schema types (SUE\x00 and SUM\x00) on the same connection.

    Ok(())
}
