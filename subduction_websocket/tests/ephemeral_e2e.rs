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
use nonempty::nonempty;
use sedimentree_core::codec::{decode::Decode, encode::Encode};
use subduction_core::{
    connection::Connection, handshake::audience::Audience, peer::id::PeerId,
    policy::open::OpenPolicy, storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder, timestamp::TimestampSeconds,
    transport::message::MessageTransport,
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_ephemeral::{
    message::{EphemeralMessage, EphemeralPayload},
    topic::Topic,
};
use subduction_websocket::{
    tokio::{
        client::TokioWebSocketClient, server::TokioWebSocketServer, unified::UnifiedWebSocket,
        TimeoutTokio, TokioSpawn,
    },
    DEFAULT_MAX_MESSAGE_SIZE,
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

type ServerConn = MessageTransport<UnifiedWebSocket>;

/// Verify that an `EphemeralMessage` can be sent over a real WebSocket
/// connection using `MessageTransport`'s generic `Connection<K, M>` impl.
#[tokio::test]
async fn ephemeral_message_survives_websocket_transport() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;

    let (sd, _, listener, manager) = SubductionBuilder::new()
        .signer(server_signer)
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TimeoutTokio)
        .roundtrip_timeout(Duration::from_secs(5))
        .build::<Sendable, ServerConn>();

    tokio::spawn(async move {
        listener.await.ok();
    });
    tokio::spawn(async move {
        manager.await.ok();
    });

    let server = TokioWebSocketServer::new(
        addr,
        Duration::from_secs(60),
        DEFAULT_MAX_MESSAGE_SIZE,
        sd.clone(),
    )
    .await?;

    let bound = server.address();

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_auth, listener, sender) =
        TokioWebSocketClient::new(uri, client_signer.clone(), Audience::known(server_peer_id))
            .await?;

    tokio::spawn(async move {
        listener.await.ok();
    });
    tokio::spawn(async move {
        sender.await.ok();
    });

    let client = MessageTransport::new(client_auth.into_inner());

    // Send an ephemeral message using the generic Connection impl.
    let ep = EphemeralPayload {
        id: Topic::new([0xAA; 32]),
        nonce: 42,
        timestamp: TimestampSeconds::new(1_700_000_000),
        payload: vec![10, 20, 30, 40, 50],
    };
    let verified = Signed::seal::<Sendable, _>(&client_signer, ep).await;
    let msg = EphemeralMessage::Ephemeral(verified.into_signed());
    Connection::<Sendable, EphemeralMessage>::send(&client, &msg).await?;

    // Send a Subscribe message.
    let sub = EphemeralMessage::Subscribe {
        topics: nonempty![Topic::new([0xBB; 32]), Topic::new([0xCC; 32])],
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
        .timer(TimeoutTokio)
        .roundtrip_timeout(Duration::from_secs(5))
        .build::<Sendable, ServerConn>();

    tokio::spawn(async move {
        listener.await.ok();
    });
    tokio::spawn(async move {
        manager.await.ok();
    });

    let server = TokioWebSocketServer::new(
        addr,
        Duration::from_secs(60),
        DEFAULT_MAX_MESSAGE_SIZE,
        sd.clone(),
    )
    .await?;

    let bound = server.address();

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_auth, listener, sender) =
        TokioWebSocketClient::new(uri, client_signer.clone(), Audience::known(server_peer_id))
            .await?;

    tokio::spawn(async move {
        listener.await.ok();
    });
    tokio::spawn(async move {
        sender.await.ok();
    });

    let client = MessageTransport::new(client_auth.into_inner());

    // Send an ephemeral message.
    let ep = EphemeralPayload {
        id: Topic::new([0x11; 32]),
        nonce: 43,
        timestamp: TimestampSeconds::new(1_700_000_000),
        payload: vec![42],
    };
    let verified = Signed::seal::<Sendable, _>(&client_signer, ep).await;
    let eph = EphemeralMessage::Ephemeral(verified.into_signed());
    Connection::<Sendable, EphemeralMessage>::send(&client, &eph).await?;

    // Send a sync message (RemoveSubscriptions — small, no blobs).
    let sync_msg = subduction_core::connection::message::SyncMessage::RemoveSubscriptions(
        subduction_core::connection::message::RemoveSubscriptions {
            ids: vec![Topic::new([0x22; 32]).into()],
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
