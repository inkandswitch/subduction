//! The server must not outlive its Subduction listener/manager futures:
//! an accept loop that keeps completing handshakes nobody will ever service
//! is worse than a stopped server. The supervised spawns in
//! `setup_with_keepalive` stop the whole server when either critical future
//! exits outside an orderly shutdown. Socket driver termination must also wake
//! callers waiting for inbound data, even if transport clones remain alive.

use core::{future::IntoFuture, time::Duration};

use async_tungstenite::{WebSocketStream, tokio::TokioAdapter};
use future_form::Sendable;

use sedimentree_core::depth::CountLeadingZeroBytes;
use subduction_core::{
    nonce_cache::NonceCache, peer::id::PeerId, policy::open::OpenPolicy,
    storage::memory::MemoryStorage, transport::Transport,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::{
    DEFAULT_MAX_MESSAGE_SIZE,
    sleep::TokioSleeper,
    tokio::{TimeoutTokio, server::TokioWebSocketServer},
    websocket::{KeepAlive, KeepAliveOutcome, WebSocket},
};
use testresult::TestResult;

/// Losing the sender closes the stop channel and cancels the socket listener.
/// That must also close the inbound data channel, rather than stranding recv.
#[tokio::test(start_paused = true)]
async fn dropping_sender_wakes_pending_receive_with_transport_clones() -> TestResult {
    // Keep the remote end open and silent so the read cannot complete via EOF.
    let (local, _remote) = tokio::io::duplex(1024);
    let socket = WebSocketStream::from_raw_socket(
        TokioAdapter::new(local),
        tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let (transport, sender, keepalive): (WebSocket<_, Sendable>, _, _) =
        WebSocket::new_with_keepalive(
            socket,
            PeerId::new([7; 32]),
            KeepAlive::balanced(),
            TokioSleeper,
        );
    let retained = transport.clone();
    let mut listener = Box::pin(transport.listen());
    let mut sender = Box::pin(sender);
    let mut keepalive = keepalive.into_future();
    let mut receive = retained.recv_bytes();

    // Establish the pending operations explicitly, without scheduler sleeps.
    assert!(futures::poll!(&mut listener).is_pending());
    assert!(futures::poll!(&mut sender).is_pending());
    assert!(futures::poll!(&mut keepalive).is_pending());
    assert!(futures::poll!(&mut receive).is_pending());

    // No explicit disconnect: dropping the sender is the only shutdown signal.
    drop(sender);
    let deadline = Duration::from_secs(1);
    tokio::time::timeout(deadline, listener).await??;
    assert_eq!(
        tokio::time::timeout(deadline, keepalive).await?,
        KeepAliveOutcome::ConnectionClosed,
    );
    let result = tokio::time::timeout(deadline, receive).await.map_err(
        |_| "sender stopped, but recv_bytes remained pending after listener cancellation",
    )?;
    assert!(result.is_err(), "receiver must report the disconnection");
    assert!(retained.send_bytes(b"after sender exit").await.is_err());
    Ok(())
}

/// If the core pipeline shuts down behind the server's back (standing in for
/// a listener/manager death), the accept loop must stop accepting rather
/// than zombify.
#[tokio::test]
async fn server_stops_accepting_when_core_pipeline_dies() -> TestResult {
    let server = TokioWebSocketServer::setup(
        "127.0.0.1:0".parse()?,
        TimeoutTokio,
        Duration::from_secs(60),
        DEFAULT_MAX_MESSAGE_SIZE,
        MemorySigner::from_bytes(&[0xAA; 32]),
        None,
        MemoryStorage::new(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await?;
    let address = server.address();

    // Sanity: the server accepts TCP while healthy.
    tokio::net::TcpStream::connect(address).await?;

    // Kill the core pipeline without going through `stop()`: the listener
    // and manager futures exit via their channel-close paths, as if they had
    // died unexpectedly.
    server.subduction().shutdown();

    // The supervisor must cancel the accept loop; poll until connections
    // are refused. The deadline only bounds the failure case.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::net::TcpStream::connect(address).await.is_err() {
            break; // accept loop is gone
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "server kept accepting connections after its core pipeline died"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    Ok(())
}
