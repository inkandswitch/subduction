//! Tests for the frame size limits on the Iroh (QUIC) transport.
//!
//! When the listener reads a frame whose length prefix exceeds its limit, it
//! tears the connection down: the inbound channel is closed so a parked
//! `recv_bytes` is notified immediately, and the QUIC connection is closed with
//! application error code 1009 (Message Too Big), the QUIC analog of a
//! WebSocket Close(Size) frame. A frame within the limit is delivered whole,
//! including when the limit is above the default.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::time::Duration;

use future_form::Sendable;
use iroh::endpoint::{Connection, Endpoint, presets};
use subduction_core::{peer::id::PeerId, transport::Transport};
use subduction_iroh::{
    DEFAULT_MAX_FRAME_SIZE,
    error::{RecvError, RunError},
    tasks::listener_task,
    transport::IrohTransport,
};
use testresult::TestResult;

/// The listener's limit in the over-cap test. Small, so no test allocates a
/// large frame.
const LIMIT: usize = 4 * 1024;

const TEST_ALPN: &[u8] = b"subduction-over-cap-test";

/// What the listener did with one frame.
struct Outcome {
    listen_result: Result<(), RunError>,
    received: Result<Vec<u8>, RecvError>,
    client: Connection,
    // Both endpoints stay alive, so the server can send CONNECTION_CLOSE and
    // the client can receive it.
    _endpoints: (Endpoint, Endpoint),
    _server_conn: Connection,
}

/// A server endpoint accepting [`TEST_ALPN`], and a client endpoint.
async fn endpoints() -> (Endpoint, Endpoint) {
    let server = Endpoint::builder(presets::Minimal)
        .alpns(vec![TEST_ALPN.to_vec()])
        .bind()
        .await
        .expect("bind server endpoint");
    let client = Endpoint::builder(presets::Minimal)
        .bind()
        .await
        .expect("bind client endpoint");
    (server, client)
}

/// `len` bytes that differ from their neighbours, so a misordered chunk shows.
fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).expect("below 251"))
        .collect()
}

/// Run a listener with `limit`, and have a client write one frame that
/// declares `declared` bytes and carries `payload` of them before finishing the
/// stream.
async fn send_one_frame(limit: usize, declared: u32, payload: usize) -> Outcome {
    let (server_ep, client_ep) = endpoints().await;
    let server_addr = server_ep.addr();

    // Server: accept a bi-stream and run the listener on its recv half. The
    // send half is parked; only the read path matters here.
    let server_join = tokio::spawn(async move {
        let incoming = server_ep.accept().await.expect("incoming");
        let conn = incoming.await.expect("accept connection");
        let (_send, recv) = conn.accept_bi().await.expect("accept bi-stream");

        let (transport, _outbound_rx): (IrohTransport, _) =
            IrohTransport::new(PeerId::new([7u8; 32]), conn.clone());

        // Park a consumer on the inbound channel, as `Subduction::listen` would.
        let recv_transport = transport.clone();
        let recv_join =
            tokio::spawn(async move { Transport::<Sendable>::recv_bytes(&recv_transport).await });

        let listen_result = listener_task(transport, recv, limit).await;
        (server_ep, conn, recv_join, listen_result)
    });

    let client = client_ep
        .connect(server_addr, TEST_ALPN)
        .await
        .expect("client connect");
    let (mut send, _recv) = client.open_bi().await.expect("open bi-stream");
    send.write_all(&declared.to_be_bytes())
        .await
        .expect("write length prefix");
    send.write_all(&pattern(payload))
        .await
        .expect("write payload");
    send.finish().expect("finish stream");

    let (server_ep, server_conn, recv_join, listen_result) =
        tokio::time::timeout(Duration::from_secs(10), server_join)
            .await
            .expect("server task should finish promptly")
            .expect("server task join");

    let received = tokio::time::timeout(Duration::from_millis(500), recv_join)
        .await
        .expect("recv_bytes should resolve promptly")
        .expect("recv task join");

    Outcome {
        listen_result,
        received,
        client,
        _endpoints: (server_ep, client_ep),
        _server_conn: server_conn,
    }
}

/// The application close code the client observes.
async fn close_code(client: &Connection) -> u64 {
    let reason = tokio::time::timeout(Duration::from_secs(5), client.closed())
        .await
        .expect("client should observe the connection close");
    let iroh::endpoint::ConnectionError::ApplicationClosed(app) = &reason else {
        panic!("expected ApplicationClosed, got {reason:?}");
    };
    u64::from(app.error_code)
}

#[tokio::test]
async fn over_cap_frame_tears_down_and_closes_1009() -> TestResult {
    // Fewer payload bytes than declared, so the listener acts on the length
    // prefix, not on the incomplete payload.
    let outcome = send_one_frame(LIMIT, u32::try_from(LIMIT + 1)?, 16).await;

    assert!(
        outcome.listen_result.is_err(),
        "listener should error on an over-cap frame, got {:?}",
        outcome.listen_result
    );
    assert!(
        outcome.received.is_err(),
        "recv_bytes should return Err after the connection tears down"
    );
    assert_eq!(close_code(&outcome.client).await, 1009);
    Ok(())
}

#[tokio::test]
async fn a_frame_within_the_limit_is_delivered_whole() -> TestResult {
    // Larger than one read chunk (64 KiB) and not a multiple of it, so the
    // frame arrives over several reads.
    const LEN: usize = 200 * 1024 + 7;
    let outcome = send_one_frame(256 * 1024, u32::try_from(LEN)?, LEN).await;

    assert_eq!(
        outcome.received.expect("the frame should be delivered"),
        pattern(LEN)
    );
    assert!(
        outcome.listen_result.is_ok(),
        "the stream then ends normally, got {:?}",
        outcome.listen_result
    );
    Ok(())
}

#[tokio::test]
async fn a_frame_of_exactly_the_limit_is_delivered() -> TestResult {
    let outcome = send_one_frame(LIMIT, u32::try_from(LIMIT)?, LIMIT).await;

    assert_eq!(
        outcome.received.expect("a frame at the limit is within it"),
        pattern(LIMIT)
    );
    assert!(outcome.listen_result.is_ok());
    Ok(())
}

#[tokio::test]
async fn a_limit_above_the_default_is_honoured() -> TestResult {
    // The frame declares more than the default allows but carries only 16
    // bytes before the stream ends, so nothing large is sent or allocated. A
    // listener that capped the limit at the default would close with 1009;
    // one that honours it reads until the stream ends early.
    let declared = DEFAULT_MAX_FRAME_SIZE + 1;
    let outcome = send_one_frame(declared, u32::try_from(declared)?, 16).await;

    assert!(
        outcome.listen_result.is_ok(),
        "a stream that ends mid-frame is a normal disconnect, got {:?}",
        outcome.listen_result
    );
    assert!(outcome.received.is_err(), "no message was completed");
    assert_eq!(close_code(&outcome.client).await, 0);
    Ok(())
}
