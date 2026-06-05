//! Tests for the over-cap frame failure mode on the Iroh (QUIC) transport.
//!
//! When the listener reads a frame whose length prefix exceeds the cap, it
//! tears the connection down: the inbound channel is closed so a parked
//! `recv_bytes` is notified immediately, and the QUIC connection is closed with
//! application error code 1009 (Message Too Big), the QUIC analog of a
//! WebSocket Close(Size) frame.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::time::Duration;

use future_form::Sendable;
use iroh::endpoint::presets;
use subduction_core::{peer::id::PeerId, transport::Transport};
use subduction_iroh::{tasks::listener_task, transport::IrohTransport};
use testresult::TestResult;

/// A frame prefix larger than the listener's 50 MiB cap.
const OVER_CAP_LEN: u32 = 60 * 1024 * 1024;

const TEST_ALPN: &[u8] = b"subduction-over-cap-test";

#[tokio::test]
async fn over_cap_frame_tears_down_and_closes_1009() -> TestResult {
    // Two endpoints on loopback.
    let server_ep = iroh::Endpoint::builder(presets::Minimal)
        .alpns(vec![TEST_ALPN.to_vec()])
        .bind()
        .await
        .expect("bind server endpoint");
    let client_ep = iroh::Endpoint::builder(presets::Minimal)
        .bind()
        .await
        .expect("bind client endpoint");

    let server_addr = server_ep.addr();

    // Server: accept the connection, accept a bi-stream, run the listener on the
    // recv half. The send half is parked (we only care about the read path).
    // The endpoint is returned so its QUIC driver keeps running long enough to
    // transmit the CONNECTION_CLOSE frame to the client.
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

        let listen_result = listener_task(transport, recv).await;
        (server_ep, conn, recv_join, listen_result)
    });

    // Client: connect, open a bi-stream, write a raw over-cap length prefix.
    let conn = client_ep
        .connect(server_addr, TEST_ALPN)
        .await
        .expect("client connect");
    let (mut send, _recv) = conn.open_bi().await.expect("open bi-stream");
    send.write_all(&OVER_CAP_LEN.to_be_bytes())
        .await
        .expect("write over-cap length prefix");
    // A few payload bytes (fewer than declared) so the listener acts on the
    // length prefix, not on the (incomplete) payload.
    send.write_all(&[0u8; 16]).await.expect("write payload");

    let (_server_ep, _server_conn, recv_join, listen_result) =
        tokio::time::timeout(Duration::from_secs(10), server_join)
            .await
            .expect("server task should finish promptly")
            .expect("server task join");

    // The listener returns an error for the over-cap frame.
    assert!(
        listen_result.is_err(),
        "listener should error on an over-cap frame, got {listen_result:?}"
    );

    // The parked recv is notified promptly (inbound channel closed on teardown).
    let recv_outcome = tokio::time::timeout(Duration::from_millis(500), recv_join)
        .await
        .expect("recv_bytes should resolve promptly after teardown")
        .expect("recv task join");
    assert!(
        recv_outcome.is_err(),
        "recv_bytes should return Err after the connection tears down"
    );

    // The client observes the remote (application) close with code 1009.
    let reason = tokio::time::timeout(Duration::from_secs(5), conn.closed())
        .await
        .expect("client should observe the connection close");
    let iroh::endpoint::ConnectionError::ApplicationClosed(app) = &reason else {
        panic!("expected ApplicationClosed, got {reason:?}");
    };
    assert_eq!(
        u64::from(app.error_code),
        1009,
        "over-cap should close with code 1009"
    );

    Ok(())
}
