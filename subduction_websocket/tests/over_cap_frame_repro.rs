//! Tests for the over-cap WebSocket frame failure mode.
//!
//! Two properties are covered:
//!
//! 1. An inbound frame whose declared length exceeds the receiver's
//!    `max_frame_size` returns `Err(Capacity(MessageTooLong))` and leaves the
//!    `tungstenite` stream unrecoverable: a small, valid follow-up message on
//!    the same connection is not delivered. A "skip the offending message and
//!    keep reading" strategy is therefore impossible at the transport layer.
//!
//! 2. When [`WebSocket::listen`] exits on an over-cap or otherwise fatal read
//!    error, it closes the inbound channel so a parked `recv_bytes` is notified
//!    immediately rather than stranding until keepalive reaps the peer (~80 s
//!    with [`KeepAlive::balanced`]), and it sends the peer a graceful Close
//!    frame.
//!
//! Property 1 is a fact about `tungstenite`. If a future version drains and
//! discards an over-cap frame, [`oversized_frame_poisons_receiver_stream`]
//! should be revisited.

#![allow(clippy::expect_used, reason = "test-only assertions")]
#![allow(clippy::unwrap_used, reason = "test-only assertions")]
#![allow(clippy::panic, reason = "an intentional assertion failure in a test")]

use std::time::Duration;

use futures_util::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tungstenite::protocol::WebSocketConfig;

/// Receiver-side cap used for both tests. Small so we can exceed it cheaply.
const RECV_CAP: usize = 4 * 1024;

/// Marker byte filling the small follow-up message, so we can recognize it
/// among any post-error frames.
const MARKER: u8 = 0x5A;

/// Build a connected client/server `WebSocketStream` pair over loopback TCP,
/// with the *server* (receiver) configured to the given cap on both message
/// and frame size — mirroring the project convention of setting them equal.
async fn connected_pair(
    recv_cap: usize,
) -> (
    async_tungstenite::WebSocketStream<
        async_tungstenite::tokio::TokioAdapter<TcpStream>,
    >,
    async_tungstenite::WebSocketStream<
        async_tungstenite::tokio::TokioAdapter<TcpStream>,
    >,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let recv_config = WebSocketConfig::default()
        .max_message_size(Some(recv_cap))
        .max_frame_size(Some(recv_cap));

    let server_join = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept tcp");
        async_tungstenite::tokio::accept_async_with_config(tcp, Some(recv_config))
            .await
            .expect("ws accept")
    });

    let tcp = TcpStream::connect(addr).await.expect("connect tcp");
    let uri = format!("ws://{addr}/");
    let (client_ws, _resp) =
        async_tungstenite::tokio::client_async_with_config(uri, tcp, None)
            .await
            .expect("ws client");

    let server_ws = server_join.await.expect("server task");
    (client_ws, server_ws)
}

/// An over-cap frame leaves the receiver stream unrecoverable.
///
/// The sender writes one binary frame larger than the receiver's cap, then a
/// small valid binary frame tagged with a distinctive byte. The receiver sees
/// `MessageTooLong` on the over-cap frame and cannot read the small follow-up
/// on any subsequent poll.
///
/// `tungstenite` returns `MessageTooLong` after parsing the over-cap frame's
/// header but before consuming its payload, and without discarding the stuck
/// header. The unread payload bytes remain in the socket and the next read
/// re-encounters the same header, so there is no way to resynchronize and
/// reach the follow-up message.
#[tokio::test]
async fn oversized_frame_poisons_receiver_stream() {
    let (mut client_ws, mut server_ws) = connected_pair(RECV_CAP).await;

    // One oversized message (browsers send this as a single unfragmented
    // frame), followed by a small valid one with a recognizable marker byte.
    let oversized = vec![0xABu8; RECV_CAP * 2];
    let small = vec![MARKER; 16];

    let sender = tokio::spawn(async move {
        client_ws
            .send(tungstenite::Message::Binary(oversized.into()))
            .await
            .expect("send oversized");
        client_ws
            .send(tungstenite::Message::Binary(small.into()))
            .await
            .expect("send small");
        // Keep the client alive so the server's failure is about the frame,
        // not about the TCP connection being torn down.
        tokio::time::sleep(Duration::from_millis(300)).await;
        client_ws
    });

    // First read: the over-cap frame must surface as MessageTooLong.
    let first = server_ws.next().await.expect("a first item");
    let err = first.expect_err("over-cap frame must be an error, not delivered");
    assert!(
        matches!(
            err,
            tungstenite::Error::Capacity(
                tungstenite::error::CapacityError::MessageTooLong { .. }
            )
        ),
        "expected Capacity(MessageTooLong), got {err:?}"
    );

    // Now poll the stream several more times. If it were recoverable, the small
    // follow-up (a `Binary` whose bytes are all MARKER) would appear on one of
    // these reads. Record the exact sequence so the mechanism is visible, and
    // assert the follow-up NEVER arrives cleanly.
    let mut transcript: Vec<String> = Vec::new();
    let mut recovered_followup = false;

    for attempt in 0..5 {
        // Bound each poll so a permanently-stuck read can't hang the test.
        let polled = tokio::time::timeout(Duration::from_millis(200), server_ws.next()).await;

        match polled {
            Err(_elapsed) => {
                transcript.push(format!("attempt {attempt}: read STUCK (timed out)"));
                // A stuck read is itself proof of non-recovery; stop probing.
                break;
            }
            Ok(None) => {
                transcript.push(format!("attempt {attempt}: stream ended (None)"));
                break;
            }
            Ok(Some(Err(e))) => {
                transcript.push(format!("attempt {attempt}: Err({e:?})"));
            }
            Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) => {
                let is_followup = bytes.len() == 16 && bytes.iter().all(|&b| b == MARKER);
                transcript.push(format!(
                    "attempt {attempt}: Ok(Binary[{} bytes], is_followup={is_followup})",
                    bytes.len()
                ));
                if is_followup {
                    recovered_followup = true;
                    break;
                }
            }
            Ok(Some(Ok(other))) => {
                transcript.push(format!("attempt {attempt}: Ok({other:?})"));
            }
        }
    }

    let report = transcript.join("\n  ");
    assert!(
        !recovered_followup,
        "the small follow-up message was delivered cleanly after the over-cap \
         frame, so the stream resynchronized. If tungstenite now drains and \
         discards over-cap frames, revisit this test.\n  Transcript:\n  {report}"
    );

    // Surface the transcript with `--nocapture`.
    eprintln!("post-error read transcript:\n  {report}");

    let _client_ws = sender.await.expect("sender task");
}

/// A fatal/peer-induced `listen()` error tears the connection down promptly.
///
/// When `listen()` exits on an over-cap inbound frame (or any other fatal read
/// error), it closes the inbound channel so a `recv_bytes()` parked on it
/// resolves with `Err` immediately, rather than stranding until keepalive reaps
/// the peer (~80 s). It also sends the peer a graceful Close frame.
///
/// Drives a real `subduction_websocket::WebSocket` receiver against a raw
/// `async-tungstenite` sender, runs `listen()` and a concurrent `recv_bytes()`
/// together, and requires the recv to resolve within a short budget.
#[tokio::test]
async fn listen_error_tears_down_connection_promptly() {
    use future_form::Sendable;
    use subduction_core::peer::id::PeerId;
    use subduction_websocket::websocket::WebSocket;

    let (mut client_ws, server_ws) = connected_pair(RECV_CAP).await;

    let (ws, sender_task): (
        WebSocket<async_tungstenite::tokio::TokioAdapter<TcpStream>, Sendable>,
        _,
    ) = WebSocket::new(server_ws, PeerId::new([7u8; 32]));

    // Spawn the sender task (drives the write half) so the receiver is fully
    // operational, matching production wiring.
    let sender_join = tokio::spawn(sender_task);

    // A consumer parked on the inbound channel, as `Subduction::listen` would
    // be. It must observe the disconnect promptly.
    let ws_for_recv = ws.clone();
    let recv_join = tokio::spawn(async move {
        subduction_core::transport::Transport::<Sendable>::recv_bytes(&ws_for_recv).await
    });

    // Sender: one over-cap frame, then keep reading its own stream so we can
    // observe the graceful Close frame the receiver originates. Returns the
    // first Close frame's code (if any) for a wire-level assertion below.
    let oversized = vec![0xCDu8; RECV_CAP * 2];
    let client_join = tokio::spawn(async move {
        client_ws
            .send(tungstenite::Message::Binary(oversized.into()))
            .await
            .expect("send oversized");

        // Drain inbound frames until we see a Close (or the stream ends).
        let mut close_code = None;
        for _ in 0..10 {
            match tokio::time::timeout(Duration::from_millis(300), client_ws.next()).await {
                Ok(Some(Ok(tungstenite::Message::Close(frame)))) => {
                    close_code = frame.map(|f| f.code);
                    break;
                }
                Ok(Some(Ok(_other))) => {}
                Ok(Some(Err(_)) | None) | Err(_) => break,
            }
        }
        close_code
    });

    // `listen()` observes the over-cap frame and returns an error.
    let listen_result = tokio::time::timeout(Duration::from_secs(5), ws.listen())
        .await
        .expect("listen should resolve (not hang) on an over-cap frame");

    assert!(
        listen_result.is_err(),
        "listen() should return Err on an over-cap inbound frame, got {listen_result:?}"
    );

    // The parked recv resolves (with Err) because the connection tore itself
    // down.
    let recv_outcome = tokio::time::timeout(Duration::from_millis(500), recv_join).await;

    assert!(
        recv_outcome.is_ok(),
        "recv_bytes should be notified promptly after listen() errors out, \
         instead of waiting ~80 s for keepalive"
    );
    let recv_result = recv_outcome.expect("recv resolved within budget").expect("recv task joined");
    assert!(
        recv_result.is_err(),
        "the prompt recv result should be an Err (disconnect), got Ok"
    );

    // The peer receives a graceful Close(Size) frame (1009 Message Too Big),
    // not an abrupt TCP drop.
    let close_code = client_join.await.expect("client task");
    assert_eq!(
        close_code,
        Some(tungstenite::protocol::frame::coding::CloseCode::Size),
        "peer should receive a graceful Close(Size) frame on over-cap, got {close_code:?}"
    );

    ws.close_channels();
    drop(sender_join.await);
}

/// A clean peer-initiated disconnect notifies a parked `recv_bytes` promptly
/// and does not originate a duplicate Close.
///
/// When the peer sends a `Message::Close`, `tungstenite` auto-echoes the Close
/// reply, so the listener only tears down its channels (notifying `recv_bytes`)
/// without enqueuing a second Close of its own.
#[tokio::test]
async fn clean_close_tears_down_promptly_without_double_close() {
    use future_form::Sendable;
    use subduction_core::peer::id::PeerId;
    use subduction_websocket::websocket::WebSocket;

    // Nothing here is over-cap; this exercises the clean-close path.
    let (mut client_ws, server_ws) = connected_pair(RECV_CAP).await;

    let (ws, sender_task): (
        WebSocket<async_tungstenite::tokio::TokioAdapter<TcpStream>, Sendable>,
        _,
    ) = WebSocket::new(server_ws, PeerId::new([9u8; 32]));
    let sender_join = tokio::spawn(sender_task);

    // Parked consumer, as `Subduction::listen` would be.
    let ws_for_recv = ws.clone();
    let recv_join = tokio::spawn(async move {
        subduction_core::transport::Transport::<Sendable>::recv_bytes(&ws_for_recv).await
    });

    // Peer initiates a clean close, then drains its own stream to observe how
    // many Close frames come back. tungstenite auto-echoes exactly one; our
    // listener must not add a second.
    let client_join = tokio::spawn(async move {
        client_ws
            .send(tungstenite::Message::Close(Some(
                tungstenite::protocol::CloseFrame {
                    code: tungstenite::protocol::frame::coding::CloseCode::Normal,
                    reason: "bye".into(),
                },
            )))
            .await
            .expect("send close");

        let mut close_frames = 0u32;
        for _ in 0..10 {
            match tokio::time::timeout(Duration::from_millis(200), client_ws.next()).await {
                Ok(Some(Ok(tungstenite::Message::Close(_)))) => close_frames += 1,
                Ok(Some(Ok(_other))) => {}
                Ok(Some(Err(_)) | None) | Err(_) => break,
            }
        }
        close_frames
    });

    // listen() should return Ok(()) on a clean remote close.
    let listen_result = tokio::time::timeout(Duration::from_secs(5), ws.listen())
        .await
        .expect("listen should resolve on a clean remote close");
    assert!(
        listen_result.is_ok(),
        "clean remote close should be Ok(()), got {listen_result:?}"
    );

    // The parked recv is notified promptly.
    let recv_outcome = tokio::time::timeout(Duration::from_millis(500), recv_join).await;
    assert!(
        recv_outcome.is_ok(),
        "recv_bytes should be notified promptly on a clean remote close, not \
         left parked until keepalive"
    );
    let recv_result = recv_outcome
        .expect("recv resolved within budget")
        .expect("recv task joined");
    assert!(recv_result.is_err(), "recv after disconnect should be Err");

    // No duplicate Close: tungstenite's auto-echo is the only Close the peer
    // sees (we must not originate our own on the peer-initiated path).
    let close_frames = client_join.await.expect("client task");
    assert!(
        close_frames <= 1,
        "expected at most one (auto-echoed) Close frame, saw {close_frames}"
    );

    ws.close_channels();
    drop(sender_join.await);
}
