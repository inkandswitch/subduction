//! A peer uploading one large message slowly must not be reaped by the
//! keepalive while its bytes are still arriving.
//!
//! A browser sends each message as one frame, and a pong cannot go out in the
//! middle of a frame, so the pong to every ping waits until the upload ends.
//! The client here behaves the same way: it trickles one frame for longer
//! than the unanswered-ping ceiling and never reads, so it never pongs.

#![allow(clippy::expect_used, missing_docs, unreachable_pub)]

use core::{future::IntoFuture, net::SocketAddr, time::Duration};

use future_form::Sendable;
use subduction_core::{peer::id::PeerId, transport::Transport};
use subduction_websocket::{
    counted::Counted,
    sleep::TokioSleeper,
    tokio::stream::AcceptStream,
    websocket::{KeepAlive, KeepAliveOutcome, MAX_UNANSWERED_PINGS, WebSocket},
};
use testresult::TestResult;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

const KEEPALIVE: KeepAlive = KeepAlive {
    ping_interval: Duration::from_millis(100),
    pong_timeout: Duration::from_millis(50),
    missed_pong_threshold: core::num::NonZeroU32::new(2).expect("2 is nonzero"),
};

/// The unanswered-ping ceiling under [`KEEPALIVE`]: 8 cycles of 150 ms.
const CEILING: Duration = Duration::from_millis(150 * MAX_UNANSWERED_PINGS as u64);

/// 32 KiB in 256-byte chunks every 15 ms takes about 1.9 s, past the ceiling.
const PAYLOAD_LEN: usize = 32 * 1024;
const CHUNK: usize = 256;
const PAUSE: Duration = Duration::from_millis(15);

struct Server {
    transport: WebSocket<AcceptStream, Sendable>,
    keepalive: JoinHandle<KeepAliveOutcome>,
}

/// Accept one connection the way the CLI does: count inbound reads under
/// tungstenite, then run the transport with keepalive.
async fn accept(listener: TcpListener) -> Server {
    let (tcp, _) = listener.accept().await.expect("accept");
    let tcp = Counted::new(tcp);
    let inbound = tcp.progress();
    let ws = async_tungstenite::tokio::accept_async(tcp)
        .await
        .expect("WebSocket upgrade");

    let (transport, sender, keepalive) =
        WebSocket::new_with_keepalive(ws, inbound, PeerId::new([1; 32]), KEEPALIVE, TokioSleeper);
    tokio::spawn(sender);
    let listening = transport.clone();
    tokio::spawn(async move { listening.listen().await });

    Server {
        transport,
        keepalive: tokio::spawn(keepalive.into_future()),
    }
}

/// Connect and complete the HTTP upgrade by hand, reading the response head
/// byte by byte so no WebSocket frame is consumed with it.
async fn connect(addr: SocketAddr) -> TestResult<TcpStream> {
    let mut client = TcpStream::connect(addr).await?;
    client.set_nodelay(true)?;
    let request = format!(
        "GET / HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    client.write_all(request.as_bytes()).await?;

    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(client.read_u8().await?);
    }
    assert!(
        head.starts_with(b"HTTP/1.1 101"),
        "upgrade refused: {head:?}"
    );
    Ok(client)
}

/// One masked binary frame, as a client must send it (RFC 6455 §5.2).
fn masked_binary_frame(payload: &[u8]) -> TestResult<Vec<u8>> {
    const MASK: [u8; 4] = [0x37, 0xFA, 0x21, 0x3D];
    let mut frame = vec![0x82];
    match u16::try_from(payload.len()) {
        Ok(len) if len <= 125 => frame.push(0x80 | u8::try_from(len)?),
        Ok(len) => {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&len.to_be_bytes());
        }
        Err(_) => {
            frame.push(0x80 | 127);
            frame.extend_from_slice(&u64::try_from(payload.len())?.to_be_bytes());
        }
    }
    frame.extend_from_slice(&MASK);
    frame.extend(payload.iter().zip(MASK.iter().cycle()).map(|(b, m)| b ^ m));
    Ok(frame)
}

fn payload() -> Vec<u8> {
    (0..PAYLOAD_LEN)
        .map(|i| u8::try_from(i % 251).expect("below 251"))
        .collect()
}

async fn trickle(client: &mut TcpStream, bytes: &[u8]) -> TestResult {
    for chunk in bytes.chunks(CHUNK) {
        client.write_all(chunk).await?;
        tokio::time::sleep(PAUSE).await;
    }
    Ok(())
}

#[tokio::test]
async fn a_slow_upload_is_delivered_without_a_reap() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(accept(listener));
    let mut client = connect(addr).await?;
    let server = server.await?;

    let payload = payload();
    let frame = masked_binary_frame(&payload)?;
    let started = tokio::time::Instant::now();
    let upload = tokio::spawn(async move { trickle(&mut client, &frame).await.map(|()| client) });

    let received = tokio::time::timeout(Duration::from_secs(10), server.transport.recv_bytes())
        .await?
        .map_err(|_| "the connection was torn down before the upload finished")?;
    let elapsed = started.elapsed();

    assert_eq!(received, payload);
    assert!(
        elapsed > CEILING,
        "the upload took {elapsed:?}; it must outlast the {CEILING:?} ceiling to test it"
    );
    assert!(
        !server.keepalive.is_finished(),
        "keepalive must not have reaped the connection"
    );

    drop(upload.await??);
    Ok(())
}

#[tokio::test]
async fn an_upload_that_stalls_is_still_reaped() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(accept(listener));
    let mut client = connect(addr).await?;
    let server = server.await?;

    // Half the frame takes about 0.96 s, past the 300 ms miss threshold, so
    // the connection must survive that long and then be reaped once the
    // bytes stop. The client stays open the whole time.
    let frame = masked_binary_frame(&payload())?;
    let (first_half, _) = frame.split_at(frame.len() / 2);
    trickle(&mut client, first_half).await?;
    assert!(
        !server.keepalive.is_finished(),
        "keepalive must not reap while bytes are arriving"
    );

    let outcome = tokio::time::timeout(Duration::from_secs(5), server.keepalive)
        .await
        .map_err(|_| "a stalled peer must still be reaped")??;
    assert_eq!(outcome, KeepAliveOutcome::Timeout { missed: 2 });
    assert!(
        server.transport.recv_bytes().await.is_err(),
        "the reaped connection must be torn down"
    );

    drop(client);
    Ok(())
}
