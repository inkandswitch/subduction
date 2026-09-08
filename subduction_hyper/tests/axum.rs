//! End-to-end coverage for `TungsteniteUpgrade`: an axum router accepts the
//! upgrade, an `async_tungstenite` client connects, and the resulting stream is
//! driven through the real Subduction handshake and [`WebSocket`] transport.

#![allow(clippy::expect_used, reason = "test-only assertions")]
#![allow(
    clippy::cast_possible_truncation,
    reason = "hand-built frame with a 5-byte payload"
)]

use std::{net::SocketAddr, time::Duration};

use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::get,
    Router,
};
use future_form::Sendable;
use futures_util::StreamExt;
use subduction_core::{
    handshake::{self, audience::Audience},
    nonce_cache::NonceCache,
    peer::id::PeerId,
    timestamp::TimestampSeconds,
    transport::Transport,
};
use subduction_crypto::{nonce::Nonce, signer::memory::MemorySigner};
use subduction_hyper::{axum::TungsteniteUpgrade, upgrade::HyperIo};
use subduction_websocket::{handshake::WebSocketHandshake, websocket::WebSocket};
use tokio::{net::TcpListener, sync::mpsc};
use tower::ServiceExt as _;
use tungstenite::{error::CapacityError, protocol::WebSocketConfig, Message};

const MAX_DRIFT: Duration = Duration::from_secs(60);
const SERVER_SEED: u8 = 1;
const CLIENT_SEED: u8 = 2;

/// Header a test middleware requires, proving the extractor composes with the
/// rest of the tower stack instead of bypassing it.
const GATE_HEADER: &str = "x-test-gate";

fn signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn peer_id(seed: u8) -> PeerId {
    PeerId::from(signer(seed).verifying_key())
}

type Accepted = mpsc::UnboundedSender<async_tungstenite::WebSocketStream<HyperIo>>;

#[derive(Clone)]
struct AppState {
    config: WebSocketConfig,
    /// Every accepted stream is sent here so the test can drive the server side.
    accepted: Accepted,
}

async fn ws_route(upgrade: TungsteniteUpgrade, State(state): State<AppState>) -> Response {
    upgrade.on_upgrade(state.config, move |ws| async move {
        drop(state.accepted.send(ws));
    })
}

async fn gate(req: Request, next: Next) -> Result<Response, StatusCode> {
    if req.headers().contains_key(GATE_HEADER) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// `/ws` and `/health`, both behind the gate middleware.
fn router(config: WebSocketConfig, accepted: Accepted) -> Router {
    Router::new()
        .route("/ws", get(ws_route))
        .route("/health", get(|| async { "ok" }))
        .layer(middleware::from_fn(gate))
        .with_state(AppState { config, accepted })
}

/// Serve [`router`] on a loopback port.
async fn serve(
    config: WebSocketConfig,
) -> (
    SocketAddr,
    mpsc::UnboundedReceiver<async_tungstenite::WebSocketStream<HyperIo>>,
) {
    let (accepted, rx) = mpsc::unbounded_channel();
    let app = router(config, accepted);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum::serve");
    });

    (addr, rx)
}

/// Connect an `async_tungstenite` client to `/ws`, passing the gate.
async fn connect(
    addr: SocketAddr,
) -> async_tungstenite::WebSocketStream<async_tungstenite::tokio::ConnectStream> {
    let mut req =
        tungstenite::client::IntoClientRequest::into_client_request(format!("ws://{addr}/ws"))
            .expect("client request");
    req.headers_mut()
        .insert(GATE_HEADER, HeaderValue::from_static("1"));

    let (ws, resp) = async_tungstenite::tokio::connect_async(req)
        .await
        .expect("connect");
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    ws
}

/// Frames flow both ways through the upgraded stream, and the tower middleware
/// ran in front of the extractor.
#[tokio::test]
async fn upgrade_yields_working_stream_behind_middleware() {
    let (addr, mut accepted) = serve(WebSocketConfig::default()).await;

    let mut client = connect(addr).await;
    let mut server = accepted.recv().await.expect("server side of the upgrade");

    client
        .send(Message::Binary(b"ping".as_slice().into()))
        .await
        .expect("client send");
    let got = server.next().await.expect("server recv").expect("frame");
    assert_eq!(got, Message::Binary(b"ping".as_slice().into()));

    server
        .send(Message::Binary(b"pong".as_slice().into()))
        .await
        .expect("server send");
    let got = client.next().await.expect("client recv").expect("frame");
    assert_eq!(got, Message::Binary(b"pong".as_slice().into()));
}

/// The full Subduction path: challenge/response handshake over the upgraded
/// stream, then bytes through `WebSocket::send_bytes` / `recv_bytes`.
#[tokio::test]
async fn subduction_handshake_and_transport_over_axum_upgrade() {
    let (addr, mut accepted) = serve(WebSocketConfig::default()).await;

    let client_ws = connect(addr).await;
    let server_ws = accepted.recv().await.expect("server side of the upgrade");

    let server_signer = signer(SERVER_SEED);
    let nonce_cache = NonceCache::default();
    let server_task = tokio::spawn(async move {
        let (auth, sender_fut) = handshake::respond::<Sendable, _, _, _, _>(
            WebSocketHandshake::new(server_ws),
            |hs, peer_id| WebSocket::<_, Sendable>::new(hs.into_inner(), peer_id),
            &server_signer,
            &nonce_cache,
            peer_id(SERVER_SEED),
            None,
            TimestampSeconds::now(),
            MAX_DRIFT,
        )
        .await
        .expect("server handshake");
        tokio::spawn(sender_fut);
        auth
    });

    let client_signer = signer(CLIENT_SEED);
    let (client_auth, client_sender) = handshake::initiate::<Sendable, _, _, _, _>(
        WebSocketHandshake::new(client_ws),
        |hs, peer_id| WebSocket::<_, Sendable>::new(hs.into_inner(), peer_id),
        &client_signer,
        Audience::known(peer_id(SERVER_SEED)),
        TimestampSeconds::now(),
        Nonce::random(),
    )
    .await
    .expect("client handshake");
    tokio::spawn(client_sender);

    let server_auth = server_task.await.expect("server task");

    assert_eq!(client_auth.peer_id(), peer_id(SERVER_SEED));
    assert_eq!(server_auth.peer_id(), peer_id(CLIENT_SEED));

    let client = client_auth.into_inner();
    let server = server_auth.into_inner();
    let listener = server.clone();
    tokio::spawn(async move { listener.listen().await });

    client.send_bytes(b"hello via axum").await.expect("send");
    let got = tokio::time::timeout(Duration::from_secs(5), server.recv_bytes())
        .await
        .expect("timely")
        .expect("recv");
    assert_eq!(got, b"hello via axum");
}

/// `WebSocketConfig` passed to `on_upgrade` is honoured: an over-cap frame
/// surfaces as `Capacity(MessageTooLong)` on the server stream.
#[tokio::test]
async fn config_caps_are_applied() {
    const CAP: usize = 1024;

    let config = WebSocketConfig::default()
        .max_message_size(Some(CAP))
        .max_frame_size(Some(CAP));
    let (addr, mut accepted) = serve(config).await;

    let mut client = connect(addr).await;
    let mut server = accepted.recv().await.expect("server side of the upgrade");

    client
        .send(Message::Binary(vec![0u8; CAP * 4].into()))
        .await
        .expect("client send");

    let err = server
        .next()
        .await
        .expect("server yields something")
        .expect_err("over-cap frame must error");
    assert!(
        matches!(
            err,
            tungstenite::Error::Capacity(CapacityError::MessageTooLong { .. })
        ),
        "unexpected error: {err:?}"
    );
}

/// Ordinary HTTP requests are rejected with a status (never hung), the
/// rejection carries the RFC-required advisory header, and unrelated routes
/// are untouched. Driven through `tower::ServiceExt::oneshot`, so no TCP.
#[tokio::test]
async fn plain_http_requests_are_rejected() {
    let (accepted, _rx) = mpsc::unbounded_channel();
    let app = router(WebSocketConfig::default(), accepted);

    let get = |path: &str, headers: &[(&str, &str)]| {
        let mut builder = axum::http::Request::get(path);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        builder.body(Body::empty()).expect("valid request")
    };

    // Gate middleware runs first.
    let resp = app
        .clone()
        .oneshot(get("/ws", &[]))
        .await
        .expect("infallible");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Past the gate, a non-upgrade GET is a bad request…
    let resp = app
        .clone()
        .oneshot(get("/ws", &[(GATE_HEADER, "1")]))
        .await
        .expect("infallible");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // …an unsupported version is 426 and advertises what we speak…
    let resp = app
        .clone()
        .oneshot(get(
            "/ws",
            &[
                (GATE_HEADER, "1"),
                (header::CONNECTION.as_str(), "upgrade"),
                (header::UPGRADE.as_str(), "websocket"),
                (header::SEC_WEBSOCKET_VERSION.as_str(), "8"),
            ],
        ))
        .await
        .expect("infallible");
    assert_eq!(resp.status(), StatusCode::UPGRADE_REQUIRED);
    assert_eq!(
        resp.headers().get(header::SEC_WEBSOCKET_VERSION),
        Some(&HeaderValue::from_static("13"))
    );

    // …a fully-formed upgrade with no hyper connection behind it is a server
    // error, not a client one…
    let resp = app
        .clone()
        .oneshot(get(
            "/ws",
            &[
                (GATE_HEADER, "1"),
                (header::CONNECTION.as_str(), "upgrade"),
                (header::UPGRADE.as_str(), "websocket"),
                (header::SEC_WEBSOCKET_VERSION.as_str(), "13"),
                (
                    header::SEC_WEBSOCKET_KEY.as_str(),
                    "dGhlIHNhbXBsZSBub25jZQ==",
                ),
            ],
        ))
        .await
        .expect("infallible");
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

    // …and unrelated routes are untouched.
    let resp = app
        .oneshot(get("/health", &[(GATE_HEADER, "1")]))
        .await
        .expect("infallible");
    assert_eq!(resp.status(), StatusCode::OK);
}

/// An HTTP/1.0 upgrade attempt over a real socket is a `400`, not a `500`:
/// hyper never arms `OnUpgrade` for 1.0, so the version check must catch it
/// before the extension lookup does.
#[tokio::test]
async fn http10_upgrade_is_a_client_error_on_the_wire() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (addr, _accepted) = serve(WebSocketConfig::default()).await;
    let mut tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");

    let req = format!(
        "GET /ws HTTP/1.0\r\nHost: {addr}\r\n{GATE_HEADER}: 1\r\n\
         Connection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    tcp.write_all(req.as_bytes()).await.expect("write");

    let mut head = Vec::new();
    tcp.read_to_end(&mut head).await.expect("read");
    let status_line = std::str::from_utf8(&head)
        .expect("utf8")
        .lines()
        .next()
        .expect("status line");
    assert!(
        status_line.ends_with("400 Bad Request"),
        "got {status_line:?}"
    );
}

/// A client that pipelines its first frame into the same TCP segment as the
/// upgrade request must not lose it: hyper's leftover read buffer is replayed
/// ahead of the socket when the connection is handed over.
#[tokio::test]
async fn first_frame_pipelined_with_request_is_not_lost() {
    use tokio::io::AsyncWriteExt;

    let (addr, mut accepted) = serve(WebSocketConfig::default()).await;
    let mut tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");

    let req = format!(
        "GET /ws HTTP/1.1\r\nHost: {addr}\r\n{GATE_HEADER}: 1\r\n\
         Connection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );

    // A masked binary frame (clients must mask) carrying `b"early"`.
    // FIN + opcode 0x2; MASK bit + len 5; mask key; payload XOR key.
    let mask = [0x11, 0x22, 0x33, 0x44];
    let payload = b"early";
    let mut frame = vec![0x82, 0x80 | payload.len() as u8];
    frame.extend_from_slice(&mask);
    frame.extend(payload.iter().zip(mask.iter().cycle()).map(|(b, m)| b ^ m));

    let mut segment = req.into_bytes();
    segment.extend_from_slice(&frame);
    tcp.write_all(&segment)
        .await
        .expect("write request + frame together");

    let mut server = accepted.recv().await.expect("server side of the upgrade");
    let got = tokio::time::timeout(Duration::from_secs(5), server.next())
        .await
        .expect("timely")
        .expect("server recv")
        .expect("frame");
    assert_eq!(got, Message::Binary(payload.as_slice().into()));
}
