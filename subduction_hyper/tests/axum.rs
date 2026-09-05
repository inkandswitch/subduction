//! End-to-end coverage for `TungsteniteUpgrade`: an axum router accepts the
//! upgrade, an `async_tungstenite` client connects, and the resulting stream is
//! driven through the real Subduction handshake and [`WebSocket`] transport.

#![allow(clippy::expect_used, reason = "test-only assertions")]
#![allow(clippy::unwrap_used, reason = "test-only assertions")]
#![allow(clippy::panic, reason = "an intentional assertion failure in a test")]

use std::{net::SocketAddr, time::Duration};

use axum::{
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

#[derive(Clone)]
struct AppState {
    config: WebSocketConfig,
    /// Every accepted stream is sent here so the test can drive the server side.
    accepted: mpsc::UnboundedSender<async_tungstenite::WebSocketStream<HyperIo>>,
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

/// Start an axum server with `/ws` behind the gate middleware.
async fn serve(
    config: WebSocketConfig,
) -> (
    SocketAddr,
    mpsc::UnboundedReceiver<async_tungstenite::WebSocketStream<HyperIo>>,
) {
    let (accepted, rx) = mpsc::unbounded_channel();

    let app = Router::new()
        .route("/ws", get(ws_route))
        .route("/health", get(|| async { "ok" }))
        .layer(middleware::from_fn(gate))
        .with_state(AppState { config, accepted });

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

/// Ordinary HTTP requests to the route are rejected with a status, not hung.
#[tokio::test]
async fn plain_http_requests_are_rejected() {
    let (addr, _accepted) = serve(WebSocketConfig::default()).await;
    let client = reqwest_lite::Client::new(addr);

    // Gate middleware runs first.
    assert_eq!(client.get("/ws", &[]).await, StatusCode::UNAUTHORIZED);

    // Past the gate, a non-upgrade GET is a bad request…
    assert_eq!(
        client.get("/ws", &[(GATE_HEADER, "1")]).await,
        StatusCode::BAD_REQUEST
    );

    // …an unsupported version is 426…
    assert_eq!(
        client
            .get(
                "/ws",
                &[
                    (GATE_HEADER, "1"),
                    (header::CONNECTION.as_str(), "upgrade"),
                    (header::UPGRADE.as_str(), "websocket"),
                    (header::SEC_WEBSOCKET_VERSION.as_str(), "8"),
                ],
            )
            .await,
        StatusCode::UPGRADE_REQUIRED
    );

    // …and unrelated routes are untouched.
    assert_eq!(
        client.get("/health", &[(GATE_HEADER, "1")]).await,
        StatusCode::OK
    );
}

/// Minimal HTTP/1.1 GET over raw TCP so the test doesn't need an HTTP client
/// dependency. Returns only the status code.
mod reqwest_lite {
    use std::{fmt::Write as _, net::SocketAddr};

    use axum::http::StatusCode;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    pub(crate) struct Client {
        addr: SocketAddr,
    }

    impl Client {
        pub(crate) const fn new(addr: SocketAddr) -> Self {
            Self { addr }
        }

        pub(crate) async fn get(&self, path: &str, headers: &[(&str, &str)]) -> StatusCode {
            let mut tcp = TcpStream::connect(self.addr).await.expect("connect");

            let mut req = format!("GET {path} HTTP/1.1\r\nHost: {}\r\n", self.addr);
            for (k, v) in headers {
                write!(req, "{k}: {v}\r\n").expect("String is infallible");
            }
            req.push_str("\r\n");
            tcp.write_all(req.as_bytes()).await.expect("write");

            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = tcp.read(&mut chunk).await.expect("read");
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(chunk.get(..n).expect("n <= chunk.len()"));
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }

            let head = std::str::from_utf8(&buf).expect("utf8 status line");
            let code = head
                .split_whitespace()
                .nth(1)
                .expect("status code in status line");
            StatusCode::from_bytes(code.as_bytes()).expect("valid status")
        }
    }
}
