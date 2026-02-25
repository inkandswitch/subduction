//! Reqwest-based HTTP long-poll client.
//!
//! Connects to a Subduction server via HTTP long-poll, performing the
//! Ed25519 handshake and spawning background tasks for send and recv.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │                  HttpLongPollClient                       │
//! │                                                          │
//! │  Connection::send(msg) ──► outbound_tx ──► sender_task   │
//! │                                    POST /lp/send ──────► │
//! │                                                          │
//! │  Connection::recv() ◄── inbound_reader ◄── poll_task     │
//! │                                    POST /lp/recv ◄────── │
//! │                                                          │
//! │  HttpLongPollConnection handles channel routing,         │
//! │  pending map, and request ID generation.                 │
//! └──────────────────────────────────────────────────────────┘
//! ```

use alloc::{string::String, vec::Vec};
use core::time::Duration;

use subduction_core::{
    connection::{
        authenticated::Authenticated,
        handshake::{self, Audience, HandshakeMessage},
        message::Message,
    },
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};

use future_form::Sendable;
use futures::{future::BoxFuture, FutureExt};
use tokio_util::sync::CancellationToken;

use crate::{
    connection::HttpLongPollConnection, error::ClientError, session::SessionId, SESSION_ID_HEADER,
};

/// An HTTP long-poll client that connects to a Subduction server.
///
/// After connecting, use the returned [`HttpLongPollConnection`] which
/// implements [`Connection<Sendable>`](subduction_core::connection::Connection).
#[derive(Debug)]
pub struct HttpLongPollClient {
    base_url: String,
    http: reqwest::Client,
    default_time_limit: Duration,
}

impl HttpLongPollClient {
    /// Create a new HTTP long-poll client.
    ///
    /// The `base_url` should be the server's base URL, e.g., `http://localhost:8080`.
    /// The long-poll endpoints are at `/lp/handshake`, `/lp/send`, `/lp/recv`, `/lp/disconnect`.
    #[must_use]
    pub fn new(base_url: &str, default_time_limit: Duration) -> Self {
        let base_url = base_url.trim_end_matches('/').to_string();
        Self {
            base_url: base_url.clone(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            default_time_limit,
        }
    }

    /// Connect to the server using discovery mode.
    ///
    /// Performs the Ed25519 handshake, then spawns background tasks for
    /// sending and receiving messages.
    ///
    /// Returns the authenticated connection and the session ID.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the handshake or connection setup fails.
    pub async fn connect_discover<S: Signer<Sendable>>(
        &self,
        signer: &S,
        service_name: &str,
        cancel: CancellationToken,
    ) -> Result<(Authenticated<HttpLongPollConnection, Sendable>, SessionId), ClientError> {
        let audience = Audience::discover(service_name.as_bytes());
        self.connect_inner(signer, audience, cancel).await
    }

    /// Connect to the server with a known peer ID.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the handshake or connection setup fails.
    pub async fn connect<S: Signer<Sendable>>(
        &self,
        signer: &S,
        expected_peer_id: PeerId,
        cancel: CancellationToken,
    ) -> Result<(Authenticated<HttpLongPollConnection, Sendable>, SessionId), ClientError> {
        let audience = Audience::known(expected_peer_id);
        self.connect_inner(signer, audience, cancel).await
    }

    async fn connect_inner<S: Signer<Sendable>>(
        &self,
        signer: &S,
        audience: Audience,
        cancel: CancellationToken,
    ) -> Result<(Authenticated<HttpLongPollConnection, Sendable>, SessionId), ClientError> {
        let now = TimestampSeconds::now();
        let nonce = Nonce::random();

        let mut client_handshake = ClientHttpHandshake {
            http: self.http.clone(),
            base_url: self.base_url.clone(),
            session_id: None,
            response_bytes: None,
        };

        let default_time_limit = self.default_time_limit;
        let base_url = self.base_url.clone();
        let http = self.http.clone();

        let (authenticated, session_id) = handshake::initiate::<Sendable, _, _, _, _>(
            &mut client_handshake,
            |handshake, peer_id| {
                let session_id = handshake
                    .session_id
                    .expect("session_id set after handshake");

                let conn = HttpLongPollConnection::new(peer_id, default_time_limit);

                // Spawn the recv polling task
                let poll_conn = conn.clone();
                let poll_url = format!("{base_url}/lp/recv");
                let poll_http = http.clone();
                let poll_session = session_id;
                let poll_cancel = cancel.clone();

                tokio::spawn(async move {
                    poll_loop(poll_http, poll_url, poll_session, poll_conn, poll_cancel).await;
                });

                // Spawn the send task
                let send_conn = conn.clone();
                let send_url = format!("{base_url}/lp/send");
                let send_http = http;
                let send_session = session_id;
                let send_cancel = cancel;

                tokio::spawn(async move {
                    send_loop(send_http, send_url, send_session, send_conn, send_cancel).await;
                });

                (conn, session_id)
            },
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(|e| ClientError::Authentication(e.to_string()))?;

        Ok((authenticated, session_id))
    }
}

/// Background task that continuously polls `POST /lp/recv` and pushes
/// messages into the connection's inbound channel.
async fn poll_loop(
    http: reqwest::Client,
    url: String,
    session_id: SessionId,
    conn: HttpLongPollConnection,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                tracing::debug!("recv poll loop cancelled");
                break;
            }
            result = http.post(&url)
                .header(SESSION_ID_HEADER, session_id.to_hex())
                .send() => {
                match result {
                    Ok(resp) => {
                        match resp.status().as_u16() {
                            200 => {
                                match resp.bytes().await {
                                    Ok(bytes) => {
                                        match Message::try_decode(&bytes) {
                                            Ok(msg) => {
                                                if conn.push_inbound(msg).await.is_err() {
                                                    tracing::error!("inbound channel closed");
                                                    break;
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!("failed to decode recv message: {e}");
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("failed to read recv response body: {e}");
                                    }
                                }
                            }
                            204 => {
                                // Poll timeout — immediately re-poll
                            }
                            410 => {
                                tracing::info!("session closed by server (410 Gone)");
                                break;
                            }
                            status => {
                                tracing::error!("unexpected recv status: {status}");
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("recv request error: {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}

/// Background task that drains the connection's outbound channel and sends
/// each message via `POST /lp/send`.
async fn send_loop(
    http: reqwest::Client,
    url: String,
    session_id: SessionId,
    conn: HttpLongPollConnection,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                tracing::debug!("send loop cancelled");
                break;
            }
            result = conn.pull_outbound() => {
                match result {
                    Ok(msg) => {
                        let encoded = msg.encode();
                        match http.post(&url)
                            .header(SESSION_ID_HEADER, session_id.to_hex())
                            .header("content-type", "application/octet-stream")
                            .body(encoded)
                            .send()
                            .await
                        {
                            Ok(resp) if resp.status().is_success() => {}
                            Ok(resp) => {
                                tracing::error!("send returned status {}", resp.status());
                            }
                            Err(e) => {
                                tracing::error!("send request error: {e}");
                            }
                        }
                    }
                    Err(_) => {
                        tracing::debug!("outbound channel closed");
                        break;
                    }
                }
            }
        }
    }
}

/// Client-side handshake adapter for HTTP.
///
/// In the `initiate` flow:
/// 1. `send(challenge_bytes)` — POSTs to `/lp/handshake`, captures response + session ID
/// 2. `recv()` — returns the response bytes captured during `send()`
///
/// This maps the streaming `Handshake` interface to a single HTTP round-trip.
struct ClientHttpHandshake {
    http: reqwest::Client,
    base_url: String,
    session_id: Option<SessionId>,
    response_bytes: Option<Vec<u8>>,
}

impl subduction_core::connection::handshake::Handshake<Sendable> for &mut ClientHttpHandshake {
    type Error = ClientError;

    fn send(&mut self, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), Self::Error>> {
        let url = format!("{}/lp/handshake", self.base_url);
        let http = self.http.clone();

        async move {
            let resp = http
                .post(&url)
                .header("content-type", "application/octet-stream")
                .body(bytes)
                .send()
                .await
                .map_err(|e| ClientError::Request(e.to_string()))?;

            let status = resp.status().as_u16();

            // Extract session ID from response header before consuming body
            if let Some(sid_header) = resp.headers().get(SESSION_ID_HEADER) {
                if let Ok(sid_str) = sid_header.to_str() {
                    self.session_id = SessionId::from_hex(sid_str);
                }
            }

            let body = resp
                .bytes()
                .await
                .map_err(|e| ClientError::Request(e.to_string()))?;

            if status == 401 {
                return Err(ClientError::HandshakeRejected {
                    reason: match HandshakeMessage::try_decode(&body) {
                        Ok(HandshakeMessage::Rejection(r)) => alloc::format!("{:?}", r.reason),
                        _ => String::from_utf8_lossy(&body).into_owned(),
                    },
                });
            }

            if status != 200 {
                return Err(ClientError::UnexpectedStatus {
                    status,
                    body: String::from_utf8_lossy(&body).into_owned(),
                });
            }

            // Stash the response bytes for the subsequent recv() call
            self.response_bytes = Some(body.to_vec());

            Ok(())
        }
        .boxed()
    }

    fn recv(&mut self) -> BoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        // Return the response bytes captured during send()
        let bytes = self.response_bytes.take();
        async move {
            bytes.ok_or_else(|| {
                ClientError::HandshakeDecode(
                    "no response bytes available (send not called?)".into(),
                )
            })
        }
        .boxed()
    }
}
