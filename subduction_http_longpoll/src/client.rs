//! Generic HTTP long-poll client.
//!
//! Connects to a Subduction server via HTTP long-poll, performing the
//! Ed25519 handshake and returning background task futures for send and recv.
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
//!
//! The client returns background futures rather than spawning them, leaving
//! the caller to decide how to run them (e.g., `tokio::spawn`, `spawn_local`).

use alloc::{format, string::String, vec::Vec};
use core::time::Duration;

use future_form::{FutureForm, Local, Sendable};
use futures::{
    FutureExt,
    future::{Either, select},
    pin_mut,
};
use subduction_core::{
    connection::{
        handshake::{self, Audience, HandshakeMessage},
        message::Message,
        timeout::Timeout,
    },
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};

use crate::{
    SESSION_ID_HEADER, connection::HttpLongPollConnection, error::ClientError,
    http_client::HttpClient, session::SessionId,
};

/// Result of a successful connection, containing the authenticated connection
/// and background task futures that the caller must spawn.
pub struct ConnectResult<K: future_form::FutureForm, O>
where
    HttpLongPollConnection<O>: subduction_core::connection::Connection<K>,
{
    /// The authenticated connection, ready for registration with Subduction.
    pub authenticated:
        subduction_core::connection::authenticated::Authenticated<HttpLongPollConnection<O>, K>,

    /// The session ID assigned by the server.
    pub session_id: SessionId,

    /// Background recv-polling task. Must be spawned to drive inbound messages.
    pub poll_task: K::Future<'static, ()>,

    /// Background send task. Must be spawned to drive outbound messages.
    pub send_task: K::Future<'static, ()>,
}

impl<K: future_form::FutureForm, O> core::fmt::Debug for ConnectResult<K, O>
where
    HttpLongPollConnection<O>: subduction_core::connection::Connection<K>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConnectResult")
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

/// An HTTP long-poll client that connects to a Subduction server.
///
/// After connecting, spawn the returned [`ConnectResult::poll_task`] and
/// [`ConnectResult::send_task`] futures to drive the connection.
///
/// # Type Parameters
///
/// - `H`: The HTTP client implementation
/// - `O`: The timeout strategy
#[derive(Debug, Clone)]
pub struct HttpLongPollClient<H, O> {
    base_url: String,
    http: H,
    timeout: O,
    default_time_limit: Duration,
}

impl<H, O> HttpLongPollClient<H, O> {
    /// Create a new HTTP long-poll client.
    ///
    /// - `base_url`: The server's base URL, e.g., `http://localhost:8080`.
    /// - `http`: The HTTP client implementation.
    /// - `timeout`: The timeout strategy.
    /// - `default_time_limit`: Default timeout for call operations.
    #[must_use]
    pub fn new(base_url: &str, http: H, timeout: O, default_time_limit: Duration) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
            timeout,
            default_time_limit,
        }
    }
}

// ---------------------------------------------------------------------------
// Connect methods stamped out for Sendable and Local
// ---------------------------------------------------------------------------

/// Generates connect methods for a concrete [`FutureForm`] variant.
///
/// The only differences between `Sendable` and `Local` are the concrete
/// `K` type, `.boxed()` vs `.boxed_local()` for task futures, extra
/// `Send + Sync` bounds for `Sendable`, and the public method names
/// (`connect` / `connect_local`, `connect_discover` / `connect_discover_local`).
macro_rules! impl_connect {
    ($K:ty, $box_fn:ident, $connect:ident, $discover:ident, $inner:ident $(, $extra_bounds:tt)*) => {
        impl<H, O> HttpLongPollClient<H, O>
        where
            H: HttpClient<$K> $(+ $extra_bounds)* + 'static,
            O: Timeout<$K> $(+ $extra_bounds)* + 'static,
        {
            /// Connect to the server using discovery mode.
            ///
            /// # Errors
            ///
            /// Returns [`ClientError`] if the handshake or connection setup fails.
            pub async fn $discover<Sig: Signer<$K>>(
                &self,
                signer: &Sig,
                service_name: &str,
                now: TimestampSeconds,
            ) -> Result<ConnectResult<$K, O>, ClientError> {
                let audience = Audience::discover(service_name.as_bytes());
                self.$inner(signer, audience, now).await
            }

            /// Connect to the server with a known peer ID.
            ///
            /// # Errors
            ///
            /// Returns [`ClientError`] if the handshake or connection setup fails.
            pub async fn $connect<Sig: Signer<$K>>(
                &self,
                signer: &Sig,
                expected_peer_id: PeerId,
                now: TimestampSeconds,
            ) -> Result<ConnectResult<$K, O>, ClientError> {
                let audience = Audience::known(expected_peer_id);
                self.$inner(signer, audience, now).await
            }

            async fn $inner<Sig: Signer<$K>>(
                &self,
                signer: &Sig,
                audience: Audience,
                now: TimestampSeconds,
            ) -> Result<ConnectResult<$K, O>, ClientError> {
                let nonce = Nonce::random();

                let mut client_handshake = ClientHttpHandshake::<$K, H> {
                    http: self.http.clone(),
                    base_url: self.base_url.clone(),
                    session_id: None,
                    response_bytes: None,
                    _k: core::marker::PhantomData,
                };

                let default_time_limit = self.default_time_limit;
                let timeout = self.timeout.clone();
                let base_url = self.base_url.clone();
                let http = self.http.clone();

                let (authenticated, session_id) = handshake::initiate::<$K, _, _, _, _>(
                    &mut client_handshake,
                    |handshake, peer_id| {
                        #[allow(clippy::expect_used)]
                        let session_id = handshake
                            .session_id
                            .expect("session_id set during handshake send");

                        let conn = HttpLongPollConnection::new(
                            peer_id,
                            default_time_limit,
                            timeout.clone(),
                        );

                        (conn, session_id)
                    },
                    signer,
                    audience,
                    now,
                    nonce,
                )
                .await
                .map_err(|e| ClientError::Authentication(e.to_string()))?;

                let conn = authenticated.inner().clone();

                let (cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);
                let send_cancel_rx = cancel_rx.clone();

                conn.set_cancel_guard(cancel_tx).await;

                let poll_url = format!("{base_url}/lp/recv");
                let poll_http = http.clone();
                let poll_conn = conn.clone();

                let poll_task = async move {
                    poll_loop(poll_http, poll_url, session_id, poll_conn, cancel_rx).await;
                }
                .$box_fn();

                let send_url = format!("{base_url}/lp/send");
                let send_http = http;
                let send_conn = conn;

                let send_task = async move {
                    send_loop(send_http, send_url, session_id, send_conn, send_cancel_rx).await;
                }
                .$box_fn();

                Ok(ConnectResult {
                    authenticated,
                    session_id,
                    poll_task,
                    send_task,
                })
            }
        }
    };
}

impl_connect!(
    Sendable,
    boxed,
    connect,
    connect_discover,
    connect_inner,
    Send,
    Sync
);
impl_connect!(
    Local,
    boxed_local,
    connect_local,
    connect_discover_local,
    connect_inner_local
);

// ---------------------------------------------------------------------------
// Background tasks (single generic implementation)
// ---------------------------------------------------------------------------

/// Background task that continuously polls `POST /lp/recv` and pushes
/// messages into the connection's inbound channel.
async fn poll_loop<K: future_form::FutureForm, H: HttpClient<K>, O>(
    http: H,
    url: String,
    session_id: SessionId,
    conn: HttpLongPollConnection<O>,
    cancel: async_channel::Receiver<()>,
) {
    loop {
        let recv_fut = http.post(
            &url,
            &[(SESSION_ID_HEADER, &session_id.to_hex())],
            Vec::new(),
        );

        let cancel_fut = cancel.recv();
        pin_mut!(recv_fut, cancel_fut);

        match select(recv_fut, cancel_fut).await {
            Either::Right(_) => {
                tracing::debug!("recv poll loop cancelled");
                break;
            }
            Either::Left((result, _)) => match result {
                Ok(resp) => match resp.status {
                    200 => match Message::try_decode(&resp.body) {
                        Ok(msg) => {
                            if conn.push_inbound(msg).await.is_err() {
                                tracing::error!("inbound channel closed");
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::error!("failed to decode recv message: {e}");
                        }
                    },
                    204 => {
                        // Poll timeout — immediately re-poll
                    }
                    410 => {
                        tracing::info!("session closed by server (410 Gone)");
                        break;
                    }
                    status => {
                        tracing::error!("unexpected recv status: {status}");
                        futures_timer::Delay::new(Duration::from_secs(1)).await;
                    }
                },
                Err(e) => {
                    tracing::error!("recv request error: {e}");
                    futures_timer::Delay::new(Duration::from_secs(1)).await;
                }
            },
        }
    }
}

/// Background task that drains the connection's outbound channel and sends
/// each message via `POST /lp/send`.
async fn send_loop<K: future_form::FutureForm, H: HttpClient<K>, O>(
    http: H,
    url: String,
    session_id: SessionId,
    conn: HttpLongPollConnection<O>,
    cancel: async_channel::Receiver<()>,
) {
    loop {
        let outbound_fut = conn.pull_outbound();
        let cancel_fut = cancel.recv();
        pin_mut!(outbound_fut, cancel_fut);

        match select(outbound_fut, cancel_fut).await {
            Either::Right(_) => {
                tracing::debug!("send loop cancelled");
                break;
            }
            Either::Left((result, _)) => {
                if let Ok(msg) = result {
                    let encoded = msg.encode();
                    match http
                        .post(
                            &url,
                            &[
                                (SESSION_ID_HEADER, &session_id.to_hex()),
                                ("content-type", "application/octet-stream"),
                            ],
                            encoded,
                        )
                        .await
                    {
                        Ok(resp) if resp.status < 300 => {}
                        Ok(resp) => {
                            tracing::error!("send returned status {}", resp.status);
                        }
                        Err(e) => {
                            tracing::error!("send request error: {e}");
                        }
                    }
                } else {
                    tracing::debug!("outbound channel closed");
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Handshake adapter (single generic implementation)
// ---------------------------------------------------------------------------

/// Client-side handshake adapter for HTTP.
///
/// In the `initiate` flow:
/// 1. `send(challenge_bytes)` — POSTs to `/lp/handshake`, captures response + session ID
/// 2. `recv()` — returns the response bytes captured during `send()`
///
/// This maps the streaming `Handshake` interface to a single HTTP round-trip.
struct ClientHttpHandshake<K: future_form::FutureForm, H: HttpClient<K>> {
    http: H,
    base_url: String,
    session_id: Option<SessionId>,
    response_bytes: Option<Vec<u8>>,
    _k: core::marker::PhantomData<K>,
}

#[future_form::future_form(Sendable where H: Send, Local)]
impl<K: future_form::FutureForm, H: HttpClient<K>> handshake::Handshake<K>
    for &mut ClientHttpHandshake<K, H>
{
    type Error = ClientError;

    fn send(&mut self, bytes: Vec<u8>) -> K::Future<'_, Result<(), Self::Error>> {
        let url = format!("{}/lp/handshake", self.base_url);
        let http = self.http.clone();

        K::from_future(async move {
            let resp = http
                .post(&url, &[("content-type", "application/octet-stream")], bytes)
                .await
                .map_err(|e| ClientError::Request(e.to_string()))?;

            if let Some(sid_str) = resp.header(SESSION_ID_HEADER) {
                self.session_id = SessionId::from_hex(sid_str);
            }

            if resp.status == 401 {
                return Err(ClientError::HandshakeRejected {
                    reason: match HandshakeMessage::try_decode(&resp.body) {
                        Ok(HandshakeMessage::Rejection(r)) => {
                            format!("{:?}", r.reason)
                        }
                        _ => String::from_utf8_lossy(&resp.body).into_owned(),
                    },
                });
            }

            if resp.status != 200 {
                return Err(ClientError::UnexpectedStatus {
                    status: resp.status,
                    body: String::from_utf8_lossy(&resp.body).into_owned(),
                });
            }

            self.response_bytes = Some(resp.body);
            Ok(())
        })
    }

    fn recv(&mut self) -> K::Future<'_, Result<Vec<u8>, Self::Error>> {
        let bytes = self.response_bytes.take();
        K::from_future(async move {
            bytes.ok_or_else(|| {
                ClientError::HandshakeDecode(
                    "no response bytes available (send not called?)".into(),
                )
            })
        })
    }
}
