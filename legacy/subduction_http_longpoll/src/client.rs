//! Generic HTTP long-poll client.
//!
//! Connects to a Subduction server via HTTP long-poll, performing the
//! Ed25519 handshake and returning background task futures for send and recv.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │                  HttpLongPollClient                      │
//! │                                                          │
//! │  Connection::send(msg) ──► outbound_tx ──► sender_task   │
//! │                                    POST /lp/send ──────► │
//! │                                                          │
//! │  Connection::recv() ◄── inbound_reader ◄── poll_task     │
//! │                                    POST /lp/recv ◄────── │
//! │                                                          │
//! │  HttpLongPollTransport handles channel routing,          │
//! │  pending map, and request ID generation.                 │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! The client returns background futures rather than spawning them, leaving
//! the caller to decide how to run them (e.g., `tokio::spawn`, `spawn_local`).

use alloc::{format, string::String, vec::Vec};
use core::time::Duration;

use future_form::{FutureForm, Local, Sendable, future_form};
use futures::{
    future::{Either, select},
    pin_mut,
};
use subduction_core::{
    handshake::{self, HandshakeMessage, audience::Audience},
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};

use crate::{
    SESSION_ID_HEADER, error::ClientError, http_client::HttpClient, session::SessionId,
    transport::HttpLongPollTransport,
};

/// Result of a successful connection, containing the authenticated connection
/// and background task futures that the caller must spawn.
pub struct ConnectResult<Async: FutureForm> {
    /// The authenticated connection, ready for registration with Subduction.
    pub authenticated: subduction_core::authenticated::Authenticated<HttpLongPollTransport, Async>,

    /// The session ID assigned by the server.
    pub session_id: SessionId,

    /// Background recv-polling task. Must be spawned to drive inbound messages.
    pub poll_task: Async::Future<'static, ()>,

    /// Background send task. Must be spawned to drive outbound messages.
    pub send_task: Async::Future<'static, ()>,
}

impl<Async: FutureForm> core::fmt::Debug for ConnectResult<Async> {
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
#[derive(Debug, Clone)]
pub struct HttpLongPollClient<H> {
    base_url: String,
    http: H,
}

impl<H> HttpLongPollClient<H> {
    /// Create a new HTTP long-poll client.
    ///
    /// - `base_url`: The server's base URL, e.g., `http://localhost:8080`.
    /// - `http`: The HTTP client implementation.
    #[must_use]
    pub fn new(base_url: &str, http: H) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        }
    }
}

// ---------------------------------------------------------------------------
// Connect trait + future_form impl
// ---------------------------------------------------------------------------

/// Connect to a Subduction server via HTTP long-poll.
///
/// This trait is implemented for [`HttpLongPollClient`] via
/// [`#[future_form]`](future_form::future_form), generating concrete impls
/// for both [`Sendable`](future_form::Sendable) and [`Local`](future_form::Local).
///
/// Prefer the convenience methods [`HttpLongPollClient::connect`] and
/// [`HttpLongPollClient::connect_discover`] over calling this trait directly.
pub trait Connect<Async: FutureForm, Sign: Signer<Async>> {
    /// Connect with a specific [`Audience`] (known peer ID or service discovery).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the handshake or connection setup fails.
    #[allow(clippy::type_complexity)]
    fn connect_with_audience<'a>(
        &'a self,
        signer: &'a Sign,
        audience: Audience,
        now: TimestampSeconds,
    ) -> Async::Future<'a, Result<ConnectResult<Async>, ClientError>>;
}

#[future_form(Sendable where H: Send + Sync, Sign: Sync, H::Error: Send, Local)]
impl<Async: FutureForm, Sign: Signer<Async>, H: HttpClient<Async> + 'static> Connect<Async, Sign>
    for HttpLongPollClient<H>
{
    fn connect_with_audience<'a>(
        &'a self,
        signer: &'a Sign,
        audience: Audience,
        now: TimestampSeconds,
    ) -> Async::Future<'a, Result<ConnectResult<Async>, ClientError>> {
        let http = self.http.clone();
        let base_url = self.base_url.clone();

        Async::from_future(async move {
            let nonce = Nonce::random();

            let mut client_handshake = ClientHttpHandshake::<Async, H> {
                http: http.clone(),
                base_url: base_url.clone(),
                session_id: None,
                response_bytes: None,
                _k: core::marker::PhantomData,
            };

            #[allow(clippy::expect_used)]
            let (authenticated, session_id) = handshake::initiate::<Async, _, _, _, _>(
                &mut client_handshake,
                |handshake, peer_id| {
                    let session_id = handshake
                        .session_id
                        .expect("session_id set during handshake send");

                    let conn = HttpLongPollTransport::new(peer_id);

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

            let poll_task = Async::from_future(async move {
                poll_loop::<Async, H>(poll_http, poll_url, session_id, poll_conn, cancel_rx).await;
            });

            let send_url = format!("{base_url}/lp/send");
            let send_http = http;
            let send_conn = conn;

            let send_task = Async::from_future(async move {
                send_loop::<Async, H>(send_http, send_url, session_id, send_conn, send_cancel_rx)
                    .await;
            });

            Ok(ConnectResult {
                authenticated,
                session_id,
                poll_task,
                send_task,
            })
        })
    }
}

impl<H> HttpLongPollClient<H> {
    /// Connect to the server with a known peer ID.
    ///
    /// The [`FutureForm`] variant `Async` is inferred from the HTTP client `H`:
    /// [`Sendable`](future_form::Sendable) for native clients,
    /// [`Local`](future_form::Local) for browser `fetch()`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the handshake or connection setup fails.
    pub fn connect<'a, Async: FutureForm, Sign: Signer<Async>>(
        &'a self,
        signer: &'a Sign,
        expected_peer_id: PeerId,
        now: TimestampSeconds,
    ) -> Async::Future<'a, Result<ConnectResult<Async>, ClientError>>
    where
        Self: Connect<Async, Sign>,
    {
        Connect::<Async, Sign>::connect_with_audience(
            self,
            signer,
            Audience::known(expected_peer_id),
            now,
        )
    }

    /// Connect to the server using service discovery.
    ///
    /// The [`FutureForm`] variant `Async` is inferred from the HTTP client `H`:
    /// [`Sendable`](future_form::Sendable) for native clients,
    /// [`Local`](future_form::Local) for browser `fetch()`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the handshake or connection setup fails.
    pub fn connect_discover<'a, Async: FutureForm, Sign: Signer<Async>>(
        &'a self,
        signer: &'a Sign,
        service_name: &str,
        now: TimestampSeconds,
    ) -> Async::Future<'a, Result<ConnectResult<Async>, ClientError>>
    where
        Self: Connect<Async, Sign>,
    {
        Connect::<Async, Sign>::connect_with_audience(
            self,
            signer,
            Audience::discover(service_name.as_bytes()),
            now,
        )
    }
}

// ---------------------------------------------------------------------------
// Background tasks (single generic implementation)
// ---------------------------------------------------------------------------

/// Consecutive poll failures (transport errors or unexpected statuses)
/// tolerated before the poll loop concludes the peer is gone, closes the
/// connection, and exits.
///
/// Closing the transport surfaces a benign disconnect to
/// [`Subduction`](subduction_core::subduction::Subduction)'s listen loop —
/// which in turn cancels any in-flight roundtrip calls via
/// [`Multiplexer::cancel_all_pending`](subduction_core::multiplexer::Multiplexer::cancel_all_pending).
/// Without this, a long-poll client whose server has died (but whose
/// `POST /lp/recv` keeps erroring) would re-poll forever and a blocking
/// `sync_with_peer` would rely solely on its caller-side deadline. This
/// gives the transport its own eventual-disconnect guarantee, matching
/// WebSocket keepalive and Iroh/QUIC idle timeouts.
const MAX_CONSECUTIVE_POLL_FAILURES: u32 = 5;

/// Backoff between failed poll attempts.
const POLL_FAILURE_BACKOFF: Duration = Duration::from_secs(1);

/// Background task that continuously polls `POST /lp/recv` and pushes
/// raw bytes into the connection's inbound channel.
///
/// Exits (closing the transport) when cancelled, when the server reports
/// the session is gone (`410`), or after
/// [`MAX_CONSECUTIVE_POLL_FAILURES`] consecutive failures.
async fn poll_loop<Async: FutureForm, H: HttpClient<Async>>(
    http: H,
    url: String,
    session_id: SessionId,
    conn: HttpLongPollTransport,
    cancel: async_channel::Receiver<()>,
) {
    let mut consecutive_failures: u32 = 0;
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
                    200 => {
                        consecutive_failures = 0;
                        if conn.push_inbound(resp.body).await.is_err() {
                            tracing::error!("inbound channel closed");
                            conn.close();
                            break;
                        }
                    }
                    204 => {
                        // Poll timeout — a healthy idle cycle. Re-poll.
                        consecutive_failures = 0;
                    }
                    410 => {
                        tracing::info!("session closed by server (410 Gone)");
                        conn.close();
                        break;
                    }
                    status => {
                        tracing::error!(status = status, "unexpected recv status");
                        consecutive_failures += 1;
                        if consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES {
                            tracing::warn!(
                                count = consecutive_failures,
                                status = status,
                                "long-poll recv failed repeatedly; closing connection"
                            );
                            conn.close();
                            break;
                        }
                        futures_timer::Delay::new(POLL_FAILURE_BACKOFF).await;
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, "recv request error");
                    consecutive_failures += 1;
                    if consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES {
                        tracing::warn!(
                            count = consecutive_failures,
                            "long-poll recv errored repeatedly; closing connection"
                        );
                        conn.close();
                        break;
                    }
                    futures_timer::Delay::new(POLL_FAILURE_BACKOFF).await;
                }
            },
        }
    }
}

/// Background task that drains the connection's outbound channel and sends
/// each message via `POST /lp/send`.
async fn send_loop<Async: FutureForm, H: HttpClient<Async>>(
    http: H,
    url: String,
    session_id: SessionId,
    conn: HttpLongPollTransport,
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
                if let Ok(bytes) = result {
                    match http
                        .post(
                            &url,
                            &[
                                (SESSION_ID_HEADER, &session_id.to_hex()),
                                ("content-type", "application/octet-stream"),
                            ],
                            bytes,
                        )
                        .await
                    {
                        Ok(resp) if resp.status < 300 => {}
                        Ok(resp) => {
                            tracing::error!(status = resp.status, "send returned error status");
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "send request error");
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
struct ClientHttpHandshake<Async: FutureForm, H: HttpClient<Async>> {
    http: H,
    base_url: String,
    session_id: Option<SessionId>,
    response_bytes: Option<Vec<u8>>,
    _k: core::marker::PhantomData<Async>,
}

#[future_form(Sendable where H: Send, Local)]
impl<Async: FutureForm, H: HttpClient<Async>> handshake::Handshake<Async>
    for &mut ClientHttpHandshake<Async, H>
{
    type Error = ClientError;

    fn send(&mut self, bytes: Vec<u8>) -> Async::Future<'_, Result<(), Self::Error>> {
        let url = format!("{}/lp/handshake", self.base_url);
        let http = self.http.clone();

        Async::from_future(async move {
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

    fn recv(&mut self) -> Async::Future<'_, Result<Vec<u8>, Self::Error>> {
        let bytes = self.response_bytes.take();
        Async::from_future(async move {
            bytes.ok_or_else(|| {
                ClientError::HandshakeDecode(
                    "no response bytes available (send not called?)".into(),
                )
            })
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod poll_loop_tests {
    use core::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use future_form::Sendable;

    use super::*;
    use crate::{http_client::HttpResponse, session::SessionId, transport::HttpLongPollTransport};

    /// An `HttpClient` whose `POST /lp/recv` always fails, counting calls.
    #[derive(Clone)]
    struct AlwaysFailHttp {
        calls: Arc<AtomicU32>,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("mock http failure")]
    struct MockHttpError;

    impl HttpClient<Sendable> for AlwaysFailHttp {
        type Error = MockHttpError;

        fn post(
            &self,
            _url: &str,
            _headers: &[(&str, &str)],
            _body: Vec<u8>,
        ) -> <Sendable as FutureForm>::Future<'_, Result<HttpResponse, Self::Error>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(MockHttpError) })
        }
    }

    /// After `MAX_CONSECUTIVE_POLL_FAILURES` consecutive recv failures, the
    /// poll loop closes the transport and exits — giving the long-poll
    /// transport its own eventual-disconnect guarantee so a parked
    /// `recv_bytes` (and thus the Subduction listen loop) is notified.
    ///
    /// Runs in ~real time: `MAX_CONSECUTIVE_POLL_FAILURES - 1` backoffs of
    /// `POLL_FAILURE_BACKOFF` (the final failure closes without backing off).
    #[tokio::test]
    async fn poll_loop_closes_transport_after_sustained_failures() {
        let peer_id = PeerId::new([3u8; 32]);
        let conn = HttpLongPollTransport::new(peer_id);
        let calls = Arc::new(AtomicU32::new(0));
        let http = AlwaysFailHttp {
            calls: calls.clone(),
        };
        let (_cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);

        poll_loop::<Sendable, AlwaysFailHttp>(
            http,
            "http://example.invalid/lp/recv".into(),
            SessionId::random(),
            conn.clone(),
            cancel_rx,
        )
        .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            MAX_CONSECUTIVE_POLL_FAILURES,
            "loop must stop after exactly the failure threshold"
        );

        // The transport is closed: a recv now errors (channel closed) rather
        // than parking forever.
        let recv =
            <HttpLongPollTransport as subduction_core::transport::Transport<Sendable>>::recv_bytes(
                &conn,
            )
            .await;
        assert!(
            recv.is_err(),
            "transport must be closed after the poll loop gives up"
        );
    }
}
