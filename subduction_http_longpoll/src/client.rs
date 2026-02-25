//! Generic HTTP long-poll client.
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

use alloc::{format, string::String, vec::Vec};
use core::time::Duration;

use future_form::FutureForm;
use futures::{
    FutureExt,
    future::{Either, select},
};
use subduction_core::{
    connection::{
        authenticated::Authenticated,
        handshake::{self, Audience, HandshakeMessage},
        manager::Spawn,
        message::Message,
        timeout::Timeout,
    },
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};

use crate::{
    SESSION_ID_HEADER,
    connection::HttpLongPollConnection,
    error::ClientError,
    http_client::{HttpClient, HttpResponse},
    session::SessionId,
};

/// An HTTP long-poll client that connects to a Subduction server.
///
/// After connecting, use the returned [`HttpLongPollConnection`] which
/// implements [`Connection<K>`](subduction_core::connection::Connection).
///
/// # Type Parameters
///
/// - `K`: The future form (`Sendable` or `Local`)
/// - `H`: The HTTP client implementation
/// - `O`: The timeout strategy
/// - `S`: The task spawner
#[derive(Debug)]
pub struct HttpLongPollClient<H, O, S> {
    base_url: String,
    http: H,
    timeout: O,
    spawner: S,
    default_time_limit: Duration,
}

impl<H: Clone, O: Clone, S: Clone> Clone for HttpLongPollClient<H, O, S> {
    fn clone(&self) -> Self {
        Self {
            base_url: self.base_url.clone(),
            http: self.http.clone(),
            timeout: self.timeout.clone(),
            spawner: self.spawner.clone(),
            default_time_limit: self.default_time_limit,
        }
    }
}

impl<H, O, S> HttpLongPollClient<H, O, S> {
    /// Create a new HTTP long-poll client.
    ///
    /// - `base_url`: The server's base URL, e.g., `http://localhost:8080`.
    /// - `http`: The HTTP client implementation.
    /// - `timeout`: The timeout strategy.
    /// - `spawner`: The task spawner for background send/recv loops.
    /// - `default_time_limit`: Default timeout for call operations.
    #[must_use]
    pub fn new(
        base_url: &str,
        http: H,
        timeout: O,
        spawner: S,
        default_time_limit: Duration,
    ) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
            timeout,
            spawner,
            default_time_limit,
        }
    }
}

impl<K, H, O, S> HttpLongPollClient<H, O, S>
where
    K: FutureForm,
    H: HttpClient<K> + Send + Sync + 'static,
    O: Timeout<K> + Clone + Send + Sync + 'static,
    S: Spawn<K> + Clone + Send + Sync + 'static,
{
    /// Connect to the server using discovery mode.
    ///
    /// Performs the Ed25519 handshake, then spawns background tasks for
    /// sending and receiving messages.
    ///
    /// The `cancel` future resolves when the client should shut down.
    /// Typically this is an `async_channel::Receiver<()>`, a
    /// `tokio_util::sync::CancellationToken::cancelled()` future, or similar.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the handshake or connection setup fails.
    pub async fn connect_discover<Sig, Cancel>(
        &self,
        signer: &Sig,
        service_name: &str,
        cancel: Cancel,
    ) -> Result<(Authenticated<HttpLongPollConnection<O>, K>, SessionId), ClientError>
    where
        Sig: Signer<K>,
        Cancel: core::future::Future<Output = ()> + Send + Sync + 'static,
    {
        let audience = Audience::discover(service_name.as_bytes());
        self.connect_inner(signer, audience, cancel).await
    }

    /// Connect to the server with a known peer ID.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the handshake or connection setup fails.
    pub async fn connect<Sig, Cancel>(
        &self,
        signer: &Sig,
        expected_peer_id: PeerId,
        cancel: Cancel,
    ) -> Result<(Authenticated<HttpLongPollConnection<O>, K>, SessionId), ClientError>
    where
        Sig: Signer<K>,
        Cancel: core::future::Future<Output = ()> + Send + Sync + 'static,
    {
        let audience = Audience::known(expected_peer_id);
        self.connect_inner(signer, audience, cancel).await
    }

    async fn connect_inner<Sig, Cancel>(
        &self,
        signer: &Sig,
        audience: Audience,
        cancel: Cancel,
    ) -> Result<(Authenticated<HttpLongPollConnection<O>, K>, SessionId), ClientError>
    where
        Sig: Signer<K>,
        Cancel: core::future::Future<Output = ()> + Send + Sync + 'static,
    {
        let now = TimestampSeconds::now();
        let nonce = Nonce::random();

        let mut client_handshake = ClientHttpHandshake::<K, H> {
            http: self.http.clone(),
            base_url: self.base_url.clone(),
            session_id: None,
            response_bytes: None,
            _marker: core::marker::PhantomData,
        };

        let default_time_limit = self.default_time_limit;
        let timeout = self.timeout.clone();
        let base_url = self.base_url.clone();
        let http = self.http.clone();
        let spawner = self.spawner.clone();

        let (authenticated, session_id) = handshake::initiate::<K, _, _, _, _>(
            &mut client_handshake,
            |handshake, peer_id| {
                #[allow(clippy::expect_used)]
                let session_id = handshake
                    .session_id
                    .expect("session_id set during handshake send");

                let conn =
                    HttpLongPollConnection::new(peer_id, default_time_limit, timeout.clone());

                // Spawn the recv polling task
                let poll_conn = conn.clone();
                let poll_url = format!("{base_url}/lp/recv");
                let poll_http = http.clone();
                let poll_session = session_id;
                let (cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);

                // We split the cancel future: one half for poll, one for send.
                // Use a shared channel instead.
                let send_cancel_rx = cancel_rx.clone();

                spawner.spawn(K::from_future(async move {
                    poll_loop::<K, H>(poll_http, poll_url, poll_session, poll_conn, cancel_rx)
                        .await;
                }));

                // Spawn the send task
                let send_conn = conn.clone();
                let send_url = format!("{base_url}/lp/send");
                let send_http = http;

                spawner.spawn(K::from_future(async move {
                    send_loop::<K, H>(send_http, send_url, session_id, send_conn, send_cancel_rx)
                        .await;
                }));

                // Drive the external cancel signal into our internal channel
                spawner.spawn(K::from_future(async move {
                    cancel.await;
                    drop(cancel_tx);
                }));

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
async fn poll_loop<K: FutureForm, H: HttpClient<K>>(
    http: H,
    url: String,
    session_id: SessionId,
    conn: HttpLongPollConnection<impl Timeout<K> + Clone>,
    cancel: async_channel::Receiver<()>,
) {
    loop {
        let recv_fut = http.post(
            &url,
            &[(SESSION_ID_HEADER, &session_id.to_hex())],
            Vec::new(),
        );

        let cancel_fut = cancel.recv();

        match select(recv_fut.fuse(), cancel_fut.fuse()).await {
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
                        // Brief backoff on unexpected errors
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
async fn send_loop<K: FutureForm, H: HttpClient<K>>(
    http: H,
    url: String,
    session_id: SessionId,
    conn: HttpLongPollConnection<impl Timeout<K> + Clone>,
    cancel: async_channel::Receiver<()>,
) {
    loop {
        let outbound_fut = conn.pull_outbound();
        let cancel_fut = cancel.recv();

        match select(outbound_fut.fuse(), cancel_fut.fuse()).await {
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

/// Client-side handshake adapter for HTTP.
///
/// In the `initiate` flow:
/// 1. `send(challenge_bytes)` — POSTs to `/lp/handshake`, captures response + session ID
/// 2. `recv()` — returns the response bytes captured during `send()`
///
/// This maps the streaming `Handshake` interface to a single HTTP round-trip.
struct ClientHttpHandshake<K: FutureForm, H: HttpClient<K>> {
    http: H,
    base_url: String,
    session_id: Option<SessionId>,
    response_bytes: Option<Vec<u8>>,
    _marker: core::marker::PhantomData<fn() -> K>,
}

impl<K: FutureForm, H: HttpClient<K>> subduction_core::connection::handshake::Handshake<K>
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

            // Extract session ID from response header
            if let Some(sid_str) = resp.header(SESSION_ID_HEADER) {
                self.session_id = SessionId::from_hex(sid_str);
            }

            if resp.status == 401 {
                return Err(ClientError::HandshakeRejected {
                    reason: match HandshakeMessage::try_decode(&resp.body) {
                        Ok(HandshakeMessage::Rejection(r)) => format!("{:?}", r.reason),
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

            // Stash the response bytes for the subsequent recv() call
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
