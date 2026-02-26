//! Wasm-specific HTTP long-poll client using `web_sys::fetch`.
//!
//! This module provides:
//! - [`WasmLongPollTimeout`]: A [`Timeout<Local>`] implementation using browser `setTimeout`
//! - [`WasmHttpLongPollClient`]: Connects to a server, spawns background tasks via `spawn_local`

use alloc::{format, string::String, vec::Vec};
use core::time::Duration;

use future_form::Local;
use futures::{
    future::{select, Either, LocalBoxFuture},
    pin_mut, FutureExt,
};
use subduction_core::{
    connection::{
        authenticated::Authenticated,
        handshake::{self, Audience, HandshakeMessage},
        message::Message,
        timeout::{TimedOut, Timeout},
    },
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

use crate::{
    connection::HttpLongPollConnection,
    error::ClientError,
    http_client::{FetchHttpClient, HttpClient},
    session::SessionId,
    SESSION_ID_HEADER,
};

// ---------------------------------------------------------------------------
// WasmLongPollTimeout
// ---------------------------------------------------------------------------

/// A [`Timeout<Local>`] implementation using browser `setTimeout` / `clearTimeout`.
///
/// Works in both `Window` and `WorkerGlobalScope` contexts.
#[derive(Debug, Clone, Copy)]
pub struct WasmLongPollTimeout;

impl Timeout<Local> for WasmLongPollTimeout {
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: LocalBoxFuture<'a, T>,
    ) -> LocalBoxFuture<'a, Result<T, TimedOut>> {
        async move {
            let ms = dur.as_millis().try_into().unwrap_or(i32::MAX);
            let (sleep, handle) = wasm_timeout(ms);
            match select(fut, sleep).await {
                Either::Left((val, _)) => {
                    cancel_timeout(handle);
                    Ok(val)
                }
                Either::Right(_) => Err(TimedOut),
            }
        }
        .boxed_local()
    }
}

/// Browser timeout handle for cancellation.
struct TimeoutHandle {
    id: wasm_bindgen::JsValue,
}

/// Create a future that resolves after `ms` milliseconds, plus a handle to cancel it.
fn wasm_timeout(ms: i32) -> (LocalBoxFuture<'static, ()>, TimeoutHandle) {
    use js_sys::{Function, Promise, Reflect};
    use wasm_bindgen::JsValue;

    let mut out_id = JsValue::UNDEFINED;

    let promise = Promise::new(&mut |resolve, _reject| {
        let global = js_sys::global();

        let set_timeout = match Reflect::get(&global, &JsValue::from_str("setTimeout"))
            .and_then(JsCast::dyn_into::<Function>)
        {
            Ok(f) => f,
            Err(_) => {
                // No setTimeout available — resolve immediately
                drop(resolve.call0(&JsValue::NULL));
                return;
            }
        };

        let callback = resolve;
        out_id = set_timeout
            .call2(&global, callback.as_ref(), &JsValue::from(ms))
            .unwrap_or(JsValue::UNDEFINED);
    });

    let fut = async move {
        drop(JsFuture::from(promise).await);
    }
    .boxed_local();

    (fut, TimeoutHandle { id: out_id })
}

/// Cancel a pending browser timeout.
fn cancel_timeout(handle: TimeoutHandle) {
    use js_sys::{Function, Reflect};
    use wasm_bindgen::JsValue;

    let global = js_sys::global();
    if let Ok(clear_timeout) = Reflect::get(&global, &JsValue::from_str("clearTimeout"))
        .and_then(JsCast::dyn_into::<Function>)
    {
        drop(clear_timeout.call1(&global, &handle.id));
    }
}

// ---------------------------------------------------------------------------
// WasmHttpLongPollClient
// ---------------------------------------------------------------------------

/// An HTTP long-poll client for browser/worker environments.
///
/// Uses [`FetchHttpClient`] for HTTP and `wasm_bindgen_futures::spawn_local`
/// for background tasks.
#[derive(Debug, Clone)]
pub struct WasmHttpLongPollClient {
    base_url: String,
    http: FetchHttpClient,
    default_time_limit: Duration,
}

impl WasmHttpLongPollClient {
    /// Create a new wasm HTTP long-poll client.
    #[must_use]
    pub fn new(base_url: &str, default_time_limit: Duration) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http: FetchHttpClient::new(),
            default_time_limit,
        }
    }

    /// Connect to the server using discovery mode.
    pub async fn connect_discover<Sig>(
        &self,
        signer: &Sig,
        service_name: &str,
        cancel: async_channel::Receiver<()>,
    ) -> Result<
        (
            Authenticated<HttpLongPollConnection<WasmLongPollTimeout>, Local>,
            SessionId,
        ),
        ClientError,
    >
    where
        Sig: Signer<Local>,
    {
        let audience = Audience::discover(service_name.as_bytes());
        self.connect_inner(signer, audience, cancel).await
    }

    /// Connect to the server with a known peer ID.
    pub async fn connect<Sig>(
        &self,
        signer: &Sig,
        expected_peer_id: PeerId,
        cancel: async_channel::Receiver<()>,
    ) -> Result<
        (
            Authenticated<HttpLongPollConnection<WasmLongPollTimeout>, Local>,
            SessionId,
        ),
        ClientError,
    >
    where
        Sig: Signer<Local>,
    {
        let audience = Audience::known(expected_peer_id);
        self.connect_inner(signer, audience, cancel).await
    }

    async fn connect_inner<Sig>(
        &self,
        signer: &Sig,
        audience: Audience,
        cancel: async_channel::Receiver<()>,
    ) -> Result<
        (
            Authenticated<HttpLongPollConnection<WasmLongPollTimeout>, Local>,
            SessionId,
        ),
        ClientError,
    >
    where
        Sig: Signer<Local>,
    {
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let mut client_handshake = WasmClientHttpHandshake {
            http: self.http,
            base_url: self.base_url.clone(),
            session_id: None,
            response_bytes: None,
        };

        let default_time_limit = self.default_time_limit;
        let base_url = self.base_url.clone();
        let http = self.http;

        let (authenticated, session_id) = handshake::initiate::<Local, _, _, _, _>(
            &mut client_handshake,
            |handshake, peer_id| {
                #[allow(clippy::expect_used)]
                let session_id = handshake
                    .session_id
                    .expect("session_id set during handshake send");

                let conn =
                    HttpLongPollConnection::new(peer_id, default_time_limit, WasmLongPollTimeout);

                let (cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);
                let send_cancel_rx = cancel_rx.clone();

                // Store the cancel guard in the connection so background tasks
                // stay alive as long as the connection (and its clones) exist.
                // When `close()` is called (via disconnect), the guard is cleared
                // and the cancel channel closes, stopping poll/send tasks.
                let guard_conn = conn.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    guard_conn.set_cancel_guard(cancel_tx).await;
                });

                // Spawn the recv polling task
                let poll_conn = conn.clone();
                let poll_url = format!("{base_url}/lp/recv");
                let poll_http = http;

                wasm_bindgen_futures::spawn_local(async move {
                    poll_loop(poll_http, poll_url, session_id, poll_conn, cancel_rx).await;
                });

                // Spawn the send task
                let send_conn = conn.clone();
                let send_url = format!("{base_url}/lp/send");
                let send_http = http;

                wasm_bindgen_futures::spawn_local(async move {
                    send_loop(send_http, send_url, session_id, send_conn, send_cancel_rx).await;
                });

                // Bridge the external cancel signal (if any) to connection close
                let ext_cancel_conn = conn.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    // Blocks until the external cancel sender is dropped.
                    // If no external cancel is used, this task just parks forever.
                    cancel.recv().await.ok();
                    ext_cancel_conn.close();
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

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

async fn poll_loop(
    http: FetchHttpClient,
    url: String,
    session_id: SessionId,
    conn: HttpLongPollConnection<WasmLongPollTimeout>,
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
                tracing::debug!("wasm recv poll loop cancelled");
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
                        delay_ms(1000).await;
                    }
                },
                Err(e) => {
                    tracing::error!("recv request error: {e}");
                    delay_ms(1000).await;
                }
            },
        }
    }
}

async fn send_loop(
    http: FetchHttpClient,
    url: String,
    session_id: SessionId,
    conn: HttpLongPollConnection<WasmLongPollTimeout>,
    cancel: async_channel::Receiver<()>,
) {
    loop {
        let outbound_fut = conn.pull_outbound();
        let cancel_fut = cancel.recv();
        pin_mut!(outbound_fut, cancel_fut);

        match select(outbound_fut, cancel_fut).await {
            Either::Right(_) => {
                tracing::debug!("wasm send loop cancelled");
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

/// Simple delay using `setTimeout` (wasm-compatible replacement for `futures_timer::Delay`).
async fn delay_ms(ms: i32) {
    let (fut, _handle) = wasm_timeout(ms);
    fut.await;
}

// ---------------------------------------------------------------------------
// Handshake adapter
// ---------------------------------------------------------------------------

struct WasmClientHttpHandshake {
    http: FetchHttpClient,
    base_url: String,
    session_id: Option<SessionId>,
    response_bytes: Option<Vec<u8>>,
}

impl handshake::Handshake<Local> for &mut WasmClientHttpHandshake {
    type Error = ClientError;

    fn send(&mut self, bytes: Vec<u8>) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let url = format!("{}/lp/handshake", self.base_url);
        let http = self.http;

        async move {
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

            self.response_bytes = Some(resp.body);
            Ok(())
        }
        .boxed_local()
    }

    fn recv(&mut self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        let bytes = self.response_bytes.take();
        async move {
            bytes.ok_or_else(|| {
                ClientError::HandshakeDecode(
                    "no response bytes available (send not called?)".into(),
                )
            })
        }
        .boxed_local()
    }
}
