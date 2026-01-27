//! HTTP long-polling client implementation.
//!
//! Provides [`HttpConnection`] which implements the [`Connection`] trait
//! using HTTP long-polling for server-to-client message delivery.

mod poll_loop;

use alloc::sync::Arc;
use core::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

use async_lock::Mutex;
use futures::{
    FutureExt,
    channel::oneshot,
    future::BoxFuture,
};
use future_form::Sendable;
use reqwest::Client;
use sedimentree_core::collections::Map;
use subduction_core::{
    connection::{
        Connection,
        handshake::{self, Audience, Challenge, Nonce, Rejection, Response},
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
    },
    crypto::{signed::Signed, signer::Signer},
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::{
    DEFAULT_CALL_TIMEOUT_SECS, DEFAULT_POLL_TIMEOUT_SECS, SESSION_ID_HEADER,
    error::{CallError, DisconnectionError, HttpHandshakeError, RecvError, SendError},
    session::SessionId,
};

use poll_loop::poll_loop;

/// HTTP message wrapper for handshake.
#[derive(Debug, minicbor::Encode, minicbor::Decode)]
enum HandshakeMessage {
    /// A signed challenge from the client.
    #[n(0)]
    SignedChallenge(#[n(0)] Signed<Challenge>),

    /// A signed response from the server.
    #[n(1)]
    SignedResponse(#[n(0)] Signed<Response>),

    /// An unsigned rejection from the server.
    #[n(2)]
    Rejection(#[n(0)] Rejection),
}

/// HTTP long-polling connection to a server.
///
/// Implements [`Connection<Sendable>`] for use with Subduction.
#[derive(Debug)]
pub struct HttpConnection {
    peer_id: PeerId,
    session_id: SessionId,
    base_url: Url,
    client: Client,
    req_id_counter: Arc<AtomicU64>,
    pending: Arc<Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>>,
    inbound_reader: async_channel::Receiver<Message>,
    poll_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    closed: Arc<AtomicBool>,
    call_timeout: Duration,
}

impl HttpConnection {
    /// Connect to a server and perform the handshake.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection or handshake fails.
    pub async fn connect<S: Signer<Sendable>>(
        base_url: Url,
        signer: &S,
        audience: Audience,
    ) -> Result<Self, HttpHandshakeError> {
        Self::connect_with_options(base_url, signer, audience, ConnectOptions::default()).await
    }

    /// Connect with custom options.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection or handshake fails.
    ///
    /// # Panics
    ///
    /// Panics if CBOR encoding fails (should never happen with well-formed types).
    pub async fn connect_with_options<S: Signer<Sendable>>(
        base_url: Url,
        signer: &S,
        audience: Audience,
        options: ConnectOptions,
    ) -> Result<Self, HttpHandshakeError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(options.request_timeout_secs))
            .build()
            .map_err(HttpHandshakeError::Http)?;

        let handshake_url = base_url
            .join("/handshake")
            .map_err(|e| HttpHandshakeError::InvalidUrl(e.to_string()))?;

        let now = TimestampSeconds::now();
        let nonce = Nonce::random();
        let challenge = Challenge::new(audience, now, nonce);
        let signed_challenge = Signed::seal(signer, challenge).await.into_signed();
        let msg = HandshakeMessage::SignedChallenge(signed_challenge);

        #[allow(clippy::expect_used)]
        let body = minicbor::to_vec(&msg).expect("encoding should not fail");

        debug!(url = %handshake_url, "sending handshake request");

        let response = client
            .post(handshake_url)
            .header("Content-Type", "application/cbor")
            .body(body)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(HttpHandshakeError::ServerError {
                status: response.status().as_u16(),
                message: response.text().await.unwrap_or_default(),
            });
        }

        let session_id_str = response
            .headers()
            .get(SESSION_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .ok_or(HttpHandshakeError::MissingSessionId)?;

        let session_id =
            SessionId::from_base58(session_id_str).ok_or(HttpHandshakeError::InvalidSessionId)?;

        let response_bytes = response.bytes().await?;
        let handshake_msg: HandshakeMessage = minicbor::decode(&response_bytes)?;

        let peer_id = match handshake_msg {
            HandshakeMessage::SignedResponse(signed_response) => {
                let verified = handshake::verify_response(&signed_response, &challenge)?;
                info!(server_id = %verified.server_id, "handshake completed");
                verified.server_id
            }
            HandshakeMessage::Rejection(rejection) => {
                warn!(reason = ?rejection.reason, "handshake rejected");
                return Err(HttpHandshakeError::Rejected(rejection.reason));
            }
            HandshakeMessage::SignedChallenge(_) => {
                return Err(HttpHandshakeError::Handshake(
                    handshake::HandshakeError::InvalidSignature,
                ));
            }
        };

        let (inbound_writer, inbound_reader) = async_channel::bounded(128);
        let pending = Arc::new(Mutex::new(Map::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let closed = Arc::new(AtomicBool::new(false));

        let poll_url = base_url
            .join("/poll")
            .map_err(|e| HttpHandshakeError::InvalidUrl(e.to_string()))?;

        let poll_handle = tokio::spawn(poll_loop(
            client.clone(),
            poll_url,
            session_id,
            inbound_writer,
            pending.clone(),
            closed.clone(),
            Duration::from_secs(options.poll_timeout_secs),
        ));

        let starting_counter = {
            let mut bytes = [0u8; 8];
            #[allow(clippy::expect_used)]
            getrandom::getrandom(&mut bytes).expect("failed to generate random bytes");
            u64::from_le_bytes(bytes)
        };

        Ok(Self {
            peer_id,
            session_id,
            base_url,
            client,
            req_id_counter: Arc::new(AtomicU64::new(starting_counter)),
            pending,
            inbound_reader,
            poll_handle: Arc::new(Mutex::new(Some(poll_handle))),
            closed,
            call_timeout: Duration::from_secs(options.call_timeout_secs),
        })
    }

    /// Get the session ID for this connection.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Get the base URL of the server.
    #[must_use]
    pub const fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// Check if the connection has been closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn build_url(&self, path: &str) -> Result<Url, SendError> {
        self.base_url
            .join(path)
            .map_err(|_| SendError::ConnectionClosed)
    }
}

impl Connection<Sendable> for HttpConnection {
    type DisconnectionError = DisconnectionError;
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async move {
            info!(peer_id = %self.peer_id, "disconnecting");
            self.closed.store(true, Ordering::SeqCst);

            if let Some(handle) = self.poll_handle.lock().await.take() {
                handle.abort();
            }

            Ok(())
        }
        .boxed()
    }

    fn send(&self, message: &Message) -> BoxFuture<'_, Result<(), Self::SendError>> {
        let request_id = message.request_id();
        #[allow(clippy::expect_used)]
        let msg_bytes = minicbor::to_vec(message).expect("serialization should be infallible");

        async move {
            if self.is_closed() {
                return Err(SendError::ConnectionClosed);
            }

            let url = self.build_url("/send")?;

            debug!(
                message_id = ?request_id,
                peer_id = %self.peer_id,
                "sending message"
            );

            let response = self
                .client
                .post(url)
                .header("Content-Type", "application/cbor")
                .header(SESSION_ID_HEADER, self.session_id.to_base58())
                .body(msg_bytes)
                .send()
                .await
                .map_err(SendError::Http)?;

            if response.status().as_u16() == 401 || response.status().as_u16() == 404 {
                return Err(SendError::SessionExpired);
            }

            if !response.status().is_success() {
                error!(status = %response.status(), "send failed");
                return Err(SendError::ConnectionClosed);
            }

            Ok(())
        }
        .boxed()
    }

    fn recv(&self) -> BoxFuture<'_, Result<Message, Self::RecvError>> {
        let chan = self.inbound_reader.clone();

        async move {
            chan.recv().await.map_err(|_| {
                error!("inbound channel closed");
                RecvError::ChannelClosed
            })
        }
        .boxed()
    }

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        async move {
            let nonce = self.req_id_counter.fetch_add(1, Ordering::Relaxed);
            RequestId {
                requestor: self.peer_id,
                nonce,
            }
        }
        .boxed()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout_override: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async move {
            if self.is_closed() {
                return Err(CallError::ConnectionClosed);
            }

            let req_id = req.req_id;
            debug!(req_id = ?req_id, "making call");

            let url = self
                .base_url
                .join("/call")
                .map_err(|_| CallError::ConnectionClosed)?;

            #[allow(clippy::expect_used)]
            let body =
                minicbor::to_vec(Message::BatchSyncRequest(req)).expect("encoding infallible");

            let timeout = timeout_override.unwrap_or(self.call_timeout);

            let response = tokio::time::timeout(timeout, async {
                self.client
                    .post(url)
                    .header("Content-Type", "application/cbor")
                    .header(SESSION_ID_HEADER, self.session_id.to_base58())
                    .body(body)
                    .send()
                    .await
            })
            .await
            .map_err(|_| CallError::Timeout)?
            .map_err(CallError::Http)?;

            if response.status().as_u16() == 401 || response.status().as_u16() == 404 {
                return Err(CallError::SessionExpired);
            }

            if !response.status().is_success() {
                error!(status = %response.status(), "call failed");
                return Err(CallError::ConnectionClosed);
            }

            let bytes = response.bytes().await.map_err(CallError::Http)?;
            let msg: Message = minicbor::decode(&bytes)?;

            if let Message::BatchSyncResponse(resp) = msg {
                info!(req_id = ?req_id, "call completed");
                Ok(resp)
            } else {
                error!("unexpected response type");
                Err(CallError::ConnectionClosed)
            }
        }
        .boxed()
    }
}

impl Clone for HttpConnection {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id,
            session_id: self.session_id,
            base_url: self.base_url.clone(),
            client: self.client.clone(),
            req_id_counter: self.req_id_counter.clone(),
            pending: self.pending.clone(),
            inbound_reader: self.inbound_reader.clone(),
            poll_handle: self.poll_handle.clone(),
            closed: self.closed.clone(),
            call_timeout: self.call_timeout,
        }
    }
}

impl PartialEq for HttpConnection {
    fn eq(&self, other: &Self) -> bool {
        self.session_id == other.session_id && self.peer_id == other.peer_id
    }
}

/// Options for connecting to an HTTP server.
#[derive(Debug, Clone, Copy)]
pub struct ConnectOptions {
    /// Timeout for individual HTTP requests (seconds).
    pub request_timeout_secs: u64,

    /// Timeout for long-poll requests (seconds).
    pub poll_timeout_secs: u64,

    /// Timeout for `call()` operations (seconds).
    pub call_timeout_secs: u64,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            request_timeout_secs: 60,
            poll_timeout_secs: DEFAULT_POLL_TIMEOUT_SECS,
            call_timeout_secs: DEFAULT_CALL_TIMEOUT_SECS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_options_default() {
        let opts = ConnectOptions::default();
        assert_eq!(opts.poll_timeout_secs, DEFAULT_POLL_TIMEOUT_SECS);
        assert_eq!(opts.call_timeout_secs, DEFAULT_CALL_TIMEOUT_SECS);
    }
}
