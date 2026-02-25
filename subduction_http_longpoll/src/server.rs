//! Hyper-based HTTP server for long-poll transport.
//!
//! Handles incoming HTTP requests and routes them to the appropriate handler:
//!
//! | Endpoint            | Method | Purpose                               |
//! |---------------------|--------|---------------------------------------|
//! | `/lp/handshake`     | POST   | Ed25519 mutual authentication         |
//! | `/lp/send`          | POST   | Client sends a message to the server  |
//! | `/lp/recv`          | POST   | Client long-polls for the next message|
//! | `/lp/disconnect`    | POST   | Clean session teardown                |
//!
//! The server is designed to run alongside a WebSocket server on the same
//! TCP listener by dispatching based on request path.

use alloc::{string::ToString, sync::Arc, vec::Vec};
use core::time::Duration;

use async_lock::Mutex;
use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    Request, Response, StatusCode,
};
use subduction_core::{
    connection::{
        authenticated::Authenticated,
        handshake::{self, Audience},
        nonce_cache::NonceCache,
        timeout::Timeout,
    },
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use subduction_crypto::signer::Signer;

use future_form::{FutureForm, Sendable};
use futures::{future::BoxFuture, FutureExt};

use crate::{
    connection::HttpLongPollConnection,
    error::ServerError,
    session::{SessionEntry, SessionId, SessionStore},
    DEFAULT_MAX_BODY_SIZE, DEFAULT_POLL_TIMEOUT_SECS, SESSION_ID_HEADER,
};

/// Server-side handler state, shared across request handlers.
#[derive(Debug, Clone)]
pub struct LongPollHandler<Sig, O: Timeout<Sendable> + Send + Sync> {
    sessions: SessionStore<O>,
    signer: Sig,
    nonce_cache: Arc<NonceCache>,
    our_peer_id: PeerId,
    discovery_audience: Option<Audience>,
    handshake_max_drift: Duration,
    default_time_limit: Duration,
    timeout: O,
    max_body_size: usize,
    poll_timeout: Duration,
}

impl<Sig: Signer<Sendable> + Clone + Send + Sync, O: Timeout<Sendable> + Clone + Send + Sync>
    LongPollHandler<Sig, O>
{
    /// Create a new long-poll handler.
    #[must_use]
    pub fn new(
        signer: Sig,
        nonce_cache: Arc<NonceCache>,
        our_peer_id: PeerId,
        discovery_audience: Option<Audience>,
        handshake_max_drift: Duration,
        default_time_limit: Duration,
        timeout: O,
    ) -> Self {
        Self {
            sessions: SessionStore::new(),
            signer,
            nonce_cache,
            our_peer_id,
            discovery_audience,
            handshake_max_drift,
            default_time_limit,
            timeout,
            max_body_size: DEFAULT_MAX_BODY_SIZE,
            poll_timeout: Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS),
        }
    }

    /// Set the maximum request body size.
    #[must_use]
    pub const fn with_max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = size;
        self
    }

    /// Set the long-poll timeout.
    #[must_use]
    pub const fn with_poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Access the session store.
    #[must_use]
    pub const fn sessions(&self) -> &SessionStore<O> {
        &self.sessions
    }

    /// Route an incoming HTTP request to the appropriate handler.
    ///
    /// Returns an HTTP response. Errors are mapped to appropriate status codes.
    ///
    /// # Errors
    ///
    /// Returns `hyper::Error` if the underlying HTTP transport fails.
    ///
    /// # Panics
    ///
    /// Panics if `Response::builder()` fails, which cannot happen with
    /// the static headers used here.
    #[allow(clippy::expect_used)]
    pub async fn handle(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let path = req.uri().path();
        let method = req.method().clone();

        tracing::debug!("HTTP long-poll: {method} {path}");

        let response = match (method.as_str(), path) {
            ("POST", "/lp/handshake") => self.handle_handshake(req).await,
            ("POST", "/lp/send") => self.handle_send(req).await,
            ("POST", "/lp/recv") => self.handle_recv(req).await,
            ("POST", "/lp/disconnect") => self.handle_disconnect(req).await,
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"not found")))
                .expect("static response")),
        };

        match response {
            Ok(resp) => Ok(resp),
            Err(e) => {
                tracing::error!("handler error: {e}");
                Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &e))
            }
        }
    }

    /// Handle `POST /lp/handshake`.
    ///
    /// The client sends `Signed<Challenge>` bytes in the request body.
    /// The server responds with `Signed<Response>` bytes and a session ID header.
    #[allow(clippy::expect_used)]
    async fn handle_handshake(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, ServerError> {
        let body = read_body(req, self.max_body_size).await?;

        let response_slot: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));

        let http_handshake = HttpHandshake {
            challenge_bytes: Some(body),
            response_slot: response_slot.clone(),
        };

        let now = TimestampSeconds::now();
        let default_time_limit = self.default_time_limit;
        let timeout = self.timeout.clone();

        let result = handshake::respond::<Sendable, _, _, _, _>(
            http_handshake,
            |_handshake, peer_id| {
                let conn =
                    HttpLongPollConnection::new(peer_id, default_time_limit, timeout.clone());
                (conn.clone(), conn)
            },
            &self.signer,
            &self.nonce_cache,
            self.our_peer_id,
            self.discovery_audience,
            now,
            self.handshake_max_drift,
        )
        .await;

        let response_bytes = response_slot.lock().await.take();

        match result {
            Ok((authenticated, conn)) => {
                let peer_id = authenticated.peer_id();
                let session_id = SessionId::random();

                tracing::info!(
                    "HTTP long-poll handshake complete: peer {peer_id}, session {session_id}"
                );

                self.sessions
                    .insert(
                        session_id,
                        SessionEntry {
                            peer_id,
                            connection: conn.clone(),
                            authenticated: Some(authenticated),
                        },
                    )
                    .await;

                let response_bytes = response_bytes
                    .ok_or_else(|| ServerError::Handshake("no response generated".into()))?;

                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(SESSION_ID_HEADER, session_id.to_hex())
                    .header("content-type", "application/octet-stream")
                    .body(Full::new(Bytes::from(response_bytes)))
                    .expect("valid response"))
            }
            Err(e) => {
                tracing::warn!("handshake failed: {e}");

                if let Some(response_bytes) = response_bytes {
                    Ok(Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .header("content-type", "application/octet-stream")
                        .body(Full::new(Bytes::from(response_bytes)))
                        .expect("valid response"))
                } else {
                    Ok(Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(Full::new(Bytes::from(e.to_string())))
                        .expect("valid response"))
                }
            }
        }
    }

    /// Handle `POST /lp/send`.
    ///
    /// The client sends a binary-encoded `Message` in the body.
    #[allow(clippy::expect_used)]
    async fn handle_send(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, ServerError> {
        let session_id = extract_session_id(&req)?;
        let entry = self
            .sessions
            .get(&session_id)
            .await
            .ok_or(ServerError::SessionNotFound)?;

        let body = read_body(req, self.max_body_size).await?;

        let msg = subduction_core::connection::message::Message::try_decode(&body)
            .map_err(ServerError::MessageDecode)?;

        tracing::debug!(
            "POST /lp/send: peer {} message {:?}",
            entry.peer_id,
            msg.request_id()
        );

        entry
            .connection
            .push_inbound(msg)
            .await
            .map_err(|e| ServerError::ChanSend(Box::new(e)))?;

        Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .expect("valid response"))
    }

    /// Handle `POST /lp/recv`.
    ///
    /// Long-polls until an outbound message is available or the poll timeout expires.
    #[allow(clippy::expect_used)]
    async fn handle_recv(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, ServerError> {
        let session_id = extract_session_id(&req)?;
        let entry = self
            .sessions
            .get(&session_id)
            .await
            .ok_or(ServerError::SessionNotFound)?;

        tracing::debug!("POST /lp/recv: peer {} waiting...", entry.peer_id);

        let pull_fut = Sendable::from_future(async move { entry.connection.pull_outbound().await });

        match self.timeout.timeout(self.poll_timeout, pull_fut).await {
            Ok(Ok(msg)) => {
                let encoded = msg.encode();
                tracing::debug!("POST /lp/recv: delivering message {:?}", msg.request_id(),);
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/octet-stream")
                    .body(Full::new(Bytes::from(encoded)))
                    .expect("valid response"))
            }
            Ok(Err(_)) => {
                tracing::debug!("POST /lp/recv: channel closed");
                Ok(Response::builder()
                    .status(StatusCode::GONE)
                    .body(Full::new(Bytes::from_static(b"session closed")))
                    .expect("valid response"))
            }
            Err(_timed_out) => {
                tracing::debug!("POST /lp/recv: poll timeout");
                Ok(Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Full::new(Bytes::new()))
                    .expect("valid response"))
            }
        }
    }

    /// Handle `POST /lp/disconnect`.
    #[allow(clippy::expect_used)]
    async fn handle_disconnect(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, ServerError> {
        let session_id = extract_session_id(&req)?;

        if let Some(entry) = self.sessions.remove(&session_id).await {
            tracing::info!(
                "POST /lp/disconnect: peer {} session {session_id}",
                entry.peer_id
            );
            entry.connection.close();
        }

        Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .expect("valid response"))
    }

    /// Take the authenticated connection for a session (for Subduction registration).
    ///
    /// This removes the `Authenticated` wrapper from the session entry.
    /// The caller is responsible for registering it with `Subduction`.
    pub async fn take_authenticated(
        &self,
        session_id: &SessionId,
    ) -> Option<Authenticated<HttpLongPollConnection<O>, Sendable>> {
        let mut sessions = self.sessions.sessions.lock().await;
        sessions
            .get_mut(session_id)
            .and_then(|entry| entry.authenticated.take())
    }
}

/// In-memory handshake transport for HTTP request-response.
///
/// Satisfies the `Handshake<Sendable>` trait by:
/// 1. Yielding pre-loaded challenge bytes on `recv()`
/// 2. Storing response bytes in a shared slot on `send()`
///
/// After `respond()` completes, the caller reads the response bytes
/// from the shared `response_slot`.
struct HttpHandshake {
    challenge_bytes: Option<Vec<u8>>,
    response_slot: Arc<Mutex<Option<Vec<u8>>>>,
}

impl subduction_core::connection::handshake::Handshake<Sendable> for HttpHandshake {
    type Error = ServerError;

    fn send(&mut self, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), Self::Error>> {
        let slot = self.response_slot.clone();
        async move {
            *slot.lock().await = Some(bytes);
            Ok(())
        }
        .boxed()
    }

    fn recv(&mut self) -> BoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        let bytes = self.challenge_bytes.take();
        async move {
            bytes.ok_or_else(|| ServerError::Handshake("no challenge bytes available".into()))
        }
        .boxed()
    }
}

/// Extract the session ID from the `X-Session-Id` header.
fn extract_session_id<T>(req: &Request<T>) -> Result<SessionId, ServerError> {
    let header_value = req
        .headers()
        .get(SESSION_ID_HEADER)
        .ok_or(ServerError::InvalidSessionId)?;

    let header_str = header_value
        .to_str()
        .map_err(|_| ServerError::InvalidSessionId)?;

    SessionId::from_hex(header_str).ok_or(ServerError::InvalidSessionId)
}

/// Read the request body up to `max_size` bytes.
async fn read_body(req: Request<Incoming>, max_size: usize) -> Result<Vec<u8>, ServerError> {
    let body = req.into_body();
    let collected = body
        .collect()
        .await
        .map_err(|e| ServerError::BodyRead(e.to_string()))?;

    let bytes = collected.to_bytes();
    if bytes.len() > max_size {
        return Err(ServerError::BodyTooLarge);
    }

    Ok(bytes.to_vec())
}

/// Build a simple error response.
#[allow(clippy::expect_used)]
fn error_response(status: StatusCode, err: &ServerError) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(err.to_string())))
        .expect("valid error response")
}
