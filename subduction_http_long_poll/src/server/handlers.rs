//! HTTP request handlers for long-polling server.

use alloc::{sync::Arc, vec::Vec};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use future_form::Sendable;
use subduction_core::{
    connection::{
        handshake::{
            self, Audience, Challenge, HandshakeError, Rejection, RejectionReason, Response,
            MAX_PLAUSIBLE_DRIFT,
        },
        message::Message,
    },
    crypto::{signed::Signed, signer::Signer},
    timestamp::TimestampSeconds,
};
use tracing::{debug, info, warn};

use super::state::HttpServerState;
use crate::{SESSION_ID_HEADER, session::SessionId};

/// HTTP message wrapper for handshake.
#[derive(Debug, minicbor::Encode, minicbor::Decode)]
enum HandshakeMessage {
    #[n(0)]
    SignedChallenge(#[n(0)] Signed<Challenge>),

    #[n(1)]
    SignedResponse(#[n(0)] Signed<Response>),

    #[n(2)]
    Rejection(#[n(0)] Rejection),
}

/// Create the Axum router for HTTP long-polling.
pub fn router<S>(state: Arc<HttpServerState<S>>) -> Router
where
    S: Signer<Sendable> + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/handshake", post(handle_handshake::<S>))
        .route("/send", post(handle_send::<S>))
        .route("/poll", get(handle_poll::<S>))
        .route("/call", post(handle_call::<S>))
        .with_state(state)
}

/// Handle handshake requests.
async fn handle_handshake<S>(
    State(state): State<Arc<HttpServerState<S>>>,
    body: Bytes,
) -> impl IntoResponse
where
    S: Signer<Sendable> + Clone + Send + Sync + 'static,
{
    let now = TimestampSeconds::now();

    let msg: HandshakeMessage = match minicbor::decode(&body) {
        Ok(m) => m,
        Err(e) => {
            warn!(error = %e, "failed to decode handshake message");
            return (StatusCode::BAD_REQUEST, HeaderMap::new(), Vec::new());
        }
    };

    let HandshakeMessage::SignedChallenge(signed_challenge) = msg else {
        warn!("expected SignedChallenge");
        return (StatusCode::BAD_REQUEST, HeaderMap::new(), Vec::new());
    };

    let known_audience = Audience::known(state.server_peer_id);
    let discovery_audience = Audience::discover(state.server_peer_id.as_bytes());

    let verified = match handshake::verify_challenge(&signed_challenge, &known_audience, now, MAX_PLAUSIBLE_DRIFT) {
        Ok(v) => v,
        Err(HandshakeError::ChallengeValidation(handshake::ChallengeValidationError::InvalidAudience)) => {
            match handshake::verify_challenge(&signed_challenge, &discovery_audience, now, MAX_PLAUSIBLE_DRIFT) {
                Ok(v) => v,
                Err(e) => {
                    let rejection = create_rejection(&e, now);
                    return send_rejection(rejection);
                }
            }
        }
        Err(e) => {
            let rejection = create_rejection(&e, now);
            return send_rejection(rejection);
        }
    };

    if state
        .nonce_cache
        .try_claim(verified.client_id, verified.challenge.nonce, now)
        .await
        .is_err()
    {
        let rejection = Rejection::new(RejectionReason::ReplayedNonce, now);
        return send_rejection(rejection);
    }

    let signed_response = handshake::create_response(&state.signer, &verified.challenge, now).await;
    let session_id = state.create_session(verified.client_id).await;

    let response_msg = HandshakeMessage::SignedResponse(signed_response);

    #[allow(clippy::expect_used)]
    let response_bytes = minicbor::to_vec(&response_msg).expect("encoding should not fail");

    let mut headers = HeaderMap::new();
    #[allow(clippy::expect_used)]
    headers.insert(
        SESSION_ID_HEADER,
        session_id.to_base58().parse().expect("valid header value"),
    );

    info!(session_id = %session_id, client_id = %verified.client_id, "handshake completed");

    (StatusCode::OK, headers, response_bytes)
}

const fn create_rejection(e: &HandshakeError, now: TimestampSeconds) -> Rejection {
    let reason = match e {
        HandshakeError::ChallengeValidation(cv) => cv.to_rejection_reason(),
        HandshakeError::InvalidSignature | HandshakeError::ResponseValidation(_) => {
            RejectionReason::InvalidSignature
        }
    };
    Rejection::new(reason, now)
}

fn send_rejection(rejection: Rejection) -> (StatusCode, HeaderMap, Vec<u8>) {
    let msg = HandshakeMessage::Rejection(rejection);
    #[allow(clippy::expect_used)]
    let bytes = minicbor::to_vec(&msg).expect("encoding should not fail");
    (StatusCode::UNAUTHORIZED, HeaderMap::new(), bytes)
}

/// Handle send requests (client → server messages).
async fn handle_send<S>(
    State(state): State<Arc<HttpServerState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse
where
    S: Signer<Sendable> + Clone + Send + Sync + 'static,
{
    let Some(session_id) = extract_session_id(&headers) else {
        return StatusCode::UNAUTHORIZED;
    };

    if state.get_session(session_id).await.is_none() {
        return StatusCode::NOT_FOUND;
    }

    state.touch_session(session_id).await;

    let Ok(msg) = minicbor::decode::<Message>(&body) else {
        warn!("failed to decode message");
        return StatusCode::BAD_REQUEST;
    };

    debug!(session_id = %session_id, message = ?msg.variant_name(), "received message");

    StatusCode::OK
}

/// Handle poll requests (long-polling for server → client messages).
async fn handle_poll<S>(
    State(state): State<Arc<HttpServerState<S>>>,
    headers: HeaderMap,
) -> impl IntoResponse
where
    S: Signer<Sendable> + Clone + Send + Sync + 'static,
{
    let Some(session_id) = extract_session_id(&headers) else {
        return (StatusCode::UNAUTHORIZED, Vec::new());
    };

    let Some((messages, rx)) = state.take_or_wait_messages(session_id).await else {
        return (StatusCode::NOT_FOUND, Vec::new());
    };

    if !messages.is_empty() {
        #[allow(clippy::expect_used)]
        let bytes = minicbor::to_vec(&messages).expect("encoding should not fail");
        return (StatusCode::OK, bytes);
    }

    let poll_timeout = state.poll_timeout();
    let messages = match tokio::time::timeout(poll_timeout, rx).await {
        Ok(Ok(msgs)) => msgs,
        Ok(Err(_)) | Err(_) => Vec::new(),
    };

    if messages.is_empty() {
        return (StatusCode::NO_CONTENT, Vec::new());
    }

    #[allow(clippy::expect_used)]
    let bytes = minicbor::to_vec(&messages).expect("encoding should not fail");
    (StatusCode::OK, bytes)
}

/// Handle call requests (synchronous request/response).
async fn handle_call<S>(
    State(state): State<Arc<HttpServerState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse
where
    S: Signer<Sendable> + Clone + Send + Sync + 'static,
{
    let Some(session_id) = extract_session_id(&headers) else {
        return (StatusCode::UNAUTHORIZED, Vec::new());
    };

    if state.get_session(session_id).await.is_none() {
        return (StatusCode::NOT_FOUND, Vec::new());
    }

    state.touch_session(session_id).await;

    let Ok(msg) = minicbor::decode::<Message>(&body) else {
        warn!("failed to decode call message");
        return (StatusCode::BAD_REQUEST, Vec::new());
    };

    debug!(session_id = %session_id, message = ?msg.variant_name(), "received call");

    (StatusCode::OK, Vec::new())
}

fn extract_session_id(headers: &HeaderMap) -> Option<SessionId> {
    let header_value = headers.get(SESSION_ID_HEADER)?;
    let header_str = header_value.to_str().ok()?;
    SessionId::from_base58(header_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_message_roundtrips() {
        let rejection = Rejection::new(RejectionReason::ClockDrift, TimestampSeconds::new(1000));
        let msg = HandshakeMessage::Rejection(rejection);

        let bytes = minicbor::to_vec(&msg).expect("encode");
        let decoded: HandshakeMessage = minicbor::decode(&bytes).expect("decode");

        let HandshakeMessage::Rejection(decoded_rejection) = decoded else {
            panic!("expected Rejection");
        };

        assert_eq!(decoded_rejection.reason, rejection.reason);
    }
}
