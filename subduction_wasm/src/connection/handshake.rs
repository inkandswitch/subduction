//! Wasm-specific handshake implementation for WebSocket connections.
//!
//! This module provides the client-side handshake for authenticating
//! WebSocket connections from browser environments.

use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};

use futures::channel::oneshot;
use futures_kind::Local;
use subduction_core::{
    connection::handshake::{self, Audience, Challenge, Nonce, Rejection, RejectionReason},
    crypto::{signed::Signed, signer::Signer},
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use wasm_bindgen::{JsCast, closure::Closure};
use web_sys::{MessageEvent, WebSocket, js_sys};

use crate::error::WasmHandshakeError;

/// Handshake message types for WebSocket transport.
///
/// Must match the structure used by `subduction_websocket::handshake`.
#[derive(Debug, minicbor::Encode, minicbor::Decode)]
pub(super) enum HandshakeMessage {
    /// A signed challenge from the client.
    #[n(0)]
    SignedChallenge(#[n(0)] Signed<Challenge>),

    /// A signed response from the server.
    #[n(1)]
    SignedResponse(#[n(0)] Signed<handshake::Response>),

    /// An unsigned rejection from the server.
    #[n(2)]
    Rejection(#[n(0)] Rejection),
}

/// Result of a successful client handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClientHandshakeResult {
    /// The verified server peer ID.
    pub server_id: PeerId,
    /// The server's timestamp (for drift correction).
    pub server_timestamp: TimestampSeconds,
}

/// Perform the client-side handshake over a WebSocket.
///
/// # Arguments
///
/// * `ws` - The WebSocket (must be in OPEN state)
/// * `signer` - The client's signer for creating the challenge
/// * `expected_peer_id` - The expected server peer ID
///
/// # Errors
///
/// Returns an error if:
/// - The WebSocket message could not be sent/received
/// - The server rejected the handshake
/// - The response signature is invalid
/// - The response doesn't match the challenge
pub(super) async fn client_handshake(
    ws: &WebSocket,
    signer: &impl Signer<Local>,
    expected_peer_id: PeerId,
) -> Result<ClientHandshakeResult, WasmHandshakeError> {
    // Create and send challenge
    let audience = Audience::known(expected_peer_id);
    // Get current time from JavaScript's Date.now() (milliseconds since epoch)
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let now_secs = (js_sys::Date::now() / 1000.0) as u64;
    let now = TimestampSeconds::new(now_secs);
    let nonce = Nonce::random();
    let challenge = Challenge::new(audience, now, nonce);
    let signed_challenge = Signed::seal::<Local, _>(signer, challenge).await;

    let challenge_msg = HandshakeMessage::SignedChallenge(signed_challenge);
    let challenge_bytes = minicbor::to_vec(&challenge_msg)
        .map_err(|e| WasmHandshakeError::InvalidMessage(e.to_string()))?;

    ws.send_with_u8_array(&challenge_bytes)
        .map_err(|e| WasmHandshakeError::WebSocket(format!("{e:?}")))?;

    // Set up oneshot channel to receive the response
    let (tx, rx) = oneshot::channel::<Result<Vec<u8>, String>>();
    let tx_cell = core::cell::RefCell::new(Some(tx));

    let onmessage = Closure::<dyn FnMut(_)>::new(move |event: MessageEvent| {
        if let Some(tx) = tx_cell.borrow_mut().take() {
            if let Ok(buf) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes: Vec<u8> = js_sys::Uint8Array::new(&buf).to_vec();
                drop(tx.send(Ok(bytes)));
            } else {
                drop(tx.send(Err("expected binary message".into())));
            }
        }
    });

    // Temporarily replace the onmessage handler
    let old_onmessage = ws.onmessage();
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

    // Wait for response
    let response_bytes = rx
        .await
        .map_err(|_| WasmHandshakeError::WebSocket("channel canceled".into()))?
        .map_err(WasmHandshakeError::WebSocket)?;

    // Restore original handler (will be set up properly by setup())
    ws.set_onmessage(old_onmessage.as_ref());

    // Keep closure alive until we're done
    drop(onmessage);

    // Decode and verify response
    let handshake_msg: HandshakeMessage = minicbor::decode(&response_bytes)
        .map_err(|e| WasmHandshakeError::InvalidMessage(e.to_string()))?;

    match handshake_msg {
        HandshakeMessage::SignedResponse(signed_response) => {
            let verified = handshake::verify_response(&signed_response, &challenge)
                .map_err(|_| WasmHandshakeError::InvalidSignature)?;

            Ok(ClientHandshakeResult {
                server_id: verified.server_id,
                server_timestamp: verified.response.server_timestamp,
            })
        }
        HandshakeMessage::Rejection(rejection) => {
            let reason_str = match rejection.reason {
                RejectionReason::ClockDrift => "clock drift too large",
                RejectionReason::InvalidAudience => "invalid audience",
                RejectionReason::ReplayedNonce => "replayed nonce",
                RejectionReason::InvalidSignature => "invalid signature",
            };
            Err(WasmHandshakeError::Rejected(reason_str.into()))
        }
        HandshakeMessage::SignedChallenge(_) => Err(WasmHandshakeError::InvalidMessage(
            "expected response, got challenge".into(),
        )),
    }
}
