//! Authentication of structural JS transports. The result is the core Rust
//! authentication witness, never a wasm-bindgen class or a forgeable JS record.
#![allow(clippy::missing_errors_doc)] // All helpers propagate handshake failures.
use super::{DEFAULT_LOCAL_SERVICE_NAME, JsTransport};
use crate::{error::WasmHandshakeError, signer::JsSigner};
use alloc::string::String;
use core::time::Duration;
use future_form::Local;
use subduction_core::{
    authenticated::Authenticated,
    handshake::{self as hs, audience::Audience},
    nonce_cache::NonceCache,
    peer::id::PeerId,
    timestamp::TimestampSeconds,
    transport::message::MessageTransport,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};

/// Core proof of a successfully authenticated transport.
pub type AuthenticatedTransport = Authenticated<MessageTransport<JsTransport>, Local>;

#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn now() -> TimestampSeconds {
    TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64)
}

/// Initiate a handshake with an expected peer identity.
pub async fn setup(
    transport: JsTransport,
    signer: &JsSigner,
    peer: &PeerId,
) -> Result<AuthenticatedTransport, WasmHandshakeError> {
    initiate(transport, signer, Audience::known(*peer)).await
}
/// Initiate discovery with a service name.
pub async fn setup_discover(
    transport: JsTransport,
    signer: &JsSigner,
    service: Option<String>,
) -> Result<AuthenticatedTransport, WasmHandshakeError> {
    let service = service.unwrap_or_else(|| DEFAULT_LOCAL_SERVICE_NAME.into());
    initiate(transport, signer, Audience::discover(service.as_bytes())).await
}
async fn initiate(
    transport: JsTransport,
    signer: &JsSigner,
    audience: Audience,
) -> Result<AuthenticatedTransport, WasmHandshakeError> {
    let (auth, _) = hs::initiate::<Local, _, _, _, _>(
        transport,
        |transport, peer| (MessageTransport::new(transport), peer),
        signer,
        audience,
        now(),
        Nonce::random(),
    )
    .await?;
    Ok(auth)
}
/// Accept an incoming handshake addressed to this signer.
pub async fn accept(
    transport: JsTransport,
    signer: &JsSigner,
    max_drift_seconds: Option<u32>,
) -> Result<AuthenticatedTransport, WasmHandshakeError> {
    respond(
        transport,
        signer,
        None,
        Duration::from_secs(u64::from(max_drift_seconds.unwrap_or(600))),
    )
    .await
}
/// Accept discovery only for the specified service name.
pub async fn accept_discover(
    transport: JsTransport,
    signer: &JsSigner,
    service: String,
) -> Result<AuthenticatedTransport, WasmHandshakeError> {
    respond(
        transport,
        signer,
        Some(Audience::discover(service.as_bytes())),
        Duration::from_secs(600),
    )
    .await
}
async fn respond(
    transport: JsTransport,
    signer: &JsSigner,
    audience: Option<Audience>,
    max_drift: Duration,
) -> Result<AuthenticatedTransport, WasmHandshakeError> {
    let (auth, _) = hs::respond::<Local, _, _, _, _>(
        transport,
        |transport, peer| (MessageTransport::new(transport), peer),
        signer,
        &NonceCache::default(),
        signer.verifying_key().into(),
        audience,
        now(),
        max_drift,
    )
    .await?;
    Ok(auth)
}
