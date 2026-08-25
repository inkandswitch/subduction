//! Server-side connection acceptance (tokio).

use async_tungstenite::tokio::accept_async;
use thiserror::Error;
use tokio::net::TcpStream;

use crate::transport::{attach, WebSocketTransport};

/// Upgrade one accepted TCP stream to a WebSocket, yielding the
/// transport and its pump future (spawn the pump, then hand the
/// transport to
/// [`Handle::connect`](subduction_runtime::driver::handle::Handle::connect)
/// with `Direction::Inbound`).
///
/// # Errors
///
/// Returns [`AcceptFailed`] if the WebSocket handshake fails.
pub async fn accept(
    stream: TcpStream,
) -> Result<(WebSocketTransport, impl Future<Output = ()>), AcceptFailed> {
    let ws = accept_async(stream).await.map_err(AcceptFailed)?;
    Ok(attach(ws))
}

/// The WebSocket handshake failed.
#[derive(Debug, Error)]
#[error("websocket accept failed: {0}")]
pub struct AcceptFailed(#[from] tungstenite::Error);
