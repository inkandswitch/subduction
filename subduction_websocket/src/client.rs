//! Client-side connection establishment (tokio).

use async_tungstenite::tokio::connect_async;
use thiserror::Error;

use crate::transport::{attach, WebSocketTransport};

/// Connect to a WebSocket server, yielding the transport and its pump
/// future (spawn the pump on your runtime, then hand the transport to
/// [`Handle::connect`](subduction_runtime::driver::handle::Handle::connect)).
///
/// # Errors
///
/// Returns [`ConnectFailed`] if the TCP connection or WebSocket
/// handshake fails.
pub async fn connect(
    url: &str,
) -> Result<(WebSocketTransport, impl Future<Output = ()> + use<>), ConnectFailed> {
    let (ws, _response) = connect_async(url).await.map_err(ConnectFailed)?;
    Ok(attach(ws))
}

/// The TCP connection or WebSocket handshake failed.
#[derive(Debug, Error)]
#[error("websocket connect failed: {0}")]
pub struct ConnectFailed(#[from] tungstenite::Error);
