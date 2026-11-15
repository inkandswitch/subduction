//! Tokio socket types with inbound progress counted under tungstenite.
//!
//! The keepalive needs a [`Counted`] stream directly on the socket, below
//! TLS and the WebSocket framer (see [`counted`](crate::counted)). Accepted
//! connections get it by wrapping the `TcpStream` before the upgrade.
//! `connect_async` opens its own socket, so [`dial`] replaces it.

use async_tungstenite::{
    WebSocketStream,
    tokio::{ClientStream, TokioAdapter, client_async_tls_with_connector_and_config},
};
use tokio::net::TcpStream;
use tungstenite::{Error, client::IntoClientRequest, error::UrlError, protocol::WebSocketConfig};

use crate::counted::{Counted, InboundProgress};

/// The stream under a WebSocket accepted from a TCP listener.
pub type AcceptStream = TokioAdapter<Counted<TcpStream>>;

/// The stream under a WebSocket dialed over TCP, with TLS for `wss://`.
pub type DialStream = ClientStream<Counted<TcpStream>>;

/// Connect to a WebSocket server, as `connect_async_with_config` does, with
/// the socket's inbound progress counted.
///
/// # Errors
///
/// Returns an error if the request has no host or a scheme other than `ws`
/// or `wss` without a port, if the TCP connection or TLS fails, or if the
/// WebSocket upgrade fails.
pub async fn dial<R: IntoClientRequest + Unpin>(
    request: R,
    config: Option<WebSocketConfig>,
) -> Result<(WebSocketStream<DialStream>, InboundProgress), Error> {
    let request = request.into_client_request()?;
    let uri = request.uri();

    let host = uri.host().ok_or(Error::Url(UrlError::NoHostName))?;
    // An IPv6 host keeps its brackets in the URI, but they are not part of
    // the address.
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
        .to_owned();
    let port = uri
        .port_u16()
        .or(match uri.scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or(Error::Url(UrlError::UnsupportedUrlScheme))?;

    let tcp = Counted::new(TcpStream::connect((host.as_str(), port)).await?);
    let inbound = tcp.progress();
    let (ws, _response) =
        client_async_tls_with_connector_and_config(request, tcp, None, config).await?;
    Ok((ws, inbound))
}
