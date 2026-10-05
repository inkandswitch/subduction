//! Optional outbound WebSocket connector. No detached transport tasks.
use crate::{
    AttemptScope, AuthenticationContext, Connector, Error, ErrorKind, EstablishedConnection,
};
use futures::future::BoxFuture;
use std::{future::IntoFuture, str::FromStr};
use subduction_core::handshake::AuthenticateError;
use subduction_websocket::{
    tokio::client::{ClientConnectError, TokioWebSocketClient},
    websocket::KeepAlive,
};

/// Repeatable outbound WebSocket dial configuration.
#[derive(Debug, Clone)]
#[must_use]
pub struct WebSocketConnector {
    url: String,
    max_message_size: usize,
    keepalive: KeepAlive,
}
impl WebSocketConnector {
    /// Connect to this URL on every attempt. The node supplies the signer and audience.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            max_message_size: subduction_websocket::DEFAULT_MAX_MESSAGE_SIZE,
            keepalive: KeepAlive::balanced(),
        }
    }
    /// Maximum WebSocket message and frame size.
    pub const fn max_message_size(mut self, bytes: usize) -> Self {
        self.max_message_size = bytes;
        self
    }
    /// Transport keepalive configuration.
    pub const fn keepalive(mut self, keepalive: KeepAlive) -> Self {
        self.keepalive = keepalive;
        self
    }
}
impl Connector for WebSocketConnector {
    fn establish(
        &self,
        attempt: AttemptScope,
        authentication: AuthenticationContext,
    ) -> BoxFuture<'_, Result<EstablishedConnection, Error>> {
        Box::pin(async move {
            let uri = FromStr::from_str(&self.url)
                .map_err(|e| Error::with_source(ErrorKind::Configuration, "WebSocket URL", e))?;
            let (transport, listener, sender, keepalive) = TokioWebSocketClient::with_options(
                uri,
                authentication.signer().clone(),
                authentication.audience(),
                self.max_message_size,
                self.keepalive,
            )
            .await
            .map_err(|error| {
                let kind = match &error {
                    ClientConnectError::Handshake(
                        AuthenticateError::Transport(_) | AuthenticateError::ConnectionClosed,
                    )
                    | ClientConnectError::WebSocket(_) => ErrorKind::Transport,
                    ClientConnectError::Handshake(_) => ErrorKind::Authentication,
                };
                Error::with_source(kind, "WebSocket establish", error)
            })?;
            // No await between obtaining the socket and transferring cleanup
            // ownership. Startup failures are then covered by the attempt scope.
            let established = attempt.established(transport)?;
            attempt.spawn("WebSocket listener", async move {
                listener
                    .await
                    .map_err(|e| Error::with_source(ErrorKind::Transport, "WebSocket listener", e))
            })?;
            attempt.spawn("WebSocket sender", async move {
                sender
                    .await
                    .map_err(|e| Error::with_source(ErrorKind::Transport, "WebSocket sender", e))
            })?;
            attempt.spawn("WebSocket keepalive", async move {
                let outcome = keepalive.into_future().await;
                Err(Error::new(
                    ErrorKind::Transport,
                    format!("WebSocket keepalive: {outcome:?}"),
                ))
            })?;
            Ok(established)
        })
    }
}
