//! Client-side connection establishment for Iroh transport.
//!
//! Connects to a remote peer by their iroh [`EndpointAddr`] and returns an
//! [`Authenticated`] connection with background listener and sender tasks.

use future_form::Sendable;
use futures::future::BoxFuture;
use iroh::{Endpoint, EndpointAddr, endpoint::Connection as QuicConnection};
use subduction_core::{
    authenticated::Authenticated,
    handshake::{self, audience::Audience},
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};

use crate::{
    ALPN,
    error::{ConnectError, RunError},
    handshake::IrohHandshake,
    transport::IrohTransport,
};

/// Result of a successful connection attempt.
///
/// Contains the authenticated connection plus two background task futures
/// that must be spawned externally for the connection to function.
pub struct ConnectResult {
    /// The authenticated connection, ready for use with Subduction.
    pub authenticated: Authenticated<IrohTransport, Sendable>,

    /// Background task that reads from the QUIC stream.
    /// Must be spawned (e.g., via `tokio::spawn`).
    pub listener_task: BoxFuture<'static, Result<(), RunError>>,

    /// Background task that writes to the QUIC stream.
    /// Must be spawned (e.g., via `tokio::spawn`).
    pub sender_task: BoxFuture<'static, Result<(), RunError>>,
}

impl core::fmt::Debug for ConnectResult {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConnectResult")
            .field("authenticated", &self.authenticated)
            .finish_non_exhaustive()
    }
}

/// Connect to a remote peer via Iroh.
///
/// Opens a QUIC connection to the given address, negotiates the Subduction
/// ALPN, opens a bi-directional stream, and runs the Subduction handshake
/// to mutually authenticate both peers.
///
/// Returns a [`ConnectResult`] containing the authenticated connection and
/// background tasks that must be spawned.
///
/// # Errors
///
/// Returns an error if the QUIC connection, stream establishment, or
/// handshake authentication fails.
///
/// # Example
///
/// ```ignore
/// let result = connect(
///     &endpoint, addr, Duration::from_secs(30), timeout,
///     &signer, Audience::known(peer_id),
/// ).await?;
/// tokio::spawn(result.listener_task);
/// tokio::spawn(result.sender_task);
/// subduction.add_connection(result.authenticated).await?;
/// ```
pub async fn connect<S>(
    endpoint: &Endpoint,
    addr: EndpointAddr,
    signer: &S,
    audience: Audience,
) -> Result<ConnectResult, ConnectError>
where
    S: Signer<Sendable>,
{
    let quic_conn: QuicConnection = endpoint.connect(addr, ALPN).await?;

    tracing::info!(
        remote_id = %quic_conn.remote_id(),
        "iroh: QUIC connection established, starting handshake"
    );

    let (send_stream, recv_stream) = quic_conn.open_bi().await?;

    let now = TimestampSeconds::now();
    let nonce = Nonce::random();
    let quic_conn_clone = quic_conn.clone();

    let (authenticated, (listener_task, sender_task)) =
        handshake::initiate::<Sendable, _, _, _, _>(
            IrohHandshake::new(send_stream, recv_stream),
            move |iroh_handshake, peer_id| {
                let (send_stream, recv_stream) = iroh_handshake.into_parts();

                let (conn, outbound_rx) = IrohTransport::new(peer_id, quic_conn_clone);

                let listener_conn = conn.clone();
                let listener_task: BoxFuture<'static, Result<(), RunError>> =
                    Box::pin(crate::tasks::listener_task(listener_conn, recv_stream));
                let sender_task: BoxFuture<'static, Result<(), RunError>> =
                    Box::pin(crate::tasks::sender_task(send_stream, outbound_rx));

                (conn, (listener_task, sender_task))
            },
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(Box::new)?;

    let peer_id = authenticated.peer_id();
    tracing::info!(peer_id = %peer_id, "iroh: handshake complete, authenticated");

    Ok(ConnectResult {
        authenticated,
        listener_task,
        sender_task,
    })
}

/// Derive a [`PeerId`] from an Iroh QUIC connection's remote identity.
///
/// Iroh uses ed25519 public keys for endpoint identity. The 32-byte
/// compressed Edwards y-coordinate maps directly to our `PeerId`.
#[must_use]
pub fn iroh_peer_id(conn: &QuicConnection) -> PeerId {
    PeerId::new(*conn.remote_id().as_bytes())
}
