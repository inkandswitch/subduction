//! Server-side accept loop for Iroh transport.
//!
//! Accepts incoming QUIC connections, runs the Subduction handshake to
//! authenticate the remote peer, and creates [`Authenticated`] connections
//! ready for registration with Subduction.

use core::time::Duration;

use future_form::Sendable;
use futures::future::BoxFuture;
use iroh::Endpoint;
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

use crate::{
    client::iroh_peer_id,
    connection::IrohConnection,
    error::{AcceptError, RunError},
    handshake::IrohHandshake,
};

/// Result of accepting a single incoming connection.
pub struct AcceptResult<O: Timeout<Sendable> + Send + Sync> {
    /// The authenticated connection.
    pub authenticated: Authenticated<IrohConnection<O>, Sendable>,

    /// The remote peer's identity.
    pub peer_id: PeerId,

    /// Background listener task -- must be spawned.
    pub listener_task: BoxFuture<'static, Result<(), RunError>>,

    /// Background sender task -- must be spawned.
    pub sender_task: BoxFuture<'static, Result<(), RunError>>,
}

impl<O: Timeout<Sendable> + Send + Sync> core::fmt::Debug for AcceptResult<O> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AcceptResult")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

/// Accept a single incoming Iroh connection.
///
/// Waits for an incoming QUIC connection, completes the TLS handshake,
/// accepts a bi-directional stream, and runs the Subduction handshake
/// to authenticate the remote peer.
///
/// # Errors
///
/// Returns an error if no connection arrives (endpoint shutting down),
/// the TLS handshake / stream acceptance fails, or Subduction handshake
/// authentication fails.
///
/// # Example
///
/// ```ignore
/// loop {
///     let result = accept_one(
///         &endpoint, Duration::from_secs(30), timeout.clone(),
///         &signer, &nonce_cache, our_peer_id, None, Duration::from_secs(600),
///     ).await?;
///     tokio::spawn(result.listener_task);
///     tokio::spawn(result.sender_task);
///     subduction.register(result.authenticated).await?;
/// }
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn accept_one<O, S>(
    endpoint: &Endpoint,
    default_time_limit: Duration,
    timeout: O,
    signer: &S,
    nonce_cache: &NonceCache,
    our_peer_id: PeerId,
    discovery_audience: Option<Audience>,
    handshake_max_drift: Duration,
) -> Result<AcceptResult<O>, AcceptError>
where
    O: Timeout<Sendable> + Clone + Send + Sync + 'static,
    S: Signer<Sendable>,
{
    let incoming = endpoint.accept().await.ok_or(AcceptError::NoIncoming)?;

    let quic_conn = incoming.await?;
    let remote_iroh_id = iroh_peer_id(&quic_conn);

    tracing::info!(
        remote_iroh_id = %remote_iroh_id,
        "iroh: accepted QUIC connection, starting handshake"
    );

    let (send_stream, recv_stream) = quic_conn.accept_bi().await?;

    let now = TimestampSeconds::now();
    let timeout_clone = timeout.clone();
    let quic_conn_clone = quic_conn.clone();

    let (authenticated, (listener_task, sender_task)) = handshake::respond::<Sendable, _, _, _, _>(
        IrohHandshake::new(send_stream, recv_stream),
        move |iroh_handshake, peer_id| {
            let (send_stream, recv_stream) = iroh_handshake.into_parts();

            let (conn, outbound_rx) =
                IrohConnection::new(peer_id, quic_conn_clone, default_time_limit, timeout_clone);

            let listener_conn = conn.clone();
            let listener_task: BoxFuture<'static, Result<(), RunError>> =
                Box::pin(crate::tasks::listener_task(listener_conn, recv_stream));
            let sender_task: BoxFuture<'static, Result<(), RunError>> =
                Box::pin(crate::tasks::sender_task(send_stream, outbound_rx));

            (conn, (listener_task, sender_task))
        },
        signer,
        nonce_cache,
        our_peer_id,
        discovery_audience,
        now,
        handshake_max_drift,
    )
    .await
    .map_err(Box::new)?;

    let peer_id = authenticated.peer_id();
    tracing::info!(peer_id = %peer_id, "iroh: handshake complete, authenticated");

    Ok(AcceptResult {
        authenticated,
        peer_id,
        listener_task,
        sender_task,
    })
}
