//! HTTP long-polling server implementation.
//!
//! Provides an Axum router for handling HTTP long-polling connections.

mod handlers;
mod state;

pub use handlers::router;
pub use state::{HttpServerState, SessionState};

use alloc::sync::Arc;
use core::time::Duration;

use future_form::Sendable;
use subduction_core::{crypto::signer::Signer, peer::id::PeerId};

use crate::{DEFAULT_POLL_TIMEOUT_SECS, DEFAULT_SESSION_TIMEOUT_SECS};

/// Builder for creating an HTTP server.
#[derive(Debug)]
pub struct HttpServerBuilder<S> {
    signer: S,
    server_peer_id: PeerId,
    poll_timeout: Duration,
    session_timeout: Duration,
}

impl<S: Signer<Sendable> + Clone + Send + Sync + 'static> HttpServerBuilder<S> {
    /// Create a new server builder.
    pub fn new(signer: S) -> Self {
        let server_peer_id = signer.peer_id();
        Self {
            signer,
            server_peer_id,
            poll_timeout: Duration::from_secs(DEFAULT_POLL_TIMEOUT_SECS),
            session_timeout: Duration::from_secs(DEFAULT_SESSION_TIMEOUT_SECS),
        }
    }

    /// Set the long-poll timeout.
    #[must_use]
    pub const fn poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Set the session timeout.
    #[must_use]
    pub const fn session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }

    /// Build the server state.
    #[must_use]
    pub fn build(self) -> Arc<HttpServerState<S>> {
        Arc::new(HttpServerState::new(
            self.signer,
            self.server_peer_id,
            self.poll_timeout,
            self.session_timeout,
        ))
    }

    /// Build and create the Axum router.
    pub fn into_router(self) -> axum::Router {
        let state = self.build();
        router(state)
    }
}
