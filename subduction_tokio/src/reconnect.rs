//! Reconnection policy and observable logical connections.
use crate::{Error, ErrorKind};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use subduction_core::peer::id::PeerId;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Bounded retry scheduling. Disabled unless passed to `Connect::reconnect`.
#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    /// Initial backoff, before jitter.
    pub initial_delay: Duration,
    /// Maximum backoff, including jitter.
    pub max_delay: Duration,
    /// Maximum consecutive replacement attempts.
    pub max_attempts: u32,
    /// Maximum elapsed time in an outage, including attempts and cleanup.
    pub max_elapsed: Duration,
    /// A connection must remain healthy this long to reset retry history.
    pub healthy_after: Duration,
}
impl ReconnectPolicy {
    /// Exponential backoff with full jitter, capped at 30 seconds and 20 retries.
    #[must_use]
    pub const fn exponential_backoff() -> Self {
        Self {
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(30),
            max_attempts: 20,
            max_elapsed: Duration::from_secs(300),
            healthy_after: Duration::from_secs(30),
        }
    }
    pub(crate) fn delay(&self, failures: u32) -> Duration {
        let factor = 1_u32.checked_shl(failures.min(31)).unwrap_or(u32::MAX);
        let cap = self
            .initial_delay
            .saturating_mul(factor)
            .min(self.max_delay);
        cap.mul_f64(rand::random::<f64>())
    }
}

/// Current logical connection state, independent of physical transport type.
#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    /// Establishing a fresh physical connection.
    Connecting {
        /// Attempt generation.
        generation: u64,
    },
    /// Authenticated and registered, not necessarily caught up.
    Connected {
        /// Verified peer identity.
        peer: PeerId,
        /// Attempt generation.
        generation: u64,
    },
    /// Releasing one failed attempt before a retry or terminal outcome.
    CleaningUp {
        /// Physical attempt generation.
        generation: u64,
        /// Failure which caused cleanup.
        error: Error,
    },
    /// Waiting before a replacement attempt.
    Backoff {
        /// Next attempt generation.
        generation: u64,
        /// Last failure.
        error: Error,
        /// Scheduled delay.
        delay: Duration,
    },
    /// Explicit close or terminal cleanup in progress.
    Closing,
    /// Retained terminal outcome.
    Closed(Result<(), Error>),
}

pub(crate) struct Logical {
    pub(crate) id: u64,
    pub(crate) cancel: CancellationToken,
    pub(crate) peer: Mutex<Option<PeerId>>,
    pub(crate) status: watch::Sender<ConnectionStatus>,
}
impl std::fmt::Debug for Logical {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Logical")
            .field("id", &self.id)
            .field("status", &*self.status.borrow())
            .finish_non_exhaustive()
    }
}

/// Stable logical connection. Clones are observers/controllers, not owners.
/// Dropping a handle does not stop retries; `close` does.
#[derive(Debug, Clone)]
pub struct ConnectionHandle(pub(crate) Arc<Logical>);
impl ConnectionHandle {
    /// ID within this client; unchanged across attempts.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.0.id
    }
    /// Last bound peer, including while waiting to reconnect.
    pub fn peer_id(&self) -> Option<PeerId> {
        *self
            .0
            .peer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    /// Current state. The terminal outcome is retained.
    #[must_use]
    pub fn status(&self) -> ConnectionStatus {
        self.0.status.borrow().clone()
    }
    /// Watch state changes; slow consumers see the latest state rather than an event log.
    #[must_use]
    pub fn watch(&self) -> watch::Receiver<ConnectionStatus> {
        self.0.status.subscribe()
    }
    /// Disable retries and request connection-scoped cleanup.
    pub fn shutdown(&self) {
        self.0.cancel.cancel();
    }
    /// Wait for terminal cleanup without initiating it.
    ///
    /// # Errors
    /// Returns the retained terminal connection or cleanup failure.
    pub async fn closed(&self) -> Result<(), Error> {
        let mut receiver = self.watch();
        loop {
            if let ConnectionStatus::Closed(outcome) = receiver.borrow_and_update().clone() {
                return outcome;
            }
            if receiver.changed().await.is_err() {
                return Err(Error::new(
                    ErrorKind::Background,
                    "connection supervisor disappeared",
                ));
            }
        }
    }
    /// Disable retries and await cleanup. Cancelling this wait does not cancel cleanup.
    ///
    /// # Errors
    /// Returns the retained terminal connection or cleanup failure.
    pub async fn close(&self) -> Result<(), Error> {
        self.shutdown();
        self.closed().await
    }
}
