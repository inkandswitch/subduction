//! Cloneable failures with retained backend sources.
use std::{error::Error as StdError, fmt, sync::Arc};

/// Failure classification independent of backend error types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Runtime admission is closed.
    Closed,
    /// Caller or owner cancelled work.
    Cancelled,
    /// An attempt or operation exceeded its deadline.
    Timeout,
    /// Transient network/transport failure.
    Transport,
    /// Authentication failed, including an unexpected identity.
    Authentication,
    /// Local policy rejected connection or storage access.
    Policy,
    /// Storage or replication operation failed.
    Operation,
    /// Background task panicked or exited unexpectedly.
    Background,
    /// Resources did not terminate within their cleanup budget.
    Cleanup,
    /// Invalid configuration or invocation.
    Configuration,
}

/// Authenticated identity did not match the configured peer expectation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("expected {expected}, authenticated {actual}")]
pub struct PeerIdentityMismatch {
    /// Configured peer identity.
    pub expected: subduction_core::peer::id::PeerId,
    /// Verified identity returned by the connector.
    pub actual: subduction_core::peer::id::PeerId,
}

/// An owned failure, safe to retain in status and deliver to multiple waiters.
#[derive(Debug, Clone)]
pub struct Error {
    kind: ErrorKind,
    context: Arc<str>,
    source: Option<Arc<dyn StdError + Send + Sync>>,
}
impl Error {
    /// Construct a failure without a backend source.
    pub fn new(kind: ErrorKind, context: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            context: context.into(),
            source: None,
        }
    }
    /// Preserve a typed backend source rather than converting it to a string.
    pub fn with_source<E: StdError + Send + Sync + 'static>(
        kind: ErrorKind,
        context: impl Into<Arc<str>>,
        source: E,
    ) -> Self {
        Self {
            kind,
            context: context.into(),
            source: Some(Arc::new(source)),
        }
    }
    /// Portable failure category.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.context)?;
        if let Some(source) = &self.source {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}
impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_ref().map(|source| source.as_ref() as _)
    }
}
