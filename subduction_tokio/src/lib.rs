//! Owned Tokio runtime integration with erased transports and public connectors.
//!
//! Start with [`TokioClient::builder`], explicitly supplying storage, policy, and
//! signer. [`ClientHandle`] provides managed operations without a raw core escape
//! hatch. [`Connector`] implementations own establishment through [`AttemptScope`];
//! the runtime owns retries and teardown. Enable `websocket` for the bundled connector.
//!
//! Close the client before shutting down Tokio. Cancellation is not rollback;
//! reconnection does not restore subscriptions or imply replication convergence.

mod client;
pub mod connector;
mod core;
pub mod error;
pub mod reconnect;
mod tasks;
pub mod transport;
#[cfg(feature = "websocket")]
pub mod websocket;

pub use client::{
    ClientBuilder, ClientEvent, ClientHandle, ClientStatus, CloseReport, Connect, TokioClient,
    Unset,
};
pub use connector::{
    AttemptScope, AuthenticationContext, Connector, EstablishedConnection, RetryDecision,
    SharedSigner,
};
pub use core::PeerSyncReport;
pub use error::{Error, ErrorKind, PeerIdentityMismatch};
pub use reconnect::{ConnectionHandle, ConnectionStatus, ReconnectPolicy};
pub use subduction_core::handshake::audience::Audience as ExpectedPeer;
pub use subduction_core::timeout::call::CallTimeout;
pub use transport::ErasedTransport;
