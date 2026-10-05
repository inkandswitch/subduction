//! Owned Tokio runtime integration with erased transports and public connectors.
//!
//! Start with [`Node::builder`], explicitly supplying storage, policy, and
//! signer. [`NodeHandle`] provides managed operations without a raw core escape
//! hatch. [`Connector`] implementations own establishment through [`AttemptScope`];
//! the runtime owns retries and teardown. Enable `websocket` for the bundled connector.
//!
//! Close the node before shutting down Tokio. Cancellation is not rollback;
//! reconnection does not restore subscriptions or imply replication convergence.

pub mod connector;
mod core;
pub mod error;
mod node;
pub mod reconnect;
mod tasks;
pub mod transport;
#[cfg(feature = "websocket")]
pub mod websocket;

pub use connector::{
    AttemptScope, AuthenticationContext, Connector, EstablishedConnection, RetryDecision,
    SharedSigner,
};
pub use core::PeerSyncReport;
pub use error::{Error, ErrorKind, PeerIdentityMismatch};
pub use node::{CloseReport, Connect, Node, NodeBuilder, NodeEvent, NodeHandle, NodeStatus, Unset};
pub use reconnect::{ConnectionHandle, ConnectionStatus, ReconnectPolicy};
pub use subduction_core::{
    handshake::audience::Audience as ExpectedPeer,
    stored::{LoadError, StoredEvent, StoredItem, StoredUpdates},
    timeout::call::CallTimeout,
};
pub use transport::ErasedTransport;
