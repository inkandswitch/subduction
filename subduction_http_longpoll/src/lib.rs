//! # Subduction HTTP Long-Poll Transport
//!
//! An HTTP long-poll transport layer for the Subduction sync protocol,
//! providing an alternative to WebSocket for environments where WebSocket
//! connections are unreliable or unavailable (e.g., restrictive proxies,
//! corporate firewalls, some serverless platforms).
//!
//! # Protocol
//!
//! The transport maps Subduction's bidirectional message stream onto HTTP
//! request-response pairs:
//!
//! ```text
//! ┌──────────┐                          ┌──────────┐
//! │  Client   │                          │  Server   │
//! └────┬─────┘                          └────┬─────┘
//!      │                                     │
//!      │  POST /lp/handshake                 │
//!      │  Body: Signed<Challenge> (157 B)    │
//!      │ ──────────────────────────────────►  │
//!      │  200 + Signed<Response> (140 B)     │
//!      │  X-Session-Id: <session_id>         │
//!      │ ◄──────────────────────────────────  │
//!      │                                     │
//!      │  POST /lp/send                      │
//!      │  X-Session-Id: <id>                 │
//!      │  Body: Message (binary)             │
//!      │ ──────────────────────────────────►  │
//!      │  204 No Content                     │
//!      │ ◄──────────────────────────────────  │
//!      │                                     │
//!      │  POST /lp/recv                      │
//!      │  X-Session-Id: <id>                 │
//!      │ ──────────────────────────────────►  │
//!      │           ... (blocks) ...          │
//!      │  200 + Message (binary)             │
//!      │ ◄──────────────────────────────────  │
//!      │                                     │
//!      │  POST /lp/disconnect                │
//!      │  X-Session-Id: <id>                 │
//!      │ ──────────────────────────────────►  │
//!      │  204 No Content                     │
//!      │ ◄──────────────────────────────────  │
//! ```
//!
//! # Architecture
//!
//! The server maintains per-session state with async channels that mirror
//! the WebSocket transport's internal architecture:
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │            HttpLongPollConnection                │
//! │                                                 │
//! │  outbound_tx ──► [bounded channel] ──► /recv    │
//! │  /send ──► [bounded channel] ──► inbound_reader │
//! │  pending: Map<RequestId, oneshot::Tx>           │
//! └─────────────────────────────────────────────────┘
//! ```

extern crate alloc;

pub use future_form::Sendable;

pub mod client;
pub mod connection;
pub mod error;
pub mod http_client;
pub mod session;

#[cfg(feature = "server")]
pub mod server;

/// Default long-poll timeout in seconds.
///
/// If no message is available after this duration, the `/lp/recv`
/// endpoint returns 204 No Content and the client should re-poll.
pub const DEFAULT_POLL_TIMEOUT_SECS: u64 = 30;

/// Default maximum HTTP request body size (50 MB).
///
/// Matches the WebSocket transport's `DEFAULT_MAX_MESSAGE_SIZE`.
pub const DEFAULT_MAX_BODY_SIZE: usize = 50 * 1024 * 1024;

/// Session ID header name.
pub const SESSION_ID_HEADER: &str = "x-session-id";
