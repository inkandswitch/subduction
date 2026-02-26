//! Session management for HTTP long-poll connections.
//!
//! Each authenticated client gets a [`SessionId`] that maps to server-side
//! channel state. Sessions are created during handshake and cleaned up on
//! disconnect or expiry.

use alloc::{collections::BTreeMap, sync::Arc};
use core::fmt;

use async_lock::Mutex;
use future_form::Sendable;
use rand::{RngCore, rngs::OsRng};
use subduction_core::{
    connection::{authenticated::Authenticated, timeout::Timeout},
    peer::id::PeerId,
};

use crate::connection::HttpLongPollConnection;

// NOTE: SessionStore and SessionEntry are concrete on `Sendable` (not generic
// over `K: FutureForm`) because `HttpLongPollConnection<O>` only implements
// `Connection<Sendable>`. This mirrors the `subduction_websocket` pattern.

/// An opaque session identifier, assigned after successful handshake.
///
/// Encoded as a 32-byte hex string in HTTP headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId([u8; 16]);

impl SessionId {
    /// Generate a new random session ID.
    #[must_use]
    pub fn random() -> Self {
        let mut bytes = [0u8; 16];
        OsRng.fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// Encode the session ID as a hex string.
    #[must_use]
    pub fn to_hex(&self) -> alloc::string::String {
        let mut buf = alloc::string::String::with_capacity(32);
        for byte in &self.0 {
            use core::fmt::Write;
            let _ = write!(buf, "{byte:02x}");
        }
        buf
    }

    /// Decode a session ID from a hex string.
    ///
    /// # Errors
    ///
    /// Returns `None` if the string is not exactly 32 hex characters.
    #[must_use]
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 32 {
            return None;
        }

        let mut bytes = [0u8; 16];
        #[allow(clippy::indexing_slicing)]
        for (i, chunk) in s.as_bytes().chunks_exact(2).enumerate() {
            let hi = hex_digit(chunk[0])?;
            let lo = hex_digit(chunk[1])?;
            bytes[i] = (hi << 4) | lo;
        }

        Some(Self(bytes))
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Parse a single hex digit.
const fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Thread-safe session store mapping [`SessionId`] to connection state.
#[derive(Debug, Clone)]
pub struct SessionStore<O: Timeout<Sendable> + Send + Sync> {
    pub(crate) sessions: Arc<Mutex<BTreeMap<SessionId, SessionEntry<O>>>>,
}

/// A single session entry containing the connection and peer identity.
#[derive(Debug, Clone)]
pub struct SessionEntry<O: Timeout<Sendable> + Send + Sync> {
    /// The peer's identity.
    pub peer_id: PeerId,

    /// The connection channels for this session.
    pub connection: HttpLongPollConnection<O>,

    /// The authenticated wrapper, present until consumed by Subduction registration.
    pub authenticated: Option<Authenticated<HttpLongPollConnection<O>, Sendable>>,
}

impl<O: Timeout<Sendable> + Send + Sync> SessionStore<O> {
    /// Create a new empty session store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Insert a new session.
    pub async fn insert(&self, id: SessionId, entry: SessionEntry<O>) {
        self.sessions.lock().await.insert(id, entry);
    }

    /// Look up a session by ID.
    pub async fn get(&self, id: &SessionId) -> Option<SessionEntry<O>>
    where
        SessionEntry<O>: Clone,
    {
        self.sessions.lock().await.get(id).cloned()
    }

    /// Remove a session, returning the entry if it existed.
    pub async fn remove(&self, id: &SessionId) -> Option<SessionEntry<O>> {
        self.sessions.lock().await.remove(id)
    }
}

impl<O: Timeout<Sendable> + Send + Sync> Default for SessionStore<O> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, feature = "std", feature = "bolero"))]
#[allow(clippy::expect_used)]
mod proptests {
    use super::*;

    #[test]
    fn prop_hex_roundtrip() {
        bolero::check!().with_type::<[u8; 16]>().for_each(|bytes| {
            let id = SessionId(*bytes);
            let hex = id.to_hex();
            assert_eq!(hex.len(), 32);
            let decoded = SessionId::from_hex(&hex).expect("roundtrip should succeed");
            assert_eq!(id, decoded);
        });
    }

    #[test]
    fn prop_hex_output_is_valid_lowercase_hex() {
        bolero::check!().with_type::<[u8; 16]>().for_each(|bytes| {
            let hex = SessionId(*bytes).to_hex();
            assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
            assert!(hex.chars().all(|c| !c.is_ascii_uppercase()));
        });
    }

    #[test]
    fn prop_from_hex_rejects_non_32_char_strings() {
        bolero::check!().with_type::<String>().for_each(|s| {
            if s.len() != 32 {
                assert!(SessionId::from_hex(s).is_none());
            }
        });
    }

    #[test]
    fn prop_from_hex_rejects_non_hex_chars() {
        bolero::check!().with_type::<[u8; 16]>().for_each(|bytes| {
            let hex = SessionId(*bytes).to_hex();
            // Replace the first character with a non-hex char
            let corrupted = alloc::format!("z{}", &hex[1..]);
            assert!(SessionId::from_hex(&corrupted).is_none());
        });
    }
}
