//! Session management for HTTP long-poll connections.
//!
//! Each authenticated client gets a [`SessionId`] that maps to server-side
//! channel state. Sessions are created during handshake and cleaned up on
//! disconnect or expiry.

use alloc::{collections::BTreeMap, sync::Arc};
use core::fmt;

use async_lock::Mutex;
use subduction_core::peer::id::PeerId;

use future_form::Sendable;
use subduction_core::connection::authenticated::Authenticated;

use crate::connection::HttpLongPollConnection;

/// An opaque session identifier, assigned after successful handshake.
///
/// Encoded as a 32-byte hex string in HTTP headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId([u8; 16]);

impl SessionId {
    /// Generate a new random session ID.
    pub fn random() -> Self {
        Self(rand::random())
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
pub struct SessionStore {
    pub(crate) sessions: Arc<Mutex<BTreeMap<SessionId, SessionEntry>>>,
}

/// A single session entry containing the connection and peer identity.
#[derive(Debug, Clone)]
pub struct SessionEntry {
    /// The peer's identity.
    pub peer_id: PeerId,

    /// The connection channels for this session.
    pub connection: HttpLongPollConnection,

    /// The authenticated wrapper, present until consumed by Subduction registration.
    pub authenticated: Option<Authenticated<HttpLongPollConnection, Sendable>>,
}

impl SessionStore {
    /// Create a new empty session store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Insert a new session.
    pub async fn insert(&self, id: SessionId, entry: SessionEntry) {
        self.sessions.lock().await.insert(id, entry);
    }

    /// Look up a session by ID.
    pub async fn get(&self, id: &SessionId) -> Option<SessionEntry> {
        self.sessions.lock().await.get(id).cloned()
    }

    /// Remove a session, returning the entry if it existed.
    pub async fn remove(&self, id: &SessionId) -> Option<SessionEntry> {
        self.sessions.lock().await.remove(id)
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_hex_roundtrip() {
        let id = SessionId::random();
        let hex = id.to_hex();
        assert_eq!(hex.len(), 32);
        let decoded = SessionId::from_hex(&hex).expect("valid hex");
        assert_eq!(id, decoded);
    }

    #[test]
    fn session_id_from_hex_rejects_invalid() {
        assert!(SessionId::from_hex("").is_none());
        assert!(SessionId::from_hex("too_short").is_none());
        assert!(SessionId::from_hex("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_none());
    }
}
