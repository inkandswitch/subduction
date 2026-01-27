//! Server state management.

use alloc::{sync::Arc, vec::Vec};
use core::time::Duration;

use async_lock::RwLock;
use sedimentree_core::collections::Map;
use subduction_core::{
    connection::{message::Message, nonce_cache::NonceCache},
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use tokio::sync::oneshot;

use crate::session::SessionId;

/// Server-side state for HTTP long-polling.
#[derive(Debug)]
pub struct HttpServerState<S> {
    /// Active sessions.
    pub(crate) sessions: Arc<RwLock<Map<SessionId, SessionState>>>,

    /// Nonce cache for replay protection.
    pub(crate) nonce_cache: Arc<NonceCache>,

    /// The server's signer.
    pub(crate) signer: S,

    /// The server's peer ID.
    pub(crate) server_peer_id: PeerId,

    /// Long-poll timeout.
    pub(crate) poll_timeout: Duration,

    /// Session timeout.
    pub(crate) session_timeout: Duration,
}

impl<S> HttpServerState<S> {
    /// Create a new server state.
    pub fn new(
        signer: S,
        server_peer_id: PeerId,
        poll_timeout: Duration,
        session_timeout: Duration,
    ) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(Map::new())),
            nonce_cache: Arc::new(NonceCache::default()),
            signer,
            server_peer_id,
            poll_timeout,
            session_timeout,
        }
    }

    /// Get the server's peer ID.
    #[must_use]
    pub const fn server_peer_id(&self) -> PeerId {
        self.server_peer_id
    }

    /// Get the poll timeout.
    #[must_use]
    pub const fn poll_timeout(&self) -> Duration {
        self.poll_timeout
    }

    /// Get the session timeout.
    #[must_use]
    pub const fn session_timeout(&self) -> Duration {
        self.session_timeout
    }

    /// Get a reference to the signer.
    #[must_use]
    pub const fn signer(&self) -> &S {
        &self.signer
    }

    /// Create a new session.
    pub async fn create_session(&self, peer_id: PeerId) -> SessionId {
        let session_id = SessionId::random();
        let session = SessionState::new(peer_id);

        self.sessions.write().await.insert(session_id, session);
        tracing::info!(session_id = %session_id, peer_id = %peer_id, "created session");

        session_id
    }

    /// Get session info if it exists and is not expired.
    pub async fn get_session(&self, session_id: SessionId) -> Option<SessionInfo> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(&session_id)?;

        let now = TimestampSeconds::now();
        if now.abs_diff(session.last_activity) > self.session_timeout {
            drop(sessions);
            self.sessions.write().await.remove(&session_id);
            tracing::info!(session_id = %session_id, "session expired");
            return None;
        }

        Some(SessionInfo {
            peer_id: session.peer_id,
            last_activity: session.last_activity,
        })
    }

    /// Update session activity timestamp.
    pub async fn touch_session(&self, session_id: SessionId) {
        if let Some(session) = self.sessions.write().await.get_mut(&session_id) {
            session.last_activity = TimestampSeconds::now();
        }
    }

    /// Remove a session.
    pub async fn remove_session(&self, session_id: SessionId) {
        self.sessions.write().await.remove(&session_id);
        tracing::info!(session_id = %session_id, "removed session");
    }

    /// Queue a message for a session.
    pub async fn queue_message(&self, session_id: SessionId, message: Message) -> bool {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(&session_id) {
            if let Some(tx) = session.pending_poll.take()
                && tx.send(vec![message.clone()]).is_ok()
            {
                return true;
            }

            session.outbound_queue.push(message);
            true
        } else {
            false
        }
    }

    /// Take queued messages for a session, or wait for new messages.
    pub async fn take_or_wait_messages(
        &self,
        session_id: SessionId,
    ) -> Option<(Vec<Message>, oneshot::Receiver<Vec<Message>>)> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(&session_id)?;

        session.last_activity = TimestampSeconds::now();

        if !session.outbound_queue.is_empty() {
            let messages = core::mem::take(&mut session.outbound_queue);
            return Some((messages, oneshot::channel().1));
        }

        let (tx, rx) = oneshot::channel();
        session.pending_poll = Some(tx);
        Some((Vec::new(), rx))
    }

    /// Run session cleanup task.
    pub async fn cleanup_expired_sessions(&self) {
        let now = TimestampSeconds::now();
        let mut sessions = self.sessions.write().await;

        let expired: Vec<SessionId> = sessions
            .iter()
            .filter(|(_, session)| now.abs_diff(session.last_activity) > self.session_timeout)
            .map(|(id, _)| *id)
            .collect();

        for id in expired {
            sessions.remove(&id);
            tracing::debug!(session_id = %id, "cleaned up expired session");
        }
    }
}

/// Cloneable session info for external use.
#[derive(Debug, Clone, Copy)]
pub struct SessionInfo {
    /// The peer ID of the connected client.
    pub peer_id: PeerId,

    /// Last activity timestamp.
    pub last_activity: TimestampSeconds,
}

/// State for a single session.
#[derive(Debug)]
pub struct SessionState {
    /// The peer ID of the connected client.
    pub peer_id: PeerId,

    /// Last activity timestamp.
    pub last_activity: TimestampSeconds,

    /// Messages queued for delivery to the client.
    pub outbound_queue: Vec<Message>,

    /// Pending poll request sender (if client is waiting).
    pub(crate) pending_poll: Option<oneshot::Sender<Vec<Message>>>,
}

impl SessionState {
    /// Create a new session state.
    #[must_use]
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            last_activity: TimestampSeconds::now(),
            outbound_queue: Vec::new(),
            pending_poll: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use subduction_core::crypto::signer::MemorySigner;

    fn test_signer() -> MemorySigner {
        MemorySigner::from_bytes(&[42u8; 32])
    }

    #[tokio::test]
    async fn create_and_get_session() {
        let signer = test_signer();
        let state = HttpServerState::new(
            signer.clone(),
            signer.peer_id(),
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let peer_id = PeerId::new([1u8; 32]);
        let session_id = state.create_session(peer_id).await;

        let session = state.get_session(session_id).await;
        assert!(session.is_some());
        assert_eq!(session.expect("session").peer_id, peer_id);
    }

    #[tokio::test]
    async fn remove_session() {
        let signer = test_signer();
        let state = HttpServerState::new(
            signer.clone(),
            signer.peer_id(),
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let peer_id = PeerId::new([1u8; 32]);
        let session_id = state.create_session(peer_id).await;

        state.remove_session(session_id).await;

        let session = state.get_session(session_id).await;
        assert!(session.is_none());
    }
}
