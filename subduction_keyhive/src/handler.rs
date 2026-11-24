//! Subduction [`Handler`] for keyhive frames.
//!
//! [`KeyhiveHandler`] runs a [`KeyhiveProtocol`] over Subduction's own
//! connections. It shares Subduction's connection map and reaches each peer
//! through a [`PeerConnections`], which sends on whichever of the peer's
//! connections accepts the frame.
//!
//! Subduction tells handlers when a peer's last connection drops, but not when
//! a peer connects. Whoever adds a connection to Subduction calls
//! [`KeyhiveHandler::add_peer`] for a peer it had no connection to before.

use alloc::{sync::Arc, vec::Vec};

use async_lock::Mutex;
use dupe::Dupe;
use future_form::{FutureForm, Local, Sendable, future_form};
use nonempty::NonEmpty;
use sedimentree_core::collections::Map;
use subduction_core::{
    authenticated::Authenticated, connection::Connection, handler::Handler, peer::id::PeerId,
};

use crate::{
    KeyhiveAccess, KeyhiveMessage, KeyhivePeerId, KeyhiveProtocol, KeyhiveProtocolError,
    MessageOutcome, SignedMessage, connection::KeyhiveConnection, signed_message::CborError,
    storage::KeyhiveStorage,
};

/// Subduction's connections, by peer.
pub type Connections<Conn, Async> = Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>>;

/// Receives the [`MessageOutcome`] of each keyhive frame a [`KeyhiveHandler`]
/// handles without error.
pub trait OutcomeObserver {
    /// Called with the outcome of a frame from `peer`.
    fn on_outcome(&self, peer: PeerId, outcome: MessageOutcome);
}

impl OutcomeObserver for () {
    fn on_outcome(&self, _peer: PeerId, _outcome: MessageOutcome) {}
}

/// A peer's keyhive connection. It sends each frame on the peer's
/// Subduction connections in turn until one accepts it since a connection
/// can close before Subduction removes it.
pub struct PeerConnections<Conn: Clone, Async: FutureForm> {
    peer: PeerId,
    connections: Connections<Conn, Async>,
}

impl<Conn: Clone, Async: FutureForm> Clone for PeerConnections<Conn, Async> {
    fn clone(&self) -> Self {
        Self {
            peer: self.peer,
            connections: self.connections.clone(),
        }
    }
}

impl<Conn: Clone, Async: FutureForm> core::fmt::Debug for PeerConnections<Conn, Async> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PeerConnections")
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

/// Why [`PeerConnections`] could not send a frame.
#[derive(Debug, thiserror::Error)]
pub enum PeerSendError<E: core::error::Error + 'static> {
    /// The signed message could not be encoded.
    #[error("encode signed message: {0}")]
    Encode(#[from] CborError),

    /// Subduction has no connection to the peer.
    #[error("the peer is not connected")]
    NotConnected,

    /// Every connection to the peer refused the frame. This is the last
    /// connection's error.
    #[error("send keyhive frame: {0}")]
    Transport(#[source] E),
}

#[future_form(
    Sendable where
        Conn: Connection<Sendable, KeyhiveMessage> + Send + Sync,
        Conn::SendError: Send + 'static,
    Local where
        Conn: Connection<Local, KeyhiveMessage>,
        Conn::SendError: 'static
)]
impl<Async: FutureForm, Conn> KeyhiveConnection<Async> for PeerConnections<Conn, Async> {
    type SendError = PeerSendError<Conn::SendError>;

    fn peer_id(&self) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes(*self.peer.as_bytes())
    }

    fn send(&self, message: SignedMessage) -> Async::Future<'_, Result<(), Self::SendError>> {
        Async::from_future(async move {
            let frame = KeyhiveMessage::from_signed(&message)?;
            let conns: Vec<Authenticated<Conn, Async>> = self
                .connections
                .lock()
                .await
                .get(&self.peer)
                .map(|conns| conns.iter().cloned().collect())
                .unwrap_or_default();
            let mut error = PeerSendError::NotConnected;
            for conn in conns {
                match conn.send(&frame).await {
                    Ok(()) => return Ok(()),
                    Err(e) => error = PeerSendError::Transport(e),
                }
            }
            Err(error)
        })
    }
}

/// The protocol a [`KeyhiveHandler`] runs.
pub type HandlerProtocol<K, Store, Conn, Async> =
    KeyhiveProtocol<K, PeerConnections<Conn, Async>, Store, Async>;

/// Errors from handling a keyhive frame.
#[derive(Debug, thiserror::Error)]
pub enum HandleError<E: core::error::Error + 'static> {
    /// The frame's payload is not a signed message.
    #[error("failed to decode keyhive payload: {0}")]
    Decode(#[from] CborError),

    /// The keyhive protocol returned an error. Events can already have been
    /// ingested.
    #[error("keyhive protocol error: {0}")]
    Protocol(#[source] E),
}

/// [`Handler`] that runs the keyhive sync protocol for keyhive frames
/// arriving on Subduction's connections.
pub struct KeyhiveHandler<Async, K, Store, Conn, Obs = ()>
where
    Async: FutureForm,
    K: KeyhiveAccess<Async>,
    Store: KeyhiveStorage<Async>,
    Conn: Clone,
    PeerConnections<Conn, Async>: KeyhiveConnection<Async>,
{
    protocol: Arc<HandlerProtocol<K, Store, Conn, Async>>,
    connections: Connections<Conn, Async>,
    observer: Obs,
}

impl<Async, K, Store, Conn, Obs> Clone for KeyhiveHandler<Async, K, Store, Conn, Obs>
where
    Async: FutureForm,
    K: KeyhiveAccess<Async>,
    Store: KeyhiveStorage<Async>,
    Conn: Clone,
    PeerConnections<Conn, Async>: KeyhiveConnection<Async>,
    Obs: Clone,
{
    fn clone(&self) -> Self {
        Self {
            protocol: self.protocol.clone(),
            connections: self.connections.clone(),
            observer: self.observer.clone(),
        }
    }
}

impl<Async, K, Store, Conn, Obs> core::fmt::Debug for KeyhiveHandler<Async, K, Store, Conn, Obs>
where
    Async: FutureForm,
    K: KeyhiveAccess<Async>,
    Store: KeyhiveStorage<Async>,
    Conn: Clone,
    PeerConnections<Conn, Async>: KeyhiveConnection<Async>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyhiveHandler").finish_non_exhaustive()
    }
}

impl<Async, K, Store, Conn, Obs> KeyhiveHandler<Async, K, Store, Conn, Obs>
where
    Async: FutureForm,
    K: KeyhiveAccess<Async> + Dupe,
    Store: KeyhiveStorage<Async>,
    Conn: Clone,
    PeerConnections<Conn, Async>: KeyhiveConnection<Async>,
    <PeerConnections<Conn, Async> as KeyhiveConnection<Async>>::SendError: 'static,
{
    /// A handler for `protocol` over `connections`, the map Subduction keeps
    /// (see [`SyncHandler::connections`]). `observer` receives the outcome of
    /// each frame.
    ///
    /// [`SyncHandler::connections`]: subduction_core::handler::sync::SyncHandler::connections
    pub const fn new(
        protocol: Arc<HandlerProtocol<K, Store, Conn, Async>>,
        connections: Connections<Conn, Async>,
        observer: Obs,
    ) -> Self {
        Self {
            protocol,
            connections,
            observer,
        }
    }

    /// The protocol, for starting rounds and refreshing its cache.
    #[must_use]
    pub const fn protocol(&self) -> &Arc<HandlerProtocol<K, Store, Conn, Async>> {
        &self.protocol
    }

    /// Register `peer` with the protocol. Call it when Subduction adds the
    /// first connection to `peer`.
    pub async fn add_peer(&self, peer: PeerId) {
        self.protocol
            .add_peer(keyhive_peer(peer), self.peer_connections(peer))
            .await;
    }

    /// Unregister `peer` unless it has a connection again.
    ///
    /// Subduction removes a peer's last connection before it reports the
    /// disconnect, so the peer can connect again, and be registered, in
    /// between.
    pub async fn remove_peer(&self, peer: PeerId) {
        if self.connections.lock().await.contains_key(&peer) {
            return;
        }
        self.protocol.remove_peer(&keyhive_peer(peer)).await;
    }

    fn peer_connections(&self, peer: PeerId) -> PeerConnections<Conn, Async> {
        PeerConnections {
            peer,
            connections: self.connections.clone(),
        }
    }
}

const fn keyhive_peer(peer: PeerId) -> KeyhivePeerId {
    KeyhivePeerId::from_bytes(*peer.as_bytes())
}

#[future_form(
    Sendable where
        K: KeyhiveAccess<Sendable> + Dupe + Send + Sync + 'static,
        Store: KeyhiveStorage<Sendable> + Send + Sync + 'static,
        Conn: Connection<Sendable, KeyhiveMessage> + Send + Sync + 'static,
        Conn::SendError: Send + Sync + 'static,
        Obs: OutcomeObserver + Send + Sync,
    Local where
        K: KeyhiveAccess<Local> + Dupe,
        Store: KeyhiveStorage<Local>,
        Conn: Connection<Local, KeyhiveMessage> + 'static,
        Conn::SendError: 'static,
        Obs: OutcomeObserver
)]
impl<Async: FutureForm, K, Store, Conn, Obs> Handler<Async, Conn>
    for KeyhiveHandler<Async, K, Store, Conn, Obs>
{
    type Message = KeyhiveMessage;
    type HandlerError = HandleError<KeyhiveProtocolError<K, PeerConnections<Conn, Async>, Async>>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<Conn, Async>,
        message: KeyhiveMessage,
    ) -> Async::Future<'a, Result<(), Self::HandlerError>> {
        Async::from_future(async move {
            let signed = message.into_signed()?;
            let peer = conn.peer_id();
            let outcome = self
                .protocol
                .handle_message(
                    &keyhive_peer(peer),
                    signed,
                    Some(self.peer_connections(peer)),
                )
                .await
                .map_err(HandleError::Protocol)?;
            self.observer.on_outcome(peer, outcome);
            Ok(())
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> Async::Future<'_, ()> {
        Async::from_future(self.remove_peer(peer))
    }
}

#[cfg(all(test, feature = "test-utils"))]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use alloc::vec;

    use subduction_core::connection::test_utils::{
        ChannelMockConnection, ChannelMockConnectionHandle,
    };

    use super::*;
    use crate::{
        MemoryKeyhiveStorage,
        test_utils::{SimpleKeyhive, keyhive_peer_id, make_keyhive},
    };

    type MockConn = ChannelMockConnection<KeyhiveMessage>;
    type TestHandler = KeyhiveHandler<Local, SimpleKeyhive, MemoryKeyhiveStorage, MockConn>;

    const PEER: PeerId = PeerId::new([7; 32]);

    async fn make_handler() -> TestHandler {
        let keyhive = make_keyhive().await;
        let peer_id = keyhive_peer_id(&keyhive);
        let card = keyhive.generate_contact_card().await.unwrap();
        let protocol = KeyhiveProtocol::new(
            Arc::new(Mutex::new(keyhive)),
            MemoryKeyhiveStorage::new(),
            peer_id,
            card.to_bytes(),
        );
        KeyhiveHandler::new(Arc::new(protocol), Arc::default(), ())
    }

    async fn connect(handler: &TestHandler) -> ChannelMockConnectionHandle<KeyhiveMessage> {
        let (conn, handle) = MockConn::new_with_handle(PEER);
        let conn = conn.authenticated();
        let mut connections = handler.connections.lock().await;
        match connections.get_mut(&PEER) {
            Some(conns) => conns.push(conn),
            None => {
                connections.insert(PEER, NonEmpty::new(conn));
            }
        }
        handle
    }

    #[tokio::test(flavor = "current_thread")]
    async fn remove_peer_keeps_a_peer_that_has_a_connection_again() {
        let handler = make_handler().await;
        let _handle = connect(&handler).await;
        handler.add_peer(PEER).await;

        handler.remove_peer(PEER).await;
        assert!(handler.protocol.has_peer(&keyhive_peer(PEER)).await);

        handler.connections.lock().await.clear();
        handler.remove_peer(PEER).await;
        assert!(!handler.protocol.has_peer(&keyhive_peer(PEER)).await);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_frame_is_sent_on_the_first_connection_that_accepts_it() {
        let handler = make_handler().await;
        drop(connect(&handler).await);
        let open = connect(&handler).await;
        let message = SignedMessage::new(vec![1, 2, 3]);

        handler
            .peer_connections(PEER)
            .send(message.clone())
            .await
            .unwrap();

        let sent = open.outbound_rx.recv().await.unwrap();
        assert_eq!(sent.into_signed().unwrap(), message);
    }
}
