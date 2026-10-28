//! Keyhive-enabled Subduction nodes for end-to-end tests.
//!
//! A [`Node`] is a whole Subduction instance with the keyhive runtime wired in
//! the way an application wires it:
//!
//! ```text
//!             one connection per peer
//!   ┌─────────────────────────────────────────┐
//!   │ NodeMessage::Sync  ──▶ SyncHandler       │──▶ SubductionKeyhive gates
//!   │ NodeMessage::Keyhive ─▶ KeyhiveHandler  │    connect / fetch / put
//!   └─────────────────────────────────────────┘
//! ```
//!
//! Keyhive (`SUK`) frames share the connection with sync (`SUM`) traffic, and
//! [`SubductionKeyhive`] over the same Keyhive the protocol updates is the
//! storage policy. Nodes are generic over the transport, so one harness drives
//! in-memory channel pairs and real network clients alike. [`NodeMessage`] is
//! wire-compatible with the CLI's message envelope (minus ephemeral traffic),
//! so a node can talk to a running server.

use std::{boxed::Box, fmt, sync::Arc, time::Duration, vec::Vec};

use async_lock::Mutex;
use dupe::Dupe;
use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
use keyhive_crypto::identifier::Identifier;
use sedimentree_core::{
    codec::{
        decode::Decode,
        encode::Encode,
        error::{DecodeError, InvalidSchema},
    },
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{
        Connection,
        message::{
            BatchSyncResponse, MESSAGE_SCHEMA, SyncMessage, TryAsBatchSyncResponse,
            TryAsSubscribeRequest,
        },
        test_utils::{ChannelTransport, TokioSpawn},
    },
    handler::{Handler, sync::SyncHandler},
    peer::id::PeerId,
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    storage::memory::MemoryStorage,
    subduction::{
        Subduction,
        builder::SubductionBuilder,
        error::{AddConnectionError, IoError, ListenError},
    },
    timeout::{TimedOut, Timeout},
    transport::{Transport, message::MessageTransport},
};
use subduction_crypto::signer::memory::MemorySigner;

use crate::{
    KEYHIVE_SCHEMA, KeyhiveMessage, KeyhivePeerId, MemoryKeyhiveStorage, ProtocolError,
    SignedMessage,
    connection::KeyhiveConnection,
    handler::{SendableKeyhiveHandler, SendableRuntimeProtocol},
    policy::SubductionKeyhive,
    runtime::{SendableRuntimeKeyhive, init_sendable_keyhive},
    signed_message::CborError,
};

/// How long [`eventually`] waits by default.
pub const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(10);

/// What a transport needs for a [`Node`] to run over it.
pub trait NodeTransport:
    Transport<
        Sendable,
        SendError: Send + Sync + 'static,
        RecvError: Send + Sync + 'static,
        DisconnectionError: Send + Sync + 'static,
    > + Clone
    + PartialEq
    + fmt::Debug
    + Send
    + Sync
    + 'static
{
}

impl<T> NodeTransport for T where
    T: Transport<
            Sendable,
            SendError: Send + Sync + 'static,
            RecvError: Send + Sync + 'static,
            DisconnectionError: Send + Sync + 'static,
        > + Clone
        + PartialEq
        + fmt::Debug
        + Send
        + Sync
        + 'static
{
}

/// A node's Subduction connection.
pub type NodeConn<T> = MessageTransport<T>;

/// The storage policy: membership in the node's own Keyhive.
pub type NodePolicy = SubductionKeyhive<
    Sendable,
    keyhive_crypto::signer::memory::MemorySigner,
    crate::change_id::ChangeId,
    Vec<u8>,
    keyhive_core::store::ciphertext::memory::MemoryCiphertextStore<
        crate::change_id::ChangeId,
        Vec<u8>,
    >,
    keyhive_core::listener::no_listener::NoListener,
    rand::rngs::OsRng,
    crate::handler::RuntimeKeyline,
>;

/// The keyhive protocol a node runs.
pub type NodeProtocol<T> = SendableRuntimeProtocol<NodeKeyhiveConn<T>, MemoryKeyhiveStorage>;

/// A node's sync handler.
pub type NodeSyncHandler<T> = SyncHandler<
    Sendable,
    MemoryStorage,
    NodeConn<T>,
    NodePolicy,
    CountLeadingZeroBytes,
    TokioSpawn,
>;

/// A node's keyhive handler.
pub type NodeKeyhiveHandler<T> = SendableKeyhiveHandler<
    NodeKeyhiveConn<T>,
    MemoryKeyhiveStorage,
    NodeConn<T>,
    fn(Authenticated<NodeConn<T>, Sendable>) -> NodeKeyhiveConn<T>,
>;

/// A node's Subduction instance.
pub type NodeSubduction<T> = Subduction<
    'static,
    Sendable,
    MemoryStorage,
    NodeConn<T>,
    NodeHandler<T>,
    NodePolicy,
    MemorySigner,
    TokioTimeout,
    TokioSpawn,
    CountLeadingZeroBytes,
>;

type NodeListenError<T> = ListenError<Sendable, MemoryStorage, NodeConn<T>, NodeMessage>;

/// A Subduction instance running the keyhive protocol, with keyhive
/// membership as its storage policy.
pub struct Node<T: NodeTransport> {
    /// The Subduction instance.
    pub subduction: Arc<NodeSubduction<T>>,

    /// The keyhive protocol, sharing the policy's Keyhive.
    pub protocol: Arc<NodeProtocol<T>>,

    /// The node's Keyhive. Changes are visible to the protocol and the policy.
    pub keyhive: SendableRuntimeKeyhive,

    signer: MemorySigner,
}

impl<T: NodeTransport> Node<T> {
    /// A node whose Subduction and Keyhive identities share the signing key
    /// `[seed; 32]`, as the CLI derives them.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError::Init`] if the Keyhive can't be generated.
    pub async fn new(seed: u8) -> Result<Self, NodeError> {
        let key = [seed; 32];
        let keyhive_signer: keyhive_crypto::signer::memory::MemorySigner =
            ed25519_dalek::SigningKey::from_bytes(&key).into();
        let (keyhive, keyhive_peer_id, contact_card) = init_sendable_keyhive(keyhive_signer)
            .await
            .map_err(NodeError::Init)?;

        let protocol = Arc::new(NodeProtocol::new(
            Arc::new(Mutex::new(keyhive.dupe())),
            MemoryKeyhiveStorage::new(),
            keyhive_peer_id,
            contact_card,
        ));

        let policy = Arc::new(SubductionKeyhive::new(keyhive.dupe()));
        let keyhive_protocol = Arc::clone(&protocol);
        let signer = MemorySigner::from_bytes(&key);
        let (subduction, listener, manager, ()) = SubductionBuilder::new()
            .signer(signer.clone())
            .storage(MemoryStorage::new(), policy)
            .spawner(TokioSpawn)
            .timer(TokioTimeout)
            .build_composed::<Sendable, NodeConn<T>, NodeHandler<T>, ()>(|sync| {
                let keyhive_handler: NodeKeyhiveHandler<T> =
                    SendableKeyhiveHandler::new(keyhive_protocol, NodeKeyhiveConn::new);
                (
                    Arc::new(NodeHandler {
                        sync,
                        keyhive: keyhive_handler,
                    }),
                    (),
                )
            });
        tokio::spawn(listener);
        tokio::spawn(manager);

        Ok(Self {
            subduction,
            protocol,
            keyhive,
            signer,
        })
    }

    /// The node's Subduction peer ID.
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.subduction.peer_id()
    }

    /// The node's Subduction signer, for handshakes.
    #[must_use]
    pub const fn signer(&self) -> &MemorySigner {
        &self.signer
    }

    /// The node's Keyhive identity.
    #[must_use]
    pub fn identifier(&self) -> Identifier {
        self.keyhive.id().into()
    }

    /// Register an authenticated connection with Subduction and keyhive, as
    /// a server does when a peer connects.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection policy refuses the peer.
    pub async fn attach(
        &self,
        auth: Authenticated<NodeConn<T>, Sendable>,
    ) -> Result<(), AddConnectionError<keyhive_crypto::identifier::InvalidIdentifier>> {
        self.subduction.add_connection(auth.clone()).await?;
        let conn = NodeKeyhiveConn::new(auth);
        self.protocol.add_peer(conn.peer_id(), conn).await;
        Ok(())
    }

    /// Start a keyhive sync with every connected peer. It runs to completion
    /// over the connections; use [`eventually`] to wait for its effects.
    ///
    /// # Errors
    ///
    /// Returns an error if a sync request can't be built or sent.
    pub async fn sync_keyhive(
        &self,
    ) -> Result<(), ProtocolError<NodeKeyhiveSendError<T::SendError>>> {
        self.protocol.sync_keyhive(None).await
    }
}

impl Node<ChannelTransport> {
    /// Connect two nodes over an in-memory channel pair, `self` dialing.
    ///
    /// # Errors
    ///
    /// Returns an error if either side's connection policy refuses the other.
    pub async fn connect(
        &self,
        other: &Self,
    ) -> Result<(), AddConnectionError<keyhive_crypto::identifier::InvalidIdentifier>> {
        let (ours, theirs) = ChannelTransport::pair();
        self.attach(Authenticated::new_for_test(
            MessageTransport::new(ours),
            other.peer_id(),
            Direction::Dialed,
        ))
        .await?;
        other
            .attach(Authenticated::new_for_test(
                MessageTransport::new(theirs),
                self.peer_id(),
                Direction::Accepted,
            ))
            .await
    }
}

impl<T: NodeTransport> fmt::Debug for Node<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("peer_id", &self.peer_id())
            .finish_non_exhaustive()
    }
}

/// Wait until `check` holds, polling every few milliseconds.
///
/// # Errors
///
/// Returns [`NotConverged`] if `check` still fails after `timeout`.
pub async fn eventually<F, Fut>(
    what: &'static str,
    timeout: Duration,
    mut check: F,
) -> Result<(), NotConverged>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check().await {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(NotConverged(what));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A [`Timeout`] on the Tokio timer, so a lost response fails a call instead
/// of hanging the test.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioTimeout;

impl Timeout<Sendable> for TokioTimeout {
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: BoxFuture<'a, T>,
    ) -> BoxFuture<'a, Result<T, TimedOut>> {
        async move { tokio::time::timeout(dur, fut).await.map_err(|_| TimedOut) }.boxed()
    }
}

/// The wire envelope a node speaks: sync or keyhive traffic, told apart by
/// the 4-byte schema header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeMessage {
    /// A sync-protocol message (`SUM\x00`).
    Sync(Box<SyncMessage>),

    /// A keyhive-protocol message (`SUK\x00`).
    Keyhive(KeyhiveMessage),
}

impl From<SyncMessage> for NodeMessage {
    fn from(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }
}

impl From<KeyhiveMessage> for NodeMessage {
    fn from(msg: KeyhiveMessage) -> Self {
        Self::Keyhive(msg)
    }
}

impl TryAsBatchSyncResponse for NodeMessage {
    fn try_as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            Self::Sync(sync) => sync.try_as_batch_sync_response(),
            Self::Keyhive(_) => None,
        }
    }
}

impl TryAsSubscribeRequest for NodeMessage {
    fn try_as_subscribe_request(&self) -> Option<SedimentreeId> {
        match self {
            Self::Sync(sync) => sync.try_as_subscribe_request(),
            Self::Keyhive(_) => None,
        }
    }
}

impl Encode for NodeMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Sync(msg) => Encode::encode(msg.as_ref()),
            Self::Keyhive(msg) => msg.encode(),
        }
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::Sync(msg) => msg.encoded_size(),
            Self::Keyhive(msg) => msg.encoded_size(),
        }
    }
}

impl Decode for NodeMessage {
    const MIN_SIZE: usize = 8;

    fn try_decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let schema: [u8; 4] =
            buf.get(0..4)
                .and_then(|s| s.try_into().ok())
                .ok_or(DecodeError::MessageTooShort {
                    type_name: "NodeMessage schema",
                    need: 4,
                    have: buf.len(),
                })?;

        match schema {
            MESSAGE_SCHEMA => SyncMessage::try_decode(buf).map(|m| Self::Sync(Box::new(m))),
            KEYHIVE_SCHEMA => KeyhiveMessage::try_decode(buf).map(Self::Keyhive),
            _ => Err(InvalidSchema {
                expected: MESSAGE_SCHEMA,
                got: schema,
            }
            .into()),
        }
    }
}

/// Dispatches [`NodeMessage`]s to the sync and keyhive handlers.
pub struct NodeHandler<T: NodeTransport> {
    sync: Arc<NodeSyncHandler<T>>,
    keyhive: NodeKeyhiveHandler<T>,
}

impl<T: NodeTransport> fmt::Debug for NodeHandler<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHandler").finish_non_exhaustive()
    }
}

impl<T: NodeTransport> RemoteHeadsNotifier for NodeHandler<T> {
    fn notify_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        RemoteHeadsNotifier::notify_remote_heads(self.sync.as_ref(), id, peer, heads);
    }
}

impl<T: NodeTransport> Handler<Sendable, NodeConn<T>> for NodeHandler<T> {
    type Message = NodeMessage;
    type HandlerError = NodeListenError<T>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<NodeConn<T>, Sendable>,
        message: NodeMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        async move {
            match message {
                NodeMessage::Sync(msg) => {
                    Handler::<Sendable, NodeConn<T>>::handle(self.sync.as_ref(), conn, *msg)
                        .await
                        .map_err(rewrap_listen_error)
                }
                NodeMessage::Keyhive(msg) => {
                    if let Err(e) =
                        Handler::<Sendable, NodeConn<T>>::handle(&self.keyhive, conn, msg).await
                    {
                        tracing::warn!(error = %e, "keyhive handler error");
                    }
                    Ok(())
                }
            }
        }
        .boxed()
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> BoxFuture<'_, ()> {
        async move {
            Handler::<Sendable, NodeConn<T>>::on_peer_disconnect(self.sync.as_ref(), peer).await;
            Handler::<Sendable, NodeConn<T>>::on_peer_disconnect(&self.keyhive, peer).await;
        }
        .boxed()
    }
}

fn rewrap_listen_error<T: NodeTransport>(
    err: ListenError<Sendable, MemoryStorage, NodeConn<T>, SyncMessage>,
) -> NodeListenError<T> {
    match err {
        ListenError::IoError(io) => ListenError::IoError(match io {
            IoError::Storage(e) => IoError::Storage(e),
            IoError::ConnSend(e) => IoError::ConnSend(e),
            IoError::ConnRecv(e) => IoError::ConnRecv(e),
            IoError::ConnCall(e) => IoError::ConnCall(e),
            IoError::BlobMismatch(e) => IoError::BlobMismatch(e),
        }),
        ListenError::TrySendError => ListenError::TrySendError,
    }
}

/// A Subduction connection seen as a [`KeyhiveConnection`]: outbound keyhive
/// messages go out as [`NodeMessage::Keyhive`] frames. Inbound frames arrive
/// through the [`NodeHandler`], so `recv` never resolves.
#[derive(Debug, Clone)]
pub struct NodeKeyhiveConn<T: NodeTransport> {
    auth: Authenticated<NodeConn<T>, Sendable>,
}

impl<T: NodeTransport> NodeKeyhiveConn<T> {
    /// Wrap an authenticated connection.
    #[must_use]
    pub const fn new(auth: Authenticated<NodeConn<T>, Sendable>) -> Self {
        Self { auth }
    }
}

impl<T: NodeTransport> KeyhiveConnection<Sendable> for NodeKeyhiveConn<T> {
    type SendError = NodeKeyhiveSendError<T::SendError>;
    type RecvError = core::convert::Infallible;
    type DisconnectError = core::convert::Infallible;

    fn peer_id(&self) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes(*self.auth.peer_id().as_bytes())
    }

    fn send(&self, message: SignedMessage) -> BoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            let frame = NodeMessage::Keyhive(KeyhiveMessage::from_signed(&message)?);
            <NodeConn<T> as Connection<Sendable, NodeMessage>>::send(self.auth.inner(), &frame)
                .await
                .map_err(NodeKeyhiveSendError::Transport)
        }
        .boxed()
    }

    fn recv(&self) -> BoxFuture<'_, Result<SignedMessage, Self::RecvError>> {
        futures::future::pending().boxed()
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectError>> {
        futures::future::ready(Ok(())).boxed()
    }
}

/// Errors from [`NodeKeyhiveConn::send`].
#[derive(Debug, thiserror::Error)]
pub enum NodeKeyhiveSendError<E: core::error::Error> {
    /// The signed message couldn't be encoded.
    #[error("encode signed message: {0}")]
    Encode(#[from] CborError),

    /// The transport failed to send the frame.
    #[error("send keyhive frame: {0}")]
    Transport(E),
}

/// Errors building a [`Node`].
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The Keyhive couldn't be generated. The runtime reports this as text.
    #[error("keyhive init: {0}")]
    Init(std::string::String),
}

/// A condition [`eventually`] waited on never held.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("did not converge: {0}")]
pub struct NotConverged(pub &'static str);
