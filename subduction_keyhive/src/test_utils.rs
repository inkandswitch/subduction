//! Test utilities for the keyhive protocol crate.

use alloc::{string::ToString, sync::Arc, vec, vec::Vec};

use async_channel::{Receiver, Sender};
use async_lock::Mutex;
use future_form::Local;
use futures::{future::LocalBoxFuture, FutureExt};
use keyhive_core::{
    access::Access,
    crypto::signer::memory::MemorySigner,
    keyhive::Keyhive,
    listener::no_listener::NoListener,
    principal::{group::id::GroupId, membered::Membered},
    store::ciphertext::memory::MemoryCiphertextStore,
};
use rand::rngs::OsRng;

use crate::{
    connection::KeyhiveConnection, peer_id::KeyhivePeerId, protocol::KeyhiveProtocol,
    signed_message::SignedMessage, storage::MemoryKeyhiveStorage,
};

/// Type alias for the simple keyhive type used in tests.
pub(crate) type SimpleKeyhive = Keyhive<
    MemorySigner,
    [u8; 32],
    Vec<u8>,
    MemoryCiphertextStore<[u8; 32], Vec<u8>>,
    NoListener,
    OsRng,
>;

/// An in-memory connection using async channels for testing.
#[derive(Debug, Clone)]
pub(crate) struct ChannelConnection {
    remote_peer_id: KeyhivePeerId,
    pub(crate) outbound_tx: Sender<SignedMessage>,
    pub(crate) inbound_rx: Receiver<SignedMessage>,
}

/// Error type for channel connection operations.
#[derive(Debug, Clone)]
pub(crate) struct ChannelError(pub alloc::string::String);

impl core::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "channel error: {}", self.0)
    }
}

impl core::error::Error for ChannelError {}

impl ChannelConnection {
    /// Create a new channel connection with the given channels.
    pub(crate) const fn new(
        remote_peer_id: KeyhivePeerId,
        outbound_tx: Sender<SignedMessage>,
        inbound_rx: Receiver<SignedMessage>,
    ) -> Self {
        Self {
            remote_peer_id,
            outbound_tx,
            inbound_rx,
        }
    }
}

/// Create a pair of connected channel connections.
///
/// Returns `(conn_a_to_b, conn_b_to_a)` where:
/// * `conn_a_to_b` sends messages from A that B receives
/// * `conn_b_to_a` sends messages from B that A receives
pub(crate) fn create_channel_pair(
    peer_a: KeyhivePeerId,
    peer_b: &KeyhivePeerId,
) -> (ChannelConnection, ChannelConnection) {
    let (a_to_b_tx, a_to_b_rx) = async_channel::unbounded();
    let (b_to_a_tx, b_to_a_rx) = async_channel::unbounded();

    let conn_a = ChannelConnection::new(peer_b.clone(), a_to_b_tx, b_to_a_rx);
    let conn_b = ChannelConnection::new(peer_a, b_to_a_tx, a_to_b_rx);

    (conn_a, conn_b)
}

impl KeyhiveConnection<Local> for ChannelConnection {
    type SendError = ChannelError;
    type RecvError = ChannelError;
    type DisconnectError = ChannelError;

    fn peer_id(&self) -> KeyhivePeerId {
        self.remote_peer_id.clone()
    }

    fn send(&self, message: SignedMessage) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let tx = self.outbound_tx.clone();
        async move {
            tx.send(message)
                .await
                .map_err(|e| ChannelError(e.to_string()))
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<SignedMessage, Self::RecvError>> {
        let rx = self.inbound_rx.clone();
        async move { rx.recv().await.map_err(|e| ChannelError(e.to_string())) }.boxed_local()
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectError>> {
        self.outbound_tx.close();
        self.inbound_rx.close();
        async { Ok(()) }.boxed_local()
    }
}

/// Create a simple keyhive instance for testing.
pub(crate) async fn make_keyhive() -> SimpleKeyhive {
    let mut csprng = OsRng;
    let sk = MemorySigner::generate(&mut csprng);
    Keyhive::generate(sk, MemoryCiphertextStore::new(), NoListener, csprng)
        .await
        .expect("failed to create keyhive")
}

/// Get the peer ID for a keyhive instance.
pub(crate) fn keyhive_peer_id(keyhive: &SimpleKeyhive) -> KeyhivePeerId {
    let id: keyhive_core::principal::identifier::Identifier = keyhive.id().into();
    KeyhivePeerId::from_bytes(id.to_bytes())
}

/// Serialize a contact card to CBOR bytes.
pub(crate) fn serialize_contact_card(
    contact_card: &keyhive_core::contact_card::ContactCard,
) -> Vec<u8> {
    let mut buf = Vec::new();
    ciborium::into_writer(contact_card, &mut buf).expect("failed to serialize contact card");
    buf
}

/// Type alias for the test protocol type.
pub(crate) type TestProtocol = KeyhiveProtocol<
    MemorySigner,
    [u8; 32],
    Vec<u8>,
    MemoryCiphertextStore<[u8; 32], Vec<u8>>,
    NoListener,
    OsRng,
    ChannelConnection,
    MemoryKeyhiveStorage,
    Local,
>;

/// Create a test protocol with a shared keyhive reference.
///
/// Unlike `make_protocol()` in the protocol tests, this does NOT clone the
/// keyhive. The returned `Arc<Mutex<SimpleKeyhive>>` is the same one the
/// protocol uses, so mutations made through the Arc are visible to the
/// protocol.
pub(crate) async fn make_protocol_with_shared_keyhive(
    keyhive: SimpleKeyhive,
) -> (
    TestProtocol,
    Arc<Mutex<SimpleKeyhive>>,
    MemoryKeyhiveStorage,
) {
    let peer_id = keyhive_peer_id(&keyhive);
    let cc = keyhive
        .contact_card()
        .await
        .expect("failed to get contact card");
    let cc_bytes = serialize_contact_card(&cc);
    let storage = MemoryKeyhiveStorage::new();
    let shared = Arc::new(Mutex::new(keyhive));
    let protocol = TestProtocol::new(shared.clone(), storage.clone(), peer_id, cc_bytes);
    (protocol, shared, storage)
}

/// Run a complete sync round from initiator to responder.
///
/// Executes the three-phase sync protocol:
/// 1. Initiator sends `SyncRequest`
/// 2. Responder replies with `SyncResponse`
/// 3. Initiator sends `SyncOps` (if any were requested)
///
/// Returns the number of messages exchanged.
pub(crate) async fn run_sync_round(
    initiator_proto: &TestProtocol,
    responder_proto: &TestProtocol,
    initiator_id: &KeyhivePeerId,
    responder_id: &KeyhivePeerId,
    initiator_conn: &ChannelConnection,
    responder_conn: &ChannelConnection,
) -> usize {
    let mut messages = 0;

    // 1. Initiator sends SyncRequest
    initiator_proto
        .sync_keyhive(Some(responder_id))
        .await
        .expect("initiator sync_keyhive failed");

    // 2. Forward SyncRequest to responder
    let sync_request = responder_conn
        .inbound_rx
        .recv()
        .await
        .expect("failed to receive sync request");
    responder_proto
        .handle_message(initiator_id, sync_request)
        .await
        .expect("responder failed to handle sync request");
    messages += 1;

    // 3. Forward SyncResponse to initiator
    let sync_response = initiator_conn
        .inbound_rx
        .recv()
        .await
        .expect("failed to receive sync response");
    initiator_proto
        .handle_message(responder_id, sync_response)
        .await
        .expect("initiator failed to handle sync response");
    messages += 1;

    // 4. If initiator sent SyncOps, forward them
    if let Ok(sync_ops) = responder_conn.inbound_rx.try_recv() {
        responder_proto
            .handle_message(initiator_id, sync_ops)
            .await
            .expect("responder failed to handle sync ops");
        messages += 1;
    }

    messages
}

/// Harness for a two-peer test setup with shared keyhives.
pub(crate) struct TwoPeerHarness {
    /// Alice's protocol handler.
    pub alice_proto: TestProtocol,
    /// Bob's protocol handler.
    pub bob_proto: TestProtocol,
    /// Shared reference to Alice's keyhive.
    pub alice_kh: Arc<Mutex<SimpleKeyhive>>,
    /// Shared reference to Bob's keyhive.
    pub bob_kh: Arc<Mutex<SimpleKeyhive>>,
    /// Alice's peer ID.
    pub alice_id: KeyhivePeerId,
    /// Bob's peer ID.
    pub bob_id: KeyhivePeerId,
    /// Alice's channel connection (her end of the pair).
    pub alice_conn: ChannelConnection,
    /// Bob's channel connection (his end of the pair).
    pub bob_conn: ChannelConnection,
}

/// Exchange contact cards between two fresh keyhives and set up protocols
/// and channel pairs. Returns a [`TwoPeerHarness`] with everything wired up.
pub(crate) async fn exchange_contact_cards_and_setup() -> TwoPeerHarness {
    let alice_keyhive = make_keyhive().await;
    let bob_keyhive = make_keyhive().await;

    // Exchange contact cards at the keyhive level
    let alice_cc = alice_keyhive
        .contact_card()
        .await
        .expect("alice contact card");
    let bob_cc = bob_keyhive.contact_card().await.expect("bob contact card");
    alice_keyhive
        .receive_contact_card(&bob_cc)
        .await
        .expect("alice receive bob cc");
    bob_keyhive
        .receive_contact_card(&alice_cc)
        .await
        .expect("bob receive alice cc");

    let alice_id = keyhive_peer_id(&alice_keyhive);
    let bob_id = keyhive_peer_id(&bob_keyhive);

    let (alice_proto, alice_kh, _) = make_protocol_with_shared_keyhive(alice_keyhive).await;
    let (bob_proto, bob_kh, _) = make_protocol_with_shared_keyhive(bob_keyhive).await;

    let (alice_conn, bob_conn) = create_channel_pair(alice_id.clone(), &bob_id);

    alice_proto
        .add_peer(bob_id.clone(), alice_conn.clone())
        .await;
    bob_proto.add_peer(alice_id.clone(), bob_conn.clone()).await;

    TwoPeerHarness {
        alice_proto,
        bob_proto,
        alice_kh,
        bob_kh,
        alice_id,
        bob_id,
        alice_conn,
        bob_conn,
    }
}

/// Exchange contact cards between every pair of keyhives.
pub(crate) async fn exchange_all_contact_cards(keyhives: &[&SimpleKeyhive]) {
    let mut cards = Vec::new();
    for kh in keyhives {
        cards.push(kh.contact_card().await.expect("contact_card"));
    }
    for (i, kh) in keyhives.iter().enumerate() {
        for (j, cc) in cards.iter().enumerate() {
            if i != j {
                kh.receive_contact_card(cc)
                    .await
                    .expect("receive_contact_card");
            }
        }
    }
}

/// Create a group and add the given peers as Read members.
pub(crate) async fn create_group_with_read_members(
    kh: &SimpleKeyhive,
    member_ids: &[&KeyhivePeerId],
) -> GroupId {
    let group = kh.generate_group(vec![]).await.expect("generate_group");
    let group_id = group.lock().await.group_id();
    for member_id in member_ids {
        let identifier = member_id.to_identifier().expect("to_identifier");
        let agent = kh.get_agent(identifier).await.expect("get_agent");
        kh.add_member(
            agent,
            &Membered::Group(group_id, group.clone()),
            Access::Read,
            &[],
        )
        .await
        .expect("add_member");
    }
    group_id
}
