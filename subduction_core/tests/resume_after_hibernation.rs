//! A connection survives the server process being replaced, via `ResumeToken`.
//!
//! Models Durable Object WebSocket hibernation: the client's socket stays
//! open while the server engine is evicted and later rebuilt from scratch.
//!
//! ```text
//! client ── client_half ═══ socket ═══ relay(gen N) ── server N
//! ```
//!
//! The "socket" is a channel pair the test owns. Each server generation gets
//! its own channel, pumped to the socket by a relay. Hibernating aborts the
//! relay (and the server's tasks), so the old engine can no longer see
//! traffic; the client never notices. The new engine attaches to the same
//! socket using only the persisted token bytes, without a new handshake.

#![allow(clippy::expect_used)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::{
        Authenticated,
        resume::{ResumeError, ResumeToken},
    },
    connection::test_utils::{ChannelClosed, ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    handshake::{self, Handshake, audience::Audience},
    nonce_cache::NonceCache,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    timestamp::TimestampSeconds,
    transport::{Transport, message::MessageTransport},
};
use subduction_crypto::{nonce::Nonce, signed::Signed, signer::memory::MemorySigner};
use testresult::TestResult;
use tokio::task::JoinHandle;

type Conn = MessageTransport<ChannelTransport>;

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

const NOW: TimestampSeconds = TimestampSeconds::new(1_000_000);
const TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(2_000);
const MAX_AGE: Duration = Duration::from_secs(3_600);

/// A running node plus the tasks that drive it, so it can be "evicted".
struct Node {
    sd: TestSubduction,
    tasks: Vec<JoinHandle<()>>,
}

impl Node {
    fn start(signer: MemorySigner) -> Self {
        let (sd, _handler, listener, manager) = SubductionBuilder::new()
            .signer(signer)
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, Conn>();
        let tasks = vec![
            tokio::spawn(async move {
                let _ = listener.await;
            }),
            tokio::spawn(async move {
                let _ = manager.await;
            }),
        ];
        Self { sd, tasks }
    }

    fn evict(self) {
        for task in self.tasks {
            task.abort();
        }
    }
}

/// Run the byte-level handshake over a [`ChannelTransport`].
struct Hs(ChannelTransport);

impl Handshake<Sendable> for Hs {
    type Error = ChannelClosed;

    fn send(&mut self, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), ChannelClosed>> {
        Box::pin(async move { self.0.send_bytes(&bytes).await })
    }

    fn recv(&mut self) -> BoxFuture<'_, Result<Vec<u8>, ChannelClosed>> {
        self.0.recv_bytes()
    }
}

/// Pump bytes both ways between the socket and one server generation.
fn relay(socket: ChannelTransport, generation: ChannelTransport) -> Vec<JoinHandle<()>> {
    let (s1, g1) = (socket.clone(), generation.clone());
    vec![
        tokio::spawn(async move {
            while let Ok(bytes) = s1.recv_bytes().await {
                if g1.send_bytes(&bytes).await.is_err() {
                    break;
                }
            }
        }),
        tokio::spawn(async move {
            while let Ok(bytes) = generation.recv_bytes().await {
                if socket.send_bytes(&bytes).await.is_err() {
                    break;
                }
            }
        }),
    ]
}

const fn doc(n: u8) -> SedimentreeId {
    SedimentreeId::new([n; 32])
}

async fn add(node: &TestSubduction, id: SedimentreeId, seed: u8) -> TestResult {
    node.add_commit(
        id,
        CommitId::new([seed; 32]),
        BTreeSet::new(),
        Blob::new(vec![seed; 16]),
    )
    .await?;
    Ok(())
}

/// Wait (bounded) for `node` to hold a commit for `id`. Sync is 1.5 round
/// trips: the requestor's final data push is fire-and-forget, so it can land
/// just after `sync_with_peer` returns.
async fn has_commit(node: &TestSubduction, id: SedimentreeId) -> bool {
    for _ in 0..100 {
        if node.get_commits(id).await.is_some_and(|c| !c.is_empty()) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test]
async fn connection_resumes_on_a_fresh_engine_without_rehandshake() -> TestResult {
    let client_signer = MemorySigner::from_bytes(&[1; 32]);
    let server_signer = MemorySigner::from_bytes(&[2; 32]);
    let client_peer = PeerId::from(client_signer.verifying_key());
    let server_peer = PeerId::from(server_signer.verifying_key());

    let (client_half, socket) = ChannelTransport::pair();

    // ── Generation 1: real handshake, then issue and persist a token ──────
    let (gen1_server, gen1_relay) = ChannelTransport::pair();
    let gen1_relays = relay(socket.clone(), gen1_relay);

    let server_signer_hs = server_signer.clone();
    let server_hs = tokio::spawn(async move {
        handshake::respond::<Sendable, _, _, _, _>(
            Hs(gen1_server),
            |hs, _peer| (MessageTransport::new(hs.0), ()),
            &server_signer_hs,
            &NonceCache::default(),
            server_peer,
            None,
            NOW,
            Duration::from_secs(600),
        )
        .await
    });
    let (client_auth, ()) = handshake::initiate::<Sendable, _, _, _, _>(
        Hs(client_half),
        |hs, _peer| (MessageTransport::new(hs.0), ()),
        &client_signer,
        Audience::known(server_peer),
        NOW,
        Nonce::random(),
    )
    .await?;
    let (server_auth, ()) = server_hs.await??;
    assert_eq!(server_auth.peer_id(), client_peer);

    let client = Node::start(client_signer);
    let server1 = Node::start(server_signer.clone());
    client.sd.add_connection(client_auth).await?;
    server1.sd.add_connection(server_auth.clone()).await?;

    // What a host would store next to the socket (e.g. serializeAttachment).
    let persisted: Vec<u8> = ResumeToken::issue(&server_auth, &server_signer, NOW)
        .await
        .into_bytes();

    add(&client.sd, doc(1), 1).await?;
    client
        .sd
        .sync_with_peer(&server_peer, doc(1), true, TIMEOUT)
        .await?;
    assert!(has_commit(&server1.sd, doc(1)).await, "baseline sync works");

    // ── Hibernate: the engine and its link to the socket disappear ────────
    for task in gen1_relays {
        task.abort();
    }
    server1.evict();
    drop(server_auth);

    // ── Generation 2: a fresh engine resumes from the persisted bytes ─────
    let (gen2_server, gen2_relay) = ChannelTransport::pair();
    let _gen2_relays = relay(socket, gen2_relay);

    let server2 = Node::start(server_signer.clone());
    let token = Signed::<ResumeToken>::try_decode(&persisted)?;
    let resumed = Authenticated::<Conn, Sendable>::resume(
        MessageTransport::new(gen2_server),
        &token,
        &server_signer.verifying_key(),
        TimestampSeconds::new(NOW.as_secs() + 60),
        MAX_AGE,
    )?;
    assert_eq!(resumed.peer_id(), client_peer);
    assert!(server2.sd.add_connection(resumed).await?, "new connection");

    // Client → server, over the client's original (never re-handshaken) link.
    add(&client.sd, doc(2), 2).await?;
    client
        .sd
        .sync_with_peer(&server_peer, doc(2), true, TIMEOUT)
        .await?;
    assert!(
        has_commit(&server2.sd, doc(2)).await,
        "resumed server receives"
    );

    // Server → client.
    add(&server2.sd, doc(3), 3).await?;
    server2
        .sd
        .sync_with_peer(&client_peer, doc(3), true, TIMEOUT)
        .await?;
    assert!(has_commit(&client.sd, doc(3)).await, "resumed server sends");

    Ok(())
}

#[tokio::test]
async fn resume_rejects_token_issued_by_a_different_node() -> TestResult {
    let server_signer = MemorySigner::from_bytes(&[2; 32]);
    let impostor_signer = MemorySigner::from_bytes(&[3; 32]);

    // The impostor completes a real handshake of its own and mints a token.
    let (a, b) = ChannelTransport::pair();
    let impostor = impostor_signer.clone();
    let impostor_peer = PeerId::from(impostor_signer.verifying_key());
    let responder = tokio::spawn(async move {
        handshake::respond::<Sendable, _, _, _, _>(
            Hs(b),
            |hs, _peer| (MessageTransport::new(hs.0), ()),
            &impostor,
            &NonceCache::default(),
            impostor_peer,
            None,
            NOW,
            Duration::from_secs(600),
        )
        .await
    });
    handshake::initiate::<Sendable, _, _, _, _>(
        Hs(a),
        |hs, _peer| (MessageTransport::new(hs.0), ()),
        &MemorySigner::from_bytes(&[1; 32]),
        Audience::known(impostor_peer),
        NOW,
        Nonce::random(),
    )
    .await?;
    let (auth, ()) = responder.await??;
    let token = ResumeToken::issue(&auth, &impostor_signer, NOW).await;

    let (conn, _) = ChannelTransport::pair();
    let result = Authenticated::<Conn, Sendable>::resume(
        MessageTransport::new(conn),
        &token,
        &server_signer.verifying_key(),
        NOW,
        MAX_AGE,
    );
    assert!(
        matches!(result, Err(ResumeError::WrongIssuer)),
        "a token from another key must not resume"
    );
    Ok(())
}
