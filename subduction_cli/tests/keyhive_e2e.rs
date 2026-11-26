//! End-to-end: keyhive authorization in a running `subduction_cli` server.
//!
//! Spawns a server with keyhive auth (the default) and connects in-process
//! keyhive nodes to it over HTTP long-poll:
//!
//! ```text
//!   alice ──┐   HTTP long-poll    ┌───────────────────────┐
//!           ├────────────────────▶│ subduction_cli server │
//!   bob ────┘   SUM + SUK frames  └───────────────────────┘
//! ```
//!
//! The server learns membership only from `SUK` frames, so this covers the
//! binary's whole keyhive stack: runtime setup, frame routing, peer
//! registration, and the authorization policy (legacy IDs included). The
//! server can't be inspected directly; what it allows shows in whether it
//! authorizes a sync and in what the clients receive.

use std::{
    collections::BTreeSet,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use ed25519_dalek::SigningKey;
use future_form::Sendable;
use keyhive_core::{
    contact_card::ContactCard,
    principal::{document::id::DocumentId, individual::id::IndividualId},
};
use keyhive_crypto::identifier::Identifier;
use keyline::power::Power;
use nonempty::nonempty;
use reqwest::header::HeaderValue;
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::{stats::SyncStats, test_utils::TokioSpawn},
    effects::config::Config,
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    timestamp::TimestampSeconds,
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_http_longpoll::{
    client::{ConnectResult, HttpLongPollClient},
    http_client::reqwest_client::ReqwestHttpClient,
    transport::HttpLongPollTransport,
};
use subduction_keyhive::{
    change_id::ChangeId,
    protocol::is_known_agent,
    test_utils::node::{CONVERGENCE_TIMEOUT, Node, eventually},
};
use subduction_websocket::timeout::FuturesTimerTimeout;
use testresult::TestResult;

#[path = "common/server_ready.rs"]
mod server_ready;

type LpNode = Node<HttpLongPollTransport>;

type LpConn = MessageTransport<HttpLongPollTransport>;

/// A client without keyhive's effects: in-memory storage, open policy.
type OpenEffects = Config<MemoryStorage, OpenPolicy, MemorySigner, FuturesTimerTimeout, TokioSpawn>;

/// A client without keyhive, as legacy documents' users run.
type OpenClient = Arc<
    Subduction<
        'static,
        Sendable,
        OpenEffects,
        LpConn,
        SyncHandler<Sendable, OpenEffects, LpConn, CountLeadingZeroBytes>,
        CountLeadingZeroBytes,
    >,
>;

const SERVICE_NAME: &str = "keyhive-e2e-test";
const SERVER_KEY_SEED: [u8; 32] = [0x5e; 32];
const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
/// Well under [`CONVERGENCE_TIMEOUT`]: most syncs run inside [`until`], and a
/// lost long-poll response must leave time to retry.
const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(3_000);

/// Alice publishes a document through the server, admits Bob, then revokes
/// him. Every change reaches the server over `SUK`.
#[tokio::test]
async fn the_server_enforces_membership_it_learns_over_suk() -> TestResult {
    let server = Server::spawn().await?;
    let (alice, bob) = (LpNode::new(0x11).await?, LpNode::new(0x12).await?);
    let (alice, bob) = (&alice, &bob);
    let server_peer = server.connect(alice).await?;
    server.connect(bob).await?;
    let server_id = Identifier::from_bytes(*server_peer.as_bytes())?;

    for client in [alice, bob] {
        client.sync_keyhive().await?;
        until("client learns the server", move || async move {
            is_known_agent(&client.keyhive, server_id)
                .await
                .unwrap_or(false)
        })
        .await?;
    }

    // Bob's invite reaches Alice out of band.
    let card = bob.keyhive.generate_contact_card().await?;
    alice.keyhive.receive_contact_card(&card).await?;

    let doc = alice
        .keyhive
        .generate_doc(vec![], nonempty![change(1)])
        .await?;
    alice
        .keyhive
        .add_member(server_id, doc, Power::Relay)
        .await?;
    let id = tree(doc);
    commit(alice, id, 1).await?;

    let (authorized, _) = sync_tree(alice, server_peer, id).await?;
    assert!(!authorized, "the server has not heard of the document yet");

    alice.sync_keyhive().await?;
    until("the server authorizes alice", move || async move {
        sync_tree(alice, server_peer, id)
            .await
            .is_ok_and(|(authorized, _)| authorized)
    })
    .await?;

    // Bob's own policy won't sync a document he hasn't heard of, so a client
    // without keyhive stands in for a non-member: its request reaches the
    // server.
    let (mallory, _) = server.connect_open(0x13).await?;
    let (authorized, _) = open_push(&mallory, server_peer, id, 7).await?;
    assert!(!authorized, "a non-member is refused");

    commit(bob, id, 2).await?;

    alice
        .keyhive
        .add_member(bob.identifier(), doc, Power::Read)
        .await?;
    alice.sync_keyhive().await?;
    // Bob learns the membership from the server, which relays it. His own
    // policy needs it to accept the document's commits from the server.
    until_synced(
        bob,
        "bob learns the server relays the document",
        move || async move {
            bob.keyhive
                .access_for_doc(server_id, doc)
                .await
                .is_ok_and(|p| p == Some(Power::Relay))
        },
    )
    .await?;

    // The server relays CGKA ops too, so Bob can decrypt what Alice seals.
    let sealed = alice
        .keyhive
        .try_encrypt_content(doc, &change(9), &[], b"for readers only")
        .await?;
    alice.sync_keyhive().await?;
    let sealed = sealed.encrypted_content();
    until_synced(bob, "bob decrypts through the server", move || async move {
        bob.keyhive
            .try_decrypt_content(doc, sealed)
            .await
            .is_ok_and(|opened| opened.plaintext == b"for readers only")
    })
    .await?;

    until("bob receives alice's commit", move || async move {
        drop(sync_tree(bob, server_peer, id).await);
        commit_count(bob, id).await == 2
    })
    .await?;
    until("the server stores bob's commit", move || async move {
        drop(sync_tree(alice, server_peer, id).await);
        commit_count(alice, id).await == 2
    })
    .await?;

    alice
        .keyhive
        .revoke_member(bob.identifier(), true, doc)
        .await?;
    alice.sync_keyhive().await?;
    until("the server stops authorizing bob", move || async move {
        sync_tree(bob, server_peer, id)
            .await
            .is_ok_and(|(authorized, _)| !authorized)
    })
    .await?;

    commit(alice, id, 3).await?;
    let (authorized, stats) = sync_tree(alice, server_peer, id).await?;
    assert!(
        authorized && stats.commits_sent == 1,
        "alice still publishes"
    );
    let (authorized, _) = sync_tree(bob, server_peer, id).await?;
    assert!(!authorized);
    assert_eq!(commit_count(bob, id).await, 2, "bob misses the new commit");

    server.stop()
}

/// Two peered servers: Alice uses one, Bob the other. What Alice grants or
/// revokes through her server reaches Bob's only over the servers' own
/// connection.
///
/// ```text
///   alice ──▶ server A ◀── ws ── server B ◀── bob
/// ```
#[tokio::test]
#[ignore = "servers never start a keyhive sync with each other, so B never learns the membership"]
async fn a_peered_server_follows_membership_from_its_peer() -> TestResult {
    let server_a = Server::spawn_peered([0x5a; 32], None).await?;
    let server_b = Server::spawn_peered([0x5b; 32], Some(&server_a)).await?;
    let (alice, bob) = (LpNode::new(0x31).await?, LpNode::new(0x32).await?);
    let (alice, bob) = (&alice, &bob);
    let peer_a = server_a.connect(alice).await?;
    let peer_b = server_b.connect(bob).await?;
    let id_a = Identifier::from_bytes(*peer_a.as_bytes())?;
    let id_b = Identifier::from_bytes(*peer_b.as_bytes())?;

    for (client, server_id) in [(alice, id_a), (bob, id_b)] {
        client.sync_keyhive().await?;
        until("client learns its server", move || async move {
            is_known_agent(&client.keyhive, server_id)
                .await
                .unwrap_or(false)
        })
        .await?;
    }

    // Alice knows Bob and his server out of band.
    alice
        .keyhive
        .receive_contact_card(&server_b.contact_card().await?)
        .await?;
    let card = bob.keyhive.generate_contact_card().await?;
    alice.keyhive.receive_contact_card(&card).await?;

    let doc = alice
        .keyhive
        .generate_doc(vec![], nonempty![change(1)])
        .await?;
    for (member, power) in [
        (id_a, Power::Relay),
        (id_b, Power::Relay),
        (bob.identifier(), Power::Read),
    ] {
        alice.keyhive.add_member(member, doc, power).await?;
    }
    let id = tree(doc);
    commit(alice, id, 1).await?;
    alice.sync_keyhive().await?;
    until("server A stores alice's commit", move || async move {
        sync_tree(alice, peer_a, id)
            .await
            .is_ok_and(|(authorized, _)| authorized)
    })
    .await?;

    until_synced(
        bob,
        "bob learns server B relays the document",
        move || async move {
            bob.keyhive
                .access_for_doc(id_b, doc)
                .await
                .is_ok_and(|p| p == Some(Power::Relay))
        },
    )
    .await?;
    until(
        "bob gets alice's commit through server B",
        move || async move {
            drop(sync_tree(bob, peer_b, id).await);
            commit_count(bob, id).await == 1
        },
    )
    .await?;

    alice
        .keyhive
        .revoke_member(bob.identifier(), true, doc)
        .await?;
    alice.sync_keyhive().await?;
    until("server B stops authorizing bob", move || async move {
        sync_tree(bob, peer_b, id)
            .await
            .is_ok_and(|(authorized, _)| !authorized)
    })
    .await?;

    server_b.stop()?;
    server_a.stop()
}

/// Legacy document IDs (zero-padded) are open to anyone: the server stores
/// and serves them without any membership. Their users predate keyhive, so
/// the clients here run without it.
#[tokio::test]
async fn legacy_documents_need_no_membership() -> TestResult {
    let server = Server::spawn().await?;
    let (carol, server_peer) = server.connect_open(0x21).await?;
    let (dave, _) = server.connect_open(0x22).await?;

    let mut legacy = [0; 32];
    legacy[..16].copy_from_slice(&[7; 16]);
    let id = SedimentreeId::new(legacy);

    let (authorized, stats) = open_push(&carol, server_peer, id, 1).await?;
    assert!(
        authorized && stats.commits_sent == 1,
        "carol pushes her commit"
    );

    let dave = &dave;
    until("dave gets carol's commit", move || async move {
        let authorized = dave
            .sync_with_peer(&server_peer, id, false, SYNC_TIMEOUT)
            .await
            .is_ok_and(|(authorized, _, _)| authorized);
        authorized && dave.get_commits(id).await.is_some_and(|c| c.len() == 1)
    })
    .await?;

    server.stop()
}

#[tokio::test]
async fn the_server_serves_its_own_contact_card() -> TestResult {
    let server = Server::spawn().await?;

    let resp = reqwest::get(format!("{}/keyhive/contact-card", server.base_url))
        .await?
        .error_for_status()?;
    let cors = resp.headers().get("access-control-allow-origin").cloned();
    let card = ContactCard::from_bytes(&resp.bytes().await?)?;

    assert_eq!(cors.as_ref().map(HeaderValue::as_bytes), Some(&b"*"[..]));
    assert_eq!(
        card.id(),
        IndividualId::from(&SigningKey::from_bytes(&SERVER_KEY_SEED))
    );
    server.stop()
}

/// A `subduction_cli server` process with keyhive auth, killed on drop.
struct Server {
    child: Child,
    base_url: String,
    port: u16,
    _data_dir: tempfile::TempDir,
}

impl Server {
    async fn spawn() -> TestResult<Self> {
        Self::spawn_peered(SERVER_KEY_SEED, None).await
    }

    /// Spawn a server with signing key `key_seed` that dials `peer` over
    /// WebSocket on startup.
    async fn spawn_peered(key_seed: [u8; 32], peer: Option<&Self>) -> TestResult<Self> {
        let data_dir = tempfile::tempdir()?;
        let ready_file = data_dir.path().join("ready");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_subduction_cli"));
        cmd.args(["server", "--socket", "127.0.0.1:0", "--key-seed"])
            .arg(hex::encode(key_seed))
            .args(["--service-name", SERVICE_NAME, "--data-dir"])
            .arg(data_dir.path())
            .arg("--ready-file")
            .arg(&ready_file);
        if let Some(peer) = peer {
            cmd.arg("--ws-peer")
                .arg(format!("ws://127.0.0.1:{}", peer.port));
        }
        let child = cmd
            .env("RUST_LOG", "warn")
            .stderr(Stdio::inherit())
            .stdout(Stdio::null())
            .spawn()?;
        let mut server = Self {
            child,
            base_url: String::new(),
            port: 0,
            _data_dir: data_dir,
        };
        let ready = server_ready::wait_for(&ready_file, SERVER_STARTUP_TIMEOUT)
            .await
            .ok_or("the server never became ready")?;
        let port: u16 = ready
            .get("port")
            .ok_or("no port in the ready file")?
            .parse()?;
        server.base_url = format!("http://127.0.0.1:{port}");
        server.port = port;
        Ok(server)
    }

    /// The server's contact card, from `GET /keyhive/contact-card`.
    async fn contact_card(&self) -> TestResult<ContactCard> {
        let resp = reqwest::get(format!("{}/keyhive/contact-card", self.base_url))
            .await?
            .error_for_status()?;
        Ok(ContactCard::from_bytes(&resp.bytes().await?)?)
    }

    /// Connect `node` to the server over HTTP long-poll, returning the
    /// server's peer ID.
    async fn connect(&self, node: &LpNode) -> TestResult<PeerId> {
        let auth = self.handshake(node.signer()).await?;
        let server_peer = auth.peer_id();
        node.attach(auth.map(MessageTransport::new)).await?;
        Ok(server_peer)
    }

    /// Connect a keyhive-free client with signing key `[seed; 32]`.
    async fn connect_open(&self, seed: u8) -> TestResult<(OpenClient, PeerId)> {
        let signer = MemorySigner::from_bytes(&[seed; 32]);
        let (client, _handler, listener, manager) = SubductionBuilder::new()
            .signer(signer.clone())
            .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(FuturesTimerTimeout)
            .build::<Sendable, LpConn>();
        tokio::spawn(listener);
        tokio::spawn(manager);

        let auth = self.handshake(&signer).await?;
        let server_peer = auth.peer_id();
        client
            .add_connection(auth.map(MessageTransport::new))
            .await?;
        Ok((client, server_peer))
    }

    /// Open an HTTP long-poll session, driving it in the background.
    async fn handshake(
        &self,
        signer: &MemorySigner,
    ) -> TestResult<Authenticated<HttpLongPollTransport, Sendable>> {
        let client = HttpLongPollClient::new(&self.base_url, ReqwestHttpClient::new());
        let ConnectResult {
            authenticated,
            poll_task,
            send_task,
            ..
        } = client
            .connect_discover(signer, SERVICE_NAME, TimestampSeconds::now())
            .await?;
        tokio::spawn(poll_task);
        tokio::spawn(send_task);
        Ok(authenticated)
    }

    fn stop(mut self) -> TestResult {
        self.child.kill()?;
        self.child.wait()?;
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        drop(self.child.kill());
        drop(self.child.wait());
    }
}

/// Poll `check` until it holds.
async fn until<F: FnMut() -> Fut, Fut: Future<Output = bool>>(
    what: &'static str,
    check: F,
) -> TestResult {
    eventually(what, CONVERGENCE_TIMEOUT, check).await?;
    Ok(())
}

/// Poll `check` until it holds, running `node`'s keyhive sync before each
/// try: what it waits for may not have reached the server yet.
async fn until_synced<F: FnMut() -> Fut, Fut: Future<Output = bool>>(
    node: &LpNode,
    what: &'static str,
    mut check: F,
) -> TestResult {
    until(what, || {
        let checked = check();
        async move {
            drop(node.sync_keyhive().await);
            tokio::time::sleep(Duration::from_millis(200)).await;
            checked.await
        }
    })
    .await
}

async fn commit(node: &LpNode, id: SedimentreeId, seed: u8) -> TestResult {
    node.subduction
        .add_commit(
            id,
            CommitId::new([seed; 32]),
            BTreeSet::new(),
            Blob::new(vec![seed; 8]),
        )
        .await?;
    Ok(())
}

/// Sync one tree with the server, both ways: whether the server authorized
/// it, and what moved.
async fn sync_tree(
    node: &LpNode,
    server: PeerId,
    id: SedimentreeId,
) -> TestResult<(bool, SyncStats)> {
    let (authorized, stats, _errors) = node
        .subduction
        .sync_with_peer(&server, id, false, SYNC_TIMEOUT)
        .await?;
    Ok((authorized, stats))
}

/// Author a commit of `id` on a keyhive-free `client` and sync the tree with
/// the server: whether the server authorized it, and what moved.
async fn open_push(
    client: &OpenClient,
    server: PeerId,
    id: SedimentreeId,
    seed: u8,
) -> TestResult<(bool, SyncStats)> {
    client
        .add_commit(
            id,
            CommitId::new([seed; 32]),
            BTreeSet::new(),
            Blob::new(vec![seed; 8]),
        )
        .await?;
    let (authorized, stats, _errors) = client
        .sync_with_peer(&server, id, false, SYNC_TIMEOUT)
        .await?;
    Ok((authorized, stats))
}

async fn commit_count(node: &LpNode, id: SedimentreeId) -> usize {
    node.subduction.get_commits(id).await.map_or(0, |c| c.len())
}

const fn change(seed: u8) -> ChangeId {
    ChangeId::new(CommitId::new([seed; 32]))
}

fn tree(doc: DocumentId) -> SedimentreeId {
    SedimentreeId::new(Identifier::from(doc).to_bytes())
}
