//! Multithreaded (`Sendable` future form) stress tests.
//!
//! Each test runs on a multi-thread tokio runtime so we exercise the
//! actual concurrency paths used by native deployments (`subduction_cli`,
//! WebSocket / iroh / longpoll transports). These are correctness
//! tests under high load: the goal is to catch deadlocks, races, leaks,
//! and missed cleanup, not to measure throughput. Numerical
//! bottleneck-hunting lives in the matching benchmarks.
//!
//! Scenarios:
//!
//! - `many_peers_fan_in` — one server, many concurrently-connected
//!   clients, each syncing one document with the server.
//! - `reconnect_storm` — peers connect, sync, disconnect rapidly, then
//!   reconnect; verifies the connection pool doesn't grow unbounded.
//! - `hostile_peer_burst` — a peer sends a burst of malformed
//!   `LooseCommit` payloads (blob mismatch). Verifies rate-limit /
//!   backpressure / cleanup don't deadlock the receiver.
//! - `mixed_workload` — fan-in + fan-out + reconnect concurrently.
//!   Worst-case "do all the things at once" to surface inter-feature
//!   races.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use future_form::Sendable;
use subduction_core::{
    authenticated::Authenticated,
    connection::{message::SyncMessage, test_utils::ChannelTransport},
    peer::id::PeerId,
    remote_heads::RemoteHeads,
    transport::message::MessageTransport,
};

#[path = "common.rs"]
mod common;

use common::{
    Conn, SETTLE, blob, commit_id, connect_sendable_pair, make_sendable_node,
    populate_linear_chain_sendable, sed_id, signer,
};

// ─── Fan-in: many clients, one server ───────────────────────────────────────

/// Many clients fan-in to one server. Each client owns a unique
/// document, populates a small chain, and syncs concurrently.
///
/// What this catches: deadlocks under shard-lock contention (every
/// client targets a different sedimentree, so shards get fanned out
/// across the `ShardedMap`); cleanup races where two clients hit the
/// same shard at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_peers_fan_in() {
    const NUM_CLIENTS: u32 = 50;
    const COMMITS_PER_CLIENT: u32 = 5;
    const BLOB_SIZE: usize = 64;
    const SYNC_TIMEOUT: Duration = Duration::from_secs(30);

    let server = make_sendable_node(0);
    let server_seed = 0u32;

    let mut handles = Vec::with_capacity(NUM_CLIENTS as usize);
    for client_idx in 0..NUM_CLIENTS {
        let client_seed = client_idx + 1;
        let client = make_sendable_node(client_seed);
        connect_sendable_pair(&client, client_seed, &server, server_seed)
            .await
            .expect("connect");

        let id = sed_id(client_idx);
        populate_linear_chain_sendable(&client, id, client_idx, COMMITS_PER_CLIENT, BLOB_SIZE)
            .await
            .expect("populate");

        let server_peer_id = PeerId::from(signer(server_seed).verifying_key());
        handles.push(tokio::spawn(async move {
            let result = client
                .full_sync_with_peer(&server_peer_id, true, Some(SYNC_TIMEOUT))
                .await;
            assert!(result.0, "client {client_idx}: full_sync should succeed");
            assert!(
                result.2.is_empty(),
                "client {client_idx}: per-conn errors: {:?}",
                result.2
            );
            assert!(
                result.3.is_empty(),
                "client {client_idx}: per-tree IO errors: {:?}",
                result.3
            );
            client_idx
        }));
    }

    for h in handles {
        h.await.expect("client task did not panic");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Every client's data should be on the server.
    for client_idx in 0..NUM_CLIENTS {
        let id = sed_id(client_idx);
        let server_commits = server
            .get_commits(id)
            .await
            .unwrap_or_else(|| panic!("server missing tree for client_idx={client_idx}"));
        assert_eq!(
            server_commits.len() as u32,
            COMMITS_PER_CLIENT,
            "server should have all commits for client_idx={client_idx}"
        );
    }
}

// ─── Reconnect storm ────────────────────────────────────────────────────────

/// Rapid connect/sync/disconnect cycles between the same peer pair.
///
/// What this catches: connection-pool growth (the issue we saw in
/// production logs where a half-open connection produced a wall of
/// "sender task stopped" warnings); per-cycle resource leaks; multiplexer
/// table cleanup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_storm() {
    const NUM_CYCLES: u32 = 40;
    const SYNC_TIMEOUT: Duration = Duration::from_secs(10);

    let alice = make_sendable_node(100);
    let bob = make_sendable_node(101);
    let bob_peer_id = PeerId::from(signer(101).verifying_key());
    let id = sed_id(0);

    populate_linear_chain_sendable(&alice, id, 0, 3, 32)
        .await
        .expect("populate");

    for cycle in 0..NUM_CYCLES {
        connect_sendable_pair(&alice, 100, &bob, 101)
            .await
            .expect("connect cycle {cycle}");
        tokio::time::sleep(Duration::from_millis(5)).await;

        let result = alice
            .full_sync_with_peer(&bob_peer_id, true, Some(SYNC_TIMEOUT))
            .await;
        assert!(result.0, "sync should succeed on cycle {cycle}");

        // Disconnect *both* sides — disconnect_from_peer is symmetric in
        // intent but each side keeps its own connection table, so we
        // hit both to clear out the cross-references.
        let _ = alice.disconnect_from_peer(&bob_peer_id).await;
        let _ = bob
            .disconnect_from_peer(&PeerId::from(signer(100).verifying_key()))
            .await;

        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // After all cycles, neither node should be retaining stale
    // connections. We assert by checking the connected-peer-id set is
    // empty after the final disconnect.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let alice_peers = alice.connected_peer_ids().await;
    let bob_peers = bob.connected_peer_ids().await;
    assert!(
        alice_peers.is_empty(),
        "alice still tracks peers after disconnect: {alice_peers:?}"
    );
    assert!(
        bob_peers.is_empty(),
        "bob still tracks peers after disconnect: {bob_peers:?}"
    );
}

// ─── Hostile peer: malformed message burst ──────────────────────────────────

/// A peer streams `LooseCommit` messages with mismatched blobs as fast as
/// possible. The receiver must:
/// - reject every malformed message (no data is stored),
/// - not deadlock under the load,
/// - keep accepting messages from other (well-behaved) peers.
///
/// What this catches: backpressure stalls in the `recv_commit` reject
/// path, mishandled errors that close the wrong connection, and any
/// global state corruption from invalid input.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hostile_peer_burst() {
    const BURST_SIZE: u32 = 200;

    let receiver = make_sendable_node(200);
    let receiver_peer_id = PeerId::from(signer(200).verifying_key());

    // Build a raw channel-pair connection for the hostile peer. Bypassing
    // `connect_sendable_pair` because we don't want a Subduction on the
    // hostile side — we just want to inject raw `SyncMessage`s.
    let (transport_attacker, transport_receiver) = ChannelTransport::pair();
    let attacker_signer = signer(201);
    let attacker_peer_id = PeerId::from(attacker_signer.verifying_key());

    // The Subduction-side connection handle.
    let receiver_conn = MessageTransport::new(transport_receiver);
    let receiver_auth: Authenticated<Conn, Sendable> =
        Authenticated::new_for_test(receiver_conn, attacker_peer_id);
    receiver
        .add_connection(receiver_auth)
        .await
        .expect("add_connection");

    tokio::time::sleep(SETTLE).await;

    // Burst a stream of LooseCommit messages whose blobs don't match the
    // claimed metadata. We have to drop into raw bytes because the wire
    // protocol expects a signed payload — but the receiver will reject
    // these on signature failure long before reaching the blob check, so
    // we use *this peer's* signing key to fool the signature-verify step
    // and trigger the blob-mismatch check specifically.
    use sedimentree_core::{blob::BlobMeta, loose_commit::LooseCommit};
    use subduction_core::transport::Transport;
    use subduction_crypto::signed::Signed;

    let id = sed_id(42);

    let mut sent = 0u32;
    for i in 0..BURST_SIZE {
        // Pick a "claimed" blob for the metadata...
        let claimed = blob(i, 64);
        let blob_meta = BlobMeta::new(&claimed);
        let head = commit_id(0xBEEF, i);
        let commit = LooseCommit::new(id, head, BTreeSet::new(), blob_meta);
        // ...and sign with the attacker's key.
        let signed = Signed::seal::<Sendable, _>(&attacker_signer, commit)
            .await
            .into_signed();
        // But attach a *different* blob to the wire.
        let actual = blob(i.wrapping_add(0xFFFF), 64);
        let msg = SyncMessage::LooseCommit {
            id,
            commit: signed,
            blob: actual,
            sender_heads: RemoteHeads::default(),
        };
        let bytes = msg.encode();
        // If the channel is full or closed, stop early — the test still
        // demonstrates the receiver survives a burst.
        if <ChannelTransport as Transport<Sendable>>::send_bytes(&transport_attacker, &bytes)
            .await
            .is_err()
        {
            break;
        }
        sent += 1;

        // Belay-and-suppress; we want a tight burst.
        if i % 25 == 0 {
            tokio::task::yield_now().await;
        }
    }

    // Give the receiver time to drain the burst.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The malformed sedimentree should not be present.
    let stored = receiver.get_commits(id).await;
    assert!(
        stored.as_ref().is_none_or(Vec::is_empty),
        "blob-mismatched commits should not be stored: {stored:?}"
    );

    // Sanity: receiver is still alive and accepts a well-formed write
    // from a fresh peer afterwards.
    let good_client = make_sendable_node(202);
    connect_sendable_pair(&good_client, 202, &receiver, 200)
        .await
        .expect("good client connect");
    let id2 = sed_id(0xC0FFEE);
    populate_linear_chain_sendable(&good_client, id2, 0, 1, 32)
        .await
        .expect("good populate");
    let result = good_client
        .full_sync_with_peer(&receiver_peer_id, true, Some(Duration::from_secs(5)))
        .await;
    assert!(
        result.0,
        "well-formed sync after burst should succeed; sent={sent}"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    let receiver_good = receiver.get_commits(id2).await;
    assert_eq!(
        receiver_good.map(|c| c.len()),
        Some(1),
        "receiver should accept post-burst well-formed write"
    );
}

// ─── Mixed workload ─────────────────────────────────────────────────────────

/// Concurrent fan-in + fan-out + reconnect cycles. The "do everything at
/// once" smoke test.
///
/// Three concurrent task groups:
///
/// - **Adders**: a pool of clients each populating multiple documents
///   and triggering syncs to a central server.
/// - **Readers**: a separate set of clients pulling already-synced data
///   back from the server.
/// - **Reconnecters**: one peer cycling connect/disconnect against the
///   server in the background.
///
/// What this catches: combined cross-path races, deadlocks where one
/// path holds a lock another path waits on, sender-task hangs when a
/// reconnecting peer's outbound channel sees mid-flight messages.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_workload() {
    const ADDERS: u32 = 8;
    const DOCS_PER_ADDER: u32 = 10;
    const COMMITS_PER_DOC: u32 = 3;
    const READERS: u32 = 4;
    const RECONNECT_CYCLES: u32 = 8;
    const TIMEOUT: Duration = Duration::from_secs(30);

    let server = make_sendable_node(300);
    let server_peer_id = PeerId::from(signer(300).verifying_key());

    // Adders writing.
    let total_progress = Arc::new(AtomicUsize::new(0));
    let mut adder_handles = Vec::with_capacity(ADDERS as usize);
    for adder_idx in 0..ADDERS {
        let client_seed = 400 + adder_idx;
        let client = make_sendable_node(client_seed);
        connect_sendable_pair(&client, client_seed, &server, 300)
            .await
            .expect("adder connect");
        let progress = total_progress.clone();
        adder_handles.push(tokio::spawn(async move {
            for doc in 0..DOCS_PER_ADDER {
                let id = sed_id(adder_idx * DOCS_PER_ADDER + doc);
                populate_linear_chain_sendable(
                    &client,
                    id,
                    adder_idx * DOCS_PER_ADDER + doc,
                    COMMITS_PER_DOC,
                    32,
                )
                .await
                .expect("adder populate");
                progress.fetch_add(1, Ordering::Relaxed);
            }
            let result = client
                .full_sync_with_peer(&server_peer_id, true, Some(TIMEOUT))
                .await;
            assert!(result.0, "adder {adder_idx}: sync");
        }));
    }

    // Readers pulling.
    let mut reader_handles = Vec::with_capacity(READERS as usize);
    for reader_idx in 0..READERS {
        let client_seed = 500 + reader_idx;
        let client = make_sendable_node(client_seed);
        connect_sendable_pair(&client, client_seed, &server, 300)
            .await
            .expect("reader connect");
        reader_handles.push(tokio::spawn(async move {
            // Polling until either we see something or the loop budget
            // is exhausted. Doesn't matter what we read; we're stressing
            // the read path under contention with adders.
            for _ in 0..10 {
                drop(
                    client
                        .full_sync_with_peer(&server_peer_id, true, Some(TIMEOUT))
                        .await,
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }));
    }

    // Reconnecter cycling.
    let reconnecter = make_sendable_node(600);
    let reconnecter_seed = 600u32;
    let reconnect_handle = tokio::spawn({
        let server = server.clone();
        async move {
            for _ in 0..RECONNECT_CYCLES {
                connect_sendable_pair(&reconnecter, reconnecter_seed, &server, 300)
                    .await
                    .expect("reconnecter connect");
                tokio::time::sleep(Duration::from_millis(10)).await;
                let _ = reconnecter.disconnect_from_peer(&server_peer_id).await;
                let _ = server
                    .disconnect_from_peer(&PeerId::from(signer(reconnecter_seed).verifying_key()))
                    .await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    });

    for h in adder_handles {
        h.await.expect("adder did not panic");
    }
    for h in reader_handles {
        h.await.expect("reader did not panic");
    }
    reconnect_handle.await.expect("reconnecter did not panic");

    // Wait for any tail broadcasts.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let progress = total_progress.load(Ordering::Relaxed);
    assert_eq!(
        progress as u32,
        ADDERS * DOCS_PER_ADDER,
        "all adders should have populated their docs"
    );

    // Spot-check: server should see at least one document per adder.
    for adder_idx in 0..ADDERS {
        let id = sed_id(adder_idx * DOCS_PER_ADDER);
        let commits = server.get_commits(id).await;
        assert!(
            commits.is_some_and(|c| !c.is_empty()),
            "server should have commits for adder {adder_idx} doc 0"
        );
    }
}


