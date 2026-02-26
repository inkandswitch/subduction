//! End-to-end test: two CLI server processes sync data over Iroh (QUIC).
//!
//! Spawns two `subduction_cli server` processes with `--iroh` enabled,
//! connects to each via HTTP long-poll to inject and verify data, and
//! confirms that a commit written to server A propagates to server B
//! through the iroh transport.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::{
        mpsc::{self, RecvTimeoutError},
        Arc,
    },
    time::Duration,
};

use future_form::Sendable;
use sedimentree_core::{blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId};
use subduction_core::{
    connection::{nonce_cache::NonceCache, test_utils::TokioSpawn},
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS, Subduction},
    timestamp::TimestampSeconds,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_http_longpoll::{
    client::HttpLongPollClient, connection::HttpLongPollConnection, http_client::ReqwestHttpClient,
};
use subduction_websocket::timeout::FuturesTimerTimeout;

const SYNC_TIMEOUT: Duration = Duration::from_secs(10);
const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        HttpLongPollConnection<FuturesTimerTimeout>,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
    >,
>;

/// Strip ANSI escape sequences from a string.
fn strip_ansi(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            for c2 in chars.by_ref() {
                if c2.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn cli_binary() -> std::path::PathBuf {
    let mut path = std::env::current_exe().expect("test executable path");
    path.pop(); // remove test binary name
    path.pop(); // remove `deps`
    path.push("subduction_cli");
    assert!(
        path.exists(),
        "CLI binary not found at {} — run `cargo build -p subduction_cli` first",
        path.display()
    );
    path
}

fn signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

/// Spawn a server process and extract its iroh node ID from stderr.
///
/// Returns the child process and the iroh node ID (if found within the timeout).
fn spawn_server(
    port: u16,
    key_seed: &str,
    service_name: &str,
    iroh_peer: Option<&str>,
    data_dir: &std::path::Path,
) -> (Child, Option<String>) {
    eprintln!(">>> Spawning server on port {port}...");

    let mut cmd = Command::new(cli_binary());
    cmd.arg("server")
        .arg("--socket")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--key-seed")
        .arg(key_seed)
        .arg("--service-name")
        .arg(service_name)
        .arg("--iroh")
        .arg("--data-dir")
        .arg(data_dir)
        .env("RUST_LOG", "info")
        .stderr(Stdio::piped())
        .stdout(Stdio::null());

    if let Some(peer_id) = iroh_peer {
        cmd.arg("--iroh-peer").arg(peer_id);
    }

    let mut child = cmd.spawn().expect("failed to spawn server");

    // Parse stderr in a background thread, send the node ID over a channel.
    let stderr = child.stderr.take().expect("stderr not captured");
    let (tx, rx) = mpsc::channel::<String>();

    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line_buf = String::new();
        let mut sent = false;

        loop {
            line_buf.clear();
            match reader.read_line(&mut line_buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }

            let line = line_buf.trim_end();
            eprintln!("[server:{port}] {line}");

            if !sent {
                let stripped = strip_ansi(line);
                if let Some(idx) = stripped.find("node ID = ") {
                    let rest = &stripped[idx + "node ID = ".len()..];
                    let id: String = rest.chars().take_while(|c| c.is_alphanumeric()).collect();
                    if !id.is_empty() {
                        drop(tx.send(id));
                        sent = true;
                    }
                }
            }
        }
    });

    // Wait for the node ID with a timeout
    let node_id = match rx.recv_timeout(SERVER_STARTUP_TIMEOUT) {
        Ok(id) => {
            eprintln!(">>> Server on port {port}: iroh node ID = {id}");
            Some(id)
        }
        Err(RecvTimeoutError::Timeout) => {
            eprintln!(">>> Server on port {port}: timed out waiting for iroh node ID");
            None
        }
        Err(RecvTimeoutError::Disconnected) => {
            eprintln!(">>> Server on port {port}: stderr reader ended without finding node ID");
            None
        }
    };

    (child, node_id)
}

/// Wait for the HTTP long-poll endpoint to be ready.
async fn wait_for_http(base_url: &str) {
    let client = reqwest::Client::new();
    for _ in 0..50 {
        if client
            .request(reqwest::Method::OPTIONS, format!("{base_url}/lp/handshake"))
            .send()
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server at {base_url} did not become ready in 5 seconds");
}

/// Connect to a running server via HTTP long-poll and return a Subduction instance.
async fn connect_to_server(base_url: &str, client_seed: u8, service_name: &str) -> TestSubduction {
    let client_signer = signer(client_seed);

    let (subduction, listener_fut, manager_fut): (TestSubduction, _, _) = Subduction::new(
        None,
        client_signer.clone(),
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::new(),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(listener_fut);
    tokio::spawn(manager_fut);

    let lp_client = HttpLongPollClient::new(
        base_url,
        ReqwestHttpClient::new(),
        FuturesTimerTimeout,
        SYNC_TIMEOUT,
    );

    let now = TimestampSeconds::now();
    let result = lp_client
        .connect_discover(&client_signer, service_name, now)
        .await
        .expect("HTTP long-poll connect");

    tokio::spawn(result.poll_task);
    tokio::spawn(result.send_task);
    subduction
        .register(result.authenticated)
        .await
        .expect("register");

    subduction
}

#[tokio::test]
async fn iroh_sync_between_two_cli_servers() {
    let tmp_a = tempfile::tempdir().expect("tmpdir A");
    let tmp_b = tempfile::tempdir().expect("tmpdir B");

    let service_name = "iroh-e2e-test";

    // Deterministic key seeds (64 hex chars = 32 bytes)
    let key_seed_a = "aa".repeat(32);
    let key_seed_b = "bb".repeat(32);

    // Use ports unlikely to conflict
    let port_a = 28081;
    let port_b = 28082;

    // ── Start server A ───────────────────────────────────────────────────
    let (mut child_a, node_id_a) =
        spawn_server(port_a, &key_seed_a, service_name, None, tmp_a.path());

    let node_id_a = node_id_a.expect("server A should report its iroh node ID");

    // ── Start server B, connecting to A via iroh ─────────────────────────
    let (mut child_b, _node_id_b) = spawn_server(
        port_b,
        &key_seed_b,
        service_name,
        Some(&node_id_a),
        tmp_b.path(),
    );

    // ── Wait for both servers to be ready ────────────────────────────────
    wait_for_http(&format!("http://127.0.0.1:{port_a}")).await;
    wait_for_http(&format!("http://127.0.0.1:{port_b}")).await;

    // Give iroh time to establish the QUIC connection + handshake
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ── Connect to server A, push a commit ───────────────────────────────
    let client_a =
        connect_to_server(&format!("http://127.0.0.1:{port_a}"), 0xCC, service_name).await;

    let sed_id = SedimentreeId::new([42u8; 32]);
    let blob = Blob::new(b"hello from iroh e2e test".to_vec());
    client_a
        .add_commit(sed_id, BTreeSet::new(), blob)
        .await
        .expect("add commit");

    let (had_success, _stats, call_errs, io_errs) = client_a.full_sync(Some(SYNC_TIMEOUT)).await;
    assert!(
        call_errs.is_empty(),
        "call errors pushing to A: {call_errs:?}"
    );
    assert!(io_errs.is_empty(), "io errors pushing to A: {io_errs:?}");
    assert!(had_success, "sync to server A should succeed");

    // ── Wait for iroh to propagate A → B ─────────────────────────────────
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ── Connect to server B, verify the commit arrived ───────────────────
    let client_b =
        connect_to_server(&format!("http://127.0.0.1:{port_b}"), 0xDD, service_name).await;

    // Pull from B — B should already have the data via iroh sync from A
    let _result = client_b.sync_all(sed_id, true, Some(SYNC_TIMEOUT)).await;

    let commits_b = client_b.get_commits(sed_id).await;
    assert!(
        commits_b.is_some() && !commits_b.as_ref().unwrap().is_empty(),
        "server B should have the commit that was written to server A (got: {commits_b:?})"
    );

    // ── Cleanup ──────────────────────────────────────────────────────────
    child_a.kill().ok();
    child_b.kill().ok();
    child_a.wait().ok();
    child_b.wait().ok();
}
