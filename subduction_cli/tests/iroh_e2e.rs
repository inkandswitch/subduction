//! End-to-end test: two CLI server processes sync data over Iroh (QUIC).
//!
//! Spawns two `subduction_cli server` processes with `--iroh` enabled,
//! connects to each via HTTP long-poll to inject and verify data, and
//! confirms that commits sync bidirectionally through the iroh transport.
//!
//! # Strategy
//!
//! Because there is no sedimentree discovery protocol yet, a server only
//! syncs sedimentrees it already knows about. The test works around this by
//! pushing one commit to _each_ server for the same sedimentree, then
//! waiting for the background sync to reconcile. After sync, both servers
//! should hold both commits.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::{
    collections::{BTreeSet, HashMap},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use future_form::Sendable;
use sedimentree_core::{blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId};
use subduction_core::{
    connection::{nonce_cache::NonceCache, test_utils::TokioSpawn},
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
    timestamp::TimestampSeconds,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_http_longpoll::{
    client::HttpLongPollClient, connection::HttpLongPollConnection, http_client::ReqwestHttpClient,
};
use subduction_websocket::timeout::FuturesTimerTimeout;

const SYNC_TIMEOUT: Duration = Duration::from_secs(10);
const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);

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

/// Information extracted from the server's ready file.
struct ServerInfo {
    child: Child,
    port: u16,
    node_id: String,
    iroh_addrs: Vec<String>,
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

/// Parse a simple `key=value\n` file into a map.
fn parse_ready_file(content: &str) -> HashMap<String, String> {
    content
        .lines()
        .filter_map(|line| {
            let (k, v) = line.split_once('=')?;
            Some((k.to_owned(), v.to_owned()))
        })
        .collect()
}

/// Spawn a server process with port 0 (OS-assigned) and wait for its ready file.
///
/// If `iroh_peer` is provided as `(node_id, &[addr])`, the server is started
/// with `--iroh-peer <node_id>` plus `--iroh-peer-addr <addr>` for each address.
#[allow(clippy::zombie_processes)]
async fn spawn_server(
    key_seed: &str,
    service_name: &str,
    iroh_peer: Option<(&str, &[String])>,
    data_dir: &std::path::Path,
    ready_file: &std::path::Path,
) -> ServerInfo {
    let mut cmd = Command::new(cli_binary());
    cmd.arg("server")
        .arg("--socket")
        .arg("127.0.0.1:0")
        .arg("--key-seed")
        .arg(key_seed)
        .arg("--service-name")
        .arg(service_name)
        .arg("--iroh")
        .arg("--iroh-direct-only")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--ready-file")
        .arg(ready_file)
        .env("RUST_LOG", "info")
        .stderr(Stdio::inherit())
        .stdout(Stdio::null());

    if let Some((peer_id, addrs)) = iroh_peer {
        cmd.arg("--iroh-peer").arg(peer_id);
        for addr in addrs {
            cmd.arg("--iroh-peer-addr").arg(addr);
        }
    }

    let child = cmd.spawn().expect("failed to spawn server");

    // Poll for the ready file
    let deadline = tokio::time::Instant::now() + SERVER_STARTUP_TIMEOUT;
    loop {
        assert!(
            tokio::time::Instant::now() <= deadline,
            "timed out waiting for ready file at {}",
            ready_file.display()
        );
        if ready_file.exists()
            && let Ok(content) = std::fs::read_to_string(ready_file)
        {
            let map = parse_ready_file(&content);
            if let (Some(port_str), Some(node_id)) = (map.get("port"), map.get("iroh_node_id")) {
                let port: u16 = port_str
                    .parse()
                    .expect("ready file port should be a valid u16");
                let iroh_addrs = map
                    .get("iroh_addrs")
                    .map(|s| s.split(',').map(String::from).collect())
                    .unwrap_or_default();
                return ServerInfo {
                    child,
                    port,
                    node_id: node_id.clone(),
                    iroh_addrs,
                };
            }
        }
        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
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

/// Push a commit to a server via LP client and sync.
async fn push_commit(client: &TestSubduction, sed_id: SedimentreeId, payload: &[u8]) {
    let blob = Blob::new(payload.to_vec());
    client
        .add_commit(sed_id, BTreeSet::new(), blob)
        .await
        .expect("add commit");

    let (had_success, _stats, call_errs, io_errs) = client.full_sync(Some(SYNC_TIMEOUT)).await;
    assert!(call_errs.is_empty(), "call errors: {call_errs:?}");
    assert!(io_errs.is_empty(), "io errors: {io_errs:?}");
    assert!(had_success, "sync should succeed");
}

#[tokio::test]
async fn iroh_sync_between_two_cli_servers() {
    let tmp_a = tempfile::tempdir().expect("tmpdir A");
    let tmp_b = tempfile::tempdir().expect("tmpdir B");

    let service_name = "iroh-e2e-test";

    // Deterministic key seeds (64 hex chars = 32 bytes)
    let key_seed_a = "aa".repeat(32);
    let key_seed_b = "bb".repeat(32);

    let ready_a = tmp_a.path().join("ready");
    let ready_b = tmp_b.path().join("ready");

    // ── Start server A (port 0 = OS-assigned) ────────────────────────────
    let mut server_a = spawn_server(&key_seed_a, service_name, None, tmp_a.path(), &ready_a).await;

    // ── Start server B, connecting to A via iroh with direct addresses ──
    let mut server_b = spawn_server(
        &key_seed_b,
        service_name,
        Some((&server_a.node_id, &server_a.iroh_addrs)),
        tmp_b.path(),
        &ready_b,
    )
    .await;

    let url_a = format!("http://127.0.0.1:{}", server_a.port);
    let url_b = format!("http://127.0.0.1:{}", server_b.port);

    // ── Wait for both HTTP endpoints to be ready ─────────────────────────
    wait_for_http(&url_a).await;
    wait_for_http(&url_b).await;

    // Give iroh time to establish the QUIC connection + handshake.
    // On slower machines or under load this can take longer than expected.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let sed_id = SedimentreeId::new([42u8; 32]);

    // ── Push one commit to each server ───────────────────────────────────
    // Both servers must know about the sedimentree for the background sync
    // to reconcile (no sedimentree discovery protocol yet).
    let client_a = connect_to_server(&url_a, 0xCC, service_name).await;
    push_commit(&client_a, sed_id, b"commit from server A").await;

    let client_b = connect_to_server(&url_b, 0xDD, service_name).await;
    push_commit(&client_b, sed_id, b"commit from server B").await;

    // ── Poll until both servers have both commits ────────────────────────
    // The server runs full_sync every 5 seconds over iroh. We reuse a
    // single verify client per server to avoid connection churn (each
    // fresh client spawns poll/send tasks that linger), then call
    // sync_all on each iteration to pull the latest state.
    let sync_deadline = tokio::time::Instant::now() + Duration::from_secs(90);

    let verify_a = connect_to_server(&url_a, 0xE0, service_name).await;
    let verify_b = connect_to_server(&url_b, 0xE1, service_name).await;

    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let result_a = verify_a.sync_all(sed_id, true, Some(SYNC_TIMEOUT)).await;
        if let Err(ref e) = result_a {
            eprintln!("verify_a sync_all error: {e:?}");
        }
        let count_a = verify_a
            .get_commits(sed_id)
            .await
            .as_ref()
            .map_or(0, Vec::len);

        let result_b = verify_b.sync_all(sed_id, true, Some(SYNC_TIMEOUT)).await;
        if let Err(ref e) = result_b {
            eprintln!("verify_b sync_all error: {e:?}");
        }
        let count_b = verify_b
            .get_commits(sed_id)
            .await
            .as_ref()
            .map_or(0, Vec::len);

        if count_a >= 2 && count_b >= 2 {
            break;
        }

        assert!(
            tokio::time::Instant::now() < sync_deadline,
            "timed out waiting for iroh sync: server A has {count_a} commits, \
             server B has {count_b} commits (expected >= 2 each)"
        );
    }

    // ── Cleanup ──────────────────────────────────────────────────────────
    server_a.child.kill().ok();
    server_b.child.kill().ok();
    server_a.child.wait().ok();
    server_b.child.wait().ok();
}
