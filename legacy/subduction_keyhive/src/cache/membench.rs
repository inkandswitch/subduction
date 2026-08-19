//! In-crate heap-profiling membenches for the periodic event cache.
//!
//! Gated behind `dhat-heap` (which installs dhat as the global allocator)
//! so normal builds and `cargo test` are unaffected.
//!
//! Both are `#[ignore]`d: dhat's profiler is a global singleton, so the two
//! cannot run concurrently (the default multi-threaded runner would panic with
//! "a profiler is already running"). Run one at a time, explicitly:
//!   cargo test -p subduction_keyhive --release --features test-utils,dhat-heap \
//!       serving_membench -- --nocapture --include-ignored
//!   cargo test -p subduction_keyhive --release --features test-utils,dhat-heap \
//!       keyhive_baseline_membench -- --nocapture --include-ignored

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::indexing_slicing,
    clippy::items_after_statements,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used
)]

use super::*;

/// One held in-flight response in the serve phase: the `Arc`-shared per-pair
/// map plus its wire-built `found_ops` (empty unless the `cold` mode builds it).
type InflightResponse = (Arc<AgentHashMap>, Vec<Arc<[u8]>>);

/// Read a `usize` tuning knob from env var `key`, falling back to `default`.
/// Shared by the `dhat-heap` membench tests below.
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// In-crate heap membench for the periodic cache, run under dhat as the
/// global allocator. It exercises `cache.refresh` (the periodic rebuild) and
/// `cache.events_for_peer_pair` (per-peer serving) directly, so heap
/// behaviour is measured against the real methods with no test-only API.
///
/// Two phases:
///   1. REFRESH: build a dense server keyhive, then loop `cache.refresh`
///      while injecting PCS edits, reporting retained and peak heap per
///      iteration.
///   2. SERVE: hold `MEMBENCH_SERVE` concurrent `events_for_peer_pair`
///      responses at once and report the heap they occupy.
///
/// Parameters come from env vars (defaults in parens): MEMBENCH_N (100),
/// MEMBENCH_K (5), MEMBENCH_ROT (5), MEMBENCH_ITERS (10), MEMBENCH_EDITS (5),
/// MEMBENCH_SERVE (500). Run:
///   cargo test -p subduction_keyhive --release --features test-utils,dhat-heap \
///       serving_membench -- --nocapture --include-ignored
#[test]
#[ignore = "dhat profiling harness; run explicitly, one at a time (global profiler)"]
fn serving_membench() {
    use alloc::vec::Vec;

    use keyhive_core::{
        access::Access,
        principal::{
            agent::Agent, identifier::Identifier, individual::op::KeyOp, membered::Membered,
            public::Public,
        },
    };
    use nonempty::nonempty;

    use crate::{
        message::Message,
        test_utils::{keyhive_peer_id, make_keyhive, make_protocol_with_shared_keyhive},
    };

    let n = env_usize("MEMBENCH_N", 100);
    let k = env_usize("MEMBENCH_K", 5);
    let rotations = env_usize("MEMBENCH_ROT", 5);
    let iters = env_usize("MEMBENCH_ITERS", 10);
    let edits = env_usize("MEMBENCH_EDITS", 5);
    let serve = env_usize("MEMBENCH_SERVE", 500);
    // Wire-build model for the serve phase: "off" (default) holds only the
    // Arc-shared per-pair maps (a warm peer, whose found_ops diff is empty);
    // "cold" also builds found_ops = the full event set per response (a peer
    // that has nothing yet).
    let wire = std::env::var("MEMBENCH_WIRE").unwrap_or_else(|_| "off".into());
    const MB: f64 = 1024.0 * 1024.0;

    let _profiler = dhat::Profiler::new_heap();
    eprintln!(
        "\nserving_membench: N={n} K={k} rotations={rotations} iters={iters} \
         edits/iter={edits} serve={serve}"
    );

    futures::executor::block_on(async move {
        // Build a dense server keyhive: N peer agents known (contact
        // card + prekey rotations), N public docs each with K peer readers.
        let server = make_keyhive().await;
        let mut peers = Vec::with_capacity(n);
        for _ in 0..n {
            peers.push(make_keyhive().await);
        }
        for p in &peers {
            let cc = p.contact_card().await.expect("contact_card");
            server
                .receive_contact_card(&cc)
                .await
                .expect("receive_contact_card");
            for _ in 0..rotations {
                let add = p.expand_prekeys().await.expect("expand_prekeys");
                server
                    .receive_prekey_op(&KeyOp::Add(add.clone()))
                    .await
                    .expect("receive add");
                let rot = p
                    .rotate_prekey(add.payload().share_key)
                    .await
                    .expect("rotate_prekey");
                server
                    .receive_prekey_op(&KeyOp::Rotate(rot))
                    .await
                    .expect("receive rotate");
            }
        }
        let mut docs = Vec::with_capacity(n);
        for i in 0..n {
            let fill = ((i % 251) + 1) as u8;
            let doc = server
                .generate_doc(vec![], nonempty![[fill; 32]])
                .await
                .expect("generate_doc");
            let doc_id = doc.lock().await.doc_id();
            let membered = Membered::Document(doc_id, doc.clone());
            let public_agent: Agent<_, _, _, _> = Public.individual().into();
            server
                .add_member(public_agent, &membered, Access::Read, &[])
                .await
                .expect("add public");
            for step in 1..=k.min(n.saturating_sub(1)) {
                let j = (i + step) % n;
                if j == i {
                    continue;
                }
                let id: Identifier = peers[j].id().into();
                if let Some(agent) = server.get_agent(id).await {
                    server
                        .add_member(agent, &membered, Access::Read, &[])
                        .await
                        .expect("add member");
                }
            }
            docs.push(doc);
        }

        // Server's own peer id (the `local` side of every served pair),
        // captured before the keyhive is moved into the protocol.
        let local = keyhive_peer_id(&server);
        let peer_ids: Vec<KeyhivePeerId> = peers.iter().map(keyhive_peer_id).collect();
        let (proto, shared, _storage) = make_protocol_with_shared_keyhive(server).await;

        // ---- Phase 1: REFRESH (real cache.refresh) over a mutating graph.
        let mut cache = PeriodicEventCache::new();
        let base = dhat::HeapStats::get();
        eprintln!(
            "\n[build] retained={:.1} MB  peak={:.1} MB  (docs={})",
            base.curr_bytes as f64 / MB,
            base.max_bytes as f64 / MB,
            docs.len()
        );
        eprintln!("[refresh]  iter | rebuilt | retained MB | peak MB");
        for it in 0..iters {
            for e in 0..edits {
                if docs.is_empty() {
                    break;
                }
                let idx = (it * edits + e) % docs.len();
                let doc = docs[idx].clone();
                let _op = shared.lock().await.force_pcs_update(doc).await;
            }
            let rebuilt = cache.refresh(&proto).await.expect("refresh");
            let s = dhat::HeapStats::get();
            eprintln!(
                "{it:9} | {:7} | {:11.1} | {:7.1}",
                if rebuilt { "yes" } else { "skip" },
                s.curr_bytes as f64 / MB,
                s.max_bytes as f64 / MB,
            );
        }
        let after_refresh = dhat::HeapStats::get();
        eprintln!(
            "[refresh] retained={:.1} MB  peak={:.1} MB  | cache: {} agents, \
             {} public hashes, {} event_data entries",
            after_refresh.curr_bytes as f64 / MB,
            after_refresh.max_bytes as f64 / MB,
            cache.agent_count(),
            cache.public_hashes().len(),
            cache.event_data.len(),
        );

        // ---- Phase 2: SERVE. Hold `serve` concurrent responses. Modes:
        //   "off":       hold only the Arc-shared per-pair maps. A warm peer
        //                 whose found_ops diff is empty.
        //   "cold":      also hold found_ops = the full event set as
        //                 `Vec<Arc<[u8]>>`, which clones the cache `Arc`s
        //                 rather than copying bytes.
        //   "serialize": build and hold one serialized `SyncResponse` frame
        //                 per peer, dropping the maps and found_ops first (as
        //                 the handler does once the frame is queued for send).
        //                 Models an outbound send queue of full responses.
        if serve > 0 {
            let cold = wire == "cold";
            let serialize = wire == "serialize";
            let pre = dhat::HeapStats::get();
            let mut inflight: Vec<InflightResponse> =
                Vec::with_capacity(if serialize { 0 } else { serve });
            let mut frames: Vec<alloc::vec::Vec<u8>> =
                Vec::with_capacity(if serialize { serve } else { 0 });
            let mut per_response = 0;
            for m in 0..serve {
                let peer = peer_ids[m % peer_ids.len()].clone();
                let response = cache.events_for_peer_pair(&local, &peer);
                per_response = response.len();
                if serialize {
                    let found_ops: Vec<Arc<[u8]>> = response.values().map(Dupe::dupe).collect();
                    let msg = Message::SyncResponse {
                        sender_id: local.clone(),
                        target_id: peer,
                        requested: alloc::vec::Vec::new(),
                        found: found_ops,
                        sync_responder_total: 0,
                        sync_requester_total: 0,
                    };
                    let mut buf = alloc::vec::Vec::new();
                    ciborium::into_writer(&msg, &mut buf).expect("encode SyncResponse");
                    frames.push(buf);
                    // response and found_ops drop here, as in the handler.
                } else {
                    let found_ops: Vec<Arc<[u8]>> = if cold {
                        response.values().map(Dupe::dupe).collect()
                    } else {
                        Vec::new()
                    };
                    inflight.push((response, found_ops));
                }
            }
            let s = dhat::HeapStats::get();
            eprintln!(
                "\n[serve] {serve} concurrent responses, ~{per_response} events each, \
                 wire={wire}\n  \
                 before={:.1} MB  live={:.1} MB  peak={:.1} MB  (Δlive {:+.1} MB)",
                pre.curr_bytes as f64 / MB,
                s.curr_bytes as f64 / MB,
                s.max_bytes as f64 / MB,
                (s.curr_bytes as i64 - pre.curr_bytes as i64) as f64 / MB,
            );
            core::hint::black_box((&inflight, &frames));
        }
    });
}

/// In-crate heap membench isolating the **keyhive-on BASELINE**: the
/// retained footprint of the server's keyhive op-graph after ingesting N
/// peers (contact cards + prekey rotations) and N public docs (public + K
/// readers), with NO cache and NO serving. It reports retained heap at each
/// build stage so the baseline can be attributed to ingest vs prekeys vs
/// docs vs membership and scaled vs N, then drops the client keyhives (which
/// do not exist on the real server) before the final reading so the number
/// is the server op-graph alone. On `_profiler` drop dhat writes
/// `dhat-heap.json` (cwd) whose retained set is the server op-graph. Load it in
/// the dhat viewer to see which `keyhive_core` structures dominate.
///
/// Params (env, defaults): MEMBENCH_N (100), MEMBENCH_K (5),
/// MEMBENCH_ROT (5). Run:
///   cargo test -p subduction_keyhive --release --features test-utils,dhat-heap \
///       keyhive_baseline_membench -- --nocapture --include-ignored
#[test]
#[ignore = "dhat profiling harness; run explicitly, one at a time (global profiler)"]
fn keyhive_baseline_membench() {
    use alloc::vec::Vec;

    use keyhive_core::{
        access::Access,
        principal::{
            agent::Agent, identifier::Identifier, individual::op::KeyOp, membered::Membered,
            public::Public,
        },
    };
    use nonempty::nonempty;

    use crate::test_utils::{keyhive_peer_id, make_keyhive};

    let n = env_usize("MEMBENCH_N", 100);
    let k = env_usize("MEMBENCH_K", 5);
    let rotations = env_usize("MEMBENCH_ROT", 5);
    const MB: f64 = 1024.0 * 1024.0;

    let _profiler = dhat::Profiler::new_heap();
    eprintln!("\nkeyhive_baseline_membench: N={n} K={k} rotations={rotations}");

    futures::executor::block_on(async move {
        macro_rules! stage {
            ($label:expr) => {{
                let s = dhat::HeapStats::get();
                eprintln!(
                    "[{:<26}] retained={:8.1} MB  peak={:8.1} MB",
                    $label,
                    s.curr_bytes as f64 / MB,
                    s.max_bytes as f64 / MB,
                );
            }};
        }

        let server = make_keyhive().await;
        stage!("empty server");

        // N client keyhives, used only to mint events the server ingests.
        let mut peers = Vec::with_capacity(n);
        for _ in 0..n {
            peers.push(make_keyhive().await);
        }
        stage!("N client keyhives");

        for p in &peers {
            let cc = p.contact_card().await.expect("contact_card");
            server
                .receive_contact_card(&cc)
                .await
                .expect("receive_contact_card");
        }
        stage!("+ contact cards");

        for p in &peers {
            for _ in 0..rotations {
                let add = p.expand_prekeys().await.expect("expand_prekeys");
                server
                    .receive_prekey_op(&KeyOp::Add(add.clone()))
                    .await
                    .expect("receive add");
                let rot = p
                    .rotate_prekey(add.payload().share_key)
                    .await
                    .expect("rotate_prekey");
                server
                    .receive_prekey_op(&KeyOp::Rotate(rot))
                    .await
                    .expect("receive rotate");
            }
        }
        stage!("+ prekey rotations");

        let mut docs = Vec::with_capacity(n);
        for i in 0..n {
            let fill = ((i % 251) + 1) as u8;
            let doc = server
                .generate_doc(vec![], nonempty![[fill; 32]])
                .await
                .expect("generate_doc");
            let doc_id = doc.lock().await.doc_id();
            let membered = Membered::Document(doc_id, doc.clone());
            let public_agent: Agent<_, _, _, _> = Public.individual().into();
            server
                .add_member(public_agent, &membered, Access::Read, &[])
                .await
                .expect("add public");
            docs.push(membered);
        }
        stage!("+ N public docs");

        for (i, membered) in docs.iter().enumerate() {
            for step in 1..=k.min(n.saturating_sub(1)) {
                let j = (i + step) % n;
                if j == i {
                    continue;
                }
                let id: Identifier = peers[j].id().into();
                if let Some(agent) = server.get_agent(id).await {
                    server
                        .add_member(agent, membered, Access::Read, &[])
                        .await
                        .expect("add member");
                }
            }
        }
        stage!("+ K readers/doc");

        // The real server never holds the client keyhives; drop them so the
        // final reading is the server op-graph alone.
        let peer_ids: Vec<KeyhivePeerId> = peers.iter().map(keyhive_peer_id).collect();
        drop(peers);
        stage!("SERVER BASELINE (peers dropped)");

        // Keep the server op-graph (and only it) alive for the dhat dump.
        core::hint::black_box((&server, &docs, &peer_ids));
    });
}
