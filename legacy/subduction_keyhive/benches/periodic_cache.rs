//! Benchmark: the server's periodic event cache (`PeriodicEventCache`) build cost.
//!
//! Run with:
//!
//! ```text
//! cargo bench -p subduction_keyhive --features test-utils --bench periodic_cache
//! ```
//!
//! The 2 s `refresh_cache` calls `KeyhiveProtocol::all_agent_events`, which builds
//! a per-agent reachable-hash set for every agent. This
//! bench measures how that build's time and the resulting cache size scale with
//! the number of peers/docs, under two access shapes:
//!
//! - `public`: N peer agents are known to the server; each of N docs is shared
//!   with the Public agent (mirrors the load test's `setPublicAccess`).
//! - `members`: additionally, each doc lists K specific peers as Read members
//!   (mirrors cross-subscription / non-public reachability).
//!
//! For each (shape, N) the size table is printed once to stderr: agents, total
//! per-agent hash entries (Sigma |`agent_hashes`[a]|), distinct events. The criterion
//! timing measures the `all_agent_events` rebuild itself (run every 2 s in production).

#![allow(
    missing_docs,
    unreachable_pub,
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::wildcard_enum_match_arm
)]

use std::collections::BTreeSet;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::executor::block_on;
use keyhive_core::{
    access::Access,
    principal::{agent::Agent, identifier::Identifier, membered::Membered, public::Public},
};
use nonempty::nonempty;
use subduction_keyhive::{
    AllAgentEvents,
    test_utils::{
        SimpleKeyhive, TestProtocol, create_channel_pair, keyhive_peer_id, make_keyhive,
        make_protocol_with_shared_keyhive, sync_pair_rounds,
    },
};

/// Access shape for the generated docs.
#[derive(Clone, Copy)]
enum Shape {
    /// Each doc shared with the Public agent only.
    Public,
    /// Each doc additionally lists `K` specific peers as Read members.
    Members(usize),
    /// Each doc lists all other N - 1 peers as Read members.
    AllOthers,
}

/// Measured cache size for one built configuration.
struct Sizes {
    agents: usize,
    agent_hash_entries: usize,
    /// Largest single per-agent reachable set.
    max_per_agent: usize,
    events: usize,
}

impl Sizes {
    /// Snapshot the cache-relevant sizes of a built [`AllAgentEvents`].
    fn from_events(events: &AllAgentEvents) -> Self {
        Self {
            agents: events.agent_count(),
            agent_hash_entries: events.agent_hashes.values().map(BTreeSet::len).sum(),
            max_per_agent: events
                .agent_hashes
                .values()
                .map(BTreeSet::len)
                .max()
                .unwrap_or(0),
            events: events.event_count(),
        }
    }
}

/// Build a server keyhive with `n` known peer agents and `n` docs under `shape`,
/// wrap it in a protocol, and report the resulting cache snapshot size.
async fn build(n: usize, shape: Shape) -> (TestProtocol, Sizes) {
    let server = make_keyhive().await;

    // N peer keyhives, and make the server aware of each as an agent (mirrors
    // the server learning peer identities via SUK ingest under load).
    let mut peers: Vec<SimpleKeyhive> = Vec::with_capacity(n);
    for _ in 0..n {
        peers.push(make_keyhive().await);
    }
    for p in &peers {
        let cc = p.contact_card().await.expect("contact_card");
        server
            .receive_contact_card(&cc)
            .await
            .expect("receive_contact_card");
    }

    // N docs owned by the server, shared per `shape`.
    for i in 0..n {
        let fill = ((i % 251) + 1) as u8;
        let doc = server
            .generate_doc(vec![], nonempty![[fill; 32]])
            .await
            .expect("generate_doc");
        let doc_id = doc.lock().await.doc_id();
        let membered = Membered::Document(doc_id, doc.clone());
        match shape {
            Shape::Public => {
                let public_agent: Agent<_, _, _, _> = Public.individual().into();
                server
                    .add_member(public_agent, &membered, Access::Read, &[])
                    .await
                    .expect("add_member public");
            }
            Shape::Members(_) | Shape::AllOthers => {
                let k = match shape {
                    Shape::Members(k) => k.min(n.saturating_sub(1)),
                    _ => n.saturating_sub(1),
                };
                for step in 1..=k {
                    let j = (i + step) % n;
                    if j == i {
                        continue;
                    }
                    let id: Identifier = peers[j].id().into();
                    if let Some(agent) = server.get_agent(id).await {
                        server
                            .add_member(agent, &membered, Access::Read, &[])
                            .await
                            .expect("add_member peer");
                    }
                }
            }
        }
    }

    let (proto, _shared, _storage) = make_protocol_with_shared_keyhive(server).await;

    let events = proto
        .all_agent_events(&BTreeSet::new())
        .await
        .expect("all_agent_events");
    let sizes = Sizes::from_events(&events);
    (proto, sizes)
}

/// Each of N peers owns its own public doc, adds the server as a Relay member,
/// then syncs keyhive through the server. The server then ingests the graph
/// (peer-owned docs + cross-propagated individual / prekey ops), unlike `build`
/// where the server owns every doc. Returns the server's resulting cache snapshot.
async fn build_synced(n: usize) -> (TestProtocol, Sizes) {
    let server_kh = make_keyhive().await;
    let server_id = keyhive_peer_id(&server_kh);
    let server_cc = server_kh.contact_card().await.expect("server cc");
    let server_identifier: Identifier = server_kh.id().into();

    let mut peer_khs: Vec<SimpleKeyhive> = Vec::with_capacity(n);
    for _ in 0..n {
        peer_khs.push(make_keyhive().await);
    }
    // Contact cards both directions so peer<->server can sync.
    for p in &peer_khs {
        let p_cc = p.contact_card().await.expect("peer cc");
        server_kh
            .receive_contact_card(&p_cc)
            .await
            .expect("server recv cc");
        p.receive_contact_card(&server_cc)
            .await
            .expect("peer recv cc");
    }
    // Each peer owns a doc, makes it public, and grants the server relay (Read).
    for (i, p) in peer_khs.iter().enumerate() {
        let fill = ((i % 251) + 1) as u8;
        let doc = p
            .generate_doc(vec![], nonempty![[fill; 32]])
            .await
            .expect("generate_doc");
        let doc_id = doc.lock().await.doc_id();
        let membered = Membered::Document(doc_id, doc.clone());
        let public_agent: Agent<_, _, _, _> = Public.individual().into();
        p.add_member(public_agent, &membered, Access::Read, &[])
            .await
            .expect("add public");
        let server_agent = p
            .get_agent(server_identifier)
            .await
            .expect("server agent known to peer");
        p.add_member(server_agent, &membered, Access::Read, &[])
            .await
            .expect("add server relay");
    }

    let (server_proto, _server_shared, _) = make_protocol_with_shared_keyhive(server_kh).await;
    for p_kh in peer_khs {
        let p_id = keyhive_peer_id(&p_kh);
        let (p_proto, _p_shared, _) = make_protocol_with_shared_keyhive(p_kh).await;
        let (p_conn, s_conn) = create_channel_pair(p_id.clone(), &server_id);
        p_proto.add_peer(server_id.clone(), p_conn.clone()).await;
        server_proto.add_peer(p_id.clone(), s_conn.clone()).await;
        // Bidirectional sync so the server ingests this peer's keyhive state.
        sync_pair_rounds(
            &p_proto,
            &server_proto,
            &p_id,
            &server_id,
            &p_conn,
            &s_conn,
            3,
        )
        .await;
    }

    let events = server_proto
        .all_agent_events(&BTreeSet::new())
        .await
        .expect("all_agent_events");
    let sizes = Sizes::from_events(&events);
    (server_proto, sizes)
}

/// Benchmark `all_agent_events` (the per-refresh rebuild) across N for one shape,
/// printing the size table to stderr as configurations are built.
fn bench_shape(c: &mut Criterion, label: &str, shape: Shape, sizes: &[usize]) {
    let mut group = c.benchmark_group(format!("all_agent_events/{label}"));
    group.sample_size(10);
    eprintln!("\n=== {label} : cache size scaling ===");
    eprintln!("    N | agents | agent_hash_entries | max/agent | events");
    for &n in sizes {
        let (proto, s) = block_on(build(n, shape));
        eprintln!(
            "{n:5} | {:6} | {:18} | {:9} | {:6}",
            s.agents, s.agent_hash_entries, s.max_per_agent, s.events
        );
        group.bench_with_input(BenchmarkId::from_parameter(n), &proto, |b, proto| {
            b.iter(|| {
                block_on(proto.all_agent_events(&BTreeSet::new())).expect("all_agent_events")
            });
        });
    }
    group.finish();
}

fn bench_synced(c: &mut Criterion, sizes: &[usize]) {
    let mut group = c.benchmark_group("all_agent_events/synced");
    group.sample_size(10);
    eprintln!(
        "\n=== synced (real peers own public docs + server relay, synced) : cache size scaling ==="
    );
    eprintln!("    N | agents | agent_hash_entries | max/agent | events");
    for &n in sizes {
        let (proto, s) = block_on(build_synced(n));
        eprintln!(
            "{n:5} | {:6} | {:18} | {:9} | {:6}",
            s.agents, s.agent_hash_entries, s.max_per_agent, s.events
        );
        group.bench_with_input(BenchmarkId::from_parameter(n), &proto, |b, proto| {
            b.iter(|| {
                block_on(proto.all_agent_events(&BTreeSet::new())).expect("all_agent_events")
            });
        });
    }
    group.finish();
}

fn bench_cache(c: &mut Criterion) {
    let sizes = [25usize, 50, 100, 200];
    bench_shape(c, "public", Shape::Public, &sizes);
    bench_shape(c, "members_k5", Shape::Members(5), &sizes);
    // Dense graph: every agent reads every other doc.
    bench_shape(c, "all_to_all", Shape::AllOthers, &[10usize, 20, 40, 80]);
    // Peers own docs + sync through the server.
    bench_synced(c, &[10usize, 25, 50]);
}

criterion_group!(benches, bench_cache);
criterion_main!(benches);
