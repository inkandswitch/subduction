//! Isolated A/B for the server-side `listen()` dispatch strategy.
//!
//! ## Why this is a separate, synthetic bench
//!
//! The `inbound_dispatch.rs` `server_fan_in` scenario runs the server *and*
//! all client nodes on one shared runtime, so its thread-scaling can't tell us
//! whether the server's single-task `listen()` dispatch is the bottleneck — the
//! clients get the extra cores too. Wiring a real spawned-dispatch variant into
//! the production `listen()` proved invasive (it forces `'static`/`Send` bounds
//! through the whole `StartListener` lifetime chain). So this bench reproduces
//! the *dispatch mechanism* in isolation, with representative per-message work,
//! and A/Bs the two strategies directly:
//!
//! - **`FuturesUnordered`** — every handler future pushed into one
//!   `FuturesUnordered` drained by a single task (today's `listen()` at
//!   `subduction.rs` `in_flight.push(...)`).
//! - **`spawn`** — each handler future `tokio::spawn`-ed onto the worker pool,
//!   completion reported via a channel (the proposed change).
//!
//! ## Representative work
//!
//! Each "message" does one Ed25519 `verify_strict` (the dominant CPU in
//! `recv_commit` / `recv_batch_sync_request`, ~tens of µs) plus a
//! `tokio::task::yield_now()` to model the `.await` points (storage / policy)
//! where the single-task `FuturesUnordered` can interleave but not parallelize.
//!
//! ## What to read
//!
//! For each (strategy × messages × `worker_threads`): wall-clock to process all
//! messages. If `FuturesUnordered` is flat across worker threads while `spawn`
//! falls, the dispatch is the serial wall and spawning would help. If both
//! scale similarly, the dispatch is *not* the bottleneck.
//!
//! ```text
//! cargo bench -p subduction_core --bench listen_dispatch
//! ```

#![allow(missing_docs, unreachable_pub, clippy::expect_used)]

use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ed25519_dalek::{Signer as _, SigningKey, VerifyingKey};
use futures::StreamExt;
use tokio::runtime::Builder as RuntimeBuilder;

const WORKER_THREADS: &[usize] = &[1, 2, 4];
const MESSAGE_COUNTS: &[usize] = &[1000, 4000];

/// A pre-signed message: verifying the signature is the per-message CPU cost.
#[derive(Clone)]
struct Msg {
    key: VerifyingKey,
    sig: ed25519_dalek::Signature,
    payload: [u8; 64],
}

fn make_messages(n: usize) -> Vec<Msg> {
    // Deterministic key from fixed bytes (no rng feature needed); the bench
    // measures verification cost, not key freshness.
    let sk = SigningKey::from_bytes(&[7u8; 32]);
    let key = sk.verifying_key();
    (0..n)
        .map(|i| {
            let mut payload = [0u8; 64];
            payload[..8].copy_from_slice(&(i as u64).to_le_bytes());
            let sig = sk.sign(&payload);
            Msg { key, sig, payload }
        })
        .collect()
}

/// The representative per-message handler: verify (CPU) + an await boundary.
async fn handle(msg: Msg) -> bool {
    // CPU-bound: Ed25519 verification, as in the real inbound handlers.
    let ok = msg.key.verify_strict(&msg.payload, &msg.sig).is_ok();
    // Model the storage/policy `.await` in the real handler: a yield lets the
    // single-task `FuturesUnordered` interleave other handlers here (the source
    // of its concurrency) — but never parallelize the CPU above.
    tokio::task::yield_now().await;
    ok
}

#[derive(Clone, Copy)]
enum Strategy {
    FuturesUnordered,
    Spawn,
}

fn run_dispatch(rt: &tokio::runtime::Runtime, strat: Strategy, messages: &[Msg]) -> Duration {
    let messages = messages.to_vec();
    rt.block_on(async move {
        let start = Instant::now();
        let mut verified = 0usize;

        match strat {
            Strategy::FuturesUnordered => {
                // All handler futures on ONE task's FuturesUnordered.
                let mut in_flight: futures::stream::FuturesUnordered<_> =
                    messages.into_iter().map(handle).collect();
                while let Some(ok) = in_flight.next().await {
                    verified += usize::from(ok);
                }
            }
            Strategy::Spawn => {
                // Each handler spawned onto the worker pool; completion via a
                // channel (the "report (conn, result) back to listen()" shape).
                let (tx, rx) = async_channel::unbounded::<bool>();
                let mut outstanding = 0usize;
                for msg in messages {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let ok = handle(msg).await;
                        let _ = tx.send(ok).await;
                    });
                    outstanding += 1;
                }
                drop(tx);
                while outstanding > 0 {
                    match rx.recv().await {
                        Ok(ok) => {
                            verified += usize::from(ok);
                            outstanding -= 1;
                        }
                        Err(_) => break,
                    }
                }
            }
        }

        let elapsed = start.elapsed();
        assert!(verified > 0, "no messages verified");
        elapsed
    })
}

fn bench_listen_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("listen_dispatch");
    group.sample_size(20);

    for &threads in WORKER_THREADS {
        let rt = RuntimeBuilder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .expect("build multi-thread runtime");

        for &(strat, sname) in &[
            (Strategy::FuturesUnordered, "futuresunordered"),
            (Strategy::Spawn, "spawn"),
        ] {
            for &messages in MESSAGE_COUNTS {
                let corpus = make_messages(messages);
                let id =
                    BenchmarkId::new(format!("wt{threads}_{sname}"), format!("msgs{messages}"));
                group.bench_with_input(id, &corpus, |bch, corpus| {
                    bch.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += run_dispatch(&rt, strat, corpus);
                        }
                        total
                    });
                });
            }
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_listen_dispatch,
}

criterion_main!(benches);
