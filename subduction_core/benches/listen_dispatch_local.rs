//! Isolated A/B for the `listen()` dispatch strategy on the **single-threaded**
//! (`Local` / Wasm) runtime.
//!
//! ## Why a separate Local bench
//!
//! `listen_dispatch.rs` measures the `Sendable` path on a multi-thread tokio
//! runtime, where `spawn` wins by spreading handlers across worker cores. The
//! `Local` path has **no cores to spread across** — `WasmSpawn` is
//! `wasm_bindgen_futures::spawn_local`, a single-threaded task spawn onto the
//! event loop. So spawning cannot buy parallelism here; the only thing it can
//! do is *add per-message scheduling overhead* relative to the previous
//! design, where every handler future was multiplexed in one
//! `FuturesUnordered` polled inline by the listener task.
//!
//! This bench A/Bs those two strategies on a single-threaded runtime to answer:
//! **does routing each handler through `spawn_local` measurably degrade the
//! Local path vs. the inline `FuturesUnordered`?**
//!
//! - **`FuturesUnordered`** — the previous Local behaviour: all handler futures
//!   in one `FuturesUnordered`, polled inline on the listener task.
//! - **`spawn_local`** — the current Local behaviour: each handler spawned as
//!   its own task on a `tokio::task::LocalSet`, completion reported via a
//!   channel. `tokio`'s `spawn_local` is the native stand-in for
//!   `wasm_bindgen_futures::spawn_local`: both are single-threaded local task
//!   spawns, so the relative overhead is representative even though absolute
//!   timings differ from a browser.
//!
//! ## Representative work
//!
//! Each "message" does one Ed25519 `verify_strict` (the dominant CPU in the
//! real inbound handlers) plus a `yield_now()` to model the storage/policy
//! `.await` point where the inline `FuturesUnordered` interleaves other
//! handlers.
//!
//! ## What to read
//!
//! Single-threaded, so neither strategy parallelizes. If `spawn_local` is
//! consistently *slower* than `FuturesUnordered` by more than noise, the Local
//! path pays a real per-message spawn tax for the `Sendable` parallelism win —
//! quantify it here. If they track each other, the change is free on Local.
//!
//! ```text
//! cargo bench -p subduction_core --bench listen_dispatch_local
//! ```

#![allow(missing_docs, unreachable_pub, clippy::expect_used)]

use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ed25519_dalek::{Signer as _, SigningKey, VerifyingKey};
use futures::StreamExt;
use tokio::{runtime::Builder as RuntimeBuilder, task::LocalSet};

const MESSAGE_COUNTS: &[usize] = &[1000, 4000];

/// A pre-signed message: verifying the signature is the per-message CPU cost.
#[derive(Clone)]
struct Msg {
    key: VerifyingKey,
    sig: ed25519_dalek::Signature,
    payload: [u8; 64],
}

fn make_messages(n: usize) -> Vec<Msg> {
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
    let ok = msg.key.verify_strict(&msg.payload, &msg.sig).is_ok();
    // Model the storage/policy `.await` in the real handler.
    tokio::task::yield_now().await;
    ok
}

#[derive(Clone, Copy)]
enum Strategy {
    /// Previous Local behaviour: inline `FuturesUnordered`.
    FuturesUnordered,
    /// Current Local behaviour: `spawn_local` per handler.
    SpawnLocal,
}

/// Run one dispatch pass on a single-threaded runtime + `LocalSet`.
fn run_dispatch_local(
    rt: &tokio::runtime::Runtime,
    local: &LocalSet,
    strat: Strategy,
    messages: &[Msg],
) -> Duration {
    let messages = messages.to_vec();
    local.block_on(rt, async move {
        let start = Instant::now();
        let mut verified = 0usize;

        match strat {
            Strategy::FuturesUnordered => {
                let mut in_flight: futures::stream::FuturesUnordered<_> =
                    messages.into_iter().map(handle).collect();
                while let Some(ok) = in_flight.next().await {
                    verified += usize::from(ok);
                }
            }
            Strategy::SpawnLocal => {
                // Mirrors the production Local path: each handler `spawn_local`'d,
                // completion reported back through a channel (the
                // "report (conn, result) to listen()" shape).
                let (tx, rx) = async_channel::unbounded::<bool>();
                let mut outstanding = 0usize;
                for msg in messages {
                    let tx = tx.clone();
                    tokio::task::spawn_local(async move {
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

fn bench_listen_dispatch_local(c: &mut Criterion) {
    let mut group = c.benchmark_group("listen_dispatch_local");
    group.sample_size(20);

    let rt = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");

    for &(strat, sname) in &[
        (Strategy::FuturesUnordered, "futuresunordered"),
        (Strategy::SpawnLocal, "spawn_local"),
    ] {
        for &messages in MESSAGE_COUNTS {
            let corpus = make_messages(messages);
            let id = BenchmarkId::new(sname, format!("msgs{messages}"));
            group.bench_with_input(id, &corpus, |bch, corpus| {
                bch.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Fresh LocalSet per pass: a used LocalSet cannot be
                        // re-driven after its tasks complete.
                        let local = LocalSet::new();
                        total += run_dispatch_local(&rt, &local, strat, corpus);
                    }
                    total
                });
            });
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_listen_dispatch_local,
}
criterion_main!(benches);
