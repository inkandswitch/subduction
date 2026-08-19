//! Local (transport-free) handshake benchmarks.
//!
//! Isolates the handshake protocol's CPU cost — two Ed25519 signatures (the
//! initiator's challenge and the responder's response) and the two matching
//! verifications — from any network or WebSocket-framing overhead. Compare
//! against `subduction_websocket`'s `e2e` `handshake` bench (full TCP + WS
//! upgrade + the same crypto) to attribute where the latency actually goes.
//!
//! ```sh
//! cargo bench -p subduction_core --bench handshake
//! ```

#![allow(clippy::expect_used, missing_docs, unreachable_pub)]

use core::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use future_form::Sendable;
use futures::executor::block_on;
use subduction_core::{
    handshake::{self, audience::Audience},
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::memory::MemorySigner};

const MAX_DRIFT: Duration = Duration::from_secs(60);

fn signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn bench_handshake(c: &mut Criterion) {
    let client = signer(1);
    let server = signer(2);
    let now = TimestampSeconds::new(1_000);
    let audience = Audience::discover(b"https://example.com");
    let nonce = Nonce::from_u128(0xABCD_EF01);

    let mut group = c.benchmark_group("handshake_local");

    // The whole protocol with no transport: sign challenge, verify it, sign
    // response, verify it. Two Ed25519 signs + two verifies.
    group.bench_function("full_protocol", |b| {
        b.iter(|| {
            let signed_challenge = block_on(handshake::create_challenge::<Sendable, _>(
                &client, audience, now, nonce,
            ));
            let verified =
                handshake::verify_challenge(&signed_challenge, &audience, now, MAX_DRIFT)
                    .expect("verify challenge");
            let signed_response = block_on(handshake::create_response::<Sendable, _>(
                &server,
                &verified.challenge,
                now,
            ));
            handshake::verify_response(&signed_response, &verified.challenge)
                .expect("verify response")
        });
    });

    // Initiator: sign the challenge (one Ed25519 sign + encode).
    group.bench_function("create_challenge", |b| {
        b.iter(|| {
            block_on(handshake::create_challenge::<Sendable, _>(
                &client, audience, now, nonce,
            ))
        });
    });

    let signed_challenge = block_on(handshake::create_challenge::<Sendable, _>(
        &client, audience, now, nonce,
    ));
    let challenge = handshake::verify_challenge(&signed_challenge, &audience, now, MAX_DRIFT)
        .expect("verify challenge")
        .challenge;

    // Responder: verify the challenge (one Ed25519 verify + validation).
    group.bench_function("verify_challenge", |b| {
        b.iter(|| {
            handshake::verify_challenge(&signed_challenge, &audience, now, MAX_DRIFT)
                .expect("verify challenge")
        });
    });

    // Responder: sign the response (one Ed25519 sign + encode).
    group.bench_function("create_response", |b| {
        b.iter(|| {
            block_on(handshake::create_response::<Sendable, _>(
                &server, &challenge, now,
            ))
        });
    });

    let signed_response = block_on(handshake::create_response::<Sendable, _>(
        &server, &challenge, now,
    ));

    // Initiator: verify the response (one Ed25519 verify + validation).
    group.bench_function("verify_response", |b| {
        b.iter(|| {
            handshake::verify_response(&signed_response, &challenge).expect("verify response")
        });
    });

    group.finish();
}

criterion_group!(benches, bench_handshake);
criterion_main!(benches);
