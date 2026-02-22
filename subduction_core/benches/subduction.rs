//! Benchmarks for `subduction_core`.
//!
//! Run with: `cargo bench -p subduction_core`
//!
//! ## Benchmark Philosophy
//!
//! These benchmarks cover the sync protocol's data types and operations. They're organized into:
//!
//! | Category              | Description |
//! |-----------------------|-------------|
//! | Microbenchmarks       | ID types, accessors, trivial operations. Sub-microsecond, kept for regression detection. |
//! | Scaling benchmarks    | Collection operations, message construction at various sizes. |
//! | Protocol operations   | `SyncDiff`, `BatchSync` request/response construction. |
//!
//! ## What's NOT Tested Here
//!
//! - Actual sync protocol execution (see integration tests)
//! - Message serialization/deserialization
//! - WebSocket round-trip latency
//! - Storage I/O
//! - Async runtime overhead

#![allow(missing_docs, unreachable_pub)]

use criterion::{criterion_group, criterion_main};

mod generators {
    use future_form::Sendable;
    use futures::executor::block_on;
    use std::collections::BTreeSet;

    use rand::{Rng, SeedableRng, rngs::StdRng};
    use sedimentree_core::{
        blob::{Blob, BlobMeta},
        crypto::{digest::Digest, fingerprint::FingerprintSeed},
        fragment::Fragment,
        id::SedimentreeId,
        loose_commit::LooseCommit,
        sedimentree::FingerprintSummary,
    };
    use subduction_core::{
        connection::message::{
            BatchSyncRequest, BatchSyncResponse, RequestId, RequestedData, SyncDiff, SyncResult,
        },
        peer::id::PeerId,
        storage::key::StorageKey,
    };
    use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};

    /// Generate a deterministic commit digest from a seed.
    pub(super) fn digest_from_seed(seed: u64) -> Digest<LooseCommit> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        Digest::from_bytes(bytes)
    }

    /// Generate a deterministic blob digest from a seed.
    pub(super) fn blob_digest_from_seed(seed: u64) -> Digest<Blob> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        Digest::from_bytes(bytes)
    }

    /// Generate a peer ID from a seed.
    pub(super) fn peer_id_from_seed(seed: u64) -> PeerId {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        PeerId::new(bytes)
    }

    /// Generate a sedimentree ID from a seed.
    pub(super) fn sedimentree_id_from_seed(seed: u64) -> SedimentreeId {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        SedimentreeId::new(bytes)
    }

    /// Generate a blob with random data.
    pub(super) fn blob_from_seed(seed: u64, size: usize) -> Blob {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut data = vec![0u8; size];
        rng.fill(data.as_mut_slice());
        Blob::new(data)
    }

    /// Generate a loose commit from a seed.
    pub(super) fn loose_commit_from_seed(seed: u64) -> LooseCommit {
        let digest = digest_from_seed(seed);
        let parent = digest_from_seed(seed.wrapping_add(1));
        #[allow(clippy::cast_possible_truncation)]
        let blob_meta = BlobMeta::new(&[seed as u8; 64]);
        LooseCommit::new(digest, BTreeSet::from([parent]), blob_meta)
    }

    /// Generate a fragment from a seed.
    #[allow(clippy::cast_sign_loss)]
    pub(super) fn fragment_from_seed(
        seed: u64,
        num_parents: usize,
        num_members: usize,
    ) -> Fragment {
        let digest = digest_from_seed(seed);
        let parents: BTreeSet<Digest<LooseCommit>> = (0..num_parents)
            .map(|i| digest_from_seed(seed.wrapping_add(100 + i as u64)))
            .collect();
        let members: Vec<Digest<LooseCommit>> = (0..num_members)
            .map(|i| digest_from_seed(seed.wrapping_add(200 + i as u64)))
            .collect();
        #[allow(clippy::cast_possible_truncation)]
        let blob_meta = BlobMeta::new(&[seed as u8; 128]);
        Fragment::new(digest, parents, &members, blob_meta)
    }

    /// Generate a request ID from seeds.
    pub(super) fn request_id_from_seed(peer_seed: u64, nonce: u64) -> RequestId {
        RequestId {
            requestor: peer_id_from_seed(peer_seed),
            nonce,
        }
    }

    /// Generate a storage key with path segments.
    pub(super) fn storage_key_from_seed(seed: u64, depth: usize) -> StorageKey {
        let segments: Vec<String> = (0..depth).map(|i| format!("segment_{seed}_{i}")).collect();
        StorageKey::new(segments)
    }

    /// Generate a signer from a seed.
    pub(super) fn signer_from_seed(seed: u64) -> MemorySigner {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        MemorySigner::from_bytes(&bytes)
    }

    /// Generate a signed loose commit from a seed.
    pub(super) fn signed_loose_commit_from_seed(seed: u64) -> Signed<LooseCommit> {
        let signer = signer_from_seed(seed);
        let commit = loose_commit_from_seed(seed);
        let ctx = sedimentree_id_from_seed(seed);
        block_on(Signed::seal::<Sendable, _>(&signer, commit, &ctx)).into_signed()
    }

    /// Generate a signed fragment from a seed.
    pub(super) fn signed_fragment_from_seed(seed: u64) -> Signed<Fragment> {
        let signer = signer_from_seed(seed);
        let fragment = fragment_from_seed(seed, 3, 10);
        let ctx = sedimentree_id_from_seed(seed);
        block_on(Signed::seal::<Sendable, _>(&signer, fragment, &ctx)).into_signed()
    }

    /// Generate a sync diff with commits and fragments.
    pub(super) fn sync_diff_from_seed(
        seed: u64,
        num_commits: usize,
        num_fragments: usize,
        blob_size: usize,
    ) -> SyncDiff {
        let signer = signer_from_seed(seed);
        let ctx = sedimentree_id_from_seed(seed);

        let missing_commits: Vec<(Signed<LooseCommit>, Blob)> = (0..num_commits)
            .map(|i| {
                let commit = loose_commit_from_seed(seed.wrapping_add(i as u64));
                let verified = block_on(Signed::seal::<Sendable, _>(&signer, commit, &ctx));
                let blob = blob_from_seed(seed.wrapping_add(1000 + i as u64), blob_size);
                (verified.into_signed(), blob)
            })
            .collect();

        let missing_fragments: Vec<(Signed<Fragment>, Blob)> = (0..num_fragments)
            .map(|i| {
                let fragment = fragment_from_seed(seed.wrapping_add(2000 + i as u64), 3, 10);
                let verified = block_on(Signed::seal::<Sendable, _>(&signer, fragment, &ctx));
                let blob = blob_from_seed(seed.wrapping_add(3000 + i as u64), blob_size * 4);
                (verified.into_signed(), blob)
            })
            .collect();

        SyncDiff {
            missing_commits,
            missing_fragments,
            requesting: RequestedData {
                commit_fingerprints: Vec::new(),
                fragment_fingerprints: Vec::new(),
            },
        }
    }

    /// Generate a batch sync request.
    pub(super) fn batch_sync_request_from_seed(seed: u64) -> BatchSyncRequest {
        BatchSyncRequest {
            id: sedimentree_id_from_seed(seed),
            req_id: request_id_from_seed(seed, seed),
            fingerprint_summary: FingerprintSummary::new(
                FingerprintSeed::new(seed, seed),
                BTreeSet::new(),
                BTreeSet::new(),
            ),
            subscribe: false,
        }
    }

    /// Generate a batch sync response.
    pub(super) fn batch_sync_response_from_seed(
        seed: u64,
        num_commits: usize,
        num_fragments: usize,
        blob_size: usize,
    ) -> BatchSyncResponse {
        BatchSyncResponse {
            id: sedimentree_id_from_seed(seed),
            req_id: request_id_from_seed(seed, seed),
            result: SyncResult::Ok(sync_diff_from_seed(
                seed,
                num_commits,
                num_fragments,
                blob_size,
            )),
        }
    }
}

mod id {
    use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, black_box};
    use sedimentree_core::id::SedimentreeId;
    use subduction_core::{
        connection::{id::ConnectionId, message::RequestId},
        peer::id::PeerId,
        storage::{id::StorageId, key::StorageKey},
    };

    use super::generators::{peer_id_from_seed, request_id_from_seed, storage_key_from_seed};

    /// Microbenchmarks for identifier types (`PeerId`, `ConnectionId`, `RequestId`).
    ///
    /// **Intent**: Establish baseline costs for ID operations. These are foundational
    /// types used throughout the sync protocol.
    ///
    /// **Expected performance**: Sub-100ns for all operations. These are effectively
    /// just byte array/integer wrappers.
    pub fn bench_id_micros(c: &mut Criterion) {
        let mut group = c.benchmark_group("id_micro");

        // PeerId operations
        group.bench_function("peer_id/new", |b| {
            let bytes = [42u8; 32];
            b.iter(|| PeerId::new(black_box(bytes)));
        });

        group.bench_function("peer_id/as_bytes", |b| {
            let peer_id = peer_id_from_seed(12345);
            b.iter(|| black_box(&peer_id).as_bytes());
        });

        group.bench_function("peer_id/compare", |b| {
            let peer_id1 = peer_id_from_seed(12345);
            let peer_id2 = peer_id_from_seed(54321);
            b.iter(|| black_box(&peer_id1).cmp(black_box(&peer_id2)));
        });

        group.bench_function("peer_id/hash", |b| {
            use std::{
                collections::hash_map::DefaultHasher,
                hash::{Hash, Hasher},
            };
            let peer_id = peer_id_from_seed(12345);
            b.iter(|| {
                let mut hasher = DefaultHasher::new();
                black_box(&peer_id).hash(&mut hasher);
                hasher.finish()
            });
        });

        // ConnectionId operations
        group.bench_function("connection_id/new", |b| {
            b.iter(|| ConnectionId::new(black_box(42usize)));
        });

        group.bench_function("connection_id/compare", |b| {
            let id1 = ConnectionId::new(1);
            let id2 = ConnectionId::new(2);
            b.iter(|| black_box(&id1).cmp(black_box(&id2)));
        });

        // RequestId operations
        group.bench_function("request_id/new", |b| {
            let peer_id = peer_id_from_seed(12345);
            b.iter(|| RequestId {
                requestor: black_box(peer_id),
                nonce: black_box(42u64),
            });
        });

        group.bench_function("request_id/compare", |b| {
            let req1 = request_id_from_seed(12345, 42);
            let req2 = request_id_from_seed(54321, 1);
            b.iter(|| black_box(&req1).cmp(black_box(&req2)));
        });

        // SedimentreeId operations
        group.bench_function("sedimentree_id/new", |b| {
            let bytes = [42u8; 32];
            b.iter(|| SedimentreeId::new(black_box(bytes)));
        });

        // StorageId operations
        group.bench_function("storage_id/new", |b| {
            b.iter(|| StorageId::new(black_box("test-storage-id".to_string())));
        });

        group.bench_function("storage_id/as_str", |b| {
            let id = StorageId::new("test-storage-id".to_string());
            b.iter(|| black_box(&id).as_str());
        });

        group.finish();
    }

    /// Microbenchmarks for `StorageKey` operations.
    ///
    /// **Intent**: Measure cost of path-based storage key operations.
    /// `StorageKeys` are used for addressing data in storage backends.
    ///
    /// **Expected complexity**: O(depth) for most operations.
    pub fn bench_storage_key(c: &mut Criterion) {
        let mut group = c.benchmark_group("storage_key");

        for depth in [1, 3, 5, 10, 20] {
            group.throughput(Throughput::Elements(depth as u64));

            group.bench_with_input(BenchmarkId::new("new", depth), &depth, |b, &depth| {
                b.iter_batched(
                    || (0..depth).map(|i| format!("seg_{i}")).collect::<Vec<_>>(),
                    |segments| StorageKey::new(black_box(segments)),
                    BatchSize::SmallInput,
                );
            });

            group.bench_with_input(BenchmarkId::new("as_slice", depth), &depth, |b, &depth| {
                let key = storage_key_from_seed(12345, depth);
                b.iter(|| black_box(&key).as_slice());
            });

            group.bench_with_input(BenchmarkId::new("to_vec", depth), &depth, |b, &depth| {
                let key = storage_key_from_seed(12345, depth);
                b.iter(|| black_box(&key).to_vec());
            });

            group.bench_with_input(BenchmarkId::new("clone", depth), &depth, |b, &depth| {
                let key = storage_key_from_seed(12345, depth);
                b.iter(|| black_box(&key).clone());
            });
        }

        group.finish();
    }
}

mod message {
    use criterion::{BenchmarkId, Criterion, Throughput, black_box};
    use subduction_core::connection::message::Message;

    use super::generators::{
        batch_sync_request_from_seed, batch_sync_response_from_seed, blob_digest_from_seed,
        blob_from_seed, sedimentree_id_from_seed, signed_fragment_from_seed,
        signed_loose_commit_from_seed,
    };

    /// Benchmark Message enum construction with various payloads.
    ///
    /// **Intent**: Measure the cost of building protocol messages. These are constructed
    /// during sync operations before serialization.
    ///
    /// **Note**: These benchmarks include cloning of input data as part of the cost,
    /// which reflects real usage where messages typically own their data.
    pub fn bench_message_construction(c: &mut Criterion) {
        let mut group = c.benchmark_group("message_construction");

        // LooseCommit message (single commit + blob)
        for blob_size in [64, 256, 1024, 4096] {
            group.throughput(Throughput::Bytes(blob_size as u64));

            group.bench_with_input(
                BenchmarkId::new("loose_commit", blob_size),
                &blob_size,
                |b, &size| {
                    let id = sedimentree_id_from_seed(12345);
                    let commit = signed_loose_commit_from_seed(12345);
                    let blob = blob_from_seed(12345, size);
                    b.iter(|| Message::LooseCommit {
                        id: black_box(id),
                        commit: black_box(commit.clone()),
                        blob: black_box(blob.clone()),
                    });
                },
            );
        }

        // Fragment message
        for blob_size in [256, 1024, 4096, 16384] {
            group.throughput(Throughput::Bytes(blob_size as u64));

            group.bench_with_input(
                BenchmarkId::new("fragment", blob_size),
                &blob_size,
                |b, &size| {
                    let id = sedimentree_id_from_seed(12345);
                    let fragment = signed_fragment_from_seed(12345);
                    let blob = blob_from_seed(12345, size);
                    b.iter(|| Message::Fragment {
                        id: black_box(id),
                        fragment: black_box(fragment.clone()),
                        blob: black_box(blob.clone()),
                    });
                },
            );
        }

        // BlobsRequest message (varying number of digests)
        #[allow(clippy::cast_sign_loss)]
        for num_digests in [1, 10, 50, 100, 500] {
            group.throughput(Throughput::Elements(num_digests as u64));

            group.bench_with_input(
                BenchmarkId::new("blobs_request", num_digests),
                &num_digests,
                |b, &n| {
                    let digests: Vec<_> = (0..n).map(|i| blob_digest_from_seed(i as u64)).collect();
                    b.iter(|| Message::BlobsRequest {
                        id: black_box(sedimentree_id_from_seed(1)),
                        digests: black_box(digests.clone()),
                    });
                },
            );
        }

        // BlobsResponse message (varying number and size of blobs)
        #[allow(clippy::cast_sign_loss)]
        for (num_blobs, blob_size) in [(1, 256), (10, 256), (50, 256), (10, 4096), (50, 4096)] {
            let total_bytes = num_blobs * blob_size;
            group.throughput(Throughput::Bytes(total_bytes as u64));

            group.bench_with_input(
                BenchmarkId::new("blobs_response", format!("{num_blobs}x{blob_size}")),
                &(num_blobs, blob_size),
                |b, &(n, size)| {
                    let blobs: Vec<_> = (0..n).map(|i| blob_from_seed(i as u64, size)).collect();
                    b.iter(|| Message::BlobsResponse {
                        id: black_box(sedimentree_id_from_seed(1)),
                        blobs: black_box(blobs.clone()),
                    });
                },
            );
        }

        group.finish();
    }

    /// Benchmark extracting request IDs from messages.
    ///
    /// **Intent**: Measure the cost of routing messages by request ID.
    /// This is a simple pattern match operation.
    ///
    /// **Expected performance**: Sub-10ns - just enum variant matching.
    pub fn bench_message_request_id(c: &mut Criterion) {
        let mut group = c.benchmark_group("message_request_id");

        // Messages without request IDs (should return None quickly)
        let msg_loose = Message::LooseCommit {
            id: sedimentree_id_from_seed(1),
            commit: signed_loose_commit_from_seed(1),
            blob: blob_from_seed(1, 64),
        };
        group.bench_function("loose_commit_none", |b| {
            b.iter(|| black_box(&msg_loose).request_id());
        });

        let msg_blobs_req = Message::BlobsRequest {
            id: sedimentree_id_from_seed(1),
            digests: vec![blob_digest_from_seed(1)],
        };
        group.bench_function("blobs_request_none", |b| {
            b.iter(|| black_box(&msg_blobs_req).request_id());
        });

        // Messages with request IDs
        let msg_batch_req = Message::BatchSyncRequest(batch_sync_request_from_seed(1));
        group.bench_function("batch_sync_request_some", |b| {
            b.iter(|| black_box(&msg_batch_req).request_id());
        });

        let msg_batch_resp =
            Message::BatchSyncResponse(batch_sync_response_from_seed(1, 5, 3, 256));
        group.bench_function("batch_sync_response_some", |b| {
            b.iter(|| black_box(&msg_batch_resp).request_id());
        });

        group.finish();
    }
}

mod sync {
    use std::collections::BTreeSet;

    use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, black_box};
    use sedimentree_core::{crypto::fingerprint::FingerprintSeed, sedimentree::FingerprintSummary};
    use subduction_core::connection::message::{
        BatchSyncRequest, BatchSyncResponse, Message, SyncResult,
    };

    use super::generators::{
        batch_sync_request_from_seed, batch_sync_response_from_seed, request_id_from_seed,
        sedimentree_id_from_seed, sync_diff_from_seed,
    };

    /// Benchmark `SyncDiff` construction and cloning at various scales.
    ///
    /// **Intent**: Measure the cost of building sync diffs, which represent the
    /// data delta to send to a peer. Includes blob data allocation.
    ///
    /// **Expected complexity**: O(commits + fragments) for construction,
    /// with significant constant factor from blob allocation.
    pub fn bench_sync_diff(c: &mut Criterion) {
        let mut group = c.benchmark_group("sync_diff");

        // Varying number of items with fixed blob size
        for (commits, fragments) in [(1, 1), (10, 5), (50, 20), (100, 50), (200, 100)] {
            let total = commits + fragments;
            group.throughput(Throughput::Elements(total as u64));

            group.bench_with_input(
                BenchmarkId::new("construction_256b", format!("{commits}c_{fragments}f")),
                &(commits, fragments),
                |b, &(c, f)| {
                    b.iter(|| sync_diff_from_seed(black_box(12345), c, f, 256));
                },
            );
        }

        // Varying blob size with fixed item count
        for blob_size in [64, 256, 1024, 4096, 16384] {
            let commits = 20;
            let fragments = 10;
            let total_bytes = commits * blob_size + fragments * blob_size * 4;
            group.throughput(Throughput::Bytes(total_bytes as u64));

            group.bench_with_input(
                BenchmarkId::new("construction_varying_blob", blob_size),
                &blob_size,
                |b, &size| {
                    b.iter(|| sync_diff_from_seed(black_box(12345), commits, fragments, size));
                },
            );
        }

        // Clone benchmarks (important for message passing)
        for (commits, fragments) in [(10, 5), (50, 20), (100, 50)] {
            let total = commits + fragments;
            group.throughput(Throughput::Elements(total as u64));

            let diff = sync_diff_from_seed(12345, commits, fragments, 256);
            group.bench_with_input(
                BenchmarkId::new("clone", format!("{commits}c_{fragments}f")),
                &diff,
                |b, diff| {
                    b.iter(|| black_box(diff).clone());
                },
            );
        }

        group.finish();
    }

    /// Benchmark `BatchSyncRequest` and `BatchSyncResponse` operations.
    ///
    /// **Intent**: Measure the cost of constructing the main sync protocol messages.
    /// These wrap `SyncDiff` with routing metadata.
    ///
    /// **Note**: Construction cost is dominated by `SyncDiff` for responses.
    pub fn bench_batch_sync(c: &mut Criterion) {
        let mut group = c.benchmark_group("batch_sync");

        // Request construction (lightweight - just metadata)
        group.bench_function("request_new", |b| {
            let id = sedimentree_id_from_seed(1);
            let req_id = request_id_from_seed(1, 42);
            let fp_summary = FingerprintSummary::new(
                FingerprintSeed::new(1, 2),
                BTreeSet::new(),
                BTreeSet::new(),
            );
            b.iter(|| BatchSyncRequest {
                id: black_box(id),
                req_id: black_box(req_id),
                fingerprint_summary: black_box(fp_summary.clone()),
                subscribe: false,
            });
        });

        // Request to Message conversion
        group.bench_function("request_into_message", |b| {
            b.iter_batched(
                || batch_sync_request_from_seed(12345),
                |req| -> Message { req.into() },
                BatchSize::SmallInput,
            );
        });

        // Response construction with varying payload sizes
        for (commits, fragments) in [(1, 1), (10, 5), (50, 20), (100, 50)] {
            let total = commits + fragments;
            group.throughput(Throughput::Elements(total as u64));

            group.bench_with_input(
                BenchmarkId::new("response_new", format!("{commits}c_{fragments}f")),
                &(commits, fragments),
                |b, &(c, f)| {
                    b.iter_batched(
                        || {
                            let id = sedimentree_id_from_seed(1);
                            let req_id = request_id_from_seed(1, 42);
                            let diff = sync_diff_from_seed(12345, c, f, 256);
                            (id, req_id, diff)
                        },
                        |(id, req_id, diff)| BatchSyncResponse {
                            id: black_box(id),
                            req_id: black_box(req_id),
                            result: SyncResult::Ok(black_box(diff)),
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        // Response to Message conversion
        for (commits, fragments) in [(10, 5), (50, 20)] {
            group.bench_with_input(
                BenchmarkId::new("response_into_message", format!("{commits}c_{fragments}f")),
                &(commits, fragments),
                |b, &(c, f)| {
                    b.iter_batched(
                        || batch_sync_response_from_seed(12345, c, f, 256),
                        |resp| -> Message { resp.into() },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        group.finish();
    }
}

mod collections {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

    use criterion::{BenchmarkId, Criterion, Throughput, black_box};
    use sedimentree_core::id::SedimentreeId;
    use subduction_core::{connection::id::ConnectionId, peer::id::PeerId};

    use super::generators::{peer_id_from_seed, sedimentree_id_from_seed};

    /// Benchmark collection operations with subduction ID types as keys.
    ///
    /// **Intent**: Measure HashMap/BTreeMap performance with our ID types. The sync
    /// protocol maintains maps of peers, connections, and sedimentrees.
    ///
    /// **Expected complexity**: O(1) average for `HashMap`, O(log n) for `BTreeMap`.
    /// Hash quality of our ID types affects `HashMap` performance.
    #[allow(clippy::too_many_lines, clippy::cast_sign_loss)]
    pub fn bench_collections(c: &mut Criterion) {
        let mut group = c.benchmark_group("collections");

        for size in [10, 100, 1000, 10000] {
            group.throughput(Throughput::Elements(size as u64));

            // HashMap insertion (bulk)
            group.bench_with_input(
                BenchmarkId::new("hashmap_peer_id/insert_all", size),
                &size,
                |b, &size| {
                    let peer_ids: Vec<PeerId> =
                        (0..size).map(|i| peer_id_from_seed(i as u64)).collect();
                    b.iter(|| {
                        let mut map: HashMap<PeerId, usize> = HashMap::with_capacity(size);
                        for (i, id) in peer_ids.iter().enumerate() {
                            map.insert(*id, i);
                        }
                        map
                    });
                },
            );

            // HashMap lookup (single key)
            #[allow(clippy::cast_sign_loss)]
            group.bench_with_input(
                BenchmarkId::new("hashmap_peer_id/lookup", size),
                &size,
                |b, &size| {
                    let peer_ids: Vec<PeerId> =
                        (0..size).map(|i| peer_id_from_seed(i as u64)).collect();
                    let map: HashMap<PeerId, usize> = peer_ids
                        .iter()
                        .enumerate()
                        .map(|(i, id)| (*id, i))
                        .collect();
                    let lookup_key = peer_id_from_seed((size / 2) as u64);
                    b.iter(|| map.get(black_box(&lookup_key)));
                },
            );

            // HashMap lookup (missing key)
            group.bench_with_input(
                BenchmarkId::new("hashmap_peer_id/lookup_miss", size),
                &size,
                |b, &size| {
                    let peer_ids: Vec<PeerId> =
                        (0..size).map(|i| peer_id_from_seed(i as u64)).collect();
                    let map: HashMap<PeerId, usize> = peer_ids
                        .iter()
                        .enumerate()
                        .map(|(i, id)| (*id, i))
                        .collect();
                    let missing_key = peer_id_from_seed(999_999); // Not in map
                    b.iter(|| map.get(black_box(&missing_key)));
                },
            );

            // BTreeMap insertion
            group.bench_with_input(
                BenchmarkId::new("btreemap_peer_id/insert_all", size),
                &size,
                |b, &size| {
                    let peer_ids: Vec<PeerId> =
                        (0..size).map(|i| peer_id_from_seed(i as u64)).collect();
                    b.iter(|| {
                        let mut map: BTreeMap<PeerId, usize> = BTreeMap::new();
                        for (i, id) in peer_ids.iter().enumerate() {
                            map.insert(*id, i);
                        }
                        map
                    });
                },
            );

            // BTreeSet contains
            #[allow(clippy::cast_sign_loss)]
            group.bench_with_input(
                BenchmarkId::new("btreeset_peer_id/contains", size),
                &size,
                |b, &size| {
                    let set: BTreeSet<PeerId> =
                        (0..size).map(|i| peer_id_from_seed(i as u64)).collect();
                    let lookup_key = peer_id_from_seed((size / 2) as u64);
                    b.iter(|| set.contains(black_box(&lookup_key)));
                },
            );

            // HashSet contains
            group.bench_with_input(
                BenchmarkId::new("hashset_peer_id/contains", size),
                &size,
                |b, &size| {
                    let set: HashSet<PeerId> =
                        (0..size).map(|i| peer_id_from_seed(i as u64)).collect();
                    let lookup_key = peer_id_from_seed((size / 2) as u64);
                    b.iter(|| set.contains(black_box(&lookup_key)));
                },
            );
        }

        for size in [10, 100, 1000, 10000] {
            group.throughput(Throughput::Elements(size as u64));

            group.bench_with_input(
                BenchmarkId::new("btreemap_connection_id/insert_all", size),
                &size,
                |b, &size| {
                    b.iter(|| {
                        let mut map: BTreeMap<ConnectionId, usize> = BTreeMap::new();
                        for i in 0..size {
                            map.insert(ConnectionId::new(i), i);
                        }
                        map
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("btreemap_connection_id/lookup", size),
                &size,
                |b, &size| {
                    let map: BTreeMap<ConnectionId, usize> =
                        (0..size).map(|i| (ConnectionId::new(i), i)).collect();
                    let lookup_key = ConnectionId::new(size / 2);
                    b.iter(|| map.get(black_box(&lookup_key)));
                },
            );
        }

        #[allow(clippy::cast_sign_loss)]
        for size in [10, 100, 1000, 10000] {
            group.throughput(Throughput::Elements(size as u64));

            group.bench_with_input(
                BenchmarkId::new("btreemap_sedimentree_id/insert_all", size),
                &size,
                |b, &size| {
                    let ids: Vec<SedimentreeId> = (0..size)
                        .map(|i| sedimentree_id_from_seed(i as u64))
                        .collect();
                    b.iter(|| {
                        let mut map: BTreeMap<SedimentreeId, usize> = BTreeMap::new();
                        for (i, id) in ids.iter().enumerate() {
                            map.insert(*id, i);
                        }
                        map
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("btreemap_sedimentree_id/lookup", size),
                &size,
                |b, &size| {
                    let ids: Vec<SedimentreeId> = (0..size)
                        .map(|i| sedimentree_id_from_seed(i as u64))
                        .collect();
                    let map: BTreeMap<SedimentreeId, usize> =
                        ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
                    let lookup_key = sedimentree_id_from_seed((size / 2) as u64);
                    b.iter(|| map.get(black_box(&lookup_key)));
                },
            );
        }

        group.finish();
    }
}

mod cloning {
    use criterion::{BenchmarkId, Criterion, Throughput, black_box};
    use subduction_core::connection::{id::ConnectionId, message::Message};

    use super::generators::{
        batch_sync_response_from_seed, blob_from_seed, request_id_from_seed,
        sedimentree_id_from_seed, signed_fragment_from_seed, signed_loose_commit_from_seed,
        storage_key_from_seed, sync_diff_from_seed,
    };

    /// Benchmark cloning costs for various protocol types.
    ///
    /// **Intent**: Cloning is common in async message passing. Understanding clone
    /// costs helps identify where Arc/Rc might be beneficial.
    ///
    /// **Note**: Small ID types (`PeerId`, `RequestId`) are Copy/cheap-Clone.
    /// Larger types (`SyncDiff`, Messages) have significant clone costs.
    pub fn bench_cloning(c: &mut Criterion) {
        let mut group = c.benchmark_group("cloning");

        // Small types (should be very fast)
        group.bench_function("peer_id", |b| {
            let peer_id = super::generators::peer_id_from_seed(12345);
            b.iter(|| black_box(peer_id)); // Copy
        });

        group.bench_function("request_id", |b| {
            let req_id = request_id_from_seed(12345, 42);
            b.iter(|| black_box(req_id)); // Copy
        });

        group.bench_function("sedimentree_id", |b| {
            let id = sedimentree_id_from_seed(12345);
            b.iter(|| black_box(id)); // Copy
        });

        group.bench_function("connection_id", |b| {
            let id = ConnectionId::new(42);
            b.iter(|| black_box(id)); // Copy
        });

        // StorageKey (heap allocated)
        for depth in [3, 10, 20] {
            group.bench_with_input(
                BenchmarkId::new("storage_key", depth),
                &depth,
                |b, &depth| {
                    let key = storage_key_from_seed(12345, depth);
                    b.iter(|| black_box(&key).clone());
                },
            );
        }

        // SyncDiff (significant clone cost)
        for (commits, fragments) in [(5, 3), (20, 10), (50, 25)] {
            let diff = sync_diff_from_seed(12345, commits, fragments, 256);
            let total = commits + fragments;
            group.throughput(Throughput::Elements(total as u64));

            group.bench_with_input(
                BenchmarkId::new("sync_diff", format!("{commits}c_{fragments}f")),
                &diff,
                |b, diff| {
                    b.iter(|| black_box(diff).clone());
                },
            );
        }

        // Message variants
        let msg_loose = Message::LooseCommit {
            id: sedimentree_id_from_seed(1),
            commit: signed_loose_commit_from_seed(1),
            blob: blob_from_seed(1, 256),
        };
        group.bench_function("message/loose_commit_256b", |b| {
            b.iter(|| black_box(&msg_loose).clone());
        });

        let msg_fragment = Message::Fragment {
            id: sedimentree_id_from_seed(1),
            fragment: signed_fragment_from_seed(1),
            blob: blob_from_seed(1, 1024),
        };
        group.bench_function("message/fragment_1kb", |b| {
            b.iter(|| black_box(&msg_fragment).clone());
        });

        let msg_batch_resp =
            Message::BatchSyncResponse(batch_sync_response_from_seed(1, 20, 10, 256));
        group.bench_function("message/batch_sync_response_20c_10f", |b| {
            b.iter(|| black_box(&msg_batch_resp).clone());
        });

        // Large message (stress test)
        let msg_large_resp =
            Message::BatchSyncResponse(batch_sync_response_from_seed(1, 100, 50, 1024));
        group.bench_function("message/batch_sync_response_100c_50f_1kb", |b| {
            b.iter(|| black_box(&msg_large_resp).clone());
        });

        group.finish();
    }
}

mod display {
    use criterion::{Criterion, black_box};
    use subduction_core::storage::id::StorageId;

    use super::generators::{digest_from_seed, peer_id_from_seed, sedimentree_id_from_seed};

    /// Benchmark Display implementations for ID types.
    ///
    /// **Intent**: Display is used in logging and debugging. High-frequency logging
    /// could make this a bottleneck.
    ///
    /// **Expected performance**: Dominated by string allocation and hex encoding.
    pub fn bench_display(c: &mut Criterion) {
        let mut group = c.benchmark_group("display");

        group.bench_function("peer_id", |b| {
            let peer_id = peer_id_from_seed(12345);
            b.iter(|| format!("{}", black_box(&peer_id)));
        });

        group.bench_function("sedimentree_id", |b| {
            let id = sedimentree_id_from_seed(12345);
            b.iter(|| format!("{}", black_box(&id)));
        });

        group.bench_function("storage_id", |b| {
            let id = StorageId::new("test-storage-id".to_string());
            b.iter(|| format!("{}", black_box(&id)));
        });

        group.bench_function("digest", |b| {
            let digest = digest_from_seed(12345);
            b.iter(|| format!("{}", black_box(&digest)));
        });

        group.finish();
    }
}

criterion_group!(
    benches,
    id::bench_id_micros,
    id::bench_storage_key,
    message::bench_message_construction,
    message::bench_message_request_id,
    sync::bench_sync_diff,
    sync::bench_batch_sync,
    collections::bench_collections,
    cloning::bench_cloning,
    display::bench_display,
);

criterion_main!(benches);
