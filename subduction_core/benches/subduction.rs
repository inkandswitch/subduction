//! Benchmarks for `subduction_core`.
//!
//! These benchmarks focus on the synchronous operations in the crate,
//! including message construction, ID operations, and type manipulations.

#![allow(missing_docs)]

use criterion::{
    BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sedimentree_core::{
    Fragment, LooseCommit, SedimentreeId, SedimentreeSummary,
    blob::{Blob, BlobMeta, Digest},
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use subduction_core::{
    connection::{
        id::ConnectionId,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId, SyncDiff},
    },
    peer::id::PeerId,
    storage::{id::StorageId, key::StorageKey},
};

// =============================================================================
// SYNTHETIC DATA GENERATORS
// =============================================================================

/// Generate a deterministic digest from a seed.
fn digest_from_seed(seed: u64) -> Digest {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    Digest::from(bytes)
}

/// Generate a peer ID from a seed.
fn peer_id_from_seed(seed: u64) -> PeerId {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    PeerId::new(bytes)
}

/// Generate a sedimentree ID from a seed.
fn sedimentree_id_from_seed(seed: u64) -> SedimentreeId {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    SedimentreeId::new(bytes)
}

/// Generate a blob with random data.
fn blob_from_seed(seed: u64, size: usize) -> Blob {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = vec![0u8; size];
    rng.fill(data.as_mut_slice());
    Blob::new(data)
}

/// Generate a loose commit from a seed.
fn loose_commit_from_seed(seed: u64) -> LooseCommit {
    let digest = digest_from_seed(seed);
    let parent = digest_from_seed(seed.wrapping_add(1));
    let blob_meta = BlobMeta::new(&[seed as u8; 64]);
    LooseCommit::new(digest, vec![parent], blob_meta)
}

/// Generate a fragment from a seed.
fn fragment_from_seed(seed: u64, num_parents: usize, num_members: usize) -> Fragment {
    let digest = digest_from_seed(seed);
    let parents: Vec<Digest> = (0..num_parents)
        .map(|i| digest_from_seed(seed.wrapping_add(100 + i as u64)))
        .collect();
    let members: Vec<Digest> = (0..num_members)
        .map(|i| digest_from_seed(seed.wrapping_add(200 + i as u64)))
        .collect();
    let blob_meta = BlobMeta::new(&[seed as u8; 128]);
    Fragment::new(digest, parents, members, blob_meta)
}

/// Generate a request ID from seeds.
fn request_id_from_seed(peer_seed: u64, nonce: u64) -> RequestId {
    RequestId {
        requestor: peer_id_from_seed(peer_seed),
        nonce,
    }
}

/// Generate a storage key with path segments.
fn storage_key_from_seed(seed: u64, depth: usize) -> StorageKey {
    let segments: Vec<String> = (0..depth)
        .map(|i| format!("segment_{seed}_{i}"))
        .collect();
    StorageKey::new(segments)
}

/// Generate a sync diff with commits and fragments.
fn sync_diff_from_seed(seed: u64, num_commits: usize, num_fragments: usize) -> SyncDiff {
    let missing_commits: Vec<(LooseCommit, Blob)> = (0..num_commits)
        .map(|i| {
            let commit = loose_commit_from_seed(seed.wrapping_add(i as u64));
            let blob = blob_from_seed(seed.wrapping_add(1000 + i as u64), 256);
            (commit, blob)
        })
        .collect();

    let missing_fragments: Vec<(Fragment, Blob)> = (0..num_fragments)
        .map(|i| {
            let fragment = fragment_from_seed(seed.wrapping_add(2000 + i as u64), 3, 10);
            let blob = blob_from_seed(seed.wrapping_add(3000 + i as u64), 1024);
            (fragment, blob)
        })
        .collect();

    SyncDiff {
        missing_commits,
        missing_fragments,
    }
}

/// Generate a batch sync request.
fn batch_sync_request_from_seed(seed: u64) -> BatchSyncRequest {
    BatchSyncRequest {
        id: sedimentree_id_from_seed(seed),
        req_id: request_id_from_seed(seed.wrapping_add(1), seed),
        sedimentree_summary: SedimentreeSummary::default(),
    }
}

/// Generate a batch sync response.
fn batch_sync_response_from_seed(
    seed: u64,
    num_commits: usize,
    num_fragments: usize,
) -> BatchSyncResponse {
    BatchSyncResponse {
        id: sedimentree_id_from_seed(seed),
        req_id: request_id_from_seed(seed.wrapping_add(1), seed),
        diff: sync_diff_from_seed(seed.wrapping_add(2), num_commits, num_fragments),
    }
}

// =============================================================================
// PEER ID BENCHMARKS
// =============================================================================

fn bench_peer_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("peer_id");

    // Construction
    group.bench_function("new", |b| {
        let bytes = [42u8; 32];
        b.iter(|| PeerId::new(black_box(bytes)));
    });

    // Accessors
    group.bench_function("as_bytes", |b| {
        let peer_id = peer_id_from_seed(12345);
        b.iter(|| black_box(&peer_id).as_bytes());
    });

    group.bench_function("as_slice", |b| {
        let peer_id = peer_id_from_seed(12345);
        b.iter(|| black_box(&peer_id).as_slice());
    });

    // Display formatting
    group.bench_function("display_format", |b| {
        let peer_id = peer_id_from_seed(12345);
        b.iter(|| format!("{}", black_box(&peer_id)));
    });

    // Comparison
    group.bench_function("compare_equal", |b| {
        let peer_id1 = peer_id_from_seed(12345);
        let peer_id2 = peer_id_from_seed(12345);
        b.iter(|| black_box(&peer_id1) == black_box(&peer_id2));
    });

    group.bench_function("compare_different", |b| {
        let peer_id1 = peer_id_from_seed(12345);
        let peer_id2 = peer_id_from_seed(54321);
        b.iter(|| black_box(&peer_id1).cmp(black_box(&peer_id2)));
    });

    // Hashing
    group.bench_function("hash", |b| {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        let peer_id = peer_id_from_seed(12345);
        b.iter(|| {
            let mut hasher = DefaultHasher::new();
            black_box(&peer_id).hash(&mut hasher);
            hasher.finish()
        });
    });

    group.finish();
}

// =============================================================================
// CONNECTION ID BENCHMARKS
// =============================================================================

fn bench_connection_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("connection_id");

    group.bench_function("new", |b| {
        b.iter(|| ConnectionId::new(black_box(42usize)));
    });

    group.bench_function("compare", |b| {
        let id1 = ConnectionId::new(1);
        let id2 = ConnectionId::new(2);
        b.iter(|| black_box(&id1).cmp(black_box(&id2)));
    });

    group.finish();
}

// =============================================================================
// REQUEST ID BENCHMARKS
// =============================================================================

fn bench_request_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("request_id");

    group.bench_function("new", |b| {
        let peer_id = peer_id_from_seed(12345);
        b.iter(|| RequestId {
            requestor: black_box(peer_id),
            nonce: black_box(42u64),
        });
    });

    group.bench_function("compare_equal", |b| {
        let req1 = request_id_from_seed(12345, 42);
        let req2 = request_id_from_seed(12345, 42);
        b.iter(|| black_box(&req1) == black_box(&req2));
    });

    group.bench_function("compare_by_requestor", |b| {
        let req1 = request_id_from_seed(12345, 100);
        let req2 = request_id_from_seed(54321, 1);
        b.iter(|| black_box(&req1).cmp(black_box(&req2)));
    });

    group.bench_function("compare_by_nonce", |b| {
        let req1 = request_id_from_seed(12345, 1);
        let req2 = request_id_from_seed(12345, 2);
        b.iter(|| black_box(&req1).cmp(black_box(&req2)));
    });

    group.finish();
}

// =============================================================================
// STORAGE KEY BENCHMARKS
// =============================================================================

fn bench_storage_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_key");

    for depth in [1, 3, 5, 10] {
        group.bench_with_input(BenchmarkId::new("new", depth), &depth, |b, &depth| {
            let segments: Vec<String> = (0..depth).map(|i| format!("seg_{i}")).collect();
            b.iter(|| StorageKey::new(black_box(segments.clone())));
        });

        group.bench_with_input(BenchmarkId::new("as_slice", depth), &depth, |b, &depth| {
            let key = storage_key_from_seed(12345, depth);
            b.iter(|| black_box(&key).as_slice());
        });

        group.bench_with_input(BenchmarkId::new("to_vec", depth), &depth, |b, &depth| {
            let key = storage_key_from_seed(12345, depth);
            b.iter(|| black_box(&key).to_vec());
        });

        group.bench_with_input(BenchmarkId::new("into_vec", depth), &depth, |b, &depth| {
            b.iter_batched(
                || storage_key_from_seed(12345, depth),
                subduction_core::storage::key::StorageKey::into_vec,
                criterion::BatchSize::SmallInput,
            );
        });
    }

    // Comparison benchmarks
    group.bench_function("compare_equal", |b| {
        let key1 = storage_key_from_seed(12345, 5);
        let key2 = storage_key_from_seed(12345, 5);
        b.iter(|| black_box(&key1) == black_box(&key2));
    });

    group.bench_function("compare_different_length", |b| {
        let key1 = storage_key_from_seed(12345, 3);
        let key2 = storage_key_from_seed(12345, 5);
        b.iter(|| black_box(&key1).cmp(black_box(&key2)));
    });

    group.finish();
}

// =============================================================================
// STORAGE ID BENCHMARKS
// =============================================================================

fn bench_storage_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_id");

    group.bench_function("new", |b| {
        b.iter(|| StorageId::new(black_box("test-storage-id".to_string())));
    });

    group.bench_function("as_str", |b| {
        let id = StorageId::new("test-storage-id".to_string());
        b.iter(|| black_box(&id).as_str());
    });

    group.bench_function("display_format", |b| {
        let id = StorageId::new("test-storage-id".to_string());
        b.iter(|| format!("{}", black_box(&id)));
    });

    group.bench_function("compare", |b| {
        let id1 = StorageId::new("storage-a".to_string());
        let id2 = StorageId::new("storage-b".to_string());
        b.iter(|| black_box(&id1).cmp(black_box(&id2)));
    });

    group.finish();
}

// =============================================================================
// MESSAGE BENCHMARKS
// =============================================================================

fn bench_message_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_construction");

    // LooseCommit message
    group.bench_function("loose_commit", |b| {
        let id = sedimentree_id_from_seed(12345);
        let commit = loose_commit_from_seed(12345);
        let blob = blob_from_seed(12345, 256);
        b.iter(|| {
            Message::LooseCommit {
                id: black_box(id),
                commit: black_box(commit.clone()),
                blob: black_box(blob.clone()),
            }
        });
    });

    // Fragment message
    group.bench_function("fragment", |b| {
        let id = sedimentree_id_from_seed(12345);
        let fragment = fragment_from_seed(12345, 3, 10);
        let blob = blob_from_seed(12345, 1024);
        b.iter(|| {
            Message::Fragment {
                id: black_box(id),
                fragment: black_box(fragment.clone()),
                blob: black_box(blob.clone()),
            }
        });
    });

    // BlobsRequest message
    for num_digests in [1, 10, 100] {
        group.bench_with_input(
            BenchmarkId::new("blobs_request", num_digests),
            &num_digests,
            |b, &n| {
                let digests: Vec<Digest> = (0..n).map(|i| digest_from_seed(i as u64)).collect();
                b.iter(|| Message::BlobsRequest(black_box(digests.clone())));
            },
        );
    }

    // BlobsResponse message
    for num_blobs in [1, 10, 50] {
        group.bench_with_input(
            BenchmarkId::new("blobs_response", num_blobs),
            &num_blobs,
            |b, &n| {
                let blobs: Vec<Blob> = (0..n).map(|i| blob_from_seed(i as u64, 256)).collect();
                b.iter(|| Message::BlobsResponse(black_box(blobs.clone())));
            },
        );
    }

    // BatchSyncRequest message
    group.bench_function("batch_sync_request", |b| {
        let req = batch_sync_request_from_seed(12345);
        b.iter(|| Message::BatchSyncRequest(black_box(req.clone())));
    });

    // BatchSyncResponse message with varying sizes
    for (commits, fragments) in [(1, 1), (10, 5), (50, 20)] {
        group.bench_with_input(
            BenchmarkId::new("batch_sync_response", format!("{commits}c_{fragments}f")),
            &(commits, fragments),
            |b, &(c, f)| {
                let resp = batch_sync_response_from_seed(12345, c, f);
                b.iter(|| Message::BatchSyncResponse(black_box(resp.clone())));
            },
        );
    }

    group.finish();
}

fn bench_message_request_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_request_id");

    // Messages without request IDs
    group.bench_function("loose_commit_none", |b| {
        let msg = Message::LooseCommit {
            id: sedimentree_id_from_seed(1),
            commit: loose_commit_from_seed(1),
            blob: blob_from_seed(1, 64),
        };
        b.iter(|| black_box(&msg).request_id());
    });

    group.bench_function("fragment_none", |b| {
        let msg = Message::Fragment {
            id: sedimentree_id_from_seed(1),
            fragment: fragment_from_seed(1, 2, 5),
            blob: blob_from_seed(1, 64),
        };
        b.iter(|| black_box(&msg).request_id());
    });

    group.bench_function("blobs_request_none", |b| {
        let msg = Message::BlobsRequest(vec![digest_from_seed(1)]);
        b.iter(|| black_box(&msg).request_id());
    });

    group.bench_function("blobs_response_none", |b| {
        let msg = Message::BlobsResponse(vec![blob_from_seed(1, 64)]);
        b.iter(|| black_box(&msg).request_id());
    });

    // Messages with request IDs
    group.bench_function("batch_sync_request_some", |b| {
        let msg = Message::BatchSyncRequest(batch_sync_request_from_seed(1));
        b.iter(|| black_box(&msg).request_id());
    });

    group.bench_function("batch_sync_response_some", |b| {
        let msg = Message::BatchSyncResponse(batch_sync_response_from_seed(1, 5, 3));
        b.iter(|| black_box(&msg).request_id());
    });

    group.finish();
}

// =============================================================================
// SYNC DIFF BENCHMARKS
// =============================================================================

fn bench_sync_diff(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_diff");

    for (commits, fragments) in [(0, 0), (1, 1), (10, 5), (50, 20), (100, 50)] {
        group.throughput(Throughput::Elements((commits + fragments) as u64));

        group.bench_with_input(
            BenchmarkId::new("construction", format!("{commits}c_{fragments}f")),
            &(commits, fragments),
            |b, &(c, f)| {
                b.iter(|| sync_diff_from_seed(black_box(12345), c, f));
            },
        );
    }

    // Clone benchmark
    for (commits, fragments) in [(10, 5), (50, 20)] {
        group.bench_with_input(
            BenchmarkId::new("clone", format!("{commits}c_{fragments}f")),
            &(commits, fragments),
            |b, &(c, f)| {
                let diff = sync_diff_from_seed(12345, c, f);
                b.iter(|| black_box(&diff).clone());
            },
        );
    }

    group.finish();
}

// =============================================================================
// BATCH SYNC REQUEST/RESPONSE BENCHMARKS
// =============================================================================

fn bench_batch_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_sync");

    // Request construction
    group.bench_function("request_new", |b| {
        let id = sedimentree_id_from_seed(1);
        let req_id = request_id_from_seed(1, 42);
        let summary = SedimentreeSummary::default();
        b.iter(|| BatchSyncRequest {
            id: black_box(id),
            req_id: black_box(req_id),
            sedimentree_summary: black_box(summary.clone()),
        });
    });

    // Request to Message conversion
    group.bench_function("request_into_message", |b| {
        b.iter_batched(
            || batch_sync_request_from_seed(12345),
            |req| -> Message { req.into() },
            criterion::BatchSize::SmallInput,
        );
    });

    // Response construction with varying sizes
    for (commits, fragments) in [(1, 1), (10, 5), (50, 20)] {
        group.bench_with_input(
            BenchmarkId::new("response_new", format!("{commits}c_{fragments}f")),
            &(commits, fragments),
            |b, &(c, f)| {
                let id = sedimentree_id_from_seed(1);
                let req_id = request_id_from_seed(1, 42);
                let diff = sync_diff_from_seed(12345, c, f);
                b.iter(|| BatchSyncResponse {
                    id: black_box(id),
                    req_id: black_box(req_id),
                    diff: black_box(diff.clone()),
                });
            },
        );
    }

    // Response to Message conversion
    group.bench_function("response_into_message", |b| {
        b.iter_batched(
            || batch_sync_response_from_seed(12345, 10, 5),
            |resp| -> Message { resp.into() },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// =============================================================================
// COLLECTION BENCHMARKS (HashMap/BTreeMap with subduction types)
// =============================================================================

fn bench_collections(c: &mut Criterion) {
    let mut group = c.benchmark_group("collections");

    // PeerId in collections
    for size in [10, 100, 1000] {
        // HashMap insertion
        group.bench_with_input(
            BenchmarkId::new("hashmap_peer_id_insert", size),
            &size,
            |b, &size| {
                let peer_ids: Vec<PeerId> = (0..size).map(|i| peer_id_from_seed(i as u64)).collect();
                b.iter(|| {
                    let mut map: HashMap<PeerId, usize> = HashMap::new();
                    for (i, id) in peer_ids.iter().enumerate() {
                        map.insert(*id, i);
                    }
                    map
                });
            },
        );

        // HashMap lookup
        group.bench_with_input(
            BenchmarkId::new("hashmap_peer_id_lookup", size),
            &size,
            |b, &size| {
                let peer_ids: Vec<PeerId> = (0..size).map(|i| peer_id_from_seed(i as u64)).collect();
                let map: HashMap<PeerId, usize> = peer_ids
                    .iter()
                    .enumerate()
                    .map(|(i, id)| (*id, i))
                    .collect();
                let lookup_key = peer_id_from_seed((size / 2) as u64);
                b.iter(|| map.get(black_box(&lookup_key)));
            },
        );

        // BTreeMap insertion
        group.bench_with_input(
            BenchmarkId::new("btreemap_peer_id_insert", size),
            &size,
            |b, &size| {
                let peer_ids: Vec<PeerId> = (0..size).map(|i| peer_id_from_seed(i as u64)).collect();
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
        group.bench_with_input(
            BenchmarkId::new("btreeset_peer_id_contains", size),
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
            BenchmarkId::new("hashset_peer_id_contains", size),
            &size,
            |b, &size| {
                let set: HashSet<PeerId> =
                    (0..size).map(|i| peer_id_from_seed(i as u64)).collect();
                let lookup_key = peer_id_from_seed((size / 2) as u64);
                b.iter(|| set.contains(black_box(&lookup_key)));
            },
        );
    }

    // ConnectionId in BTreeMap (as used in Subduction)
    for size in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("btreemap_connection_id_insert", size),
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
            BenchmarkId::new("btreemap_connection_id_lookup", size),
            &size,
            |b, &size| {
                let map: BTreeMap<ConnectionId, usize> =
                    (0..size).map(|i| (ConnectionId::new(i), i)).collect();
                let lookup_key = ConnectionId::new(size / 2);
                b.iter(|| map.get(black_box(&lookup_key)));
            },
        );
    }

    // SedimentreeId in BTreeMap (as used in Subduction)
    for size in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("btreemap_sedimentree_id_insert", size),
            &size,
            |b, &size| {
                let ids: Vec<SedimentreeId> =
                    (0..size).map(|i| sedimentree_id_from_seed(i as u64)).collect();
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
            BenchmarkId::new("btreemap_sedimentree_id_lookup", size),
            &size,
            |b, &size| {
                let ids: Vec<SedimentreeId> =
                    (0..size).map(|i| sedimentree_id_from_seed(i as u64)).collect();
                let map: BTreeMap<SedimentreeId, usize> = ids
                    .iter()
                    .enumerate()
                    .map(|(i, id)| (*id, i))
                    .collect();
                let lookup_key = sedimentree_id_from_seed((size / 2) as u64);
                b.iter(|| map.get(black_box(&lookup_key)));
            },
        );
    }

    group.finish();
}

// =============================================================================
// CLONING BENCHMARKS
// =============================================================================

fn bench_cloning(c: &mut Criterion) {
    let mut group = c.benchmark_group("cloning");

    // Clone PeerId (Copy type, should be very fast)
    group.bench_function("peer_id", |b| {
        let peer_id = peer_id_from_seed(12345);
        b.iter(|| black_box(peer_id));
    });

    // Clone RequestId
    group.bench_function("request_id", |b| {
        let req_id = request_id_from_seed(12345, 42);
        b.iter(|| black_box(req_id));
    });

    // Clone StorageKey
    for depth in [3, 10] {
        group.bench_with_input(
            BenchmarkId::new("storage_key", depth),
            &depth,
            |b, &depth| {
                let key = storage_key_from_seed(12345, depth);
                b.iter(|| black_box(&key).clone());
            },
        );
    }

    // Clone SyncDiff
    for (commits, fragments) in [(5, 3), (20, 10)] {
        group.bench_with_input(
            BenchmarkId::new("sync_diff", format!("{commits}c_{fragments}f")),
            &(commits, fragments),
            |b, &(c, f)| {
                let diff = sync_diff_from_seed(12345, c, f);
                b.iter(|| black_box(&diff).clone());
            },
        );
    }

    // Clone Message variants
    group.bench_function("message_loose_commit", |b| {
        let msg = Message::LooseCommit {
            id: sedimentree_id_from_seed(1),
            commit: loose_commit_from_seed(1),
            blob: blob_from_seed(1, 256),
        };
        b.iter(|| black_box(&msg).clone());
    });

    group.bench_function("message_fragment", |b| {
        let msg = Message::Fragment {
            id: sedimentree_id_from_seed(1),
            fragment: fragment_from_seed(1, 3, 10),
            blob: blob_from_seed(1, 1024),
        };
        b.iter(|| black_box(&msg).clone());
    });

    group.bench_function("message_batch_sync_response", |b| {
        let msg = Message::BatchSyncResponse(batch_sync_response_from_seed(1, 10, 5));
        b.iter(|| black_box(&msg).clone());
    });

    group.finish();
}

// =============================================================================
// CRITERION GROUPS AND MAIN
// =============================================================================

criterion_group!(
    benches,
    bench_peer_id,
    bench_connection_id,
    bench_request_id,
    bench_storage_key,
    bench_storage_id,
    bench_message_construction,
    bench_message_request_id,
    bench_sync_diff,
    bench_batch_sync,
    bench_collections,
    bench_cloning,
);

criterion_main!(benches);
