//! `Subduction::get_heads`: heads for a chosen list of sedimentrees without
//! hydrating the whole node.
//!
//! Covers equivalence with `get_all_heads`, residency (a queried cold tree is
//! not left resident and never evicts others; resident trees are served
//! without storage reads), and the edge cases (unknown id, empty tree,
//! duplicates, empty input, per-tree storage failure).

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use future_form::{FutureForm, Sendable};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sedimentree_core::{
    blob::Blob,
    collections::Set,
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    connection::test_utils::{MockConnection, TokioSpawn, TokioTimeout},
    handler::sync::SyncHandler,
    policy::open::OpenPolicy,
    storage::{
        memory::{MemoryStorage, MemoryStorageError},
        traits::Storage,
    },
    subduction::{Subduction, builder::SubductionBuilder},
};
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};
use testresult::TestResult;

// ============================================================================
// Probe storage: MemoryStorage + read counters + injectable per-tree failure
// ============================================================================

#[derive(Debug, thiserror::Error)]
enum ProbeError {
    #[error("injected failure")]
    Injected,
    #[error(transparent)]
    Inner(#[from] MemoryStorageError),
}

#[derive(Debug, Default)]
struct Counters {
    /// `load_loose_commit_metas` + `load_fragment_metas` calls.
    meta_loads: AtomicUsize,
    /// Calls that read blobs (`load_loose_commits`, `load_fragments`, singles).
    blob_loads: AtomicUsize,
    /// `contains_sedimentree_id` calls.
    contains: AtomicUsize,
}

impl Counters {
    fn total(&self) -> usize {
        self.meta_loads.load(Ordering::SeqCst)
            + self.blob_loads.load(Ordering::SeqCst)
            + self.contains.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Clone, Default)]
struct Probe {
    inner: MemoryStorage,
    counters: Arc<Counters>,
    fail_for: Arc<Mutex<Option<SedimentreeId>>>,
}

impl Probe {
    fn check(&self, id: SedimentreeId) -> Result<(), ProbeError> {
        if *self.fail_for.lock().expect("lock") == Some(id) {
            Err(ProbeError::Injected)
        } else {
            Ok(())
        }
    }
}

macro_rules! fwd {
    ($self:ident, $method:ident($($arg:ident),*)) => {
        Sendable::from_future(async move {
            <MemoryStorage as Storage<Sendable>>::$method(&$self.inner, $($arg),*).await.map_err(ProbeError::from)
        })
    };
}

impl Storage<Sendable> for Probe {
    type Error = ProbeError;

    fn save_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        fwd!(self, save_sedimentree_id(id))
    }

    fn delete_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        fwd!(self, delete_sedimentree_id(id))
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        fwd!(self, load_all_sedimentree_ids())
    }

    fn contains_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<bool, Self::Error>> {
        self.counters.contains.fetch_add(1, Ordering::SeqCst);
        Sendable::from_future(async move {
            self.check(id)?;
            Ok(
                <MemoryStorage as Storage<Sendable>>::contains_sedimentree_id(&self.inner, id)
                    .await?,
            )
        })
    }

    fn save_loose_commit(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        fwd!(self, save_loose_commit(id, verified))
    }

    fn list_commit_ids(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
        fwd!(self, list_commit_ids(id))
    }

    fn load_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>>
    {
        self.counters.blob_loads.fetch_add(1, Ordering::SeqCst);
        Sendable::from_future(async move {
            self.check(id)?;
            Ok(<MemoryStorage as Storage<Sendable>>::load_loose_commits(&self.inner, id).await?)
        })
    }

    fn load_loose_commit_metas(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Vec<LooseCommit>, Self::Error>> {
        self.counters.meta_loads.fetch_add(1, Ordering::SeqCst);
        Sendable::from_future(async move {
            self.check(id)?;
            Ok(
                <MemoryStorage as Storage<Sendable>>::load_loose_commit_metas(&self.inner, id)
                    .await?,
            )
        })
    }

    fn load_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>>
    {
        self.counters.blob_loads.fetch_add(1, Ordering::SeqCst);
        fwd!(self, load_loose_commit(id, commit_id))
    }

    fn delete_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        fwd!(self, delete_loose_commit(id, commit_id))
    }

    fn delete_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        fwd!(self, delete_loose_commits(id))
    }

    fn save_fragment(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        fwd!(self, save_fragment(id, verified))
    }

    fn load_fragment(
        &self,
        id: SedimentreeId,
        head: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>
    {
        self.counters.blob_loads.fetch_add(1, Ordering::SeqCst);
        fwd!(self, load_fragment(id, head))
    }

    fn list_fragment_ids(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
        fwd!(self, list_fragment_ids(id))
    }

    fn load_fragments(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>>
    {
        self.counters.blob_loads.fetch_add(1, Ordering::SeqCst);
        Sendable::from_future(async move {
            self.check(id)?;
            Ok(<MemoryStorage as Storage<Sendable>>::load_fragments(&self.inner, id).await?)
        })
    }

    fn load_fragment_metas(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Vec<Fragment>, Self::Error>> {
        self.counters.meta_loads.fetch_add(1, Ordering::SeqCst);
        Sendable::from_future(async move {
            self.check(id)?;
            Ok(<MemoryStorage as Storage<Sendable>>::load_fragment_metas(&self.inner, id).await?)
        })
    }

    fn delete_fragment(
        &self,
        id: SedimentreeId,
        head: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        fwd!(self, delete_fragment(id, head))
    }

    fn delete_fragments(
        &self,
        id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        fwd!(self, delete_fragments(id))
    }

    fn save_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<usize, Self::Error>> {
        fwd!(self, save_batch(id, commits, fragments))
    }
}

// ============================================================================
// Node helpers
// ============================================================================

type Conn = MockConnection;
type TestSyncHandler =
    SyncHandler<Sendable, Probe, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>;
type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        Probe,
        Conn,
        TestSyncHandler,
        OpenPolicy,
        MemorySigner,
        TokioTimeout,
        TokioSpawn,
    >,
>;

/// A node over `storage` whose resident cache is capped at `cap` trees.
fn node(storage: Probe, cap: usize) -> TestSubduction {
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(MemorySigner::from_bytes(&[7u8; 32]))
        .storage(storage, Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .max_resident_trees(cap)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

fn sid(n: u32) -> SedimentreeId {
    let mut bytes = [0u8; 32];
    bytes[0..4].copy_from_slice(&n.to_le_bytes());
    SedimentreeId::new(bytes)
}

fn cid(n: u32) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0..4].copy_from_slice(&n.to_le_bytes());
    bytes[4] = 0x01;
    CommitId::new(bytes)
}

fn blob(seed: u32) -> Blob {
    Blob::new(vec![seed.to_le_bytes()[0]; 32])
}

/// Populate `count` random trees: loose commits forming small DAGs plus
/// fragments with arbitrary boundaries/checkpoints. Tree `i` gets id `sid(i)`.
/// Some trees come out empty (registered, no data).
async fn populate(sd: &TestSubduction, storage: &Probe, count: u32, seed: u64) -> TestResult {
    let mut rng = StdRng::seed_from_u64(seed);
    for t in 0..count {
        let id = sid(t);
        let n_commits: usize = rng.gen_range(0..6);
        let n_frags: usize = rng.gen_range(0..3);
        if n_commits == 0 && n_frags == 0 {
            register(storage, id).await?;
            continue;
        }
        let pool: Vec<CommitId> = (0..n_commits + 4)
            .map(|i| cid(t * 100 + u32::try_from(i).expect("small")))
            .collect();
        for i in 0..n_commits {
            let mut parents = BTreeSet::new();
            if i > 0 {
                for _ in 0..=rng.gen_range(0..2) {
                    parents.insert(pool[rng.gen_range(0..i)]);
                }
            }
            sd.store_commit(id, pool[i], parents, blob(t + u32::try_from(i)?))
                .await?;
        }
        for f in 0..n_frags {
            let head = pool[rng.gen_range(0..pool.len())];
            let boundary: BTreeSet<CommitId> = (0..=rng.gen_range(0..2))
                .map(|_| pool[rng.gen_range(0..pool.len())])
                .filter(|b| *b != head)
                .collect();
            let checkpoints: Vec<CommitId> = (0..rng.gen_range(0..3))
                .map(|_| pool[rng.gen_range(0..pool.len())])
                .collect();
            sd.store_fragment(
                id,
                head,
                boundary,
                &checkpoints,
                blob(t + u32::try_from(f)? + 50),
            )
            .await?;
        }
    }
    Ok(())
}

async fn register(storage: &Probe, id: SedimentreeId) -> Result<(), MemoryStorageError> {
    <MemoryStorage as Storage<Sendable>>::save_sedimentree_id(&storage.inner, id).await
}

fn sorted(mut v: Vec<CommitId>) -> Vec<CommitId> {
    v.sort();
    v
}

fn ok_map(
    got: Vec<(SedimentreeId, Result<Vec<CommitId>, ProbeError>)>,
) -> BTreeMap<SedimentreeId, Vec<CommitId>> {
    got.into_iter()
        .map(|(id, r)| (id, sorted(r.expect("no per-tree failure"))))
        .collect()
}

fn all_map(all: Vec<(SedimentreeId, Vec<CommitId>)>) -> BTreeMap<SedimentreeId, Vec<CommitId>> {
    all.into_iter().map(|(id, h)| (id, sorted(h))).collect()
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn equals_get_all_heads_cold_and_warm() -> TestResult {
    let storage = Probe::default();
    let writer = node(storage.clone(), 1024);
    populate(&writer, &storage, 40, 0x5EED).await?;

    // Reference: a separate cold node's full sweep.
    let reference = all_map(node(storage.clone(), 1024).get_all_heads().await);
    assert_eq!(reference.len(), 40);
    assert!(
        reference.values().any(Vec::is_empty),
        "fixture should include empty trees"
    );
    assert!(
        reference.values().any(|h| !h.is_empty()),
        "fixture should include non-empty trees"
    );

    let wanted: Vec<SedimentreeId> = (0..40).filter(|n| n % 3 != 1).map(sid).collect();
    let expected: BTreeMap<_, _> = reference
        .iter()
        .filter(|(id, _)| wanted.contains(id))
        .map(|(id, h)| (*id, h.clone()))
        .collect();

    // Cold node.
    let cold = node(storage.clone(), 1024);
    assert_eq!(ok_map(cold.get_heads(&wanted).await), expected);

    // Warm node: sweep first so trees are resident, then query.
    let warm = node(storage.clone(), 1024);
    drop(warm.get_all_heads().await);
    assert_eq!(ok_map(warm.get_heads(&wanted).await), expected);

    // Both agree on every id at once too.
    let everything: Vec<SedimentreeId> = (0..40).map(sid).collect();
    assert_eq!(ok_map(cold.get_heads(&everything).await), reference);
    Ok(())
}

#[tokio::test]
async fn cold_query_leaves_no_residency_and_reads_no_blobs() -> TestResult {
    let storage = Probe::default();
    populate(&node(storage.clone(), 1024), &storage, 20, 7).await?;

    let cold = node(storage.clone(), 1024);
    assert_eq!(cold.resident_sedimentree_count().await, 0);

    let ids: Vec<SedimentreeId> = (0..20).map(sid).collect();
    let before_blob = storage.counters.blob_loads.load(Ordering::SeqCst);
    let got = cold.get_heads(&ids).await;
    assert_eq!(got.len(), 20);

    assert_eq!(
        cold.resident_sedimentree_count().await,
        0,
        "queried trees must not stay resident"
    );
    assert_eq!(
        storage.counters.blob_loads.load(Ordering::SeqCst),
        before_blob,
        "heads must be computed from metadata only"
    );
    Ok(())
}

#[tokio::test]
async fn resident_trees_untouched_and_served_without_storage_reads() -> TestResult {
    let storage = Probe::default();
    populate(&node(storage.clone(), 1024), &storage, 10, 11).await?;

    let sd = node(storage.clone(), 1024);
    // Make trees 0..5 resident.
    for n in 0..5 {
        drop(sd.get_commits(sid(n)).await);
    }
    let resident_before = sd.resident_sedimentree_count().await;
    assert!(resident_before > 0);

    // Resident-only query: no storage access at all.
    let resident_ids: Vec<SedimentreeId> = (0..5).map(sid).collect();
    let reads_before = storage.counters.total();
    drop(sd.get_heads(&resident_ids).await);
    assert_eq!(storage.counters.total(), reads_before);
    assert_eq!(sd.resident_sedimentree_count().await, resident_before);

    // Mixed query: resident set unchanged.
    let mixed: Vec<SedimentreeId> = (0..10).map(sid).collect();
    drop(sd.get_heads(&mixed).await);
    assert_eq!(sd.resident_sedimentree_count().await, resident_before);
    Ok(())
}

#[tokio::test]
async fn query_does_not_grow_a_full_cache_or_evict() -> TestResult {
    let storage = Probe::default();
    populate(&node(storage.clone(), 1024), &storage, 600, 3).await?;

    // Tiny cap: at most one tree per shard resident.
    let sd = node(storage.clone(), 4);
    let hot = sid(0);
    drop(sd.get_commits(hot).await);
    let before = sd.resident_sedimentree_count().await;

    let ids: Vec<SedimentreeId> = (0..600).map(sid).collect();
    let before_reads = storage.counters.total();
    drop(sd.get_heads(&ids).await);
    assert!(storage.counters.total() > before_reads);
    assert_eq!(sd.resident_sedimentree_count().await, before);

    // The hot tree is still resident: querying it needs no storage read.
    let reads = storage.counters.total();
    drop(sd.get_heads(&[hot]).await);
    assert_eq!(storage.counters.total(), reads);
    Ok(())
}

#[tokio::test]
async fn edge_cases() -> TestResult {
    let storage = Probe::default();
    let sd = node(storage.clone(), 1024);
    sd.store_commit(sid(1), cid(1), BTreeSet::new(), blob(1))
        .await?;
    register(&storage, sid(2)).await?; // registered, empty

    let cold = node(storage.clone(), 1024);

    // Empty input.
    assert!(cold.get_heads(&[]).await.is_empty());

    // Unknown id is omitted; empty tree is present with no heads.
    let got = cold.get_heads(&[sid(99), sid(2), sid(1)]).await;
    let got = ok_map(got);
    assert_eq!(got.len(), 2);
    assert_eq!(got[&sid(1)], vec![cid(1)]);
    assert!(got[&sid(2)].is_empty());

    // Duplicates are answered once, in first-occurrence order.
    let got = cold.get_heads(&[sid(1), sid(2), sid(1), sid(1)]).await;
    let order: Vec<_> = got.iter().map(|(id, _)| *id).collect();
    assert_eq!(order, vec![sid(1), sid(2)]);
    Ok(())
}

#[tokio::test]
async fn failing_tree_is_reported_per_id_without_failing_the_rest() -> TestResult {
    let storage = Probe::default();
    let sd = node(storage.clone(), 1024);
    for n in 1..=3 {
        sd.store_commit(sid(n), cid(n), BTreeSet::new(), blob(n))
            .await?;
    }
    *storage.fail_for.lock().expect("lock") = Some(sid(2));

    let cold = node(storage.clone(), 1024);
    let got = cold.get_heads(&[sid(1), sid(2), sid(3)]).await;
    assert_eq!(got.len(), 3);
    assert!(matches!(&got[0], (id, Ok(h)) if *id == sid(1) && *h == vec![cid(1)]));
    assert!(matches!(&got[1], (id, Err(_)) if *id == sid(2)));
    assert!(matches!(&got[2], (id, Ok(h)) if *id == sid(3) && *h == vec![cid(3)]));
    assert_eq!(cold.resident_sedimentree_count().await, 0);
    Ok(())
}
