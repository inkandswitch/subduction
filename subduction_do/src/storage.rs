//! [`Storage<Local>`] backed by the Durable Object's embedded SQLite database.
//!
//! The DO SQLite API is *synchronous* (`exec` returns a cursor immediately), so
//! each async method here just runs its query and wraps the result in a ready
//! future. Commits and fragments are stored compound (signed bytes + blob) in
//! one row, keyed by `(tree, head, content_digest)` — the trailing digest lets
//! Byzantine-equivocating payloads for the same head coexist, matching the
//! `redb` backend's semantics.
//!
//! This is what makes hibernation safe: all durable sync state lives in SQLite,
//! so when the isolate is evicted and later reconstructed, the engine
//! re-hydrates trees from here on demand.
//!
//! # Backend abstraction
//!
//! The SQL work is written against the tiny [`Sql`] trait rather than
//! `worker::SqlStorage` directly. In the Worker isolate the backend is
//! [`wasm::WorkerSql`] (over the DO's SQLite); in host unit tests it is an
//! in-memory `rusqlite` connection. Everything above the trait — schema,
//! encode/decode, subscriptions, replay nonces, and compaction — is therefore
//! exercised natively with `cargo test`, no `workerd` required.

use std::{cell::Cell, rc::Rc};

use future_form::{FutureForm, Local};
use futures::future::LocalBoxFuture;
use sedimentree_core::{
    blob::Blob,
    codec::error::DecodeError,
    collections::Set,
    crypto::digest::Digest,
    depth::DepthMetric,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
    sedimentree::Sedimentree,
};
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use thiserror::Error;

/// `meta` key holding the reserved high-water mark for the per-peer send
/// counter base (see [`SqlStore::reserve_counter_base`]).
const COUNTER_BASE_KEY: &str = "send_counter_base";

/// A SQL value, mirroring the small subset of column types this layer uses.
///
/// Deliberately backend-agnostic so the same query code runs against the DO's
/// `worker::SqlStorage` in production and `rusqlite` in host tests.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    /// SQL `NULL`.
    Null,
    /// 64-bit signed integer.
    Integer(i64),
    /// Double-precision float.
    Float(f64),
    /// UTF-8 text.
    Text(String),
    /// Binary blob.
    Blob(Vec<u8>),
}

/// A minimal synchronous SQL backend: bind parameters, run a statement, and get
/// back every result row as raw column values.
pub trait Sql: Clone {
    /// Execute `query` with positional `binds`, returning all result rows.
    ///
    /// Non-`SELECT` statements return an empty row vector.
    ///
    /// # Errors
    ///
    /// Returns [`DoStorageError::Sql`] if the statement fails.
    fn exec(&self, query: &str, binds: Vec<SqlValue>)
        -> Result<Vec<Vec<SqlValue>>, DoStorageError>;
}

/// Errors from the SQLite-backed storage.
#[derive(Debug, Error)]
pub enum DoStorageError {
    /// A SQL statement failed.
    #[error("sql error: {0}")]
    Sql(String),
    /// A stored row could not be decoded back into a payload.
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),
    /// A column had an unexpected SQL type.
    #[error("unexpected column type in {0}")]
    UnexpectedColumn(&'static str),
}

/// Storage backend over a synchronous [`Sql`] connection.
#[derive(Clone, Debug)]
pub struct SqlStore<S: Sql> {
    sql: S,
    /// Set whenever a fragment is written, since a new fragment may make some
    /// loose commits redundant. Read-and-cleared to decide whether an alarm
    /// should be scheduled to compact (see the Durable Object's `on_sync`).
    compaction_hint: Rc<Cell<bool>>,
}

impl<S: Sql> SqlStore<S> {
    /// Wrap a SQL backend.
    #[must_use]
    pub fn from_backend(sql: S) -> Self {
        Self {
            sql,
            compaction_hint: Rc::new(Cell::new(false)),
        }
    }

    /// Create the tables the sync layer needs. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns any error from the `CREATE TABLE` statements.
    pub fn init_schema(&self) -> Result<(), DoStorageError> {
        self.run(
            "CREATE TABLE IF NOT EXISTS trees (id BLOB PRIMARY KEY);",
            vec![],
        )?;
        self.run(
            "CREATE TABLE IF NOT EXISTS commits (
                 tree BLOB NOT NULL,
                 head BLOB NOT NULL,
                 digest BLOB NOT NULL,
                 signed BLOB NOT NULL,
                 blob BLOB NOT NULL,
                 PRIMARY KEY (tree, head, digest)
             );",
            vec![],
        )?;
        self.run(
            "CREATE TABLE IF NOT EXISTS fragments (
                 tree BLOB NOT NULL,
                 head BLOB NOT NULL,
                 digest BLOB NOT NULL,
                 signed BLOB NOT NULL,
                 blob BLOB NOT NULL,
                 PRIMARY KEY (tree, head, digest)
             );",
            vec![],
        )?;
        self.run(
            "CREATE TABLE IF NOT EXISTS subscriptions (
                 tree BLOB NOT NULL,
                 peer BLOB NOT NULL,
                 PRIMARY KEY (tree, peer)
             );",
            vec![],
        )?;
        // Durable replay protection: a claimed `(peer, nonce)` from a successful
        // handshake, valid until `expires_at` (challenge timestamp + max drift).
        // Survives hibernation, unlike the in-memory `NonceCache`.
        self.run(
            "CREATE TABLE IF NOT EXISTS nonces (
                 peer BLOB NOT NULL,
                 nonce BLOB NOT NULL,
                 expires_at INTEGER NOT NULL,
                 PRIMARY KEY (peer, nonce)
             );",
            vec![],
        )?;
        self.run(
            "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v BLOB NOT NULL);",
            vec![],
        )?;
        Ok(())
    }

    // ---- meta key/value (server signer seed, etc.) -----------------------

    /// Read a raw value from the `meta` table.
    ///
    /// # Errors
    ///
    /// Returns any SQL error.
    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>, DoStorageError> {
        let rows = self.query(
            "SELECT v FROM meta WHERE k = ?;",
            vec![SqlValue::Text(key.to_string())],
        )?;
        match rows.into_iter().next() {
            Some(row) => Ok(Some(blob_at(&row, 0, "meta.v")?)),
            None => Ok(None),
        }
    }

    /// Write a raw value to the `meta` table.
    ///
    /// # Errors
    ///
    /// Returns any SQL error.
    pub fn put_meta(&self, key: &str, value: Vec<u8>) -> Result<(), DoStorageError> {
        self.run(
            "INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?);",
            vec![SqlValue::Text(key.to_string()), SqlValue::Blob(value)],
        )
    }

    /// Reserve a fresh, strictly-increasing base for the per-peer send counter
    /// and return it.
    ///
    /// Each call reads the persisted high-water base, advances it by `stride`,
    /// and persists the new value — so successive Durable Object lifetimes get
    /// disjoint, increasing `[base, base + stride)` ranges. Seeding the
    /// in-memory `PeerCounter` from this base (see `PeerCounter::advance_to`)
    /// keeps outgoing counters monotonic across hibernation, as long as one
    /// lifetime never issues more than `stride` messages to a single peer.
    ///
    /// The advance is `saturating` and clamped to `u64::MAX - 1`: once the base
    /// reaches that ceiling it stops growing and successive lifetimes reuse it,
    /// which could let counters collide. With the deployed `stride` of `2^32`
    /// that needs `2^32` isolate re-creations of a single document —
    /// practically unreachable — so we cap rather than wrap (which would
    /// silently rewind the sequence). The base is never `u64::MAX` because
    /// seeding `PeerCounter` with it would force the next stamp to wrap to `0`
    /// (see [`PeerCounter::advance_to`], which also clamps defensively).
    ///
    /// [`PeerCounter::advance_to`]: subduction_core::peer::counter::PeerCounter::advance_to
    ///
    /// # Errors
    ///
    /// Returns any SQL error.
    pub fn reserve_counter_base(&self, stride: u64) -> Result<u64, DoStorageError> {
        let base = self
            .get_meta(COUNTER_BASE_KEY)?
            .and_then(|b| <[u8; 8]>::try_from(b).ok())
            .map_or(0, u64::from_le_bytes)
            .min(u64::MAX - 1);
        self.put_meta(
            COUNTER_BASE_KEY,
            base.saturating_add(stride)
                .min(u64::MAX - 1)
                .to_le_bytes()
                .to_vec(),
        )?;
        Ok(base)
    }

    // ---- subscriptions (persisted so they survive hibernation) -----------

    /// Load every persisted `(tree, peer)` subscription pair.
    ///
    /// # Errors
    ///
    /// Returns any SQL error, or a decode error if a stored id is malformed.
    pub fn load_subscriptions(&self) -> Result<Vec<(SedimentreeId, [u8; 32])>, DoStorageError> {
        let rows = self.query("SELECT tree, peer FROM subscriptions;", vec![])?;
        let mut out = Vec::new();
        for row in rows {
            let tree =
                SedimentreeId::new(array32(&blob_at(&row, 0, "subscriptions.tree")?, "tree")?);
            let peer = array32(&blob_at(&row, 1, "subscriptions.peer")?, "peer")?;
            out.push((tree, peer));
        }
        Ok(out)
    }

    /// Replace the persisted subscription set with `pairs`.
    ///
    /// Called after handling any message that could mutate subscriptions, so
    /// the on-disk set stays in sync with the in-memory map across hibernation.
    ///
    /// The `DELETE` and the per-row `INSERT`s run as one **synchronous** burst
    /// with no intervening `await`. On the Durable Object backend such a burst is
    /// coalesced and persisted atomically *if it runs to completion*, and
    /// `sql.exec()` rejects explicit `BEGIN`/`COMMIT` — so do **not** add one, it
    /// would error at runtime.
    ///
    /// This is **not** a hard all-or-nothing guarantee: the method returns the
    /// SQL error instead of panicking, so a failure after the `DELETE` (quota,
    /// corruption, …) can leave a partial/empty set on disk. That's acceptable
    /// here because the in-memory subscription map stays authoritative for the
    /// live isolate and the next mutating message rewrites the full set; callers
    /// log the error (see `teardown`) rather than trusting the on-disk set after
    /// a fault.
    ///
    /// # Errors
    ///
    /// Returns any SQL error.
    pub fn replace_subscriptions(
        &self,
        pairs: &[(SedimentreeId, [u8; 32])],
    ) -> Result<(), DoStorageError> {
        self.run("DELETE FROM subscriptions;", vec![])?;
        for (tree, peer) in pairs {
            self.run(
                "INSERT OR IGNORE INTO subscriptions (tree, peer) VALUES (?, ?);",
                vec![
                    SqlValue::Blob(tree.as_bytes().to_vec()),
                    SqlValue::Blob(peer.to_vec()),
                ],
            )?;
        }
        Ok(())
    }

    // ---- replay nonces (durable across hibernation) ----------------------

    /// Whether this `(peer, nonce)` pair has already been claimed and is still
    /// within its validity window (`expires_at > now`).
    ///
    /// # Errors
    ///
    /// Returns any SQL error.
    pub fn nonce_seen(
        &self,
        peer: &[u8; 32],
        nonce: &[u8; 16],
        now: u64,
    ) -> Result<bool, DoStorageError> {
        let rows = self.query(
            "SELECT 1 FROM nonces WHERE peer = ? AND nonce = ? AND expires_at > ? LIMIT 1;",
            vec![
                SqlValue::Blob(peer.to_vec()),
                SqlValue::Blob(nonce.to_vec()),
                SqlValue::Integer(clamp_i64(now)),
            ],
        )?;
        Ok(!rows.is_empty())
    }

    /// Record a claimed `(peer, nonce)` pair, valid until `expires_at`.
    ///
    /// # Errors
    ///
    /// Returns any SQL error.
    pub fn record_nonce(
        &self,
        peer: &[u8; 32],
        nonce: &[u8; 16],
        expires_at: u64,
    ) -> Result<(), DoStorageError> {
        self.run(
            "INSERT OR IGNORE INTO nonces (peer, nonce, expires_at) VALUES (?, ?, ?);",
            vec![
                SqlValue::Blob(peer.to_vec()),
                SqlValue::Blob(nonce.to_vec()),
                SqlValue::Integer(clamp_i64(expires_at)),
            ],
        )
    }

    /// Delete every replay nonce whose validity window has passed.
    ///
    /// # Errors
    ///
    /// Returns any SQL error.
    pub fn gc_nonces(&self, now: u64) -> Result<(), DoStorageError> {
        self.run(
            "DELETE FROM nonces WHERE expires_at <= ?;",
            vec![SqlValue::Integer(clamp_i64(now))],
        )
    }

    /// Count replay nonces still within their validity window. Used to decide
    /// whether another cleanup alarm is worth scheduling.
    ///
    /// # Errors
    ///
    /// Returns any SQL error.
    pub fn active_nonce_count(&self, now: u64) -> Result<u64, DoStorageError> {
        let rows = self.query(
            "SELECT COUNT(*) FROM nonces WHERE expires_at > ?;",
            vec![SqlValue::Integer(clamp_i64(now))],
        )?;
        let count = rows
            .first()
            .map(|row| int_at(row, 0, "nonces.count"))
            .transpose()?
            .unwrap_or(0);
        Ok(count.max(0) as u64)
    }

    // ---- compaction hint -------------------------------------------------

    /// Read and clear the "a fragment was written" flag. Cloned handles share
    /// the flag, so the value set by the storage layer during message handling
    /// is visible to the Durable Object afterwards.
    pub fn take_compaction_hint(&self) -> bool {
        self.compaction_hint.replace(false)
    }

    // ---- compaction ------------------------------------------------------

    /// Compact one tree: drop loose commits recoverable from a kept fragment's
    /// blob and fragments subsumed by a strictly-deeper kept fragment, per
    /// [`Sedimentree::minimize`]. Lossless — anything deleted is reconstructable
    /// from what remains. Returns the number of `(head)`s removed.
    ///
    /// # Errors
    ///
    /// Returns any SQL or decode error.
    pub fn compact_tree<M: DepthMetric>(
        &self,
        tree: SedimentreeId,
        metric: &M,
    ) -> Result<usize, DoStorageError> {
        let commits = self.commit_metas(tree)?;
        let fragments = self.fragment_metas(tree)?;
        if commits.is_empty() && fragments.is_empty() {
            return Ok(0);
        }

        let stored_commit_heads: Set<CommitId> = commits.iter().map(LooseCommit::head).collect();
        let stored_fragment_heads: Set<CommitId> = fragments.iter().map(Fragment::head).collect();

        let minimized = Sedimentree::new(fragments, commits).minimize(metric);
        let keep_commits: Set<CommitId> =
            minimized.loose_commits().map(LooseCommit::head).collect();
        let keep_fragments: Set<CommitId> = minimized.fragments().map(Fragment::head).collect();

        let mut removed = 0;
        for head in stored_commit_heads.difference(&keep_commits) {
            self.delete_head("commits", tree, *head)?;
            removed += 1;
        }
        for head in stored_fragment_heads.difference(&keep_fragments) {
            self.delete_head("fragments", tree, *head)?;
            removed += 1;
        }
        Ok(removed)
    }

    /// Compact every tree in the database. Returns the total number of `(head)`s
    /// removed across all trees.
    ///
    /// # Errors
    ///
    /// Returns any SQL or decode error.
    pub fn compact_all<M: DepthMetric>(&self, metric: &M) -> Result<usize, DoStorageError> {
        let mut removed = 0;
        for tree in self.all_tree_ids()? {
            removed += self.compact_tree(tree, metric)?;
        }
        Ok(removed)
    }

    fn all_tree_ids(&self) -> Result<Vec<SedimentreeId>, DoStorageError> {
        let rows = self.query("SELECT id FROM trees;", vec![])?;
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            ids.push(SedimentreeId::new(array32(
                &blob_at(&row, 0, "trees.id")?,
                "id",
            )?));
        }
        Ok(ids)
    }

    fn commit_metas(&self, tree: SedimentreeId) -> Result<Vec<LooseCommit>, DoStorageError> {
        // Deterministic `(head, digest)` order (matching the primary key and the
        // full `load_loose_commits`) so meta-only and full loads resolve to the
        // same first-wins representative per `CommitId` under equivocation — the
        // storage conformance contract (`assert_metas_match_full_load`).
        let rows = self.query(
            "SELECT signed FROM commits WHERE tree = ? ORDER BY head, digest;",
            vec![SqlValue::Blob(tree.as_bytes().to_vec())],
        )?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let signed = Signed::<LooseCommit>::try_decode(&blob_at(&row, 0, "commits.signed")?)?;
            out.push(signed.try_decode_trusted_payload()?);
        }
        Ok(out)
    }

    fn fragment_metas(&self, tree: SedimentreeId) -> Result<Vec<Fragment>, DoStorageError> {
        // See `commit_metas`: deterministic `(head, digest)` order so meta-only
        // and full fragment loads pick the same representative per `CommitId`.
        let rows = self.query(
            "SELECT signed FROM fragments WHERE tree = ? ORDER BY head, digest;",
            vec![SqlValue::Blob(tree.as_bytes().to_vec())],
        )?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let signed = Signed::<Fragment>::try_decode(&blob_at(&row, 0, "fragments.signed")?)?;
            out.push(signed.try_decode_trusted_payload()?);
        }
        Ok(out)
    }

    fn delete_head(
        &self,
        table: &str,
        tree: SedimentreeId,
        head: CommitId,
    ) -> Result<(), DoStorageError> {
        // `table` is a fixed internal string ("commits" | "fragments"), never
        // user input, so this format is not an injection vector.
        let sql = format!("DELETE FROM {table} WHERE tree = ? AND head = ?;");
        self.run(
            &sql,
            vec![
                SqlValue::Blob(tree.as_bytes().to_vec()),
                SqlValue::Blob(head.as_bytes().to_vec()),
            ],
        )
    }

    // ---- low-level SQL helpers -------------------------------------------

    fn run(&self, query: &str, binds: Vec<SqlValue>) -> Result<(), DoStorageError> {
        self.sql.exec(query, binds).map(|_| ())
    }

    fn query(
        &self,
        query: &str,
        binds: Vec<SqlValue>,
    ) -> Result<Vec<Vec<SqlValue>>, DoStorageError> {
        self.sql.exec(query, binds)
    }

    // ---- compound (signed + blob) row helpers ----------------------------

    fn save_item(
        &self,
        table: &str,
        tree: SedimentreeId,
        head: CommitId,
        digest: &[u8; 32],
        signed_bytes: &[u8],
        blob: &[u8],
    ) -> Result<(), DoStorageError> {
        // Register the tree id atomically-ish with the item (same synchronous
        // storage, no concurrent writers inside a single-threaded DO).
        self.run(
            "INSERT OR IGNORE INTO trees (id) VALUES (?);",
            vec![SqlValue::Blob(tree.as_bytes().to_vec())],
        )?;
        let sql = format!(
            "INSERT OR IGNORE INTO {table} (tree, head, digest, signed, blob) VALUES (?, ?, ?, ?, ?);"
        );
        self.run(
            &sql,
            vec![
                SqlValue::Blob(tree.as_bytes().to_vec()),
                SqlValue::Blob(head.as_bytes().to_vec()),
                SqlValue::Blob(digest.to_vec()),
                SqlValue::Blob(signed_bytes.to_vec()),
                SqlValue::Blob(blob.to_vec()),
            ],
        )
    }
}

/// Clamp a `u64` timestamp into the `i64` SQLite integer domain (saturating at
/// `i64::MAX`, which is ~292 billion years out — safe for any real timestamp).
#[allow(clippy::cast_possible_wrap)]
fn clamp_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

/// Extract the blob bytes from column `idx` of a row.
///
/// Every blob column we read is declared `NOT NULL`, and an empty blob comes
/// back as a zero-length `Blob`, not `Null`. So a `Null` here means corruption
/// or a type mismatch and is surfaced as an error rather than masked as an
/// empty value (which would, e.g., turn a bad signer seed into an empty one).
fn blob_at(row: &[SqlValue], idx: usize, ctx: &'static str) -> Result<Vec<u8>, DoStorageError> {
    match row.get(idx) {
        Some(SqlValue::Blob(b)) => Ok(b.clone()),
        _ => Err(DoStorageError::UnexpectedColumn(ctx)),
    }
}

/// Extract an integer from column `idx` of a row.
fn int_at(row: &[SqlValue], idx: usize, ctx: &'static str) -> Result<i64, DoStorageError> {
    match row.get(idx) {
        Some(SqlValue::Integer(i)) => Ok(*i),
        _ => Err(DoStorageError::UnexpectedColumn(ctx)),
    }
}

fn array32(bytes: &[u8], ctx: &'static str) -> Result<[u8; 32], DoStorageError> {
    <[u8; 32]>::try_from(bytes).map_err(|_| DoStorageError::UnexpectedColumn(ctx))
}

/// An order-independent, collision-resistant fingerprint of a `(tree, peer)`
/// subscription set, used to detect whether the set changed before rewriting it
/// to SQLite.
///
/// The pairs are sorted into a canonical order and fed, length-prefixed, into a
/// single blake3 hash whose full 32-byte digest is returned. Sorting makes the
/// result independent of map iteration order; the full digest (rather than a
/// truncated 64-bit sum) means `persist_subscriptions` won't skip a real change
/// on a fingerprint collision and reload a stale set after hibernation.
///
/// Only referenced from the wasm Durable Object and host tests; `allow(dead_code)`
/// keeps the plain host `lib` build quiet.
#[must_use]
#[allow(dead_code)]
pub(crate) fn subscriptions_fingerprint(pairs: &[(SedimentreeId, [u8; 32])]) -> [u8; 32] {
    let mut sorted: Vec<(&[u8; 32], &[u8; 32])> = pairs
        .iter()
        .map(|(tree, peer)| (tree.as_bytes(), peer))
        .collect();
    sorted.sort_unstable();

    let mut hasher = blake3::Hasher::new();
    hasher.update(&(sorted.len() as u64).to_le_bytes());
    for (tree, peer) in sorted {
        // Both fields are fixed 32 bytes, so concatenation is unambiguous.
        hasher.update(tree);
        hasher.update(peer);
    }
    *hasher.finalize().as_bytes()
}

fn row_to_commit(
    signed_bytes: &[u8],
    blob_bytes: Vec<u8>,
) -> Result<VerifiedMeta<LooseCommit>, DoStorageError> {
    let signed = Signed::<LooseCommit>::try_decode(signed_bytes)?;
    VerifiedMeta::try_from_trusted(signed, Blob::new(blob_bytes)).map_err(DoStorageError::from)
}

fn row_to_fragment(
    signed_bytes: &[u8],
    blob_bytes: Vec<u8>,
) -> Result<VerifiedMeta<Fragment>, DoStorageError> {
    let signed = Signed::<Fragment>::try_decode(signed_bytes)?;
    VerifiedMeta::try_from_trusted(signed, Blob::new(blob_bytes)).map_err(DoStorageError::from)
}

impl<S: Sql> Storage<Local> for SqlStore<S> {
    type Error = DoStorageError;

    // ==================== Sedimentree IDs ====================

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.run(
                "INSERT OR IGNORE INTO trees (id) VALUES (?);",
                vec![SqlValue::Blob(sedimentree_id.as_bytes().to_vec())],
            )
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            let id = SqlValue::Blob(sedimentree_id.as_bytes().to_vec());
            self.run("DELETE FROM trees WHERE id = ?;", vec![id.clone()])?;
            self.run("DELETE FROM commits WHERE tree = ?;", vec![id.clone()])?;
            self.run("DELETE FROM fragments WHERE tree = ?;", vec![id.clone()])?;
            // Drop the tree's subscriptions too, otherwise they'd be reloaded
            // after hibernation and drive stale fan-out for a tree we no longer
            // hold.
            self.run("DELETE FROM subscriptions WHERE tree = ?;", vec![id])?;
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> LocalBoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Local::from_future(async move {
            let mut ids = Set::new();
            for id in self.all_tree_ids()? {
                ids.insert(id);
            }
            Ok(ids)
        })
    }

    fn contains_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<bool, Self::Error>> {
        Local::from_future(async move {
            let rows = self.query(
                "SELECT 1 FROM trees WHERE id = ? LIMIT 1;",
                vec![SqlValue::Blob(sedimentree_id.as_bytes().to_vec())],
            )?;
            Ok(!rows.is_empty())
        })
    }

    // ==================== Loose Commits ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            let head = verified.payload().head();
            let digest = Digest::hash(verified.payload());
            self.save_item(
                "commits",
                sedimentree_id,
                head,
                digest.as_bytes(),
                verified.signed().as_bytes(),
                verified.blob().as_slice(),
            )
        })
    }

    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Local::from_future(async move {
            let rows = self.query(
                "SELECT DISTINCT head FROM commits WHERE tree = ?;",
                vec![SqlValue::Blob(sedimentree_id.as_bytes().to_vec())],
            )?;
            let mut ids = Set::new();
            for row in rows {
                ids.insert(CommitId::new(array32(
                    &blob_at(&row, 0, "commits.head")?,
                    "head",
                )?));
            }
            Ok(ids)
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Local::from_future(async move {
            // Deterministic `(head, digest)` order matching `commit_metas` (see
            // its note) so both loads resolve the same representative per id.
            let rows = self.query(
                "SELECT signed, blob FROM commits WHERE tree = ? ORDER BY head, digest;",
                vec![SqlValue::Blob(sedimentree_id.as_bytes().to_vec())],
            )?;
            let mut out = Vec::new();
            for row in rows {
                let signed = blob_at(&row, 0, "commits.signed")?;
                let blob = blob_at(&row, 1, "commits.blob")?;
                out.push(row_to_commit(&signed, blob)?);
            }
            Ok(out)
        })
    }

    fn load_loose_commit_metas(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        Local::from_future(async move { self.commit_metas(sedimentree_id) })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> LocalBoxFuture<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Local::from_future(async move {
            let rows = self.query(
                // `ORDER BY digest` so that, under equivocation (several rows
                // sharing this head), the single-item load picks the same
                // first-wins representative as the ordered full/meta loads
                // instead of an arbitrary one.
                "SELECT signed, blob FROM commits WHERE tree = ? AND head = ? ORDER BY digest LIMIT 1;",
                vec![
                    SqlValue::Blob(sedimentree_id.as_bytes().to_vec()),
                    SqlValue::Blob(commit_id.as_bytes().to_vec()),
                ],
            )?;
            match rows.into_iter().next() {
                Some(row) => {
                    let signed = blob_at(&row, 0, "commits.signed")?;
                    let blob = blob_at(&row, 1, "commits.blob")?;
                    Ok(Some(row_to_commit(&signed, blob)?))
                }
                None => Ok(None),
            }
        })
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.run(
                "DELETE FROM commits WHERE tree = ? AND head = ?;",
                vec![
                    SqlValue::Blob(sedimentree_id.as_bytes().to_vec()),
                    SqlValue::Blob(commit_id.as_bytes().to_vec()),
                ],
            )
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.run(
                "DELETE FROM commits WHERE tree = ?;",
                vec![SqlValue::Blob(sedimentree_id.as_bytes().to_vec())],
            )
        })
    }

    // ==================== Fragments ====================

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            let head = verified.payload().head();
            let digest = Digest::hash(verified.payload());
            let result = self.save_item(
                "fragments",
                sedimentree_id,
                head,
                digest.as_bytes(),
                verified.signed().as_bytes(),
                verified.blob().as_slice(),
            );
            // A new fragment may subsume loose commits — flag that compaction is
            // worth running on the next alarm.
            if result.is_ok() {
                self.compaction_hint.set(true);
            }
            result
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> LocalBoxFuture<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Local::from_future(async move {
            let rows = self.query(
                // `ORDER BY digest` so an equivocating fragment head resolves to
                // the same representative here as in the ordered full/meta loads.
                "SELECT signed, blob FROM fragments WHERE tree = ? AND head = ? ORDER BY digest LIMIT 1;",
                vec![
                    SqlValue::Blob(sedimentree_id.as_bytes().to_vec()),
                    SqlValue::Blob(fragment_head.as_bytes().to_vec()),
                ],
            )?;
            match rows.into_iter().next() {
                Some(row) => {
                    let signed = blob_at(&row, 0, "fragments.signed")?;
                    let blob = blob_at(&row, 1, "fragments.blob")?;
                    Ok(Some(row_to_fragment(&signed, blob)?))
                }
                None => Ok(None),
            }
        })
    }

    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Local::from_future(async move {
            let rows = self.query(
                "SELECT DISTINCT head FROM fragments WHERE tree = ?;",
                vec![SqlValue::Blob(sedimentree_id.as_bytes().to_vec())],
            )?;
            let mut ids = Set::new();
            for row in rows {
                ids.insert(CommitId::new(array32(
                    &blob_at(&row, 0, "fragments.head")?,
                    "head",
                )?));
            }
            Ok(ids)
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Local::from_future(async move {
            // Deterministic `(head, digest)` order matching `fragment_metas` so
            // both loads resolve the same representative per id.
            let rows = self.query(
                "SELECT signed, blob FROM fragments WHERE tree = ? ORDER BY head, digest;",
                vec![SqlValue::Blob(sedimentree_id.as_bytes().to_vec())],
            )?;
            let mut out = Vec::new();
            for row in rows {
                let signed = blob_at(&row, 0, "fragments.signed")?;
                let blob = blob_at(&row, 1, "fragments.blob")?;
                out.push(row_to_fragment(&signed, blob)?);
            }
            Ok(out)
        })
    }

    fn load_fragment_metas(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        Local::from_future(async move { self.fragment_metas(sedimentree_id) })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.run(
                "DELETE FROM fragments WHERE tree = ? AND head = ?;",
                vec![
                    SqlValue::Blob(sedimentree_id.as_bytes().to_vec()),
                    SqlValue::Blob(fragment_head.as_bytes().to_vec()),
                ],
            )
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.run(
                "DELETE FROM fragments WHERE tree = ?;",
                vec![SqlValue::Blob(sedimentree_id.as_bytes().to_vec())],
            )
        })
    }

    // ==================== Batch ====================

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> LocalBoxFuture<'_, Result<usize, Self::Error>> {
        Local::from_future(async move {
            let total = commits.len() + fragments.len();
            self.run(
                "INSERT OR IGNORE INTO trees (id) VALUES (?);",
                vec![SqlValue::Blob(sedimentree_id.as_bytes().to_vec())],
            )?;
            for vm in &commits {
                self.save_item(
                    "commits",
                    sedimentree_id,
                    vm.payload().head(),
                    Digest::hash(vm.payload()).as_bytes(),
                    vm.signed().as_bytes(),
                    vm.blob().as_slice(),
                )?;
            }
            for vm in &fragments {
                self.save_item(
                    "fragments",
                    sedimentree_id,
                    vm.payload().head(),
                    Digest::hash(vm.payload()).as_bytes(),
                    vm.signed().as_bytes(),
                    vm.blob().as_slice(),
                )?;
            }
            if !fragments.is_empty() {
                self.compaction_hint.set(true);
            }
            Ok(total)
        })
    }
}

// ---------------------------------------------------------------------------
// Worker (production) backend: the Durable Object's embedded SQLite.
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::{DoStorageError, Sql, SqlStore, SqlValue};
    use worker::{SqlStorage, SqlStorageValue};

    /// The production storage type: [`SqlStore`] over the DO's SQLite.
    pub type DoSqlStorage = SqlStore<WorkerSql>;

    impl DoSqlStorage {
        /// Wrap the DO's SQL handle.
        #[must_use]
        pub fn new(sql: SqlStorage) -> Self {
            Self::from_backend(WorkerSql(sql))
        }
    }

    /// [`Sql`] backend over `worker::SqlStorage`.
    #[derive(Clone, Debug)]
    pub struct WorkerSql(pub SqlStorage);

    impl Sql for WorkerSql {
        fn exec(
            &self,
            query: &str,
            binds: Vec<SqlValue>,
        ) -> Result<Vec<Vec<SqlValue>>, DoStorageError> {
            let converted: Vec<SqlStorageValue> = binds.into_iter().map(to_worker).collect();
            let cursor = self
                .0
                .exec(query, converted)
                .map_err(|e| DoStorageError::Sql(e.to_string()))?;
            let mut rows = Vec::new();
            for row in cursor.raw() {
                let row = row.map_err(|e| DoStorageError::Sql(e.to_string()))?;
                rows.push(row.into_iter().map(from_worker).collect());
            }
            Ok(rows)
        }
    }

    fn to_worker(v: SqlValue) -> SqlStorageValue {
        match v {
            SqlValue::Null => SqlStorageValue::Null,
            SqlValue::Integer(i) => SqlStorageValue::Integer(i),
            SqlValue::Float(f) => SqlStorageValue::Float(f),
            SqlValue::Text(s) => SqlStorageValue::String(s),
            SqlValue::Blob(b) => SqlStorageValue::Blob(b),
        }
    }

    fn from_worker(v: SqlStorageValue) -> SqlValue {
        match v {
            SqlStorageValue::Null => SqlValue::Null,
            SqlStorageValue::Boolean(b) => SqlValue::Integer(i64::from(b)),
            SqlStorageValue::Integer(i) => SqlValue::Integer(i),
            SqlStorageValue::Float(f) => SqlValue::Float(f),
            SqlStorageValue::String(s) => SqlValue::Text(s),
            SqlStorageValue::Blob(b) => SqlValue::Blob(b),
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::DoSqlStorage;

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::BTreeSet, rc::Rc};

    use futures::executor::block_on;
    use rusqlite::{types::ValueRef, Connection};
    use sedimentree_core::{
        blob::{verified::VerifiedBlobMeta, Blob},
        depth::CountLeadingZeroBytes,
    };
    use subduction_crypto::signer::memory::MemorySigner;

    /// `rusqlite` [`Sql`] backend for host tests (in-memory database).
    #[derive(Clone)]
    struct RusqliteSql(Rc<Connection>);

    impl std::fmt::Debug for RusqliteSql {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("RusqliteSql")
        }
    }

    impl Sql for RusqliteSql {
        fn exec(
            &self,
            query: &str,
            binds: Vec<SqlValue>,
        ) -> Result<Vec<Vec<SqlValue>>, DoStorageError> {
            let params = rusqlite::params_from_iter(binds.iter().map(to_rusqlite));
            let mut stmt = self
                .0
                .prepare(query)
                .map_err(|e| DoStorageError::Sql(e.to_string()))?;
            let col_count = stmt.column_count();
            let mut out = Vec::new();
            let mut rows = stmt
                .query(params)
                .map_err(|e| DoStorageError::Sql(e.to_string()))?;
            while let Some(row) = rows
                .next()
                .map_err(|e| DoStorageError::Sql(e.to_string()))?
            {
                let mut cols = Vec::with_capacity(col_count);
                for i in 0..col_count {
                    let value = row
                        .get_ref(i)
                        .map_err(|e| DoStorageError::Sql(e.to_string()))?;
                    cols.push(from_rusqlite(value));
                }
                out.push(cols);
            }
            Ok(out)
        }
    }

    fn to_rusqlite(v: &SqlValue) -> rusqlite::types::Value {
        match v {
            SqlValue::Null => rusqlite::types::Value::Null,
            SqlValue::Integer(i) => rusqlite::types::Value::Integer(*i),
            SqlValue::Float(f) => rusqlite::types::Value::Real(*f),
            SqlValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
            SqlValue::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
        }
    }

    fn from_rusqlite(v: ValueRef<'_>) -> SqlValue {
        match v {
            ValueRef::Null => SqlValue::Null,
            ValueRef::Integer(i) => SqlValue::Integer(i),
            ValueRef::Real(f) => SqlValue::Float(f),
            ValueRef::Text(t) => SqlValue::Text(String::from_utf8_lossy(t).into_owned()),
            ValueRef::Blob(b) => SqlValue::Blob(b.to_vec()),
        }
    }

    fn store() -> SqlStore<RusqliteSql> {
        let conn = Connection::open_in_memory().expect("open in-memory sqlite");
        let store = SqlStore::from_backend(RusqliteSql(Rc::new(conn)));
        store.init_schema().expect("init schema");
        store
    }

    /// A [`CommitId`] with exactly `n` leading zero bytes, so
    /// [`CountLeadingZeroBytes`] assigns it depth `n`. `seed` disambiguates.
    fn id_with_depth(n: u8, seed: u8) -> CommitId {
        let mut bytes = [0u8; 32];
        bytes[n as usize] = 1;
        bytes[n as usize + 1] = seed;
        CommitId::new(bytes)
    }

    fn seal_commit(
        signer: &MemorySigner,
        tree: SedimentreeId,
        head: CommitId,
        parents: BTreeSet<CommitId>,
        payload: &[u8],
    ) -> VerifiedMeta<LooseCommit> {
        block_on(
            VerifiedMeta::<LooseCommit>::seal::<future_form::Sendable, _>(
                signer,
                (tree, head, parents),
                VerifiedBlobMeta::new(Blob::new(payload.to_vec())),
            ),
        )
    }

    fn seal_fragment(
        signer: &MemorySigner,
        tree: SedimentreeId,
        head: CommitId,
        boundary: BTreeSet<CommitId>,
        checkpoints: Vec<CommitId>,
        payload: &[u8],
    ) -> VerifiedMeta<Fragment> {
        block_on(VerifiedMeta::<Fragment>::seal::<future_form::Sendable, _>(
            signer,
            (tree, head, boundary, checkpoints),
            VerifiedBlobMeta::new(Blob::new(payload.to_vec())),
        ))
    }

    #[test]
    fn meta_round_trips() {
        let s = store();
        assert_eq!(s.get_meta("seed").expect("get"), None);
        s.put_meta("seed", vec![1, 2, 3]).expect("put");
        assert_eq!(s.get_meta("seed").expect("get"), Some(vec![1, 2, 3]));
        // Overwrite.
        s.put_meta("seed", vec![9]).expect("put");
        assert_eq!(s.get_meta("seed").expect("get"), Some(vec![9]));
    }

    #[test]
    fn counter_base_reserves_disjoint_increasing_windows() {
        let s = store();
        // First reservation starts at 0; each call advances by the stride so
        // successive Durable Object lifetimes get disjoint, increasing windows.
        assert_eq!(s.reserve_counter_base(1000).expect("reserve"), 0);
        assert_eq!(s.reserve_counter_base(1000).expect("reserve"), 1000);
        assert_eq!(s.reserve_counter_base(500).expect("reserve"), 2000);
        assert_eq!(s.reserve_counter_base(1).expect("reserve"), 2500);
    }

    #[test]
    fn counter_base_never_reserves_the_wrapping_value() {
        let s = store();
        // A max-width stride saturates the persisted high-water, but the base is
        // clamped below u64::MAX so seeding PeerCounter with it can never force
        // the next stamp to wrap to 0.
        assert_eq!(s.reserve_counter_base(u64::MAX).expect("reserve"), 0);
        let second = s.reserve_counter_base(u64::MAX).expect("reserve");
        assert_eq!(
            second,
            u64::MAX - 1,
            "saturated base is clamped one below the wrapping value"
        );
        // And it stays there across further lifetimes rather than reaching MAX.
        assert_eq!(
            s.reserve_counter_base(u64::MAX).expect("reserve"),
            u64::MAX - 1
        );
    }

    #[test]
    fn subscriptions_round_trip() {
        let s = store();
        let tree = SedimentreeId::new([1u8; 32]);
        let a = [0xAAu8; 32];
        let b = [0xBBu8; 32];
        s.replace_subscriptions(&[(tree, a), (tree, b)])
            .expect("replace");
        let mut loaded = s.load_subscriptions().expect("load");
        loaded.sort();
        let mut expected = vec![(tree, a), (tree, b)];
        expected.sort();
        assert_eq!(loaded, expected);

        // Replacing with a smaller set drops the removed peer.
        s.replace_subscriptions(&[(tree, a)]).expect("replace");
        assert_eq!(s.load_subscriptions().expect("load"), vec![(tree, a)]);
    }

    #[test]
    fn fingerprint_is_order_independent_and_change_sensitive() {
        let tree = SedimentreeId::new([2u8; 32]);
        let a = [1u8; 32];
        let b = [2u8; 32];
        let fp1 = subscriptions_fingerprint(&[(tree, a), (tree, b)]);
        let fp2 = subscriptions_fingerprint(&[(tree, b), (tree, a)]);
        assert_eq!(fp1, fp2, "order must not matter");

        let fp3 = subscriptions_fingerprint(&[(tree, a)]);
        assert_ne!(fp1, fp3, "removing a pair must change the fingerprint");

        // The empty set has a stable fingerprint distinct from any non-empty one.
        assert_eq!(
            subscriptions_fingerprint(&[]),
            subscriptions_fingerprint(&[])
        );
        assert_ne!(
            fp3,
            subscriptions_fingerprint(&[]),
            "empty must differ from non-empty"
        );
    }

    #[test]
    fn nonce_replay_is_rejected_and_scoped() {
        let s = store();
        let peer = [7u8; 32];
        let other_peer = [8u8; 32];
        let nonce = [3u8; 16];
        let other_nonce = [4u8; 16];

        assert!(!s.nonce_seen(&peer, &nonce, 100).expect("seen"));
        s.record_nonce(&peer, &nonce, 1000).expect("record");
        assert!(
            s.nonce_seen(&peer, &nonce, 100).expect("seen"),
            "same pair is a replay"
        );

        // A different peer or nonce is a distinct claim.
        assert!(!s.nonce_seen(&other_peer, &nonce, 100).expect("seen"));
        assert!(!s.nonce_seen(&peer, &other_nonce, 100).expect("seen"));
    }

    #[test]
    fn nonce_expires_and_gc_reclaims() {
        let s = store();
        let peer = [7u8; 32];
        let nonce = [3u8; 16];
        s.record_nonce(&peer, &nonce, 1000).expect("record");

        // Past expiry, the pair no longer counts as seen (freshness check alone
        // now blocks the replay).
        assert!(!s.nonce_seen(&peer, &nonce, 1000).expect("seen"));
        assert!(!s.nonce_seen(&peer, &nonce, 2000).expect("seen"));
        assert_eq!(s.active_nonce_count(500).expect("count"), 1);
        assert_eq!(s.active_nonce_count(2000).expect("count"), 0);

        s.gc_nonces(2000).expect("gc");
        assert_eq!(s.active_nonce_count(0).expect("count"), 0);
    }

    #[test]
    fn commit_round_trips_through_storage() {
        let s = store();
        let signer = MemorySigner::generate();
        let tree = SedimentreeId::new([5u8; 32]);
        let head = CommitId::new([9u8; 32]);
        let vm = seal_commit(&signer, tree, head, BTreeSet::new(), &[1, 2, 3, 4]);

        block_on(s.save_loose_commit(tree, vm)).expect("save");

        let ids = block_on(s.list_commit_ids(tree)).expect("list");
        assert!(ids.contains(&head));
        let loaded = block_on(s.load_loose_commits(tree)).expect("load");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].blob().as_slice(), &[1, 2, 3, 4]);
    }

    #[test]
    fn saving_a_fragment_sets_the_compaction_hint() {
        let s = store();
        let signer = MemorySigner::generate();
        let tree = SedimentreeId::new([4u8; 32]);
        assert!(!s.take_compaction_hint(), "no hint before any fragment");

        let frag = seal_fragment(
            &signer,
            tree,
            id_with_depth(2, 1),
            BTreeSet::from([id_with_depth(1, 100)]),
            Vec::new(),
            &[10],
        );
        block_on(s.save_fragment(tree, frag)).expect("save fragment");
        assert!(s.take_compaction_hint(), "fragment write sets the hint");
        assert!(!s.take_compaction_hint(), "hint is cleared after reading");
    }

    #[test]
    fn compaction_prunes_a_dominated_fragment() {
        // Mirrors subduction_core's `add_fragment_prunes_dominated_shallow_fragment`:
        // a depth-3 fragment whose checkpoints cover a depth-2 fragment dominates
        // it, so minimize (and therefore compaction) drops the shallow one.
        let s = store();
        let signer = MemorySigner::generate();
        let tree = SedimentreeId::new([1u8; 32]);

        let shallow_head = id_with_depth(2, 1);
        let shallow_boundary = id_with_depth(1, 100);
        let shallow = seal_fragment(
            &signer,
            tree,
            shallow_head,
            BTreeSet::from([shallow_boundary]),
            Vec::new(),
            &[10],
        );
        block_on(s.save_fragment(tree, shallow)).expect("save shallow");

        let deep_head = id_with_depth(3, 2);
        let deep = seal_fragment(
            &signer,
            tree,
            deep_head,
            BTreeSet::from([id_with_depth(1, 101)]),
            vec![shallow_head, shallow_boundary],
            &[20],
        );
        block_on(s.save_fragment(tree, deep)).expect("save deep");

        assert_eq!(block_on(s.list_fragment_ids(tree)).expect("list").len(), 2);

        let removed = s
            .compact_tree(tree, &CountLeadingZeroBytes)
            .expect("compact");
        assert_eq!(
            removed, 1,
            "exactly the dominated shallow fragment is removed"
        );

        let surviving = block_on(s.list_fragment_ids(tree)).expect("list after");
        assert_eq!(
            surviving,
            std::iter::once(deep_head).collect(),
            "only the dominating deep fragment survives"
        );
    }

    #[test]
    fn client_sedimentation_fragment_compacts_a_chain() {
        // Proves the demo's client-side sedimentation shape actually compacts:
        // a *chained* run of loose commits c0 <- c1 <- c2 (c2 a boundary), plus
        // a fragment headed at the boundary commit c2 with an empty boundary and
        // no checkpoints — exactly what `sendMessage` builds on a
        // `FragmentRequested` — makes minimize drop every covered loose commit.
        //
        // Coverage rule (see `CommitDag::simplify`): walking reverse-topo from
        // the tip, every commit in the range lands under range-head c2, and a
        // fragment whose `head()` is c2 covers that range. So the whole chain is
        // recoverable from the fragment blob and is pruned.
        let s = store();
        let signer = MemorySigner::generate();
        let tree = SedimentreeId::new([7u8; 32]);

        let c0 = id_with_depth(0, 1); // depth 0, ordinary
        let c1 = id_with_depth(0, 2); // depth 0, ordinary
        let c2 = id_with_depth(1, 3); // depth 1, boundary (fragment head)

        for (head, parents) in [
            (c0, BTreeSet::new()),
            (c1, BTreeSet::from([c0])),
            (c2, BTreeSet::from([c1])),
        ] {
            let vm = seal_commit(&signer, tree, head, parents, &[head.as_bytes()[1]]);
            block_on(s.save_loose_commit(tree, vm)).expect("save commit");
        }

        // The fragment the client mints on the boundary: head = c2, empty
        // boundary, no checkpoints, blob = the covered messages' bytes.
        let frag = seal_fragment(&signer, tree, c2, BTreeSet::new(), Vec::new(), b"c0c1c2");
        block_on(s.save_fragment(tree, frag)).expect("save fragment");
        assert!(
            s.take_compaction_hint(),
            "the fragment write hints compaction"
        );

        let removed = s
            .compact_tree(tree, &CountLeadingZeroBytes)
            .expect("compact");
        assert_eq!(removed, 3, "all three chained loose commits are pruned");

        assert!(
            block_on(s.list_commit_ids(tree)).expect("list").is_empty(),
            "no loose commits survive — they live in the fragment blob now"
        );
        assert_eq!(
            block_on(s.list_fragment_ids(tree)).expect("list"),
            std::iter::once(c2).collect(),
            "the covering fragment is what remains"
        );
    }

    #[test]
    fn client_sedimentation_keeps_commits_above_the_boundary() {
        // The recent tail must stay loose: commits newer than the latest
        // boundary are rangeless and never pruned, so a chat keeps showing its
        // most recent (not-yet-sedimented) messages as loose commits.
        let s = store();
        let signer = MemorySigner::generate();
        let tree = SedimentreeId::new([8u8; 32]);

        let c0 = id_with_depth(0, 1);
        let boundary = id_with_depth(1, 2); // fragment head
        let c_recent = id_with_depth(0, 3); // added after the boundary

        for (head, parents) in [
            (c0, BTreeSet::new()),
            (boundary, BTreeSet::from([c0])),
            (c_recent, BTreeSet::from([boundary])),
        ] {
            let vm = seal_commit(&signer, tree, head, parents, &[head.as_bytes()[1]]);
            block_on(s.save_loose_commit(tree, vm)).expect("save commit");
        }

        let frag = seal_fragment(&signer, tree, boundary, BTreeSet::new(), Vec::new(), b"c0b");
        block_on(s.save_fragment(tree, frag)).expect("save fragment");

        let removed = s
            .compact_tree(tree, &CountLeadingZeroBytes)
            .expect("compact");
        assert_eq!(removed, 2, "only c0 and the boundary commit are covered");

        assert_eq!(
            block_on(s.list_commit_ids(tree)).expect("list"),
            std::iter::once(c_recent).collect(),
            "the post-boundary commit stays loose"
        );
    }

    #[test]
    fn compaction_is_a_noop_without_fragments() {
        // With only loose commits and no covering fragment, minimize keeps
        // everything, so compaction removes nothing.
        let s = store();
        let signer = MemorySigner::generate();
        let tree = SedimentreeId::new([6u8; 32]);
        for i in 0..3u8 {
            let vm = seal_commit(
                &signer,
                tree,
                CommitId::new([i + 100; 32]),
                BTreeSet::new(),
                &[i],
            );
            block_on(s.save_loose_commit(tree, vm)).expect("save");
        }
        let removed = s.compact_all(&CountLeadingZeroBytes).expect("compact");
        assert_eq!(
            removed, 0,
            "nothing is redundant without a covering fragment"
        );
        assert_eq!(block_on(s.list_commit_ids(tree)).expect("list").len(), 3);
    }

    // ---- shared Storage conformance suite --------------------------------
    //
    // Run subduction_core's contract checks against the DO backend, so it is
    // held to the same invariants as `MemoryStorage`/redb rather than only its
    // own bespoke tests.

    #[test]
    fn metas_match_full_load_conformance_under_equivocation() {
        // The metadata-only loads must resolve to the same first-wins
        // representative per CommitId as the full loads — *including* under
        // Byzantine equivocation (several payloads sharing one head). This is
        // exactly what the content-digest key + `ORDER BY head, digest`
        // guarantee; without them SQLite could order the two loads differently
        // and pick divergent representatives.
        use subduction_core::storage::conformance;

        let s = store();
        let signer = MemorySigner::generate();
        let tree = SedimentreeId::new([0x24; 32]);

        // A few well-behaved, distinct commits.
        for i in 0..3u8 {
            let vm = seal_commit(
                &signer,
                tree,
                CommitId::new([i; 32]),
                BTreeSet::new(),
                &[i; 48],
            );
            block_on(s.save_loose_commit(tree, vm)).expect("save commit");
        }
        // An equivocating commit id: same head, two different payloads (distinct
        // parents + blob → distinct content digests → both rows coexist).
        let equ_head = CommitId::new([0x77; 32]);
        block_on(s.save_loose_commit(
            tree,
            seal_commit(&signer, tree, equ_head, BTreeSet::new(), &[1; 32]),
        ))
        .expect("save equivocation A");
        block_on(s.save_loose_commit(
            tree,
            seal_commit(
                &signer,
                tree,
                equ_head,
                BTreeSet::from([CommitId::new([0x66; 32])]),
                &[2; 32],
            ),
        ))
        .expect("save equivocation B");

        // Distinct and equivocating fragments too.
        block_on(s.save_fragment(
            tree,
            seal_fragment(
                &signer,
                tree,
                id_with_depth(2, 1),
                BTreeSet::from([id_with_depth(1, 10)]),
                Vec::new(),
                &[9; 24],
            ),
        ))
        .expect("save fragment");
        let equ_frag_head = id_with_depth(2, 2);
        block_on(s.save_fragment(
            tree,
            seal_fragment(
                &signer,
                tree,
                equ_frag_head,
                BTreeSet::from([id_with_depth(1, 11)]),
                Vec::new(),
                &[3; 20],
            ),
        ))
        .expect("save fragment equivocation A");
        block_on(s.save_fragment(
            tree,
            seal_fragment(
                &signer,
                tree,
                equ_frag_head,
                BTreeSet::from([id_with_depth(1, 12)]),
                vec![id_with_depth(1, 13)],
                &[4; 20],
            ),
        ))
        .expect("save fragment equivocation B");

        block_on(conformance::assert_metas_match_full_load::<
            future_form::Local,
            _,
        >(&s, tree));
    }

    #[test]
    fn saves_register_tree_ids_conformance() {
        // Each helper requires the tree to be unregistered before the save, so
        // use a fresh in-memory store per assertion.
        use subduction_core::storage::conformance;

        let signer = MemorySigner::generate();

        let tree_c = SedimentreeId::new([0xC0; 32]);
        let commit = seal_commit(
            &signer,
            tree_c,
            CommitId::new([1; 32]),
            BTreeSet::new(),
            &[1, 2, 3],
        );
        block_on(conformance::assert_commit_save_registers_tree_id::<
            future_form::Local,
            _,
        >(&store(), commit));

        let tree_f = SedimentreeId::new([0xF0; 32]);
        let fragment = seal_fragment(
            &signer,
            tree_f,
            id_with_depth(2, 1),
            BTreeSet::from([id_with_depth(1, 5)]),
            Vec::new(),
            &[7],
        );
        block_on(conformance::assert_fragment_save_registers_tree_id::<
            future_form::Local,
            _,
        >(&store(), fragment));

        let tree_b = SedimentreeId::new([0xB0; 32]);
        let batch_commit = seal_commit(
            &signer,
            tree_b,
            CommitId::new([2; 32]),
            BTreeSet::new(),
            &[4, 5, 6],
        );
        block_on(conformance::assert_batch_save_registers_tree_id::<
            future_form::Local,
            _,
        >(&store(), tree_b, vec![batch_commit], Vec::new()));
    }
}
