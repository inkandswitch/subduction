//! Group-commit writer: coalesces concurrent single-item writes into one
//! redb transaction (one fsync per drain), preserving per-call durability.
//!
//! redb has a single database-wide writer and fsyncs on every commit, so a
//! burst of N concurrent [`save_loose_commit`]/[`save_fragment`] calls would
//! serialize into N fsync'd transactions — the last waits behind every fsync
//! ahead of it.
//!
//! Routing those writes through a single task that drains the queue and writes
//! the whole drain in **one** transaction turns N fsyncs into one. There is no
//! added latency: a lone write drains immediately (batch of one), while writes
//! that arrive *during* a commit ride the next drain — the batching window is
//! exactly however long the previous fsync took, self-tuning to the load.
//!
//! Durability is unchanged ([`redb::Durability::Immediate`]): a waiter is
//! signalled only after `commit()` returns, and the whole drain is one atomic
//! transaction, so a crash never leaves a partial batch.
//!
//! [`save_loose_commit`]: subduction_core::storage::traits::Storage::save_loose_commit
//! [`save_fragment`]: subduction_core::storage::traits::Storage::save_fragment

mod ack;

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

use redb::Database;
use sedimentree_core::id::SedimentreeId;
use tokio::sync::{mpsc, oneshot};

use crate::{
    COMMITS, FRAGMENTS, RedbStorageError, TREES,
    insert::{PendingInsert, insert_compound, stage_external_blob_sync},
};

use self::ack::Ack;

/// Bounded write queue depth. When full, callers await on `send` — backpressure
/// that bounds both queued memory and the largest possible single drain.
pub(crate) const WRITER_QUEUE_CAPACITY: usize = 1024;

/// Which table a pending insert targets.
#[derive(Clone, Copy)]
pub(crate) enum WriteKind {
    Commit,
    Fragment,
}

/// One enqueued write awaiting durability, with the channel to signal its
/// caller once the containing transaction commits (or, on batch failure, once
/// its isolated retry resolves).
pub(crate) struct WriteJob {
    pub(crate) tree: SedimentreeId,
    pub(crate) insert: PendingInsert,
    pub(crate) kind: WriteKind,
    pub(crate) responder: oneshot::Sender<Result<(), RedbStorageError>>,
}

/// Spawn the group-commit writer task and return its queue sender.
///
/// Must be called from within a Tokio runtime (writes always are; the redb
/// backend already runs every op on the blocking pool).
///
/// The task holds only a [`Weak`] handle to the database, upgrading per drain,
/// so the last [`RedbStorage`](crate::RedbStorage) drop releases the file lock
/// immediately rather than waiting for the task to notice its queue closed.
pub(crate) fn spawn(
    db: &Arc<Database>,
    blobs_dir: Arc<PathBuf>,
    inline_threshold: usize,
) -> mpsc::Sender<WriteJob> {
    let (tx, rx) = mpsc::channel(WRITER_QUEUE_CAPACITY);
    tokio::runtime::Handle::current().spawn(run(
        rx,
        Arc::downgrade(db),
        blobs_dir,
        inline_threshold,
    ));
    tx
}

/// Drain-and-commit loop. Exits when every sender is dropped (storage closed)
/// or the database has been released, after flushing whatever is already
/// queued.
async fn run(
    mut rx: mpsc::Receiver<WriteJob>,
    db: Weak<Database>,
    blobs_dir: Arc<PathBuf>,
    inline_threshold: usize,
) {
    while let Some(first) = rx.recv().await {
        let mut jobs = vec![first];
        // Coalesce everything already queued (bounded by the channel capacity).
        // Whatever arrives after this point rides the next drain.
        while let Ok(job) = rx.try_recv() {
            jobs.push(job);
        }

        // Upgrade only for the write: a live caller awaiting its ack keeps the
        // storage (hence the database) alive, so this succeeds whenever there's
        // real work. It fails only if every caller cancelled and the storage was
        // dropped — then the database is gone and there's nothing to write.
        let Some(db) = db.upgrade() else {
            for job in jobs {
                drop(job.responder.send(Err(RedbStorageError::WriterClosed)));
            }
            return;
        };

        let blobs_dir = Arc::clone(&blobs_dir);
        let blocking = tokio::task::spawn_blocking(move || {
            write_drain(&db, &blobs_dir, inline_threshold, jobs)
        });

        match blocking.await {
            // The blocking closure has returned, so the strong `Arc<Database>`
            // it captured is already dropped. Acking only now guarantees a
            // caller that drops its storage the instant it's signalled still
            // releases redb's file lock: this drain no longer holds a handle.
            Ok(acks) => {
                for ack in acks {
                    ack.send();
                }
            }

            Err(join_err) => {
                // The blocking task panicked: its `WriteJob`s (and their
                // responders) were dropped, so callers already observe
                // `WriterClosed` via the closed oneshot. Keep the writer alive.
                tracing::error!(error = %join_err, "redb group-commit writer task panicked");
            }
        }
    }
}

/// Write a whole drain in one transaction, returning each waiter's channel
/// paired with its result for the caller to deliver *after* this function's
/// strong [`Database`] handle is dropped (see [`Ack`]).
///
/// On a batch failure, fall back to writing each item in its own transaction so
/// one poison item can't fail unrelated writes — and each caller gets its own
/// real error (`RedbStorageError` is not `Clone`, so a shared result can't be
/// fanned out).
///
/// Runs on the blocking pool.
fn write_drain(
    db: &Database,
    blobs_dir: &Path,
    inline_threshold: usize,
    jobs: Vec<WriteJob>,
) -> Vec<Ack> {
    #[cfg(feature = "metrics")]
    let _blocking = crate::BlockingGuard::new();

    match flush_all(db, blobs_dir, inline_threshold, &jobs) {
        Ok(()) => jobs
            .into_iter()
            .map(|job| Ack::new(job.responder, Ok(())))
            .collect(),

        Err(_batch_err) => jobs
            .into_iter()
            .map(|job| {
                let result = flush_one(db, blobs_dir, inline_threshold, &job);
                Ack::new(job.responder, result)
            })
            .collect(),
    }
}

/// Stage every item's blob, then insert the whole drain in one transaction.
fn flush_all(
    db: &Database,
    blobs_dir: &Path,
    inline_threshold: usize,
    jobs: &[WriteJob],
) -> Result<(), RedbStorageError> {
    // Blob files are staged before the write txn opens: file I/O must not hold
    // the database-wide writer slot (see `insert::stage_external_blobs_sync`).
    for job in jobs {
        stage_external_blob_sync(&job.insert, blobs_dir, inline_threshold)?;
    }

    let txn = db.begin_write()?;
    {
        // Register every tree id (idempotent; duplicate ids within one txn are
        // harmless). Per the Storage contract, persisting items registers their
        // sedimentree ids atomically with the items.
        let mut trees = txn.open_table(TREES)?;
        for job in jobs {
            trees.insert(job.tree.as_bytes(), ())?;
        }
    }
    {
        let mut commits = txn.open_table(COMMITS)?;
        let mut fragments = txn.open_table(FRAGMENTS)?;
        for job in jobs {
            match job.kind {
                WriteKind::Commit => insert_compound(&mut commits, &job.insert, inline_threshold)?,
                WriteKind::Fragment => {
                    insert_compound(&mut fragments, &job.insert, inline_threshold)?;
                }
            }
        }
    }
    txn.commit()?;
    Ok(())
}

/// Write a single job in its own transaction (the batch-failure fallback).
fn flush_one(
    db: &Database,
    blobs_dir: &Path,
    inline_threshold: usize,
    job: &WriteJob,
) -> Result<(), RedbStorageError> {
    stage_external_blob_sync(&job.insert, blobs_dir, inline_threshold)?;

    let txn = db.begin_write()?;
    {
        txn.open_table(TREES)?.insert(job.tree.as_bytes(), ())?;
        match job.kind {
            WriteKind::Commit => {
                insert_compound(&mut txn.open_table(COMMITS)?, &job.insert, inline_threshold)?;
            }
            WriteKind::Fragment => {
                insert_compound(
                    &mut txn.open_table(FRAGMENTS)?,
                    &job.insert,
                    inline_threshold,
                )?;
            }
        }
    }
    txn.commit()?;
    Ok(())
}
