# `subduction_redb_storage`

A hybrid [redb] + filesystem storage backend for Sedimentree,
implementing the `Storage` trait from `subduction_core`. This is the native
server storage backend: the `subduction_cli` server builds on it, replacing
the earlier `sedimentree_fs_storage` directory layout.

> [!NOTE]
> The `benches/backends.rs` shoot-out against `FsStorage` (and the
> `concurrent_writes` group that gated the cutover) is retained as a local
> performance regression guard; it is not wired into CI.

## Design

Metadata and small blobs live in a transactional redb B+tree; large
blobs live as flat, content-addressed files beside it. The split
captures both measured sweet spots:

- a B+tree packs small records densely (no per-file block rounding) and
  range-scans them fast
- the filesystem streams large values faster and without redb's ~2x
  page amplification

The inline/external cutoff is `DEFAULT_INLINE_THRESHOLD` (16 KiB),
configurable via `RedbStorage::with_inline_threshold`.

## Layout

```text
root/
├── sedimentree.redb           ← all metadata + blobs ≤ inline threshold
└── blobs/                     ← blobs > inline threshold (content-addressed)
    └── {hex[0..2]}/           ← 256 buckets by digest prefix
        └── {blob_digest_hex}  ← one flat file per blob, deduplicated
```

Three tables (`trees`, `commits`, `fragments`) key items by
`tree_id ++ item_id ++ content_digest`. Keys sort lexicographically, so
all items of a tree are contiguous — a bulk load is one range scan — and
Byzantine-equivocating payloads (multiple blobs under one `CommitId`)
coexist via the trailing digest, mirroring the filesystem backend's
content-addressed filenames.

## Durability

redb fsyncs on every transaction commit, so each `save_*` is durable on
return; `save_batch` amortizes one fsync across the batch. External blob
files are fully written and fsynced _before_ the referencing database
transaction commits, so a stored record always points at a complete
blob. A crash in between leaves at most a harmless orphan blob file
(content-addressed, so a later save adopts it).

## Migrating from the filesystem backend

An existing `sedimentree_fs_storage` store is converted with the CLI:

```text
# stop the server first — both stores must be quiescent
subduction migrate --from /var/lib/subduction --to /var/lib/subduction-redb
# then point the server's --data-dir at the new directory
```

The migration streams one tree per durable transaction and is resumable:
re-running skips any tree already present in the destination (id and items
are committed atomically). The server's keyhive state (`.keyhive/`) is copied
across too, so the destination is a complete data directory the server can run
against directly. `--from` and `--to` must differ so the source store is left
untouched (the migration is non-destructive and reversible).

Pass `--dry-run` to report what would be migrated without writing anything.

See [`MIGRATION.md`](./MIGRATION.md) for the full step-by-step guide, including
preview, verification, and rollback.

## Backup

The store is a single `sedimentree.redb` file plus the `blobs/` directory.
Back them up together while the server is **stopped** (or otherwise not
writing): a copy taken mid-write may capture a torn database page. There is
no incremental backup; snapshot the whole root.

## Known limitations

- **No blob GC.** `delete_*` removes the redb record but not the external
  blob file (large blobs are content-addressed and may be shared across
  trees, so they cannot be unlinked on a single tree's removal). The only
  caller that deletes is whole-document removal (`Subduction::remove_sedimentree`);
  normal sync is append-only, so this leaks only in proportion to document
  removals. Revisit with reference-counting or a mark-and-sweep pass if
  whole-document removal becomes routine.

## Example

```rust,no_run
use std::path::PathBuf;
use subduction_redb_storage::RedbStorage;

let storage = RedbStorage::new(PathBuf::from("./data"))
    .expect("failed to open storage");
```

[redb]: https://github.com/cberner/redb
