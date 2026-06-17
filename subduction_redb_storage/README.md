# `subduction_redb_storage`

A hybrid [redb] + filesystem storage backend for Sedimentree,
implementing the `Storage` trait from `subduction_core`. Intended as an
alternative to `sedimentree_fs_storage` for native servers.

> [!NOTE]
> This is an _evaluation_ crate (`publish = false`). Nothing in the
> workspace depends on it; it exists for the `benches/backends.rs`
> shoot-out against `FsStorage`. See `.ignore/DECISIONS.md` (internal)
> for the comparison rationale.

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
(content-addressed, so a later save adopts it). Deletes do not remove
external blob files; a GC sweep is future work.

## Example

```rust,no_run
use std::path::PathBuf;
use subduction_redb_storage::RedbStorage;

let storage = RedbStorage::new(PathBuf::from("./data"))
    .expect("failed to open storage");
```

[redb]: https://github.com/cberner/redb
