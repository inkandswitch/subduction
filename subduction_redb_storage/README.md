# `subduction_redb_storage`

A [redb]-backed storage backend for Sedimentree, implementing the
`Storage` trait from `subduction_core`.

Compared to `sedimentree_fs_storage`, all metadata and blobs live in a
single transactional B+tree file:

- bulk per-tree loads are one range scan instead of one filesystem walk
- batch saves commit atomically with a single fsync
- on-disk footprint avoids per-file block rounding

This crate is currently an evaluation backend used by the
`benches/backends.rs` shoot-out against `FsStorage`. See
`.ignore/DECISIONS.md` (internal) for the comparison rationale.

[redb]: https://github.com/cberner/redb
