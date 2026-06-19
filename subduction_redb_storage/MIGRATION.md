# Migrating a server from filesystem storage to redb

`subduction_cli` stores sedimentrees with the redb backend
([`subduction_redb_storage`]). A server that previously ran on the filesystem
backend (`sedimentree_fs_storage`) converts its data directory with `subduction
migrate`. The migration is **non-destructive**: it reads the old store and
writes a new one, leaving the source untouched so you can roll back.

## Before you start

- **Stop the server.** redb holds an exclusive process lock on
  `sedimentree.redb`, and both stores must be quiescent — a copy taken while the
  server is writing can capture a torn page. `migrate`, `purge`, and `inspect`
  all require the server stopped (or a copy of the data).
- **Have disk space for both stores.** The migration writes the redb store
  beside the (untouched) filesystem store. The redb store is roughly the logical
  data size; the filesystem store stays until you delete it.
- **Pick a fresh destination** (`--to`), distinct from the source (`--from`).

## Steps

1. **Preview — writes nothing.** Confirm the tree and commit counts and size the
   maintenance window:

   ```text
   subduction migrate --from /var/lib/subduction --to /var/lib/subduction-redb --dry-run
   ```

2. **Migrate.** One durable redb transaction per tree:

   ```text
   subduction migrate --from /var/lib/subduction --to /var/lib/subduction-redb
   ```

   Keyhive state (`.keyhive/`) is copied across too, so the destination is a
   complete data directory. (A server running `--auth open` has no keyhive
   state, so nothing is copied there.)

3. **Point `--data-dir` at the new directory and start the server:**

   ```text
   # --data-dir /var/lib/subduction-redb
   systemctl start subduction   # or however you run it
   ```

4. **Verify.** With the server running and `--admin-addr 127.0.0.1:9091` set,
   confirm the tree count matches the `--dry-run` total:

   ```text
   subduction inspect --admin 127.0.0.1:9091
   ```

## Rolling back

The source filesystem store is untouched. To revert: stop the server, point
`--data-dir` back at the original directory, and start the previous binary.

## Notes

- **Safe to re-run.** redb commits each tree's id and items in one transaction,
  so a re-run skips any tree already written — an interrupted migration just
  continues where it stopped.
- **Exclusive lock.** Only one process may open the redb store. Never run
  `migrate` or `inspect` against a directory a live server is using.
- **One-way.** `migrate` only goes filesystem to redb. Keep the source until you
  have verified the new store and are confident in the cutover.

[`subduction_redb_storage`]: ./README.md
