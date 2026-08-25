# `subduction_redb_storage`

A [redb](https://github.com/cberner/redb) backend for the Subduction
sans-io driver's `Storage` capability. One transaction per storage op:
the driver's op-shaped trait batches at the protocol level, so each
`persist_items` amortizes a single fsync across its whole batch.
