# `subduction_runtime`

The generic async driver for `subduction_protocol`. Tagless-final traits
(`Transport`, `Storage`, `Clock`, `Spawn`, `Rng`, `CryptoWorker`) over
`future_form::FutureForm`, a lock-free actor funnel that owns the state
machine, and executors for the effects it emits.

Platform crates (tokio websocket, redb storage, browser Wasm, …) implement
the traits; this crate provides the loop.

Part of the sans-io rewrite; see `design/sans-io.md` at the repository
root. The previous implementation lives in `legacy/` until the rewrite
reaches parity.
