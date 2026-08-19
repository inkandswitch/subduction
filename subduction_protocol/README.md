# `subduction_protocol`

The sans-io core of [Subduction]: a pure state machine that makes every
protocol decision — handshake, sync sessions, subscriptions, backoff,
timeouts — and performs no IO. No futures, no locks, no clock, no threads.

Drivers (see `subduction_runtime`) feed it events and execute the effects
it emits. This is what makes the protocol testable as a pure function and
bindable to any platform (tokio, browser Wasm, Swift, Kotlin, Python).

Part of the sans-io rewrite; see `design/sans-io.md` at the repository
root. The previous implementation lives in `legacy/` until the rewrite
reaches parity.

[Subduction]: https://github.com/inkandswitch/subduction
