//! Cross-transport comparison benchmarks for Subduction.
//!
//! # Layout
//!
//! This crate is purely a bench host — it has no library code. All the measurement lives
//! under `benches/transport_comparison.rs`. The `lib.rs` exists only so that `cargo check`
//! has something to build when the crate is part of the workspace.
//!
//! # What it measures
//!
//! Four transport implementations under identical workloads:
//!
//! | Transport | Network cost          | Handshake cost         |
//! |-----------|-----------------------|------------------------|
//! | `ChannelTransport` | zero (in-process channel) | zero (`new_for_test`) |
//! | `TokioWebSocket`   | loopback TCP + WS     | Ed25519 mutual         |
//! | `HttpLongPoll`     | loopback HTTP/1.1     | Ed25519 mutual         |
//! | `Iroh`             | loopback QUIC         | Ed25519 mutual + iroh  |
//!
//! The `ChannelTransport` row is the control — any delta between it and the network rows is
//! attributable to the transport layer.
//!
//! # Running
//!
//! ```sh
//! cargo bench -p subduction_transport_benches --bench transport_comparison
//! ```
