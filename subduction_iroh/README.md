# subduction_iroh

Iroh (QUIC) transport layer for the [Subduction](https://github.com/inkandswitch/subduction) sync protocol.

Uses [iroh](https://iroh.computer/) to establish direct peer-to-peer connections via QUIC, with optional relay fallback for NAT traversal. Native-only (no Wasm support).

## Protocol

- ALPN identifier: `subduction/0`
- Single bidirectional QUIC stream per connection
- Length-prefixed framing (4-byte big-endian u32)

## Modes

| Mode                | Description                                      |
|---------------------|--------------------------------------------------|
| Discovery (default) | Connects via iroh relay for NAT traversal        |
| Direct-only         | Requires explicit IP addresses, no relay traffic |
| Custom relay        | Routes through a user-specified relay URL        |
