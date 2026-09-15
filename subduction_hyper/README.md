# Subduction over hyper

> [!WARNING]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

Accept [Subduction](https://github.com/inkandswitch/subduction) WebSocket connections from any HTTP server built on hyper 1.x, and hand them to `subduction_websocket` as an `async_tungstenite::WebSocketStream`.

## Why

Framework WebSocket modules wrap their own copy of tungstenite and seal the stream, so nothing from them can reach `WebSocket::new_with_keepalive`. `hyper-tungstenite` has the mirror-image problem: it yields a `tokio_tungstenite::WebSocketStream`, and `subduction_websocket` is built on `async-tungstenite`.

This crate handles the upgrade directly with hyper, one layer below the framework, and exposes a raw `WebSocketStream` that `subduction_websocket` can consume. One framer, no message conversion.

```
HTTP request ─▶ upgrade::validate(parts)        ─▶ AcceptKey     (http types only)
             ─▶ upgrade::accept_response(key)   ─▶ 101 response  (send it)
             ─▶ OnUpgrade.await                 ─▶ Upgraded      (hyper hands over the pipe)
             ─▶ upgrade::from_upgraded(io, cfg) ─▶ WebSocketStream<HyperIo>
```

## Use

With the `axum` feature, the `TungsteniteUpgrade` extractor does all four steps. On anything else built on hyper 1.x, call the `upgrade` module from a handler.

Serve the connection with upgrades enabled — `http1::Builder::serve_connection(..).with_upgrades()`, or `auto::Builder::serve_connection_with_upgrades(..)` from `hyper-util`. Otherwise `OnUpgrade` is never populated.

What you get back is a `WebSocketStream`, which is the point: your server owns the listener, the routing, and the TLS, and this crate hands over the socket once the upgrade completes. In exchange you wire up the connection yourself — spawn the listen, sender, and keepalive tasks, then call `Subduction::add_connection`. The axum module docs carry that code in full.

`TokioWebSocketServer` in `subduction_websocket` does that wiring for you, but it owns its own `TcpListener` and cannot accept a socket from elsewhere, so the two do not compose.

## Limitations

- No HTTP/2 support at present. This is not a design limit: extended `CONNECT` arrives through the same `OnUpgrade` and `Upgraded` types, so `from_upgraded` and everything downstream would be unchanged. hyper accepts it only when the server calls `enable_connect_protocol`, which this crate never does, so a compliant client will not attempt it and nothing breaks silently. Please file an issue if this is important for your use case.
- No `Sec-WebSocket-Protocol` negotiation. Subduction clients do not offer a subprotocol; a client that does will fail its own handshake.
- No `Origin` policy. Enforce it in middleware if browsers can reach the endpoint.
- No deadline on the upgrade. The spawned task and its hyper connection live until the client goes away; wrap `upgrade::upgrade` in `tokio::time::timeout` if that is not acceptable.
- Nothing between the handler and the socket may replace the `101`. Hyper hands the connection over when the connection ends, so a layer that rewrites that response leaves the callback to run at teardown over a stream that never spoke WebSocket.

## axum

```rust
use subduction_hyper::axum::TungsteniteUpgrade;
use tungstenite::protocol::WebSocketConfig;

async fn ws(upgrade: TungsteniteUpgrade, State(spawner): State<MySpawner>) -> Response {
    upgrade.on_upgrade(&spawner, WebSocketConfig::default(), |ws| async move {
        // `ws` is an `async_tungstenite::WebSocketStream`. Run
        // `handshake::respond` over it, then add the result to the node.
    })
}
```

Pass the spawner the node was built with, so upgrade tasks share its lifecycle.

`respond`, not `initiate`: a server accepts connections, and the two differ in more than who speaks first. The node records which side it was, so the choice outlives the handshake — and because a mutual `initiate` still authenticates, getting it backwards here fails silently rather than loudly.

The module docs carry the full callback body, and `tests/axum.rs` runs it end to end.

To drive the upgrade yourself rather than have it spawned, use `upgrade.into_parts()` with `upgrade::upgrade`. The handler must then return `upgrade::accept_response(&key)` itself, or the client waits forever for a `101` that never comes.

## Tokio

The socket is tokio-bound whichever spawner runs the tasks: `HyperIo` adapts hyper's `Upgraded` through tokio's I/O traits to the `futures` traits `async-tungstenite` wants, so a tokio reactor has to be driving it. An adapter choice rather than a design one, but it is the reason there is no `no_std` build.

If you're using tokio, `subduction_websocket::tokio` is a spawn helper:

|                     |                                                                                                                                 |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `TokioSpawn`        | Detached `tokio::spawn`. A unit struct, so `&TokioSpawn` inline works and the `State` extractor above can go.                   |
| `TrackedTokioSpawn` | Carries a `TaskTracker` that joins tasks at shutdown. Must be threaded through, or upgrade tasks sit outside graceful shutdown. |

## Raw hyper

A hyper service must return `Ok(response)` for a rejection. Returning `Err` makes hyper drop the connection without sending anything.

```rust
use std::convert::Infallible;
use subduction_hyper::upgrade;

async fn handle<Sp: Spawn<Sendable>>(
    mut req: Request<Incoming>,
    spawner: &Sp,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let key = match upgrade::validate(&req) {
        Ok(key) => key,
        Err(rejection) => return Ok(rejection.response().map(|s| Full::new(Bytes::from(s)))),
    };
    let on_upgrade = hyper::upgrade::on(&mut req);

    upgrade::spawn_upgrade(spawner, on_upgrade, WebSocketConfig::default(), |ws| async move {
        // handshake::respond, as in the axum callback above
    });

    Ok(upgrade::accept_response(&key).map(|()| Full::new(Bytes::new())))
}
```

## License

Dual-licensed under [MIT](../LICENSE-MIT) or [Apache-2.0](../LICENSE-APACHE), at your option.
