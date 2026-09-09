# Subduction over hyper

> [!WARNING]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

Accept [Subduction](https://github.com/inkandswitch/subduction) WebSocket connections from any HTTP server built on hyper 1.x, and hand them to `subduction_websocket` as an `async_tungstenite::WebSocketStream`.

## Why

Framework WebSocket modules (`axum::extract::ws`, `warp::ws`, …) wrap their own copy of tungstenite and seal the stream, so nothing from them can reach `WebSocket::new_with_keepalive`. The existing `hyper-tungstenite` crate has the same problem from the other side: it yields a `tokio_tungstenite::WebSocketStream`, and `subduction_websocket` is built on `async-tungstenite`.

This crate goes one layer down. hyper exposes the raw post-`101` connection through `hyper::upgrade::OnUpgrade`, and that is all a WebSocket framer needs. One framer, no message conversion, and the keepalive / close-code / error-classification logic in `subduction_websocket` runs unchanged.

```
HTTP request ─▶ upgrade::validate(parts)        ─▶ AcceptKey     (http types only)
             ─▶ upgrade::accept_response(key)   ─▶ 101 response  (send it)
             ─▶ OnUpgrade.await                 ─▶ Upgraded      (hyper hands over the pipe)
             ─▶ upgrade::from_upgraded(io, cfg) ─▶ WebSocketStream<HyperIo>
```

## Coverage

| Server | Path |
|--------|------|
| axum | `axum::TungsteniteUpgrade` extractor (feature `axum`) |
| poem, salvo, tower, raw hyper | `upgrade::{validate, accept_response, spawn_upgrade}` from a handler |
| rocket | Not needed: rocket's `IoHandler` yields raw I/O; use `WebSocketStream::from_raw_socket` directly |
| actix-web | Not supported: `actix-ws` only exposes frames, never the connection |

## Limitations

- HTTP/1.1 only. RFC 8441 (WebSocket over HTTP/2 extended `CONNECT`) is rejected with `400`.
- No `Sec-WebSocket-Protocol` negotiation. Subduction clients do not offer a subprotocol; a client that does will fail its own handshake.
- No `Origin` policy. Enforce it in middleware if browsers can reach the endpoint.
- The result is a `WebSocketStream`, not a `TokioWebSocketServer` connection. That server's accepted type is fixed to plain TCP, so embedders spawn the listen / sender / keepalive tasks and call `Subduction::add_connection` themselves, as `subduction_cli/src/server.rs` does.

## axum

Swap the extractor type in the handler signature. Routing, middleware, and state are otherwise unchanged. The extractor consumes the request's `OnUpgrade`, so it replaces `axum::extract::ws::WebSocketUpgrade` rather than wrapping it.

```rust
use subduction_hyper::axum::TungsteniteUpgrade;

async fn ws(upgrade: TungsteniteUpgrade, State(app): State<App>) -> Response {
    upgrade.on_upgrade(app.ws_config, |ws| async move {
        // handshake::respond(WebSocketHandshake::new(ws), ...)
        // then WebSocket::new_with_keepalive(...)
    })
}
```

To control task spawning (e.g. a `TaskTracker` for graceful shutdown), use `upgrade.into_parts()` with `upgrade::upgrade` instead of `on_upgrade`.

## Raw hyper

A hyper service must return `Ok(response)` for a rejection. Returning `Err` makes hyper drop the connection without sending anything.

```rust
use std::convert::Infallible;
use subduction_hyper::upgrade;

async fn handle(mut req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let key = match upgrade::validate(&req) {
        Ok(key) => key,
        Err(rejection) => return Ok(rejection.response().map(|s| Full::new(Bytes::from(s)))),
    };
    let on_upgrade = hyper::upgrade::on(&mut req);

    upgrade::spawn_upgrade(on_upgrade, WebSocketConfig::default(), |ws| async move {
        // same as above
    });

    Ok(upgrade::accept_response(&key).map(|()| Full::new(Bytes::new())))
}
```

Serve the connection with upgrades enabled: `http1::Builder::serve_connection(..).with_upgrades()`, or `auto::Builder::serve_connection_with_upgrades(..)` from `hyper-util`. Otherwise `OnUpgrade` is never populated.

## License

Dual-licensed under [MIT](../LICENSE-MIT) or [Apache-2.0](../LICENSE-APACHE), at your option.
