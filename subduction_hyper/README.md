# Subduction over hyper

> [!WARNING]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

Accept [Subduction](https://github.com/inkandswitch/subduction) WebSocket connections from any HTTP server built on hyper 1.x, and hand them to `subduction_websocket` as an `async_tungstenite::WebSocketStream`.

## Why

Framework WebSocket modules (`axum::extract::ws`, `warp::ws`, …) wrap their own copy of tungstenite and seal the stream. Nothing from them can reach `WebSocket::new_with_keepalive`. This crate goes one layer down: hyper exposes the raw post-`101` connection through `hyper::upgrade::OnUpgrade`, and that is all a WebSocket framer needs. One framer, no message conversion, and the existing keepalive / close-code / error-classification logic in `subduction_websocket` runs unchanged.

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

Only HTTP/1.1 `Upgrade:` is supported; RFC 8441 (HTTP/2 extended `CONNECT`) is rejected.

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

## Raw hyper

```rust
use subduction_hyper::upgrade::{self, Rejection};

async fn handle(mut req: Request<Incoming>) -> Result<Response<Empty<Bytes>>, Rejection> {
    let key = upgrade::validate(&req)?;
    let on_upgrade = hyper::upgrade::on(&mut req);

    upgrade::spawn_upgrade(on_upgrade, WebSocketConfig::default(), |ws| async move {
        // same as above
    });

    Ok(upgrade::accept_response(&key).map(|()| Empty::new()))
}
```

Remember `.with_upgrades()` on the hyper connection builder, or `OnUpgrade` is never populated.

## License

See the workspace [`LICENSE`](../LICENSE) file.
