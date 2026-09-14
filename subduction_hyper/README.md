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

`upgrade::validate` takes anything implementing `RequestHead`, which `http::Request` and `http::request::Parts` implement. Frameworks that hand you either work as they are; frameworks with their own request type need a few lines to borrow the method, version, and headers.

| Server           | Path                                                                                                                                                                                      |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| axum             | `subduction_hyper::axum::TungsteniteUpgrade` extractor (feature `axum`)                                                                                                                   |
| tower, raw hyper | `upgrade::{validate, accept_response, spawn_upgrade}` from a handler                                                                                                                      |
| poem, salvo      | Same, once their request type is passed to `validate` as `http` parts                                                                                                                     |
| rocket           | Not needed: rocket's `IoHandler` yields raw I/O. Wrap it in `TokioAdapter` and call `WebSocketStream::from_raw_socket`. (Rocket 0.5 is hyper 0.14, so this crate cannot serve it anyway.) |
| actix-web        | Not supported: `actix-ws` only exposes frames, never the connection                                                                                                                       |

## Limitations

- HTTP/1.1 today. HTTP/2 is a deferral rather than a limitation — see [HTTP/2 (RFC 8441)](#http2-rfc-8441).
- No `Sec-WebSocket-Protocol` negotiation. Subduction clients do not offer a subprotocol; a client that does will fail its own handshake.
- No `Origin` policy. Enforce it in middleware if browsers can reach the endpoint.
- No deadline on the upgrade. The spawned task and its hyper connection live until the client goes away; wrap `upgrade::upgrade` in `tokio::time::timeout` if that is not acceptable.
- Nothing between the handler and the socket may replace the `101`. Hyper hands the connection over when the connection ends, so a layer that rewrites that response leaves the callback to run at teardown over a stream that never spoke WebSocket.
- The result is a `WebSocketStream`, not a `TokioWebSocketServer` connection. That server's accepted type is fixed to plain TCP, so embedders spawn the listen / sender / keepalive tasks and call `Subduction::add_connection` themselves, as `handle_websocket` in `subduction_cli/src/server.rs` does.

## HTTP/2 (RFC 8441)

Not implemented, and not a structural limitation: it is a different handshake — `CONNECT` carrying `:protocol`, no `Sec-WebSocket-Key`, a `200` rather than a `101` — but hyper hands extended `CONNECT` over through the same `OnUpgrade` and `Upgraded` types used here, so `from_upgraded` and everything downstream would be unchanged. The addition is a sibling of `validate` plus a `200` response.

It is opt-in on both sides. hyper accepts extended `CONNECT` only when the server calls `enable_connect_protocol`, which this crate never does, so a compliant client will not attempt it and nothing breaks silently; a request that arrives regardless is refused with `400`.

Worth adding when someone needs it. h2-only ingress is the case that forces it; connection coalescing (sync sharing one connection with the HTTP routes it already shares a port with) is a secondary benefit. Measure flow control first: the default 64 KiB stream and connection windows are small relative to Subduction's sync messages, so `initial_stream_window_size` will likely need raising.

## axum

Swap the extractor type in the handler signature. Routing, middleware, and state are otherwise unchanged. The handler body changes: `on_upgrade` takes the spawner the node was built with and a `WebSocketConfig` instead of builder methods, and the callback receives a tungstenite stream instead of axum's sealed `WebSocket`. The extractor consumes the request's `OnUpgrade`, so it replaces `axum::extract::ws::WebSocketUpgrade` rather than wrapping it.

```rust
use subduction_hyper::axum::TungsteniteUpgrade;

async fn ws(upgrade: TungsteniteUpgrade, State(app): State<App>) -> Response {
    upgrade.on_upgrade(&app.spawner, app.ws_config, |ws| async move {
        // handshake::respond(WebSocketHandshake::new(ws), ...)
        // then WebSocket::new_with_keepalive(...)
    })
}
```

To control task spawning (e.g. a `TaskTracker` for graceful shutdown), use `upgrade.into_parts()` with `upgrade::upgrade` instead of `on_upgrade`. The handler must then return `upgrade::accept_response(&key)` itself, or the client waits forever for a `101` that never comes.

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

    upgrade::spawn_upgrade(&spawner, on_upgrade, WebSocketConfig::default(), |ws| async move {
        // same as above
    });

    Ok(upgrade::accept_response(&key).map(|()| Full::new(Bytes::new())))
}
```

Serve the connection with upgrades enabled: `http1::Builder::serve_connection(..).with_upgrades()`, or `auto::Builder::serve_connection_with_upgrades(..)` from `hyper-util`. Otherwise `OnUpgrade` is never populated.

## License

Dual-licensed under [MIT](../LICENSE-MIT) or [Apache-2.0](../LICENSE-APACHE), at your option.
