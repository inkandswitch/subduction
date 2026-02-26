# subduction_http_longpoll

HTTP long-poll transport layer for the [Subduction](https://github.com/inkandswitch/subduction) sync protocol.

Provides an alternative to WebSocket for environments where WebSocket connections are unreliable or unavailable (e.g., restrictive proxies, corporate firewalls, some serverless platforms).

## Protocol

| Endpoint            | Method | Purpose                                |
|---------------------|--------|----------------------------------------|
| `/lp/handshake`     | POST   | Ed25519 mutual authentication          |
| `/lp/send`          | POST   | Client sends a message to the server   |
| `/lp/recv`          | POST   | Client long-polls for the next message |
| `/lp/disconnect`    | POST   | Clean session teardown                 |
