# Subduction on Cloudflare Durable Objects

> [!CAUTION]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

Runs the [Subduction](https://github.com/inkandswitch/subduction) sync layer as a
Cloudflare [Durable Object](https://developers.cloudflare.com/durable-objects/),
using **hibernatable WebSockets** and the DO's **SQLite** storage. A live
deployment is at [subduct.io](https://subduct.io).

Clients connect over WebSocket to `/sync/<room>`, perform the standard Subduction
discovery handshake, then batch-sync and subscribe to documents. The object fans
new commits out to subscribers inline, persists everything to SQLite, and can be
evicted from memory between messages — rehydrating from storage on the next one.

## Why hibernation

A naive port would keep a long-lived `recv()` loop per connection alive in
memory. That defeats the whole point of Durable Objects: an idle object can't be
evicted, so you pay for a resident isolate per open socket.

Instead this crate leans into
[WebSocket Hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/#websocket-hibernation-api).
The object does all its work **per message**, inside the `websocket_message`
event, and holds no volatile state it can't rebuild:

- **`CollectingSpawner`** — Subduction's sync handler fans out by spawning tasks.
  On a hibernatable object we can't detach them (the isolate may be evicted
  before they run), so the spawner *collects* the futures into a queue and the
  event handler awaits them inline before returning. The object stays resident
  exactly as long as it takes to write every push to the wire, then is free to
  hibernate.
- **SQLite storage (`storage.rs`)** — commits, fragments, subscriptions, replay
  nonces, and the send-counter base are all persisted. Storage is written
  against a small synchronous `Sql` trait so the same logic runs on the
  Worker's `SqlStorage` in production and on `rusqlite` in host unit tests.
- **Rehydration** — on each message the object rebuilds its in-memory
  connection/subscription state from `getWebSockets()` + SQLite, reconstructs
  authenticated peers from the persisted peer id, and re-seeds per-peer send
  counters above a persisted high-water base so `RemoteHeads` counters stay
  monotonic across evictions.
- **Alarms** — replay-nonce GC and Sedimentree compaction run off the hot path
  in the DO `alarm()` handler, and the alarm is only armed when there's actually
  something to do (a nonce recorded, or a fragment written).

```
  client ──ws──►  Worker fetch (/sync/<room>)
                       │  id_from_name(room)
                       ▼
                 SyncDurableObject  ── getWebSockets() + SQLite ─► rebuild state
                       │
                 websocket_message(msg)
                       │  SyncHandler.handle(msg)
                       │     └─ fan-out queued in CollectingSpawner
                       ▼
                 drain + await queue  ──► push to subscribers
                       │
                 persist to SQLite ──► (hibernate until next message)
```

## Routing: the `<room>` seam

`/sync/<room>` maps the `<room>` path segment to a Durable Object via
`id_from_name(room)`. `<room>` is an **opaque grouping key** — the engine never
sees it and is document-agnostic (every wire frame carries its own
`SedimentreeId`, so one object multiplexes any number of documents):

- **One room per document** (`room == doc`) — maximum isolation and horizontal
  spread; each document is its own hibernatable actor. Costs one WebSocket +
  handshake per document. This is what the [chat example](examples/chat) uses.
- **One room per workspace** (`room == workspace`) — a client multiplexes every
  document in the workspace over a *single* connection. One handshake amortised
  across the whole workspace and far fewer objects, at the cost of concentrating
  a workspace on one single-threaded object. See the
  [workspace example](examples/workspace) for a benchmark and a resilient client.

Because the object body is identical either way, granularity is purely this
routing choice plus how the client groups its sync calls.

## Layout

```
src/
  lib.rs            Worker entrypoint + /sync/<room> routing
  durable_object.rs SyncDurableObject: hibernation, handshake, alarms
  storage.rs        Sql trait + SqlStore (host-testable) + WorkerSql adapter
  spawn.rs          CollectingSpawner (inline fan-out for hibernation)
  transport.rs      DoConnection + OneShot handshake over worker::WebSocket
site/               Landing page (assembled into public/ and deployed at /)
examples/
  chat/             Standalone multi-tab chat (one room per document)
  workspace/        Multiplexing benchmark + resilient RoomClient demo
scripts/            build-site.sh / build-chat.sh / build-workspace.sh
wrangler.toml       Worker + DO + assets + custom-domain config
```

## Prerequisites

- Rust with the `wasm32-unknown-unknown` target:
  `rustup target add wasm32-unknown-unknown`
- [`worker-build`](https://crates.io/crates/worker-build): `cargo install worker-build`
- Node (for `wrangler` and, for the examples, `wasm-pack`): `npm install`

## Local development

```bash
npm install
npm run dev          # build the landing page + wrangler dev on :8787
```

Then:

- `GET /` → the landing page.
- `GET /sync/<room>` **without** a WebSocket upgrade → `426 Upgrade Required`.
- `ws://127.0.0.1:8787/sync/<room>` → the sync endpoint.

### Examples

The examples are standalone front ends; they are **not** served by the Worker.
Build the client wasm and serve them, pointing at a running service with
`?server=<ws-base>`:

```bash
npm run chat         # http://localhost:3000/?server=ws://127.0.0.1:8787
npm run workspace    # workspace multiplexing benchmark + live resilience demo
```

The workspace example measures the cost of opening an N-document workspace as one
multiplexed connection vs. one-connection-per-document, and includes a
[`RoomClient`](examples/workspace/room-client.js): one WebSocket per room,
multiplexed over N documents, with automatic reconnect + re-subscribe.

## Deployment

The deployed service is the Worker (`src/`) plus the static landing page
(`site/` → `public/`). The chat/workspace examples are intentionally not
deployed.

```bash
npm run deploy       # build the landing page + wrangler deploy
```

`wrangler.toml` provisions the `subduct.io` custom domain (the zone must exist in
the same Cloudflare account) and keeps the `*.workers.dev` URL live alongside it.

## Testing

The storage/subscription/nonce/compaction logic is host-testable through the
`Sql` trait (backed by `rusqlite`), so most behaviour runs under plain
`cargo test` with no `workerd` required:

```bash
cargo test                                      # host unit + conformance tests
cargo clippy --all-targets -- -D warnings       # host lints
cargo clippy --target wasm32-unknown-unknown    # Worker-glue lints (wasm-only code)
```

`subduction_do` is deliberately excluded from the root workspace (it's a
`wasm32` Worker with its own lockfile and toolchain flags), so it has a dedicated
CI job (`.github/workflows/test-do.yml`) running the commands above.

## Client usage

Any Subduction client works. In the browser via
[`subduction_wasm`](../subduction_wasm):

```js
const sync = new Subduction({ signer, storage });
// The discovery service name must match the object's SERVICE_NAME.
await sync.connectDiscover(new URL("wss://subduct.io/sync/my-room"), "subduction-do");
await sync.syncWithAllPeers(docId, /* subscribe */ true, 5000);
```

To multiplex many documents over one connection, connect once to a room and call
`syncWithAllPeers(docId, true)` (or `syncWithPeer`) per document — see
[`RoomClient`](examples/workspace/room-client.js).

## Admission control (atproto service auth)

The Worker can gate `/sync/<room>` behind [atproto](https://atproto.com) **service
auth**, so only holders of a Bluesky (atproto) identity may connect. This is
**admission only** — a valid identity admits the connection; once in, the
per-room `Policy` still governs reads/writes (permissive by default; see below).

It's off by default. Enable it with two vars in `wrangler.toml` (or the
dashboard):

```toml
[vars]
REQUIRE_ATPROTO_AUTH = "true"
SERVICE_DID = "did:web:subduct.io"   # the JWT `aud` clients must target
```

**How it works.** A client mints a short-lived JWT with
`com.atproto.server.getServiceAuth` (`iss` = their DID, `aud` = `SERVICE_DID`,
signed by their atproto signing key) and passes it as `?auth=<jwt>` on the
WebSocket URL. Browsers can't set custom headers on `new WebSocket(...)` and the
wasm client has no subprotocol hook, so the query string is the portable channel;
service-auth tokens are short-lived, which bounds putting one in a URL. The
Worker verifies it **offline at the edge** — resolving `iss` (`did:plc` via
plc.directory, or `did:web`) to its `#atproto` signing key and checking the
signature (`ES256`/p256, `ES256K`/k256, or `EdDSA`/ed25519), plus `aud` and
`exp` — *before* a Durable Object is ever woken. Unauthenticated upgrades get a
`401`. The verifier is a pure, [host-tested module](src/atproto.rs); only DID
resolution touches the network.

**Minting a token for testing:**

```bash
npm run mint:jwt -- --identifier you.bsky.social --password <app-password>
# create the app password at https://bsky.app/settings/app-passwords
```

It prints a JWT and a ready-to-open URL; both the chat and workspace examples
forward `?auth=<jwt>` to the service, e.g.
`examples/workspace/index.html?server=wss://subduct.io&auth=<jwt>`.

**Not yet covered** (admission-only scope): no per-room/per-document ACLs (any
valid identity reaches any room), no `jti` replay dedupe across the fleet (relies
on short `exp`), and resolved DID docs aren't cached (one resolution per
connect). All are additive follow-ups.

## Operational note

Unless admission is enabled (above), the deployed service runs a **permissive,
open policy**: anyone who knows a room key can read and write it, storage is
uncapped, and data may be reset at any time. It is for evaluation only. Self-host
with your own `Policy`/`EphemeralPolicy` for real use.

## License

See the workspace [`LICENSE`](../LICENSE) file.
