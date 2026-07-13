# subduction_do — later / could-do

Running list of follow-ups for the Durable Object deploy. Nothing here is
blocking; the current state is a working, hibernatable sync service with
**optional** atproto admission (off by default). Roughly ordered by value.

## Auth & security

- [ ] **Real authorization, not just admission.** Today atproto auth is
      *admission only*: a valid identity gets in, but the per-room `Policy` is
      still `OpenPolicy` (any admitted identity can read/write any room). Promote
      to a DID→`PeerId` `Policy` with per-room / per-document ACLs, and
      eventually wire it to the Keyhive capability layer.
- [ ] **"Sign in with Bluesky" (OAuth) in the examples.** Right now a token can
      only be obtained via the `mint:jwt` CLI (app password → `getServiceAuth`).
      A real gated deploy needs an in-browser OAuth flow so users don't hand-mint
      tokens.
- [ ] **Cache resolved DID documents / signing keys** (short TTL, e.g. Workers
      Cache API or KV). Currently every connect resolves the issuer DID freshly
      — adds handshake latency, load on plc.directory, and a mild DoS amplifier.
- [ ] **Cross-fleet `jti` replay dedupe.** `prevalidate` returns `jti` but we
      don't store it; replay protection currently leans on the short `exp` +
      skew window. Persist seen `jti`s (KV or a shared nonce table) to close it.
- [ ] **Harden `did:web` resolution.** It fetches an https URL derived from the
      untrusted `iss`. Low risk today (Workers can't reach private IPs, and the
      response must still yield a key the signature verifies against), but worth:
      a host allowlist/denylist, handling ports (`%3A`) and path forms, and a
      response size cap.
- [ ] **Don't log the `?auth=` token.** Cloudflare observability may capture the
      query string (JWT included). Scrub it, or move the token to
      `Sec-WebSocket-Protocol` once the wasm client exposes a subprotocol hook.
- [ ] **Publish `/.well-known/did.json`** for `did:web:subduct.io` so the service
      DID resolves. Not required for verification (we only match `aud` as a
      string) but good hygiene / future-proofing.
- [ ] **Connect rate limiting / abuse controls** on `/sync` (per-IP / per-DID).
- [ ] `WWW-Authenticate` header on the 401, and configurable clock skew / max
      token age.

## Scaling & performance

- [ ] Benchmark room-per-workspace multiplexing at larger N and document
      guidance (when to shard a workspace across rooms).
- [ ] **Abandoned-room GC**: alarm-driven eviction / storage reclamation for
      rooms with no live sockets and no recent writes.
- [ ] Message size limits + backpressure on fan-out.
- [ ] Compaction tuning (thresholds, alarm cadence) under real write load.

## Productionization

- [ ] Replace `OpenPolicy` with a real `Policy`/`EphemeralPolicy` + storage
      quotas for any non-evaluation deploy.
- [ ] **Integration/e2e tests** against `wrangler dev` / miniflare (handshake,
      hibernation rehydration, alarm firing, admission gate). Today we have host
      unit + conformance tests and manual browser checks only.
- [ ] Extend the `test-do` CI job with a `worker-build --release` smoke build
      (currently host tests + clippy, wasm clippy — not the full wasm-bindgen
      pipeline).
- [ ] Metrics/observability: connection counts, per-room storage, admission
      accept/reject rates.

## Nice-to-haves

- [ ] A tiny status/health endpoint distinct from the landing page.
- [ ] Optional gated demo route (e.g. a separate worker/domain with
      `REQUIRE_ATPROTO_AUTH=true`) so admission can be shown off without gating
      the open `subduct.io` demo.
