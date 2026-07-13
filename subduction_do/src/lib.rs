//! Subduction's sync layer, packaged as a Cloudflare Durable Object.
//!
//! * [`durable_object`] holds `SyncDurableObject`, the hibernatable sync engine.
//!   The engine is **document-agnostic and multi-tree**: it never sees the
//!   routing key, and its storage, subscription map, and fan-out are all keyed
//!   by `SedimentreeId` (the per-message document id on the wire). A single
//!   instance can therefore host any number of documents.
//! * The [`fetch`] entrypoint routes the WebSocket upgrade on `/sync/<room>` to
//!   the Durable Object for `<room>` and lets everything else fall through to
//!   the static site (a landing page served from `public/`). The chat under
//!   `examples/chat` is a standalone example and is not part of this deploy.
//!
//! # Routing granularity (the `<room>` seam)
//!
//! `<room>` is an **opaque grouping key**, not necessarily a document id. It
//! decides which documents share an isolate + SQLite + client connection:
//!
//! * **One room per document** (`room == doc`) — the default the chat example
//!   uses. Maximum isolation and horizontal spread (each doc is its own
//!   hibernatable actor), at the cost of one WebSocket + handshake per document.
//! * **One room per workspace** (`room == workspace`, many docs) — the client
//!   multiplexes every document in the workspace over a *single* connection
//!   (the wire protocol tags every frame with its `SedimentreeId`, so the DO
//!   demuxes them into per-tree state for free). One handshake amortised across
//!   the whole workspace, far fewer objects — trading per-document isolation
//!   for a single-threaded room. See `examples/workspace` for a benchmark.
//!
//! Because the DO body is identical either way, the granularity is purely this
//! routing decision plus how the client groups its `syncWithAllPeers` calls.
//!
//! # Admission control (optional)
//!
//! When the `REQUIRE_ATPROTO_AUTH` var is `"true"`, [`fetch`] requires each
//! `/sync` upgrade to carry a valid atproto service-auth JWT (`?auth=<jwt>`,
//! `aud == SERVICE_DID`) and verifies it offline (see [`atproto`]) *before*
//! waking a Durable Object. It is **admission only**: a valid identity admits
//! the connection; the per-room `Policy` still governs reads/writes.
//!
//! Only [`storage`] is compiled for the host (it is backend-agnostic and unit
//! tested with `cargo test`); the rest depends on the `worker` runtime and is
//! wasm-only.

pub mod atproto;
pub mod routing;
pub mod storage;

#[cfg(target_arch = "wasm32")]
mod durable_object;
#[cfg(target_arch = "wasm32")]
mod spawn;
#[cfg(target_arch = "wasm32")]
mod transport;

#[cfg(target_arch = "wasm32")]
pub use durable_object::SyncDurableObject;

#[cfg(target_arch = "wasm32")]
mod worker_entry {
    use crate::routing::{route, Route};
    use worker::{event, Context, Date, Env, Request, Response, Result};

    /// Default service DID clients target as the JWT `aud`. Overridable via the
    /// `SERVICE_DID` var so the same binary works on other hostnames.
    const DEFAULT_SERVICE_DID: &str = "did:web:subduct.io";

    /// Enforce atproto **service-auth** admission on a `/sync` request when the
    /// `REQUIRE_ATPROTO_AUTH` var is `"true"`. Returns `Ok(None)` to admit (or
    /// when auth is disabled), or `Ok(Some(response))` with the rejection to send.
    ///
    /// The token rides on the WebSocket URL as `?auth=<jwt>` — browsers can't set
    /// custom headers on `new WebSocket(...)`, and the wasm client doesn't expose
    /// a subprotocol hook, so the query string is the only portable channel.
    /// Service-auth JWTs are short-lived, which bounds the exposure of putting one
    /// in the URL. Verification happens here, at the edge, so unauthenticated
    /// requests never wake a Durable Object.
    async fn admit_or_reject(req: &Request, env: &Env) -> Result<Option<Response>> {
        let required = env
            .var("REQUIRE_ATPROTO_AUTH")
            .map(|v| v.to_string())
            .unwrap_or_default()
            == "true";
        if !required {
            return Ok(None);
        }

        let service_did = env
            .var("SERVICE_DID")
            .map(|v| v.to_string())
            .unwrap_or_else(|_| DEFAULT_SERVICE_DID.to_string());

        let token = req
            .url()?
            .query_pairs()
            .find(|(k, _)| k == "auth")
            .map(|(_, v)| v.into_owned());
        let Some(token) = token else {
            return Ok(Some(Response::error("missing atproto auth token", 401)?));
        };

        let now = (Date::now().as_millis() / 1000) as i64;
        match crate::atproto::admit(&token, now, &service_did).await {
            Ok(did) => {
                worker::console_log!("admitted atproto identity {did}");
                Ok(None)
            }
            Err(e) => Ok(Some(Response::error(
                format!("atproto auth failed: {e}"),
                401,
            )?)),
        }
    }

    /// Worker entrypoint. `/sync/<room>` is upgraded and forwarded to the
    /// Durable Object for `<room>`; all other paths are served from the static
    /// site (the landing page in `public/`).
    ///
    /// `<room>` is an opaque grouping key (see the crate docs): using the
    /// document id gives one isolated object per document; using a workspace id
    /// lets a client multiplex every document in that workspace over a single
    /// connection to a single object. Either way this is just the routing
    /// choice — the engine behind it is the same document-agnostic, multi-tree
    /// object, so load and storage spread across the fleet at whatever
    /// granularity the caller picks.
    ///
    /// Path classification lives in [`crate::routing`] so it can be unit tested
    /// on the host; this function only wires it to the Worker's I/O.
    #[event(fetch)]
    async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
        console_error_panic_hook::set_once();

        match route(&req.path()) {
            // `id_from_name(room)` is a stable, collision-resistant mapping from
            // the room key to a Durable Object id, so every client naming the
            // same room reaches the same instance.
            Route::Sync(room) => {
                // Admission control (atproto service auth), enforced at the edge
                // before we spin up / wake the object. No-op unless enabled.
                if let Some(rejection) = admit_or_reject(&req, &env).await? {
                    return Ok(rejection);
                }
                let namespace = env.durable_object("SYNC")?;
                let stub = namespace.id_from_name(room)?.get_stub()?;
                stub.fetch_with_request(req).await
            }
            Route::BadRequest(msg) => Response::error(msg, 400),
            // Static landing page (served from `public/`).
            Route::Asset => env.assets("ASSETS")?.fetch_request(req).await,
        }
    }
}
