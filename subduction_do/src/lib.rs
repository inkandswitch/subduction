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
//! Only [`storage`] is compiled for the host (it is backend-agnostic and unit
//! tested with `cargo test`); the rest depends on the `worker` runtime and is
//! wasm-only.

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
    use worker::{event, Context, Env, Request, Response, Result};

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
