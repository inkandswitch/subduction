//! Subduction's sync layer, packaged as a Cloudflare Durable Object.
//!
//! * [`durable_object`] holds `SyncDurableObject`, the hibernatable sync engine
//!   — **one Durable Object instance per document**. The Worker derives the
//!   instance id from the document id in the request path, so each document
//!   gets its own isolate and its own SQLite database.
//! * The [`fetch`] entrypoint routes the WebSocket upgrade on `/sync/<doc>` to
//!   that document's Durable Object and lets everything else fall through to the
//!   static site (a landing page served from `public/`). The chat under
//!   `examples/chat` is a standalone example and is not part of this deploy.
//!
//! Only [`storage`] is compiled for the host (it is backend-agnostic and unit
//! tested with `cargo test`); the rest depends on the `worker` runtime and is
//! wasm-only.

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
    use worker::{event, Context, Env, Request, Response, Result};

    /// URL prefix carrying the document id: `/sync/<doc>`.
    const SYNC_PREFIX: &str = "/sync/";

    /// Worker entrypoint. `/sync/<doc>` is upgraded and forwarded to the Durable
    /// Object dedicated to `<doc>`; all other paths are served from the static
    /// site (the landing page in `public/`).
    ///
    /// Routing one Durable Object per document (rather than a single global
    /// object) is what lets the deployment scale: each document is an
    /// independent, individually-hibernatable actor with its own SQLite, so load
    /// and storage spread across the fleet instead of piling onto one isolate.
    #[event(fetch)]
    async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
        console_error_panic_hook::set_once();

        let path = req.path();
        if let Some(doc) = path.strip_prefix(SYNC_PREFIX) {
            // Trailing-slash / empty document ids are not routable.
            if doc.is_empty() || doc.contains('/') {
                return Response::error("expected /sync/<document-id>", 400);
            }
            // `id_from_name(doc)` is a stable, collision-resistant mapping from
            // the document id to a Durable Object id, so every client naming the
            // same document reaches the same instance.
            let namespace = env.durable_object("SYNC")?;
            let stub = namespace.id_from_name(doc)?.get_stub()?;
            return stub.fetch_with_request(req).await;
        }

        // Static landing page (served from `public/`).
        env.assets("ASSETS")?.fetch_request(req).await
    }
}
