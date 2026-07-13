//! Pure request-path routing, split out from the `wasm32`-only Worker glue so it
//! can be unit tested on the host (the `worker` runtime types don't compile off
//! `wasm32`). [`route`] is the single source of truth for how `fetch` classifies
//! an incoming path; the Worker entrypoint only adds the I/O around it.

/// Maximum room-key length, matching the Durable Object `id_from_name` limit of
/// 256 bytes. A longer key would fail the stub lookup with an opaque 500, so we
/// reject it up front with a clean 400.
pub const MAX_ROOM_KEY_LEN: usize = 256;

/// URL prefix carrying the room routing key: `/sync/<room>`.
const SYNC_PREFIX: &str = "/sync/";

/// The classification of an incoming request path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route<'a> {
    /// A well-formed sync request for `<room>` (an opaque grouping key). The
    /// caller maps it to a Durable Object via `id_from_name(room)`.
    Sync(&'a str),
    /// Not a `/sync/...` path: serve the static site.
    Asset,
    /// A malformed `/sync/...` path. The `&str` is a client-facing 400 message.
    BadRequest(&'static str),
}

/// Classify a (already percent-decoded) request path.
///
/// A room key must be exactly one non-empty path segment no longer than
/// [`MAX_ROOM_KEY_LEN`]. Everything that isn't a `/sync/...` path is an
/// [`Route::Asset`] so it falls through to the landing page.
#[must_use]
pub fn route(path: &str) -> Route<'_> {
    let Some(room) = path.strip_prefix(SYNC_PREFIX) else {
        return Route::Asset;
    };
    if room.is_empty() || room.contains('/') {
        return Route::BadRequest("expected /sync/<room>");
    }
    if room.len() > MAX_ROOM_KEY_LEN {
        return Route::BadRequest("room key too long (max 256 bytes)");
    }
    Route::Sync(room)
}

#[cfg(test)]
mod tests {
    use super::{route, Route, MAX_ROOM_KEY_LEN};

    #[test]
    fn routes_a_valid_room() {
        assert_eq!(route("/sync/my-room"), Route::Sync("my-room"));
        // A hex document id is just a room key whose room == doc.
        assert_eq!(route("/sync/07070707"), Route::Sync("07070707"));
    }

    #[test]
    fn non_sync_paths_fall_through_to_assets() {
        assert_eq!(route("/"), Route::Asset);
        assert_eq!(route("/index.html"), Route::Asset);
        assert_eq!(route("/favicon.ico"), Route::Asset);
        // Not our prefix even though it contains "sync".
        assert_eq!(route("/syncthing"), Route::Asset);
    }

    #[test]
    fn empty_room_is_rejected() {
        assert!(matches!(route("/sync/"), Route::BadRequest(_)));
    }

    #[test]
    fn multi_segment_room_is_rejected() {
        // A room key is a single path segment; nested paths are malformed.
        assert!(matches!(route("/sync/a/b"), Route::BadRequest(_)));
        assert!(matches!(route("/sync/room/"), Route::BadRequest(_)));
    }

    #[test]
    fn room_at_the_length_limit_is_accepted() {
        let key = "a".repeat(MAX_ROOM_KEY_LEN);
        assert_eq!(route(&format!("/sync/{key}")), Route::Sync(key.as_str()));
    }

    #[test]
    fn over_length_room_is_rejected() {
        let key = "a".repeat(MAX_ROOM_KEY_LEN + 1);
        assert!(matches!(
            route(&format!("/sync/{key}")),
            Route::BadRequest(_)
        ));
    }
}
