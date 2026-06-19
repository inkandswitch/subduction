//! Localhost admin HTTP server exposing live store inspection.
//!
//! Inspection runs in-process against the server's own [`RedbStorage`] handle:
//! a redb read transaction (MVCC), concurrent with the live writer, so the
//! store can be queried without stopping the server. The listener is meant for
//! loopback only — it exposes storage internals (tree ids, heads, digests) and
//! has no authentication.
//!
//! Routes (all `GET`, plain-text responses):
//!
//! ```text
//! /inspect                       whole-store summary + per-tree table
//! /inspect/tree/{id}             one tree's stats
//! /inspect/tree/{id}/heads       that tree's commit + fragment heads
//! /inspect/find/{head}           locate a head id across every tree
//! ```
//!
//! `heads` and `find` accept `?digests=true` to include content digests.

use std::{fmt::Write as _, net::SocketAddr};

use axum::{
    Router,
    extract::{Path, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};
use subduction_redb_storage::{
    HeadKind, HeadLocation, RedbStorage, StoreStats, TreeHeads, TreeStats,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Start the localhost admin server, spawning a task that serves inspection
/// routes until `token` is cancelled. Returns the bound address (useful when
/// `addr`'s port is `0`).
///
/// # Errors
///
/// Returns an error if the listener cannot bind to `addr`.
pub(crate) async fn start_admin_server(
    addr: SocketAddr,
    storage: RedbStorage,
    token: CancellationToken,
) -> eyre::Result<SocketAddr> {
    if !addr.ip().is_loopback() {
        tracing::warn!(
            %addr,
            "Admin (inspect) server bound to a non-loopback address; it has no auth and \
             exposes storage internals — prefer a 127.0.0.1 address"
        );
    }

    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(addr = %bound, "Admin (inspect) server listening");

    let app = router(storage);
    tokio::spawn(async move {
        let server = axum::serve(listener, app)
            .with_graceful_shutdown(async move { token.cancelled().await });
        if let Err(e) = server.await {
            tracing::error!(error = %e, "Admin server error");
        }
    });

    Ok(bound)
}

/// Build the inspection router over a shared storage handle.
fn router(storage: RedbStorage) -> Router {
    Router::new()
        .route("/inspect", get(overview))
        .route("/inspect/heads", get(all_heads))
        .route("/inspect/tree/{id}", get(tree))
        .route("/inspect/tree/{id}/heads", get(tree_heads))
        .route("/inspect/find/{head}", get(find_head))
        .with_state(storage)
}

async fn overview(State(storage): State<RedbStorage>) -> (StatusCode, String) {
    match storage.inspect_overview().await {
        Ok((store, trees)) => (StatusCode::OK, render_overview(&store, &trees)),
        Err(e) => internal_error(&e),
    }
}

async fn tree(State(storage): State<RedbStorage>, Path(id): Path<String>) -> (StatusCode, String) {
    let id = match parse_tree_id(&id) {
        Ok(id) => id,
        Err(resp) => return resp,
    };
    match storage.inspect_tree(id).await {
        Ok(Some(stats)) => (StatusCode::OK, render_tree(&stats)),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            format!("tree {} not found\n", hex::encode(id.as_bytes())),
        ),
        Err(e) => internal_error(&e),
    }
}

async fn tree_heads(
    State(storage): State<RedbStorage>,
    Path(id): Path<String>,
    RawQuery(query): RawQuery,
) -> (StatusCode, String) {
    let id = match parse_tree_id(&id) {
        Ok(id) => id,
        Err(resp) => return resp,
    };
    let with_digests = query_flag(query.as_deref(), "digests");
    match storage.inspect_tree_heads(id, with_digests).await {
        Ok(heads) => (StatusCode::OK, render_heads(id, &heads)),
        Err(e) => internal_error(&e),
    }
}

async fn all_heads(
    State(storage): State<RedbStorage>,
    RawQuery(query): RawQuery,
) -> (StatusCode, String) {
    let with_digests = query_flag(query.as_deref(), "digests");
    match storage.inspect_all_heads(with_digests).await {
        Ok(trees) => (StatusCode::OK, render_all_heads(&trees)),
        Err(e) => internal_error(&e),
    }
}

async fn find_head(
    State(storage): State<RedbStorage>,
    Path(head): Path<String>,
    RawQuery(query): RawQuery,
) -> (StatusCode, String) {
    let head = match parse_head(&head) {
        Ok(head) => head,
        Err(resp) => return resp,
    };
    let with_digests = query_flag(query.as_deref(), "digests");
    // Optional `?tree=<hex>` filters the results to one tree (the scan still
    // covers every tree, then we keep only matches in that tree).
    let tree_filter = match query_value(query.as_deref(), "tree") {
        Some(t) => match parse_tree_id(t) {
            Ok(id) => Some(id),
            Err(resp) => return resp,
        },
        None => None,
    };

    match storage.inspect_find_head(head, with_digests).await {
        Ok(mut locations) => {
            if let Some(tree) = tree_filter {
                locations.retain(|l| l.tree == tree);
            }
            (StatusCode::OK, render_find(head, &locations))
        }
        Err(e) => internal_error(&e),
    }
}

// ── parsing / errors ──────────────────────────────────────────────────

fn parse_tree_id(s: &str) -> Result<SedimentreeId, (StatusCode, String)> {
    crate::parse_32_bytes(s, "tree id")
        .map(SedimentreeId::new)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid tree id: {e}\n")))
}

fn parse_head(s: &str) -> Result<CommitId, (StatusCode, String)> {
    crate::parse_32_bytes(s, "head id")
        .map(CommitId::new)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid head id: {e}\n")))
}

fn internal_error(e: &impl std::fmt::Display) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("inspection failed: {e}\n"),
    )
}

/// `digests`, `digests=true`, or `digests=1` ⇒ true.
fn query_flag(query: Option<&str>, key: &str) -> bool {
    query.unwrap_or_default().split('&').any(|pair| {
        let mut kv = pair.splitn(2, '=');
        kv.next() == Some(key) && matches!(kv.next(), None | Some("true" | "1"))
    })
}

/// The value of the first `key=value` pair matching `key`, if any.
fn query_value<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
    query?.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then_some(v)
    })
}

// ── rendering ─────────────────────────────────────────────────────────

fn render_overview(store: &StoreStats, trees: &[TreeStats]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "store");
    let _ = writeln!(out, "  trees:      {}", store.trees);
    let _ = writeln!(out, "  commits:    {}", store.commits);
    let _ = writeln!(out, "  fragments:  {}", store.fragments);
    let _ = writeln!(out, "  redb file:  {} bytes", store.redb_file_bytes);
    let _ = writeln!(
        out,
        "  blob files: {} ({} bytes)",
        store.blob_file_count, store.blob_total_bytes
    );
    let _ = writeln!(out, "  logical:    {} bytes", store.logical_bytes);

    if !trees.is_empty() {
        let _ = writeln!(out, "\ntrees (by sedimentree id):");
        for t in trees {
            let _ = writeln!(
                out,
                "  {}  commits={} fragments={} bytes={}",
                hex::encode(t.id.as_bytes()),
                t.commits,
                t.fragments,
                t.bytes,
            );
        }
    }
    out
}

fn render_tree(stats: &TreeStats) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "tree {}", hex::encode(stats.id.as_bytes()));
    let _ = writeln!(out, "  commits:   {}", stats.commits);
    let _ = writeln!(out, "  fragments: {}", stats.fragments);
    let _ = writeln!(out, "  bytes:     {}", stats.bytes);
    out
}

fn render_heads(id: SedimentreeId, heads: &TreeHeads) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "tree {} heads", hex::encode(id.as_bytes()));

    for (label, entries) in [("commits", &heads.commits), ("fragments", &heads.fragments)] {
        let _ = writeln!(out, "{label} ({}):", entries.len());
        for entry in entries {
            let suffix = if entry.variants > 1 {
                format!("  (equivocated ×{})", entry.variants)
            } else {
                String::new()
            };
            let _ = writeln!(out, "  {}{suffix}", hex::encode(entry.id.as_bytes()));
            for digest in &entry.digests {
                let _ = writeln!(out, "    {}", hex::encode(digest));
            }
        }
    }
    out
}

fn render_all_heads(trees: &[(SedimentreeId, TreeHeads)]) -> String {
    if trees.is_empty() {
        return "no trees\n".to_owned();
    }
    let mut out = String::new();
    for (id, heads) in trees {
        out.push_str(&render_heads(*id, heads));
        out.push('\n');
    }
    out
}

fn render_find(head: CommitId, locations: &[HeadLocation]) -> String {
    let mut out = String::new();
    if locations.is_empty() {
        let _ = writeln!(out, "head {} not found", hex::encode(head.as_bytes()));
        return out;
    }

    let _ = writeln!(
        out,
        "head {} found in {} location(s):",
        hex::encode(head.as_bytes()),
        locations.len()
    );
    for loc in locations {
        let kind = match loc.kind {
            HeadKind::Commit => "commit",
            HeadKind::Fragment => "fragment",
        };
        let _ = writeln!(
            out,
            "  tree {}  {kind}  variants={}",
            hex::encode(loc.tree.as_bytes()),
            loc.variants,
        );
        for digest in &loc.digests {
            let _ = writeln!(out, "    {}", hex::encode(digest));
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use subduction_redb_storage::HeadEntry;

    use super::*;

    fn sid(b: u8) -> SedimentreeId {
        SedimentreeId::new([b; 32])
    }

    fn cid(b: u8) -> CommitId {
        CommitId::new([b; 32])
    }

    #[test]
    fn overview_lists_per_tree_rows() {
        let store = StoreStats {
            trees: 1,
            commits: 2,
            fragments: 1,
            logical_bytes: 100,
            redb_file_bytes: 4096,
            blob_file_count: 1,
            blob_total_bytes: 20480,
        };
        let trees = [TreeStats {
            id: sid(0xA1),
            commits: 2,
            fragments: 1,
            bytes: 100,
        }];
        let out = render_overview(&store, &trees);
        assert!(out.contains("trees:      1"), "{out}");
        assert!(out.contains("blob files: 1 (20480 bytes)"), "{out}");
        assert!(out.contains(&hex::encode([0xA1; 32])), "{out}");
        assert!(out.contains("commits=2 fragments=1 bytes=100"), "{out}");
    }

    #[test]
    fn heads_annotate_equivocation_and_digests() {
        let heads = TreeHeads {
            commits: vec![
                HeadEntry {
                    id: cid(0x01),
                    variants: 2,
                    digests: vec![[0xAA; 32], [0xBB; 32]],
                },
                HeadEntry {
                    id: cid(0x02),
                    variants: 1,
                    digests: Vec::new(),
                },
            ],
            fragments: Vec::new(),
        };
        let out = render_heads(sid(0xA1), &heads);
        assert!(out.contains("commits (2):"), "{out}");
        assert!(out.contains("equivocated ×2"), "{out}");
        assert!(out.contains(&hex::encode([0xAA; 32])), "{out}");
        assert!(out.contains("fragments (0):"), "{out}");
    }

    #[test]
    fn find_renders_not_found_when_empty() {
        assert!(render_find(cid(0x09), &[]).contains("not found"));
    }

    #[test]
    fn query_helpers_parse_flags_and_values() {
        assert!(query_flag(Some("digests"), "digests"));
        assert!(query_flag(Some("digests=true"), "digests"));
        assert!(query_flag(Some("foo=1&digests=1"), "digests"));
        assert!(!query_flag(Some("digests=0"), "digests"));
        assert!(!query_flag(None, "digests"));
        assert_eq!(
            query_value(Some("tree=aa&digests=true"), "tree"),
            Some("aa")
        );
        assert_eq!(query_value(Some("digests=true"), "tree"), None);
    }

    #[tokio::test]
    async fn serves_inspection_over_http() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = RedbStorage::new(dir.path()).expect("open storage");
        let token = CancellationToken::new();
        let addr = start_admin_server("127.0.0.1:0".parse().expect("addr"), storage, token.clone())
            .await
            .expect("start admin server");
        let base = format!("http://{addr}");

        // Overview of an empty store.
        let body = reqwest::get(format!("{base}/inspect"))
            .await
            .expect("get /inspect")
            .text()
            .await
            .expect("text");
        assert!(body.contains("trees:      0"), "{body}");

        // Unknown (but well-formed) tree id → 404.
        let resp = reqwest::get(format!("{base}/inspect/tree/{}", hex::encode([0u8; 32])))
            .await
            .expect("get tree");
        assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

        // Malformed id → 400.
        let resp = reqwest::get(format!("{base}/inspect/tree/zzzz"))
            .await
            .expect("get bad tree");
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

        // Find an absent head → 200 with "not found".
        let body = reqwest::get(format!("{base}/inspect/find/{}", hex::encode([7u8; 32])))
            .await
            .expect("get find")
            .text()
            .await
            .expect("text");
        assert!(body.contains("not found"), "{body}");

        // All heads of an empty store.
        let body = reqwest::get(format!("{base}/inspect/heads"))
            .await
            .expect("get heads")
            .text()
            .await
            .expect("text");
        assert!(body.contains("no trees"), "{body}");

        token.cancel();
    }
}
