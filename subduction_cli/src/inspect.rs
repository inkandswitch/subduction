//! `subduction inspect` — query a running server's admin endpoint.
//!
//! A thin HTTP client over the server's `--admin-addr` listener. Inspection
//! runs *inside* the server (the only process that may hold the redb handle),
//! so this works against a live server. The server renders the response; this
//! command just prints it.

use std::net::SocketAddr;

use eyre::{Context, Result};

/// Default admin address — must match the server's `--admin-addr`.
const DEFAULT_ADMIN_ADDR: &str = "127.0.0.1:9091";

/// Arguments for the inspect command.
#[derive(Debug, clap::Parser)]
pub(crate) struct InspectArgs {
    /// Admin address of the running server (its `--admin-addr`).
    #[arg(long, default_value = DEFAULT_ADMIN_ADDR)]
    pub(crate) admin: SocketAddr,

    /// Restrict to one sedimentree (64 hex characters).
    #[arg(long, value_name = "HEX")]
    pub(crate) tree: Option<String>,

    /// Look up a single commit/fragment head (64 hex characters). Scoped to
    /// `--tree` when given, otherwise searched across every tree.
    #[arg(long, value_name = "HEX")]
    pub(crate) id: Option<String>,

    /// List commit and fragment heads (of `--tree`, or of every tree).
    #[arg(long, default_value_t = false)]
    pub(crate) ids: bool,

    /// Include content digests in head listings.
    #[arg(long, default_value_t = false)]
    pub(crate) digests: bool,
}

/// Run the inspect command.
pub(crate) async fn run(args: InspectArgs) -> Result<()> {
    let url = build_url(&args);

    let response = reqwest::get(&url).await.wrap_err_with(|| {
        format!(
            "request to {url} failed — is the server running with --admin-addr {}?",
            args.admin
        )
    })?;
    let status = response.status();
    let body = response.text().await.wrap_err("read admin response")?;

    print!("{body}");
    if !status.is_success() {
        eyre::bail!("admin endpoint returned {status}");
    }
    Ok(())
}

/// Map the flags to an admin URL.
fn build_url(args: &InspectArgs) -> String {
    let mut query: Vec<String> = Vec::new();
    if args.digests && (args.ids || args.id.is_some()) {
        query.push("digests=true".to_owned());
    }

    let path = if let Some(id) = &args.id {
        if let Some(tree) = &args.tree {
            query.push(format!("tree={tree}"));
        }
        format!("/inspect/find/{id}")
    } else if args.ids {
        args.tree.as_ref().map_or_else(
            || "/inspect/heads".to_owned(),
            |tree| format!("/inspect/tree/{tree}/heads"),
        )
    } else if let Some(tree) = &args.tree {
        format!("/inspect/tree/{tree}")
    } else {
        "/inspect".to_owned()
    };

    let query = if query.is_empty() {
        String::new()
    } else {
        format!("?{}", query.join("&"))
    };

    format!("http://{}{path}{query}", args.admin)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn args() -> InspectArgs {
        InspectArgs {
            admin: "127.0.0.1:9091".parse().expect("addr"),
            tree: None,
            id: None,
            ids: false,
            digests: false,
        }
    }

    #[test]
    fn url_maps_flags_to_routes() {
        let base = "http://127.0.0.1:9091";

        assert_eq!(build_url(&args()), format!("{base}/inspect"));

        let mut a = args();
        a.tree = Some("aa".into());
        assert_eq!(build_url(&a), format!("{base}/inspect/tree/aa"));

        a.ids = true;
        assert_eq!(build_url(&a), format!("{base}/inspect/tree/aa/heads"));

        a.digests = true;
        assert_eq!(
            build_url(&a),
            format!("{base}/inspect/tree/aa/heads?digests=true")
        );

        let mut all = args();
        all.ids = true;
        assert_eq!(build_url(&all), format!("{base}/inspect/heads"));

        let mut find = args();
        find.id = Some("bb".into());
        assert_eq!(build_url(&find), format!("{base}/inspect/find/bb"));

        find.tree = Some("aa".into());
        find.digests = true;
        assert_eq!(
            build_url(&find),
            format!("{base}/inspect/find/bb?digests=true&tree=aa")
        );
    }
}
