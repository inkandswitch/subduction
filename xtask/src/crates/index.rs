//! Which versions of a crate crates.io has, read from the sparse index.
//!
//! The index lists yanked versions too. That is correct here: a yanked version
//! cannot be published again. The index can lag an upload by a few minutes,
//! so re-running a release right after a partial failure may retry an upload
//! that crates.io rejects as a duplicate; wait and run it again.

use std::{collections::BTreeSet, ops::Range};

use reqwest::{StatusCode, blocking::Client};
use semver::Version;
use serde::Deserialize;
use thiserror::Error;

const SPARSE_INDEX: &str = "https://index.crates.io";

/// A crates.io sparse index client.
#[derive(Debug)]
pub(crate) struct Index {
    client: Client,
}

/// What the registry knows about one crate.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Listing {
    /// Never published.
    Missing,

    /// Every published version, yanked or not.
    Versions(BTreeSet<Version>),
}

impl Index {
    pub(crate) fn crates_io() -> Result<Self, IndexError> {
        let client = Client::builder()
            .user_agent(concat!(
                "subduction-xtask/",
                env!("CARGO_PKG_VERSION"),
                " (https://github.com/inkandswitch/subduction)"
            ))
            .build()?;
        Ok(Self { client })
    }

    pub(crate) fn listing(&self, name: &str) -> Result<Listing, IndexError> {
        let url = format!("{SPARSE_INDEX}/{}", index_path(name));
        let response = self.client.get(&url).send()?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(Listing::Missing);
        }
        parse_listing(&response.error_for_status()?.text()?)
    }
}

/// The sparse index path for a crate, per the Cargo registry docs.
fn index_path(name: &str) -> String {
    let name = name.to_ascii_lowercase();
    let part = |range: Range<usize>| name.get(range).unwrap_or_default();
    match name.len() {
        0..=2 => format!("{}/{name}", name.len()),
        3 => format!("3/{}/{name}", part(0..1)),
        _ => format!("{}/{}/{name}", part(0..2), part(2..4)),
    }
}

/// One JSON object per line, each with at least a `vers` field.
fn parse_listing(body: &str) -> Result<Listing, IndexError> {
    #[derive(Deserialize)]
    struct Entry {
        vers: Version,
    }

    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Entry>(line).map(|entry| entry.vers))
        .collect::<Result<_, _>>()
        .map(Listing::Versions)
        .map_err(IndexError::Parse)
}

/// Querying the index failed.
#[derive(Debug, Error)]
pub(crate) enum IndexError {
    /// The request failed or returned an error status other than 404.
    #[error("crates.io index request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// An index line was not the expected JSON.
    #[error("unexpected crates.io index entry: {0}")]
    Parse(#[source] serde_json::Error),
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn index_paths_match_the_registry_layout() {
        for (name, path) in [
            ("a", "1/a"),
            ("ab", "2/ab"),
            ("abc", "3/a/abc"),
            ("serde", "se/rd/serde"),
            ("Subduction_Core", "su/bd/subduction_core"),
        ] {
            assert_eq!(index_path(name), path);
        }
    }

    #[test]
    fn index_path_follows_the_layout_for_long_names() {
        bolero::check!().with_type::<String>().for_each(|name| {
            let name: String = name
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                .collect();
            let lower = name.to_ascii_lowercase();
            if let (Some(first), Some(second)) = (lower.get(0..2), lower.get(2..4)) {
                assert_eq!(index_path(&name), format!("{first}/{second}/{lower}"));
            }
        });
    }

    #[test]
    fn listing_collects_versions_and_ignores_other_fields() {
        let body = concat!(
            "{\"name\":\"x\",\"vers\":\"0.1.0\",\"yanked\":true}\n",
            "\n",
            "{\"name\":\"x\",\"vers\":\"0.2.0-rc.1\",\"deps\":[]}\n",
        );
        let expected = ["0.1.0", "0.2.0-rc.1"]
            .into_iter()
            .map(|v| Version::parse(v).expect("valid version"))
            .collect();
        assert_eq!(parse_listing(body).ok(), Some(Listing::Versions(expected)));
    }
}
