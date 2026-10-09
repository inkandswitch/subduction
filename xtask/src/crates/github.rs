//! Create git tags through the GitHub REST API.
//!
//! Tags are made with the API rather than `git push`, so the tagging job needs
//! only a `contents: write` token and no push credentials in the checkout.

use std::{env, process::Command};

use reqwest::{StatusCode, blocking::Client};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;

/// A GitHub API client for one repository.
#[derive(Debug)]
pub(crate) struct GitHub {
    client: Client,
    api: String,
    repository: String,
    token: String,
}

/// What [`GitHub::create_tag`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TagOutcome {
    /// The tag is new.
    Created,

    /// The tag already pointed at this commit.
    Exists,
}

impl GitHub {
    /// Read `GITHUB_TOKEN`, `GITHUB_REPOSITORY`, and optionally `GITHUB_API_URL`.
    pub(crate) fn from_env() -> Result<Self, GitHubError> {
        let var = |name: &'static str| env::var(name).map_err(|_| GitHubError::MissingEnv(name));
        let client = Client::builder()
            .user_agent(concat!("subduction-xtask/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            client,
            api: env::var("GITHUB_API_URL").unwrap_or_else(|_| "https://api.github.com".to_owned()),
            repository: var("GITHUB_REPOSITORY")?,
            token: var("GITHUB_TOKEN")?,
        })
    }

    /// Point `refs/tags/<name>` at `commit`, unless it already exists.
    ///
    /// An existing tag at a different commit is an error: a published version
    /// never moves.
    pub(crate) fn create_tag(&self, name: &str, commit: &str) -> Result<TagOutcome, GitHubError> {
        let repo = format!("{}/repos/{}", self.api, self.repository);
        let body = json!({ "ref": format!("refs/tags/{name}"), "sha": commit }).to_string();
        let response = self
            .client
            .post(format!("{repo}/git/refs"))
            .bearer_auth(&self.token)
            .header("Accept", "application/vnd.github+json")
            .header("Content-Type", "application/json")
            .body(body)
            .send()?;
        if response.status() != StatusCode::UNPROCESSABLE_ENTITY {
            response.error_for_status()?;
            return Ok(TagOutcome::Created);
        }

        // 422: the ref exists. Accept it only if it is the same commit.
        let existing = self
            .client
            .get(format!("{repo}/git/ref/tags/{name}"))
            .bearer_auth(&self.token)
            .header("Accept", "application/vnd.github+json")
            .send()?
            .error_for_status()?
            .text()?;
        let existing: Ref = serde_json::from_str(&existing)?;
        if existing.object.sha == commit {
            Ok(TagOutcome::Exists)
        } else {
            Err(GitHubError::TagMoved {
                tag: name.to_owned(),
                existing: existing.object.sha,
            })
        }
    }
}

/// The part of a `GET /git/ref/...` response we read.
#[derive(Deserialize)]
struct Ref {
    object: Object,
}

#[derive(Deserialize)]
struct Object {
    sha: String,
}

/// The commit being released: `GITHUB_SHA` in CI, else `git rev-parse HEAD`.
pub(crate) fn head_commit() -> Result<String, GitHubError> {
    if let Ok(sha) = env::var("GITHUB_SHA") {
        return Ok(sha);
    }
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(GitHubError::Git)?;
    if !output.status.success() {
        return Err(GitHubError::Git(std::io::Error::other(
            "git rev-parse HEAD failed",
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Creating a tag failed.
#[derive(Debug, Error)]
pub(crate) enum GitHubError {
    /// A required environment variable is not set.
    #[error("{0} is not set")]
    MissingEnv(&'static str),

    /// The API request failed.
    #[error("GitHub API request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// The API returned JSON of an unexpected shape.
    #[error("unexpected GitHub API response: {0}")]
    Parse(#[from] serde_json::Error),

    /// The tag exists at another commit.
    #[error("tag {tag} already exists at {existing}; refusing to move it")]
    TagMoved {
        /// The tag name.
        tag: String,

        /// The commit it points at.
        existing: String,
    },

    /// `git rev-parse HEAD` could not run.
    #[error("could not read the current commit: {0}")]
    Git(#[source] std::io::Error),
}
