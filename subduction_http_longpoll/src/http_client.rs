//! Generic HTTP client trait for portable long-poll transport.
//!
//! This abstracts over the HTTP client implementation, allowing the
//! long-poll client to work with both native (`reqwest`) and browser
//! (`web_sys::fetch`) HTTP clients.

use alloc::{string::String, vec::Vec};

use future_form::FutureForm;
use thiserror::Error;

/// A minimal async HTTP client for POST requests.
///
/// Implementations should handle the mechanics of making HTTP requests
/// (TLS, connection pooling, etc.) while this trait exposes only what
/// the long-poll transport needs.
pub trait HttpClient<K: FutureForm>: Clone {
    /// The error type for HTTP operations.
    type Error: core::error::Error + Send + 'static;

    /// Send an HTTP POST request and return the response.
    fn post(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> K::Future<'_, Result<HttpResponse, Self::Error>>;
}

/// A minimal HTTP response.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    /// The HTTP status code.
    pub status: u16,

    /// The response body bytes.
    pub body: Vec<u8>,

    /// Selected response headers (lowercase keys).
    pub headers: Vec<(String, String)>,
}

impl HttpResponse {
    /// Look up a response header by name (case-insensitive).
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        let lower = name.to_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k == &lower)
            .map(|(_, v)| v.as_str())
    }
}

/// Error type for the reqwest-based HTTP client.
#[cfg(feature = "reqwest")]
#[derive(Debug, Error)]
#[error("HTTP request failed: {0}")]
pub struct ReqwestHttpError(#[from] reqwest::Error);

/// A [`reqwest`]-backed implementation of [`HttpClient`].
#[cfg(feature = "reqwest")]
#[derive(Debug, Clone)]
pub struct ReqwestHttpClient {
    inner: reqwest::Client,
}

#[cfg(feature = "reqwest")]
impl ReqwestHttpClient {
    /// Create a new reqwest-backed HTTP client with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: reqwest::Client::new(),
        }
    }

    /// Create a new reqwest-backed HTTP client with a custom timeout.
    #[must_use]
    pub fn with_timeout(timeout: core::time::Duration) -> Self {
        Self {
            inner: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }
}

#[cfg(feature = "reqwest")]
impl Default for ReqwestHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "reqwest")]
impl HttpClient<future_form::Sendable> for ReqwestHttpClient {
    type Error = ReqwestHttpError;

    fn post(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> futures::future::BoxFuture<'_, Result<HttpResponse, Self::Error>> {
        use futures::FutureExt;

        let mut builder = self.inner.post(url);
        for &(name, value) in headers {
            builder = builder.header(name, value);
        }
        builder = builder.body(body);

        async move {
            let resp = builder.send().await?;
            let status = resp.status().as_u16();

            let resp_headers: Vec<(String, String)> = resp
                .headers()
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|v| (name.as_str().to_lowercase(), v.to_string()))
                })
                .collect();

            let body = resp.bytes().await?.to_vec();

            Ok(HttpResponse {
                status,
                body,
                headers: resp_headers,
            })
        }
        .boxed()
    }
}
