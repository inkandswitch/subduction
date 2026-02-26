//! [`reqwest`]-backed implementation of [`HttpClient`].

use alloc::{string::String, vec::Vec};
use core::time::Duration;

use futures::FutureExt;

use super::{HttpClient, HttpResponse};

/// Error type for the reqwest-based HTTP client.
#[derive(Debug, thiserror::Error)]
#[error("HTTP request failed: {0}")]
pub struct ReqwestHttpError(#[from] reqwest::Error);

/// A [`reqwest`]-backed implementation of [`HttpClient`].
#[derive(Debug, Clone)]
pub struct ReqwestHttpClient {
    inner: reqwest::Client,
}

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
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            inner: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }
}

impl Default for ReqwestHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient<future_form::Sendable> for ReqwestHttpClient {
    type Error = ReqwestHttpError;

    fn post(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> futures::future::BoxFuture<'_, Result<HttpResponse, Self::Error>> {
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
