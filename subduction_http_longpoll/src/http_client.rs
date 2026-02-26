//! Generic HTTP client trait for portable long-poll transport.
//!
//! This abstracts over the HTTP client implementation, allowing the
//! long-poll client to work with both native (`reqwest`) and browser
//! (`web_sys::fetch`) HTTP clients.

use alloc::{string::String, vec::Vec};

use future_form::FutureForm;

#[cfg(feature = "reqwest")]
pub mod reqwest_client;

#[cfg(feature = "reqwest")]
pub use reqwest_client::{ReqwestHttpClient, ReqwestHttpError};

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
