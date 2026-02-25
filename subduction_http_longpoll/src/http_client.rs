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

// ---------------------------------------------------------------------------
// reqwest (native)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// web_sys::fetch (Wasm)
// ---------------------------------------------------------------------------

/// Error type for the `web_sys::fetch`-backed HTTP client.
#[cfg(feature = "wasm")]
#[derive(Debug, Error)]
#[error("{0}")]
pub struct FetchHttpError(String);

/// A [`web_sys::fetch`]-backed implementation of [`HttpClient`] for browser
/// and service-worker environments.
///
/// Since `web_sys` types are `!Send`, this implements `HttpClient<Local>`.
#[cfg(feature = "wasm")]
#[derive(Debug, Clone, Copy)]
pub struct FetchHttpClient;

#[cfg(feature = "wasm")]
impl FetchHttpClient {
    /// Create a new fetch-backed HTTP client.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[cfg(feature = "wasm")]
impl Default for FetchHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "wasm")]
impl HttpClient<future_form::Local> for FetchHttpClient {
    type Error = FetchHttpError;

    fn post(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> futures::future::LocalBoxFuture<'_, Result<HttpResponse, Self::Error>> {
        use alloc::string::ToString;
        use futures::FutureExt;

        let url = url.to_string();
        let headers: Vec<(String, String)> = headers
            .iter()
            .map(|&(k, v)| (k.to_string(), v.to_string()))
            .collect();

        async move {
            use wasm_bindgen::JsCast;
            let opts = web_sys::RequestInit::new();
            opts.set_method("POST");
            opts.set_mode(web_sys::RequestMode::Cors);

            // Set body
            let body_array = js_sys::Uint8Array::from(body.as_slice());
            opts.set_body(&body_array);

            // Set headers
            let js_headers = web_sys::Headers::new()
                .map_err(|e| FetchHttpError(format!("Headers::new failed: {e:?}")))?;
            for (name, value) in &headers {
                js_headers
                    .set(name, value)
                    .map_err(|e| FetchHttpError(format!("header set failed: {e:?}")))?;
            }
            opts.set_headers(&js_headers);

            let request = web_sys::Request::new_with_str_and_init(&url, &opts)
                .map_err(|e| FetchHttpError(format!("Request::new failed: {e:?}")))?;

            // Call fetch — works in both Window and WorkerGlobalScope
            let promise = fetch_global(&request)
                .map_err(|e| FetchHttpError(format!("fetch failed: {e:?}")))?;

            let resp_value = wasm_bindgen_futures::JsFuture::from(promise)
                .await
                .map_err(|e| FetchHttpError(format!("fetch rejected: {e:?}")))?;

            let resp: web_sys::Response = resp_value
                .dyn_into()
                .map_err(|_| FetchHttpError("response is not a Response".into()))?;

            let status = resp.status();

            // Read response headers
            let resp_headers = resp.headers();
            let mut header_pairs = Vec::new();

            // Extract specific headers we care about (Headers iterator not
            // available in all environments, so we look up known keys).
            for key in &[crate::SESSION_ID_HEADER, "content-type"] {
                if let Ok(Some(value)) = resp_headers.get(key) {
                    header_pairs.push((key.to_lowercase(), value));
                }
            }

            // Read body as ArrayBuffer
            let body_promise = resp
                .array_buffer()
                .map_err(|e| FetchHttpError(format!("arrayBuffer() failed: {e:?}")))?;

            let body_value = wasm_bindgen_futures::JsFuture::from(body_promise)
                .await
                .map_err(|e| FetchHttpError(format!("body read failed: {e:?}")))?;

            let body_array = js_sys::Uint8Array::new(&body_value);
            let body_bytes = body_array.to_vec();

            Ok(HttpResponse {
                status,
                body: body_bytes,
                headers: header_pairs,
            })
        }
        .boxed_local()
    }
}

/// Call `fetch()` on the appropriate global scope (Window or WorkerGlobalScope).
#[cfg(feature = "wasm")]
fn fetch_global(request: &web_sys::Request) -> Result<js_sys::Promise, wasm_bindgen::JsValue> {
    use wasm_bindgen::JsCast;

    let global: js_sys::Object = js_sys::global().unchecked_into();

    // Try Window first (browser main thread)
    if let Ok(window) = global.clone().dyn_into::<web_sys::Window>() {
        return Ok(window.fetch_with_request(request));
    }

    // Fall back to WorkerGlobalScope (service worker, web worker)
    if let Ok(worker) = global.dyn_into::<web_sys::WorkerGlobalScope>() {
        return Ok(worker.fetch_with_request(request));
    }

    Err(wasm_bindgen::JsValue::from_str(
        "no global fetch available (not Window or WorkerGlobalScope)",
    ))
}
