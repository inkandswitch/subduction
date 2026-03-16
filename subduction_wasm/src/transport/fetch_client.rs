//! Browser `fetch()`-based HTTP client for the long-poll transport.
//!
//! Implements [`HttpClient<Local>`] using `web_sys::fetch`, suitable for
//! both `Window` and `WorkerGlobalScope` contexts.

use alloc::{format, string::String, vec::Vec};

use futures::FutureExt;
use subduction_http_longpoll::http_client::{HttpClient, HttpResponse};
use thiserror::Error;
use wasm_bindgen::JsCast;

/// Error type for the `web_sys::fetch`-backed HTTP client.
///
/// Variants capture the phase at which the fetch failed. Inner values are
/// stringified `JsValue`s because `JsValue` is `!Send`.
#[derive(Debug, Error)]
pub enum FetchHttpError {
    /// Failed to create the `Headers` object.
    #[error("Headers::new failed: {0}")]
    HeadersInit(String),

    /// Failed to set a header on the request.
    #[error("header set failed: {0}")]
    HeaderSet(String),

    /// Failed to construct the `Request` object.
    #[error("Request::new failed: {0}")]
    RequestInit(String),

    /// The `fetch()` call could not be dispatched (no global scope).
    #[error("fetch dispatch failed: {0}")]
    FetchDispatch(String),

    /// The `fetch()` promise was rejected.
    #[error("fetch rejected: {0}")]
    FetchRejected(String),

    /// The JS value returned by fetch was not a `Response`.
    #[error("response is not a Response object")]
    NotAResponse,

    /// Failed to start reading the response body.
    #[error("arrayBuffer() failed: {0}")]
    BodyInit(String),

    /// Failed to read the response body to completion.
    #[error("body read failed: {0}")]
    BodyRead(String),
}

/// A [`web_sys::fetch`]-backed implementation of [`HttpClient`] for browser
/// and service-worker environments.
///
/// Since `web_sys` types are `!Send`, this implements `HttpClient<Local>`.
#[derive(Debug, Clone, Copy)]
pub struct FetchHttpClient;

impl FetchHttpClient {
    /// Create a new fetch-backed HTTP client.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for FetchHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient<future_form::Local> for FetchHttpClient {
    type Error = FetchHttpError;

    fn post(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> futures::future::LocalBoxFuture<'_, Result<HttpResponse, Self::Error>> {
        use alloc::string::ToString;

        let url = url.to_string();
        let headers: Vec<(String, String)> = headers
            .iter()
            .map(|&(k, v)| (k.to_string(), v.to_string()))
            .collect();

        async move {
            let opts = web_sys::RequestInit::new();
            opts.set_method("POST");
            opts.set_mode(web_sys::RequestMode::Cors);

            // Set body
            let body_array = js_sys::Uint8Array::from(body.as_slice());
            opts.set_body(&body_array);

            // Set headers
            let js_headers = web_sys::Headers::new()
                .map_err(|e| FetchHttpError::HeadersInit(format!("{e:?}")))?;
            for (name, value) in &headers {
                js_headers
                    .set(name, value)
                    .map_err(|e| FetchHttpError::HeaderSet(format!("{e:?}")))?;
            }
            opts.set_headers(&js_headers);

            let request = web_sys::Request::new_with_str_and_init(&url, &opts)
                .map_err(|e| FetchHttpError::RequestInit(format!("{e:?}")))?;

            // Call fetch — works in both Window and WorkerGlobalScope
            let promise = fetch_global(&request)
                .map_err(|e| FetchHttpError::FetchDispatch(format!("{e:?}")))?;

            let resp_value = wasm_bindgen_futures::JsFuture::from(promise)
                .await
                .map_err(|e| FetchHttpError::FetchRejected(format!("{e:?}")))?;

            let resp: web_sys::Response = resp_value
                .dyn_into()
                .map_err(|_| FetchHttpError::NotAResponse)?;

            let status = resp.status();

            // Read response headers
            let resp_headers = resp.headers();
            let mut header_pairs = Vec::new();

            // Extract specific headers we care about (Headers iterator not
            // available in all environments, so we look up known keys).
            for key in &[subduction_http_longpoll::SESSION_ID_HEADER, "content-type"] {
                if let Ok(Some(value)) = resp_headers.get(key) {
                    header_pairs.push((key.to_lowercase(), value));
                }
            }

            // Read body as ArrayBuffer
            let body_promise = resp
                .array_buffer()
                .map_err(|e| FetchHttpError::BodyInit(format!("{e:?}")))?;

            let body_value = wasm_bindgen_futures::JsFuture::from(body_promise)
                .await
                .map_err(|e| FetchHttpError::BodyRead(format!("{e:?}")))?;

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

/// Call `fetch()` on the appropriate global scope (`Window` or `WorkerGlobalScope`).
fn fetch_global(request: &web_sys::Request) -> Result<js_sys::Promise, wasm_bindgen::JsValue> {
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
