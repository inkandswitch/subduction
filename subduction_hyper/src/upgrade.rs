//! Framework-neutral RFC 6455 upgrade over hyper.
//!
//! Three steps, each usable on its own so that any hyper-based server can
//! compose them however its handler model requires:
//!
//! 1. [`validate`] the request head and obtain an [`AcceptKey`].
//! 2. Send [`accept_response`] (the `101`) to the client.
//! 3. Once [`hyper::upgrade::OnUpgrade`] resolves, wrap the connection with
//!    [`from_upgraded`].
//!
//! The `OnUpgrade` future only resolves *after* the `101` has been written, so
//! step 3 must run on a separate task from the handler that returns step 2.
//! See [`spawn_upgrade`] for the common case.
//!
//! # Example (raw hyper)
//!
//! ```no_run
//! use http::{Request, Response};
//! use http_body_util::{Empty, Full};
//! use hyper::body::{Bytes, Incoming};
//! use subduction_hyper::upgrade::{self, Rejection};
//! use tungstenite::protocol::WebSocketConfig;
//!
//! async fn handle(mut req: Request<Incoming>) -> Result<Response<Empty<Bytes>>, Rejection> {
//!     let key = upgrade::validate(&req)?;
//!     let on_upgrade = hyper::upgrade::on(&mut req);
//!
//!     upgrade::spawn_upgrade(on_upgrade, WebSocketConfig::default(), |ws| async move {
//!         // `ws` is an `async_tungstenite::WebSocketStream`; run the Subduction
//!         // handshake and hand it to `WebSocket::new_with_keepalive`.
//!         let _ = ws;
//!     });
//!
//!     Ok(upgrade::accept_response(&key).map(|()| Empty::new()))
//! }
//! ```

use core::future::Future;

use async_tungstenite::{tokio::TokioAdapter, WebSocketStream};
use http::{
    header, HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Version,
};
use hyper::upgrade::{OnUpgrade, Upgraded};
use hyper_util::rt::TokioIo;
use tungstenite::{
    handshake::derive_accept_key,
    protocol::{Role, WebSocketConfig},
};

/// The I/O type a hyper-upgraded connection ends up on.
pub type HyperIo = TokioAdapter<TokioIo<Upgraded>>;

/// Proof that a request head passed [`validate`], carrying the
/// `Sec-WebSocket-Accept` value derived from the client's key.
///
/// Only obtainable through [`validate`], so [`accept_response`] cannot be
/// called for a request that was never checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptKey(HeaderValue);

/// Check that `req` is a well-formed HTTP/1.1 WebSocket upgrade.
///
/// Accepts anything implementing [`RequestHead`] — `&Request<B>` or
/// `&http::request::Parts` — so it works from any framework's handler.
///
/// # Errors
///
/// Returns a [`Rejection`] naming the first failed check, in the order:
/// HTTP version, method, `Connection`, `Upgrade`, `Sec-WebSocket-Version`,
/// `Sec-WebSocket-Key`.
pub fn validate<R: RequestHead + ?Sized>(req: &R) -> Result<AcceptKey, Rejection> {
    if req.version() > Version::HTTP_11 {
        return Err(Rejection::HttpVersion);
    }

    if req.method() != Method::GET {
        return Err(Rejection::MethodNotGet);
    }

    let headers = req.headers();

    if !header_contains_token(headers, header::CONNECTION, "upgrade") {
        return Err(Rejection::ConnectionNotUpgrade);
    }

    if !header_eq(headers, header::UPGRADE, "websocket") {
        return Err(Rejection::UpgradeNotWebSocket);
    }

    if !header_eq(headers, header::SEC_WEBSOCKET_VERSION, "13") {
        return Err(Rejection::Version);
    }

    let key = headers
        .get(header::SEC_WEBSOCKET_KEY)
        .ok_or(Rejection::MissingKey)?;

    let accept = derive_accept_key(key.as_bytes());
    HeaderValue::from_str(&accept)
        .map(AcceptKey)
        .map_err(|_| Rejection::MissingKey)
}

/// The `101 Switching Protocols` response for a validated upgrade.
///
/// The body is `()`; map it to whatever body type your server wants, e.g.
/// `.map(|()| Empty::new())` for hyper or `.map(|()| Body::empty())` for axum.
#[must_use]
pub fn accept_response(key: &AcceptKey) -> Response<()> {
    let mut response = Response::new(());
    *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;

    let headers = response.headers_mut();
    headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(header::SEC_WEBSOCKET_ACCEPT, key.0.clone());

    response
}

/// Wrap a hyper-upgraded connection as a server-side WebSocket stream.
///
/// `upgraded` must be the result of awaiting [`OnUpgrade`] *after* the `101`
/// from [`accept_response`] was sent; the stream starts framing from the first
/// byte after the HTTP exchange (including anything hyper had already buffered).
pub async fn from_upgraded(
    upgraded: Upgraded,
    config: WebSocketConfig,
) -> WebSocketStream<HyperIo> {
    let io = TokioAdapter::new(TokioIo::new(upgraded));
    WebSocketStream::from_raw_socket(io, Role::Server, Some(config)).await
}

/// Spawn a task that waits for `on_upgrade`, wraps the connection with
/// [`from_upgraded`], and runs `f` on the result.
///
/// Call this *before* returning the `101`; the task blocks until hyper has
/// written it. If hyper fails to hand over the connection (client vanished
/// between request and response), the failure is logged and `f` never runs.
pub fn spawn_upgrade<F, Fut>(on_upgrade: OnUpgrade, config: WebSocketConfig, f: F)
where
    F: FnOnce(WebSocketStream<HyperIo>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => f(from_upgraded(upgraded, config).await).await,
            Err(e) => tracing::warn!(error = %e, "WebSocket upgrade failed after 101"),
        }
    });
}

/// Why a request could not be upgraded to a WebSocket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Rejection {
    /// `Connection` header does not list `upgrade`.
    #[error("`Connection` header must include `upgrade`")]
    ConnectionNotUpgrade,

    /// Only HTTP/1.x `Upgrade:` is supported; RFC 8441 (HTTP/2) is not.
    #[error("WebSocket upgrade requires HTTP/1.1")]
    HttpVersion,

    /// WebSocket upgrades must be `GET`.
    #[error("request method must be GET")]
    MethodNotGet,

    /// `Sec-WebSocket-Key` header absent or malformed.
    #[error("`Sec-WebSocket-Key` header missing or invalid")]
    MissingKey,

    /// hyper did not mark the connection as upgradable.
    #[error("connection is not upgradable")]
    NotUpgradable,

    /// `Upgrade` header is not `websocket`.
    #[error("`Upgrade` header must be `websocket`")]
    UpgradeNotWebSocket,

    /// `Sec-WebSocket-Version` is not `13`.
    #[error("`Sec-WebSocket-Version` must be `13`")]
    Version,
}

impl Rejection {
    /// HTTP status this rejection maps to.
    #[must_use]
    pub const fn status(self) -> StatusCode {
        match self {
            Self::ConnectionNotUpgrade
            | Self::HttpVersion
            | Self::MissingKey
            | Self::UpgradeNotWebSocket => StatusCode::BAD_REQUEST,
            Self::MethodNotGet => StatusCode::METHOD_NOT_ALLOWED,
            Self::NotUpgradable => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Version => StatusCode::UPGRADE_REQUIRED,
        }
    }

    /// A plain-text error response for this rejection.
    ///
    /// For [`Rejection::Version`] this includes `Sec-WebSocket-Version: 13`
    /// as RFC 6455 §4.2.2 requires.
    #[must_use]
    pub fn response(self) -> Response<String> {
        let mut response = Response::new(self.to_string());
        *response.status_mut() = self.status();

        if self == Self::Version {
            response.headers_mut().insert(
                header::SEC_WEBSOCKET_VERSION,
                HeaderValue::from_static("13"),
            );
        }

        response
    }
}

/// The parts of a request [`validate`] inspects. Implemented for
/// [`http::Request`] and [`http::request::Parts`].
pub trait RequestHead {
    /// HTTP method.
    fn method(&self) -> &Method;

    /// HTTP version.
    fn version(&self) -> Version;

    /// Request headers.
    fn headers(&self) -> &HeaderMap;
}

impl<B> RequestHead for Request<B> {
    fn method(&self) -> &Method {
        Request::method(self)
    }

    fn version(&self) -> Version {
        Request::version(self)
    }

    fn headers(&self) -> &HeaderMap {
        Request::headers(self)
    }
}

impl RequestHead for http::request::Parts {
    fn method(&self) -> &Method {
        &self.method
    }

    fn version(&self) -> Version {
        self.version
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

fn header_eq(headers: &HeaderMap, name: HeaderName, value: &str) -> bool {
    headers
        .get(name)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(value.as_bytes()))
}

/// `Connection` is a comma-separated token list (e.g. `keep-alive, Upgrade`).
fn header_contains_token(headers: &HeaderMap, name: HeaderName, token: &str) -> bool {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case(token)))
}

#[cfg(test)]
#[allow(clippy::expect_used, reason = "test-only assertions")]
mod tests {
    use super::*;

    /// RFC 6455 §1.3 worked example.
    const RFC_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";
    const RFC_ACCEPT: &str = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

    fn upgrade_request() -> Request<()> {
        Request::builder()
            .method(Method::GET)
            .uri("/ws")
            .header(header::CONNECTION, "keep-alive, Upgrade")
            .header(header::UPGRADE, "WebSocket")
            .header(header::SEC_WEBSOCKET_VERSION, "13")
            .header(header::SEC_WEBSOCKET_KEY, RFC_KEY)
            .body(())
            .unwrap_or_else(|_| unreachable!("static request is valid"))
    }

    fn with<F: FnOnce(&mut Request<()>)>(f: F) -> Request<()> {
        let mut req = upgrade_request();
        f(&mut req);
        req
    }

    #[test]
    fn accepts_rfc_example_and_derives_documented_key() {
        let key = validate(&upgrade_request()).expect("RFC example is valid");
        assert_eq!(key.0, HeaderValue::from_static(RFC_ACCEPT));

        let resp = accept_response(&key);
        assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            resp.headers().get(header::SEC_WEBSOCKET_ACCEPT),
            Some(&HeaderValue::from_static(RFC_ACCEPT))
        );
        assert_eq!(
            resp.headers().get(header::UPGRADE),
            Some(&HeaderValue::from_static("websocket"))
        );
    }

    #[test]
    fn validate_accepts_parts_too() {
        let (parts, ()) = upgrade_request().into_parts();
        assert!(validate(&parts).is_ok());
    }

    #[test]
    fn rejects_non_get() {
        let req = with(|r| *r.method_mut() = Method::POST);
        assert_eq!(validate(&req).err(), Some(Rejection::MethodNotGet));
    }

    #[test]
    fn rejects_http2() {
        let req = with(|r| *r.version_mut() = Version::HTTP_2);
        assert_eq!(validate(&req).err(), Some(Rejection::HttpVersion));
    }

    #[test]
    fn rejects_missing_connection_upgrade() {
        let req = with(|r| {
            r.headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
        });
        assert_eq!(validate(&req).err(), Some(Rejection::ConnectionNotUpgrade));
    }

    #[test]
    fn rejects_wrong_upgrade_target() {
        let req = with(|r| {
            r.headers_mut()
                .insert(header::UPGRADE, HeaderValue::from_static("h2c"));
        });
        assert_eq!(validate(&req).err(), Some(Rejection::UpgradeNotWebSocket));
    }

    #[test]
    fn rejects_wrong_version() {
        let req = with(|r| {
            r.headers_mut()
                .insert(header::SEC_WEBSOCKET_VERSION, HeaderValue::from_static("8"));
        });
        assert_eq!(validate(&req).err(), Some(Rejection::Version));
    }

    #[test]
    fn rejects_missing_key() {
        let req = with(|r| {
            r.headers_mut().remove(header::SEC_WEBSOCKET_KEY);
        });
        assert_eq!(validate(&req).err(), Some(Rejection::MissingKey));
    }

    #[test]
    fn version_rejection_advertises_supported_version() {
        let resp = Rejection::Version.response();
        assert_eq!(resp.status(), StatusCode::UPGRADE_REQUIRED);
        assert_eq!(
            resp.headers().get(header::SEC_WEBSOCKET_VERSION),
            Some(&HeaderValue::from_static("13"))
        );
    }

    #[test]
    fn connection_token_matching_is_case_and_whitespace_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONNECTION,
            HeaderValue::from_static("Keep-Alive ,UPGRADE"),
        );
        assert!(header_contains_token(
            &headers,
            header::CONNECTION,
            "upgrade"
        ));

        headers.insert(
            header::CONNECTION,
            HeaderValue::from_static("upgrade-insecure"),
        );
        assert!(!header_contains_token(
            &headers,
            header::CONNECTION,
            "upgrade"
        ));
    }
}
