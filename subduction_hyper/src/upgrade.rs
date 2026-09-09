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
//! [`upgrade`] is the awaitable primitive; [`spawn_upgrade`] wraps it in
//! `tokio::spawn` for the common case.
//!
//! # Example (raw hyper)
//!
//! A hyper service must return `Ok(response)` for a rejection — returning
//! `Err` makes hyper drop the connection without sending anything.
//!
//! ```no_run
//! use std::convert::Infallible;
//!
//! use http::{Request, Response};
//! use http_body_util::Full;
//! use hyper::body::{Bytes, Incoming};
//! use subduction_hyper::upgrade;
//! use tungstenite::protocol::WebSocketConfig;
//!
//! async fn handle(mut req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
//!     let key = match upgrade::validate(&req) {
//!         Ok(key) => key,
//!         Err(rejection) => return Ok(rejection.response().map(|s| Full::new(Bytes::from(s)))),
//!     };
//!     let on_upgrade = hyper::upgrade::on(&mut req);
//!
//!     upgrade::spawn_upgrade(on_upgrade, WebSocketConfig::default(), |ws| async move {
//!         // `ws` is an `async_tungstenite::WebSocketStream`; run the Subduction
//!         // handshake and hand it to `WebSocket::new_with_keepalive`.
//!         let _ = ws;
//!     });
//!
//!     Ok(upgrade::accept_response(&key).map(|()| Full::new(Bytes::new())))
//! }
//! ```
//!
//! The hyper connection must be served with upgrades enabled:
//! `http1::Builder::serve_connection(..).with_upgrades()`, or
//! `auto::Builder::serve_connection_with_upgrades(..)` from `hyper-util`.
//!
//! # Limitations
//!
//! - HTTP/1.1 only. RFC 8441 (WebSocket over HTTP/2 extended `CONNECT`) is
//!   rejected with [`Rejection::HttpVersion`].
//! - No `Sec-WebSocket-Protocol` negotiation. Subduction clients do not offer a
//!   subprotocol; a client that does will fail its own handshake when the `101`
//!   omits the header.
//! - No `Origin` policy. Enforce it in middleware if browsers can reach the
//!   endpoint (the Subduction handshake authenticates peers, but does not stop a
//!   hostile page from opening a connection).

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
    // Exactly 1.1: RFC 6455 §4.1 requires at least 1.1, and hyper only
    // populates `OnUpgrade` for 1.1, so a 1.0 request could never complete.
    if req.version() != Version::HTTP_11 {
        return Err(Rejection::HttpVersion);
    }

    if req.method() != Method::GET {
        return Err(Rejection::MethodNotGet);
    }

    let headers = req.headers();

    if !header_contains_token(headers, header::CONNECTION, "upgrade") {
        return Err(Rejection::ConnectionNotUpgrade);
    }

    if !header_contains_token(headers, header::UPGRADE, "websocket") {
        return Err(Rejection::UpgradeNotWebSocket);
    }

    if !header_eq(headers, header::SEC_WEBSOCKET_VERSION, "13") {
        return Err(Rejection::Version);
    }

    let key = headers
        .get(header::SEC_WEBSOCKET_KEY)
        .ok_or(Rejection::MissingKey)?;

    let accept = HeaderValue::from_str(&derive_accept_key(key.as_bytes()))
        .unwrap_or_else(|_| unreachable!("base64 of a SHA-1 digest is visible ASCII"));

    Ok(AcceptKey(accept))
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

/// Wait for hyper to hand over the connection, then wrap it as a WebSocket.
///
/// This only resolves after the `101` has been written, so it must not be
/// awaited inside the handler that returns that response — drive it from a
/// separate task. Use this rather than [`spawn_upgrade`] when you want to
/// choose the spawner (e.g. a `TaskTracker` for graceful shutdown).
///
/// # Errors
///
/// Returns hyper's error if the connection went away before the upgrade
/// completed (typically the client disconnected between request and `101`).
pub async fn upgrade(
    on_upgrade: OnUpgrade,
    config: WebSocketConfig,
) -> Result<WebSocketStream<HyperIo>, hyper::Error> {
    let upgraded = on_upgrade.await?;
    Ok(from_upgraded(upgraded, config).await)
}

/// [`upgrade`] on a `tokio::spawn`ed task, running `f` on success.
///
/// Call this *before* returning the `101`. A client that vanishes before the
/// upgrade completes is logged at `debug` and `f` never runs. The returned
/// handle can be awaited or aborted; dropping it detaches the task.
///
/// # Panics
///
/// Panics if called outside a tokio runtime.
pub fn spawn_upgrade<F, Fut>(
    on_upgrade: OnUpgrade,
    config: WebSocketConfig,
    f: F,
) -> tokio::task::JoinHandle<()>
where
    F: FnOnce(WebSocketStream<HyperIo>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        match upgrade(on_upgrade, config).await {
            Ok(ws) => f(ws).await,
            Err(e) => tracing::debug!(error = %e, "client left before WebSocket upgrade completed"),
        }
    })
}

/// Why a request could not be upgraded to a WebSocket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Rejection {
    /// `Connection` header does not list `upgrade`.
    #[error("`Connection` header must include `upgrade`")]
    ConnectionNotUpgrade,

    /// Not HTTP/1.1. HTTP/1.0 cannot upgrade; RFC 8441 (HTTP/2) is unsupported.
    #[error("WebSocket upgrade requires HTTP/1.1")]
    HttpVersion,

    /// WebSocket upgrades must be `GET`.
    #[error("request method must be GET")]
    MethodNotGet,

    /// `Sec-WebSocket-Key` header absent.
    #[error("`Sec-WebSocket-Key` header missing")]
    MissingKey,

    /// hyper did not mark the connection as upgradable. On a validated
    /// HTTP/1.1 request this means the server was not built with upgrades
    /// enabled (see the module docs), hence `500` rather than a client error.
    #[error("connection is not upgradable")]
    NotUpgradable,

    /// `Upgrade` header does not list `websocket`.
    #[error("`Upgrade` header must include `websocket`")]
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
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );

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

/// `Connection` and `Upgrade` are comma-separated token lists that may also
/// be split across repeated header lines (RFC 7230 §3.2.2), e.g.
/// `Connection: keep-alive, Upgrade` or two separate `Connection:` lines.
fn header_contains_token(headers: &HeaderMap, name: HeaderName, token: &str) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|t| t.trim().eq_ignore_ascii_case(token))
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

    /// hyper never populates `OnUpgrade` for HTTP/1.0, so accepting it here
    /// would surface as a misleading `NotUpgradable` (500) downstream.
    #[test]
    fn rejects_http10() {
        let req = with(|r| *r.version_mut() = Version::HTTP_10);
        assert_eq!(validate(&req).err(), Some(Rejection::HttpVersion));
    }

    /// RFC 7230 §3.2.2: list headers may be split across repeated lines.
    #[test]
    fn accepts_connection_tokens_split_across_header_lines() {
        let req = with(|r| {
            let headers = r.headers_mut();
            headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
            headers.append(header::CONNECTION, HeaderValue::from_static("Upgrade"));
        });
        assert!(validate(&req).is_ok());
    }

    /// `Upgrade` is a token list too (RFC 6455 §4.1: "MUST include").
    #[test]
    fn accepts_upgrade_token_list_containing_websocket() {
        let req = with(|r| {
            r.headers_mut()
                .insert(header::UPGRADE, HeaderValue::from_static("h2c, websocket"));
        });
        assert!(validate(&req).is_ok());
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
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("text/plain; charset=utf-8"))
        );
    }

    /// Token matching is invariant under case, surrounding whitespace, list
    /// position, and splitting across header lines; and a token that merely
    /// *contains* the target (`upgrade-insecure`) never matches.
    #[test]
    #[allow(clippy::indexing_slicing, reason = "indices are reduced modulo len")]
    fn token_matching_properties() {
        const DECOYS: &[&str] = &["keep-alive", "close", "upgrade-insecure", "h2c", "x"];
        const PADS: &[&str] = &["", " ", "\t", "  "];

        // (decoy indices before, decoy indices after, case mask, split lines?, pad index)
        type Case = (Vec<u8>, Vec<u8>, u8, bool, u8);

        bolero::check!()
            .with_type::<Case>()
            .for_each(|(before, after, mask, split, pad_ix)| {
                let decoy = |i: &u8| DECOYS[usize::from(*i) % DECOYS.len()].to_owned();
                let pad = PADS[usize::from(*pad_ix) % PADS.len()];
                let target: String = "upgrade"
                    .chars()
                    .enumerate()
                    .map(|(i, c)| {
                        if (mask >> (i % 8)) & 1 == 1 {
                            c.to_ascii_uppercase()
                        } else {
                            c
                        }
                    })
                    .collect();

                let push = |headers: &mut HeaderMap, tokens: &[String]| {
                    if tokens.is_empty() {
                        return;
                    }
                    let line = tokens.join(&format!(",{pad}"));
                    headers.append(
                        header::CONNECTION,
                        HeaderValue::from_str(&line).expect("tokens are ASCII"),
                    );
                };

                let befores: Vec<String> = before.iter().take(3).map(decoy).collect();
                let afters: Vec<String> = after.iter().take(3).map(decoy).collect();
                let padded = vec![format!("{pad}{target}{pad}")];

                // Decoys alone never match.
                let mut headers = HeaderMap::new();
                push(&mut headers, &[befores.clone(), afters.clone()].concat());
                assert!(!header_contains_token(
                    &headers,
                    header::CONNECTION,
                    "upgrade"
                ));

                // The target anywhere, in any case, padded, possibly on its own
                // line, always matches.
                headers.clear();
                if *split {
                    push(&mut headers, &befores);
                    push(&mut headers, &padded);
                    push(&mut headers, &afters);
                } else {
                    push(&mut headers, &[befores, padded, afters].concat());
                }
                assert!(header_contains_token(
                    &headers,
                    header::CONNECTION,
                    "upgrade"
                ));
            });
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
