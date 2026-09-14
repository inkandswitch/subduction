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
//! [`upgrade`] is the awaitable primitive; [`spawn_upgrade`] runs it on a
//! caller-supplied [`Spawn`] for the common case.
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
//! use subduction_websocket::tokio::TokioSpawn;
//! use tungstenite::protocol::WebSocketConfig;
//!
//! async fn handle(
//!     mut req: Request<Incoming>,
//!     spawner: TokioSpawn,
//! ) -> Result<Response<Full<Bytes>>, Infallible> {
//!     let key = match upgrade::validate(&req) {
//!         Ok(key) => key,
//!         Err(rejection) => return Ok(rejection.response().map(|s| Full::new(Bytes::from(s)))),
//!     };
//!     let on_upgrade = hyper::upgrade::on(&mut req);
//!
//!     upgrade::spawn_upgrade(&spawner, on_upgrade, WebSocketConfig::default(), |ws| async move {
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
//! - HTTP/1.1 only. RFC 8441 (WebSocket over HTTP/2) is a different handshake
//!   — `CONNECT` carrying `:protocol`, no `Sec-WebSocket-Key`, a `200` rather
//!   than a `101` — so it is not implemented here. hyper accepts extended
//!   `CONNECT` only when the server opts in with `enable_connect_protocol`,
//!   which this crate never does, so a compliant client will not attempt it;
//!   anything that arrives regardless is refused with
//!   [`Rejection::HttpVersion`].
//! - No `Sec-WebSocket-Protocol` negotiation. Subduction clients do not offer a
//!   subprotocol; a client that does will fail its own handshake when the `101`
//!   omits the header.
//! - No `Origin` policy. Enforce it in middleware if browsers can reach the
//!   endpoint (the Subduction handshake authenticates peers, but does not stop a
//!   hostile page from opening a connection).

use core::future::Future;

use async_tungstenite::{WebSocketStream, tokio::TokioAdapter};
use future_form::Sendable;
use futures::stream::AbortHandle;
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Version, header,
};
use hyper::upgrade::{OnUpgrade, Upgraded};
use hyper_util::rt::TokioIo;
use subduction_core::spawn::Spawn;
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
/// Accepts anything implementing [`RequestHead`], which covers the `http`
/// types every hyper-based framework exposes: `&Request<B>` and
/// `&http::request::Parts`.
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

    let mut keys = headers.get_all(header::SEC_WEBSOCKET_KEY).iter();
    let key = keys.next().ok_or(Rejection::MissingKey)?;

    // RFC 6455 §4.2.1 allows exactly one. Accepting a repeat would let a proxy
    // that prefers the last value disagree with us about which nonce the `101`
    // answered.
    if keys.next().is_some() || !is_websocket_key(key.as_bytes()) {
        return Err(Rejection::MalformedKey);
    }

    // base64 of a SHA-1 digest is visible ASCII, so this cannot fail; going
    // through the fallible constructor keeps `validate` panic-free.
    let accept = HeaderValue::from_str(&derive_accept_key(key.as_bytes()))
        .map_err(|_| Rejection::MalformedKey)?;

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

/// [`upgrade`] on a task from `spawner`, running `f` on success.
///
/// Call this *before* returning the `101`. Pass the spawner the node was built
/// with, so upgrade tasks share its lifecycle rather than detaching.
///
/// Hyper hands the connection over when the *connection* ends, not when the
/// `101` is written. So the task lives as long as the connection, and if an
/// outer layer replaced the `101`, `f` still runs at teardown over a stream
/// that never spoke WebSocket. Keep `f` cheap until its handshake succeeds.
///
/// Nothing bounds the wait: a client that never reads holds the task open.
/// Await [`upgrade`] under a timeout if that matters. Dropping the returned
/// [`AbortHandle`] detaches the task; the upgrade error is logged, not
/// returned.
pub fn spawn_upgrade<Sp, F, Fut>(
    spawner: &Sp,
    on_upgrade: OnUpgrade,
    config: WebSocketConfig,
    f: F,
) -> AbortHandle
where
    Sp: Spawn<Sendable>,
    F: FnOnce(WebSocketStream<HyperIo>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    spawner.spawn(Box::pin(async move {
        match upgrade(on_upgrade, config).await {
            Ok(ws) => f(ws).await,
            // `is_user` marks the server's own misuse: no `.with_upgrades()`,
            // or an `OnUpgrade` that was never armed. Everything else is the
            // client going away, which is routine.
            Err(e) if e.is_user() => {
                tracing::warn!(error = %e, "connection was not upgradable; is the server built with upgrades enabled?");
            }
            Err(e) => {
                tracing::debug!(error = %e, "connection closed before the WebSocket upgrade completed");
            }
        }
    }))
}

/// The parts of a request [`validate`] inspects. Implemented for
/// [`http::Request`] and [`http::request::Parts`].
///
/// Sealed: [`validate`] rejects anything but HTTP/1.1 because hyper arms
/// [`OnUpgrade`] for nothing else, and an outside implementation could report
/// a version the request does not have.
pub trait RequestHead: sealed::Sealed {
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

/// RFC 6455 §4.1: base64 of a 16-byte nonce, so 24 characters ending `==`.
///
/// Checked without decoding: only that length and alphabet can encode 16 bytes.
fn is_websocket_key(key: &[u8]) -> bool {
    key.len() == 24
        && key.ends_with(b"==")
        && key
            .iter()
            .take(22)
            .all(|b| b.is_ascii_alphanumeric() || *b == b'+' || *b == b'/')
}

mod sealed {
    pub trait Sealed {}

    impl<B> Sealed for http::Request<B> {}

    impl Sealed for http::request::Parts {}
}

/// Why a request could not be upgraded to a WebSocket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Rejection {
    /// `400`. `Connection` header does not list `upgrade`.
    #[error("`Connection` header must include `upgrade`")]
    ConnectionNotUpgrade,

    /// `400`. Not HTTP/1.1: 1.0 cannot upgrade at all, and RFC 8441 (WebSocket
    /// over HTTP/2) is a separate handshake this crate does not implement.
    #[error("WebSocket upgrade requires HTTP/1.1")]
    HttpVersion,

    /// `400`. `Sec-WebSocket-Key` is not base64 of 16 bytes, or was sent twice.
    #[error("`Sec-WebSocket-Key` must be a single base64 16-byte nonce")]
    MalformedKey,

    /// `405`. RFC 6455 §4.1 requires `GET`.
    #[error("request method must be GET")]
    MethodNotGet,

    /// `400`. `Sec-WebSocket-Key` header absent.
    #[error("`Sec-WebSocket-Key` header missing")]
    MissingKey,

    /// `500`. hyper did not mark the connection as upgradable. On a validated
    /// HTTP/1.1 request that is the server's own doing — it was not built with
    /// upgrades enabled (see the module docs), or an earlier extractor already
    /// took the [`OnUpgrade`] — so it is not the client's error to fix.
    #[error("connection is not upgradable")]
    NotUpgradable,

    /// `400`. `Upgrade` header does not list `websocket`.
    #[error("`Upgrade` header must include `websocket`")]
    UpgradeNotWebSocket,

    /// `426`. `Sec-WebSocket-Version` is not `13`.
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
            | Self::MalformedKey
            | Self::MissingKey
            | Self::UpgradeNotWebSocket => StatusCode::BAD_REQUEST,
            Self::MethodNotGet => StatusCode::METHOD_NOT_ALLOWED,
            Self::NotUpgradable => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Version => StatusCode::UPGRADE_REQUIRED,
        }
    }

    /// A plain-text error response for this rejection.
    ///
    /// Carries the header each status is required to advertise:
    /// `Sec-WebSocket-Version: 13` for [`Rejection::Version`] (RFC 6455
    /// §4.2.2) and `Allow: GET` for [`Rejection::MethodNotGet`] (RFC 9110
    /// §15.5.6).
    #[must_use]
    pub fn response(self) -> Response<String> {
        let mut response = Response::new(self.to_string());
        *response.status_mut() = self.status();

        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );

        match self {
            Self::Version => {
                headers.insert(
                    header::SEC_WEBSOCKET_VERSION,
                    HeaderValue::from_static("13"),
                );
            }
            Self::MethodNotGet => {
                headers.insert(header::ALLOW, HeaderValue::from_static("GET"));
            }
            Self::ConnectionNotUpgrade
            | Self::HttpVersion
            | Self::MalformedKey
            | Self::MissingKey
            | Self::NotUpgradable
            | Self::UpgradeNotWebSocket => {}
        }

        response
    }
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

    /// Every variant, so tests over "all rejections" fail to compile rather
    /// than silently skip a new one.
    const ALL_REJECTIONS: [Rejection; 8] = [
        Rejection::ConnectionNotUpgrade,
        Rejection::HttpVersion,
        Rejection::MalformedKey,
        Rejection::MethodNotGet,
        Rejection::MissingKey,
        Rejection::NotUpgradable,
        Rejection::UpgradeNotWebSocket,
        Rejection::Version,
    ];

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

    /// RFC 6455 §4.1: the key is base64 of 16 bytes. Anything else would be
    /// echoed as a digest the client cannot check.
    #[test]
    fn rejects_keys_that_are_not_base64_16_bytes() {
        for bad in ["", "AAAA", "not-base64!!", &"A".repeat(24), &RFC_KEY[1..]] {
            let req = with(|r| {
                r.headers_mut().insert(
                    header::SEC_WEBSOCKET_KEY,
                    HeaderValue::from_str(bad).expect("header value"),
                );
            });
            assert_eq!(
                validate(&req).err(),
                Some(Rejection::MalformedKey),
                "key {bad:?} should be rejected"
            );
        }
    }

    /// A key is accepted exactly when RFC 6455 §4.1 says it encodes 16 bytes:
    /// one header line, 24 characters from the base64 alphabet, `==` tail.
    /// Generated near-misses (23 or 25 characters, `=` in the middle, an
    /// out-of-alphabet byte) are the cases a hand-written list misses.
    #[test]
    #[cfg(feature = "bolero")]
    #[allow(clippy::indexing_slicing, reason = "indices are reduced modulo len")]
    fn prop_key_accepted_iff_base64_16_bytes() {
        // Base64 alphabet plus characters that are legal in a header value but
        // not in base64, so both sides of the predicate get exercised.
        const POOL: &[u8] = b"ABCXYZabcxyz019+/=!-_ ";

        bolero::check!()
            .with_type::<(Vec<u8>, bool)>()
            .for_each(|(bytes, duplicate)| {
                let key: Vec<u8> = bytes
                    .iter()
                    .take(30)
                    .map(|b| POOL[usize::from(*b) % POOL.len()])
                    .collect();
                let Ok(value) = HeaderValue::from_bytes(&key) else {
                    return;
                };

                let req = with(|r| {
                    let headers = r.headers_mut();
                    headers.insert(header::SEC_WEBSOCKET_KEY, value.clone());
                    if *duplicate {
                        headers.append(header::SEC_WEBSOCKET_KEY, value.clone());
                    }
                });

                let well_formed = key.len() == 24
                    && key.ends_with(b"==")
                    && key[..22]
                        .iter()
                        .all(|b| b.is_ascii_alphanumeric() || *b == b'+' || *b == b'/');

                match validate(&req) {
                    Ok(_) => assert!(
                        well_formed && !*duplicate,
                        "accepted a key it should not have: {:?}",
                        String::from_utf8_lossy(&key)
                    ),
                    Err(e) => {
                        assert!(
                            !well_formed || *duplicate,
                            "rejected a well-formed key: {:?}",
                            String::from_utf8_lossy(&key)
                        );
                        assert_eq!(e, Rejection::MalformedKey);
                    }
                }
            });
    }

    /// `validate` documents the order it checks in. With several defects at
    /// once the *first* one in that order is what the client is told, which
    /// single-defect tests cannot pin.
    #[test]
    #[cfg(feature = "bolero")]
    fn prop_first_defect_in_documented_order_is_reported() {
        // (version, method, connection, upgrade, ws-version, key) — in the
        // order `validate` checks them.
        type Defects = (bool, bool, bool, bool, bool, bool);

        bolero::check!().with_type::<Defects>().for_each(
            |(bad_version, bad_method, bad_connection, bad_upgrade, bad_ws_version, no_key)| {
                let req = with(|r| {
                    if *bad_version {
                        *r.version_mut() = Version::HTTP_10;
                    }

                    if *bad_method {
                        *r.method_mut() = Method::POST;
                    }

                    let headers = r.headers_mut();
                    if *bad_connection {
                        headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
                    }

                    if *bad_upgrade {
                        headers.insert(header::UPGRADE, HeaderValue::from_static("h2c"));
                    }

                    if *bad_ws_version {
                        headers
                            .insert(header::SEC_WEBSOCKET_VERSION, HeaderValue::from_static("8"));
                    }

                    if *no_key {
                        headers.remove(header::SEC_WEBSOCKET_KEY);
                    }
                });

                let expected = [
                    (*bad_version, Rejection::HttpVersion),
                    (*bad_method, Rejection::MethodNotGet),
                    (*bad_connection, Rejection::ConnectionNotUpgrade),
                    (*bad_upgrade, Rejection::UpgradeNotWebSocket),
                    (*bad_ws_version, Rejection::Version),
                    (*no_key, Rejection::MissingKey),
                ]
                .into_iter()
                .find_map(|(present, rejection)| present.then_some(rejection));

                assert_eq!(validate(&req).err(), expected);
            },
        );
    }

    /// The accept value is `base64(sha1(key + GUID))` from RFC 6455 §4.2.2,
    /// derived here independently of `tungstenite::derive_accept_key` so the
    /// whole chain is pinned by more than the single RFC vector above.
    #[test]
    #[cfg(feature = "bolero")]
    fn prop_accept_key_is_the_rfc_derivation() {
        use base64::Engine as _;
        use sha1::{Digest, Sha1};

        const GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

        bolero::check!().with_type::<[u8; 16]>().for_each(|nonce| {
            let key = base64::engine::general_purpose::STANDARD.encode(nonce);
            let req = with(|r| {
                r.headers_mut().insert(
                    header::SEC_WEBSOCKET_KEY,
                    HeaderValue::from_str(&key).expect("base64 is a valid header value"),
                );
            });

            let mut hasher = Sha1::new();
            hasher.update(key.as_bytes());
            hasher.update(GUID);
            let expected = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());

            let accepted = validate(&req).expect("a base64 16-byte nonce is well formed");
            assert_eq!(accepted.0, HeaderValue::from_str(&expected).expect("ascii"));
        });
    }

    /// RFC 6455 §4.2.1 allows exactly one. Two lines let a proxy and this
    /// server disagree about which nonce the `101` answered.
    #[test]
    fn rejects_repeated_key_header() {
        let req = with(|r| {
            r.headers_mut().append(
                header::SEC_WEBSOCKET_KEY,
                HeaderValue::from_static("AQIDBAUGBwgJCgsMDQ4PEA=="),
            );
        });
        assert_eq!(validate(&req).err(), Some(Rejection::MalformedKey));
    }

    /// Every variant's status, so a re-mapping cannot pass unnoticed.
    #[test]
    fn rejection_statuses_are_pinned() {
        for (rejection, expected) in [
            (Rejection::ConnectionNotUpgrade, StatusCode::BAD_REQUEST),
            (Rejection::HttpVersion, StatusCode::BAD_REQUEST),
            (Rejection::MalformedKey, StatusCode::BAD_REQUEST),
            (Rejection::MethodNotGet, StatusCode::METHOD_NOT_ALLOWED),
            (Rejection::MissingKey, StatusCode::BAD_REQUEST),
            (Rejection::NotUpgradable, StatusCode::INTERNAL_SERVER_ERROR),
            (Rejection::UpgradeNotWebSocket, StatusCode::BAD_REQUEST),
            (Rejection::Version, StatusCode::UPGRADE_REQUIRED),
        ] {
            assert_eq!(rejection.status(), expected, "{rejection:?}");
            assert_eq!(rejection.response().status(), expected, "{rejection:?}");
        }
    }

    /// Every rejection response carries a content type, and the two statuses
    /// that are required to advertise something do — `Allow: GET` for `405`
    /// (RFC 9110 §15.5.6) and `Sec-WebSocket-Version: 13` for `426` (RFC 6455
    /// §4.2.2) — while no other variant carries either header.
    #[test]
    fn rejection_responses_advertise_exactly_what_their_status_requires() {
        for rejection in ALL_REJECTIONS {
            let resp = rejection.response();

            assert_eq!(
                resp.headers().get(header::CONTENT_TYPE),
                Some(&HeaderValue::from_static("text/plain; charset=utf-8")),
                "{rejection:?}"
            );

            let allow = resp.headers().get(header::ALLOW);
            if rejection == Rejection::MethodNotGet {
                assert_eq!(allow, Some(&HeaderValue::from_static("GET")));
            } else {
                assert_eq!(allow, None, "{rejection:?} should not advertise Allow");
            }

            let ws_version = resp.headers().get(header::SEC_WEBSOCKET_VERSION);
            if rejection == Rejection::Version {
                assert_eq!(ws_version, Some(&HeaderValue::from_static("13")));
            } else {
                assert_eq!(
                    ws_version, None,
                    "{rejection:?} should not advertise a version"
                );
            }
        }
    }

    /// Token matching is invariant under case, surrounding whitespace, list
    /// position, and splitting across header lines; and a token that merely
    /// *contains* the target (`upgrade-insecure`, `websocket-x`) never matches.
    /// Checked for both list headers `validate` reads.
    #[test]
    #[cfg(feature = "bolero")]
    #[allow(clippy::indexing_slicing, reason = "indices are reduced modulo len")]
    fn token_matching_properties() {
        const DECOYS: &[&str] = &[
            "keep-alive",
            "close",
            "upgrade-insecure",
            "websocket-x",
            "h2c",
            "x",
        ];
        const PADS: &[&str] = &["", " ", "\t", "  "];

        // (decoy indices before, decoy indices after, case mask, split lines?,
        //  pad index, which header)
        type Case = (Vec<u8>, Vec<u8>, u8, bool, u8, bool);

        bolero::check!().with_type::<Case>().for_each(
            |(before, after, mask, split, pad_ix, use_upgrade)| {
                let (name, token) = if *use_upgrade {
                    (header::UPGRADE, "websocket")
                } else {
                    (header::CONNECTION, "upgrade")
                };

                let decoy = |i: &u8| DECOYS[usize::from(*i) % DECOYS.len()].to_owned();
                let pad = PADS[usize::from(*pad_ix) % PADS.len()];
                let target: String = token
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
                        name.clone(),
                        HeaderValue::from_str(&line).expect("tokens are ASCII"),
                    );
                };

                let befores: Vec<String> = before.iter().take(3).map(decoy).collect();
                let afters: Vec<String> = after.iter().take(3).map(decoy).collect();
                let padded = vec![format!("{pad}{target}{pad}")];

                // Decoys alone never match.
                let mut headers = HeaderMap::new();
                push(&mut headers, &[befores.clone(), afters.clone()].concat());
                assert!(!header_contains_token(&headers, name.clone(), token));

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
                assert!(header_contains_token(&headers, name, token));
            },
        );
    }
}
