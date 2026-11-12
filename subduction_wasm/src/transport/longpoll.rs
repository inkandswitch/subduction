//! Long-poll transport driver and Rust-only authenticated connection helpers.
use super::fetch_client::FetchHttpClient;
use crate::{signer::JsSigner, timer, transport::OnDisconnect};
use alloc::string::{String, ToString};
use core::time::Duration;
use future_form::Local;
use futures::FutureExt;
use subduction_core::{
    authenticated::Authenticated,
    peer::id::PeerId,
    spawn::Spawn,
    timeout::{TimedOut, Timeout},
    transport::Transport,
};
use subduction_http_longpoll::{
    client::HttpLongPollClient, session::SessionId, transport::HttpLongPollTransport,
};
use thiserror::Error;
use wasm_bindgen::{closure::Closure, prelude::*};
use wasm_bindgen_futures::JsFuture;

/// Timeout backed by the host's setTimeout, also usable outside browsers.
#[derive(Debug, Clone, Copy)]
pub struct JsTimeout;
impl Timeout<Local> for JsTimeout {
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: futures::future::LocalBoxFuture<'a, T>,
    ) -> futures::future::LocalBoxFuture<'a, Result<T, TimedOut>> {
        use futures::{
            future::{Either, select},
            pin_mut,
        };
        async move {
            let ms = i32::try_from(dur.as_millis()).unwrap_or(i32::MAX);
            let mut timer_id = JsValue::UNDEFINED;
            let delay = js_sys::Promise::new(&mut |resolve, _| {
                timer_id = timer::set_timeout(&resolve, ms);
            });
            let _timer = ClearTimer(timer_id);
            let delay_fut = JsFuture::from(delay);
            pin_mut!(fut, delay_fut);
            match select(fut, delay_fut).await {
                Either::Left((val, _)) => Ok(val),
                Either::Right(_) => Err(TimedOut),
            }
        }
        .boxed_local()
    }
}
struct ClearTimer(JsValue);
impl Drop for ClearTimer {
    fn drop(&mut self) {
        timer::clear_timeout(&self.0);
    }
}

/// Private resource driver implementing the structural JS byte-transport service.
#[wasm_bindgen(js_name = SubductionHttpLongPoll, private, skip_typescript)]
#[derive(Debug, Clone)]
pub struct WasmHttpLongPoll {
    inner: HttpLongPollTransport,
    on_disconnect: OnDisconnect,
}
impl WasmHttpLongPoll {
    pub(crate) fn new(inner: HttpLongPollTransport) -> Self {
        Self {
            inner,
            on_disconnect: OnDisconnect::default(),
        }
    }
}
/// Failure of a driver method.
#[derive(Debug, Clone, Copy, Error)]
pub enum WasmHttpLongPollError {
    /// Outbound channel closed.
    #[error(transparent)]
    Send(#[from] subduction_http_longpoll::error::SendError),
    /// Inbound channel closed.
    #[error(transparent)]
    Recv(#[from] subduction_http_longpoll::error::RecvError),
    /// Disconnect failed.
    #[error(transparent)]
    Disconnect(#[from] subduction_http_longpoll::error::DisconnectionError),
}
impl From<WasmHttpLongPollError> for JsValue {
    fn from(err: WasmHttpLongPollError) -> Self {
        let error = js_sys::Error::new(&err.to_string());
        error.set_name(match err {
            WasmHttpLongPollError::Send(_) => "HttpLongPollSendError",
            WasmHttpLongPollError::Recv(_) => "HttpLongPollRecvError",
            WasmHttpLongPollError::Disconnect(_) => "HttpLongPollDisconnectError",
        });
        error.into()
    }
}
#[wasm_bindgen(js_class = SubductionHttpLongPoll)]
impl WasmHttpLongPoll {
    /// Send an owned frame.
    /// # Errors
    /// Returns an error when the outbound channel is closed.
    #[wasm_bindgen(js_name = sendBytes)]
    pub async fn send_bytes(&self, bytes: &[u8]) -> Result<(), WasmHttpLongPollError> {
        Transport::<Local>::send_bytes(&self.inner, bytes).await?;
        Ok(())
    }
    /// Receive a frame.
    /// # Errors
    /// Returns an error when the inbound channel is closed.
    #[wasm_bindgen(js_name = recvBytes)]
    pub async fn recv_bytes(&self) -> Result<js_sys::Uint8Array, WasmHttpLongPollError> {
        let bytes = Transport::<Local>::recv_bytes(&self.inner).await?;
        Ok(js_sys::Uint8Array::from(bytes.as_slice()))
    }
    /// Disconnect and notify the runtime observer.
    /// # Errors
    /// Propagates disconnect failures.
    pub async fn disconnect(&self) -> Result<(), WasmHttpLongPollError> {
        Transport::<Local>::disconnect(&self.inner).await?;
        self.on_disconnect.take_and_fire();
        Ok(())
    }
    /// Install the disconnect observer.
    #[wasm_bindgen(js_name = onDisconnect)]
    pub fn on_disconnect(&self, callback: js_sys::Function) {
        self.on_disconnect.set(Closure::<dyn Fn()>::new(move || {
            if let Err(e) = callback.call0(&JsValue::NULL) {
                tracing::error!("onDisconnect callback threw: {e:?}");
            }
        }));
    }
}
fn make_client(url: &str) -> HttpLongPollClient<FetchHttpClient> {
    HttpLongPollClient::new(url, FetchHttpClient::new())
}
fn host_from_url(url: &str) -> String {
    let without_scheme = url.find("://").map_or(url, |i| &url[i + 3..]);
    without_scheme
        .find('/')
        .map_or(without_scheme, |i| &without_scheme[..i])
        .into()
}
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn js_now() -> subduction_core::timestamp::TimestampSeconds {
    subduction_core::timestamp::TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64)
}
pub(crate) async fn connect_authenticated(
    url: &str,
    signer: &JsSigner,
    peer: &PeerId,
    spawner: &crate::runtime::WasmSpawn,
) -> Result<(Authenticated<HttpLongPollTransport, Local>, SessionId), LongPollTransportError> {
    let result = make_client(url)
        .connect(signer, *peer, js_now())
        .await
        .map_err(|e| LongPollTransportError::Connection(e.to_string()))?;
    spawner.spawn(result.poll_task);
    spawner.spawn(result.send_task);
    Ok((result.authenticated, result.session_id))
}
pub(crate) async fn connect_discover_authenticated(
    url: &str,
    signer: &JsSigner,
    service: Option<String>,
    spawner: &crate::runtime::WasmSpawn,
) -> Result<(Authenticated<HttpLongPollTransport, Local>, SessionId), LongPollTransportError> {
    let service = service.unwrap_or_else(|| host_from_url(url));
    let result = make_client(url)
        .connect_discover(signer, &service, js_now())
        .await
        .map_err(|e| LongPollTransportError::Connection(e.to_string()))?;
    spawner.spawn(result.poll_task);
    spawner.spawn(result.send_task);
    Ok((result.authenticated, result.session_id))
}
/// Failure while connecting or authenticating a long-poll session.
#[derive(Debug, Error)]
pub enum LongPollTransportError {
    /// Connection or handshake failed.
    #[error("long-poll connection failed: {0}")]
    Connection(String),
}
impl From<LongPollTransportError> for JsValue {
    fn from(err: LongPollTransportError) -> Self {
        let error = js_sys::Error::new(&err.to_string());
        error.set_name("LongPollTransportError");
        error.into()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn host_from_url_with_scheme_and_path() {
        assert_eq!(host_from_url("https://example.com/path"), "example.com");
    }
    #[test]
    fn host_from_url_with_scheme_and_port() {
        assert_eq!(host_from_url("http://localhost:8080/api"), "localhost:8080");
    }
    #[test]
    fn host_from_url_without_scheme() {
        assert_eq!(host_from_url("example.com/path"), "example.com");
    }
    #[test]
    fn host_from_url_bare_host() {
        assert_eq!(host_from_url("localhost"), "localhost");
    }
    #[test]
    fn host_from_url_with_port_no_path() {
        assert_eq!(host_from_url("http://localhost:8080"), "localhost:8080");
    }
}
