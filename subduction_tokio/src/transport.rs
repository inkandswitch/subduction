//! Type-erased framed transports with physical-attempt identity.
use crate::{Error, ErrorKind, connector::AttemptScope};
use future_form::Sendable;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use std::{fmt, sync::Arc};
use subduction_core::transport::Transport;

trait ByteIo: Send + Sync {
    fn on_unregistered(&self);
    fn send<'a>(&'a self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), Error>>;
    fn recv(&self) -> BoxFuture<'_, Result<Vec<u8>, Error>>;
    fn disconnect(&self) -> BoxFuture<'_, Result<(), Error>>;
}
struct Adapter<T>(T);
impl<T> ByteIo for Adapter<T>
where
    T: Transport<Sendable> + Send + Sync + 'static,
    T::SendError: Send + Sync + 'static,
    T::RecvError: Send + Sync + 'static,
    T::DisconnectionError: Send + Sync + 'static,
{
    fn on_unregistered(&self) {
        self.0.on_unregistered();
    }
    fn send<'a>(&'a self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.0.send_bytes(bytes).await.map_err(|e| {
                Error::with_source(
                    ErrorKind::Transport,
                    format!("{} send", std::any::type_name::<T>()),
                    e,
                )
            })
        })
    }
    fn recv(&self) -> BoxFuture<'_, Result<Vec<u8>, Error>> {
        Box::pin(async move {
            self.0.recv_bytes().await.map_err(|e| {
                Error::with_source(
                    ErrorKind::Transport,
                    format!("{} receive", std::any::type_name::<T>()),
                    e,
                )
            })
        })
    }
    fn disconnect(&self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.0.disconnect().await.map_err(|e| {
                Error::with_source(
                    ErrorKind::Cleanup,
                    format!("{} disconnect", std::any::type_name::<T>()),
                    e,
                )
            })
        })
    }
}

/// Cloneable transport representation shared by all connectors.
///
/// Equality is physical-attempt identity, never backend address or peer identity.
#[derive(Clone)]
pub struct ErasedTransport {
    io: Arc<dyn ByteIo>,
    identity: Arc<()>,
    attempt: AttemptScope,
    disconnect: Shared<BoxFuture<'static, Result<(), Error>>>,
}
impl ErasedTransport {
    pub(crate) fn new<T>(transport: T, attempt: AttemptScope) -> Self
    where
        T: Transport<Sendable> + Send + Sync + 'static,
        T::SendError: Send + Sync + 'static,
        T::RecvError: Send + Sync + 'static,
        T::DisconnectionError: Send + Sync + 'static,
    {
        let io: Arc<dyn ByteIo> = Arc::new(Adapter(transport));
        let closing = io.clone();
        let disconnect = async move { closing.disconnect().await }.boxed().shared();
        Self {
            io,
            identity: Arc::new(()),
            attempt,
            disconnect,
        }
    }
    pub(crate) fn cleanup(&self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(self.disconnect.clone())
    }
}
impl PartialEq for ErasedTransport {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }
}
impl Eq for ErasedTransport {}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
impl fmt::Debug for ErasedTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErasedTransport")
            .field("attempt", &self.attempt.generation())
            .finish_non_exhaustive()
    }
}
impl Transport<Sendable> for ErasedTransport {
    type SendError = Error;
    type RecvError = Error;
    type DisconnectionError = Error;
    fn on_unregistered(&self) {
        self.io.on_unregistered();
        self.attempt.fail(Error::new(
            ErrorKind::Transport,
            "core unregistered connection",
        ));
    }
    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), Error>> {
        let bytes = bytes.to_vec();
        Box::pin(async move {
            let result = tokio::select! {
                biased;
                () = self.attempt.cancelled() => Err(Error::new(ErrorKind::Cancelled, "attempt closed")),
                result = self.io.send(&bytes) => result,
            };
            if let Err(error) = &result {
                self.attempt.fail(error.clone());
            }
            result
        })
    }
    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Error>> {
        Box::pin(async move {
            let result = tokio::select! {
                biased;
                () = self.attempt.cancelled() => Err(Error::new(ErrorKind::Cancelled, "attempt closed")),
                result = self.io.recv() => result,
            };
            if let Err(error) = &result {
                self.attempt.fail(error.clone());
            }
            result
        })
    }
    fn disconnect(&self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.attempt
                .fail(Error::new(ErrorKind::Transport, "connection disconnected"));
            self.disconnect.clone().await
        })
    }
}
