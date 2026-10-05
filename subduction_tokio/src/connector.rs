//! Public connector contract and ownership of a single physical attempt.
use crate::{Error, ErrorKind, tasks::Tasks, transport::ErasedTransport};
use ed25519_dalek::{Signature, VerifyingKey};
use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
use std::{
    fmt,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};
use subduction_core::{
    authenticated::Authenticated, handshake::audience::Audience, transport::Transport,
};
use subduction_crypto::signer::Signer;
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// Repeatable establishment of one authenticated physical transport.
///
/// Register resources with the attempt before a cancellation point can strand
/// them. Never spawn detached tasks. The runtime, not this trait, owns retries.
pub trait Connector: Send + Sync + 'static {
    /// Dial and authenticate one attempt. Its tasks are owned by `attempt`.
    fn establish(
        &self,
        attempt: AttemptScope,
        authentication: AuthenticationContext,
    ) -> BoxFuture<'_, Result<EstablishedConnection, Error>>;
    /// Classify failures from establishment or an active connection.
    /// Runtime authentication/policy/cancellation rules override this choice.
    fn classify_failure(&self, error: &Error) -> RetryDecision {
        if matches!(error.kind(), ErrorKind::Transport | ErrorKind::Timeout) {
            RetryDecision::Retry
        } else {
            RetryDecision::Stop
        }
    }
}
impl<C: Connector + ?Sized> Connector for Arc<C> {
    fn establish(
        &self,
        attempt: AttemptScope,
        authentication: AuthenticationContext,
    ) -> BoxFuture<'_, Result<EstablishedConnection, Error>> {
        (**self).establish(attempt, authentication)
    }
    fn classify_failure(&self, error: &Error) -> RetryDecision {
        (**self).classify_failure(error)
    }
}

/// Backend recommendation; retry scheduling is owned by the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Transient failure; apply the configured retry budget.
    Retry,
    /// Terminal failure.
    Stop,
}

/// Shared signing capability, usable with existing generic handshake helpers.
#[derive(Clone)]
pub struct SharedSigner(Arc<dyn Signer<Sendable> + Send + Sync>);
impl SharedSigner {
    pub(crate) fn new(signer: impl Signer<Sendable> + Send + Sync + 'static) -> Self {
        Self(Arc::new(signer))
    }
}
impl fmt::Debug for SharedSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedSigner")
            .field("key", &self.verifying_key())
            .finish()
    }
}
impl Signer<Sendable> for SharedSigner {
    fn sign(&self, message: &[u8]) -> BoxFuture<'_, Signature> {
        self.0.sign(message)
    }
    fn verifying_key(&self) -> VerifyingKey {
        self.0.verifying_key()
    }
}

/// The node's signing capability and immutable identity expectation for an attempt.
#[derive(Debug, Clone)]
pub struct AuthenticationContext {
    pub(crate) signer: SharedSigner,
    pub(crate) audience: Audience,
}
impl AuthenticationContext {
    /// Signer to pass to the shared Subduction handshake.
    #[must_use]
    pub const fn signer(&self) -> &SharedSigner {
        &self.signer
    }
    /// Expected audience. The runtime also checks the authenticated result.
    #[must_use]
    pub const fn audience(&self) -> Audience {
        self.audience
    }
}

/// Successful establishment; construction is only through `AttemptScope::established`.
#[derive(Debug)]
pub struct EstablishedConnection {
    pub(crate) transport: Authenticated<ErasedTransport, Sendable>,
    pub(crate) scope: AttemptScope,
}

#[cfg(test)]
#[path = "connector_tests.rs"]
mod tests;

type Cleanup = BoxFuture<'static, Result<(), Error>>;
struct Resources {
    closed: bool,
    established: bool,
    cleanup: Vec<Cleanup>,
}
struct AttemptInner {
    generation: u64,
    tasks: Tasks,
    tracker: TaskTracker,
    cancel: CancellationToken,
    failure: watch::Sender<Option<Error>>,
    done: watch::Sender<Option<Result<(), Error>>>,
    resources: Mutex<Resources>,
}

// Constructed before spawning, so even an unpolled/aborted cleanup task closes
// admission, releases registered callbacks, and resolves cleanup waiters. In
// particular callbacks can retain a transport that itself retains this scope;
// draining them here breaks that ownership cycle on forced shutdown.
struct CleanupCompletion(AttemptScope);
impl Drop for CleanupCompletion {
    fn drop(&mut self) {
        self.0.cancel();
        let callbacks = {
            let mut resources = self
                .0
                .0
                .resources
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            resources.closed = true;
            std::mem::take(&mut resources.cleanup)
        };
        drop(callbacks);
        self.0.0.tracker.close();
        self.0.0.done.send_if_modified(|value| {
            if value.is_none() {
                *value = Some(Err(Error::new(ErrorKind::Cleanup, "cleanup aborted")));
                true
            } else {
                false
            }
        });
    }
}

/// A runtime-owned attempt, including work started before authentication finishes.
#[derive(Clone)]
pub struct AttemptScope(Arc<AttemptInner>);
impl fmt::Debug for AttemptScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttemptScope")
            .field("generation", &self.generation())
            .finish_non_exhaustive()
    }
}
impl AttemptScope {
    pub(crate) fn new(tasks: &Tasks, generation: u64, budget: Duration) -> Self {
        let scope = Self(Arc::new(AttemptInner {
            generation,
            tasks: tasks.clone(),
            tracker: TaskTracker::new(),
            cancel: CancellationToken::new(),
            failure: watch::channel(None).0,
            done: watch::channel(None).0,
            resources: Mutex::new(Resources {
                closed: false,
                established: false,
                cleanup: Vec::new(),
            }),
        }));
        let cleanup_scope = scope.clone();
        let done = scope.0.done.clone();
        let completion = CleanupCompletion(scope.clone());
        tasks.spawn("attempt cleanup", async move {
            let _completion = completion;
            cleanup_scope.cancelled().await;
            let callbacks = {
                let mut resources = cleanup_scope
                    .0
                    .resources
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                resources.closed = true;
                std::mem::take(&mut resources.cleanup)
            };
            cleanup_scope.0.tracker.close();
            let cleanup = async {
                let (results, ()) = tokio::join!(
                    futures::future::join_all(callbacks),
                    cleanup_scope.0.tracker.wait()
                );
                results
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map(|_| ())
            };
            let result = match tokio::time::timeout(
                budget,
                AssertUnwindSafe(cleanup).catch_unwind(),
            )
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(Error::new(ErrorKind::Cleanup, "cleanup panicked")),
                Err(_) => Err(Error::new(
                    ErrorKind::Cleanup,
                    "attempt cleanup deadline exceeded",
                )),
            };
            done.send_replace(Some(result));
        });
        scope
    }
    /// Generation within a logical connection (starts at zero).
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.0.generation
    }
    /// Wait until this attempt is asked to terminate.
    pub async fn cancelled(&self) {
        self.0.cancel.cancelled().await;
    }
    /// Register bounded async cleanup before an operation can strand resources.
    /// The future runs on failure, cancellation, or eventual disconnection.
    /// Callbacks are polled concurrently and must not depend on registration
    /// order. Group dependent cleanup steps into one callback.
    ///
    /// # Errors
    /// Returns an error if the attempt is already closing. The rejected future
    /// is dropped without being polled; keep a synchronous resource guard until
    /// registration succeeds.
    pub fn on_cleanup(
        &self,
        cleanup: impl Future<Output = Result<(), Error>> + Send + 'static,
    ) -> Result<(), Error> {
        let mut resources = self
            .0
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if resources.closed || self.0.cancel.is_cancelled() {
            return Err(Error::new(ErrorKind::Cancelled, "attempt closing"));
        }
        resources.cleanup.push(Box::pin(cleanup));
        Ok(())
    }
    /// Start a transport-driving task. Any exit ends the physical attempt.
    /// Cancellation drops its future; register separate cleanup for resources
    /// whose release requires asynchronous work. The task must not await cleanup.
    ///
    /// # Errors
    /// Returns an error if the attempt or runtime has stopped accepting work.
    pub fn spawn(
        &self,
        name: &'static str,
        future: impl Future<Output = Result<(), Error>> + Send + 'static,
    ) -> Result<(), Error> {
        let resources = self
            .0
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if resources.closed || self.0.cancel.is_cancelled() {
            return Err(Error::new(ErrorKind::Cancelled, "attempt closing"));
        }
        let scope = self.clone();
        let task = self.0.tracker.track_future(async move {
            let result = tokio::select! {
                biased;
                () = scope.cancelled() => return,
                result = AssertUnwindSafe(future).catch_unwind() => result,
            };
            let error = match result {
                Ok(Err(error)) => error,
                Ok(Ok(())) => Error::new(ErrorKind::Transport, format!("{name} exited")),
                Err(_) => Error::new(ErrorKind::Background, format!("{name} panicked")),
            };
            scope.fail(error);
        });
        if !self.0.tasks.spawn(name, task) {
            return Err(Error::new(ErrorKind::Closed, "runtime closed"));
        }
        Ok(())
    }
    /// Erase an authenticated transport and register its disconnection cleanup.
    /// At most one physical transport may be established by an attempt.
    ///
    /// # Errors
    /// Returns an error if this scope already established a transport or is
    /// closing. Register any async-only resource cleanup before establishment
    /// can be cancelled; rejecting this value drops it synchronously.
    pub fn established<T>(
        &self,
        transport: Authenticated<T, Sendable>,
    ) -> Result<EstablishedConnection, Error>
    where
        T: Transport<Sendable> + Send + Sync + 'static,
        T::SendError: Send + Sync + 'static,
        T::RecvError: Send + Sync + 'static,
        T::DisconnectionError: Send + Sync + 'static,
    {
        let mut resources = self
            .0
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if resources.closed || resources.established || self.0.cancel.is_cancelled() {
            return Err(Error::new(
                ErrorKind::Cancelled,
                "attempt already established or closing",
            ));
        }
        let transport = transport.map(|transport| ErasedTransport::new(transport, self.clone()));
        resources.cleanup.push(transport.inner().cleanup());
        resources.established = true;
        Ok(EstablishedConnection {
            transport,
            scope: self.clone(),
        })
    }
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(crate) fn cancel(&self) {
        self.0.cancel.cancel();
    }
    pub(crate) fn fail(&self, error: Error) {
        if self.0.cancel.is_cancelled() {
            return;
        }
        self.0.failure.send_if_modified(|slot| {
            if slot.is_none() {
                *slot = Some(error);
                true
            } else {
                false
            }
        });
    }
    pub(crate) async fn failed(&self) -> Error {
        let mut receiver = self.0.failure.subscribe();
        loop {
            if let Some(error) = receiver.borrow_and_update().clone() {
                return error;
            }
            if receiver.changed().await.is_err() {
                return Error::new(ErrorKind::Closed, "attempt dropped");
            }
        }
    }
    pub(crate) async fn cleanup(&self) -> Result<(), Error> {
        self.cancel();
        let mut receiver = self.0.done.subscribe();
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }
            if receiver.changed().await.is_err() {
                return Err(Error::new(ErrorKind::Cleanup, "cleanup lost"));
            }
        }
    }
}
/// Cancels an attempt even when its supervisor is forcibly aborted.
pub(crate) struct AttemptGuard(pub(crate) AttemptScope);
impl Drop for AttemptGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
