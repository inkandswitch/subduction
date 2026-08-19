//! Completion accounting for spawned inbound-dispatch tasks.
//!
//! Each spawned task reports its outcome back to the listener over an unbounded
//! channel. The listener uses the report to decrement the in-flight gauge and,
//! on a handler error, to tear the connection down. A task that finished
//! without reporting — for instance by panicking before sending — would leak
//! its gauge increment and, worse, leave a failed connection un-condemned.
//!
//! [`DispatchCompletion`] guarantees a report on *every* exit path. The dispatch
//! body records its outcome into the guard; the guard's [`Drop`] sends that
//! outcome (or [`DispatchOutcome::Aborted`] if the body never recorded one,
//! e.g. on unwind) over the channel with a non-blocking `try_send`. The channel
//! is unbounded, so the send cannot fail for lack of capacity — only if the
//! listener has already gone, in which case there is nothing to account.

use core::fmt;

use async_channel::Sender;
use future_form::FutureForm;

use crate::authenticated::Authenticated;

/// The outcome a spawned dispatch task reports back to the listener.
pub enum DispatchOutcome<Conn: Clone, Async: FutureForm, HandlerError> {
    /// The handler ran to completion. The listener decrements the in-flight
    /// count and, on `Err`, tears the connection down.
    Completed {
        /// The connection the message arrived on, returned so the listener can
        /// tear it down on `Err`.
        conn: Authenticated<Conn, Async>,
        /// The handler's result; `Err` condemns the connection.
        result: Result<(), HandlerError>,
    },

    /// The task's future was dropped before the handler returned (panic,
    /// cancellation). The listener decrements the in-flight count but must not
    /// infer connection health from it — a dropped future is not a broken peer.
    Aborted,
}

impl<Conn: Clone, Async: FutureForm, HandlerError> fmt::Debug
    for DispatchOutcome<Conn, Async, HandlerError>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed { result, .. } => f
                .debug_struct("Completed")
                .field("is_err", &result.is_err())
                .finish_non_exhaustive(),
            Self::Aborted => f.write_str("Aborted"),
        }
    }
}

/// Reports a [`DispatchOutcome`] to the listener when dropped, so the in-flight
/// count is released on the normal path *and* on unwind.
///
/// The dispatch body calls [`complete`](Self::complete) once it has a handler
/// result; if it never does (the future unwound first), the guard reports
/// [`DispatchOutcome::Aborted`].
pub struct DispatchCompletion<Conn: Clone, Async: FutureForm, HandlerError> {
    sender: Sender<DispatchOutcome<Conn, Async, HandlerError>>,
    outcome: Option<DispatchOutcome<Conn, Async, HandlerError>>,
}

impl<Conn: Clone, Async: FutureForm, HandlerError> fmt::Debug
    for DispatchCompletion<Conn, Async, HandlerError>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DispatchCompletion")
            .field("recorded", &self.outcome.is_some())
            .finish_non_exhaustive()
    }
}

impl<Conn: Clone, Async: FutureForm, HandlerError> DispatchCompletion<Conn, Async, HandlerError> {
    /// Create a guard that will report over `sender` when dropped.
    #[must_use]
    pub const fn new(sender: Sender<DispatchOutcome<Conn, Async, HandlerError>>) -> Self {
        Self {
            sender,
            outcome: None,
        }
    }

    /// Record the handler's outcome. The guard reports this on drop instead of
    /// [`DispatchOutcome::Aborted`].
    pub fn complete(&mut self, conn: Authenticated<Conn, Async>, result: Result<(), HandlerError>) {
        self.outcome = Some(DispatchOutcome::Completed { conn, result });
    }
}

impl<Conn: Clone, Async: FutureForm, HandlerError> Drop
    for DispatchCompletion<Conn, Async, HandlerError>
{
    fn drop(&mut self) {
        let outcome = self.outcome.take().unwrap_or(DispatchOutcome::Aborted);
        // Unbounded channel: `try_send` only fails if the listener is already
        // gone, in which case there is no in-flight count left to release — so
        // a dropped outcome is exactly the right behavior.
        let _result = self.sender.try_send(outcome);
    }
}

#[cfg(all(test, feature = "test_utils"))]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use future_form::Sendable;

    use super::*;
    use crate::{connection::test_utils::MockConnection, peer::id::PeerId};

    type TestOutcome = DispatchOutcome<MockConnection, Sendable, ()>;

    fn auth() -> Authenticated<MockConnection, Sendable> {
        Authenticated::new_for_test(MockConnection::new(), PeerId::new([0u8; 32]))
    }

    /// A guard dropped after recording a result reports `Completed`, carrying
    /// that result through to the listener.
    #[test]
    fn complete_then_drop_reports_completed() {
        let (tx, rx) = async_channel::unbounded::<TestOutcome>();

        let mut guard = DispatchCompletion::new(tx);
        guard.complete(auth(), Err(()));
        drop(guard);

        match rx.try_recv().expect("guard must report on drop") {
            DispatchOutcome::Completed { result, .. } => {
                assert!(result.is_err(), "the recorded result must be preserved");
            }
            DispatchOutcome::Aborted => panic!("recorded completion reported as Aborted"),
        }
    }

    /// A guard dropped *without* recording (the unwind / cancellation path)
    /// still reports — as `Aborted` — so the listener always decrements.
    #[test]
    fn drop_without_complete_reports_aborted() {
        let (tx, rx) = async_channel::unbounded::<TestOutcome>();

        let guard = DispatchCompletion::new(tx);
        drop(guard);

        assert!(
            matches!(
                rx.try_recv()
                    .expect("guard must report even without complete"),
                DispatchOutcome::Aborted
            ),
            "a guard dropped before `complete` must report Aborted"
        );
    }

    /// Every drop reports exactly once, so the listener's count moves by exactly
    /// one per task regardless of path.
    #[test]
    fn drop_reports_exactly_once() {
        let (tx, rx) = async_channel::unbounded::<TestOutcome>();

        {
            let mut guard = DispatchCompletion::new(tx);
            guard.complete(auth(), Ok(()));
        }

        assert!(rx.try_recv().is_ok(), "exactly one report expected");
        assert!(
            rx.try_recv().is_err(),
            "guard must not report more than once"
        );
    }
}
