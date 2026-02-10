//! Effect futures — the building block for `Driven` trait implementations.
//!
//! An `EffectFuture<E, R>` represents a single I/O request to the host:
//!
//! - On first poll: writes the effect `E` into the current `EffectSlot`,
//!   returns `Poll::Pending`.
//! - On second poll: reads the response `R` from the `EffectSlot`,
//!   returns `Poll::Ready(Ok(R))` on success, or `Poll::Ready(Err(_))`
//!   on protocol violation (missing response or type mismatch).
//!
//! This is the primitive that makes sequential async code work as a
//! sans-I/O state machine.

use std::{
    any::Any,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::driven::current_slot_ptr;

/// Error returned when the driver protocol is violated.
#[derive(Debug)]
pub enum EffectError {
    /// `provide_response` was not called before re-polling.
    MissingResponse,
    /// The response type did not match the expected type `R`.
    DowncastFailed { expected: &'static str },
    /// The future was polled after it already completed.
    PollAfterCompletion,
    /// The future was polled with a Waker not created by `make_slot_waker`
    /// (e.g., a tokio or async-std waker). Sentinel check failed.
    WrongWaker,
}

impl fmt::Display for EffectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingResponse => {
                write!(f, "polled after Pending but no response was provided")
            }
            Self::DowncastFailed { expected } => {
                write!(f, "response type mismatch: expected {expected}")
            }
            Self::PollAfterCompletion => write!(f, "EffectFuture polled after completion"),
            Self::WrongWaker => write!(
                f,
                "polled with a foreign Waker (not created by make_slot_waker)"
            ),
        }
    }
}

/// A future that yields a single effect and waits for a response.
///
/// Returns `Result<R, EffectError>` to avoid panicking on protocol violations.
pub struct EffectFuture<E: 'static, R: 'static> {
    state: EffectState<E>,
    _response: PhantomData<R>,
}

enum EffectState<E> {
    Ready(E),
    Pending,
    Done,
}

impl<E: 'static, R: 'static> Unpin for EffectFuture<E, R> {}

impl<E: 'static, R: 'static> EffectFuture<E, R> {
    pub fn new(effect: E) -> Self {
        Self {
            state: EffectState::Ready(effect),
            _response: PhantomData,
        }
    }
}

impl<E: 'static, R: 'static> Future for EffectFuture<E, R> {
    type Output = Result<R, EffectError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match std::mem::replace(&mut this.state, EffectState::Done) {
            EffectState::Ready(effect) => {
                // First poll: emit the effect.
                // Safety: we are inside a `with_slot` scope (via DrivenFuture::poll_with_slot),
                // so the pointer is valid for the duration of this poll call.
                let slot = unsafe { &*current_slot_ptr() };
                slot.emit_effect(Box::new(effect) as Box<dyn Any>);
                this.state = EffectState::Pending;
                Poll::Pending
            }
            EffectState::Pending => {
                let slot = unsafe { &*current_slot_ptr() };
                let Some(response) = slot.take_response() else {
                    return Poll::Ready(Err(EffectError::MissingResponse));
                };
                match response.downcast::<R>() {
                    Ok(typed) => Poll::Ready(Ok(*typed)),
                    Err(_) => Poll::Ready(Err(EffectError::DowncastFailed {
                        expected: std::any::type_name::<R>(),
                    })),
                }
            }
            EffectState::Done => Poll::Ready(Err(EffectError::PollAfterCompletion)),
        }
    }
}

/// Convenience function to create and await an effect in one expression.
///
/// Returns `Result<R, EffectError>` — use `?` to propagate protocol errors.
///
/// ```ignore
/// let DigestResp(d) = emit::<SaveBlobEff, DigestResp<Blob>>(SaveBlobEff(blob)).await?;
/// ```
pub fn emit<E: 'static, R: 'static>(effect: E) -> EffectFuture<E, R> {
    EffectFuture::new(effect)
}
