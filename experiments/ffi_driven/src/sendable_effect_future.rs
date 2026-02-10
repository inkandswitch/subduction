//! Sendable effect futures — `Send`-safe variant of `EffectFuture`.
//!
//! Same two-poll yield-resume pattern, but extracts the `SendableEffectSlot`
//! from the `Waker` (via `slot_from_waker`) instead of a thread-local.
//!
//! Returns `Result<R, EffectError>` instead of panicking on protocol violations.

use std::{
    any::Any,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{effect_future::EffectError, sendable_driven::slot_from_waker};

/// A `Send`-safe future that yields a single effect and waits for a response.
///
/// `E` must be `Send + 'static` (carried across threads via `Box<dyn Any + Send>`).
/// `R` must be `Send + 'static` (response comes back via `Box<dyn Any + Send>`).
pub struct SendableEffectFuture<E: Send + 'static, R: Send + 'static> {
    state: EffectState<E>,
    _response: PhantomData<R>,
}

enum EffectState<E> {
    Ready(E),
    Pending,
    Done,
}

impl<E: Send + 'static, R: Send + 'static> Unpin for SendableEffectFuture<E, R> {}

impl<E: Send + 'static, R: Send + 'static> SendableEffectFuture<E, R> {
    pub fn new(effect: E) -> Self {
        Self {
            state: EffectState::Ready(effect),
            _response: PhantomData,
        }
    }
}

impl<E: Send + 'static, R: Send + 'static> Future for SendableEffectFuture<E, R> {
    type Output = Result<R, EffectError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match std::mem::replace(&mut this.state, EffectState::Done) {
            EffectState::Ready(effect) => {
                let slot = match unsafe { slot_from_waker(cx.waker()) } {
                    Ok(s) => s,
                    Err(e) => return Poll::Ready(Err(e)),
                };
                slot.emit_effect(Box::new(effect) as Box<dyn Any + Send>);
                this.state = EffectState::Pending;
                Poll::Pending
            }
            EffectState::Pending => {
                let slot = match unsafe { slot_from_waker(cx.waker()) } {
                    Ok(s) => s,
                    Err(e) => return Poll::Ready(Err(e)),
                };
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

/// Convenience: create a sendable effect future.
pub fn sendable_emit<E: Send + 'static, R: Send + 'static>(
    effect: E,
) -> SendableEffectFuture<E, R> {
    SendableEffectFuture::new(effect)
}
