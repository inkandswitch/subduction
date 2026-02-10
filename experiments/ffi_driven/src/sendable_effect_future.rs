//! Sendable effect futures — `Send`-safe variant of `EffectFuture`.
//!
//! Same two-poll yield-resume pattern, but extracts the `SendableEffectSlot`
//! from the `Waker` (via `slot_from_waker`) instead of a thread-local.

use std::{
    any::Any,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::sendable_driven::slot_from_waker;

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

impl<E: Send + 'static, R: Send + 'static> SendableEffectFuture<E, R> {
    pub fn new(effect: E) -> Self {
        Self {
            state: EffectState::Ready(effect),
            _response: PhantomData,
        }
    }
}

impl<E: Send + 'static, R: Send + 'static> Future for SendableEffectFuture<E, R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<R> {
        let this = unsafe { self.get_unchecked_mut() };

        match std::mem::replace(&mut this.state, EffectState::Done) {
            EffectState::Ready(effect) => {
                // First poll: extract slot from Waker, emit effect.
                let slot = unsafe { slot_from_waker(cx.waker()) };
                slot.emit_effect(Box::new(effect) as Box<dyn Any + Send>);
                this.state = EffectState::Pending;
                Poll::Pending
            }
            EffectState::Pending => {
                // Second poll: extract slot from Waker, read response.
                let slot = unsafe { slot_from_waker(cx.waker()) };
                let response = slot
                    .take_response()
                    .expect("sendable driven: polled after Pending but no response provided");
                let typed: Box<R> = response
                    .downcast()
                    .expect("sendable driven: response type mismatch");
                Poll::Ready(*typed)
            }
            EffectState::Done => {
                panic!("sendable driven: EffectFuture polled after completion");
            }
        }
    }
}

// Safety: EffectState<E> is Send when E: Send. PhantomData<R> is always Send.
// The future only accesses the slot during poll (via the Waker), never stores it.
unsafe impl<E: Send + 'static, R: Send + 'static> Send for SendableEffectFuture<E, R> {}

/// Convenience: create a sendable effect future.
pub fn sendable_emit<E: Send + 'static, R: Send + 'static>(
    effect: E,
) -> SendableEffectFuture<E, R> {
    SendableEffectFuture::new(effect)
}
