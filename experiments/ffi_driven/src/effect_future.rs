//! Effect futures — the building block for `Driven` trait implementations.
//!
//! An `EffectFuture<E, R>` represents a single I/O request to the host:
//!
//! - On first poll: writes the effect `E` into the current `EffectSlot`,
//!   returns `Poll::Pending`.
//! - On second poll: reads the response `R` from the `EffectSlot`,
//!   returns `Poll::Ready(R)`.
//!
//! This is the primitive that makes sequential async code work as a
//! sans-I/O state machine.

use std::{
    any::Any,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::driven::current_slot;

/// A future that yields a single effect and waits for a response.
///
/// `E` is the effect type (must be `Any + 'static` for type-erasure).
/// `R` is the response type (must be `Any + 'static` for type-erasure).
pub struct EffectFuture<E: 'static, R: 'static> {
    state: EffectState<E>,
    _response: PhantomData<R>,
}

enum EffectState<E> {
    /// Haven't emitted the effect yet.
    Ready(E),
    /// Effect emitted, waiting for response.
    Pending,
    /// Already completed (shouldn't be polled again).
    Done,
}

impl<E: 'static, R: 'static> EffectFuture<E, R> {
    pub fn new(effect: E) -> Self {
        Self {
            state: EffectState::Ready(effect),
            _response: PhantomData,
        }
    }
}

impl<E: 'static, R: 'static> Future for EffectFuture<E, R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<R> {
        // Safety: we never move out of the Pin, and EffectState is Unpin-safe
        let this = unsafe { self.get_unchecked_mut() };

        match std::mem::replace(&mut this.state, EffectState::Done) {
            EffectState::Ready(effect) => {
                // First poll: emit the effect.
                let slot = current_slot();
                slot.emit_effect(Box::new(effect) as Box<dyn Any>);
                this.state = EffectState::Pending;
                Poll::Pending
            }
            EffectState::Pending => {
                // Second poll: read the response.
                let slot = current_slot();
                let response = slot
                    .take_response()
                    .expect("driven: polled after Pending but no response provided");
                let typed: Box<R> = response.downcast().expect("driven: response type mismatch");
                Poll::Ready(*typed)
            }
            EffectState::Done => {
                panic!("driven: EffectFuture polled after completion");
            }
        }
    }
}

/// Convenience function to create and await an effect in one expression.
///
/// Used in `DrivenStorage` method implementations:
/// ```ignore
/// let digest: Digest<Blob> = emit(SaveBlobEffect { blob }).await;
/// ```
pub fn emit<E: 'static, R: 'static>(effect: E) -> EffectFuture<E, R> {
    EffectFuture::new(effect)
}
