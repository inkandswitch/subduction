//! Approach C: Driven `FutureForm` variant.
//!
//! Uses the Rust compiler's async state machines to generate the
//! sans-I/O driver protocol automatically. Instead of hand-writing
//! state machines per trait method, we write sequential async code
//! that `.await`s "effect futures" — futures that yield an effect
//! to the external driver and resume when the host provides a response.
//!
//! The key mechanism is a custom `Waker` that carries a pointer to an
//! `EffectSlot`. When an effect future is polled, it writes its effect
//! into the slot and returns `Poll::Pending`. The external driver reads
//! the effect, performs the I/O, writes the response back into the slot,
//! and polls again.

pub mod driven;
pub mod effect_future;
pub mod ffi;
pub mod sendable_driven;
pub mod sendable_effect_future;
pub mod storage;

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time assertion: `SendableDrivenFuture` is `Send`.
    fn _assert_send<T: Send>() {}

    #[allow(dead_code)]
    fn _sendable_driven_future_is_send() {
        _assert_send::<sendable_driven::SendableDrivenFuture<'static, ()>>();
    }

    /// Compile-time assertion: `DrivenFuture` is NOT `Send`.
    /// (If this compiles, DrivenFuture became Send — that would be a bug.)
    /// We can't easily assert !Send at compile time, so we just verify
    /// SendableDrivenFuture IS Send.

    /// Runtime test: drive a SendableDrivenFuture through the slot protocol.
    #[test]
    fn sendable_driven_round_trip() {
        use sendable_driven::SendableEffectSlot;
        use sendable_effect_future::sendable_emit;
        use std::task::Poll;

        // Effect: a simple string request. Response: usize.
        let slot = SendableEffectSlot::new();
        let mut future = sendable_driven::SendableDrivenFuture::new(Box::pin(async {
            let len: usize = sendable_emit::<String, usize>("hello".to_string())
                .await
                .unwrap();
            len * 2
        }));

        // First poll: should emit the effect and return Pending.
        let poll1 = future.poll_with_slot(&slot);
        assert!(matches!(poll1, Poll::Pending));

        // Read the effect.
        let effect = slot.take_effect().expect("should have effect");
        let msg: &String = effect.downcast_ref().expect("should be String");
        assert_eq!(msg, "hello");

        // Provide response.
        slot.provide_response(Box::new(5_usize));

        // Second poll: should complete with 5 * 2 = 10.
        let poll2 = future.poll_with_slot(&slot);
        assert!(matches!(poll2, Poll::Ready(10)));
    }

    /// Verify SendableDrivenFuture can be sent to another thread.
    #[test]
    fn sendable_driven_cross_thread() {
        use sendable_driven::SendableEffectSlot;
        use sendable_effect_future::sendable_emit;
        use std::task::Poll;

        let slot = SendableEffectSlot::new();
        let mut future = sendable_driven::SendableDrivenFuture::new(Box::pin(async {
            let val: u64 = sendable_emit::<u32, u64>(42u32).await.unwrap();
            val + 1
        }));

        // First poll on this thread.
        assert!(matches!(future.poll_with_slot(&slot), Poll::Pending));
        let effect = slot.take_effect().unwrap();
        let n: &u32 = effect.downcast_ref().unwrap();
        assert_eq!(*n, 42);

        // Move to another thread, provide response, poll there.
        let result = std::thread::scope(|s| {
            s.spawn(|| {
                slot.provide_response(Box::new(100u64));
                future.poll_with_slot(&slot)
            })
            .join()
            .unwrap()
        });

        assert!(matches!(result, Poll::Ready(101)));
    }
}
