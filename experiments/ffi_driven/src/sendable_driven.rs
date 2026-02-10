//! The `SendableDriven` future form — a `Send`-safe variant of `Driven`.
//!
//! Unlike `Driven` (which uses thread-locals to locate the `EffectSlot`),
//! `SendableDriven` embeds the slot pointer in a custom `Waker`. This
//! makes the futures `Send` — they can be moved between threads because
//! they carry no thread-affine state.
//!
//! Trade-off: the `EffectSlot` uses `Mutex` instead of `Cell`, so there's
//! a (trivial, uncontended) lock on each effect emit/take.

use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use crate::effect_future::EffectError;

/// Thread-safe communication channel between the inner future and the driver.
///
/// Uses `Mutex` so it can be shared across threads (the `Send` requirement).
/// In practice the lock is never contended — the driver and the future
/// alternate strictly.
pub struct SendableEffectSlot {
    /// Sentinel checked by `slot_from_waker` to verify the Waker actually
    /// carries a `SendableEffectSlot` and wasn't produced by a foreign executor.
    magic: u64,
    effect: Mutex<Option<Box<dyn Any + Send>>>,
    response: Mutex<Option<Box<dyn Any + Send>>>,
}

/// Chosen to be unlikely to appear in a foreign waker's data pointer.
/// Not cryptographic — just a runtime sanity check.
const SLOT_MAGIC: u64 = 0xD01_ED51_0751_0700;

impl SendableEffectSlot {
    pub fn new() -> Self {
        Self {
            magic: SLOT_MAGIC,
            effect: Mutex::new(None),
            response: Mutex::new(None),
        }
    }

    pub fn emit_effect(&self, effect: Box<dyn Any + Send>) {
        *self.effect.lock().expect("effect mutex poisoned") = Some(effect);
    }

    pub fn take_effect(&self) -> Option<Box<dyn Any + Send>> {
        self.effect.lock().expect("effect mutex poisoned").take()
    }

    pub fn provide_response(&self, response: Box<dyn Any + Send>) {
        *self.response.lock().expect("response mutex poisoned") = Some(response);
    }

    pub fn take_response(&self) -> Option<Box<dyn Any + Send>> {
        self.response
            .lock()
            .expect("response mutex poisoned")
            .take()
    }
}

// ==================== Waker carrying slot pointer ====================
//
// The RawWaker's data pointer is a `*const SendableEffectSlot`.
// The vtable is trivial: clone copies the pointer, wake is a no-op
// (we poll manually), drop is a no-op (the slot outlives the waker).

const SLOT_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    slot_waker_clone,
    slot_waker_wake,
    slot_waker_wake_by_ref,
    slot_waker_drop,
);

unsafe fn slot_waker_clone(data: *const ()) -> RawWaker {
    RawWaker::new(data, &SLOT_WAKER_VTABLE)
}

unsafe fn slot_waker_wake(_data: *const ()) {}
unsafe fn slot_waker_wake_by_ref(_data: *const ()) {}
unsafe fn slot_waker_drop(_data: *const ()) {}

/// Create a `Waker` that carries a pointer to a `SendableEffectSlot`.
///
/// The `SendableEffectSlot` must outlive the returned `Waker` and any
/// `Context` created from it. In practice, both are created and consumed
/// within `poll_with_slot`.
pub fn make_slot_waker(slot: &SendableEffectSlot) -> Waker {
    let data = slot as *const SendableEffectSlot as *const ();
    // Safety: the vtable functions are all safe (clone copies pointer,
    // wake/drop are no-ops), and the slot outlives the waker.
    unsafe { Waker::from_raw(RawWaker::new(data, &SLOT_WAKER_VTABLE)) }
}

/// Extract the `SendableEffectSlot` from a `Waker`, verifying the sentinel.
///
/// Returns `Err(EffectError::WrongWaker)` if the waker was not created
/// by `make_slot_waker` (e.g., it came from tokio or another executor).
///
/// # Safety
///
/// The waker's data pointer must either:
/// - Point to a live `SendableEffectSlot` (created by `make_slot_waker`), or
/// - Point to readable memory of at least 8 bytes (for the sentinel check
///   to fail gracefully rather than segfault).
///
/// In practice this is always satisfied: foreign wakers point to their own
/// valid allocations (tokio task headers, etc.), and the sentinel check
/// rejects them before any `SendableEffectSlot` field access.
pub(crate) unsafe fn slot_from_waker(waker: &Waker) -> Result<&SendableEffectSlot, EffectError> {
    let data_ptr = waker.data();
    if data_ptr.is_null() {
        return Err(EffectError::WrongWaker);
    }

    let slot = unsafe { &*(data_ptr as *const SendableEffectSlot) };
    if slot.magic != SLOT_MAGIC {
        return Err(EffectError::WrongWaker);
    }

    Ok(slot)
}

/// A `Send`-safe future driven by an external host via the effect slot protocol.
///
/// The slot pointer is carried in the `Waker`, not in a thread-local.
pub struct SendableDrivenFuture<'a, T> {
    inner: Pin<Box<dyn Future<Output = T> + Send + 'a>>,
}

impl<'a, T> SendableDrivenFuture<'a, T> {
    pub fn new(inner: Pin<Box<dyn Future<Output = T> + Send + 'a>>) -> Self {
        Self { inner }
    }

    /// Poll the inner future, passing the slot via the Waker.
    ///
    /// Returns `Poll::Ready(T)` if the future completed, or
    /// `Poll::Pending` if an effect was emitted to the slot.
    pub fn poll_with_slot(&mut self, slot: &SendableEffectSlot) -> Poll<T> {
        let waker = make_slot_waker(slot);
        let mut cx = Context::from_waker(&waker);
        self.inner.as_mut().poll(&mut cx)
    }
}
