//! The `Driven` future form and its `DrivenFuture` type.
//!
//! `DrivenFuture<T>` wraps a `Pin<Box<dyn Future>>` and drives it using
//! a custom `Waker` that carries a pointer to an `EffectSlot`. When the
//! inner future needs I/O, it writes an effect into the slot and returns
//! `Poll::Pending`. The external driver reads the effect, performs the
//! work, writes the response, and polls again.

use std::{
    any::Any,
    cell::Cell,
    future::Future,
    pin::Pin,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

/// The communication channel between the inner future and the external driver.
///
/// Lives on the stack of the driver loop. Its pointer is stashed in a
/// custom `Waker` so inner futures can find it via `Context`.
pub struct EffectSlot {
    /// The effect emitted by the last `.await` that returned `Pending`.
    /// Type-erased because different trait methods produce different effect types.
    pub effect: Cell<Option<Box<dyn Any>>>,

    /// The response provided by the host for the pending effect.
    pub response: Cell<Option<Box<dyn Any>>>,
}

impl EffectSlot {
    pub fn new() -> Self {
        Self {
            effect: Cell::new(None),
            response: Cell::new(None),
        }
    }

    /// Stash an effect for the external driver to read.
    pub fn emit_effect(&self, effect: Box<dyn Any>) {
        self.effect.set(Some(effect));
    }

    /// Take the effect (used by the external driver).
    pub fn take_effect(&self) -> Option<Box<dyn Any>> {
        self.effect.take()
    }

    /// Provide a response (used by the external driver).
    pub fn provide_response(&self, response: Box<dyn Any>) {
        self.response.set(Some(response));
    }

    /// Take the response (used by inner effect futures).
    pub fn take_response(&self) -> Option<Box<dyn Any>> {
        self.response.take()
    }
}

/// Extract the `EffectSlot` pointer from a `Waker`.
///
/// # Safety
///
/// The waker must have been created by `make_waker` with a valid slot pointer.
pub unsafe fn slot_from_waker(_waker: &Waker) -> &EffectSlot {
    // The data pointer of our RawWaker is the EffectSlot pointer.
    // We reconstruct it by exploiting that Waker stores the RawWaker's
    // data pointer. We access it via the waker's clone behavior —
    // our clone fn just copies the pointer.
    //
    // In practice, we use a thread-local to avoid this unsafety.
    // See `CURRENT_SLOT` below.
    unreachable!("use CURRENT_SLOT instead")
}

// Instead of extracting from the Waker (which requires nightly or unsafe
// transmute), we use a thread-local. This is safe because `Driven` futures
// are `!Send` — they never cross thread boundaries.

thread_local! {
    static CURRENT_SLOT: Cell<*const EffectSlot> = const { Cell::new(std::ptr::null()) };
}

/// Set the current effect slot for the duration of a poll.
pub fn with_slot<R>(slot: &EffectSlot, f: impl FnOnce() -> R) -> R {
    CURRENT_SLOT.with(|cell| {
        let old = cell.get();
        cell.set(slot as *const EffectSlot);
        let result = f();
        cell.set(old);
        result
    })
}

/// Get the current effect slot (called by effect futures during poll).
///
/// # Panics
///
/// Panics if called outside of a `with_slot` scope.
pub fn current_slot() -> &'static EffectSlot {
    CURRENT_SLOT.with(|cell| {
        let ptr = cell.get();
        assert!(
            !ptr.is_null(),
            "current_slot() called outside of driven poll"
        );
        // Safety: the slot is alive for the duration of with_slot,
        // and we're within that scope. The 'static is a lie but safe
        // because we only use it within the poll call.
        unsafe { &*ptr }
    })
}

// ==================== No-op Waker ====================
// The Driven form doesn't use wake-based scheduling. We poll manually.

const NOOP_VTABLE: RawWakerVTable =
    RawWakerVTable::new(noop_clone, noop_wake, noop_wake_by_ref, noop_drop);

unsafe fn noop_clone(data: *const ()) -> RawWaker {
    RawWaker::new(data, &NOOP_VTABLE)
}

unsafe fn noop_wake(_data: *const ()) {}
unsafe fn noop_wake_by_ref(_data: *const ()) {}
unsafe fn noop_drop(_data: *const ()) {}

pub fn noop_waker() -> Waker {
    // Safety: the vtable functions are all safe no-ops.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_VTABLE)) }
}

/// A future that is driven by an external host via the effect slot protocol.
///
/// Not `Send` — must be driven on the thread that created it.
pub struct DrivenFuture<'a, T> {
    inner: Pin<Box<dyn Future<Output = T> + 'a>>,
}

impl<'a, T> DrivenFuture<'a, T> {
    pub fn new(inner: Pin<Box<dyn Future<Output = T> + 'a>>) -> Self {
        Self { inner }
    }

    /// Poll the inner future within the context of an effect slot.
    ///
    /// Returns `Poll::Ready(T)` if the future completed, or
    /// `Poll::Pending` if an effect was emitted to the slot.
    pub fn poll_with_slot(&mut self, slot: &EffectSlot) -> Poll<T> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        with_slot(slot, || self.inner.as_mut().poll(&mut cx))
    }
}
