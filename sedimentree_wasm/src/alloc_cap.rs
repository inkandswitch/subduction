//! Bounded capacity for JS-array-driven [`Vec::with_capacity`].
//!
//! On `wasm32-unknown-unknown` the linear-memory heap is capped at
//! 4 GiB, and `Vec::with_capacity(N)` traps the entire module if it
//! cannot grow memory to fit the request. This is unrecoverable —
//! it manifests as an opaque `RuntimeError: unreachable executed`
//! in the JS console with no diagnostic.
//!
//! Whenever a `Vec`'s capacity is derived from a JS-controlled
//! `Array.length()` (a `u32` we cannot trust), we cap the
//! pre-allocation against a generous-but-finite ceiling. The actual
//! `push`-driven growth still works correctly — we only constrain the
//! initial reservation. If JS truly does hand us a billion items, the
//! `Vec` will grow naturally up to whatever the heap can handle, and
//! the eventual OOM is at least caused by real data flowing through
//! rather than by a fake length value.
//!
//! The ceiling values (see callers) are deliberately set with **lots
//! of headroom** above any plausible real workload, so honest callers
//! never see degraded behaviour. They exist purely to neutralise
//! adversarial / buggy JS callers that might fabricate a giant
//! `length` value.

/// Cap a JS-array length for a `Vec::with_capacity` reservation.
///
/// `requested` is the raw value reported by [`js_sys::Array::length`]
/// (a `u32`, taken `as usize`). `max` is the per-call-site ceiling.
/// Returns `min(requested, max)` as a `usize`.
///
/// # Why not just `min`?
///
/// Wrapping it in a named function makes the intent legible at every
/// call site (`alloc_cap::cap_array_length(arr.length() as usize, MAX_FOO)`)
/// and gives us a single place to add instrumentation if we ever want
/// to log when a cap kicks in.
#[inline]
#[must_use]
pub fn cap_array_length(requested: usize, max: usize) -> usize {
    requested.min(max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_at_zero_max_returns_zero() {
        assert_eq!(cap_array_length(0, 0), 0);
        assert_eq!(cap_array_length(100, 0), 0);
        assert_eq!(cap_array_length(usize::MAX, 0), 0);
    }

    #[test]
    fn cap_passes_through_when_under_max() {
        assert_eq!(cap_array_length(0, 1000), 0);
        assert_eq!(cap_array_length(500, 1000), 500);
        assert_eq!(cap_array_length(1000, 1000), 1000);
    }

    #[test]
    fn cap_clamps_when_over_max() {
        assert_eq!(cap_array_length(1001, 1000), 1000);
        assert_eq!(cap_array_length(u32::MAX as usize, 1000), 1000);
        assert_eq!(cap_array_length(usize::MAX, 1000), 1000);
    }

    /// The hostile case: a JS adapter returning `{length: u32::MAX}`
    /// must produce a small, sane reservation rather than triggering
    /// a Wasm memory-grow trap.
    #[test]
    fn hostile_u32_max_does_not_explode() {
        const ONE_MILLION: usize = 1_000_000;
        let result = cap_array_length(u32::MAX as usize, ONE_MILLION);
        assert_eq!(result, ONE_MILLION);
        // Sanity: we'd never actually allocate this in a test, but
        // confirm that the returned value would be safe to pass to
        // `Vec::with_capacity` on a 32-bit Wasm target.
        assert!(
            result < (1 << 30),
            "capped value must stay well under 1 GiB of usize"
        );
    }
}
