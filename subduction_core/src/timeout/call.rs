//! Per-call timeout policy for roundtrip calls.
//!
//! [`CallTimeout`] is the *public boundary type* for the deadline applied to a
//! roundtrip [`call`](crate::connection::managed::ManagedConnection::call). It
//! names the three distinct states explicitly so that `Option<Duration>`'s
//! overloaded `None` (which means "uncapped" at the low level but "use the
//! configured default" at the high level) can never be ambiguous on the API
//! surface.
//!
//! # The three states
//!
//! | Variant | Meaning |
//! |---|---|
//! | [`Default`](CallTimeout::Default) | Inherit the connection's configured default deadline. |
//! | [`Uncapped`](CallTimeout::Uncapped) | No deadline; the bare cancel-safe future. |
//! | [`TimeoutMillis`](CallTimeout::TimeoutMillis) | An explicit per-call deadline. |
//!
//! The convenience layer (the high-level `Subduction` methods, the builder, and
//! the Wasm bindings) [`resolve`](CallTimeout::resolve)s a `CallTimeout` against
//! the configured default into the low-level
//! [`Option<Duration>`](core::option::Option) that
//! [`call`](crate::connection::managed::ManagedConnection::call) consumes, where
//! `None` means *uncapped*.
//!
//! The Wasm/JS surface only ever produces [`Default`](CallTimeout::Default)
//! (`undefined`) or [`TimeoutMillis`](CallTimeout::TimeoutMillis) (a `number`) —
//! [`Uncapped`](CallTimeout::Uncapped) is unrepresentable from JS, so a JS
//! caller always gets *some* bound.

use core::time::Duration;

/// Per-call timeout policy for a roundtrip call.
///
/// See the [module docs](crate::timeout::call) for the rationale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CallTimeout {
    /// Inherit the connection's configured default deadline
    /// (see [`DEFAULT_ROUNDTRIP_TIMEOUT`](crate::multiplexer::DEFAULT_ROUNDTRIP_TIMEOUT)).
    #[default]
    Default,

    /// No deadline. Resolves to the bare cancel-safe future; the caller owns the
    /// cancellation policy (e.g. an outer `select!`/`timeout`/shutdown). The
    /// call still resolves on disconnect via `cancel_all_pending`.
    Uncapped,

    /// An explicit per-call deadline, in milliseconds.
    TimeoutMillis(u64),
}

impl CallTimeout {
    /// Resolve this policy against the connection's configured `default` into
    /// the low-level deadline consumed by
    /// [`call`](crate::connection::managed::ManagedConnection::call).
    ///
    /// - [`Default`](Self::Default) → `Some(default)`
    /// - [`Uncapped`](Self::Uncapped) → `None` (no deadline)
    /// - [`TimeoutMillis(n)`](Self::TimeoutMillis) → `Some(Duration::from_millis(n))`
    #[must_use]
    pub const fn resolve(self, default: Duration) -> Option<Duration> {
        match self {
            Self::Default => Some(default),
            Self::Uncapped => None,
            Self::TimeoutMillis(millis) => Some(Duration::from_millis(millis)),
        }
    }
}

impl From<Option<u64>> for CallTimeout {
    /// Map an optional milliseconds value (the Wasm/JS shape) to a policy:
    /// `None`/`undefined` inherits the default, `Some(n)` is an explicit
    /// deadline. This never produces [`Uncapped`](Self::Uncapped).
    fn from(millis: Option<u64>) -> Self {
        match millis {
            None => Self::Default,
            Some(millis) => Self::TimeoutMillis(millis),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_default_uses_configured() {
        let default = Duration::from_secs(30);
        assert_eq!(CallTimeout::Default.resolve(default), Some(default));
    }

    #[test]
    fn resolve_uncapped_is_none() {
        assert_eq!(CallTimeout::Uncapped.resolve(Duration::from_secs(30)), None);
    }

    #[test]
    fn resolve_explicit_uses_millis_ignoring_default() {
        assert_eq!(
            CallTimeout::TimeoutMillis(5_000).resolve(Duration::from_secs(30)),
            Some(Duration::from_millis(5_000))
        );
    }

    #[test]
    fn from_option_millis_maps_none_to_default() {
        assert_eq!(CallTimeout::from(None), CallTimeout::Default);
    }

    #[test]
    fn from_option_millis_maps_some_to_explicit() {
        assert_eq!(
            CallTimeout::from(Some(1_234)),
            CallTimeout::TimeoutMillis(1_234)
        );
    }

    #[test]
    fn from_never_produces_uncapped() {
        // The Wasm/JS shape cannot express `Uncapped`.
        for millis in [None, Some(0), Some(1), Some(u64::MAX)] {
            assert_ne!(CallTimeout::from(millis), CallTimeout::Uncapped);
        }
    }
}
