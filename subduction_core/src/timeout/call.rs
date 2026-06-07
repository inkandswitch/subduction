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

    /// One concrete anchor per variant: documents the mapping at a glance and
    /// fails loudly if a variant's arm is swapped. The exhaustive coverage over
    /// all inputs lives in the `proptests` below.
    #[test]
    fn resolve_maps_each_variant() {
        let default = Duration::from_secs(30);
        assert_eq!(CallTimeout::Default.resolve(default), Some(default));
        assert_eq!(CallTimeout::Uncapped.resolve(default), None);
        assert_eq!(
            CallTimeout::TimeoutMillis(5_000).resolve(default),
            Some(Duration::from_millis(5_000))
        );
    }

    /// Anchors for the JS-shape conversion (`undefined`/`number`).
    #[test]
    fn from_option_millis_anchors() {
        assert_eq!(CallTimeout::from(None), CallTimeout::Default);
        assert_eq!(
            CallTimeout::from(Some(1_234)),
            CallTimeout::TimeoutMillis(1_234)
        );
    }

    #[cfg(feature = "bolero")]
    mod proptests {
        use super::*;

        /// Test-only generator for `CallTimeout` (we don't derive `Arbitrary`
        /// on the production type). A tagged choice keeps all three variants —
        /// including `Uncapped`, which `From<Option<u64>>` cannot produce —
        /// reachable.
        #[derive(Debug, Clone, Copy, arbitrary::Arbitrary)]
        enum AnyCallTimeout {
            Default,
            Uncapped,
            TimeoutMillis(u64),
        }

        impl From<AnyCallTimeout> for CallTimeout {
            fn from(any: AnyCallTimeout) -> Self {
                match any {
                    AnyCallTimeout::Default => Self::Default,
                    AnyCallTimeout::Uncapped => Self::Uncapped,
                    AnyCallTimeout::TimeoutMillis(n) => Self::TimeoutMillis(n),
                }
            }
        }

        /// `resolve` is a total mapping: each variant resolves to its defined
        /// deadline, and `Uncapped` is the *only* variant that erases the
        /// deadline (yields `None`). Holds for every variant and every default.
        #[test]
        fn prop_resolve_is_total_and_only_uncapped_is_none() {
            bolero::check!()
                .with_arbitrary::<(AnyCallTimeout, u64)>()
                .for_each(|(any, default_ms)| {
                    let ct = CallTimeout::from(*any);
                    let default = Duration::from_millis(*default_ms);
                    let got = ct.resolve(default);

                    match ct {
                        CallTimeout::Default => assert_eq!(got, Some(default)),
                        CallTimeout::Uncapped => assert_eq!(got, None),
                        CallTimeout::TimeoutMillis(n) => {
                            assert_eq!(got, Some(Duration::from_millis(n)));
                        }
                    }

                    // The deadline is erased iff the policy is `Uncapped`.
                    assert_eq!(got.is_none(), matches!(ct, CallTimeout::Uncapped));
                });
        }

        /// `From<Option<u64>>` maps the JS shape exactly, and — over the WHOLE
        /// `Option<u64>` domain — never yields `Uncapped` (JS cannot express
        /// "no timeout"). Subsumes the deleted example-based
        /// `from_never_produces_uncapped`.
        #[test]
        fn prop_from_option_millis_maps_js_shape_and_never_uncapped() {
            bolero::check!()
                .with_arbitrary::<Option<u64>>()
                .for_each(|opt| {
                    let ct = CallTimeout::from(*opt);
                    match opt {
                        None => assert_eq!(ct, CallTimeout::Default),
                        Some(n) => assert_eq!(ct, CallTimeout::TimeoutMillis(*n)),
                    }
                    assert_ne!(ct, CallTimeout::Uncapped);
                });
        }
    }
}
