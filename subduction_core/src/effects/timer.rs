//! The timeout capability.

use future_form::FutureForm;

use crate::timeout::Timeout;

/// Bounds roundtrip calls.
pub trait HasTimer<Async: FutureForm> {
    /// The timer type.
    type Timer: Timeout<Async> + Clone;
}
