//! Storage-related types and traits.

pub mod id;
pub mod key;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub mod metrics;

#[cfg(feature = "metrics")]
pub use metrics::{MetricsStorage, RefreshMetrics};
