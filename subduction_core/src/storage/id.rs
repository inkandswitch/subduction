//! Storage identifiers.

use alloc::string::String;

/// A simple newtype for storage identifiers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StorageId(String);

impl StorageId {
    /// Create a new [`StorageId`].
    #[must_use]
    pub const fn new(id: String) -> Self {
        Self(id)
    }

    /// Get the string representation of the [`StorageId`].
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl core::fmt::Display for StorageId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}
