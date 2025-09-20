//! Storage key.

/// A storage key, represented as a vector of strings.
///
/// Storage laid out this way is amenable to range queries.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StorageKey(Vec<String>);

impl StorageKey {
    /// Create a new storage key from its path segments or other identifier.
    #[must_use]
    pub const fn new(key: Vec<String>) -> Self {
        Self(key)
    }

    /// Get the storage key as a slice of strings.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn as_slice(&self) -> &[String] {
        self.0.as_slice()
    }

    /// Get the storage key as a vector of strings.
    #[must_use]
    pub fn to_vec(&self) -> Vec<String> {
        self.0.clone()
    }

    /// Consume the storage key and return its segments.
    #[must_use]
    pub fn into_vec(self) -> Vec<String> {
        self.0
    }
}

impl From<Vec<String>> for StorageKey {
    fn from(key: Vec<String>) -> Self {
        Self::new(key)
    }
}
