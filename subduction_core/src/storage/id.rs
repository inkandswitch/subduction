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

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;
    use alloc::string::ToString;

    mod display {
        use super::*;

        #[test]
        fn test_display_empty() {
            let id = StorageId::new(String::new());
            assert_eq!(format!("{id}"), "");
        }

        #[test]
        fn test_display_simple() {
            let id = StorageId::new("test-storage".to_string());
            assert_eq!(format!("{id}"), "test-storage");
        }

        #[test]
        fn test_display_with_special_chars() {
            let id = StorageId::new("storage/path:123".to_string());
            assert_eq!(format!("{id}"), "storage/path:123");
        }

        #[test]
        fn test_display_matches_inner_string() {
            let inner = "my-storage-id".to_string();
            let id = StorageId::new(inner.clone());
            assert_eq!(format!("{id}"), inner);
        }
    }

    mod as_str {
        use super::*;

        #[test]
        fn test_as_str_returns_correct_reference() {
            let id = StorageId::new("test".to_string());
            assert_eq!(id.as_str(), "test");
        }

        #[test]
        fn test_as_str_empty() {
            let id = StorageId::new(String::new());
            assert_eq!(id.as_str(), "");
        }

        #[test]
        fn test_as_str_with_unicode() {
            let id = StorageId::new("test-🦀-rust".to_string());
            assert_eq!(id.as_str(), "test-🦀-rust");
        }
    }

    mod ordering {
        use super::*;

        #[test]
        fn test_ordering_lexicographic() {
            let id1 = StorageId::new("aaa".to_string());
            let id2 = StorageId::new("bbb".to_string());

            assert!(id1 < id2);
            assert!(id2 > id1);
        }

        #[test]
        fn test_ordering_case_sensitive() {
            let id1 = StorageId::new("AAA".to_string());
            let id2 = StorageId::new("aaa".to_string());

            assert!(id1 < id2); // Uppercase comes before lowercase
        }

        #[test]
        fn test_equality() {
            let id1 = StorageId::new("test".to_string());
            let id2 = StorageId::new("test".to_string());
            let id3 = StorageId::new("other".to_string());

            assert_eq!(id1, id2);
            assert_ne!(id1, id3);
        }
    }

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_display_matches_inner_string() {
            bolero::check!()
                .with_type::<String>()
                .for_each(|s| {
                    let id = StorageId::new(s.clone());
                    assert_eq!(format!("{id}"), *s);
                });
        }

        #[test]
        fn prop_as_str_matches_inner() {
            bolero::check!()
                .with_type::<String>()
                .for_each(|s| {
                    let id = StorageId::new(s.clone());
                    assert_eq!(id.as_str(), s.as_str());
                });
        }

        #[test]
        fn prop_ordering_matches_string_ordering() {
            bolero::check!()
                .with_type::<(String, String)>()
                .for_each(|(s1, s2)| {
                    let id1 = StorageId::new(s1.clone());
                    let id2 = StorageId::new(s2.clone());
                    assert_eq!(id1.cmp(&id2), s1.cmp(s2));
                });
        }

        #[test]
        fn prop_equality_is_reflexive() {
            bolero::check!()
                .with_type::<StorageId>()
                .for_each(|id| {
                    assert_eq!(id, id);
                });
        }

        #[test]
        fn prop_ordering_is_transitive() {
            bolero::check!()
                .with_type::<(StorageId, StorageId, StorageId)>()
                .for_each(|(id1, id2, id3)| {
                    if id1 < id2 && id2 < id3 {
                        assert!(id1 < id3);
                    }
                });
        }
    }
}
