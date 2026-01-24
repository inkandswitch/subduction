//! Storage key.

use alloc::{string::String, vec::Vec};

/// A storage key, represented as a vector of strings.
///
/// Storage laid out this way is amenable to range queries.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cbor(transparent)]
pub struct StorageKey(#[n(0)] Vec<String>);

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

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;

    mod constructors {
        use super::*;

        #[test]
        fn test_new_empty() {
            let key = StorageKey::new(Vec::new());
            assert_eq!(key.as_slice().len(), 0);
        }

        #[test]
        fn test_new_single_segment() {
            let key = StorageKey::new(vec!["segment".to_string()]);
            assert_eq!(key.as_slice(), &["segment"]);
        }

        #[test]
        fn test_new_multiple_segments() {
            let key = StorageKey::new(vec![
                "users".to_string(),
                "123".to_string(),
                "profile".to_string(),
            ]);
            assert_eq!(key.as_slice(), &["users", "123", "profile"]);
        }

        #[test]
        fn test_from_vec() {
            let vec = vec!["a".to_string(), "b".to_string()];
            let key: StorageKey = vec.clone().into();
            assert_eq!(key.as_slice(), vec.as_slice());
        }
    }

    mod accessors {
        use super::*;

        #[test]
        fn test_as_slice() {
            let segments = vec!["a".to_string(), "b".to_string(), "c".to_string()];
            let key = StorageKey::new(segments.clone());
            assert_eq!(key.as_slice(), segments.as_slice());
        }

        #[test]
        fn test_to_vec() {
            let segments = vec!["x".to_string(), "y".to_string()];
            let key = StorageKey::new(segments.clone());
            let result = key.to_vec();
            assert_eq!(result, segments);
        }

        #[test]
        fn test_to_vec_is_clone() {
            let segments = vec!["test".to_string()];
            let key = StorageKey::new(segments.clone());
            let vec1 = key.to_vec();
            let vec2 = key.to_vec();
            assert_eq!(vec1, vec2);
        }

        #[test]
        fn test_into_vec() {
            let segments = vec!["one".to_string(), "two".to_string()];
            let key = StorageKey::new(segments.clone());
            let result = key.into_vec();
            assert_eq!(result, segments);
        }
    }

    mod ordering {
        use super::*;

        #[test]
        fn test_ordering_by_first_segment() {
            let key1 = StorageKey::new(vec!["aaa".to_string()]);
            let key2 = StorageKey::new(vec!["bbb".to_string()]);

            assert!(key1 < key2);
            assert!(key2 > key1);
        }

        #[test]
        fn test_ordering_shorter_is_less() {
            let key1 = StorageKey::new(vec!["a".to_string()]);
            let key2 = StorageKey::new(vec!["a".to_string(), "b".to_string()]);

            assert!(key1 < key2);
        }

        #[test]
        fn test_ordering_lexicographic() {
            let key1 = StorageKey::new(vec!["a".to_string(), "b".to_string()]);
            let key2 = StorageKey::new(vec!["a".to_string(), "c".to_string()]);

            assert!(key1 < key2);
        }

        #[test]
        fn test_equality() {
            let key1 = StorageKey::new(vec!["a".to_string(), "b".to_string()]);
            let key2 = StorageKey::new(vec!["a".to_string(), "b".to_string()]);
            let key3 = StorageKey::new(vec!["a".to_string(), "c".to_string()]);

            assert_eq!(key1, key2);
            assert_ne!(key1, key3);
        }

        #[test]
        fn test_empty_keys_equal() {
            let key1 = StorageKey::new(Vec::new());
            let key2 = StorageKey::new(Vec::new());

            assert_eq!(key1, key2);
        }
    }

    mod conversions {
        use super::*;

        #[test]
        fn test_roundtrip_to_vec() {
            let original = vec!["foo".to_string(), "bar".to_string()];
            let key = StorageKey::new(original.clone());
            let back = key.to_vec();
            assert_eq!(original, back);
        }

        #[test]
        fn test_roundtrip_into_vec() {
            let original = vec!["foo".to_string(), "bar".to_string()];
            let key = StorageKey::new(original.clone());
            let back = key.into_vec();
            assert_eq!(original, back);
        }

        #[test]
        fn test_from_vec_roundtrip() {
            let original = vec!["test".to_string()];
            let key = StorageKey::from(original.clone());
            let back = key.into_vec();
            assert_eq!(original, back);
        }
    }

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_as_slice_matches_inner() {
            bolero::check!()
                .with_type::<Vec<String>>()
                .for_each(|segments| {
                    let key = StorageKey::new(segments.clone());
                    assert_eq!(key.as_slice(), segments.as_slice());
                });
        }

        #[test]
        fn prop_to_vec_roundtrip() {
            bolero::check!()
                .with_type::<Vec<String>>()
                .for_each(|segments| {
                    let key = StorageKey::new(segments.clone());
                    assert_eq!(key.to_vec(), *segments);
                });
        }

        #[test]
        fn prop_into_vec_roundtrip() {
            bolero::check!()
                .with_type::<Vec<String>>()
                .for_each(|segments| {
                    let key = StorageKey::new(segments.clone());
                    assert_eq!(key.into_vec(), *segments);
                });
        }

        #[test]
        fn prop_from_vec_roundtrip() {
            bolero::check!()
                .with_type::<Vec<String>>()
                .for_each(|segments| {
                    let key = StorageKey::from(segments.clone());
                    assert_eq!(key.into_vec(), *segments);
                });
        }

        #[test]
        fn prop_ordering_matches_vec_ordering() {
            bolero::check!()
                .with_type::<(Vec<String>, Vec<String>)>()
                .for_each(|(v1, v2)| {
                    let key1 = StorageKey::new(v1.clone());
                    let key2 = StorageKey::new(v2.clone());
                    assert_eq!(key1.cmp(&key2), v1.cmp(v2));
                });
        }

        #[test]
        fn prop_equality_is_reflexive() {
            bolero::check!().with_type::<StorageKey>().for_each(|key| {
                assert_eq!(key, key);
            });
        }

        #[test]
        fn prop_ordering_is_transitive() {
            bolero::check!()
                .with_type::<(StorageKey, StorageKey, StorageKey)>()
                .for_each(|(k1, k2, k3)| {
                    if k1 < k2 && k2 < k3 {
                        assert!(k1 < k3);
                    }
                });
        }
    }
}
