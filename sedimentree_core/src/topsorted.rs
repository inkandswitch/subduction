//! Topologically sorted sequence witness type.
//!
//! [`Topsorted<T>`] is a newtype over `Vec<T>` whose constructor is
//! crate-private, ensuring that only code that actually performs a
//! topological sort can produce one.

use alloc::vec::Vec;

/// A sequence of items in topologically sorted (dependency-safe) order.
///
/// This is a witness type: the only way to construct a `Topsorted<T>` is
/// via [`Sedimentree::topsorted_blob_order`], which guarantees that every
/// item's causal dependencies appear before the item itself.
///
/// [`Sedimentree::topsorted_blob_order`]: crate::sedimentree::Sedimentree::topsorted_blob_order
///
/// # Example
///
/// ```rust,ignore
/// use sedimentree_core::sedimentree::SedimentreeItem;
///
/// let order = sedimentree.topsorted_blob_order()?;
/// for item in &order {
///     // dependencies are guaranteed to have been visited already
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topsorted<T>(pub(crate) Vec<T>);

impl<T> Topsorted<T> {
    /// View the topologically sorted sequence as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    /// The number of items in the sequence.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the sequence is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Consume the wrapper and return the inner `Vec`.
    #[must_use]
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}

impl<T> core::ops::Deref for Topsorted<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.0
    }
}

impl<'a, T> IntoIterator for &'a Topsorted<T> {
    type Item = &'a T;
    type IntoIter = core::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T> IntoIterator for Topsorted<T> {
    type Item = T;
    type IntoIter = alloc::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn empty() {
        let t: Topsorted<i32> = Topsorted(vec![]);
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
        assert_eq!(t.as_slice(), &[]);
    }

    #[test]
    fn len_and_is_empty() {
        let t = Topsorted(vec![1, 2, 3]);
        assert!(!t.is_empty());
        assert_eq!(t.len(), 3);
    }

    #[test]
    fn as_slice_matches_inner() {
        let t = Topsorted(vec![10, 20, 30]);
        assert_eq!(t.as_slice(), &[10, 20, 30]);
    }

    #[test]
    fn into_inner_roundtrip() {
        let v = vec![1, 2, 3, 4];
        let t = Topsorted(v.clone());
        assert_eq!(t.into_inner(), v);
    }

    #[test]
    fn deref_to_slice() {
        let t = Topsorted(vec![5, 6, 7]);
        // Deref allows slice methods directly
        assert_eq!(t.first(), Some(&5));
        assert_eq!(t.last(), Some(&7));
        assert_eq!(t.iter().sum::<i32>(), 18);
    }

    #[test]
    fn into_iter_ref() {
        let t = Topsorted(vec![1, 2, 3]);
        let collected: Vec<&i32> = (&t).into_iter().collect();
        assert_eq!(collected, vec![&1, &2, &3]);
    }

    #[test]
    fn into_iter_owned() {
        let t = Topsorted(vec![1, 2, 3]);
        let collected: Vec<i32> = t.into_iter().collect();
        assert_eq!(collected, vec![1, 2, 3]);
    }
}
