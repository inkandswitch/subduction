//! Extension trait for [`NonEmpty`] to support element removal.

use nonempty::NonEmpty;

/// Result of attempting to remove an element from a [`NonEmpty`] collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoveResult<T> {
    /// Element was not found in the collection.
    NotFound(NonEmpty<T>),
    /// Element was removed; collection still has elements.
    Removed(NonEmpty<T>),
    /// Element was removed; it was the last one.
    WasLast(T),
}

/// Extension trait for [`NonEmpty`] to support element removal.
pub trait NonEmptyExt<T> {
    /// Remove an element by value from the collection.
    ///
    /// Returns a [`RemoveResult`] indicating whether the element was found,
    /// and if so, whether the collection is now empty.
    fn remove_item(self, value: &T) -> RemoveResult<T>;
}

impl<T: PartialEq> NonEmptyExt<T> for NonEmpty<T> {
    fn remove_item(mut self, value: &T) -> RemoveResult<T> {
        // Check if it's the head
        if self.head == *value {
            return match self.tail.pop() {
                Some(new_head) => {
                    // Move popped tail element to head
                    self.head = new_head;
                    RemoveResult::Removed(self)
                }
                None => RemoveResult::WasLast(self.head),
            };
        }

        // Check tail
        if let Some(pos) = self.tail.iter().position(|x| x == value) {
            self.tail.remove(pos);
            return RemoveResult::Removed(self);
        }

        RemoveResult::NotFound(self)
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::wildcard_enum_match_arm)]
mod tests {
    use super::*;

    #[test]
    fn remove_head_with_tail() {
        let mut ne = NonEmpty::new(1);
        ne.push(2);
        ne.push(3);

        match ne.remove_item(&1) {
            RemoveResult::Removed(remaining) => {
                assert_eq!(remaining.len(), 2);
                assert!(!remaining.contains(&1));
            }
            _ => panic!("expected Removed"),
        }
    }

    #[test]
    fn remove_from_tail() {
        let mut ne = NonEmpty::new(1);
        ne.push(2);
        ne.push(3);

        match ne.remove_item(&2) {
            RemoveResult::Removed(remaining) => {
                assert_eq!(remaining.len(), 2);
                assert!(remaining.contains(&1));
                assert!(remaining.contains(&3));
            }
            _ => panic!("expected Removed"),
        }
    }

    #[test]
    fn remove_last_element() {
        let ne = NonEmpty::new(42);

        match ne.remove_item(&42) {
            RemoveResult::WasLast(value) => assert_eq!(value, 42),
            _ => panic!("expected WasLast"),
        }
    }

    #[test]
    fn remove_not_found() {
        let mut ne = NonEmpty::new(1);
        ne.push(2);

        match ne.remove_item(&99) {
            RemoveResult::NotFound(remaining) => {
                assert_eq!(remaining.len(), 2);
            }
            _ => panic!("expected NotFound"),
        }
    }
}
