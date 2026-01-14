//! Start a task in the background.

/// A trait for types that can start listening for incoming messages as a background task.
pub trait Run {
    /// Start listening for incoming messages as a background task.
    #[must_use]
    fn run(self) -> Self;
}
