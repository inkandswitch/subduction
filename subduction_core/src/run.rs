//! A trait for types that can start listening for incoming messages as a background task.

/// A trait for types that can start listening for incoming messages as a background task.
pub trait Run {
    /// Start listening for incoming messages as a background task.
    fn run(self) -> Self;
}
