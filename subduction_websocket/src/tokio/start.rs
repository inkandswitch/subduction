//! A trait for types that can start listening for incoming messages as a background task.

use tokio::task::JoinHandle;

use crate::error::RunError;

/// A trait for types that can start listening for incoming messages as a background task.
pub trait Start {
    /// Start listening for incoming messages as a background task.
    fn start(&self) -> JoinHandle<Result<(), RunError>>;
}

/// A wrapper type indicating that the inner `T` has not yet been started.
///
/// This must be consumed to access the inner `T`, either by starting it
/// with [`Unstarted::start`] or by explicitly consuming it without starting it.
#[derive(Debug, Clone, Copy)]
pub struct Unstarted<T>(pub(crate) T);

impl<T: Start> Unstarted<T> {
    /// Start listening for incoming messages as a background task.
    pub fn start(self) -> T {
        self.0.start();
        self.0
    }

    /// Consume the `Unstarted`, returning the inner type *without* starting it.
    pub fn ignore(self) -> T {
        self.0
    }
}
