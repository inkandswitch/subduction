//! Unstarted async runners.

use crate::run::Run;

/// A wrapper type indicating that the inner `T` has not yet been started.
///
/// This must be consumed to access the inner `T`, either by starting it
/// with [`Unstarted::start`] or by explicitly consuming it without starting it.
#[derive(Debug, Clone, Copy)]
pub struct Unstarted<T: Run + Clone>(T);

impl<T: Run + Clone> Unstarted<T> {
    /// Tag a value as unstarted.
    ///
    /// This forces it to be run before returning the inner value
    /// (via [`start`](Self::start))
    pub fn new(t: T) -> Self {
        Unstarted(t)
    }

    /// Start listening for incoming messages as a background task.
    pub fn start(self) -> T {
        self.0.run()
    }
}
