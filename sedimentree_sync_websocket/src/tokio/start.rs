use tokio::task::JoinHandle;

use crate::error::RunError;

pub trait Start {
    /// Start listening for incoming messages as a background task.
    fn start(&self) -> JoinHandle<Result<(), RunError>>;
}

pub struct Unstarted<T>(pub(crate) T);

impl<T: Start> Unstarted<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn start(self) -> T {
        self.0.start();
        self.0
    }
}
