//! The task-spawning capability.

use future_form::FutureForm;

use crate::spawn::Spawn;

/// Runs background tasks.
pub trait HasSpawner<Async: FutureForm> {
    /// The spawner type.
    type Spawner: Spawn<Async> + Clone;
}
