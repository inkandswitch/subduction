//! Per-caller durability acknowledgement for the group-commit writer.

use tokio::sync::oneshot;

use crate::RedbStorageError;

/// A caller's durability channel paired with the result to deliver.
///
/// The drain returns these rather than sending them inline so the writer's
/// strong `Arc<Database>` is dropped *before* any caller is signalled —
/// otherwise a caller that drops its `RedbStorage` the instant it's acked could
/// still find redb's exclusive file lock held by this in-flight drain.
pub(super) struct Ack {
    responder: oneshot::Sender<Result<(), RedbStorageError>>,
    result: Result<(), RedbStorageError>,
}

impl Ack {
    /// Pair a caller's `responder` with the `result` to hand back to it.
    pub(super) const fn new(
        responder: oneshot::Sender<Result<(), RedbStorageError>>,
        result: Result<(), RedbStorageError>,
    ) -> Self {
        Self { responder, result }
    }

    /// Signal the waiting caller. A dropped receiver just means the caller
    /// cancelled, so the failed send is ignored.
    pub(super) fn send(self) {
        drop(self.responder.send(self.result));
    }
}
