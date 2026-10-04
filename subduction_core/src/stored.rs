//! Ongoing stored-data access for applications, not a durable event log.
//!
//! See `SYNC_COMPLETION.md` for validity and lifecycle guarantees. Catch-up and
//! recovery markers do not end a stream, and stored bytes remain opaque.
use crate::peer::id::PeerId;
use alloc::{boxed::Box, string::String, sync::Arc};
use core::{
    pin::Pin,
    task::{Context, Poll},
};
use futures::{Stream, future::AbortHandle};
use sedimentree_core::{
    blob::has_meta::HasBlobMeta,
    codec::{
        decode::DecodeFields,
        encode::{Encode, EncodeFields},
        schema::Schema,
    },
    fragment::Fragment,
    loose_commit::LooseCommit,
};
use subduction_crypto::verified_meta::VerifiedMeta;

/// Verified metadata and its locally stored blob. Deduplicate by tree, kind,
/// and metadata content digest, not causal head. Replay may contain duplicates.
#[derive(Debug, Clone)]
pub enum StoredItem {
    /// A loose commit and blob.
    Commit(Arc<VerifiedMeta<LooseCommit>>),
    /// A fragment and blob.
    Fragment(Arc<VerifiedMeta<Fragment>>),
}
impl StoredItem {
    pub(crate) fn byte_len(&self) -> usize {
        match self {
            Self::Commit(v) => item_size(v),
            Self::Fragment(v) => item_size(v),
        }
    }
}
pub(crate) fn item_size<T: HasBlobMeta + Schema + EncodeFields + DecodeFields>(
    value: &VerifiedMeta<T>,
) -> usize {
    value
        .blob()
        .as_slice()
        .len()
        .saturating_add(value.payload().encoded_size().saturating_mul(2))
        .saturating_add(128)
}
/// One event on an ongoing single-tree stream.
#[derive(Debug, Clone)]
pub enum StoredEvent {
    /// New or replayed stored data; duplicates are allowed.
    Item(StoredItem),
    /// All contents read by an initial/recovery scan precede this marker.
    StorageChecked,
    /// A load-initiated peer round completed its accepted local processing.
    /// This is not universal acceptance, replica equality, or remote persistence.
    PeerSynced {
        /// Authenticated peer whose round completed.
        peer_id: PeerId,
    },
    /// Discard assumptions about the current view. A scan and `StorageChecked`
    /// follow; retired history need not be replayed.
    RescanRequired,
}
/// Terminal watch/loading failure. Unavailability does not prove global absence.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LoadError {
    /// No current candidate supplied usable data.
    #[error("no candidate peer supplied locally available data")]
    Unavailable,
    /// Initial loading, including delivery backpressure, exhausted its deadline.
    #[error("initial loading timed out")]
    TimedOut,
    /// The node closed or lost the ability to certify storage observation.
    #[error("stored-data observation closed")]
    Closed,
    /// Storage or local response processing failed.
    #[error("loading failed: {0}")]
    Operation(String),
}
/// Bounded ongoing delivery. Drop/close cancels this coordinator, not admitted
/// mutations. Snapshot reads can still materialize a whole tree and individual
/// items may exceed the notification budget. Markers never end the stream.
pub struct StoredUpdates {
    receiver: Pin<Box<async_channel::Receiver<StoredMessage>>>,
    pub(crate) terminal: Arc<spin::Mutex<Option<LoadError>>>,
    abort: AbortHandle,
    ended: bool,
}
pub(crate) struct StoredMessage {
    pub(crate) event: StoredEvent,
    // Releasing the one delivery slot permits the driver to take another item.
    pub(crate) accepted: futures::channel::oneshot::Sender<()>,
}
impl StoredUpdates {
    pub(crate) fn new(
        receiver: async_channel::Receiver<StoredMessage>,
        terminal: Arc<spin::Mutex<Option<LoadError>>>,
        abort: AbortHandle,
    ) -> Self {
        Self {
            receiver: Box::pin(receiver),
            terminal,
            abort,
            ended: false,
        }
    }
    /// Cancel the producer; pending reads wake with a closed error. Adapters
    /// can use this without retaining a stream borrow across a foreign await.
    #[must_use]
    pub fn abort_handle(&self) -> AbortHandle {
        self.abort.clone()
    }
    /// Stop this stream, release delivered/pending events, and finish iteration.
    pub fn close(&mut self) {
        self.abort.abort();
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
        self.ended = true;
    }
}
impl Stream for StoredUpdates {
    type Item = Result<StoredEvent, LoadError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        match self.receiver.as_mut().poll_next(cx) {
            Poll::Ready(Some(message)) => {
                let _ = message.accepted.send(());
                Poll::Ready(Some(Ok(message.event)))
            }
            Poll::Ready(None) => {
                self.ended = true;
                Poll::Ready(Some(Err(self
                    .terminal
                    .lock()
                    .take()
                    .unwrap_or(LoadError::Closed))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl Drop for StoredUpdates {
    fn drop(&mut self) {
        self.close();
    }
}
impl core::fmt::Debug for StoredUpdates {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StoredUpdates")
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}
