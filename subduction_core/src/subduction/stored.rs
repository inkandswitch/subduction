//! Application-stream coordination above the shared storage validity boundary.
//!
//! This driver scans full verified items through the same local Fetcher used by
//! trusted reads, delivers catch-up/recovery with backpressure, and then consumes
//! bounded notifications. It does not execute mutations or route incoming sync
//! messages. No network listener is required for storage-only watching.
use crate::{
    spawn::yield_once,
    storage::{fetcher::Fetcher, state::Watch, traits::Storage},
    stored::{LoadError, StoredEvent, StoredItem},
};
use alloc::{boxed::Box, string::ToString, sync::Arc, vec::Vec};
use future_form::FutureForm;
use futures::future::{Either, select};

pub(super) struct Output {
    pub(super) sender: async_channel::Sender<crate::stored::StoredMessage>,
    pub(super) terminal: Arc<spin::Mutex<Option<LoadError>>>,
}
impl Output {
    pub(super) async fn send(&mut self, event: StoredEvent) -> Result<(), LoadError> {
        let (accepted, wait) = futures::channel::oneshot::channel();
        self.sender
            .send(crate::stored::StoredMessage { event, accepted })
            .await
            .map_err(|_| LoadError::Closed)?;
        // Do not remove another notification while this single delivery slot
        // is occupied. This is delivery backpressure, not a storage barrier.
        wait.await.map_err(|_| LoadError::Closed)
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        let mut terminal = self.terminal.lock();
        if terminal.is_none() {
            *terminal = Some(LoadError::Closed);
        }
    }
}
pub(super) async fn snapshot<Async: FutureForm, Store: Storage<Async>>(
    fetcher: &Fetcher<Async, Store>,
    watch: &Watch,
    output: &mut Output,
) -> Result<bool, LoadError> {
    loop {
        yield_once().await;
        let Some(revision) = watch.scan_start() else {
            watch.notified().await;
            continue;
        };
        let mut items: Vec<StoredItem> = fetcher
            .load_loose_commits()
            .await
            .map_err(|e| LoadError::Operation(e.to_string()))?
            .into_iter()
            .map(|item| StoredItem::Commit(Arc::new(item)))
            .collect();
        items.extend(
            fetcher
                .load_fragments()
                .await
                .map_err(|e| LoadError::Operation(e.to_string()))?
                .into_iter()
                .map(|item| StoredItem::Fragment(Arc::new(item))),
        );
        // Validate and clear only notifications covered by this stable read.
        // An overlapping mutation makes the scan retry, never certify a gap.
        if !watch.scan_finish(revision) {
            continue;
        }
        let present = !items.is_empty();
        for item in items {
            yield_once().await;
            output.send(StoredEvent::Item(item)).await?;
        }
        output.send(StoredEvent::StorageChecked).await?;
        return Ok(present);
    }
}
pub(super) async fn deliver<Async: FutureForm, Store: Storage<Async>>(
    fetcher: &Fetcher<Async, Store>,
    watch: &Watch,
    output: &mut Output,
    event: StoredEvent,
) -> Result<(), LoadError> {
    let rescan = matches!(event, StoredEvent::RescanRequired);
    output.send(event).await?;
    if rescan {
        snapshot(fetcher, watch, output).await?;
    }
    Ok(())
}
pub(super) async fn run<Async: FutureForm, Store: Storage<Async>>(
    fetcher: Fetcher<Async, Store>,
    watch: Watch,
    mut output: Output,
) {
    let work = async {
        snapshot(&fetcher, &watch, &mut output).await?;
        loop {
            yield_once().await;
            if let Some(event) = watch.take() {
                deliver(&fetcher, &watch, &mut output, event).await?;
            } else {
                watch.notified().await;
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), LoadError>(())
    };
    let result = match select(Box::pin(fetcher.state.wait_closed()), Box::pin(work)).await {
        Either::Left(_) => Err(LoadError::Closed),
        Either::Right((result, _)) => result,
    };
    if let Err(error) = result {
        *output.terminal.lock() = Some(error);
    }
}
