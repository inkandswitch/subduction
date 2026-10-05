//! Private typed core adapter. No unrestricted core handle escapes the runtime.
use crate::{
    Error, ErrorKind,
    connector::SharedSigner,
    tasks::{ManagedSpawn, Tasks, Timer},
    transport::ErasedTransport,
};
use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use std::{sync::Arc, time::Duration};
use subduction_core::{
    authenticated::Authenticated,
    connection::stats::SyncStats,
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    storage::traits::Storage,
    stored::StoredUpdates,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};

pub(crate) type Connection = Authenticated<MessageTransport<ErasedTransport>, Sendable>;
type Handler<S, P> = SyncHandler<
    Sendable,
    S,
    MessageTransport<ErasedTransport>,
    P,
    CountLeadingZeroBytes,
    ManagedSpawn,
>;
type Node<S, P> = Subduction<
    'static,
    Sendable,
    S,
    MessageTransport<ErasedTransport>,
    Handler<S, P>,
    P,
    SharedSigner,
    Timer,
    ManagedSpawn,
>;

/// Existing local-response sync result, not a remote acknowledgement.
#[derive(Debug, Clone)]
pub struct PeerSyncReport {
    /// Whether the core completed an exchange. Rejected items may be skipped.
    pub success: bool,
    /// Core statistics; received counts are offered rather than accepted items.
    pub stats: SyncStats,
    /// Request/response call failures. The existing core may log other failures.
    pub transport_errors: Vec<Error>,
}

pub(crate) trait Core: Send + Sync {
    fn shutdown(&self);
    fn stored(&self, id: SedimentreeId, load: bool) -> StoredUpdates;
    fn add(&self, connection: Connection) -> BoxFuture<'_, Result<(), Error>>;
    fn remove(&self, connection: Connection) -> BoxFuture<'_, Result<(), Error>>;
    fn store(
        &self,
        id: SedimentreeId,
        commits: Vec<(LooseCommit, Blob)>,
        fragments: Vec<(Fragment, Blob)>,
    ) -> BoxFuture<'_, Result<(), Error>>;
    fn blobs(&self, id: SedimentreeId) -> BoxFuture<'_, Result<Vec<Blob>, Error>>;
    fn ids(&self) -> BoxFuture<'_, Result<Vec<SedimentreeId>, Error>>;
    fn sync(
        &self,
        peer: PeerId,
        id: SedimentreeId,
        subscribe: bool,
        timeout: CallTimeout,
    ) -> BoxFuture<'_, Result<PeerSyncReport, Error>>;
}
struct TypedCore<S, P>(Arc<Node<S, P>>)
where
    S: Storage<Sendable> + Send + Sync + std::fmt::Debug + 'static,
    S::Error: Send + Sync + 'static,
    P: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    P::FetchDisallowed: Send + Sync + 'static,
    P::PutDisallowed: Send + Sync + 'static;
impl<S, P> Core for TypedCore<S, P>
where
    S: Storage<Sendable> + Send + Sync + std::fmt::Debug + 'static,
    S::Error: Send + Sync + 'static,
    P: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    P::ConnectionDisallowed: Send + Sync + 'static,
    P::FetchDisallowed: Send + Sync + 'static,
    P::PutDisallowed: Send + Sync + 'static,
{
    fn shutdown(&self) {
        self.0.shutdown();
    }
    fn stored(&self, id: SedimentreeId, load: bool) -> StoredUpdates {
        if load {
            self.0.load_and_watch(id)
        } else {
            self.0.watch_stored(id)
        }
    }
    fn add(&self, connection: Connection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.0
                .add_connection(connection)
                .await
                .map(|_| ())
                .map_err(|e| Error::with_source(ErrorKind::Policy, "connection registration", e))
        })
    }
    fn remove(&self, connection: Connection) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move { self.0.disconnect(&connection).await.map(|_| ()) })
    }
    fn store(
        &self,
        id: SedimentreeId,
        commits: Vec<(LooseCommit, Blob)>,
        fragments: Vec<(Fragment, Blob)>,
    ) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.0
                .store_built_batch(id, commits, fragments)
                .await
                .map_err(|e| Error::with_source(ErrorKind::Operation, "store batch", e))
        })
    }
    fn blobs(&self, id: SedimentreeId) -> BoxFuture<'_, Result<Vec<Blob>, Error>> {
        Box::pin(async move {
            self.0
                .get_blobs(id)
                .await
                .map(|blobs| blobs.map_or_else(Vec::new, Vec::from))
                .map_err(|e| Error::with_source(ErrorKind::Operation, "read blobs", e))
        })
    }
    fn ids(&self) -> BoxFuture<'_, Result<Vec<SedimentreeId>, Error>> {
        Box::pin(async move { Ok(self.0.sedimentree_ids().await) })
    }
    fn sync(
        &self,
        peer: PeerId,
        id: SedimentreeId,
        subscribe: bool,
        timeout: CallTimeout,
    ) -> BoxFuture<'_, Result<PeerSyncReport, Error>> {
        Box::pin(async move {
            let (success, stats, errors) = self
                .0
                .sync_with_peer(&peer, id, subscribe, timeout)
                .await
                .map_err(|e| Error::with_source(ErrorKind::Operation, "sync", e))?;
            Ok(PeerSyncReport {
                success,
                stats,
                transport_errors: errors
                    .into_iter()
                    .map(|(_, e)| Error::with_source(ErrorKind::Transport, "sync roundtrip", e))
                    .collect(),
            })
        })
    }
}

pub(crate) fn build<S, P>(
    storage: S,
    policy: P,
    signer: SharedSigner,
    tasks: &Tasks,
    stop: tokio_util::sync::CancellationToken,
    timeout: Duration,
) -> Arc<dyn Core>
where
    S: Storage<Sendable> + Send + Sync + std::fmt::Debug + 'static,
    S::Error: Send + Sync + 'static,
    P: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    P::ConnectionDisallowed: Send + Sync + 'static,
    P::FetchDisallowed: Send + Sync + 'static,
    P::PutDisallowed: Send + Sync + 'static,
{
    let (node, _handler, listener, manager) = SubductionBuilder::new()
        .storage(storage, Arc::new(policy))
        .signer(signer)
        .spawner(ManagedSpawn(tasks.clone()))
        .timer(Timer)
        .roundtrip_timeout(timeout)
        .send_counter(subduction_core::peer::counter::PeerCounter::with_seed(
            subduction_core::peer::counter::wall_clock_seed,
        ))
        .build::<Sendable, MessageTransport<ErasedTransport>>();
    let fail = tasks.clone();
    let stopping = stop.clone();
    tasks.spawn("core manager", async move {
        let result = manager.await;
        if !stopping.is_cancelled() {
            let message = "core manager exited unexpectedly";
            fail.fail(result.err().map_or_else(
                || Error::new(ErrorKind::Background, message),
                |error| Error::with_source(ErrorKind::Background, message, error),
            ));
        }
    });
    let fail = tasks.clone();
    tasks.spawn("core listener", async move {
        let result = listener.await;
        if !stop.is_cancelled() {
            let message = "core listener exited unexpectedly";
            fail.fail(result.err().map_or_else(
                || Error::new(ErrorKind::Background, message),
                |error| Error::with_source(ErrorKind::Background, message, error),
            ));
        }
    });
    Arc::new(TypedCore(node))
}
