//! Subduction policy for the CLI server.
//!
//! Same actor-bridge pattern as `keyhive.rs`: a `Send + Sync` handle forwards
//! `authorize_*` calls to the keyhive thread. [`LegacyRelayPolicy`] wraps the
//! handle to short-circuit legacy (zero-suffix) doc IDs.

use std::{
    collections::HashSet,
    sync::{Arc, OnceLock},
};

use async_channel::{Receiver, Sender};
use async_lock::Mutex as AsyncMutex;
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_crypto::verified_author::VerifiedAuthor;
use subduction_keyhive_policy::{
    ConnectionDisallowedError, FetchDisallowedError, PutDisallowedError, authorize_connect_with,
    authorize_fetch_with, authorize_put_with, filter_authorized_fetch_with,
};

use crate::keyhive::CliKeyhive;

const POLICY_COMMAND_CAPACITY: usize = 1024;

#[allow(missing_debug_implementations)]
pub(crate) enum PolicyCommand {
    AuthorizeConnect {
        peer: PeerId,
        reply: Sender<Result<(), ConnectionDisallowedError>>,
    },
    AuthorizeFetch {
        peer: PeerId,
        sedimentree_id: SedimentreeId,
        reply: Sender<Result<(), FetchDisallowedError>>,
    },
    AuthorizePut {
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
        reply: Sender<Result<(), PutDisallowedError>>,
    },
    FilterAuthorizedFetch {
        peer: PeerId,
        ids: Vec<SedimentreeId>,
        reply: Sender<Vec<SedimentreeId>>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct CliKeyhivePolicyHandle {
    tx: Sender<PolicyCommand>,
}

impl CliKeyhivePolicyHandle {
    pub(crate) fn channel() -> (Self, Receiver<PolicyCommand>) {
        let (tx, rx) = async_channel::bounded(POLICY_COMMAND_CAPACITY);
        (Self { tx }, rx)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliPolicyConnectDisallowed {
    #[error(transparent)]
    Keyhive(#[from] ConnectionDisallowedError),
    #[error("keyhive policy actor has shut down")]
    ActorGone,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliPolicyFetchDisallowed {
    #[error(transparent)]
    Keyhive(#[from] FetchDisallowedError),
    #[error("keyhive policy actor has shut down")]
    ActorGone,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliPolicyPutDisallowed {
    #[error(transparent)]
    Keyhive(#[from] PutDisallowedError),
    #[error("keyhive policy actor has shut down")]
    ActorGone,
}

impl ConnectionPolicy<future_form::Sendable> for CliKeyhivePolicyHandle {
    type ConnectionDisallowed = CliPolicyConnectDisallowed;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            self.tx
                .send(PolicyCommand::AuthorizeConnect {
                    peer,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| CliPolicyConnectDisallowed::ActorGone)?;
            let result = reply_rx
                .recv()
                .await
                .map_err(|_| CliPolicyConnectDisallowed::ActorGone)?;
            result.map_err(CliPolicyConnectDisallowed::Keyhive)
        }
        .boxed()
    }
}

impl StoragePolicy<future_form::Sendable> for CliKeyhivePolicyHandle {
    type FetchDisallowed = CliPolicyFetchDisallowed;
    type PutDisallowed = CliPolicyPutDisallowed;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            self.tx
                .send(PolicyCommand::AuthorizeFetch {
                    peer,
                    sedimentree_id,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| CliPolicyFetchDisallowed::ActorGone)?;
            let result = reply_rx
                .recv()
                .await
                .map_err(|_| CliPolicyFetchDisallowed::ActorGone)?;
            result.map_err(CliPolicyFetchDisallowed::Keyhive)
        }
        .boxed()
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            self.tx
                .send(PolicyCommand::AuthorizePut {
                    requestor,
                    author,
                    sedimentree_id,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| CliPolicyPutDisallowed::ActorGone)?;
            let result = reply_rx
                .recv()
                .await
                .map_err(|_| CliPolicyPutDisallowed::ActorGone)?;
            result.map_err(CliPolicyPutDisallowed::Keyhive)
        }
        .boxed()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            if self
                .tx
                .send(PolicyCommand::FilterAuthorizedFetch {
                    peer,
                    ids,
                    reply: reply_tx,
                })
                .await
                .is_err()
            {
                return Vec::new();
            }
            reply_rx.recv().await.unwrap_or_default()
        }
        .boxed()
    }
}

pub(crate) async fn run_policy_actor(
    rx: Receiver<PolicyCommand>,
    keyhive: Arc<AsyncMutex<CliKeyhive>>,
) {
    while let Ok(cmd) = rx.recv().await {
        match cmd {
            PolicyCommand::AuthorizeConnect { peer, reply } => {
                let result = {
                    let locked_keyhive = keyhive.lock().await;
                    authorize_connect_with(&*locked_keyhive, peer).await
                };
                reply.send(result).await.ok();
            }
            PolicyCommand::AuthorizeFetch {
                peer,
                sedimentree_id,
                reply,
            } => {
                let result = {
                    let locked_keyhive = keyhive.lock().await;
                    authorize_fetch_with(&*locked_keyhive, peer, sedimentree_id).await
                };
                reply.send(result).await.ok();
            }
            PolicyCommand::AuthorizePut {
                requestor,
                author,
                sedimentree_id,
                reply,
            } => {
                let result = {
                    let locked_keyhive = keyhive.lock().await;
                    authorize_put_with(&*locked_keyhive, requestor, author, sedimentree_id).await
                };
                reply.send(result).await.ok();
            }
            PolicyCommand::FilterAuthorizedFetch { peer, ids, reply } => {
                let result = {
                    let locked_keyhive = keyhive.lock().await;
                    filter_authorized_fetch_with(&*locked_keyhive, peer, ids).await
                };
                reply.send(result).await.ok();
            }
        }
    }

    tracing::debug!("keyhive policy actor shutting down (channel closed)");
}

/// Closure that relay-fetches a legacy doc from connected peers. Stored in a
/// `OnceLock` and installed after `build_composed` returns `Arc<Subduction>`
/// because the policy is consumed by the builder before `Subduction` exists.
/// The closure captures `Weak<Subduction>` to avoid a reference cycle.
pub(crate) type RelayDriver = Arc<dyn Fn(SedimentreeId) -> BoxFuture<'static, ()> + Send + Sync>;

/// Wraps [`CliKeyhivePolicyHandle`] to handle legacy (zero-padded) doc IDs
/// outside keyhive policy. Legacy fetches are allowed. Legacy puts are allowed
/// only if the doc was previously fetched.
pub(crate) struct LegacyRelayPolicy {
    inner: CliKeyhivePolicyHandle,
    requested_legacy_docs: std::sync::Mutex<HashSet<SedimentreeId>>,
    relay_driver: OnceLock<RelayDriver>,
    relay_initiated: Arc<std::sync::Mutex<HashSet<SedimentreeId>>>,
}

#[allow(clippy::missing_fields_in_debug)]
impl core::fmt::Debug for LegacyRelayPolicy {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LegacyRelayPolicy")
            .field("requested_legacy_docs", &self.requested_legacy_docs)
            .field("relay_initiated", &self.relay_initiated)
            .field("relay_driver_installed", &self.relay_driver.get().is_some())
            .finish()
    }
}

impl LegacyRelayPolicy {
    pub(crate) fn new(inner: CliKeyhivePolicyHandle) -> Self {
        Self {
            inner,
            requested_legacy_docs: std::sync::Mutex::new(HashSet::new()),
            relay_driver: OnceLock::new(),
            relay_initiated: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }

    pub(crate) fn install_relay_driver(&self, driver: RelayDriver) {
        if self.relay_driver.set(driver).is_err() {
            tracing::warn!("relay driver installed twice; second call ignored");
        }
    }

    fn lock_requested(&self) -> std::sync::MutexGuard<'_, HashSet<SedimentreeId>> {
        self.requested_legacy_docs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_initiated(&self) -> std::sync::MutexGuard<'_, HashSet<SedimentreeId>> {
        self.relay_initiated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn relay_fetch(&self, id: SedimentreeId) {
        let Some(driver) = self.relay_driver.get() else {
            tracing::warn!(
                ?id,
                "legacy fetch before relay driver installed; doc will only sync if a peer pushes",
            );
            return;
        };
        if self.lock_initiated().insert(id) {
            let driver = Arc::clone(driver);
            let initiated = Arc::clone(&self.relay_initiated);
            tokio::spawn(async move {
                driver(id).await;
                initiated
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&id);
            });
        }
    }

    fn is_legacy(id: &SedimentreeId) -> bool {
        id.as_bytes()[16..].iter().all(|&b| b == 0)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LegacyPutDisallowed {
    #[error(transparent)]
    Inner(#[from] CliPolicyPutDisallowed),
    #[error("put for legacy doc that was not previously requested via fetch")]
    UnrequestedLegacyDoc,
}

impl ConnectionPolicy<future_form::Sendable> for LegacyRelayPolicy {
    type ConnectionDisallowed = CliPolicyConnectDisallowed;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        self.inner.authorize_connect(peer)
    }
}

impl StoragePolicy<future_form::Sendable> for LegacyRelayPolicy {
    type FetchDisallowed = CliPolicyFetchDisallowed;
    type PutDisallowed = LegacyPutDisallowed;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        if Self::is_legacy(&sedimentree_id) {
            self.lock_requested().insert(sedimentree_id);
            self.relay_fetch(sedimentree_id);
            async move { Ok(()) }.boxed()
        } else {
            let fut = self.inner.authorize_fetch(peer, sedimentree_id);
            fut.boxed()
        }
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        if Self::is_legacy(&sedimentree_id) {
            let allowed = self.lock_requested().contains(&sedimentree_id);
            async move {
                if allowed {
                    Ok(())
                } else {
                    Err(LegacyPutDisallowed::UnrequestedLegacyDoc)
                }
            }
            .boxed()
        } else {
            let fut = self.inner.authorize_put(requestor, author, sedimentree_id);
            async move { fut.await.map_err(LegacyPutDisallowed::Inner) }.boxed()
        }
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        let (legacy, keyhive_ids): (Vec<_>, Vec<_>) = ids.into_iter().partition(Self::is_legacy);

        // Only allow legacy docs that a downstream peer previously
        // requested via authorize_fetch.
        let legacy_allowed: Vec<_> = {
            let requested = self.lock_requested();
            legacy
                .into_iter()
                .filter(|id| requested.contains(id))
                .collect()
        };

        if keyhive_ids.is_empty() {
            return async move { legacy_allowed }.boxed();
        }

        let inner_fut = self.inner.filter_authorized_fetch(peer, keyhive_ids);
        async move {
            let mut allowed = inner_fut.await;
            allowed.extend(legacy_allowed);
            allowed
        }
        .boxed()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn legacy_id(seed: u8) -> SedimentreeId {
        let mut bytes = [0u8; 32];
        bytes[..16].fill(seed);
        SedimentreeId::new(bytes)
    }

    fn keyhive_id(seed: u8) -> SedimentreeId {
        SedimentreeId::new([seed; 32])
    }

    fn test_peer_id() -> PeerId {
        PeerId::new([0xAB; 32])
    }

    fn test_policy() -> LegacyRelayPolicy {
        let (handle, _rx) = CliKeyhivePolicyHandle::channel();
        LegacyRelayPolicy::new(handle)
    }

    fn recording_driver() -> (RelayDriver, Arc<std::sync::Mutex<Vec<SedimentreeId>>>) {
        let calls: Arc<std::sync::Mutex<Vec<SedimentreeId>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls_clone = Arc::clone(&calls);
        let driver: RelayDriver = Arc::new(move |id| {
            let calls = Arc::clone(&calls_clone);
            async move {
                calls.lock().unwrap().push(id);
            }
            .boxed()
        });
        (driver, calls)
    }

    async fn drain_spawned() {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    #[test]
    fn classify_keyhive_id_is_not_legacy() {
        let id = SedimentreeId::new([7u8; 32]);
        assert!(!LegacyRelayPolicy::is_legacy(&id));
    }

    #[test]
    fn classify_legacy_padded_id_is_legacy() {
        let mut bytes = [0u8; 32];
        bytes[..16].fill(0xAB);
        let id = SedimentreeId::new(bytes);
        assert!(LegacyRelayPolicy::is_legacy(&id));
    }

    #[test]
    fn classify_all_zero_id_is_legacy() {
        let id = SedimentreeId::new([0u8; 32]);
        assert!(LegacyRelayPolicy::is_legacy(&id));
    }

    #[test]
    fn classify_id_with_any_trailing_nonzero_is_not_legacy() {
        let mut bytes = [0u8; 32];
        bytes[31] = 1;
        let id = SedimentreeId::new(bytes);
        assert!(!LegacyRelayPolicy::is_legacy(&id));
    }

    #[tokio::test]
    async fn authorize_fetch_legacy_without_driver_records_only() {
        let policy = test_policy();
        let id = legacy_id(1);

        policy
            .authorize_fetch(test_peer_id(), id)
            .await
            .expect("legacy fetch should be allowed");

        assert!(policy.requested_legacy_docs.lock().unwrap().contains(&id));
        assert!(
            policy.relay_initiated.lock().unwrap().is_empty(),
            "no driver installed → relay_initiated stays empty",
        );
    }

    #[tokio::test]
    async fn authorize_fetch_legacy_with_driver_kicks_once_per_id() {
        let policy = test_policy();
        let (driver, calls) = recording_driver();
        policy.install_relay_driver(driver);

        let id_x = legacy_id(1);
        let id_y = legacy_id(2);

        policy.authorize_fetch(test_peer_id(), id_x).await.unwrap();
        policy.authorize_fetch(test_peer_id(), id_x).await.unwrap();
        policy.authorize_fetch(test_peer_id(), id_y).await.unwrap();

        drain_spawned().await;

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2, "got {:?}", *calls);
        assert!(calls.contains(&id_x));
        assert!(calls.contains(&id_y));

        assert!(
            policy.relay_initiated.lock().unwrap().is_empty(),
            "completed relays should be cleared from relay_initiated",
        );
    }

    #[tokio::test]
    async fn completed_relay_can_be_retried() {
        let policy = test_policy();
        let (driver, calls) = recording_driver();
        policy.install_relay_driver(driver);

        let id = legacy_id(1);

        policy.authorize_fetch(test_peer_id(), id).await.unwrap();
        drain_spawned().await;
        assert_eq!(calls.lock().unwrap().len(), 1);

        // After the relay completes, a new fetch should re-trigger it.
        policy.authorize_fetch(test_peer_id(), id).await.unwrap();
        drain_spawned().await;
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn filter_authorized_fetch_only_allows_previously_requested_legacy_docs() {
        let policy = test_policy();
        let (driver, _calls) = recording_driver();
        policy.install_relay_driver(driver);

        let l1 = legacy_id(1);
        let l2 = legacy_id(2);
        let k1 = keyhive_id(0xCC);

        // Only l1 was previously requested by a downstream peer.
        policy.authorize_fetch(test_peer_id(), l1).await.unwrap();

        let allowed = policy
            .filter_authorized_fetch(test_peer_id(), vec![l1, l2, k1])
            .await;

        assert!(allowed.contains(&l1), "requested legacy doc should pass");
        assert!(
            !allowed.contains(&l2),
            "unrequested legacy doc should be filtered out",
        );
        // k1 goes to the inner policy (which has no actor, so it
        // returns empty). Only legacy behaviour is tested here.
    }

    #[tokio::test]
    async fn filter_authorized_fetch_does_not_register_new_legacy_docs() {
        let policy = test_policy();

        let l1 = legacy_id(1);

        drop(
            policy
                .filter_authorized_fetch(test_peer_id(), vec![l1])
                .await,
        );

        assert!(
            !policy.requested_legacy_docs.lock().unwrap().contains(&l1),
            "filter_authorized_fetch must not add unrequested docs to the set",
        );
    }

    #[test]
    fn install_relay_driver_twice_is_idempotent() {
        let policy = test_policy();
        let driver1: RelayDriver = Arc::new(|_| async {}.boxed());
        let driver2: RelayDriver = Arc::new(|_| async {}.boxed());

        policy.install_relay_driver(driver1);
        policy.install_relay_driver(driver2); // must not panic

        assert!(
            policy.relay_driver.get().is_some(),
            "first driver should remain installed",
        );
    }
}
