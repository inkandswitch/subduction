//! Manage connections to peers in the network.

pub mod actor;
pub mod id;
pub mod message;
pub mod stream;

#[cfg(feature = "test_utils")]
pub mod test_utils;

use alloc::sync::Arc;
use core::{marker::PhantomData, time::Duration};
use sedimentree_core::{
    blob::Blob, fragment::Fragment, id::SedimentreeId, loose_commit::LooseCommit, storage::Storage,
};

use self::message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId};
use crate::{crypto::verified::Verified, peer::id::PeerId};
use futures::Future;
use futures_kind::FutureKind;
use thiserror::Error;

/// A trait representing a connection to a peer in the network.
///
/// It is assumed that a [`Connection`] is authenticated to a particular peer.
/// Encrypting this channel is also strongly recommended.
pub trait Connection<K: FutureKind + ?Sized>: Clone {
    /// A problem when gracefully disconnecting.
    type DisconnectionError: core::error::Error;

    /// A problem when sending a message.
    type SendError: core::error::Error;

    /// A problem when receiving a message.
    type RecvError: core::error::Error;

    /// A problem with a roundtrip call.
    type CallError: core::error::Error;

    /// The peer ID of the remote peer.
    fn peer_id(&self) -> PeerId;

    /// Disconnect from the peer gracefully.
    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>>;

    /// Send a message.
    fn send(&self, message: &Message) -> K::Future<'_, Result<(), Self::SendError>>;

    /// Receive a message.
    fn recv(&self) -> K::Future<'_, Result<Message, Self::RecvError>>;

    /// Get the next request ID e.g. for a [`call`].
    fn next_request_id(&self) -> K::Future<'_, RequestId>;

    /// Make a synchronous call to the peer, expecting a response.
    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> K::Future<'_, Result<BatchSyncResponse, Self::CallError>>;
}

impl<T: Connection<K>, K: FutureKind> Connection<K> for Arc<T> {
    type DisconnectionError = T::DisconnectionError;
    type SendError = T::SendError;
    type RecvError = T::RecvError;
    type CallError = T::CallError;

    fn peer_id(&self) -> PeerId {
        T::peer_id(self)
    }

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        T::disconnect(self)
    }

    fn send(&self, message: &Message) -> K::Future<'_, Result<(), Self::SendError>> {
        T::send(self, message)
    }

    fn recv(&self) -> K::Future<'_, Result<Message, Self::RecvError>> {
        T::recv(self)
    }

    fn next_request_id(&self) -> K::Future<'_, RequestId> {
        T::next_request_id(self)
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> K::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        T::call(self, req, timeout)
    }
}

/// A trait for connections that can be re-established if they drop.
pub trait Reconnect<K: FutureKind>: Connection<K> {
    /// A problem when creating the connection.
    type ConnectError: core::error::Error;

    /// Setup the connection, but don't run it.
    fn reconnect(&mut self) -> K::Future<'_, Result<(), Self::ConnectError>>;
}

/// A policy for allowing or disallowing connections from peers.
pub trait ConnectionPolicy<K: FutureKind> {
    /// Check if a connection from the given peer is allowed.
    ///
    /// # Errors
    ///
    /// * Returns [`ConnectionDisallowed`] if the connection is not allowed.
    fn check_connect(&self, peer: &PeerId) -> K::Future<'_, bool>;
}

pub trait FetchPutPolicy<K: FutureKind> {
    fn check_fetch(&self, peer: PeerId, sedimentree_id: SedimentreeId) -> K::Future<'_, bool>;
}

pub trait PutPolicy<K: FutureKind> {
    fn check_put(
        &self,
        putter: PeerId,
        issuer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, bool>;

    fn put_loose_commit_cap<F: FutureKind, S: Storage<F>>(
        &self,
        storage: S,
        putter: PeerId,
        verified: Verified<LooseCommitMessage>,
        // FIXME needed for blob? sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<PutHandle<S>, FIXME>> {
        async {
            if self.check_put(putter, verified.issuer(), verified).await {
                Ok(PutHandle {
                    commit: verified.payload,
                    storage,
                })
            } else {
                // FIXME error type
                Err(FIXME)
            }
        }
    }
}

// FIXME rename "connecter" / "puller" / etc?
// FIXME rename "connecthandle" / "pull handle" / etc?
pub trait ConnectHandle {
    fn connect(&self, peer_id: PeerId) -> FIXME;
}

pub trait FetchHandle {
    fn pull(&self) -> FIXME;
    fn sedimentree_id(&self) -> &SedimentreeId;
}

pub struct PutHandle<F: FutureKind, S: Storage<F>> {
    item: PutItem,
    storage: S,
    _phantom: PhantomData<F>,
}

impl<F: FutureKind, S: Storage<F>> PutHandle<F, S> {
    // NOTE: consumes!
    pub async fn put(&self) -> Result<(), FIXME> {
        match &self.item {
            PutItem::LooseCommit { id, commit, blob } => {
                self.storage.save_blob(blob.clone()).await.expect("FIXME");
                self.storage
                    .save_loose_commit(*id, commit.clone())
                    .await
                    .expect("FIXME");
            }
            PutItem::Fragment { id, fragment, blob } => {
                self.storage.save_blob(blob.clone()).await.expect("FIXME");
                self.storage
                    .save_fragment(*id, fragment.clone())
                    .await
                    .expect("FIXME");
            }
            PutItem::Blob { id, blob } => {
                self.storage.save_blob(blob.clone()).await.expect("FIXME");
            }
        }

        Ok(())
    }

    // FIXME other constructors
}

pub enum PutItem {
    LooseCommit {
        /// The ID of the [`Sedimentree`] that this commit belongs to.
        id: SedimentreeId,

        /// The [`LooseCommit`] being sent.
        commit: LooseCommit,

        /// The [`Blob`] containing the commit data.
        blob: Blob,
    },

    /// A single fragment being sent for a particular [`Sedimentree`].
    Fragment {
        /// The ID of the [`Sedimentree`] that this fragment belongs to.
        id: SedimentreeId,

        /// The [`Fragment`] being sent.
        fragment: Fragment,

        /// The [`Blob`] containing the fragment data.
        blob: Blob,
    },

    Blob {
        /// The ID of the [`Sedimentree`] that this blob belongs to.
        id: SedimentreeId,

        /// The [`Blob`] being sent.
        blob: Blob,
    },
}

/// An error indicating that a connection is disallowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[error("Connection disallowed")]
pub struct ConnectionDisallowed;

pub struct FIXME;

// FIXME something something
mod policy_seal {
    pub struct PolicySeal;
}
