//! Error types for the top-level `Subduction`.

use alloc::vec::Vec;

use futures_kind::FutureKind;
use sedimentree_core::{blob::Digest, storage::Storage};
use thiserror::Error;

use crate::connection::{Connection, ConnectionDisallowed};

/// An error indicating that a [`Sedimentree`] could not be hydrated from storage.
#[derive(Debug, Clone, Copy, Error)]
pub enum HydrationError<F: FutureKind, S: Storage<F>> {
    /// An error occurred while loading all sedimentree IDs.
    #[error("hydration error when loading all sedimentree IDs: {0}")]
    LoadAllIdsError(#[source] S::Error),

    /// An error occurred while loading loose commits.
    #[error("hydration error when loading loose commits: {0}")]
    LoadLooseCommitsError(#[source] S::Error),

    /// An error occurred while loading fragments.
    #[error("hydration error when loading fragments: {0}")]
    LoadFragmentsError(#[source] S::Error),
}

/// An error that can occur during I/O operations.
///
/// This covers storage and network connection errors.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Error)]
pub enum IoError<F: FutureKind + ?Sized, S: Storage<F>, C: Connection<F>> {
    /// An error occurred while using storage.
    #[error(transparent)]
    Storage(S::Error),

    /// An error occurred while sending data on the connection.
    #[error(transparent)]
    ConnSend(C::SendError),

    /// An error occurred while receiving data from the connection.
    #[error(transparent)]
    ConnRecv(C::RecvError),

    /// An error occurred during a roundtrip call on the connection.
    #[error(transparent)]
    ConnCall(C::CallError),

    /// The connection was disallowed by the [`ConnectionPolicy`] policy.
    #[error(transparent)]
    ConnPolicy(#[from] ConnectionDisallowed),

    /// An error occurred during registration of a new connection.
    #[error(transparent)]
    RegistrationError(#[from] RegistrationError),
}

/// An error that can occur while handling a blob request.
#[derive(Debug, Error)]
pub enum BlobRequestErr<F: FutureKind, S: Storage<F>, C: Connection<F>> {
    /// An IO error occurred while handling the blob request.
    #[error("IO error: {0}")]
    IoError(#[from] IoError<F, S, C>),

    /// Some requested blobs were missing locally.
    #[error("Missing blobs: {0:?}")]
    MissingBlobs(Vec<Digest>),
}

/// An error that can occur while handling a batch sync request.
#[derive(Debug, Error)]
pub enum ListenError<F: FutureKind + ?Sized, S: Storage<F>, C: Connection<F>> {
    /// An IO error occurred while handling the batch sync request.
    #[error(transparent)]
    IoError(#[from] IoError<F, S, C>),

    /// Missing blobs associated with local fragments or commits.
    #[error("Missing blobs associated to local fragments & commits: {0:?}")]
    MissingBlobs(Vec<Digest>),

    /// Tried to send a message to a closed channel.
    #[error("tried to send to closed channel")]
    TrySendError,
}

/// An error that can occur during registration of a new connection.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub enum RegistrationError {
    /// The connection was disallowed by the [`ConnectionPolicy`].
    #[error(transparent)]
    ConnectionDisallowed(#[from] ConnectionDisallowed),

    /// Tried to send a message to a closed channel.
    #[error("tried to send to closed channel")]
    SendToClosedChannel,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;

    mod registration_error {
        use super::*;

        #[test]
        fn test_send_to_closed_channel_display() {
            let err = RegistrationError::SendToClosedChannel;
            let display = format!("{err}");
            assert_eq!(display, "tried to send to closed channel");
        }

        #[test]
        fn test_connection_disallowed_display() {
            let err = RegistrationError::ConnectionDisallowed(ConnectionDisallowed);
            let display = format!("{err}");
            assert_eq!(display, "Connection disallowed");
        }

        #[test]
        fn test_from_connection_disallowed() {
            let conn_disallowed = ConnectionDisallowed;
            let reg_err: RegistrationError = conn_disallowed.into();
            assert_eq!(
                reg_err,
                RegistrationError::ConnectionDisallowed(ConnectionDisallowed)
            );
        }

        #[test]
        fn test_equality() {
            let err1 = RegistrationError::SendToClosedChannel;
            let err2 = RegistrationError::SendToClosedChannel;
            let err3 = RegistrationError::ConnectionDisallowed(ConnectionDisallowed);

            assert_eq!(err1, err2);
            assert_ne!(err1, err3);
        }

        #[test]
        fn test_clone() {
            let err1 = RegistrationError::SendToClosedChannel;
            let err2 = err1;
            assert_eq!(err1, err2);
        }
    }

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_equality_is_reflexive() {
            bolero::check!()
                .with_type::<RegistrationError>()
                .for_each(|err| {
                    assert_eq!(err, err);
                });
        }

        #[test]
        fn prop_clone_equals_original() {
            bolero::check!()
                .with_type::<RegistrationError>()
                .for_each(|err| {
                    assert_eq!(err.clone(), *err);
                });
        }

        #[test]
        fn prop_display_produces_non_empty_string() {
            bolero::check!()
                .with_type::<RegistrationError>()
                .for_each(|err| {
                    let display = format!("{err}");
                    assert!(!display.is_empty());
                });
        }
    }
}
