//! Error types.

use futures::channel::oneshot;
use subduction_core::connection::message::Message;
use thiserror::Error;

/// Problem while attempting to send a message.
#[derive(Debug, Error)]
pub enum SendError {
    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),
}

/// Problem while attempting to make a roundtrip call.
#[derive(Debug, Error)]
pub enum CallError {
    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Problem receiving on the internal channel.
    #[error("Channel canceled")]
    ChanCanceled(oneshot::Canceled),

    /// Timed out waiting for response.
    #[error("Timed out waiting for response")]
    Timeout,
}

/// Problem while attempting to receive a message.
#[derive(Debug, Clone, Copy, Error)]
pub enum RecvError {
    /// Problem receiving on the internal channel.
    #[error("Channel receive error")]
    ChanCanceled(oneshot::Canceled),

    /// Attempted to read from a closed channel.
    #[error("Attempted to read from closed channel")]
    ReadFromClosed,
}

/// Problem while attempting to gracefully disconnect.
#[derive(Debug, Clone, Copy, Error)]
#[error("Disconnected")]
pub struct DisconnectionError;

/// Errors while running the connection loop.
#[derive(Debug, Error)]
pub enum RunError {
    /// Internal MPSC channel error.
    #[error("Channel send error: {0}")]
    ChanSend(async_channel::SendError<Message>),

    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Deserialization error.
    #[error("Deserialize error")]
    Deserialize,
}

#[cfg(test)]
mod tests {
    use super::*;

    mod send_error {
        use super::*;

        #[test]
        fn test_display_includes_websocket() {
            let err = SendError::WebSocket(tungstenite::Error::ConnectionClosed);
            let display = format!("{}", err);
            assert!(display.contains("WebSocket error"));
        }

        #[test]
        fn test_from_tungstenite_error() {
            let tungstenite_err = tungstenite::Error::ConnectionClosed;
            let send_err: SendError = tungstenite_err.into();
            assert!(matches!(send_err, SendError::WebSocket(_)));
        }

        #[test]
        fn test_debug_output() {
            let err = SendError::WebSocket(tungstenite::Error::ConnectionClosed);
            let debug = format!("{:?}", err);
            assert!(debug.contains("WebSocket"));
        }
    }

    mod call_error {
        use super::*;

        #[test]
        fn test_timeout_display() {
            let err = CallError::Timeout;
            assert_eq!(format!("{}", err), "Timed out waiting for response");
        }

        #[test]
        fn test_chan_canceled_display() {
            let (_tx, rx) = oneshot::channel::<()>();
            drop(rx);
            let err = CallError::ChanCanceled(oneshot::Canceled);
            let display = format!("{}", err);
            assert!(display.contains("Channel canceled"));
        }

        #[test]
        fn test_websocket_display() {
            let err = CallError::WebSocket(tungstenite::Error::ConnectionClosed);
            let display = format!("{}", err);
            assert!(display.contains("WebSocket error"));
        }

        #[test]
        fn test_from_tungstenite_error() {
            let tungstenite_err = tungstenite::Error::ConnectionClosed;
            let call_err: CallError = tungstenite_err.into();
            assert!(matches!(call_err, CallError::WebSocket(_)));
        }

        #[test]
        fn test_timeout_variant_matches() {
            let err = CallError::Timeout;
            assert!(matches!(err, CallError::Timeout));
        }

        #[test]
        fn test_chan_canceled_variant_matches() {
            let err = CallError::ChanCanceled(oneshot::Canceled);
            assert!(matches!(err, CallError::ChanCanceled(_)));
        }
    }

    mod recv_error {
        use super::*;

        #[test]
        fn test_read_from_closed_display() {
            let err = RecvError::ReadFromClosed;
            assert_eq!(
                format!("{}", err),
                "Attempted to read from closed channel"
            );
        }

        #[test]
        fn test_chan_canceled_display() {
            let err = RecvError::ChanCanceled(oneshot::Canceled);
            let display = format!("{}", err);
            assert!(display.contains("Channel receive error"));
        }

        #[test]
        fn test_recv_error_is_clone() {
            let err1 = RecvError::ReadFromClosed;
            let err2 = err1.clone();
            assert!(matches!(err2, RecvError::ReadFromClosed));
        }

        #[test]
        fn test_recv_error_is_copy() {
            let err1 = RecvError::ReadFromClosed;
            let err2 = err1;
            // Both should still be usable
            assert!(matches!(err1, RecvError::ReadFromClosed));
            assert!(matches!(err2, RecvError::ReadFromClosed));
        }

        #[test]
        fn test_chan_canceled_variant_is_copy() {
            let err1 = RecvError::ChanCanceled(oneshot::Canceled);
            let err2 = err1;
            assert!(matches!(err1, RecvError::ChanCanceled(_)));
            assert!(matches!(err2, RecvError::ChanCanceled(_)));
        }
    }

    mod disconnection_error {
        use super::*;

        #[test]
        fn test_display() {
            let err = DisconnectionError;
            assert_eq!(format!("{}", err), "Disconnected");
        }

        #[test]
        fn test_is_clone() {
            let err1 = DisconnectionError;
            let err2 = err1.clone();
            assert_eq!(format!("{}", err2), "Disconnected");
        }

        #[test]
        fn test_is_copy() {
            let err1 = DisconnectionError;
            let err2 = err1;
            // Both should still be usable
            assert_eq!(format!("{}", err1), "Disconnected");
            assert_eq!(format!("{}", err2), "Disconnected");
        }

        #[test]
        fn test_debug_output() {
            let err = DisconnectionError;
            let debug = format!("{:?}", err);
            assert!(debug.contains("DisconnectionError"));
        }
    }

    mod run_error {
        use super::*;

        #[test]
        fn test_deserialize_display() {
            let err = RunError::Deserialize;
            assert_eq!(format!("{}", err), "Deserialize error");
        }

        #[test]
        fn test_websocket_display() {
            let err = RunError::WebSocket(tungstenite::Error::ConnectionClosed);
            let display = format!("{}", err);
            assert!(display.contains("WebSocket error"));
        }

        #[test]
        fn test_from_tungstenite_error() {
            let tungstenite_err = tungstenite::Error::ConnectionClosed;
            let run_err: RunError = tungstenite_err.into();
            assert!(matches!(run_err, RunError::WebSocket(_)));
        }

        #[test]
        fn test_deserialize_variant_matches() {
            let err = RunError::Deserialize;
            assert!(matches!(err, RunError::Deserialize));
        }

        #[test]
        fn test_debug_output() {
            let err = RunError::Deserialize;
            let debug = format!("{:?}", err);
            assert!(debug.contains("Deserialize"));
        }
    }
}
