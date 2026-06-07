//! Background tasks for reading from and writing to QUIC streams.
//!
//! Both tasks are spawned externally (by the client or server). They bridge
//! the QUIC stream halves to the connection's internal async channels.

use alloc::vec::Vec;

use iroh::endpoint::{ReadError, ReadExactError, RecvStream, SendStream};

use crate::{
    error::{RunError, StreamError},
    transport::IrohTransport,
};

/// Length-prefix size (4 bytes, big-endian u32).
const LENGTH_PREFIX_SIZE: usize = 4;

/// Maximum frame size (50 MiB).
///
/// Matches `DEFAULT_MAX_MESSAGE_SIZE` / `DEFAULT_MAX_BODY_SIZE` in the
/// WebSocket and HTTP long-poll transports.
const MAX_FRAME_SIZE: usize = 50 * 1024 * 1024;

/// Read a length-prefixed message from a QUIC recv stream.
///
/// Returns [`StreamError::FrameTooLarge`] if the peer sends a length
/// prefix exceeding [`MAX_FRAME_SIZE`], preventing OOM from a
/// malicious or buggy peer.
pub(crate) async fn read_framed(recv: &mut RecvStream) -> Result<Vec<u8>, StreamError> {
    let mut len_buf = [0u8; LENGTH_PREFIX_SIZE];
    recv.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_FRAME_SIZE {
        return Err(StreamError::FrameTooLarge {
            actual: len,
            max: MAX_FRAME_SIZE,
        });
    }

    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write a length-prefixed message to a QUIC send stream.
///
/// Returns [`StreamError::FrameTooLarge`] if `data` exceeds
/// [`MAX_FRAME_SIZE`].
pub(crate) async fn write_framed(send: &mut SendStream, data: &[u8]) -> Result<(), StreamError> {
    if data.len() > MAX_FRAME_SIZE {
        return Err(StreamError::FrameTooLarge {
            actual: data.len(),
            max: MAX_FRAME_SIZE,
        });
    }

    // SAFETY: length fits in u32 because MAX_FRAME_SIZE (50 MiB) < u32::MAX (4 GiB).
    #[allow(clippy::cast_possible_truncation)]
    let len = data.len() as u32;
    send.write_all(&len.to_be_bytes())
        .await
        .map_err(StreamError::Write)?;
    send.write_all(data).await.map_err(StreamError::Write)?;
    Ok(())
}

/// How a read error from the QUIC stream should be treated by the listener.
///
/// The log severity and the peer-visible close code both follow from the
/// category, mirroring the WebSocket transport's error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamReadErrorKind {
    /// A benign disconnect: the peer closed the connection or finished the
    /// stream. Normal lifecycle.
    ExpectedDisconnect,

    /// Peer-induced: the peer announced a frame larger than the configured cap.
    /// Not a fault on this side, so it is logged at warn rather than error.
    OverCapacity,

    /// An unanticipated transport fault.
    Fatal,
}

impl StreamReadErrorKind {
    const fn classify(e: &StreamError) -> Self {
        match e {
            // Peer-driven terminations (graceful or abrupt) are benign: the
            // connection was lost, the stream finished, the peer reset it
            // (e.g. crash / SIGKILL surfacing as RESET_STREAM), or the stream
            // is already closed. None of these is a local fault.
            StreamError::Read(
                ReadExactError::ReadError(
                    ReadError::ConnectionLost(_) | ReadError::Reset(_) | ReadError::ClosedStream,
                )
                | ReadExactError::FinishedEarly(_),
            )
            | StreamError::ConnectionClosed(_) => Self::ExpectedDisconnect,

            StreamError::FrameTooLarge { .. } => Self::OverCapacity,

            StreamError::Read(_) | StreamError::Write(_) => Self::Fatal,
        }
    }

    /// QUIC application error code to close with. The numeric values match the
    /// WebSocket Close codes (1009 Message Too Big, 1011 Internal Error) for
    /// cross-transport consistency; `0` is the normal-close code.
    fn close_code(self) -> iroh::endpoint::VarInt {
        let code: u32 = match self {
            Self::ExpectedDisconnect => 0,
            Self::OverCapacity => 1009,
            Self::Fatal => 1011,
        };
        code.into()
    }

    const fn close_reason(self) -> &'static [u8] {
        match self {
            Self::ExpectedDisconnect => b"subduction close",
            Self::OverCapacity => b"frame exceeds size limit",
            Self::Fatal => b"internal error",
        }
    }
}

/// Background task: reads framed messages from the QUIC recv stream and
/// dispatches them to the connection's inbound channel as raw bytes.
///
/// On any exit — peer close, EOF, over-cap frame, or fatal error — the
/// connection is torn down: the inbound channel is closed so a parked
/// `recv_bytes` is notified immediately, and the QUIC connection is closed with
/// an application error code reflecting the reason.
///
/// # Errors
///
/// Returns an error if reading from the stream or dispatching fails.
pub async fn listener_task(conn: IrohTransport, mut recv: RecvStream) -> Result<(), RunError> {
    let peer_id = conn.quic_connection().remote_id();
    tracing::info!(peer = %peer_id, "starting iroh listener task");

    let outcome = read_loop(&conn, &mut recv, peer_id).await;

    // Uniform teardown: close the inbound channel (notifying any parked
    // `recv_bytes`) and the QUIC connection with a code reflecting the outcome.
    let (code, reason) = match &outcome {
        Ok(()) => (
            StreamReadErrorKind::ExpectedDisconnect.close_code(),
            StreamReadErrorKind::ExpectedDisconnect.close_reason(),
        ),
        Err(e) => {
            let kind = match e {
                RunError::Stream(stream_err) => StreamReadErrorKind::classify(stream_err),
                // A closed inbound channel means the consumer is gone; treat as
                // a normal teardown.
                RunError::ChanSend(_) => StreamReadErrorKind::ExpectedDisconnect,
            };
            (kind.close_code(), kind.close_reason())
        }
    };
    conn.close_with_code(code, reason);

    tracing::info!(peer = %peer_id, "iroh listener task exiting");
    outcome
}

/// Read framed messages until the stream ends or errors, forwarding each to the
/// inbound channel. Logs read errors at a severity matching their category.
async fn read_loop(
    conn: &IrohTransport,
    recv: &mut RecvStream,
    peer_id: impl core::fmt::Display,
) -> Result<(), RunError> {
    loop {
        let bytes = match read_framed(recv).await {
            Ok(b) => b,
            Err(e) => {
                match StreamReadErrorKind::classify(&e) {
                    StreamReadErrorKind::ExpectedDisconnect => {
                        tracing::debug!(peer = %peer_id, error = %e, "iroh connection closed");
                        return Ok(());
                    }
                    StreamReadErrorKind::OverCapacity => {
                        tracing::warn!(
                            peer = %peer_id,
                            error = %e,
                            "peer sent an over-capacity frame; closing connection"
                        );
                    }
                    StreamReadErrorKind::Fatal => {
                        tracing::error!(peer = %peer_id, error = %e, "iroh read error");
                    }
                }
                return Err(e.into());
            }
        };

        tracing::debug!(peer = %peer_id, bytes = bytes.len(), "received inbound bytes");

        conn.push_inbound(bytes)
            .await
            .map_err(|e| RunError::ChanSend(Box::new(e)))?;
    }
}

/// Background task: drains the outbound channel and writes framed messages
/// to the QUIC send stream.
///
/// Exits when the outbound channel is closed.
///
/// # Errors
///
/// Returns an error if writing to the stream fails.
pub async fn sender_task(
    mut send: SendStream,
    outbound_rx: async_channel::Receiver<Vec<u8>>,
) -> Result<(), RunError> {
    tracing::info!("starting iroh sender task");

    while let Ok(bytes) = outbound_rx.recv().await {
        write_framed(&mut send, &bytes).await?;
    }

    tracing::info!("sender task: outbound channel closed, shutting down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use iroh::endpoint::{ReadError, ReadExactError};

    use super::{StreamError, StreamReadErrorKind};

    /// An over-cap frame classifies as `OverCapacity` and closes with the
    /// 1009 (Message Too Big) application code.
    #[test]
    fn over_cap_classifies_and_closes_1009() {
        let err = StreamError::FrameTooLarge {
            actual: 100,
            max: 10,
        };
        let kind = StreamReadErrorKind::classify(&err);
        assert_eq!(kind, StreamReadErrorKind::OverCapacity);

        let code: u64 = kind.close_code().into();
        assert_eq!(code, 1009);
        assert!(!kind.close_reason().is_empty());
    }

    /// Peer-driven stream terminations (reset, already-closed) are benign
    /// disconnects, not fatal errors.
    #[test]
    fn peer_stream_termination_is_expected_disconnect() {
        let cases = [
            StreamError::Read(ReadExactError::ReadError(ReadError::Reset(7u32.into()))),
            StreamError::Read(ReadExactError::ReadError(ReadError::ClosedStream)),
        ];
        for err in cases {
            assert_eq!(
                StreamReadErrorKind::classify(&err),
                StreamReadErrorKind::ExpectedDisconnect,
                "{err:?} should be a benign disconnect"
            );
        }
    }

    /// Close-code mapping is stable across all kinds.
    #[test]
    fn close_code_mapping() {
        let cases = [
            (StreamReadErrorKind::ExpectedDisconnect, 0u64),
            (StreamReadErrorKind::OverCapacity, 1009),
            (StreamReadErrorKind::Fatal, 1011),
        ];
        for (kind, expected) in cases {
            let code: u64 = kind.close_code().into();
            assert_eq!(code, expected, "{kind:?} should close with {expected}");
        }
    }
}
