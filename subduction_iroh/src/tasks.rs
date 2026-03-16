//! Background tasks for reading from and writing to QUIC streams.
//!
//! Both tasks are spawned externally (by the client or server). They bridge
//! the QUIC stream halves to the connection's internal async channels.

use iroh::endpoint::{RecvStream, SendStream};
use sedimentree_core::codec::encode::Encode;

use crate::{
    connection::{ChannelMessage, IrohConnection},
    error::{RunError, StreamError},
};

/// Length-prefix size (4 bytes, big-endian u32).
const LENGTH_PREFIX_SIZE: usize = 4;

/// Read a length-prefixed message from a QUIC recv stream.
pub(crate) async fn read_framed(recv: &mut RecvStream) -> Result<Vec<u8>, StreamError> {
    let mut len_buf = [0u8; LENGTH_PREFIX_SIZE];
    recv.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write a length-prefixed message to a QUIC send stream.
pub(crate) async fn write_framed(send: &mut SendStream, data: &[u8]) -> Result<(), StreamError> {
    #[allow(clippy::expect_used)]
    let len: u32 = data
        .len()
        .try_into()
        .expect("message too large for u32 length prefix");
    send.write_all(&len.to_be_bytes())
        .await
        .map_err(StreamError::Write)?;
    send.write_all(data).await.map_err(StreamError::Write)?;
    Ok(())
}

/// Background task: reads framed messages from the QUIC recv stream and
/// dispatches them to the connection's inbound channel.
///
/// Exits when the recv stream is closed or an error occurs.
///
/// # Errors
///
/// Returns an error if reading from the stream or dispatching fails.
pub async fn listener_task<O: Send + Sync, M: ChannelMessage>(
    conn: IrohConnection<O, M>,
    mut recv: RecvStream,
) -> Result<(), RunError<M>> {
    let peer_id = conn.quic_connection().remote_id();
    tracing::info!("starting iroh listener task for peer {peer_id}");

    loop {
        let bytes = match read_framed(&mut recv).await {
            Ok(b) => b,
            Err(StreamError::Read(iroh::endpoint::ReadExactError::ReadError(
                iroh::endpoint::ReadError::ConnectionLost(e),
            ))) => {
                tracing::debug!("iroh connection closed: {e}");
                break;
            }
            Err(StreamError::Read(iroh::endpoint::ReadExactError::FinishedEarly(_))) => {
                tracing::debug!("iroh stream finished (peer closed)");
                break;
            }
            Err(e) => return Err(e.into()),
        };

        let msg = M::try_decode(&bytes)?;

        tracing::debug!("decoded inbound message from peer {peer_id}",);

        conn.push_inbound(msg)
            .await
            .map_err(|e| RunError::ChanSend(Box::new(e)))?;
    }

    tracing::info!("iroh listener task for peer {peer_id}: exiting");
    Ok(())
}

/// Background task: drains the outbound channel and writes framed messages
/// to the QUIC send stream.
///
/// Exits when the outbound channel is closed.
///
/// # Errors
///
/// Returns an error if writing to the stream fails.
pub async fn sender_task<M: Encode + core::fmt::Debug>(
    mut send: SendStream,
    outbound_rx: async_channel::Receiver<M>,
) -> Result<(), RunError<M>> {
    tracing::info!("starting iroh sender task");

    while let Ok(msg) = outbound_rx.recv().await {
        let bytes = msg.encode();
        write_framed(&mut send, &bytes).await?;
    }

    tracing::info!("sender task: outbound channel closed, shutting down");
    Ok(())
}
