//! A stream wrapper that records inbound reads, so the keepalive can tell a
//! peer that is busy sending to us from a dead one.
//!
//! # Why this exists
//!
//! tungstenite keeps a partial frame in its own buffer and surfaces nothing
//! until the frame is complete. A peer uploading one large message therefore
//! looks silent from above, and its pong to our ping waits behind that frame
//! on the wire. Wrapping the socket _under_ tungstenite lets the keepalive see
//! bytes as they arrive:
//!
//! ```text
//! socket ──▶ Counted ──▶ tungstenite ──▶ WebSocket ──▶ keepalive
//!               │                                         ▲
//!               └──────────── InboundProgress ────────────┘
//! ```
//!
//! Wrap the socket before the WebSocket upgrade, then pass
//! [`Counted::progress`] to
//! [`WebSocket::new_with_keepalive`](crate::websocket::WebSocket::new_with_keepalive):
//!
//! ```ignore
//! let tcp = Counted::new(tcp);
//! let inbound = tcp.progress();
//! let ws = accept_hdr_async_with_config(tcp, callback, Some(config)).await?;
//! let (ws, sender, keepalive) =
//!     WebSocket::new_with_keepalive(ws, inbound, peer_id, KeepAlive::balanced(), TokioSleeper);
//! ```
//!
//! # Cost
//!
//! The count goes up once per read that returns data, not once per byte:
//! one relaxed atomic add beside each socket read, and one shared counter per
//! connection.

use alloc::sync::Arc;
use core::{
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};

use futures_util::{
    AsyncRead, AsyncWrite,
    io::{self, IoSlice, IoSliceMut},
};

/// A stream that records each read that returns data.
///
/// Implements the `futures` I/O traits, and with the `tokio_base` feature the
/// `tokio` ones, so it can sit directly on a `TcpStream` or under any adapter.
#[derive(Debug)]
pub struct Counted<S> {
    inner: S,
    reads: InboundProgress,
}

impl<S> Counted<S> {
    /// Wrap `inner`, counting from zero.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            reads: InboundProgress::new(),
        }
    }

    /// A handle to this stream's inbound progress, for a keepalive to watch.
    #[must_use]
    pub fn progress(&self) -> InboundProgress {
        self.reads.clone()
    }

    /// The wrapped stream.
    #[must_use]
    pub const fn get_ref(&self) -> &S {
        &self.inner
    }

    /// The wrapped stream, mutably. Reads through it are not counted.
    pub const fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Unwrap the stream.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

/// Evidence that bytes are arriving on one connection.
///
/// Outside this crate, only [`Counted::progress`] produces one, so a
/// keepalive that requires it always watches a real stream.
#[derive(Debug, Clone)]
pub struct InboundProgress(Arc<AtomicU64>);

impl InboundProgress {
    /// A counter that no stream feeds, for a connection without keepalive.
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    /// The number of reads that returned data so far. Only compared against
    /// an earlier value from the same handle.
    pub(crate) fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Record one read that returned data.
    pub(crate) fn record(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Counted<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        if matches!(poll, Poll::Ready(Ok(n)) if n > 0) {
            this.reads.record();
        }
        poll
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_read_vectored(cx, bufs);
        if matches!(poll, Poll::Ready(Ok(n)) if n > 0) {
            this.reads.record();
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Counted<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_close(cx)
    }
}

#[cfg(feature = "tokio_base")]
impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Counted<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        if matches!(poll, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            this.reads.record();
        }
        poll
    }
}

#[cfg(feature = "tokio_base")]
impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Counted<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use futures::{AsyncReadExt, AsyncWriteExt, io::Cursor};
    use testresult::TestResult;

    use super::Counted;

    #[tokio::test]
    async fn counts_reads_that_return_data_but_not_eof() -> TestResult {
        let mut stream = Counted::new(Cursor::new(vec![1u8, 2, 3, 4, 5]));
        let progress = stream.progress();
        let mut buf = [0u8; 2];

        assert_eq!(stream.read(&mut buf).await?, 2);
        assert_eq!(stream.read(&mut buf).await?, 2);
        assert_eq!(progress.load(), 2);

        assert_eq!(stream.read(&mut buf).await?, 1);
        assert_eq!(stream.read(&mut buf).await?, 0, "EOF");
        assert_eq!(progress.load(), 3, "EOF is not progress");
        Ok(())
    }

    #[tokio::test]
    async fn writes_pass_through_and_are_not_counted() -> TestResult {
        let mut stream = Counted::new(Cursor::new(Vec::new()));
        let progress = stream.progress();

        stream.write_all(b"hello").await?;
        stream.flush().await?;

        assert_eq!(progress.load(), 0);
        assert_eq!(stream.into_inner().into_inner(), b"hello");
        Ok(())
    }
}
