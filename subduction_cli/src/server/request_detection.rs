//! Telling what a new connection is from its first bytes.
//!
//! The server accepts WebSocket and HTTP long-poll requests on one port, so it
//! peeks at each connection's first bytes to route it. A request line can
//! arrive in more than one TCP segment, so a single short peek is not an
//! answer: peek again until the bytes decide.

use std::time::{Duration, Instant};

use tokio::{net::TcpStream, time::sleep};

/// How long to wait for a request's first bytes when they arrive in more than
/// one TCP segment.
const PEEK_DEADLINE: Duration = Duration::from_secs(5);

/// The pause between peeks while waiting for more bytes. `peek` returns
/// whatever is buffered at once, so retrying without a pause would spin.
const PEEK_RETRY: Duration = Duration::from_millis(5);

/// Peek into `buf` until it is full, the bytes so far fail `could_match`,
/// the peer closes, or [`PEEK_DEADLINE`] passes. Returns how many bytes were
/// peeked. Reads nothing from the stream.
pub(super) async fn peek_while<F: Fn(&[u8]) -> bool>(
    tcp: &TcpStream,
    buf: &mut [u8],
    could_match: F,
) -> usize {
    let deadline = Instant::now() + PEEK_DEADLINE;
    loop {
        let Ok(n) = tcp.peek(buf).await else {
            return 0;
        };
        let seen = buf.get(..n).unwrap_or_default();
        if n == buf.len() || n == 0 || !could_match(seen) || Instant::now() >= deadline {
            return n;
        }
        sleep(PEEK_RETRY).await;
    }
}

/// Request detection peeks at a connection's first bytes, which can arrive in
/// more than one TCP segment.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use core::time::Duration;

    use testresult::TestResult;
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
        time::{Instant, sleep},
    };

    use super::{PEEK_DEADLINE, peek_while};

    /// A connected server-side stream whose client writes `parts`, pausing
    /// between them, then leaves the connection open.
    async fn server_side_of(parts: &'static [&'static [u8]]) -> TestResult<TcpStream> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.expect("connect");
            for part in parts {
                client.write_all(part).await.expect("write");
                client.flush().await.expect("flush");
                sleep(Duration::from_millis(50)).await;
            }
            sleep(Duration::from_secs(10)).await;
        });
        let (server, _) = listener.accept().await?;
        Ok(server)
    }

    fn could_be_post(seen: &[u8]) -> bool {
        b"POST".starts_with(seen)
    }

    #[tokio::test]
    async fn a_method_split_across_segments_is_peeked_whole() -> TestResult {
        let tcp = server_side_of(&[b"PO", b"ST /longpoll HTTP/1.1\r\n\r\n"]).await?;
        let mut buf = [0u8; 4];
        assert_eq!(peek_while(&tcp, &mut buf, could_be_post).await, 4);
        assert_eq!(&buf, b"POST");
        Ok(())
    }

    #[tokio::test]
    async fn a_prefix_that_cannot_match_is_returned_without_waiting() -> TestResult {
        let tcp = server_side_of(&[b"XY"]).await?;
        let started = Instant::now();
        let mut buf = [0u8; 4];
        assert_eq!(peek_while(&tcp, &mut buf, could_be_post).await, 2);
        assert!(
            started.elapsed() < PEEK_DEADLINE,
            "a prefix that cannot match must not wait out the deadline"
        );
        Ok(())
    }
}
