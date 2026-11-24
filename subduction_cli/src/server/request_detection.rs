//! Telling what a new connection is from its first bytes.
//!
//! The server accepts WebSocket, HTTP long-poll and contact-card requests on
//! one port, so it peeks at each connection's first bytes to route it. A
//! request line can arrive in more than one TCP segment, so a single short
//! peek is not an answer: peek again until the bytes decide.

use std::time::{Duration, Instant};

use tokio::{net::TcpStream, time::sleep};

/// Path of the route serving this server's keyhive contact card.
pub(super) const CONTACT_CARD_PATH: &str = "/keyhive/contact-card";

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

/// Whether the request line on `tcp` is a `GET` of [`CONTACT_CARD_PATH`],
/// with or without a query. Reads nothing from the stream.
pub(super) async fn requests_contact_card(tcp: &TcpStream) -> bool {
    let request = [b"GET ".as_slice(), CONTACT_CARD_PATH.as_bytes()].concat();
    // One byte past the path, to tell `/keyhive/contact-card` from a longer path.
    let mut buf = [0u8; "GET ".len() + CONTACT_CARD_PATH.len() + 1];
    let peeked = peek_while(tcp, &mut buf, |seen| {
        seen.iter()
            .zip(&request)
            .all(|(seen, expected)| seen == expected)
    })
    .await;
    peeked == buf.len() && buf.starts_with(&request) && matches!(buf.last(), Some(b' ' | b'?'))
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

    use super::{PEEK_DEADLINE, requests_contact_card};

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

    #[tokio::test]
    async fn a_contact_card_request_split_across_segments_is_recognized() -> TestResult {
        let tcp = server_side_of(&[b"GET /keyh", b"ive/contact-card HTTP/1.1\r\n\r\n"]).await?;
        assert!(requests_contact_card(&tcp).await);
        Ok(())
    }

    #[tokio::test]
    async fn another_path_is_rejected_without_waiting() -> TestResult {
        let tcp = server_side_of(&[b"GET /ws"]).await?;
        let started = Instant::now();
        assert!(!requests_contact_card(&tcp).await);
        assert!(
            started.elapsed() < PEEK_DEADLINE,
            "a prefix that cannot match must not wait out the deadline"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_longer_path_is_not_the_contact_card() -> TestResult {
        let tcp = server_side_of(&[b"GET /keyhive/contact-cards HTTP/1.1\r\n\r\n"]).await?;
        assert!(!requests_contact_card(&tcp).await);
        Ok(())
    }
}
