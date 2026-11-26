//! Telling what a new connection is from its first bytes.
//!
//! The server accepts WebSocket, HTTP long-poll and contact-card requests on
//! one port, so it peeks at each connection's first bytes to route it. A
//! request line can arrive in more than one TCP segment, so a single short
//! peek is not an answer: peek again until the bytes decide.

use std::time::Duration;

use tokio::{
    net::TcpStream,
    time::{Instant, sleep, timeout_at},
};

/// Path of the route serving this server's keyhive contact card.
pub(super) const CONTACT_CARD_PATH: &str = "/keyhive/contact-card";

/// How long to wait for enough of a request's first bytes to route it,
/// however many TCP segments they take.
const PEEK_DEADLINE: Duration = Duration::from_secs(5);

/// The pause between peeks while waiting for more bytes. `peek` returns
/// whatever is buffered at once, so retrying without a pause would spin.
const PEEK_RETRY: Duration = Duration::from_millis(5);

/// What a connection is, by the start of its request line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestKind {
    /// A `GET` of [`CONTACT_CARD_PATH`]: this server's keyhive contact card.
    ContactCard,

    /// Any other `GET`: a WebSocket upgrade.
    WebSocket,

    /// A `POST`, or an `OPTIONS` CORS preflight: HTTP long-poll.
    LongPoll,
}

impl RequestKind {
    /// The first four bytes of each request line the server routes.
    const PREFIXES: [(&[u8; 4], Self); 3] = [
        (b"GET ", Self::WebSocket),
        (b"OPTI", Self::LongPoll),
        (b"POST", Self::LongPoll),
    ];

    /// Route `tcp` by the start of its request line. Reads nothing from the
    /// stream.
    ///
    /// # Errors
    ///
    /// Returns [`Unrecognized`] with the bytes seen if they start no request
    /// the server routes, which includes the peer sending nothing before it
    /// closes or [`PEEK_DEADLINE`] passes.
    pub(super) async fn detect(tcp: &TcpStream) -> Result<Self, Unrecognized> {
        let mut buf = [0u8; 4];
        let peeked = peek_while(tcp, &mut buf, |seen| {
            Self::PREFIXES
                .iter()
                .any(|(prefix, _)| prefix.starts_with(seen))
        })
        .await;
        let seen = buf.get(..peeked).unwrap_or_default();
        let kind = Self::PREFIXES
            .iter()
            .find(|(prefix, _)| prefix.as_slice() == seen)
            .map(|(_, kind)| *kind)
            .ok_or_else(|| Unrecognized(seen.to_vec()))?;
        if kind == Self::WebSocket && requests_contact_card(tcp).await {
            Ok(Self::ContactCard)
        } else {
            Ok(kind)
        }
    }
}

/// Peek into `buf` until it is full, the bytes so far fail `could_match`,
/// the peer closes, or [`PEEK_DEADLINE`] passes. Returns how many bytes were
/// peeked. Reads nothing from the stream.
async fn peek_while<F: Fn(&[u8]) -> bool>(
    tcp: &TcpStream,
    buf: &mut [u8],
    could_match: F,
) -> usize {
    let mut peeked = 0;
    // The deadline covers every peek, the first included, so a peer that
    // sends nothing is not waited on forever. When it passes, `peeked` holds
    // what the last peek saw.
    timeout_at(Instant::now() + PEEK_DEADLINE, async {
        loop {
            peeked = tcp.peek(buf).await.unwrap_or(0);
            let seen = buf.get(..peeked).unwrap_or_default();
            if peeked == buf.len() || peeked == 0 || !could_match(seen) {
                return;
            }
            sleep(PEEK_RETRY).await;
        }
    })
    .await
    .unwrap_or_default();
    peeked
}

/// The first bytes of a connection that start no request the server routes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unrecognized request: {0:?}")]
pub(super) struct Unrecognized(Vec<u8>);

impl Unrecognized {
    /// The bytes seen, empty if the peer sent none.
    pub(super) fn seen(&self) -> &[u8] {
        &self.0
    }
}

/// Whether the request line on `tcp` is a `GET` of [`CONTACT_CARD_PATH`],
/// with or without a query. Reads nothing from the stream.
async fn requests_contact_card(tcp: &TcpStream) -> bool {
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
    use std::time::Duration;

    use testresult::TestResult;
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
        time::{Instant, sleep},
    };

    use super::{PEEK_DEADLINE, RequestKind, peek_while, requests_contact_card};

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

    #[tokio::test]
    async fn a_method_split_across_segments_is_detected() -> TestResult {
        let tcp = server_side_of(&[b"GE", b"T / HTTP/1.1\r\n\r\n"]).await?;
        assert_eq!(RequestKind::detect(&tcp).await, Ok(RequestKind::WebSocket));
        Ok(())
    }

    #[tokio::test]
    async fn an_unroutable_prefix_is_returned() -> TestResult {
        let tcp = server_side_of(&[b"XY"]).await?;
        let unrecognized = RequestKind::detect(&tcp)
            .await
            .expect_err("XY starts no routed request");
        assert_eq!(unrecognized.seen(), b"XY");
        Ok(())
    }

    /// The deadline covers the first peek too, so a peer that never sends is
    /// given up on.
    #[tokio::test(start_paused = true)]
    async fn a_silent_peer_is_given_up_on_at_the_deadline() -> TestResult {
        let tcp = server_side_of(&[]).await?;
        let started = Instant::now();
        let mut buf = [0u8; 4];
        assert_eq!(peek_while(&tcp, &mut buf, |_| true).await, 0);
        // The client hangs up after 10s, so an unbounded first peek would
        // return then.
        let elapsed = started.elapsed();
        assert!(PEEK_DEADLINE <= elapsed && elapsed < PEEK_DEADLINE + Duration::from_secs(1));
        Ok(())
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

    #[tokio::test]
    async fn a_contact_card_request_is_detected() -> TestResult {
        let tcp = server_side_of(&[b"GET /keyhive/contact-card HTTP/1.1\r\n\r\n"]).await?;
        assert_eq!(
            RequestKind::detect(&tcp).await,
            Ok(RequestKind::ContactCard)
        );
        Ok(())
    }
}
