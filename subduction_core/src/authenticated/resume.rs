//! Resuming an [`Authenticated`] connection across process lifetimes.
//!
//! Some hosts keep a transport open after the process that authenticated it
//! is gone. The motivating case is Cloudflare Durable Object WebSocket
//! hibernation: the socket survives while the engine is evicted from memory,
//! and the next inbound frame wakes a fresh engine that has never seen the
//! handshake. Re-running the handshake is not an option, because the peer
//! considers the connection already established.
//!
//! A [`ResumeToken`] lets such a host carry the handshake's result across the
//! gap without weakening the [`Authenticated`] witness:
//!
//! 1. After a real handshake, the node calls [`ResumeToken::issue`]. This
//!    takes an [`Authenticated`] (so a token can only be minted from a
//!    completed handshake) and signs `(peer_id, direction, issued_at)` with
//!    the node's _own_ key.
//! 2. The host persists the [`Signed<ResumeToken>`] next to the transport
//!    (e.g. `serializeAttachment` on a hibernatable WebSocket).
//! 3. On wake, [`Authenticated::resume`] checks that the token is signed by
//!    this node's key and is no older than the caller's `max_age`, then
//!    rebuilds the witness.
//!
//! The token records when it was issued, and the redeemer decides how old is
//! too old, the same split as the handshake (a `Challenge` carries its
//! timestamp; the verifier supplies `max_drift`). Lowering `max_age` therefore
//! takes effect for every outstanding token at once, without rotating the
//! node's key.
//!
//! The token is a statement by this node to itself ("I verified this peer on
//! this transport"). It is never sent to peers and is not part of the wire
//! protocol. It uses its own schema (`SUA\x00`) so that a signature over a
//! token can never be confused with a handshake message signed by the same
//! key.

use alloc::vec::Vec;
use core::time::Duration;

use ed25519_dalek::VerifyingKey;
use future_form::FutureForm;
use sedimentree_core::codec::{
    decode,
    decode::DecodeFields,
    encode,
    encode::EncodeFields,
    error::{DecodeError, InvalidEnumTag},
    schema,
    schema::Schema,
};
use subduction_crypto::{signed::Signed, signer::Signer};
use thiserror::Error;

use super::{Authenticated, Direction};
use crate::{peer::id::PeerId, timestamp::TimestampSeconds};

/// A node's signed record that it authenticated `peer_id` in a handshake.
///
/// Mint with [`ResumeToken::issue`]; redeem with [`Authenticated::resume`].
/// See the [module docs](self) for the full flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResumeToken {
    /// The peer identity verified by the original handshake.
    pub peer_id: PeerId,

    /// Which side of the original handshake this node was on.
    pub direction: Direction,

    /// When this node issued the token.
    pub issued_at: TimestampSeconds,
}

/// How far in the future `issued_at` may be before [`Authenticated::resume`]
/// rejects the token.
///
/// The issuer and redeemer are the same node, so a future timestamp can only
/// come from clock skew (e.g. a Durable Object restarting on another
/// machine). This absorbs small skew without letting a token outlive
/// `max_age` by more than this margin.
pub const MAX_FUTURE_SKEW: Duration = Duration::from_secs(60);

impl ResumeToken {
    /// Sign a resume token for a connection that completed a handshake.
    ///
    /// Requiring an [`Authenticated`] keeps minting tied to the witness: a
    /// token can only describe a peer this node actually verified (or
    /// previously resumed). `signer` must be this node's identity key, the
    /// same one later passed to [`Authenticated::resume`] as `our_key`.
    ///
    /// `now` is recorded as the token's `issued_at`. To keep a long-lived
    /// connection resumable, issue a fresh token after each resume.
    pub async fn issue<Conn, Async, S>(
        authenticated: &Authenticated<Conn, Async>,
        signer: &S,
        now: TimestampSeconds,
    ) -> Signed<Self>
    where
        Conn: Clone,
        Async: FutureForm,
        S: Signer<Async>,
    {
        let token = Self {
            peer_id: authenticated.peer_id(),
            direction: authenticated.direction(),
            issued_at: now,
        };
        Signed::seal::<Async, _>(signer, token).await.into_signed()
    }
}

impl<Conn: Clone, Async: FutureForm> Authenticated<Conn, Async> {
    /// Rebuild an authenticated connection from a [`ResumeToken`].
    ///
    /// Succeeds only if `token` carries a valid signature by `our_key`, was
    /// issued no more than `max_age` before `now`, and was not issued more
    /// than [`MAX_FUTURE_SKEW`] after `now`. The returned connection has the
    /// peer identity and direction recorded in the token.
    ///
    /// This does not consult any connection policy. As with a fresh
    /// handshake, policy is applied when the connection is added to
    /// [`Subduction`](crate::subduction::Subduction), so a peer revoked
    /// since the token was issued is still turned away.
    ///
    /// # Errors
    ///
    /// * [`ResumeError::InvalidSignature`] if the signature or encoding is bad.
    /// * [`ResumeError::WrongIssuer`] if the token was signed by another key.
    /// * [`ResumeError::Expired`] if `now - issued_at > max_age`.
    /// * [`ResumeError::IssuedInFuture`] if `issued_at - now > MAX_FUTURE_SKEW`.
    pub fn resume(
        inner: Conn,
        token: &Signed<ResumeToken>,
        our_key: &VerifyingKey,
        now: TimestampSeconds,
        max_age: Duration,
    ) -> Result<Self, ResumeError> {
        let verified = token
            .try_verify()
            .map_err(|_| ResumeError::InvalidSignature)?;

        if verified.issuer() != *our_key {
            return Err(ResumeError::WrongIssuer);
        }

        let token = verified.payload();
        let issued_at = token.issued_at;
        if issued_at > now {
            if issued_at.abs_diff(now) > MAX_FUTURE_SKEW {
                return Err(ResumeError::IssuedInFuture { issued_at, now });
            }
        } else if now.abs_diff(issued_at) > max_age {
            return Err(ResumeError::Expired {
                issued_at,
                now,
                max_age,
            });
        }

        Ok(Self::from_handshake(inner, token.peer_id, token.direction))
    }
}

/// Why a [`ResumeToken`] was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ResumeError {
    /// The signature did not verify, or the token failed to decode.
    #[error("resume token signature is invalid")]
    InvalidSignature,

    /// The token was signed by a key other than this node's.
    #[error("resume token was not issued by this node")]
    WrongIssuer,

    /// The token is older than the caller's `max_age`.
    #[error("resume token issued at {issued_at:?} is older than {max_age:?} (now {now:?})")]
    Expired {
        /// When the token was issued.
        issued_at: TimestampSeconds,
        /// The time it was presented.
        now: TimestampSeconds,
        /// The maximum age the caller allowed.
        max_age: Duration,
    },

    /// The token claims to be issued further in the future than
    /// [`MAX_FUTURE_SKEW`] allows.
    #[error("resume token issued at {issued_at:?} is in the future (now {now:?})")]
    IssuedInFuture {
        /// When the token claims to have been issued.
        issued_at: TimestampSeconds,
        /// The time it was presented.
        now: TimestampSeconds,
    },
}

// ── Encoding ────────────────────────────────────────────────────────────

const DIRECTION_DIALED: u8 = 0;
const DIRECTION_ACCEPTED: u8 = 1;

/// Size of the token fields: peer id (32) + direction (1) + `issued_at` (8).
pub(crate) const RESUME_TOKEN_FIELDS_SIZE: usize = 32 + 1 + 8;

/// Size of a signed token: schema (4) + issuer (32) + fields + signature (64).
pub const RESUME_TOKEN_SIGNED_SIZE: usize = 4 + 32 + RESUME_TOKEN_FIELDS_SIZE + 64;

impl Schema for ResumeToken {
    const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
    const TYPE_BYTE: u8 = b'A'; // Authenticated-connection resume
    const VERSION: u8 = 0;
}

impl EncodeFields for ResumeToken {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        encode::array(self.peer_id.as_bytes(), buf);
        encode::u8(
            match self.direction {
                Direction::Dialed => DIRECTION_DIALED,
                Direction::Accepted => DIRECTION_ACCEPTED,
            },
            buf,
        );
        encode::u64(self.issued_at.as_secs(), buf);
    }

    fn fields_size(&self) -> usize {
        RESUME_TOKEN_FIELDS_SIZE
    }
}

impl DecodeFields for ResumeToken {
    const MIN_SIGNED_SIZE: usize = RESUME_TOKEN_SIGNED_SIZE;

    fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.len() < RESUME_TOKEN_FIELDS_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "ResumeToken",
                need: RESUME_TOKEN_FIELDS_SIZE,
                have: buf.len(),
            });
        }

        let peer_id = PeerId::new(decode::array(buf, 0)?);
        let direction = match decode::u8(buf, 32)? {
            DIRECTION_DIALED => Direction::Dialed,
            DIRECTION_ACCEPTED => Direction::Accepted,
            tag => {
                return Err(InvalidEnumTag {
                    tag,
                    type_name: "Direction",
                }
                .into());
            }
        };
        let issued_at = TimestampSeconds::new(decode::u64(buf, 33)?);

        Ok((
            Self {
                peer_id,
                direction,
                issued_at,
            },
            RESUME_TOKEN_FIELDS_SIZE,
        ))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use future_form::Sendable;
    use subduction_crypto::signer::memory::MemorySigner;

    const ISSUED_AT: u64 = 10_000;
    const MAX_AGE: Duration = Duration::from_secs(3_600);

    /// A stand-in transport; `resume` never touches it.
    #[derive(Clone, Debug, PartialEq)]
    struct Conn(u8);

    fn signer(seed: u8) -> MemorySigner {
        MemorySigner::from_bytes(&[seed; 32])
    }

    fn handshaken(peer: u8, direction: Direction) -> Authenticated<Conn, Sendable> {
        Authenticated::from_handshake(Conn(0), PeerId::new([peer; 32]), direction)
    }

    async fn issue(
        auth: &Authenticated<Conn, Sendable>,
        signer: &MemorySigner,
    ) -> Signed<ResumeToken> {
        ResumeToken::issue(auth, signer, TimestampSeconds::new(ISSUED_AT)).await
    }

    fn resume_at(
        token: &Signed<ResumeToken>,
        node: &MemorySigner,
        now: u64,
        max_age: Duration,
    ) -> Result<Authenticated<Conn, Sendable>, ResumeError> {
        Authenticated::resume(
            Conn(1),
            token,
            &node.verifying_key(),
            TimestampSeconds::new(now),
            max_age,
        )
    }

    #[tokio::test]
    async fn resume_restores_peer_and_direction() {
        let node = signer(1);
        for direction in [Direction::Accepted, Direction::Dialed] {
            let original = handshaken(9, direction);
            let token = issue(&original, &node).await;

            let resumed = resume_at(&token, &node, ISSUED_AT + 60, MAX_AGE)
                .expect("valid token should resume");

            assert_eq!(resumed.peer_id(), original.peer_id());
            assert_eq!(resumed.direction(), direction);
            assert_eq!(resumed.inner(), &Conn(1));
        }
    }

    #[tokio::test]
    async fn token_survives_byte_roundtrip() {
        let node = signer(1);
        let token = issue(&handshaken(9, Direction::Accepted), &node).await;

        let bytes = token.as_bytes().to_vec();
        assert_eq!(bytes.len(), RESUME_TOKEN_SIGNED_SIZE);
        assert_eq!(bytes.get(..4), Some(b"SUA\x00".as_slice()));

        let decoded = Signed::<ResumeToken>::try_decode(&bytes).expect("decodes");
        assert_eq!(
            decoded.try_verify().expect("verifies").payload().issued_at,
            TimestampSeconds::new(ISSUED_AT)
        );
        assert!(resume_at(&decoded, &node, ISSUED_AT, MAX_AGE).is_ok());
    }

    #[tokio::test]
    async fn rejects_token_from_another_key() {
        let node = signer(1);
        let token = issue(&handshaken(9, Direction::Accepted), &signer(2)).await;

        let err = resume_at(&token, &node, ISSUED_AT, MAX_AGE)
            .expect_err("foreign token must be rejected");
        assert_eq!(err, ResumeError::WrongIssuer);
    }

    #[tokio::test]
    async fn max_age_is_inclusive_and_set_by_the_redeemer() {
        let node = signer(1);
        let token = issue(&handshaken(9, Direction::Accepted), &node).await;
        let limit = ISSUED_AT + MAX_AGE.as_secs();

        assert!(resume_at(&token, &node, limit, MAX_AGE).is_ok());
        let err = resume_at(&token, &node, limit + 1, MAX_AGE).expect_err("one second too old");
        assert!(matches!(err, ResumeError::Expired { .. }));

        // The same, already-issued token is rejected once the caller tightens
        // `max_age`; the lifetime is not baked into the token.
        let err = resume_at(&token, &node, ISSUED_AT + 600, Duration::from_secs(300))
            .expect_err("tightened max_age must reject an existing token");
        assert!(matches!(err, ResumeError::Expired { .. }));
    }

    #[tokio::test]
    async fn tolerates_small_future_skew_only() {
        let node = signer(1);
        let token = issue(&handshaken(9, Direction::Accepted), &node).await;
        let skew = MAX_FUTURE_SKEW.as_secs();

        assert!(resume_at(&token, &node, ISSUED_AT - skew, MAX_AGE).is_ok());
        let err = resume_at(&token, &node, ISSUED_AT - skew - 1, MAX_AGE)
            .expect_err("token from too far in the future must be rejected");
        assert!(matches!(err, ResumeError::IssuedInFuture { .. }));
    }

    #[tokio::test]
    async fn rejects_tampered_peer_id() {
        let node = signer(1);
        let token = issue(&handshaken(9, Direction::Accepted), &node).await;

        // Flip the first byte of the peer id (right after schema + issuer).
        let mut bytes = token.as_bytes().to_vec();
        let peer_byte = bytes.get_mut(4 + 32).expect("in bounds");
        *peer_byte ^= 0xFF;

        let tampered = Signed::<ResumeToken>::try_decode(&bytes).expect("still decodes");
        let err = resume_at(&tampered, &node, ISSUED_AT, MAX_AGE)
            .expect_err("tampered token must be rejected");
        assert_eq!(err, ResumeError::InvalidSignature);
    }

    #[tokio::test]
    async fn rejects_unknown_direction_tag() {
        let node = signer(1);
        let token = issue(&handshaken(9, Direction::Accepted), &node).await;

        let mut bytes = token.as_bytes().to_vec();
        let direction_byte = bytes.get_mut(4 + 32 + 32).expect("in bounds");
        *direction_byte = 7;

        assert!(Signed::<ResumeToken>::try_decode(&bytes).is_err());
    }
}
