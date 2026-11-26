//! Signed message wrapper for keyhive protocol messages.
//!
//! All keyhive protocol messages are signed before being sent over the wire.
//! This module provides the wrapper type that combines the signed data
//! with an optional contact card.

use alloc::vec::Vec;

use keyhive_codec::traits::Decode;
use keyhive_crypto::{bytes::Bytes, verified::Signed};
use sedimentree_core::codec::{
    encode::Encode,
    error::{DecodeError, InvalidEnumTag},
};

use crate::{
    codec::{self, Reader},
    error::{VerificationError, WireDecodeError},
    peer_id::KeyhivePeerId,
};

/// A signed message for transmission over the network.
///
/// # Wire Format
///
/// ```text
/// ╔══════╦══════════════════════════╦══════════════════════════╗
/// ║ Tag  ║ Contact Card             ║ Signed                   ║
/// ║  1B  ║ only if tag is 1:        ║ bijou64 length + bytes   ║
/// ║      ║ bijou64 length + bytes   ║                          ║
/// ╚══════╩══════════════════════════╩══════════════════════════╝
/// ```
///
/// Both byte strings are what `keyhive_wasm` hands to JS: the contact card
/// is `ContactCard.toBytes()`, and `signed` is `SignedBytes.toBytes()` from
/// `Keyhive.trySign`, signing an encoded [`Message`](crate::Message).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SignedMessage {
    /// The `KH` frame of a contact card sent alongside this message
    /// (introduces the sender to the recipient when the recipient hasn't seen
    /// them before).
    contact_card: Option<Vec<u8>>,

    /// The `KH` frame of a `Signed<Bytes>`.
    signed: Vec<u8>,
}

impl SignedMessage {
    /// Create a new signed message without a contact card.
    #[must_use]
    pub const fn new(signed: Vec<u8>) -> Self {
        Self {
            contact_card: None,
            signed,
        }
    }

    /// Create a new signed message with a contact card's `KH` frame.
    #[must_use]
    pub const fn with_contact_card(signed: Vec<u8>, contact_card: Vec<u8>) -> Self {
        Self {
            contact_card: Some(contact_card),
            signed,
        }
    }

    /// Check if this message includes a contact card.
    #[must_use]
    pub const fn has_contact_card(&self) -> bool {
        self.contact_card.is_some()
    }

    /// Decode a signed message from its wire encoding (see
    /// [`SignedMessage`]).
    ///
    /// # Errors
    ///
    /// Returns [`WireDecodeError`] if `buf` is not exactly one encoded
    /// signed message.
    pub fn try_decode(buf: &[u8]) -> Result<Self, WireDecodeError> {
        let mut reader = Reader::new(buf);
        let contact_card = match reader.u8()? {
            NO_CONTACT_CARD => None,
            CONTACT_CARD => Some(reader.bytes()?.to_vec()),
            tag => {
                return Err(DecodeError::from(InvalidEnumTag {
                    tag,
                    type_name: "SignedMessage contact card",
                })
                .into());
            }
        };
        let signed = reader.bytes()?.to_vec();
        reader.finish()?;
        Ok(Self {
            contact_card,
            signed,
        })
    }

    /// Verify the signature and sender identity, consuming the signed message.
    ///
    /// Decodes the inner `Signed<Bytes>` frame, verifies the cryptographic
    /// signature, checks that the signer matches `expected_sender`, and
    /// returns a [`VerifiedMessage`] containing the raw payload bytes and
    /// optional contact card.
    ///
    /// # Errors
    ///
    /// Returns [`VerificationError`] if the signed frame does not decode, the
    /// signature is invalid, or the signer is not `expected_sender`.
    pub fn verify(
        self,
        expected_sender: &KeyhivePeerId,
    ) -> Result<VerifiedMessage, VerificationError> {
        let signed =
            Signed::<Bytes>::decode(&self.signed).map_err(VerificationError::SignedFrame)?;

        let sender_id = KeyhivePeerId::from_bytes(*signed.issuer().as_bytes());
        if !sender_id.same_identity(expected_sender) {
            return Err(VerificationError::SenderMismatch {
                expected: expected_sender.clone(),
                actual: sender_id,
            });
        }

        let verified = signed
            .verify()
            .map_err(|_| VerificationError::InvalidSignature)?;

        Ok(VerifiedMessage {
            sender_id,
            payload: verified.payload().as_slice().to_vec(),
            contact_card: self.contact_card,
        })
    }

    /// Get a mutable reference to the signed bytes (for testing tamper scenarios).
    ///
    /// Only used from `protocol`'s tamper-detection tests, so it has the same
    /// gate as they do.
    #[cfg(all(test, feature = "test-utils"))]
    pub(crate) const fn signed_bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.signed
    }
}

/// The wire tag of a signed message without a contact card.
const NO_CONTACT_CARD: u8 = 0;

/// The wire tag of a signed message with a contact card.
const CONTACT_CARD: u8 = 1;

impl Encode for SignedMessage {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        match &self.contact_card {
            None => buf.push(NO_CONTACT_CARD),
            Some(card) => {
                buf.push(CONTACT_CARD);
                codec::put_bytes(card, &mut buf);
            }
        }
        codec::put_bytes(&self.signed, &mut buf);
        buf
    }

    fn encoded_size(&self) -> usize {
        1 + self.contact_card.as_deref().map_or(0, codec::bytes_size)
            + codec::bytes_size(&self.signed)
    }
}

/// Result of verifying a signed message.
///
/// Contains the verified payload bytes and the sender's peer ID.
#[derive(Debug, Clone)]
pub struct VerifiedMessage {
    /// The peer ID of the sender (derived from verifying key).
    pub sender_id: KeyhivePeerId,

    /// The verified payload bytes.
    pub payload: Vec<u8>,

    /// The contact card's `KH` frame, if included.
    pub contact_card: Option<Vec<u8>>,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use alloc::vec;

    use testresult::TestResult;

    use super::*;

    #[test]
    fn roundtrip_keeps_the_contact_card_or_its_absence() -> TestResult {
        for message in [
            SignedMessage::new(vec![1, 2, 3]),
            SignedMessage::with_contact_card(vec![1, 2, 3], vec![0x4b, 0x48, 7]),
            SignedMessage::with_contact_card(Vec::new(), Vec::new()),
        ] {
            let bytes = message.encode();
            assert_eq!(bytes.len(), message.encoded_size());
            assert_eq!(SignedMessage::try_decode(&bytes)?, message);
        }
        Ok(())
    }

    #[test]
    fn truncated_or_padded_bytes_are_rejected() {
        let bytes = SignedMessage::with_contact_card(vec![1, 2, 3], vec![4, 5]).encode();
        for len in 0..bytes.len() {
            assert!(SignedMessage::try_decode(bytes.get(..len).expect("in range")).is_err());
        }
        let mut padded = bytes;
        padded.push(0);
        assert!(SignedMessage::try_decode(&padded).is_err());
    }

    #[cfg(feature = "bolero")]
    #[test]
    fn prop_roundtrip_at_encoded_size() {
        bolero::check!()
            .with_arbitrary::<SignedMessage>()
            .for_each(|message| {
                let bytes = message.encode();
                assert_eq!(bytes.len(), message.encoded_size());
                assert_eq!(SignedMessage::try_decode(&bytes).as_ref(), Ok(message));
            });
    }

    #[cfg(feature = "bolero")]
    #[test]
    fn prop_decoding_any_bytes_does_not_panic() {
        bolero::check!()
            .with_type::<Vec<u8>>()
            .for_each(|bytes| drop(SignedMessage::try_decode(bytes)));
    }
}
