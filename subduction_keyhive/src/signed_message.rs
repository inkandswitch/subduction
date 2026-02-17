//! Signed message wrapper for keyhive protocol messages.
//!
//! All keyhive protocol messages are signed before being sent over the wire.
//! This module provides the wrapper type that combines the signed data
//! with an optional contact card.

use alloc::{string::String, vec::Vec};
use core::fmt;

use crate::error::VerificationError;
use crate::peer_id::KeyhivePeerId;
use keyhive_core::crypto::signed::Signed;

/// Error type for CBOR serialization/deserialization.
#[derive(Debug, Clone)]
pub struct CborError(pub String);

impl fmt::Display for CborError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CBOR error: {}", self.0)
    }
}

#[cfg(feature = "std")]
impl std::error::Error for CborError {}

/// A signed message for transmission over the network.
///
/// All keyhive protocol messages are signed by the sender before transmission.
/// This wrapper combines:
/// * An optional contact card (to introduce the sender to the recipient)
/// * The signed message payload (message data + signature)
#[derive(Debug, Clone, PartialEq, Eq, Hash, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SignedMessage {
    /// Optional serialized contact card.
    ///
    /// Included when the sender wants to introduce themselves to the recipient,
    /// either in the first message or when requested.
    #[n(0)]
    contact_card: Option<Vec<u8>>,

    /// The signed message payload.
    ///
    /// This contains the actual message data and the cryptographic signature.
    /// The recipient should verify the signature before processing the message.
    #[n(1)]
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

    /// Create a new signed message with a contact card.
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

    /// Verify the signature and sender identity, consuming the signed message.
    ///
    /// Deserializes the inner `Signed<Vec<u8>>`, verifies the cryptographic
    /// signature, checks that the signer matches `expected_sender`, and returns
    /// a [`VerifiedMessage`] containing the raw payload bytes and optional
    /// contact card.
    ///
    /// # Errors
    ///
    /// Returns [`VerificationError`] if deserialization, signature verification,
    /// or sender identity check fails.
    pub fn verify(
        self,
        expected_sender: &KeyhivePeerId,
    ) -> Result<VerifiedMessage, VerificationError> {
        use alloc::string::ToString;

        let signed: Signed<Vec<u8>> = minicbor_serde::from_slice(self.signed.as_slice())
            .map_err(|e| VerificationError::Deserialization(e.to_string()))?;

        signed
            .try_verify()
            .map_err(|_| VerificationError::InvalidSignature)?;

        let sender_id = KeyhivePeerId::from_bytes(*signed.issuer().as_bytes());
        if !sender_id.same_identity(expected_sender) {
            return Err(VerificationError::SenderMismatch {
                expected: expected_sender.clone(),
                actual: sender_id,
            });
        }

        Ok(VerifiedMessage::new(
            sender_id,
            signed.payload().clone(),
            self.contact_card,
        ))
    }

    /// Get a mutable reference to the signed bytes (for testing tamper scenarios).
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) const fn signed_bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.signed
    }

    /// Serialize this message to CBOR bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if CBOR serialization fails.
    pub fn to_cbor(&self) -> Result<Vec<u8>, CborError> {
        use alloc::string::ToString;

        minicbor::to_vec(self).map_err(|e| CborError(e.to_string()))
    }

    /// Deserialize a message from CBOR bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if CBOR deserialization fails.
    pub fn from_cbor(bytes: &[u8]) -> Result<Self, CborError> {
        use alloc::string::ToString;

        minicbor::decode(bytes).map_err(|e| CborError(e.to_string()))
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

    /// The contact card, if included.
    pub contact_card: Option<Vec<u8>>,
}

impl VerifiedMessage {
    /// Create a new verified message.
    #[must_use]
    pub const fn new(
        sender_id: KeyhivePeerId,
        payload: Vec<u8>,
        contact_card: Option<Vec<u8>>,
    ) -> Self {
        Self {
            sender_id,
            payload,
            contact_card,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    #[cfg(all(feature = "serde", feature = "std"))]
    use super::*;

    #[cfg(all(feature = "serde", feature = "std"))]
    #[test]
    fn test_cbor_roundtrip() {
        let msg = SignedMessage::with_contact_card(vec![1, 2, 3, 4], vec![5, 6, 7, 8]);

        let cbor = msg.to_cbor().expect("should serialize");
        let decoded = SignedMessage::from_cbor(&cbor).expect("should deserialize");

        assert_eq!(msg, decoded);
    }
}
