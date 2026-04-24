//! Signed message wrapper for keyhive protocol messages.
//!
//! All keyhive protocol messages are signed before being sent over the wire.
//! This module provides the wrapper type that combines the signed data
//! with an optional contact card.

use alloc::{string::String, vec::Vec};
use core::fmt;

#[cfg(all(feature = "serde", feature = "std"))]
use crate::error::VerificationError;
use crate::peer_id::KeyhivePeerId;
#[cfg(all(feature = "serde", feature = "std"))]
use alloc::string::ToString;
#[cfg(all(feature = "serde", feature = "std"))]
use keyhive_core::contact_card::ContactCard;
#[cfg(all(feature = "serde", feature = "std"))]
use keyhive_crypto::signed::Signed;

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
/// ```text
/// CBOR map {
///   "contactCard": <text string>,   // JSON-encoded ContactCard, "" when absent
///   "signed":      <byte string>,   // bincode-serialised Signed<Vec<u8>>
/// }
/// ```
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SignedMessage {
    /// Contact card sent alongside this message (introduces the sender to
    /// the recipient when the recipient hasn't seen them before).
    #[cfg_attr(
        all(feature = "serde", feature = "std"),
        serde(rename = "contactCard", with = "contact_card_serde")
    )]
    contact_card: Option<ContactCard>,

    /// Bincode-serialized `Signed<Vec<u8>>`.
    #[cfg_attr(feature = "serde", serde(with = "serde_bytes"))]
    signed: Vec<u8>,
}

#[cfg(all(feature = "serde", feature = "std"))]
mod contact_card_serde {
    //! Maps `Option<ContactCard>` to a JSON text string in CBOR (`""` for `None`).
    use alloc::string::{String, ToString};
    use keyhive_core::contact_card::ContactCard;
    use serde::{Deserialize, Deserializer, Serializer, ser::Error as _};

    #[allow(clippy::ref_option)]
    pub(super) fn serialize<S: Serializer>(
        value: &Option<ContactCard>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(cc) => {
                let json = serde_json::to_string(cc).map_err(S::Error::custom)?;
                ser.serialize_str(&json)
            }
            None => ser.serialize_str(""),
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        de: D,
    ) -> Result<Option<ContactCard>, D::Error> {
        let s = String::deserialize(de)?;
        if s.is_empty() {
            Ok(None)
        } else {
            serde_json::from_str(&s)
                .map(Some)
                .map_err(|e| serde::de::Error::custom(e.to_string()))
        }
    }
}

impl PartialEq for SignedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.contact_card == other.contact_card && self.signed == other.signed
    }
}

impl Eq for SignedMessage {}

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
    pub const fn with_contact_card(signed: Vec<u8>, contact_card: ContactCard) -> Self {
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
    /// Deserializes the inner `Signed<Vec<u8>>` (bincode), verifies the
    /// cryptographic signature, checks that the signer matches
    /// `expected_sender`, and returns a [`VerifiedMessage`] containing the
    /// raw payload bytes and optional contact card.
    ///
    /// # Errors
    ///
    /// Returns [`VerificationError`] if deserialization, signature
    /// verification, or sender identity check fails.
    #[cfg(all(feature = "serde", feature = "std"))]
    pub fn verify(
        self,
        expected_sender: &KeyhivePeerId,
    ) -> Result<VerifiedMessage, VerificationError> {
        let signed: Signed<Vec<u8>> = bincode::deserialize(self.signed.as_slice())
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
    pub(crate) const fn signed_bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.signed
    }

    /// Serialize this message to CBOR bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if CBOR serialization fails.
    #[cfg(all(feature = "serde", feature = "std"))]
    pub fn to_cbor(&self) -> Result<Vec<u8>, CborError> {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(self, &mut buf).map_err(|e| CborError(e.to_string()))?;
        Ok(buf)
    }

    /// Deserialize a message from CBOR bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if CBOR deserialization fails.
    #[cfg(all(feature = "serde", feature = "std"))]
    pub fn from_cbor(bytes: &[u8]) -> Result<Self, CborError> {
        ciborium::de::from_reader(bytes).map_err(|e| CborError(e.to_string()))
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
    #[cfg(all(feature = "serde", feature = "std"))]
    pub contact_card: Option<ContactCard>,
}

impl VerifiedMessage {
    /// Create a new verified message.
    #[cfg(all(feature = "serde", feature = "std"))]
    #[must_use]
    pub const fn new(
        sender_id: KeyhivePeerId,
        payload: Vec<u8>,
        contact_card: Option<ContactCard>,
    ) -> Self {
        Self {
            sender_id,
            payload,
            contact_card,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::type_complexity)]
mod tests {
    #[cfg(all(feature = "serde", feature = "std"))]
    use super::*;

    #[cfg(all(feature = "serde", feature = "std"))]
    fn fake_contact_card() -> ContactCard {
        // Build a real ContactCard by spinning up a small keyhive in a
        // current-thread runtime so the test stays sync.
        use keyhive_core::{
            keyhive::Keyhive, listener::no_listener::NoListener,
            store::ciphertext::memory::MemoryCiphertextStore,
        };
        use keyhive_crypto::signer::memory::MemorySigner;

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let signer = MemorySigner::generate(&mut rand::rngs::OsRng);
            let kh: keyhive_core::keyhive::Keyhive<
                future_form::Local,
                MemorySigner,
                [u8; 32],
                Vec<u8>,
                MemoryCiphertextStore<[u8; 32], Vec<u8>>,
                NoListener,
                rand::rngs::OsRng,
            > = Keyhive::generate(
                signer,
                MemoryCiphertextStore::<[u8; 32], Vec<u8>>::new(),
                NoListener,
                rand::rngs::OsRng,
            )
            .await
            .expect("keyhive");
            kh.contact_card().await.expect("contact card")
        })
    }

    #[cfg(all(feature = "serde", feature = "std"))]
    #[test]
    fn cbor_roundtrip_preserves_contact_card_via_json_sentinel() {
        let cc = fake_contact_card();
        let msg = SignedMessage::with_contact_card(vec![1, 2, 3, 4], cc.clone());
        let cbor = msg.to_cbor().expect("serialize");
        let decoded = SignedMessage::from_cbor(&cbor).expect("deserialize");

        assert!(decoded.has_contact_card());
        let original_json = serde_json::to_string(&cc).expect("serialize original");
        let recovered_cc = decoded.contact_card.as_ref().expect("contact card present");
        let recovered_json = serde_json::to_string(recovered_cc).expect("serialize recovered");
        assert_eq!(original_json, recovered_json);
    }

    #[cfg(all(feature = "serde", feature = "std"))]
    #[test]
    fn cbor_roundtrip_with_no_contact_card_uses_empty_string_sentinel() {
        let msg = SignedMessage::new(vec![1, 2, 3]);
        assert!(!msg.has_contact_card());

        let cbor = msg.to_cbor().expect("serialize");

        // Decode with raw ciborium to verify the wire shape: the
        // contactCard key must be present with an empty STRING value.
        let raw: ciborium::Value = ciborium::de::from_reader(cbor.as_slice()).expect("raw decode");
        let map = raw.as_map().expect("map");
        let cc_field = map
            .iter()
            .find(|(k, _)| k.as_text() == Some("contactCard"))
            .expect("contactCard key present");
        assert_eq!(cc_field.1.as_text(), Some(""));

        let decoded = SignedMessage::from_cbor(&cbor).expect("deserialize");
        assert!(!decoded.has_contact_card());
    }

    #[cfg(all(feature = "serde", feature = "std"))]
    #[test]
    fn signed_field_is_cbor_byte_string() {
        // `signed` must be a CBOR byte string (major type 2), not an array.
        let msg = SignedMessage::new(vec![0x01, 0x02, 0x03, 0x04]);
        let cbor = msg.to_cbor().expect("serialize");
        let raw: ciborium::Value = ciborium::de::from_reader(cbor.as_slice()).expect("raw");
        let map = raw.as_map().expect("map");
        let (_, signed) = map
            .iter()
            .find(|(k, _)| k.as_text() == Some("signed"))
            .expect("signed key present");
        assert!(
            signed.is_bytes(),
            "`signed` must be CBOR bytes, got {signed:?}"
        );
    }

    #[cfg(all(feature = "serde", feature = "std"))]
    #[test]
    fn wire_uses_camelcase_keys() {
        let msg = SignedMessage::new(vec![0xab]);
        let cbor = msg.to_cbor().expect("serialize");
        let raw: ciborium::Value = ciborium::de::from_reader(cbor.as_slice()).expect("raw");
        let map = raw.as_map().expect("map");
        let keys: alloc::collections::BTreeSet<_> = map
            .iter()
            .filter_map(|(k, _)| k.as_text().map(ToString::to_string))
            .collect();
        assert!(keys.contains("contactCard"), "got keys {keys:?}");
        assert!(keys.contains("signed"), "got keys {keys:?}");
        assert!(!keys.contains("contact_card"), "found snake_case key");
    }
}
