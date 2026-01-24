//! Random nonce for challenge uniqueness.

/// A random nonce for challenge uniqueness.
///
/// 128 bits provides sufficient collision resistance for replay protection
/// within a ~5 minute window, especially when combined with timestamps.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cbor(transparent)]
pub struct Nonce(
    #[n(0)]
    #[cbor(with = "minicbor::bytes")]
    [u8; 16],
);

impl Nonce {
    /// Create a new nonce from a raw value.
    #[must_use]
    pub const fn new(value: u128) -> Self {
        Self(value.to_le_bytes())
    }

    /// Get the raw nonce value.
    #[must_use]
    pub const fn as_u128(&self) -> u128 {
        u128::from_le_bytes(self.0)
    }

    /// Create a nonce from bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Create a random nonce using `getrandom`.
    ///
    /// # Panics
    ///
    /// Panics if the system random number generator fails.
    #[cfg(feature = "getrandom")]
    #[cfg_attr(docsrs, doc(cfg(feature = "getrandom")))]
    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn random() -> Self {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).expect("getrandom failed");
        Self(bytes)
    }
}
