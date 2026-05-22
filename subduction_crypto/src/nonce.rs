//! Random nonce for challenge uniqueness.

/// A random nonce for challenge uniqueness.
///
/// 128 bits provides sufficient collision resistance for replay protection
/// within a ~5 minute window, especially when combined with timestamps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Nonce([u8; 16]);

impl Nonce {
    /// Create a random nonce using `getrandom`.
    ///
    /// This is the recommended constructor for production use.
    ///
    /// # Panics
    ///
    /// Panics if the system random number generator fails.
    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn random() -> Self {
        let mut bytes = [0u8; 16];
        getrandom::getrandom(&mut bytes).expect("getrandom failed");
        Self(bytes)
    }

    /// Create a nonce from a raw `u128` value.
    ///
    /// This is intended for testing and deserialization. Production code should
    /// use [`Nonce::random()`] to ensure cryptographic uniqueness.
    #[must_use]
    pub const fn from_u128(value: u128) -> Self {
        Self(value.to_le_bytes())
    }

    /// Get the raw nonce value as `u128`.
    #[must_use]
    pub const fn as_u128(&self) -> u128 {
        u128::from_le_bytes(self.0)
    }

    /// Create a nonce from raw bytes.
    ///
    /// This is intended for deserialization. Production code should use
    /// [`Nonce::random()`] to ensure cryptographic uniqueness.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    //! Mutant-coverage tests for [`Nonce::as_u128`].

    use super::Nonce;

    /// `as_u128` of a non-zero, non-one nonce must return the original
    /// value. A mutant returning `0` is caught by the non-zero input;
    /// a mutant returning `1` is caught because we use a distinct value.
    #[test]
    fn as_u128_round_trips() {
        let original = 0x1234_5678_9ABC_DEF0_FEDC_BA98_7654_3210u128;
        let nonce = Nonce::from_u128(original);
        assert_eq!(nonce.as_u128(), original, "as_u128 must invert from_u128");
    }

    /// Round-trip through `from_u128` → `as_u128` across several
    /// representative values (zero, one, max, and a typical random
    /// nonce) so the test fails for any constant-return mutant.
    #[test]
    fn as_u128_round_trips_across_values() {
        for &v in &[
            0u128,
            1u128,
            2u128,
            u128::MAX,
            u128::MAX - 1,
            0xDEAD_BEEF_CAFE_BABE_u128,
            u128::from(u64::MAX),
        ] {
            let nonce = Nonce::from_u128(v);
            assert_eq!(nonce.as_u128(), v, "round-trip failed for {v}");
        }
    }
}
