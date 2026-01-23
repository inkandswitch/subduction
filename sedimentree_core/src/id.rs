//! Unique identifier for Sedimentree data.

use alloc::{
    fmt::Formatter,
    str::FromStr,
    string::{String, ToString},
    vec::Vec,
};
use thiserror::Error;

/// A unique identifier for some data managed by Sedimentree.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cbor(transparent)]
pub struct SedimentreeId(#[n(0)] #[cbor(with = "minicbor::bytes")] [u8; 32]);

impl SedimentreeId {
    /// Constructor for a [`SedimentreeId`].
    #[must_use]
    pub const fn new(id: [u8; 32]) -> Self {
        Self(id)
    }

    /// The bytes of this [`SedimentreeId`].
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The bytes of this [`SedimentreeId`].
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// An error indicating that a [`SedimentreeId`] could not be parsed from a string.
#[derive(Debug, Clone, Error)]
pub enum BadSedimentreeId {
    /// The provided string has an odd length.
    #[error("SedimentreeId length is not even: {0}")]
    LengthNotEven(String),

    /// The provided string contains invalid hex characters.
    #[error("SedimentreeId contains invalid hex characters: {0}")]
    InvalidHex(String),

    /// The provided string has an invalid length.
    #[error("SedimentreeId must be 32 bytes (64 hex characters): {0}")]
    InvalidLength(String),
}

impl FromStr for SedimentreeId {
    type Err = BadSedimentreeId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.len().is_multiple_of(2) {
            return Err(BadSedimentreeId::LengthNotEven(s.to_string()));
        }

        let bytes = (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| BadSedimentreeId::InvalidHex(s.to_string())))
            .collect::<Result<Vec<u8>, BadSedimentreeId>>()?;

        if bytes.len() == 32 {
            let mut arr = [0; 32];
            arr.copy_from_slice(&bytes);
            Ok(SedimentreeId(arr))
        } else {
            Err(BadSedimentreeId::InvalidLength(s.to_string()))
        }
    }
}

impl core::fmt::Debug for SedimentreeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl core::fmt::Display for SedimentreeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
