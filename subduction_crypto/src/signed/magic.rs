//! Magic bytes for signed payload identification.

use thiserror::Error;

/// Unit type representing valid magic bytes in a signed payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Magic;

/// The magic byte sequence `SDN` identifying Subduction signed payloads.
const MAGIC: &[u8] = b"SDN";

impl TryFrom<&[u8]> for Magic {
    type Error = InvalidMagicError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value == MAGIC {
            Ok(Magic)
        } else {
            Err(InvalidMagicError)
        }
    }
}

impl From<Magic> for &[u8] {
    fn from(_: Magic) -> Self {
        MAGIC
    }
}

/// Error returned when magic bytes don't match the expected value.
#[derive(Debug, Clone, Copy, Error)]
#[error("invalid magic bytes")]
pub struct InvalidMagicError;
