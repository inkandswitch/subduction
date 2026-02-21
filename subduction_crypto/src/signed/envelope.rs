//! Envelope wrapper with magic bytes and protocol version.

use super::{magic::Magic, protocol_version::ProtocolVersion};

/// A signed payload envelope containing magic bytes, protocol version, and the payload.
///
/// The envelope ensures signed data has identifying markers and version information
/// for forward compatibility.
#[derive(Clone, Debug, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Envelope<T> {
    #[n(0)]
    magic: Magic,

    #[n(1)]
    protocol: ProtocolVersion,

    #[n(2)]
    payload: T,
}

impl<T> Envelope<T> {
    /// Create a new [`Envelope`].
    #[must_use]
    pub const fn new(magic: Magic, protocol: ProtocolVersion, payload: T) -> Self {
        Self {
            magic,
            protocol,
            payload,
        }
    }

    /// Get a reference to the payload.
    #[must_use]
    pub const fn payload(&self) -> &T {
        &self.payload
    }

    /// Consume the envelope and return the payload.
    pub fn into_payload(self) -> T {
        self.payload
    }

    /// Get the magic bytes.
    #[must_use]
    pub const fn magic(&self) -> Magic {
        self.magic
    }

    /// Get the protocol version.
    #[must_use]
    pub const fn protocol(&self) -> ProtocolVersion {
        self.protocol
    }
}
