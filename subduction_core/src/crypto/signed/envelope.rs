use super::{magic::Magic, protocol_version::ProtocolVersion};

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
    pub fn new(magic: Magic, protocol: ProtocolVersion, payload: T) -> Self {
        Self {
            magic,
            protocol,
            payload,
        }
    }

    /// Get a reference to the payload.
    #[must_use]
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// Consume the envelope and return the payload.
    pub fn into_payload(self) -> T {
        self.payload
    }

    /// Get the magic bytes.
    #[must_use]
    pub fn magic(&self) -> Magic {
        self.magic
    }

    /// Get the protocol version.
    #[must_use]
    pub fn protocol(&self) -> ProtocolVersion {
        self.protocol
    }
}
