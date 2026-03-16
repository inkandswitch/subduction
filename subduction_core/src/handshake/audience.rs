//! Audience and discovery types for the handshake protocol.
//!
//! An [`Audience`] declares the intended recipient of a handshake
//! [`Challenge`](super::challenge::Challenge). It supports two modes:
//! known peer identity, or discovery by service endpoint.

use crate::peer::id::PeerId;

/// A discovery identifier for locating peers by service endpoint.
///
/// This is a BLAKE3 hash of a URL or similar service identifier, used when
/// a client knows where to connect but not the peer's identity ahead of time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DiscoveryId([u8; 32]);

impl DiscoveryId {
    /// Create a discovery ID from a service identifier.
    ///
    /// The identifier is hashed with BLAKE3 to produce a 32-byte value.
    #[must_use]
    pub fn new(service_identifier: &[u8]) -> Self {
        let hash = blake3::hash(service_identifier);
        Self(hash.into())
    }

    /// Create a discovery ID from a pre-hashed value.
    #[must_use]
    pub const fn from_raw(hash: [u8; 32]) -> Self {
        Self(hash)
    }

    /// Get the raw bytes of the discovery ID.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// The intended recipient of a challenge.
///
/// Supports two modes:
/// - `Known`: Client knows the server's [`PeerId`] ahead of time
/// - `Discover`: Client knows the URL/endpoint but not the peer identity
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Audience {
    /// Known peer identity.
    Known(PeerId),

    /// Discovery mode: hash of URL or similar service identifier.
    Discover(DiscoveryId),
}

impl Audience {
    /// Create an audience from a known peer ID.
    #[must_use]
    pub const fn known(id: PeerId) -> Self {
        Self::Known(id)
    }

    /// Create a discovery audience from a service identifier.
    ///
    /// The identifier is hashed with BLAKE3 to produce a 32-byte value.
    #[must_use]
    pub fn discover(service_identifier: &[u8]) -> Self {
        Self::Discover(DiscoveryId::new(service_identifier))
    }

    /// Create a discovery audience from a pre-hashed value.
    #[must_use]
    pub const fn discover_raw(hash: [u8; 32]) -> Self {
        Self::Discover(DiscoveryId::from_raw(hash))
    }

    /// Create a discovery audience from a [`DiscoveryId`].
    #[must_use]
    pub const fn discover_id(discovery_id: DiscoveryId) -> Self {
        Self::Discover(discovery_id)
    }
}
