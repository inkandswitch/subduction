//! Protocol version for signed payload format evolution.

/// The protocol version for signed payload encoding.
///
/// Used to support forward compatibility as the signed payload format evolves.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ProtocolVersion {
    /// Version 0.1 of the signed payload protocol.
    V0_1 = 0,
}
