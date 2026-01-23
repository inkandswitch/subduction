#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ProtocolVersion {
    // NOTE: Just one for now, but maybe more in future
    #[n(0)]
    V0_1 = 0,
}
