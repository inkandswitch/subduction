//! Cryptographic primitives for verified data.

pub mod signed;
pub mod verified;

// FIXME
pub struct Challenge(signed::Signed<crate::timestamp::TimestampSeconds>);

// pub struct Witness(PeerId, SediementreeId);
//
// #[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, cbor::Encode, cbor::Decode)]
// #[cbor(transparent)]
// pub struct Timestamp(u64);
