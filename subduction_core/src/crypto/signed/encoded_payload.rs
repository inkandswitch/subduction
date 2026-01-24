//! CBOR-encoded payload bytes with phantom type tracking.

use alloc::vec::Vec;
use core::{cmp::Ordering, marker::PhantomData};

/// CBOR-encoded bytes representing a payload of type `T`.
///
/// The phantom type parameter tracks what type the bytes decode to,
/// without actually storing a decoded value.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EncodedPayload<T>(Vec<u8>, PhantomData<T>);

impl<T> EncodedPayload<T> {
    /// Create a new [`EncodedPayload`].
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes, PhantomData)
    }

    /// Get the byte slice representation of the [`EncodedPayload`].
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl<Ctx, T> minicbor::Encode<Ctx> for EncodedPayload<T> {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        e.bytes(&self.0)?;
        Ok(())
    }
}

impl<'b, Ctx, T> minicbor::Decode<'b, Ctx> for EncodedPayload<T> {
    fn decode(d: &mut minicbor::Decoder<'b>, _: &mut Ctx) -> Result<Self, minicbor::decode::Error> {
        Ok(Self(d.bytes()?.to_vec(), PhantomData))
    }
}

impl<T> PartialEq for EncodedPayload<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T> Eq for EncodedPayload<T> {}

impl<T> PartialOrd for EncodedPayload<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for EncodedPayload<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}
