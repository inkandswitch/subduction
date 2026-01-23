//! CBOR encoding helpers for types not natively supported by minicbor.

use crate::collections::Set;

/// Encode/decode a `Set<T>` as a CBOR array.
pub mod set {
    use super::*;

    #[cfg(feature = "std")]
    use core::hash::Hash;

    /// Encode a Set as a CBOR array.
    pub fn encode<Ctx, T, W>(
        set: &Set<T>,
        e: &mut minicbor::Encoder<W>,
        ctx: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>>
    where
        T: minicbor::Encode<Ctx>,
        W: minicbor::encode::Write,
    {
        e.array(set.len() as u64)?;
        for item in set {
            item.encode(e, ctx)?;
        }
        Ok(())
    }

    /// Decode a CBOR array into a Set.
    #[cfg(feature = "std")]
    pub fn decode<'b, Ctx, T>(
        d: &mut minicbor::Decoder<'b>,
        ctx: &mut Ctx,
    ) -> Result<Set<T>, minicbor::decode::Error>
    where
        T: minicbor::Decode<'b, Ctx> + Eq + Hash,
    {
        let len = d.array()?.ok_or_else(|| {
            minicbor::decode::Error::message("expected definite-length array for Set")
        })?;
        let mut set = Set::with_capacity(len as usize);
        for _ in 0..len {
            set.insert(T::decode(d, ctx)?);
        }
        Ok(set)
    }

    /// Decode a CBOR array into a Set (no_std version using BTreeSet).
    #[cfg(not(feature = "std"))]
    pub fn decode<'b, Ctx, T>(
        d: &mut minicbor::Decoder<'b>,
        ctx: &mut Ctx,
    ) -> Result<Set<T>, minicbor::decode::Error>
    where
        T: minicbor::Decode<'b, Ctx> + Ord,
    {
        let len = d.array()?.ok_or_else(|| {
            minicbor::decode::Error::message("expected definite-length array for Set")
        })?;
        let mut set = Set::new();
        for _ in 0..len {
            set.insert(T::decode(d, ctx)?);
        }
        Ok(set)
    }
}
