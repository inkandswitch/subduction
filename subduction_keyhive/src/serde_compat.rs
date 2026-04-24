//! Serde helpers for `Vec<[u8; 32]>` and `Vec<Vec<u8>>` that encode each
//! element as a CBOR byte string instead of an array of integers.
//!
//! For a single `Vec<u8>` or `[u8; N]` field, `#[serde(with = "serde_bytes")]`
//! suffices. These helpers exist because `#[serde(with)]` cannot reach inside
//! a `Vec<…>` to change each element's encoding.

#![cfg(all(feature = "serde", feature = "std"))]

/// `Vec<[u8; 32]>` with each element as a CBOR byte string.
pub(crate) mod vec_byte_array_32 {
    use alloc::vec::Vec;

    use serde::{Deserialize, Deserializer, Serializer, ser::SerializeSeq};
    use serde_bytes::ByteArray;

    pub(crate) fn serialize<S: Serializer>(
        v: &[[u8; 32]],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(v.len()))?;
        for item in v {
            seq.serialize_element(&ByteArray::new(*item))?;
        }
        seq.end()
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Vec<[u8; 32]>, D::Error> {
        let wrapped: Vec<ByteArray<32>> = Vec::deserialize(de)?;
        Ok(wrapped.into_iter().map(ByteArray::into_array).collect())
    }
}

/// `Vec<Vec<u8>>` with each element as a CBOR byte string.
pub(crate) mod vec_byte_buf {
    use alloc::vec::Vec;

    use serde::{Deserialize, Deserializer, Serializer, ser::SerializeSeq};
    use serde_bytes::ByteBuf;

    pub(crate) fn serialize<S: Serializer>(
        v: &[Vec<u8>],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(v.len()))?;
        for item in v {
            seq.serialize_element(&serde_bytes::Bytes::new(item))?;
        }
        seq.end()
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Vec<Vec<u8>>, D::Error> {
        let wrapped: Vec<ByteBuf> = Vec::deserialize(de)?;
        Ok(wrapped.into_iter().map(ByteBuf::into_vec).collect())
    }
}
