//! Reading and writing keyhive protocol values in Subduction's canonical
//! codec ([`sedimentree_core::codec`]).
//!
//! Integers are big-endian, lists carry a `u32` count, and byte strings a
//! bijou64 length prefix.

use alloc::vec::Vec;

use sedimentree_core::codec::{
    decode,
    error::{ArrayTooLarge, Bijou64Error, BlobTooLarge, DecodeError, SizeMismatch},
};

/// Append a bijou64 length prefix and `bytes`.
pub(crate) fn put_bytes(bytes: &[u8], buf: &mut Vec<u8>) {
    bijoux::u64::encode(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}

/// The encoded size of `bytes` under [`put_bytes`].
pub(crate) const fn bytes_size(bytes: &[u8]) -> usize {
    bijoux::u64::encoded_len(bytes.len() as u64) + bytes.len()
}

/// Append a list's `u32` element count.
///
/// # Panics
///
/// Panics if the list has more than `u32::MAX` elements, which no protocol
/// message can hold.
#[allow(clippy::expect_used)]
pub(crate) fn put_count(count: usize, buf: &mut Vec<u8>) {
    let count = u32::try_from(count).expect("a message list holds at most u32::MAX elements");
    buf.extend_from_slice(&count.to_be_bytes());
}

/// A cursor over encoded bytes.
#[derive(Debug)]
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    pub(crate) const fn new(buf: &'a [u8]) -> Self {
        Self { buf, offset: 0 }
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        let value = decode::u8(self.buf, self.offset)?;
        self.offset += 1;
        Ok(value)
    }

    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        let value = decode::u64(self.buf, self.offset)?;
        self.offset += 8;
        Ok(value)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let value = decode::array::<N>(self.buf, self.offset)?;
        self.offset += N;
        Ok(value)
    }

    /// A byte string written by [`put_bytes`].
    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let (len, consumed) = bijoux::u64::decode(self.buf.get(self.offset..).unwrap_or_default())
            .map_err(|kind| Bijou64Error {
                offset: self.offset,
                kind,
            })?;
        self.offset += consumed;
        let len = usize::try_from(len).map_err(|_| BlobTooLarge {
            size: len,
            max: usize::MAX as u64,
        })?;
        let bytes = decode::slice(self.buf, self.offset, len)?;
        self.offset += len;
        Ok(bytes)
    }

    /// The element count of the list `field`, written by [`put_count`].
    ///
    /// Each element takes at least `min_element_size` bytes, so a count the
    /// remaining bytes can't hold is rejected before the caller allocates
    /// for it.
    pub(crate) fn count(
        &mut self,
        field: &'static str,
        min_element_size: usize,
    ) -> Result<usize, DecodeError> {
        let count = decode::u32(self.buf, self.offset)? as usize;
        self.offset += 4;
        let max = self.remaining() / min_element_size.max(1);
        if count > max {
            return Err(ArrayTooLarge { count, max, field }.into());
        }
        Ok(count)
    }

    /// Bytes not yet read.
    pub(crate) const fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.offset)
    }

    /// Fail unless every byte was read.
    pub(crate) fn finish(self) -> Result<(), DecodeError> {
        if self.offset == self.buf.len() {
            Ok(())
        } else {
            Err(SizeMismatch {
                declared: self.offset,
                actual: self.buf.len(),
            }
            .into())
        }
    }
}
