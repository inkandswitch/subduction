//! Reading and writing keyhive protocol values in Subduction's canonical
//! codec ([`sedimentree_core::codec`]).
//!
//! Integers are big-endian. Lists carry a bijou64 element count and byte
//! strings a bijou64 length prefix.
//!
//! The format puts no limit on counts or lengths. The decoder enforces one
//! instead: a declared size the remaining bytes can't hold is rejected
//! before anything is allocated for it.

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

/// Append a list's bijou64 element count.
pub(crate) fn put_count(count: usize, buf: &mut Vec<u8>) {
    bijoux::u64::encode(count as u64, buf);
}

/// The encoded size of `count` under [`put_count`].
pub(crate) const fn count_size(count: usize) -> usize {
    bijoux::u64::encoded_len(count as u64)
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

    fn bijou64(&mut self) -> Result<u64, DecodeError> {
        let (value, consumed) = bijoux::u64::decode(
            self.buf.get(self.offset..).unwrap_or_default(),
        )
        .map_err(|kind| Bijou64Error {
            offset: self.offset,
            kind,
        })?;
        self.offset += consumed;
        Ok(value)
    }

    /// A byte string written by [`put_bytes`].
    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.bijou64()?;
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
        // A count past `usize::MAX` (possible on 32-bit targets) can't fit in
        // the buffer either, so saturating keeps it failing the check below.
        let count = usize::try_from(self.bijou64()?).unwrap_or(usize::MAX);
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
