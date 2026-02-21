//! Encoding helpers for the canonical binary codec.

use alloc::vec::Vec;

/// Encode a u8.
#[inline]
pub fn u8(value: u8, buf: &mut Vec<u8>) {
    buf.push(value);
}

/// Encode a u16 as big-endian.
#[inline]
pub fn u16(value: u16, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Encode a u32 as big-endian.
#[inline]
pub fn u32(value: u32, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Encode a u64 as big-endian.
#[inline]
pub fn u64(value: u64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Encode raw bytes.
#[inline]
pub fn bytes(value: &[u8], buf: &mut Vec<u8>) {
    buf.extend_from_slice(value);
}

/// Encode a fixed-size array.
#[inline]
pub fn array<const N: usize>(value: &[u8; N], buf: &mut Vec<u8>) {
    buf.extend_from_slice(value);
}
