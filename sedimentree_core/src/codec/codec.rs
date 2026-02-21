//! Combined encode/decode trait.

use super::{Decode, Encode};

/// A type with both encoding and decoding capabilities.
///
/// This is a convenience trait that combines [`Encode`] and [`Decode`].
/// Most signed payload types implement this full trait.
///
/// # Usage
///
/// Prefer using the specific traits when possible:
/// - Use `T: Encode` when you only need to serialize
/// - Use `T: Decode` when you only need to deserialize
/// - Use `T: Codec` when you need both (e.g., roundtrip tests)
pub trait Codec: Encode + Decode {}

impl<T: Encode + Decode> Codec for T {}
