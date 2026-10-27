//! [`ChangeId`]: the content reference the keyhive runtime uses.
//!
//! It must encode exactly like `keyhive_wasm`'s `JsChangeId`: revocation
//! retention watermarks (`ContentHeads<ChangeId>`) travel inside events, so
//! the server and the browser client must agree on the encoding. Both use
//! `keyhive_codec`'s `bijou32(len) ‖ bytes`, which is self-delimiting.

use alloc::vec::Vec;

use keyhive_codec::{
    error::DecodeError,
    traits::{Decode, Encode, StreamDecode, decode_exact},
};

/// An opaque change identifier (for Automerge, a change hash).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChangeId(Vec<u8>);

impl ChangeId {
    /// Wrap change-identifier bytes.
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The identifier's bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Recover the bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for ChangeId {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

/// `bijou32(len) ‖ bytes`, as `JsChangeId` encodes.
impl Encode for ChangeId {
    #[allow(clippy::expect_used)]
    fn encode_into(&self, out: &mut Vec<u8>) {
        u32::try_from(self.0.len())
            .expect("a change id is shorter than 4 GiB")
            .encode_into(out);
        out.extend_from_slice(&self.0);
    }
}

impl Decode for ChangeId {
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        decode_exact(bytes)
    }
}

impl StreamDecode for ChangeId {
    fn decode_next(bytes: &[u8]) -> Result<(Self, &[u8]), DecodeError> {
        let (len, rest) = u32::decode_next(bytes)?;
        let len =
            usize::try_from(len).map_err(|_| DecodeError::InvalidField("change id length"))?;
        let (id, rest) = rest
            .split_at_checked(len)
            .ok_or(DecodeError::UnexpectedEnd)?;
        Ok((Self(id.to_vec()), rest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloc::vec;

    use testresult::TestResult;

    #[test]
    fn codec_is_self_delimiting_and_round_trips() -> TestResult {
        let ids = [ChangeId::new(vec![]), ChangeId::new(vec![7; 300])];
        let mut packed = Vec::new();
        for id in &ids {
            id.encode_into(&mut packed);
        }

        let (first, rest) = ChangeId::decode_next(&packed)?;
        let (second, rest) = ChangeId::decode_next(rest)?;
        assert_eq!([first, second], ids);
        assert!(rest.is_empty());
        Ok(())
    }
}
