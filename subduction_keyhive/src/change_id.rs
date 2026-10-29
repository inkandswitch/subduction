//! [`ChangeId`]: the content reference the keyhive runtime uses.
//!
//! A change is a sedimentree commit, so a [`ChangeId`] wraps a [`CommitId`].
//! Revocation retention watermarks (`ContentHeads<ChangeId>`) then name
//! sedimentree commits directly.
//!
//! The newtype exists because Keyhive's content refs must implement
//! `keyhive_codec`'s [`Encode`] and [`StreamDecode`], and the orphan rule
//! keeps us from implementing those for `CommitId` outside `sedimentree_core`.
//!
//! # Encoding
//!
//! Watermarks travel inside events, so the server and the browser client must
//! agree on the encoding. `keyhive_wasm`'s `JsChangeId` is variable-length,
//! `bijou32(len) ‖ bytes`, which is `keyhive_codec`'s length-prefixed bytes.
//! A [`ChangeId`] encodes the same way, so it is byte-identical to a 32-byte
//! `JsChangeId`. Decoding accepts only 32 bytes: a change ID of any other
//! length is refused rather than misread. (Automerge change hashes are
//! 32 bytes.)

use alloc::vec::Vec;

use keyhive_codec::{
    error::DecodeError,
    prefixed::{decode_bytes, encode_bytes},
    traits::{Decode, Encode, StreamDecode, decode_exact},
};
use sedimentree_core::loose_commit::id::CommitId;

/// A change identifier: the sedimentree commit a change is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChangeId(CommitId);

impl ChangeId {
    /// The change that is `commit`.
    #[must_use]
    pub const fn new(commit: CommitId) -> Self {
        Self(commit)
    }

    /// The commit this change is.
    #[must_use]
    pub const fn commit_id(&self) -> CommitId {
        self.0
    }
}

impl From<CommitId> for ChangeId {
    fn from(commit: CommitId) -> Self {
        Self(commit)
    }
}

impl From<ChangeId> for CommitId {
    fn from(change: ChangeId) -> Self {
        change.0
    }
}

/// `bijou32(32) ‖ bytes`: `keyhive_codec`'s length-prefixed bytes.
impl Encode for ChangeId {
    fn encode_into(&self, out: &mut Vec<u8>) {
        encode_bytes(self.0.as_bytes(), out);
    }
}

impl Decode for ChangeId {
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        decode_exact(bytes)
    }
}

impl StreamDecode for ChangeId {
    fn decode_next(bytes: &[u8]) -> Result<(Self, &[u8]), DecodeError> {
        let (id, rest) = decode_bytes(bytes)?;
        let id: [u8; 32] = id
            .try_into()
            .map_err(|_| DecodeError::InvalidField("change id length"))?;
        Ok((Self(CommitId::new(id)), rest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use testresult::TestResult;

    fn change(seed: u8) -> ChangeId {
        ChangeId::new(CommitId::new([seed; 32]))
    }

    #[test]
    fn matches_length_prefixed_bytes() {
        let mut expected = Vec::new();
        encode_bytes(&[9; 32], &mut expected);
        assert_eq!(change(9).encode().into_bytes(), expected);
    }

    #[test]
    fn is_self_delimiting_and_round_trips() -> TestResult {
        let ids = [change(1), change(2)];
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

    #[test]
    fn refuses_other_lengths() {
        for len in [0, 31, 33] {
            let mut bytes = Vec::new();
            encode_bytes(&alloc::vec![7; len], &mut bytes);
            assert_eq!(
                ChangeId::decode(&bytes),
                Err(DecodeError::InvalidField("change id length")),
                "length {len}"
            );
        }
    }
}
