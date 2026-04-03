//! Causal identity for loose commits.

/// A user-supplied opaque identifier for a loose commit.
///
/// Unlike [`Digest`](crate::crypto::digest::Digest), which is a content hash
/// computed by the system, `CommitId` is provided by the caller at construction
/// time. The user decides how commits are identified — typically the hash of
/// the underlying document change (e.g., an Automerge `ChangeHash`).
///
/// Two commits with the same `CommitId` are the same item for
/// set reconciliation, regardless of blob metadata. Fragment maps
/// also use `CommitId` as their key (the fragment's head commit).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommitId([u8; 32]);

impl CommitId {
    /// Create a [`CommitId`] from raw bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw bytes of this identifier.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for CommitId {
    fn from(bytes: [u8; 32]) -> Self {
        Self::new(bytes)
    }
}

impl From<CommitId> for [u8; 32] {
    fn from(id: CommitId) -> Self {
        id.0
    }
}

impl core::fmt::Debug for CommitId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "CommitId(")?;
        for byte in &self.0[..4] {
            write!(f, "{byte:02x}")?;
        }
        write!(f, "…)")
    }
}

impl core::fmt::Display for CommitId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for CommitId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let bytes: [u8; 32] = u.arbitrary()?;
        Ok(Self(bytes))
    }
}

#[cfg(feature = "bolero")]
impl bolero::generator::TypeGenerator for CommitId {
    fn generate<D: bolero::Driver>(driver: &mut D) -> Option<Self> {
        let bytes: [u8; 32] = bolero::generator::TypeGenerator::generate(driver)?;
        Some(Self(bytes))
    }
}
