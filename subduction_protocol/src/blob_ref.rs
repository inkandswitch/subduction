//! The blob data plane: machines route *references* to bulk bytes; the
//! driver warehouses the bytes themselves.
//!
//! A [`BlobRef`] points into a driver-retained ingress frame. Machines
//! never hold blob bytes in state — they decide (store / forward / send)
//! using refs, and the driver resolves refs at exactly three sites
//! (storage executor, transport assembler, FFI accessor), each with
//! defined failure semantics.
//!
//! # Validity is a liveness property, not a type property
//!
//! A `BlobRef` is a claim check, like a file descriptor: resolution can
//! fail, and the design makes failure *clean and loud* rather than
//! impossible:
//!
//! - [`FrameId`]s are monotonic and **never reused** — a stale ref can
//!   only mean "gone", never "someone else's bytes" (no ABA).
//! - Content addressing backstops everything else: every downstream
//!   consumer of blob bytes sits behind a digest check, so wrong bytes
//!   cannot cross a trust boundary undetected.
//! - Lifecycle: the driver refcounts refs per effect execution; machines
//!   emit an explicit release when a ref leaves machine state; refs are
//!   tagged with their edge epoch so edge death bulk-frees stragglers.

/// A driver-assigned identifier for one retained ingress frame.
///
/// Monotonic per driver instance; **never reused** (a `u64` counter
/// cannot realistically wrap). Reuse would reintroduce the ABA problem —
/// a stale [`BlobRef`] silently resolving to different content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct FrameId(u64);

impl FrameId {
    /// Create a frame id from a raw driver-allocated value.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The raw id value.
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

/// A reference to a blob's bytes inside a driver-retained frame.
///
/// Plain integers so it crosses machine state, the event journal, and
/// FFI unchanged. See the [module docs](self) for failure semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BlobRef {
    /// The retained frame holding the bytes.
    pub frame: FrameId,

    /// Byte offset of the blob within the frame.
    pub offset: u32,

    /// Blob length in bytes.
    pub len: u32,
}

/// One piece of an outbound message: scatter-gather parts let the machine
/// emit small encoded envelope bytes and splice blob regions by reference;
/// the transport assembler concatenates at the socket (`writev`-style),
/// so fan-out to N subscribers costs N envelopes and zero blob copies.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Part {
    /// Literal bytes, encoded by the machine (envelopes, metadata).
    Bytes(alloc::vec::Vec<u8>),

    /// A blob region resolved by the driver at assembly time. If
    /// resolution fails (frame gone), the transport must drop the whole
    /// message before writing anything and surface a loud diagnostic —
    /// never emit a torn frame.
    Ref(BlobRef),
}

impl Part {
    /// The number of bytes this part contributes to the assembled frame.
    #[must_use]
    pub fn len(&self) -> u64 {
        match self {
            Self::Bytes(bytes) => u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            Self::Ref(blob) => u64::from(blob.len),
        }
    }

    /// Whether this part contributes no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
