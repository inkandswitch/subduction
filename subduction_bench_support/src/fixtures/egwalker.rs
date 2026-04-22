//! Embedded egwalker-paper Automerge test vectors.
//!
//! These `.am` files ship in `automerge_sedimentree/test-vectors/` and are compiled in here via
//! `include_bytes!`. They range from ~130 KB (`C1`) to ~830 KB (`S3`); the optional
//! `real-world.am` is ~6 MB.
//!
//! | Vector | Category        | Approx compressed size |
//! |--------|-----------------|------------------------|
//! | `A1`   | Author-A edits  | 770 KB                 |
//! | `A2`   | Author-A edits  | 570 KB                 |
//! | `C1`   | Collaborative   | 130 KB                 |
//! | `C2`   | Collaborative   | 210 KB                 |
//! | `S1`   | Sequential      | 340 KB                 |
//! | `S2`   | Sequential      | 600 KB                 |
//! | `S3`   | Sequential      | 830 KB                 |
//! | `real-world` (optional) | Real-world trace | ~6 MB      |
//!
//! # Usage
//!
//! ```ignore
//! use subduction_benches::fixtures::egwalker;
//!
//! for vector in egwalker::STANDARD {
//!     let doc = vector.load_automerge().expect("load");
//!     let changes = doc.get_changes(&[]).len();
//!     // bench body
//! }
//! ```

use automerge::Automerge;

/// A single egwalker test vector.
#[derive(Debug, Clone, Copy)]
pub struct TestVector {
    /// Short label (e.g. `"A1"`).
    pub name: &'static str,

    /// Raw compressed `.am` bytes.
    pub bytes: &'static [u8],
}

impl TestVector {
    /// Decode the bytes into a fresh `Automerge` document.
    ///
    /// # Errors
    ///
    /// Returns the underlying `automerge::AutomergeError` if decoding fails.
    pub fn load_automerge(&self) -> Result<Automerge, automerge::AutomergeError> {
        Automerge::load(self.bytes)
    }
}

macro_rules! include_vector {
    ($name:literal) => {
        TestVector {
            name: $name,
            bytes: include_bytes!(concat!(
                "../../../automerge_sedimentree/test-vectors/",
                $name,
                ".am"
            )),
        }
    };
}

/// The seven canonical egwalker vectors (A1, A2, C1, C2, S1, S2, S3).
pub const STANDARD: &[TestVector] = &[
    include_vector!("A1"),
    include_vector!("A2"),
    include_vector!("C1"),
    include_vector!("C2"),
    include_vector!("S1"),
    include_vector!("S2"),
    include_vector!("S3"),
];

/// The real-world trace (~6 MB). Kept separate because it's 7× the size of the largest standard
/// vector and we want callers to opt in consciously.
pub const REAL_WORLD: TestVector = include_vector!("real-world");
