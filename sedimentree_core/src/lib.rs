//! The core of the [Sedimentree] data partitioning scheme.
//!
//! This core library only defines the metadata tracking features of Sedimentree.
//! We assume that the actual data described by this metadata is not legible to the Sedimentree
//! (regardless of whether or not it's encrypted).
//!
//! Sedimentree is a way of organizing data into a series of layers, or strata, each of which
//! contains a set of checkpoints (hashes) that represent some fragment of a larger file or log.
//! For example, an Automerge document might be partitioned, and each fragment encrypted.
//! Sedimentree tracks just enough metadata to allow efficient diffing and synchronization
//! of these fragments.
//!
//! # Wire-format validity
//!
//! Commit and fragment metadata must fit the counts encoded by the wire format:
//!
//! | Metadata | Count encoding | Maximum unique entries |
//! | --- | --- | --- |
//! | Commit parents | `u8` | [`255`](loose_commit::MAX_PARENTS) |
//! | Fragment boundary IDs | `u8` | [`255`](fragment::MAX_BOUNDARIES) |
//! | Fragment checkpoints | `u16` | [`65,535`](fragment::MAX_CHECKPOINTS) |
//!
//! Counts apply to deduplicated sets. Full checkpoint IDs are first truncated to
//! 12-byte prefixes, then deduplicated; IDs sharing a prefix count as one checkpoint.
//! This normalization only affects transport metadata, never opaque blob bytes.
//!
//! [`LooseCommit::new`](loose_commit::LooseCommit::new),
//! [`Fragment::new`](fragment::Fragment::new), and
//! [`Fragment::from_parts`](fragment::Fragment::from_parts) reject oversized sets
//! with [`InvalidLooseCommit`](loose_commit::InvalidLooseCommit) or
//! [`InvalidFragment`](fragment::InvalidFragment). Serde deserialization and
//! [`HasBlobMeta::from_args`](blob::has_meta::HasBlobMeta::from_args) enforce the
//! same limits, so successfully constructed values have representable wire counts.
//!
//! Wire-format validity does not establish application-level validity. In
//! particular, it does not verify that a fragment describes the claimed history
//! or belongs at the claimed depth. The application remains responsible for
//! interpreting and validating its opaque payloads.
//!
//! [Sedimentree]: https://github.com/inkandswitch/keyhive/blob/main/design/sedimentree.md

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod blob;
pub mod codec;
pub mod collections;
pub mod commit;
pub mod crypto;
pub mod depth;
pub mod fragment;
pub mod hex;
pub mod id;
pub mod loose_commit;
pub mod sedimentree;
pub mod topsorted;

#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;
