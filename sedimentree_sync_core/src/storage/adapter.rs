// use sedimentree_core::{Depth, Digest, SedimentreeId};
//
// use super::key::StorageKey;
// use crate::payload::Payload;

// /// A trait for storing binary data.
// ///
// /// This is generally the data associated with a [`Sedimentree`][sedimentree_core::Sedimentree].
// pub trait StorageAdapter {
//     type StorageKey;
//     type StoragePrefix;
//
//     fn load(
//         &self,
//         id: SedimentreeId,
//         depth: Depth,
//         head: Digest,
//     ) -> impl Future<Output = Result<Option<Vec<u8>>, String>>; // TOOD error type
//
//     fn save(
//         &self,
//         id: SedimentreeId,
//         depth: Depth,
//         head: Digest,
//         data: &[u8],
//     ) -> impl Future<Output = Result<(), String>>; // TOOD error type
//
//     fn remove(
//         &self,
//         id: SedimentreeId,
//         depth: Depth,
//         key: &StorageKey,
//     ) -> impl Future<Output = Result<(), String>>; // TOOD error type
//
//     fn load_range(
//         &self,
//         id: SedimentreeId,
//         key: &StorageKey,
//     ) -> impl Future<Output = Result<Vec<Payload>, String>>; // TOOD error type
//
//     fn remove_range(
//         &self,
//         id: SedimentreeId,
//         prefix: &StorageKey,
//     ) -> impl Future<Output = Result<(), String>>; // TOOD error type
// }
