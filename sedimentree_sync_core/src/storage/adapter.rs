use super::key::StorageKey;
use crate::chunk::Chunk;

pub trait StorageAdapter {
    fn load(&self, key: &StorageKey) -> impl Future<Output = Result<Option<Vec<u8>>, String>>; // FIXME error type
    fn save(&self, key: &StorageKey, data: &[u8]) -> impl Future<Output = Result<(), String>>; // FIXME error type
    fn remove(&self, key: &StorageKey) -> impl Future<Output = Result<(), String>>; // FIXME error type
    fn load_range(&self, prefix: &StorageKey) -> impl Future<Output = Result<Vec<Chunk>, String>>; // FIXME error type
    fn remove_range(&self, prefix: &StorageKey) -> impl Future<Output = Result<(), String>>; // FIXME error type
}
