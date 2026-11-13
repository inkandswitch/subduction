//! A store that keeps nothing.

use core::{
    convert::Infallible,
    ops::{Deref, DerefMut},
};

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    id::SedimentreeId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};

use super::{Install, Lookup, MinimizedTreeStore};

/// Keeps no trees: every read misses and rebuilds from storage.
///
/// Useful for tests that must exercise the storage path, for nodes too small
/// to benefit from a store, and as a template for writing your own.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopMinimizedTreeStore;

/// The entry type of [`NoopMinimizedTreeStore`], which is never resident.
#[derive(Debug, Clone, Copy)]
pub enum NoEntry {}

impl Deref for NoEntry {
    type Target = MinimizedSedimentree;

    fn deref(&self) -> &MinimizedSedimentree {
        match *self {}
    }
}

impl DerefMut for NoEntry {
    fn deref_mut(&mut self) -> &mut MinimizedSedimentree {
        match *self {}
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> MinimizedTreeStore<Async> for NoopMinimizedTreeStore {
    type Error = Infallible;
    type Generation = ();
    type Entry = NoEntry;

    fn get(
        &self,
        _id: SedimentreeId,
    ) -> Async::Future<'_, Result<Lookup<NoEntry, ()>, Infallible>> {
        Async::from_future(async { Ok(Lookup::Miss(())) })
    }

    fn install(
        &self,
        _id: SedimentreeId,
        _tree: MinimizedSedimentree,
        (): (),
    ) -> Async::Future<'_, Result<Install, Infallible>> {
        Async::from_future(async { Ok(Install::Current) })
    }

    fn merge(
        &self,
        _id: SedimentreeId,
        _update: Sedimentree,
    ) -> Async::Future<'_, Result<(), Infallible>> {
        Async::from_future(async { Ok(()) })
    }

    fn remove(&self, _id: SedimentreeId) -> Async::Future<'_, Result<(), Infallible>> {
        Async::from_future(async { Ok(()) })
    }
}
