//! The stock [`Effects`](super::Effects) implementation.
//!
//! [`Config`] carries no data. It only names one type per capability slot,
//! and each `Has*` impl bounds only its own slot.

use core::{fmt, marker::PhantomData};

use future_form::FutureForm;
use subduction_crypto::signer::Signer;

use super::{
    minimized_tree_store::HasMinimizedTreeStore, policy::HasStoragePolicy, signer::HasSigner,
    spawner::HasSpawner, storage::HasStorage, timer::HasTimer,
};
use crate::{
    minimized_tree_store::{MinimizedTreeStore, memory::MemoryMinimizedTreeStore},
    policy::storage::StoragePolicy,
    spawn::Spawn,
    storage::traits::Storage,
    timeout::Timeout,
};

/// The stock effects type, naming each component type.
///
/// Slots, in order: storage, policy, signer, timer, spawner, minimized tree
/// store. The tree store defaults to the in-process
/// [`MemoryMinimizedTreeStore`].
pub struct Config<Store, Policy, Sign, Timer, Sp, Trees = MemoryMinimizedTreeStore>(
    #[allow(clippy::type_complexity)] PhantomData<fn() -> (Store, Policy, Sign, Timer, Sp, Trees)>,
);

impl<Async: FutureForm, Store, Policy, Sign, Timer, Sp, Trees: MinimizedTreeStore<Async>>
    HasMinimizedTreeStore<Async> for Config<Store, Policy, Sign, Timer, Sp, Trees>
{
    type MinimizedTreeStore = Trees;
}

impl<Async: FutureForm, Store, Policy: StoragePolicy<Async>, Sign, Timer, Sp, Trees>
    HasStoragePolicy<Async> for Config<Store, Policy, Sign, Timer, Sp, Trees>
{
    type FetchDisallowed = Policy::FetchDisallowed;
    type Policy = Policy;
    type PutDisallowed = Policy::PutDisallowed;
}

impl<Async: FutureForm, Store, Policy, Sign: Signer<Async>, Timer, Sp, Trees> HasSigner<Async>
    for Config<Store, Policy, Sign, Timer, Sp, Trees>
{
    type Signer = Sign;
}

impl<Async: FutureForm, Store, Policy, Sign, Timer, Sp: Spawn<Async> + Clone, Trees>
    HasSpawner<Async> for Config<Store, Policy, Sign, Timer, Sp, Trees>
{
    type Spawner = Sp;
}

impl<Async: FutureForm, Store: Storage<Async>, Policy, Sign, Timer, Sp, Trees> HasStorage<Async>
    for Config<Store, Policy, Sign, Timer, Sp, Trees>
{
    type Storage = Store;
    type StorageError = Store::Error;
}

impl<Async: FutureForm, Store, Policy, Sign, Timer: Timeout<Async> + Clone, Sp, Trees>
    HasTimer<Async> for Config<Store, Policy, Sign, Timer, Sp, Trees>
{
    type Timer = Timer;
}

// Manual impls: derives would demand the bound on every component type,
// though `Config` holds none of them.

impl<Store, Policy, Sign, Timer, Sp, Trees> Clone
    for Config<Store, Policy, Sign, Timer, Sp, Trees>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<Store, Policy, Sign, Timer, Sp, Trees> Copy for Config<Store, Policy, Sign, Timer, Sp, Trees> {}

impl<Store, Policy, Sign, Timer, Sp, Trees> fmt::Debug
    for Config<Store, Policy, Sign, Timer, Sp, Trees>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Config")
    }
}
