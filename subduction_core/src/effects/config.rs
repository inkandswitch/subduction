//! The stock [`Effects`](super::Effects) implementation.
//!
//! [`Config`] carries no data. It only names one type per capability slot,
//! and each `Has*` impl bounds only its own slot.

use core::{fmt, marker::PhantomData};

use future_form::FutureForm;
use subduction_crypto::signer::Signer;

use super::{
    policy::HasStoragePolicy, signer::HasSigner, spawner::HasSpawner, storage::HasStorage,
    timer::HasTimer,
};
use crate::{
    policy::storage::StoragePolicy, spawn::Spawn, storage::traits::Storage, timeout::Timeout,
};

/// The stock effects type, naming each component type.
///
/// Slots, in order: storage, policy, signer, timer, spawner.
pub struct Config<Store, Policy, Sign, Timer, Sp>(
    #[allow(clippy::type_complexity)] PhantomData<fn() -> (Store, Policy, Sign, Timer, Sp)>,
);

impl<Async: FutureForm, Store, Policy: StoragePolicy<Async>, Sign, Timer, Sp>
    HasStoragePolicy<Async> for Config<Store, Policy, Sign, Timer, Sp>
{
    type FetchDisallowed = Policy::FetchDisallowed;
    type Policy = Policy;
    type PutDisallowed = Policy::PutDisallowed;
}

impl<Async: FutureForm, Store, Policy, Sign: Signer<Async>, Timer, Sp> HasSigner<Async>
    for Config<Store, Policy, Sign, Timer, Sp>
{
    type Signer = Sign;
}

impl<Async: FutureForm, Store, Policy, Sign, Timer, Sp: Spawn<Async> + Clone> HasSpawner<Async>
    for Config<Store, Policy, Sign, Timer, Sp>
{
    type Spawner = Sp;
}

impl<Async: FutureForm, Store: Storage<Async>, Policy, Sign, Timer, Sp> HasStorage<Async>
    for Config<Store, Policy, Sign, Timer, Sp>
{
    type Storage = Store;
    type StorageError = Store::Error;
}

impl<Async: FutureForm, Store, Policy, Sign, Timer: Timeout<Async> + Clone, Sp> HasTimer<Async>
    for Config<Store, Policy, Sign, Timer, Sp>
{
    type Timer = Timer;
}

// Manual impls: derives would demand the bound on every component type,
// though `Config` holds none of them.

impl<Store, Policy, Sign, Timer, Sp> Clone for Config<Store, Policy, Sign, Timer, Sp> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Store, Policy, Sign, Timer, Sp> Copy for Config<Store, Policy, Sign, Timer, Sp> {}

impl<Store, Policy, Sign, Timer, Sp> fmt::Debug for Config<Store, Policy, Sign, Timer, Sp> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Config")
    }
}
