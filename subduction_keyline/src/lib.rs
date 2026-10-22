//! Keyline-backed authorization for Subduction.
//!
//! [Keyline] is Keyhive's convergent authority graph: a set of signed
//! delegations and revocations over Ed25519 identifiers. A replica's answer
//! to "what power does X have over Y?" depends only on the certificates it
//! holds, never on the order they arrived in.
//!
//! [`policy::KeylinePolicy`] implements Subduction's
//! [`ConnectionPolicy`] and [`StoragePolicy`] over any
//! [`Keyline`] store. It does not depend on `keyhive_core`,
//! so a relay can run it with only a store.
//!
//! ```text
//! subduction_core                 subduction_keyline           keyline
//! ┌───────────────────┐          ┌──────────────────┐         ┌─────────────────┐
//! │ ConnectionPolicy  │◄─────────│ KeylinePolicy<K> │────────►│ K: Keyline<F>   │
//! │ StoragePolicy     │          └──────────────────┘         │ effective_power │
//! └───────────────────┘                                       └─────────────────┘
//! ```
//!
//! [Keyline]: https://github.com/inkandswitch/keyhive/tree/split/11-keyline-in-core/keyline
//! [`ConnectionPolicy`]: subduction_core::policy::connection::ConnectionPolicy
//! [`StoragePolicy`]: subduction_core::policy::storage::StoragePolicy
//! [`Keyline`]: keyline::contract::Keyline

#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod policy;
