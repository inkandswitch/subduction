//! # Subduction
//!
//! Subduction is a sync protocol for local-first applications built on [`Sedimentree`].
//!
//! ## Getting Started
//!
//! Use [`SubductionBuilder`] to construct a [`Subduction`] instance.
//! The builder tracks required fields at the type level — calling
//! [`build`] is a compile error until `signer`, `storage`, and
//! `spawner` have all been set.
//!
//! ```ignore
//! use subduction_core::subduction::builder::SubductionBuilder;
//!
//! // Required: signer (identity), storage + policy, and spawner.
//! // Everything else has sensible defaults.
//! let (subduction, handler, listener, manager) = SubductionBuilder::new()
//!     .signer(my_signer)
//!     .storage(my_storage, Arc::new(my_policy))
//!     .spawner(TokioSpawn)
//!     .build();
//!
//! // Spawn the background tasks
//! tokio::spawn(listener);
//! tokio::spawn(manager);
//!
//! // Connect to a peer and sync
//! let peer_id = connection.peer_id();
//! subduction.add_connection(connection).await?;
//! subduction.full_sync_with_peer(&peer_id, true, None).await;
//! ```
//!
//! ### Optional configuration
//!
//! ```ignore
//! let (subduction, handler, listener, manager) = SubductionBuilder::new()
//!     .signer(my_signer)
//!     .storage(my_storage, Arc::new(my_policy))
//!     .spawner(TokioSpawn)
//!     .discovery_id(DiscoveryId::new(b"sync.example.com"))
//!     .nonce_cache(NonceCache::new(Duration::from_secs(300)))
//!     .max_pending_blob_requests(20_000)
//!     .build();
//! ```
//!
//! For hydration from storage (e.g. Wasm), pre-populate the sedimentree
//! map and pass it to the builder:
//!
//! ```ignore
//! let sedimentrees = Arc::new(ShardedMap::new());
//! // ... load from storage, populate sedimentrees ...
//!
//! let (subduction, handler, listener, manager) = SubductionBuilder::new()
//!     .signer(my_signer)
//!     .storage(my_storage, Arc::new(my_policy))
//!     .spawner(WasmSpawn)
//!     .sedimentrees(sedimentrees)
//!     .build();
//! ```
//!
//! See the [`subduction`] module for the full **API guide** covering
//! naming conventions, API levels, and common patterns.
//!
//! ## Modules
//!
//! - [`subduction`] — Main sync manager, builder, and API guide
//! - [`connection`] — Connection traits and handshake protocol
//! - [`handler`] — Message handler trait for dispatch extensibility
//! - [`policy`] — Authorization policies ([`ConnectionPolicy`], [`StoragePolicy`])
//! - [`storage`] — Storage capabilities ([`Fetcher`], [`Putter`], [`Destroyer`])
//! - [`peer`] — Peer identity ([`PeerId`])
//!
//! For cryptographic primitives ([`Signer`], [`Signed`]), see [`subduction_crypto`].
//!
//! [`Sedimentree`]: sedimentree_core::sedimentree::Sedimentree
//! [`SubductionBuilder`]: subduction::builder::SubductionBuilder
//! [`build`]: subduction::builder::SubductionBuilder::build
//! [`ConnectionPolicy`]: policy::ConnectionPolicy
//! [`StoragePolicy`]: policy::StoragePolicy
//! [`Fetcher`]: storage::fetcher::Fetcher
//! [`Putter`]: storage::putter::Putter
//! [`Destroyer`]: storage::destroyer::Destroyer
//! [`Signer`]: subduction_crypto::signer::Signer
//! [`Signed`]: subduction_crypto::signed::Signed
//! [`PeerId`]: peer::id::PeerId

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod authenticated;
pub mod connection;
pub mod handler;
pub mod handshake;
pub mod multiplexer;
pub mod nonce_cache;
pub mod peer;
pub mod policy;
pub mod remote_heads;
pub mod sharded_map;
pub mod storage;
pub mod subduction;
pub mod timeout;
pub mod timestamp;
pub mod transport;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub mod metrics;

#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;
