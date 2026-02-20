//! Cryptographic types for Subduction.
//!
//! This crate provides signed payload types and verification witnesses:
//!
//! - [`Signed<T>`] — A payload with an Ed25519 signature (unverified)
//! - [`VerifiedSignature<T>`] — Witness that the signature is valid
//! - [`VerifiedMeta<T>`] — Witness that signature is valid AND blob matches metadata
//! - [`Signer<K>`] — Trait for signing data with an ed25519 key
//!
//! # Type-State Flow
//!
//! ```text
//! Local:    T  ──seal──►  VerifiedSignature<T>  ──into_signed──►  Signed<T> (wire)
//! Remote:   Signed<T>  ──try_verify──►  VerifiedSignature<T>  ──with_blob──►  VerifiedMeta<T>
//! Storage:  Signed<T>  ──decode_payload──►  T  (trusted, no wrapper)
//! ```
//!
//! # Crate Organization
//!
//! - [`signed`] — The `Signed<T>` envelope and related types
//! - [`signer`] — The `Signer<K>` trait for signing operations
//! - [`verified_signature`] — The `VerifiedSignature<T>` witness
//! - [`verified_meta`] — The `VerifiedMeta<T>` witness (includes blob verification)
//! - [`cbor`] — CBOR encoding helpers for Ed25519 types

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod cbor;
pub mod signed;
pub mod signer;
pub mod verified_meta;
pub mod verified_signature;
