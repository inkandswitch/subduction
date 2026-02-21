# Subduction Crypto

Cryptographic types for Subduction: signed payloads and verification witnesses.

> [!CAUTION]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

## Overview

This crate provides Ed25519-signed payloads with a type-state pattern that encodes verification status at the type level. This prevents "verify and forget" bugs where signature verification is called but its result is ignored.

## Type-State Flow

```
                         Local Authoring
                         ===============

  (parts, blob) ──VerifiedMeta::seal──► VerifiedMeta<T> ──into_signed──► Signed<T>
                                              │                              │
                                              │                              │
                                            store                          wire


                        Remote Receiving
                        ================

  Signed<T> ──try_verify──► VerifiedSignature<T> ──VerifiedMeta::new──► VerifiedMeta<T>
      │                                                                       │
      │                                                                       │
    wire                                                                    store
```

## Key Types

| Type | Description |
|------|-------------|
| `Signed<T>` | Payload with Ed25519 signature (unverified) |
| `VerifiedSignature<T>` | Witness: signature has been verified |
| `VerifiedMeta<T>` | Witness: signature valid AND blob matches metadata |
| `Signer<K>` | Trait for signing data with an Ed25519 key |
| `Nonce` | Random nonce for replay protection |

## Design Principles

### No Direct Payload Access on `Signed<T>`

`Signed<T>` intentionally does _not_ expose a `payload(&self) -> &T` method. Callers must go through `try_verify()` to access the payload:

```rust
let verified = signed.try_verify()?;
let payload: &T = verified.payload();
```

### Blob Integrity by Construction

When _authoring_ locally, `VerifiedMeta::seal` takes the blob and computes its metadata internally, guaranteeing the metadata matches:

```rust
let verified_blob = VerifiedBlobMeta::new(blob);
let verified: VerifiedMeta<LooseCommit> =
    VerifiedMeta::seal(&signer, (digest, parents), verified_blob).await;
```

When _receiving_ from a peer, `VerifiedMeta::new` validates that the claimed metadata matches the actual blob:

```rust
let verified_sig = signed.try_verify()?;
let verified_meta = VerifiedMeta::new(verified_sig, blob)?;  // Returns Err on mismatch
```

### Storage Only Accepts Witnesses

The `Putter` trait only accepts `VerifiedMeta<T>`, ensuring blob integrity is checked at compile time before storage.

## Modules

| Module | Description |
|--------|-------------|
| `cbor` | CBOR encoding helpers for Ed25519 types |
| `nonce` | Random nonces for replay protection |
| `signed` | The `Signed<T>` envelope and verification |
| `signer` | The `Signer<K>` trait for signing operations |
| `verified_meta` | `VerifiedMeta<T>` witness (signature + blob) |
| `verified_signature` | `VerifiedSignature<T>` witness (signature only) |

## Features

- `std` (default) — Standard library support
- `serde` — Serde serialization support
