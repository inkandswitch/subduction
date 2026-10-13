# automerge_subduction_wasm

> [!WARNING]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

Wasm bindings for syncing Automerge documents via Subduction.

## Overview

This crate combines the Automerge document ID helpers (e.g. `commitIdOfBase58Id`) with `subduction_wasm` to provide a complete solution for syncing Automerge documents in browser and Node.js environments.

## Building and releasing

Build `dist/` with `bodge automerge_subduction_wasm` inside `nix develop`. See
[RELEASING.md](../RELEASING.md) for the full release steps.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
