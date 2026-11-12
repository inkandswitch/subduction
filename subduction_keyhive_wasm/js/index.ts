// The package API: the Wasm bindings, plus the adapter from a
// `@keyhive/keyhive` `Keyhive` to the `KeyhiveAccess` they take.
export * from '#wasm-bodge/bindings';
export { keyhiveAccess } from './keyhiveAccess.js';
