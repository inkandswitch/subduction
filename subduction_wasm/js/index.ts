// The package API: Rust owns protocol/lifecycle logic; JS owns built-in storage.
export * from '#wasm-bodge/bindings';

import type { SedimentreeStorage } from '#wasm-bodge/bindings';
import { memoryStorage, indexedDbStorage } from './storage.js';

/** Caller-owned in-memory storage. Records contain independently owned bytes. */
export function createMemoryStorage(): SedimentreeStorage {
    return memoryStorage() as SedimentreeStorage;
}

/** Open the existing version-1 IndexedDB format. Close it when no longer used. */
export async function createIndexedDbStorage(
    factory: IDBFactory,
    name?: string,
): Promise<SedimentreeStorage & { close(): void }> {
    return await indexedDbStorage(factory, name) as SedimentreeStorage & { close(): void };
}
