// Structural backends, exported by the wasm-bodge wrapper without a Wasm round
// trip. Only persist locally signed or previously verified envelopes: reloading
// through Rust checks encoding and blob agreement, not signatures again.
//
// These local-realm intrinsic getters work on foreign-realm typed arrays because
// they inspect internal slots, not the receiver's prototype or instanceof. Calling
// them directly also bypasses user-defined buffer/offset/length/tag getters.
const typedArray = Object.getPrototypeOf(Uint8Array.prototype);
const getBuffer = Object.getOwnPropertyDescriptor(typedArray, 'buffer').get;
const getOffset = Object.getOwnPropertyDescriptor(typedArray, 'byteOffset').get;
const getLength = Object.getOwnPropertyDescriptor(typedArray, 'byteLength').get;
const getTag = Object.getOwnPropertyDescriptor(typedArray, Symbol.toStringTag).get;
const arrayBufferLength = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, 'byteLength').get;

function id(value) {
    if (typeof value !== 'string' || !/^[0-9a-fA-F]{64}$/.test(value)) {
        throw new TypeError('id: expected 64 hex characters');
    }
    return value.toLowerCase();
}
function bytes(value) {
    if (getTag.call(value) !== 'Uint8Array') throw new TypeError('expected Uint8Array');
    const buffer = getBuffer.call(value);
    // Require ordinary ArrayBuffer backing. In an unmodified JS environment the
    // intrinsic typed-array buffer getter above can only return ArrayBuffer or
    // SharedArrayBuffer. Detached ArrayBuffers pass this check but fail when we
    // construct the view below; do not relabel that failure as shared memory.
    try { arrayBufferLength.call(buffer); }
    catch { throw new TypeError('expected ordinary ArrayBuffer backing (shared-memory buffers are not supported)'); }
    return new Uint8Array(new Uint8Array(buffer, getOffset.call(value), getLength.call(value)));
}
function record(tree, head, signed, blob) {
    return { sedimentree_id: id(tree), digest: id(head), signed: bytes(signed), blob: bytes(blob) };
}
function output(value) {
    return value == null ? null : { signed: bytes(value.signed), blob: bytes(value.blob) };
}
function batch(tree, commits, fragments) {
    tree = id(tree);
    if (!Array.isArray(commits) || !Array.isArray(fragments)) throw new TypeError('expected batch arrays');
    return [tree,
        commits.map(c => record(tree, c.commitId, c.signedCommit, c.blob)),
        fragments.map(f => record(tree, f.fragmentHead, f.signedFragment, f.blob))];
}

export function memoryStorage() {
    const ids = new Set();
    const commits = new Map();
    const fragments = new Map();
    const key = (tree, head) => `${tree}:${head}`;
    const all = (map, tree) => Array.from(map.values()).filter(r => r.sedimentree_id === tree);
    const methods = (map, singular, plural) => ({
        [`save${singular}`]: async (tree, head, signed, blob) => {
            const r = record(tree, head, signed, blob);
            map.set(key(r.sedimentree_id, r.digest), r);
            ids.add(r.sedimentree_id);
        },
        [`load${singular}`]: async (tree, head) => output(map.get(key(id(tree), id(head)))),
        [`list${singular}Ids`]: async tree => all(map, id(tree)).map(r => r.digest),
        [`loadAll${plural}`]: async tree => all(map, id(tree)).map(output),
        [`delete${singular}`]: async (tree, head) => { map.delete(key(id(tree), id(head))); },
        [`deleteAll${plural}`]: async tree => {
            tree = id(tree);
            for (const r of all(map, tree)) map.delete(key(tree, r.digest));
        },
    });
    return {
        async saveSedimentreeId(tree) { ids.add(id(tree)); },
        async deleteSedimentreeId(tree) { ids.delete(id(tree)); },
        async loadAllSedimentreeIds() { return [...ids]; },
        async containsSedimentreeId(tree) { return ids.has(id(tree)); },
        ...methods(commits, 'Commit', 'Commits'),
        ...methods(fragments, 'Fragment', 'Fragments'),
        async saveBatchAll(tree, cs, fs) {
            // All reads/copies/validation precede any mutation.
            [tree, cs, fs] = batch(tree, cs, fs);
            for (const r of cs) commits.set(key(tree, r.digest), r);
            for (const r of fs) fragments.set(key(tree, r.digest), r);
            ids.add(tree);
            return cs.length + fs.length;
        },
    };
}

const ID_STORE = 'sedimentree_ids';
const INDEX = 'by_sedimentree_id';
function request(req) {
    return new Promise((resolve, reject) => {
        req.onsuccess = () => { req.onsuccess = req.onerror = null; resolve(req.result); };
        req.onerror = () => { req.onsuccess = req.onerror = null; reject(req.error); };
    });
}
function completed(tx) {
    return new Promise((resolve, reject) => {
        const clean = () => { tx.oncomplete = tx.onabort = tx.onerror = null; };
        tx.oncomplete = () => { clean(); resolve(); };
        tx.onabort = () => { clean(); reject(tx.error ?? new Error('IndexedDB transaction aborted')); };
        // Abort is the terminal error event. Do not suppress the default abort.
        tx.onerror = () => {};
    });
}

export async function indexedDbStorage(factory, name) {
    if (!factory || typeof factory.open !== 'function') throw new TypeError('expected IDBFactory');
    if (name !== undefined && typeof name !== 'string') throw new TypeError('expected database name');
    const opening = factory.open(name ?? '@automerge/subduction/db', 1);
    opening.onupgradeneeded = () => {
        const db = opening.result;
        if (!db.objectStoreNames.contains(ID_STORE)) db.createObjectStore(ID_STORE);
        for (const name of ['commits', 'fragments']) {
            const store = db.objectStoreNames.contains(name)
                ? opening.transaction.objectStore(name)
                : db.createObjectStore(name, { keyPath: ['sedimentree_id', 'digest'] });
            if (!store.indexNames.contains(INDEX)) store.createIndex(INDEX, 'sedimentree_id');
        }
    };
    let db;
    try { db = await request(opening); }
    finally { opening.onupgradeneeded = null; }
    db.onversionchange = () => db.close();
    let closed = false;
    async function transaction(stores, mode, run) {
        if (closed) throw new Error('IndexedDB storage is closed');
        const tx = db.transaction(stores, mode);
        const done = completed(tx);
        try {
            const result = await run(tx);
            await done;
            return result;
        } catch (error) {
            try { tx.abort(); } catch { /* already finished */ }
            await done.catch(() => {});
            throw error;
        }
    }
    function methods(storeName, singular, plural) {
        return {
            [`save${singular}`]: async (tree, head, signed, blob) => {
                const r = record(tree, head, signed, blob);
                await transaction([storeName, ID_STORE], 'readwrite', tx => {
                    tx.objectStore(storeName).put(r);
                    tx.objectStore(ID_STORE).put(1, r.sedimentree_id);
                });
            },
            [`load${singular}`]: async (tree, head) => {
                const key = [id(tree), id(head)];
                return transaction([storeName], 'readonly', async tx => output(await request(tx.objectStore(storeName).get(key))));
            },
            [`list${singular}Ids`]: async tree => {
                tree = id(tree);
                return transaction([storeName], 'readonly', async tx =>
                    (await request(tx.objectStore(storeName).index(INDEX).getAllKeys(tree))).map(key => id(key[1])));
            },
            [`loadAll${plural}`]: async tree => {
                tree = id(tree);
                return transaction([storeName], 'readonly', async tx =>
                    (await request(tx.objectStore(storeName).index(INDEX).getAll(tree))).map(output));
            },
            [`delete${singular}`]: async (tree, head) => {
                const key = [id(tree), id(head)];
                await transaction([storeName], 'readwrite', tx => { tx.objectStore(storeName).delete(key); });
            },
            [`deleteAll${plural}`]: async tree => {
                tree = id(tree);
                await transaction([storeName], 'readwrite', tx => new Promise((resolve, reject) => {
                    const cursor = tx.objectStore(storeName).index(INDEX).openCursor(tree);
                    cursor.onerror = () => reject(cursor.error);
                    cursor.onsuccess = () => {
                        const next = cursor.result;
                        if (!next) { cursor.onsuccess = cursor.onerror = null; resolve(); return; }
                        next.delete(); next.continue();
                    };
                }));
            },
        };
    }
    // Version 1, store/index names, compound keys and byte fields are identical
    // to the previous Rust backend. No migration or re-encoding is necessary.
    return {
        close() { closed = true; db.onversionchange = null; db.close(); },
        async saveSedimentreeId(tree) {
            tree = id(tree);
            await transaction([ID_STORE], 'readwrite', tx => { tx.objectStore(ID_STORE).put(1, tree); });
        },
        async deleteSedimentreeId(tree) {
            tree = id(tree);
            await transaction([ID_STORE], 'readwrite', tx => { tx.objectStore(ID_STORE).delete(tree); });
        },
        async loadAllSedimentreeIds() {
            return transaction([ID_STORE], 'readonly', async tx => (await request(tx.objectStore(ID_STORE).getAllKeys())).map(id));
        },
        async containsSedimentreeId(tree) {
            tree = id(tree);
            return transaction([ID_STORE], 'readonly', async tx => (await request(tx.objectStore(ID_STORE).count(tree))) !== 0);
        },
        ...methods('commits', 'Commit', 'Commits'),
        ...methods('fragments', 'Fragment', 'Fragments'),
        async saveBatchAll(tree, commits, fragments) {
            [tree, commits, fragments] = batch(tree, commits, fragments);
            return transaction([ID_STORE, 'commits', 'fragments'], 'readwrite', tx => {
                for (const r of commits) tx.objectStore('commits').put(r);
                for (const r of fragments) tx.objectStore('fragments').put(r);
                tx.objectStore(ID_STORE).put(1, tree);
                return commits.length + fragments.length;
            });
        },
    };
}
