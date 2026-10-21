// Run against the built wasm-bodge package (Rust bindings + JS storage wrapper).
// scripts/test-pojo-api.sh rebuilds it before running these tests.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { resolve } = require('node:path');
const wasm = require(resolve(process.env.POJO_WASM_PATH ?? resolve(__dirname, '../dist/wrapper/cjs/node.cjs')));
const { indexedDB } = require('../node_modules/fake-indexeddb');
const { Subduction, createMemorySigner, createMemoryStorage, createIndexedDbStorage } = wasm;
const id = n => n.toString(16).padStart(2, '0').repeat(32);
const input = (n = 2) => ({ head: id(n), parents: [], blob: Uint8Array.of(n, 9) });
const deadline = promise => Promise.race([promise, new Promise((_, reject) => {
    const timer = setTimeout(() => reject(new Error('test timed out')), 5000);
    timer.unref();
})]);
function node(t, options = {}) {
    const n = new Subduction({ signer: createMemorySigner(), storage: createMemoryStorage(), ...options });
    t.after(async () => { try { await n.close(); } finally { n.free(); } });
    return n;
}
async function metadataFor(t, blob) {
    const fixture = node(t);
    await fixture.storeCommit(id(1), { ...input(), blob });
    return (await fixture.getCommits(id(1)))[0];
}
function plain(value) {
    if (value == null || typeof value !== 'object' || value instanceof Uint8Array) return;
    assert.equal(Object.getPrototypeOf(value), Array.isArray(value) ? Array.prototype : Object.prototype);
    assert.equal(value.free, undefined);
    for (const child of Object.values(value)) plain(child);
}

// The generated module must not leak private implementation classes.
test('only Subduction is exported as a library class', () => {
    const classes = Object.entries(wasm).filter(([, value]) => typeof value === 'function' && /^class\s/.test(Function.prototype.toString.call(value)));
    assert.deepEqual(classes.map(([name]) => name), ['Subduction']);
    for (const name of ['CommitId', 'Fragment', 'MemoryStorage', 'MemorySigner', 'AuthenticatedTransport', 'StoredUpdatesDriver']) {
        assert.equal(wasm[name], undefined);
    }
    // Keep the explicit public declaration in sync with generated bindings.
    const declaration = require('node:fs').readFileSync(resolve(__dirname, '../src/api.d.ts'), 'utf8');
    const body = declaration.split('export class Subduction {')[1].split('\n}')[0];
    for (const match of body.matchAll(/^\s*(static )?(\w+)\(/gm)) {
        const [, isStatic, name] = match;
        if (name !== 'constructor') assert.equal(typeof (isStatic ? Subduction : Subduction.prototype)[name], 'function', name);
    }
});

test('raw mixed batches, signed storage records and streams are recursively plain', async t => {
    const storage = createMemoryStorage();
    const save = storage.saveBatchAll;
    storage.saveBatchAll = async (tree, commits, fragments) => {
        assert.equal(tree, id(171)); plain(commits); plain(fragments);
        assert.ok(commits[0].signedCommit instanceof Uint8Array);
        assert.ok(fragments[0].signedFragment instanceof Uint8Array);
        return save(tree, commits, fragments);
    };
    const n = node(t, { storage });
    await n.storeBatch(id(171).toUpperCase(), {
        commits: [input()],
        fragments: [{ head: id(3), boundary: [id(2)], checkpoints: ['AB'.repeat(12)], blob: Uint8Array.of(4) }],
    });
    const commits = await n.getCommits(id(171));
    const fragments = await n.getFragments(id(171));
    plain(commits); plain(fragments);
    assert.equal(commits[0].head, id(2));
    assert.equal(commits[0].blobMeta.sizeBytes, 2n);
    assert.deepEqual(fragments[0].checkpoints, ['ab'.repeat(12)]);
    const stored = await storage.loadCommit(id(171), id(2));
    assert.ok(stored.signed instanceof Uint8Array);
    const reloaded = node(t, { storage });
    assert.deepEqual(await reloaded.getCommits(id(171)), commits);
    assert.deepEqual(await reloaded.getFragments(id(171)), fragments);
    const events = n.watchStored(id(171));
    let count = 0;
    for await (const event of events) {
        plain(event);
        if (event.type === 'storageChecked') break;
        count++;
    }
    assert.ok(count > 0);
    await n.close();
    assert.equal(commits[0].head, id(2)); // no lifetime tied to Wasm/node
    assert.deepEqual(stored.blob, Uint8Array.of(2, 9));
});

test('arguments are captured before any async work and frozen input is optional', async t => {
    const n = node(t);
    const commit = input();
    const batch = { commits: [commit], fragments: [] };
    const pending = n.storeBatch(id(1), batch);
    commit.head = id(8); commit.parents.push(id(9)); commit.blob.fill(0);
    batch.commits.length = 0;
    await pending;
    const [saved] = await n.getCommits(id(1));
    assert.equal(saved.head, id(2)); assert.deepEqual(saved.parents, []);
    assert.deepEqual(await n.getBlobs(id(1)), [Uint8Array.of(2, 9)]);
    await n.storeCommit(id(1), Object.freeze({ ...input(3), parents: Object.freeze([]) }));
});

test('all batch validation precedes signing/writing and errors identify field paths', async t => {
    const signer = createMemorySigner();
    let signed = 0, written = 0;
    const storage = createMemoryStorage();
    const save = storage.saveBatchAll;
    storage.saveBatchAll = (...args) => { written++; return save(...args); };
    const n = node(t, { storage, signer: { verifyingKey: () => signer.verifyingKey(), sign: bytes => { signed++; return signer.sign(bytes); } } });
    await assert.rejects(n.storeBatch(id(1), { commits: [input(), { ...input(3), head: 'zz'.repeat(32) }] }), /commits\[1\].head/);
    assert.equal(signed, 0); assert.equal(written, 0);
    for (const bad of ['', '0x' + id(1), ' ' + id(1), 'é'.repeat(32)]) {
        await assert.rejects(n.storeCommit(bad, input()), /64 hex characters/);
    }
    await assert.rejects(n.storeFragment(id(1), { head: id(3), boundary: [], checkpoints: [id(4)], blob: Uint8Array.of(1) }), /checkpoints\[0\]/);
    await assert.rejects(n.storeCommit(id(1), { ...input(), blob: [1, 2] }), /Uint8Array/);
    await assert.rejects(n.storeCommit(id(1), { ...input(), blob: new Uint8Array(new SharedArrayBuffer(2)) }), /shared-memory/);
    await assert.rejects(n.fetchBlobs(id(1), { timeoutMilliseconds: NaN }), /u32 integer/);
    await assert.rejects(n.syncWithAllPeers(id(1), { subscribe: 'yes' }), /boolean/);
    const metadata = await metadataFor(t, input().blob);
    const mismatched = { ...metadata, head: id(3), blobMeta: { ...metadata.blobMeta, sizeBytes: 99n } };
    await assert.rejects(n.storeSedimentree(id(1), { commits: [metadata, mismatched], fragments: [] }, [input().blob]), /no matching blob/);
    assert.equal(signed, 0); assert.equal(written, 0);
});

test('count limits apply after deduplication', async t => {
    const n = node(t);
    await n.storeCommit(id(1), { ...input(), parents: Array(300).fill(id(4)) });
    const parents = Array.from({length: 256}, (_, i) => i.toString(16).padStart(64, '0'));
    await assert.rejects(n.storeCommit(id(1), { ...input(3), parents }), /255/);
});

test('built records validate metadata and tree identity; whole-tree APIs remain available', async t => {
    const n = node(t);
    const blob = Uint8Array.of(1, 2);
    const metadata = await metadataFor(t, blob);
    await n.storeBuiltBatch(id(1), { commits: [{ metadata, blob }] });
    await assert.rejects(n.storeBuiltBatch(id(3), { commits: [{ metadata, blob }] }), /different sedimentree/);
    await assert.rejects(n.storeBuiltBatch(id(1), { commits: [{ metadata, blob: Uint8Array.of(9) }] }), /does not match/);
    const other = node(t);
    await other.storeSedimentree(id(1), { commits: [metadata], fragments: [] }, [blob]);
    assert.deepEqual(await other.getHeads(id(1)), [id(2)]);
    assert.deepEqual(await other.addBatch(id(1), {}), {});
});

test('callback storage failures retain their cause and corrupt blobs are rejected', async t => {
    const storage = createMemoryStorage();
    const n = node(t, { storage });
    await n.storeCommit(id(1), input());
    const stored = await storage.loadCommit(id(1), id(2));
    const cause = new Error('backend failure');
    storage.loadAllCommits = () => { throw cause; };
    // A fresh node has no resident metadata and must load through the callback.
    const other = node(t, { storage });
    await assert.rejects(other.getHeads(id(1)), error => error.name === 'SedimentreeStorageError' && error.cause === cause);
    storage.loadAllCommits = async () => [{ signed: stored.signed, blob: Uint8Array.of(99) }];
    const third = node(t, { storage });
    await assert.rejects(third.getHeads(id(1)), error => error.name === 'SedimentreeStorageError');
});

test('link, hex policies, result keys and live streams work without value wrappers', async t => {
    const checked = [];
    const policy = {
        async authorizeConnect(peer) { assert.match(peer, /^[0-9a-f]{64}$/); checked.push(peer); },
        async authorizeFetch(peer, tree) { assert.equal(typeof peer, 'string'); assert.equal(typeof tree, 'string'); },
        async authorizePut(requestor, author, tree) { [requestor, author, tree].forEach(x => assert.equal(typeof x, 'string')); },
        async filterAuthorizedFetch(peer, ids) { return ids.map(id => id.toUpperCase()); },
    };
    const a = node(t, { policy }), b = node(t, { policy });
    await deadline(Subduction.link(a, b));
    await a.storeCommit(id(1), input());
    const result = await deadline(a.syncWithAllPeers(id(1)));
    plain(result);
    assert.equal(Object.keys(result).length, 1);
    assert.equal(typeof result[Object.keys(result)[0]].success, 'boolean');
    const stream = b.loadAndWatch(id(1));
    try {
        for (;;) { const { value } = await deadline(stream.next()); if (value.type === 'commit') { assert.equal(value.metadata.head, id(2)); break; } }
    } finally { stream.close(); }
    assert.equal(checked.length, 2);
});

test('custom structural transports authenticate known peers and clean up mismatches', async t => {
    const signerA = createMemorySigner(), signerB = createMemorySigner();
    const a = node(t, { signer: signerA }), b = node(t, { signer: signerB });
    const channel = new MessageChannel();
    const ta = wasm.makeMessagePortTransport(channel.port1), tb = wasm.makeMessagePortTransport(channel.port2);
    assert.equal(Object.getPrototypeOf(ta), Object.prototype);
    const peers = await deadline(Promise.all([
        a.connectTransport(ta, { expectedPeerId: signerB.peerId() }),
        b.acceptTransport(tb, { expectedPeerId: signerA.peerId() }),
    ]));
    assert.deepEqual(peers, [signerB.peerId(), signerA.peerId()]);
});

test('single-fragment publication preserves checkpoint prefixes exactly', async t => {
    const n = node(t);
    const fragment = { head: id(2), boundary: [id(3)], checkpoints: ['CD'.repeat(12), 'ab'.repeat(12), 'AB'.repeat(12)], blob: Uint8Array.of(6) };
    await n.storeFragment(id(1), fragment);
    await n.addFragment(id(1), fragment);
    const [metadata] = await n.getFragments(id(1));
    assert.deepEqual(metadata.checkpoints, ['ab'.repeat(12), 'cd'.repeat(12)]);
    const reloaded = node(t, { storage: n.storage });
    assert.deepEqual(await reloaded.getFragments(id(1)), [metadata]);
});

test('abort and close preserve lifecycle behavior', async t => {
    const n = node(t);
    const controller = new AbortController();
    const stream = n.loadAndWatch(id(1), { signal: controller.signal });
    const reason = new Error('cancel');
    controller.abort(reason);
    await assert.rejects(stream.next(), e => e === reason);
    await n.close();
    await assert.rejects(n.storeCommit(id(1), input()), e => e.name === 'SubductionClosedError');
    assert.throws(() => n.watchStored(id(1)), e => e.name === 'SubductionClosedError');
});

test('IndexedDB preserves the version-1 record layout and reopens without wrappers', async t => {
    const name = `pojo-${Date.now()}-${Math.random()}`;
    const storage = await createIndexedDbStorage(indexedDB, name);
    t.after(() => storage.close());
    const n = node(t, { storage });
    await n.storeBatch(id(1), { commits: [input()], fragments: [{ head: id(3), boundary: [], checkpoints: [], blob: Uint8Array.of(7) }] });
    await n.close();
    storage.close();
    const reopened = await createIndexedDbStorage(indexedDB, name);
    t.after(() => reopened.close());
    const other = node(t, { storage: reopened });
    assert.deepEqual(await other.getHeads(id(1)), [id(2), id(3)]);
    assert.equal((await other.getCommits(id(1)))[0].head, id(2));
    const open = indexedDB.open(name, 1);
    const db = await new Promise((resolve, reject) => { open.onsuccess = () => resolve(open.result); open.onerror = () => reject(open.error); });
    try {
        const req = db.transaction('commits').objectStore('commits').get([id(1), id(2)]);
        const persisted = await new Promise((resolve, reject) => { req.onsuccess = () => resolve(req.result); req.onerror = () => reject(req.error); });
        assert.equal(persisted.sedimentree_id, id(1)); assert.equal(persisted.digest, id(2));
        assert.ok(persisted.signed instanceof Uint8Array); assert.ok(persisted.blob instanceof Uint8Array);
    } finally { db.close(); }
});

test('cross-realm arrays and Buffer slices are copied, not coerced', async t => {
    const vm = require('node:vm');
    const n = node(t);
    const foreign = vm.runInNewContext('({ parents: [], blob: Uint8Array.of(3, 4) })');
    await n.storeCommit(id(1), { head: id(2), ...foreign });
    const buffer = Buffer.from([99, 5, 6, 99]);
    await n.storeCommit(id(1), { head: id(3), parents: [], blob: buffer.subarray(1, 3) });
    assert.deepEqual((await n.getBlobs(id(1))).map(b => [...b]).sort(), [[3, 4], [5, 6]]);
    const bytes = Uint8Array.of(1);
    structuredClone(bytes.buffer, { transfer: [bytes.buffer] });
    await assert.rejects(n.storeCommit(id(1), { head: id(4), parents: [], blob: bytes }));
});

test('JS storage copies foreign-realm views and distinguishes shared from detached buffers', async t => {
    const vm = require('node:vm');
    const db = await createIndexedDbStorage(indexedDB, `realms-${Date.now()}`);
    t.after(() => db.close());
    for (const storage of [createMemoryStorage(), db]) {
        const foreign = vm.runInNewContext('Uint8Array.of(7, 8, 9).subarray(1)');
        // User-defined accessors must not hide a view's actual buffer or offset.
        Object.defineProperty(foreign, 'buffer', { get() { throw new Error('not intrinsic'); } });
        await storage.saveCommit(id(1), id(2), foreign, foreign);
        foreign.fill(0);
        const saved = await storage.loadCommit(id(1), id(2));
        assert.deepEqual([...saved.signed], [8, 9]);
        assert.deepEqual([...saved.blob], [8, 9]);
        const shared = vm.runInNewContext('new Uint8Array(new SharedArrayBuffer(2))');
        await assert.rejects(storage.saveCommit(id(1), id(3), shared, shared), /shared-memory/);
        const detached = Uint8Array.of(1);
        structuredClone(detached.buffer, { transfer: [detached.buffer] });
        await assert.rejects(storage.saveCommit(id(1), id(3), detached, detached), e =>
            e instanceof TypeError && !e.message.includes('shared-memory'));
        assert.equal(await storage.loadCommit(id(1), id(3)), null);
    }
});

test('getter failures reject without signing and each input field is read once', async t => {
    const n = node(t);
    const reason = new Error('getter failed');
    let promise;
    assert.doesNotThrow(() => { promise = n.storeCommit(id(1), { get head() { throw reason; } }); });
    await assert.rejects(promise, error => error === reason);
    const parents = [];
    Object.defineProperty(parents, 0, { get() { throw reason; } });
    assert.doesNotThrow(() => { promise = n.storeCommit(id(1), { ...input(), parents }); });
    await assert.rejects(promise, error => error === reason);
    const revoked = Proxy.revocable([], {}); revoked.revoke();
    assert.doesNotThrow(() => { promise = n.storeCommit(id(1), { ...input(), parents: revoked.proxy }); });
    await assert.rejects(promise, TypeError);
    const reads = { head: 0, parents: 0, blob: 0 };
    await n.storeCommit(id(1), {
        get head() { reads.head++; return id(2); },
        get parents() { reads.parents++; return []; },
        get blob() { reads.blob++; return Uint8Array.of(1); },
    });
    assert.deepEqual(reads, { head: 1, parents: 1, blob: 1 });
});

test('constructor validates numeric options and initial signer identity', () => {
    const signer = createMemorySigner(), storage = createMemoryStorage();
    for (const maxResidentTrees of [-1, 1.5, NaN, Infinity, '10']) {
        assert.throws(() => new Subduction({ signer, storage, maxResidentTrees }), /u32 integer/);
    }
    assert.throws(() => new Subduction({ signer: { sign() {}, verifyingKey: () => Uint8Array.of(1) }, storage }), /expected 32 bytes/);
    const reason = new Error('identity failure');
    assert.throws(() => new Subduction({ signer: { sign() {}, verifyingKey() { throw reason; } }, storage }), e => e === reason);
});

test('admitted custom transports are owned even when close precedes the first poll', async t => {
    const n = node(t);
    let closes = 0;
    const transport = {
        async sendBytes() {}, recvBytes: () => new Promise(() => {}),
        async disconnect() { closes++; }, onDisconnect() {},
    };
    const connecting = n.connectTransport(transport, { serviceName: 'test' });
    const rejected = assert.rejects(connecting, error => error.name === 'SubductionClosedError');
    await n.close(); await rejected;
    assert.equal(closes, 1);
    await assert.rejects(n.connectTransport(transport, { serviceName: 'test' }), error => error.name === 'SubductionClosedError');
    assert.equal(closes, 1); // Already-closed admission must not take ownership.
});

test('custom metrics receive canonical commit IDs and return numeric depths', async t => {
    const ids = [];
    const n = node(t, { hashMetricOverride: commit => { ids.push(commit); return 1; } });
    const request = await n.storeCommit(id(1), { ...input(), head: 'AB'.repeat(32) });
    assert.deepEqual(request, { head: 'ab'.repeat(32), depth: 1 });
    assert.ok(ids.every(id => /^[0-9a-f]{64}$/.test(id)));
    assert.equal(n.commitDepth(id(2)), 1);
    const [metadata] = await n.getCommits(id(1));
    const wrongSize = { ...metadata, blobMeta: { ...metadata.blobMeta, sizeBytes: 2 } };
    await assert.rejects(n.storeBuiltBatch(id(1), { commits: [{ metadata: wrongSize, blob: input().blob }] }), /bigint/);
});
