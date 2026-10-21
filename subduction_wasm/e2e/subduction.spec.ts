import { test, expect } from '@playwright/test';
import { URL } from './config';

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  await page.waitForFunction(() => window.subductionReady);
});

test('the only exported library class is Subduction', async ({ page }) => {
  const classes = await page.evaluate(() => Object.entries(window.subduction)
    .filter(([, value]) => typeof value === 'function' && /^class\s/.test(Function.prototype.toString.call(value)))
    .map(([name]) => name));
  expect(classes).toEqual(['Subduction']);
});

for (const backend of ['memory', 'indexeddb'] as const) {
  test(`${backend}: raw records, heads, binary blobs and cold-node persistence`, async ({ page }) => {
    const result = await page.evaluate(async backend => {
      const api = window.subduction;
      const storage = backend === 'memory' ? api.createMemoryStorage()
        : await api.createIndexedDbStorage(indexedDB, `pojo-${crypto.randomUUID()}`);
      const signer = await api.createWebCryptoSigner();
      const a = new api.Subduction({ signer, storage });
      const b = new api.Subduction({ signer, storage });
      const tree = 'ab'.repeat(32), head = 'cd'.repeat(32), child = 'ef'.repeat(32);
      try {
        const unknown = await b.getHeads('00'.repeat(32));
        await a.storeCommit(tree.toUpperCase(), { head, parents: [], blob: Uint8Array.of(1, 2) });
        await a.storeCommit(tree, { head: child, parents: [head], blob: Uint8Array.of(3, 4) });
        const heads = await b.getHeads(tree);
        const commits = await b.getCommits(tree);
        const decoded = commits!.find(c => c.head === child)!;
        const blob = await b.getBlob(tree, decoded.blobMeta.digest);
        const metadataPlain = commits!.every(c => Object.getPrototypeOf(c) === Object.prototype && typeof c.head === 'string');
        const sameStorage = a.storage === storage;
        await b.removeSedimentree(tree);
        return { unknown, heads, decodedHead: decoded.head, blob: Array.from(blob!), metadataPlain, sameStorage,
          remaining: await b.sedimentreeIds(), size: String(decoded.blobMeta.sizeBytes) };
      } finally {
        await Promise.all([a.close(), b.close()]); a.free(); b.free();
        if ('close' in storage) (storage as { close(): void }).close();
      }
    }, backend);
    expect(result).toEqual({ unknown: undefined, heads: ['ef'.repeat(32)], decodedHead: 'ef'.repeat(32),
      blob: [3, 4], metadataPlain: true, sameStorage: true, remaining: [], size: '2' });
  });
}

test('mixed batches snapshot input, accept frozen records, and expose plain stream events', async ({ page }) => {
  const result = await page.evaluate(async () => {
    const api = window.subduction;
    const n = new api.Subduction({ signer: api.createMemorySigner(), storage: api.createMemoryStorage() });
    const tree = '11'.repeat(32), head = '22'.repeat(32);
    const bytes = Uint8Array.of(4, 5);
    try {
      const commit = { head, parents: [] as string[], blob: bytes };
      const promise = n.storeBatch(tree, { commits: [commit], fragments: [
        Object.freeze({ head: '33'.repeat(32), boundary: Object.freeze([]), checkpoints: Object.freeze(['AA'.repeat(12)]), blob: Uint8Array.of(6) })
      ] });
      bytes.fill(0); commit.head = '44'.repeat(32); commit.parents.push('55'.repeat(32));
      await promise;
      const events: string[] = [];
      for await (const event of n.watchStored(tree)) {
        if (Object.getPrototypeOf(event) !== Object.prototype) throw new Error('non-plain event');
        events.push(event.type);
        if (event.type === 'storageChecked') break;
      }
      return { events, head: (await n.getCommits(tree))![0].head,
        parents: (await n.getCommits(tree))![0].parents,
        blobs: (await n.getBlobs(tree)).map(b => Array.from(b)),
        checkpoints: (await n.getFragments(tree))![0].checkpoints };
    } finally { await n.close(); n.free(); }
  });
  expect(result.head).toBe('22'.repeat(32));
  expect(result.parents).toEqual([]);
  expect(result.blobs).toContainEqual([4, 5]);
  expect(result.checkpoints).toEqual(['aa'.repeat(12)]);
  expect(result.events).toContain('commit'); expect(result.events).toContain('fragment');
  expect(result.events.at(-1)).toBe('storageChecked');
});

test('validation, duplicate publication, unknown queries and lifecycle errors', async ({ page }) => {
  const result = await page.evaluate(async () => {
    const api = window.subduction;
    const n = new api.Subduction({ signer: api.createMemorySigner(), storage: api.createMemoryStorage(), hashMetricOverride: () => 0 });
    const tree = '11'.repeat(32), head = '22'.repeat(32);
    const commit = { head, parents: [], blob: Uint8Array.of(1) };
    const errors: string[] = [];
    try {
      const unknown = [await n.getCommits(tree), await n.getFragments(tree), await n.getBlob(tree, head)];
      for (const invalid of ['bad', '0x' + tree]) {
        try { await n.storeCommit(invalid, commit); } catch (e) { errors.push(String(e)); }
      }
      await n.addCommit(tree, commit); await n.addCommit(tree, commit);
      const count = (await n.getCommits(tree))!.length;
      const empty = await n.syncWithAllPeers(tree);
      const depth = n.commitDepth(head);
      const controller = new AbortController();
      const stream = n.loadAndWatch(tree, { signal: controller.signal });
      controller.abort('cancelled');
      let abort;
      try { await stream.next(); } catch (e) { abort = e; }
      await n.close();
      let closed;
      try { await n.getHeads(tree); } catch (e) { closed = (e as Error).name; }
      return { unknown, errors, count, empty, depth, abort, closed };
    } finally { await n.close(); n.free(); }
  });
  expect(result.unknown).toEqual([undefined, undefined, undefined]);
  expect(result.errors.every(e => e.includes('64 hex characters'))).toBe(true);
  expect(result.count).toBe(1); expect(result.empty).toEqual({}); expect(result.depth).toBe(0);
  expect(result.abort).toBe('cancelled'); expect(result.closed).toBe('SubductionClosedError');
});
