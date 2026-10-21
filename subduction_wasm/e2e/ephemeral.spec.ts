import { test, expect } from './network-fixture';

test('topics are validated hex strings', async ({ page }) => {
  const errors = await page.evaluate(async () => {
    const api = window.subduction;
    const n = new api.Subduction({ signer: api.createMemorySigner(), storage: api.createMemoryStorage() });
    const errors: string[] = [];
    try {
      for (const topic of ['', 'aa'.repeat(31), 'gg'.repeat(32)]) {
        try { await n.publishEphemeral(topic, Uint8Array.of(1)); } catch (e) { errors.push(String(e)); }
      }
      await n.subscribeEphemeral(['AB'.repeat(32)]);
      await n.unsubscribeEphemeral(['ab'.repeat(32)]);
      return errors;
    } finally { await n.close(); n.free(); }
  });
  expect(errors).toHaveLength(3);
  expect(errors.every(e => e.includes('64 hex characters'))).toBe(true);
});

test('ephemeral gossip uses plain callback values, topic scope, distinct nonces and unsubscribe', async ({ page, serverAddress }) => {
  const result = await page.evaluate(async serverAddress => {
    const api = window.subduction;
    const received: Array<Array<{ topic: string; sender: string; bytes: number[] }>> = [[], [], []];
    const signers = [0, 1, 2].map(() => api.createMemorySigner());
    const nodes = signers.map((signer, i) => new api.Subduction({ signer, storage: api.createMemoryStorage(),
      onEphemeral: (topic, sender, payload) => received[i].push({ topic, sender, bytes: Array.from(payload) }) }));
    const topic = 'ab'.repeat(32), otherTopic = 'cd'.repeat(32);
    const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
    const until = async (predicate: () => boolean) => {
      const deadline = Date.now() + 10000;
      while (!predicate()) { if (Date.now() > deadline) throw new Error('ephemeral delivery timed out'); await wait(20); }
    };
    try {
      await Promise.all(nodes.map(n => n.connectDiscover(`ws://${serverAddress}`, serverAddress)));
      await Promise.all(nodes.map(n => n.subscribeEphemeral([topic])));
      // Subscription messages have no acknowledgement; allow remote dispatch.
      await wait(300);
      await nodes[0].publishEphemeral(topic.toUpperCase(), Uint8Array.of(7));
      await nodes[0].publishEphemeral(topic, Uint8Array.of(7));
      await until(() => received[1].length >= 2 && received[2].length >= 2);
      const first = received.map(events => events.slice());
      await nodes[0].publishEphemeral(otherTopic, Uint8Array.of(8));
      await wait(200);
      const scoped = received[1].length === 2 && received[2].length === 2;
      await nodes[1].unsubscribeEphemeral([topic]); await wait(300);
      await nodes[0].publishEphemeral(topic, Uint8Array.of(9));
      await until(() => received[2].some(e => e.bytes[0] === 9));
      await wait(200);
      return { first, scoped, unsubscribed: !received[1].some(e => e.bytes[0] === 9), sender: signers[0].peerId() };
    } finally { await Promise.all(nodes.map(n => n.close())); nodes.forEach(n => n.free()); }
  }, serverAddress);
  for (const events of result.first.slice(1)) {
    expect(events).toEqual([
      { topic: 'ab'.repeat(32), sender: result.sender, bytes: [7] },
      { topic: 'ab'.repeat(32), sender: result.sender, bytes: [7] },
    ]);
  }
  expect(result.scoped).toBe(true); expect(result.unsubscribed).toBe(true);
});
