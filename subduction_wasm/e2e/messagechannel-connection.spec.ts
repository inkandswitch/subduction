import { test, expect } from '@playwright/test';
import { URL } from './config';

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  await page.waitForFunction(() => window.subductionReady);
});

for (const mode of ['known', 'discovery', 'link'] as const) {
  test(`MessageChannel ${mode}: authentication and replication`, async ({ page }) => {
    const result = await page.evaluate(async mode => {
      const api = window.subduction;
      const sa = api.createMemorySigner(), sb = api.createMemorySigner();
      const a = new api.Subduction({ signer: sa, storage: api.createMemoryStorage() });
      const b = new api.Subduction({ signer: sb, storage: api.createMemoryStorage() });
      try {
        if (mode === 'link') await api.Subduction.link(a, b);
        else {
          const channel = new MessageChannel();
          const ta = api.makeMessagePortTransport(channel.port1), tb = api.makeMessagePortTransport(channel.port2);
          await Promise.all([
            a.connectTransport(ta, mode === 'known' ? { expectedPeerId: sb.peerId() } : { serviceName: 'pojo-test' }),
            b.acceptTransport(tb, mode === 'known' ? { expectedPeerId: sa.peerId() } : { serviceName: 'pojo-test' }),
          ]);
        }
        const tree = '11'.repeat(32), head = '22'.repeat(32);
        await a.storeCommit(tree, { head, parents: [], blob: Uint8Array.of(1, 2, 3) });
        const stream = b.loadAndWatch(tree, { signal: AbortSignal.timeout(10000) });
        let received;
        for await (const event of stream) {
          if (event.type === 'commit') { received = { head: event.metadata.head, blob: Array.from(event.blob) }; break; }
        }
        const peers = [await a.getConnectedPeerIds(), await b.getConnectedPeerIds()];
        await a.disconnectFromPeer(sb.peerId());
        return { received, peers, expected: [sb.peerId(), sa.peerId()], disconnected: await a.getConnectedPeerIds() };
      } finally { await Promise.all([a.close(), b.close()]); a.free(); b.free(); }
    }, mode);
    expect(result.received).toEqual({ head: '22'.repeat(32), blob: [1, 2, 3] });
    expect(result.peers).toEqual(result.expected.map(peer => [peer]));
    expect(result.disconnected).toEqual([]);
  });
}

test('wrong expected identity is rejected before registration and closes the transport', async ({ page }) => {
  const result = await page.evaluate(async () => {
    const api = window.subduction;
    const sa = api.createMemorySigner(), sb = api.createMemorySigner(), wrong = api.createMemorySigner();
    const a = new api.Subduction({ signer: sa, storage: api.createMemoryStorage() });
    const b = new api.Subduction({ signer: sb, storage: api.createMemoryStorage() });
    const channel = new MessageChannel();
    const ta = api.makeMessagePortTransport(channel.port1), tb = api.makeMessagePortTransport(channel.port2);
    let closed = 0;
    const disconnect = ta.disconnect;
    ta.disconnect = () => { closed++; return disconnect(); };
    try {
      const [result] = await Promise.allSettled([
        a.connectTransport(ta, { expectedPeerId: wrong.peerId() }),
        b.acceptTransport(tb, { expectedPeerId: sa.peerId() }),
      ]);
      return { rejected: result.status === 'rejected', peers: await a.getConnectedPeerIds(), closed };
    } finally { await Promise.all([a.close(), b.close()]); a.free(); b.free(); }
  });
  expect(result.rejected).toBe(true); expect(result.peers).toEqual([]); expect(result.closed).toBeGreaterThan(0);
});
