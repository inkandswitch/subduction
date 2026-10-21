import { test, expect } from './network-fixture';

export function networkSuite(protocol: 'websocket' | 'longpoll') {
  test(`${protocol}: discovery, known identity, concurrent clients and disconnect`, async ({ page, serverAddress }) => {
    const result = await page.evaluate(async ({ protocol, serverAddress }) => {
      const api = window.subduction;
      const address = `${protocol === 'websocket' ? 'ws' : 'http'}://${serverAddress}`;
      const a = new api.Subduction({ signer: api.createMemorySigner(), storage: api.createMemoryStorage() });
      const b = new api.Subduction({ signer: api.createMemorySigner(), storage: api.createMemoryStorage() });
      try {
        const peer = protocol === 'websocket'
          ? await a.connectDiscover(address, serverAddress)
          : await a.connectDiscoverLongPoll(address, serverAddress);
        const known = protocol === 'websocket'
          ? await b.connect(new URL(address), peer)
          : await b.connectLongPoll(address, peer);
        const peers = [await a.getConnectedPeerIds(), await b.getConnectedPeerIds()];
        await a.disconnectFromPeer(peer); await b.disconnectAll();
        return { peer, known, peers, after: [await a.getConnectedPeerIds(), await b.getConnectedPeerIds()] };
      } finally { await Promise.all([a.close(), b.close()]); a.free(); b.free(); }
    }, { protocol, serverAddress });
    expect(result.peer).toMatch(/^[0-9a-f]{64}$/);
    expect(result.known).toBe(result.peer);
    expect(result.peers).toEqual([[result.peer], [result.peer]]);
    expect(result.after).toEqual([[], []]);
  });

  test(`${protocol}: store, sync and load between two clients through Rust server`, async ({ page, serverAddress }) => {
    const result = await page.evaluate(async ({ protocol, serverAddress }) => {
      const api = window.subduction;
      const address = `${protocol === 'websocket' ? 'ws' : 'http'}://${serverAddress}`;
      const nodes = [0, 1].map(() => new api.Subduction({ signer: api.createMemorySigner(), storage: api.createMemoryStorage() }));
      try {
        await Promise.all(nodes.map(n => protocol === 'websocket' ? n.connectDiscover(address, serverAddress) : n.connectDiscoverLongPoll(address, serverAddress)));
        const tree = Array.from(crypto.getRandomValues(new Uint8Array(32)), b => b.toString(16).padStart(2, '0')).join('');
        const head = '22'.repeat(32);
        await nodes[0].storeBatch(tree, { commits: [{ head, parents: [], blob: Uint8Array.of(1, 2, 3) }] });
        await nodes[0].syncWithAllPeers(tree);
        for await (const event of nodes[1].loadAndWatch(tree, { signal: AbortSignal.timeout(10000) })) {
          if (event.type === 'commit') return { head: event.metadata.head, blob: Array.from(event.blob) };
        }
        throw new Error('stream ended before data arrived');
      } finally { await Promise.all(nodes.map(n => n.close())); nodes.forEach(n => n.free()); }
    }, { protocol, serverAddress });
    expect(result).toEqual({ head: '22'.repeat(32), blob: [1, 2, 3] });
  });

  test(`${protocol}: wrong expected peer is not registered`, async ({ page, serverAddress }) => {
    const result = await page.evaluate(async ({ protocol, serverAddress }) => {
      const api = window.subduction;
      const n = new api.Subduction({ signer: api.createMemorySigner(), storage: api.createMemoryStorage() });
      try {
        let rejected = false;
        try {
          const wrong = api.createMemorySigner().peerId();
          if (protocol === 'websocket') await n.connect(`ws://${serverAddress}`, wrong);
          else await n.connectLongPoll(`http://${serverAddress}`, wrong);
        } catch { rejected = true; }
        return { rejected, peers: await n.getConnectedPeerIds() };
      } finally { await n.close(); n.free(); }
    }, { protocol, serverAddress });
    expect(result).toEqual({ rejected: true, peers: [] });
  });
}
