// Typechecked against the actual package declarations by scripts/test-pojo-api.sh.
import {
    Subduction, createMemorySigner, createWebCryptoSigner, createMemoryStorage,
    createIndexedDbStorage, type StoredUpdate, type CommitMetadata, type Signer,
} from '../';

const id = 'ab'.repeat(32);
const signer: Signer = createMemorySigner();
const webSigner: Promise<Signer> = createWebCryptoSigner();
const storage = createMemoryStorage();
const node = new Subduction({ signer, storage, hashMetricOverride: id => id.startsWith('00') ? 1 : 0,
    onRemoteHeads: (tree, peer, heads) => { tree.toLowerCase(); peer.toLowerCase(); heads.map(h => h.toLowerCase()); },
    onEphemeral: (topic, sender, bytes) => { topic.toLowerCase(); sender.toLowerCase(); bytes.slice(); },
});
const raw = Object.freeze({ head: id, parents: Object.freeze([id]), blob: Uint8Array.of(1) });
node.storeCommit(id, raw);
node.storeBatch(id, { commits: [raw], fragments: [{ head: id, boundary: [], checkpoints: [id.slice(0, 24)], blob: raw.blob }] });
node.addBatch(id, {}, { timeoutMilliseconds: 1000 });
node.storeCommitsBatch(id, [raw]);
node.getHeads(id).then(heads => heads?.map(h => h.toLowerCase()));
node.syncWithAllPeers(id).then(results => results[id]?.stats.remoteHeads);
declare const metadata: CommitMetadata;
node.storeBuiltBatch(id, { commits: [{ metadata, blob: raw.blob }] });
const depth: number = node.commitDepth(id);
createIndexedDbStorage(indexedDB).then(storage => storage.close());
const stream: AsyncIterable<StoredUpdate> = node.loadAndWatch(id, { signal: new AbortController().signal });
async function read() {
    for await (const event of stream) {
        if (event.type === 'commit') event.metadata.head.toLowerCase();
        if (event.type === 'fragment') event.metadata.checkpoints.map(cp => cp.toLowerCase());
        if (event.type === 'peerSynced') event.peerId.toLowerCase();
    }
}
// @ts-expect-error IDs are strings, not binary or wrapper objects.
node.getHeads(new Uint8Array(32));
// @ts-expect-error Raw publication requires a blob.
node.storeCommit(id, { head: id, parents: [] });
// @ts-expect-error Checkpoints are hex prefixes, not byte arrays.
node.storeFragment(id, { head: id, boundary: [], checkpoints: [new Uint8Array(12)], blob: raw.blob });
// @ts-expect-error Plain metadata has no disposal method.
metadata.free();
void webSigner; void read; void depth;
