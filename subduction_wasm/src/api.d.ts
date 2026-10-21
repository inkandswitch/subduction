export interface SubductionOptions {
    signer: Signer;
    storage: SedimentreeStorage;
    serviceName?: string;
    hashMetricOverride?: (commitId: CommitId) => number;
    maxResidentTrees?: number;
    policy?: Policy;
    ephemeralPolicy?: EphemeralPolicy;
    onRemoteHeads?: (id: SedimentreeId, peer: PeerId, heads: CommitId[]) => void;
    onEphemeral?: (topic: Topic, sender: PeerId, payload: Uint8Array) => void;
    defaultTimeoutMilliseconds?: number;
}
export interface SyncOptions { subscribe?: boolean; timeoutMilliseconds?: number }
export interface TimeoutOptions { timeoutMilliseconds?: number }
export interface FragmentRequested { head: CommitId; depth: number }
export interface ErrorInfo { name: string; message: string }
export interface SyncStats {
    commitsReceived: number; fragmentsReceived: number;
    commitsSent: number; fragmentsSent: number;
    totalReceived: number; totalSent: number;
    isEmpty: boolean; remoteHeads: CommitId[];
}
export interface PeerSyncResult { success: boolean; stats: SyncStats; transportErrors: ErrorInfo[] }
/** Keys are lowercase hex. An absent peer has no property (lookup is undefined). */
export type PeerResults = Record<PeerId, PeerSyncResult>;
export interface SedimentreeHeads { id: SedimentreeId; heads: CommitId[] }
export type TransportAudience = { expectedPeerId: PeerId; serviceName?: never } | { serviceName: string; expectedPeerId?: never };

/** Data arguments are converted synchronously into owned Rust values before the
 * returned promise is scheduled. No input is consumed or detached. Outputs need
 * no disposal. Services are retained interfaces, not snapshots. */
export class Subduction {
    constructor(options: SubductionOptions);
    free(): void;
    close(): Promise<void>;
    readonly storage: SedimentreeStorage;
    storeCommit(id: SedimentreeId, commit: CommitInput): Promise<FragmentRequested | undefined>;
    addCommit(id: SedimentreeId, commit: CommitInput): Promise<FragmentRequested | undefined>;
    storeFragment(id: SedimentreeId, fragment: FragmentInput): Promise<void>;
    addFragment(id: SedimentreeId, fragment: FragmentInput): Promise<void>;
    storeBatch(id: SedimentreeId, batch: BatchInput): Promise<void>;
    addBatch(id: SedimentreeId, batch: BatchInput, options?: TimeoutOptions): Promise<PeerResults>;
    storeBuiltBatch(id: SedimentreeId, batch: BuiltBatchInput): Promise<void>;
    addBuiltBatch(id: SedimentreeId, batch: BuiltBatchInput, options?: TimeoutOptions): Promise<PeerResults>;
    storeCommitsBatch(id: SedimentreeId, commits: readonly CommitInput[]): Promise<void>;
    addCommitsBatch(id: SedimentreeId, commits: readonly CommitInput[], options?: TimeoutOptions): Promise<PeerResults>;
    storeFragmentsBatch(id: SedimentreeId, fragments: readonly FragmentInput[]): Promise<void>;
    addFragmentsBatch(id: SedimentreeId, fragments: readonly FragmentInput[], options?: TimeoutOptions): Promise<PeerResults>;
    storeSedimentree(id: SedimentreeId, tree: SedimentreeData, blobs: readonly Uint8Array[]): Promise<void>;
    addSedimentree(id: SedimentreeId, tree: SedimentreeData, blobs: readonly Uint8Array[], options?: TimeoutOptions): Promise<PeerResults>;
    removeSedimentree(id: SedimentreeId): Promise<void>;
    getBlob(id: SedimentreeId, digest: Digest): Promise<Uint8Array | undefined>;
    getBlobs(id: SedimentreeId): Promise<Uint8Array[]>;
    fetchBlobs(id: SedimentreeId, options?: TimeoutOptions): Promise<Uint8Array[] | undefined>;
    getCommits(id: SedimentreeId): Promise<CommitMetadata[] | undefined>;
    getFragments(id: SedimentreeId): Promise<FragmentMetadata[] | undefined>;
    getHeads(id: SedimentreeId): Promise<CommitId[] | undefined>;
    getAllHeads(): Promise<SedimentreeHeads[]>;
    sedimentreeIds(): Promise<SedimentreeId[]>;
    getConnectedPeerIds(): Promise<PeerId[]>;
    commitDepth(id: CommitId): number;
    watchStored(id: SedimentreeId): StoredUpdates;
    loadAndWatch(id: SedimentreeId, options?: LoadAndWatchOptions): StoredUpdates;
    syncWithPeer(peer: PeerId, id: SedimentreeId, options?: SyncOptions): Promise<PeerSyncResult>;
    syncWithAllPeers(id: SedimentreeId, options?: SyncOptions): Promise<PeerResults>;
    fullSyncWithPeer(peer: PeerId, options?: SyncOptions): Promise<PeerSyncResult>;
    fullSyncWithAllPeers(options?: TimeoutOptions): Promise<PeerSyncResult>;
    connect(address: string | URL, expectedPeerId: PeerId): Promise<PeerId>;
    connectDiscover(address: string | URL, serviceName?: string): Promise<PeerId>;
    connectLongPoll(address: string, expectedPeerId: PeerId): Promise<PeerId>;
    connectDiscoverLongPoll(address: string, serviceName?: string): Promise<PeerId>;
    connectTransport(transport: Transport, audience: TransportAudience): Promise<PeerId>;
    acceptTransport(transport: Transport, audience: TransportAudience): Promise<PeerId>;
    static link(a: Subduction, b: Subduction): Promise<void>;
    disconnectAll(): Promise<void>;
    disconnectFromPeer(peer: PeerId): Promise<boolean>;
    publishEphemeral(topic: Topic, payload: Uint8Array): Promise<void>;
    subscribeEphemeral(topics: readonly Topic[]): Promise<void>;
    unsubscribeEphemeral(topics: readonly Topic[]): Promise<void>;
    registerFrameHandler(handler?: FrameHandler | null): Promise<void>;
    sendKeyhiveMessage(payload: Uint8Array, peer: PeerId): Promise<void>;
}
