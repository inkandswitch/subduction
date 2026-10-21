export type CommitId = string;
export type SedimentreeId = string;
export type PeerId = string;
export type Digest = string;
export type Topic = string;
export type Checkpoint = string;
export type Depth = number;
export interface BlobMeta { digest: Digest; sizeBytes: bigint }
export interface CommitMetadata {
    sedimentreeId: SedimentreeId;
    head: CommitId;
    parents: CommitId[];
    blobMeta: BlobMeta;
}
export interface FragmentMetadata {
    sedimentreeId: SedimentreeId;
    head: CommitId;
    boundary: CommitId[];
    checkpoints: Checkpoint[];
    blobMeta: BlobMeta;
}
export interface CommitInput {
    readonly head: CommitId;
    readonly parents: readonly CommitId[];
    readonly blob: Uint8Array;
}
export interface FragmentInput {
    readonly head: CommitId;
    readonly boundary: readonly CommitId[];
    readonly checkpoints: readonly Checkpoint[];
    readonly blob: Uint8Array;
}
export interface BatchInput {
    readonly commits?: readonly CommitInput[];
    readonly fragments?: readonly FragmentInput[];
}
export interface StoredCommit { signed: Uint8Array; blob: Uint8Array }
export interface StoredFragment { signed: Uint8Array; blob: Uint8Array }
export interface BuiltCommitInput { readonly metadata: CommitMetadata; readonly blob: Uint8Array }
export interface BuiltFragmentInput { readonly metadata: FragmentMetadata; readonly blob: Uint8Array }
export interface BuiltBatchInput {
    readonly commits?: readonly BuiltCommitInput[];
    readonly fragments?: readonly BuiltFragmentInput[];
}
export interface SedimentreeData { commits: CommitMetadata[]; fragments: FragmentMetadata[] }
/** Trusted persistence of locally signed or previously verified envelopes.
 * Reloads check encoding and blob agreement, not signatures again. Never use
 * these callbacks to admit unverified externally supplied records. */
export interface SedimentreeStorage {
    saveSedimentreeId(id: SedimentreeId): Promise<void>;
    deleteSedimentreeId(id: SedimentreeId): Promise<void>;
    loadAllSedimentreeIds(): Promise<SedimentreeId[]>;
    containsSedimentreeId?(id: SedimentreeId): Promise<boolean>;
    saveCommit(id: SedimentreeId, head: CommitId, signed: Uint8Array, blob: Uint8Array): Promise<void>;
    loadCommit(id: SedimentreeId, head: CommitId): Promise<StoredCommit | null>;
    listCommitIds(id: SedimentreeId): Promise<CommitId[]>;
    loadAllCommits(id: SedimentreeId): Promise<StoredCommit[]>;
    deleteCommit(id: SedimentreeId, head: CommitId): Promise<void>;
    deleteAllCommits(id: SedimentreeId): Promise<void>;
    saveFragment(id: SedimentreeId, head: CommitId, signed: Uint8Array, blob: Uint8Array): Promise<void>;
    loadFragment(id: SedimentreeId, head: CommitId): Promise<StoredFragment | null>;
    listFragmentIds(id: SedimentreeId): Promise<CommitId[]>;
    loadAllFragments(id: SedimentreeId): Promise<StoredFragment[]>;
    deleteFragment(id: SedimentreeId, head: CommitId): Promise<void>;
    deleteAllFragments(id: SedimentreeId): Promise<void>;
    saveBatchAll(id: SedimentreeId,
        commits: Array<{commitId: CommitId; signedCommit: Uint8Array; blob: Uint8Array}>,
        fragments: Array<{fragmentHead: CommitId; signedFragment: Uint8Array; blob: Uint8Array}>
    ): Promise<number>;
}
