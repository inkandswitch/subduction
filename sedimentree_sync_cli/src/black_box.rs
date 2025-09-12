use std::collections::HashMap;

use nonempty::NonEmpty;
use sedimentree_core::{Blob, BlobMeta, Chunk, Depth, Digest, LooseCommit, Sedimentree};
use sedimentree_sync_core::sync::ChunkRequested;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct BlackBoxOp {
    pub payload: [u8; 32],
    pub parents: Vec<Digest>,
}

impl BlackBoxOp {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self, bincode::config::standard()).expect("FIXME")
    }

    pub fn hash(&self) -> Digest {
        Digest::hash(self.serialize().as_slice())
    }
}

#[derive(Debug, Default, Clone)]
pub struct BlackBox {
    sedimentree: Sedimentree,
    ops_by_digest: HashMap<Digest, BlackBoxOp>,
    blobs_by_digest: HashMap<Digest, Blob>,
}

impl BlackBox {
    pub fn add_commit(&mut self, parents: Vec<Digest>, blob: Blob) -> Digest {
        let digest = Digest::hash(blob.as_slice());
        let blob_meta = BlobMeta::new(blob.as_slice());

        if self
            .sedimentree
            .add_commit(LooseCommit::new(digest, parents, blob_meta))
        {
            self.blobs_by_digest.insert(digest, blob);
        }

        digest
    }

    pub fn add_chunk(
        &mut self,
        start: Digest,
        checkpoints: Vec<Digest>,
        ends: NonEmpty<Digest>,
        blob: Blob,
    ) -> Digest {
        let meta = BlobMeta::new(blob.as_slice());
        let chunk = Chunk::new(start, ends, checkpoints, meta);
        let digest = chunk.digest();
        self.sedimentree.add_chunk(chunk);
        digest
    }

    pub fn rechunk(
        &self,
        chunk_requested: ChunkRequested,
        depth: Depth,
    ) -> Result<(Chunk, Blob), String> {
        if let Some(op) = self.ops_by_digest.get(&chunk_requested.head) {
            let mut ops: HashMap<Digest, BlackBoxOp> =
                HashMap::from_iter([(op.hash(), op.clone())]);
            let mut explore = op.parents.clone();
            let mut ends = Vec::new();

            while let Some(digest) = explore.pop() {
                if let Some(op) = self.ops_by_digest.get(&digest) {
                    ops.insert(digest, op.clone());

                    for parent_digest in op.parents.iter() {
                        if Depth::from(parent_digest) < depth {
                            explore.push(*parent_digest);
                        } else {
                            ends.push(digest);
                        }
                    }
                } else {
                    return Err(format!("unknown op digest: {:?}", digest));
                }
            }

            let blob =
                bincode::serde::encode_to_vec(&ops, bincode::config::standard()).expect("FIXME");

            let chunk = Chunk::new(
                chunk_requested.head,
                NonEmpty::from_vec(ends).expect("FIXME"),
                vec![],
                BlobMeta::new(&blob),
            );

            Ok((chunk, Blob::new(blob)))
        } else {
            Err(format!("unknown head digest: {:?}", chunk_requested.head))
        }
    }
}
