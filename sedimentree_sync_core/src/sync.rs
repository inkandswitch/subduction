use crate::{
    message::Message, network::adapter::NetworkAdapter, peer::id::PeerId,
    storage::adapter::StorageAdapter,
};
use automerge::{transaction::Transactable, Automerge};
use sedimentree_core::DocumentId;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SedimentreeSync<N: NetworkAdapter, S: StorageAdapter> {
    docs: HashMap<DocumentId, Automerge>,
    network_adapters: HashMap<String, N>,
    storage_adapters: HashMap<String, S>,
}

impl<N: NetworkAdapter, S: StorageAdapter> SedimentreeSync<N, S> {
    #[tracing::instrument(skip_all, fields(docs = ?docs))]
    pub fn new(
        docs: HashMap<DocumentId, Automerge>,
        network_adapters: HashMap<String, N>,
        storage_adapters: HashMap<String, S>,
    ) -> Self {
        Self {
            docs,
            network_adapters,
            storage_adapters,
        }
    }

    // FIXME return more info about unstarted adapters
    #[tracing::instrument(skip(self))]
    pub fn start(&mut self) {
        for (name, adapter) in &self.network_adapters {
            if adapter.is_ready() {
                tracing::info!("Adapter {} is ready", name);
                adapter.send(Message {
                    action: "find".to_string(),
                    sender_id: adapter.peer_id().clone(),
                    target_id: PeerId::new("me".to_string()),
                });
            } else {
                tracing::warn!("Adapter {} is not ready", name);
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn find(&self, doc_id: DocumentId) -> Option<Vec<Vec<u8>>> {
        for (_name, adapter) in &self.network_adapters {
            adapter.send(Message {
                action: "find".to_string(),
                sender_id: "peer-id".to_string().into(),
                target_id: "me".to_string().into(),
            });
        }

        let mut am = automerge::AutoCommit::new();
        am.put(automerge::ROOT, "foo", "bar").unwrap();
        Some(vec![am.save()])
    }

    // FIXME closure tyep?
    // FIXME error type
    #[tracing::instrument(skip(self, callback))]
    pub fn on<F: Fn() -> ()>(&mut self, event: String, callback: &F) -> Result<(), String> {
        todo!();
        Ok(())
    }

    // FIXME closure tyep?
    // FIXME error type
    #[tracing::instrument(skip(self, callback))]
    pub fn off<F: Fn() -> ()>(&mut self, doc_id: DocumentId, callback: &F) -> Result<(), String> {
        todo!();
        Ok(())
    }

    // #[tracing::instrument]
    // fn entry(&mut self, doc_id: DocumentId) -> Entry<'_, DocumentId, HashSet<Callback>> {
    //     self.adapters.entry(doc_id)
    // }

    #[tracing::instrument(skip(self))]
    pub async fn stop(&mut self) {
        todo!()
    }

    #[tracing::instrument(skip(self))]
    pub async fn when_ready(&self) -> bool {
        true
    }

    #[tracing::instrument(skip(self))]
    pub async fn new_commit(&mut self, _document_id: DocumentId, hash: String, data: Vec<u8>) {
        todo!()
    }
}
