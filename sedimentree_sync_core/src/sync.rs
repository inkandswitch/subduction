use crate::{
    message::Message, network::adapter::NetworkAdapter, peer::id::PeerId,
    storage::adapter::StorageAdapter,
};
use sedimentree_core::{Sedimentree, SedimentreeId};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, str::FromStr};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Digest([u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Error)]
#[error("Unknown event: {0}")]
pub struct UnknownEvent(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Event {
    Message,
    OpenDoc,
    SyncState,
    Metrics,
}

impl FromStr for Event {
    type Err = UnknownEvent;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "message" => Ok(Event::Message),
            "open_doc" => Ok(Event::OpenDoc),
            "sync_state" => Ok(Event::SyncState),
            "metrics" => Ok(Event::Metrics),
            _ => Err(UnknownEvent(s.to_string())),
        }
    }
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct SedimentreeSync<N: NetworkAdapter, S: StorageAdapter> {
    sedimentrees: HashMap<SedimentreeId, Sedimentree>,
    network_adapters: HashMap<String, N>,
    storage_adapters: HashMap<String, S>,
}

impl<N: NetworkAdapter, S: StorageAdapter> SedimentreeSync<N, S> {
    #[tracing::instrument(skip_all, fields(sedimentree_ids = ?sedimentrees.keys()))]
    pub fn new(
        sedimentrees: HashMap<SedimentreeId, Sedimentree>,
        network_adapters: HashMap<String, N>,
        storage_adapters: HashMap<String, S>,
    ) -> Self {
        Self {
            sedimentrees,
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
    pub async fn stop(&mut self) {
        todo!()
    }

    #[tracing::instrument(skip(self))]
    pub async fn when_ready(&self) -> bool {
        true
    }

    #[tracing::instrument(skip(self))]
    pub async fn find(&self, id: SedimentreeId) -> Option<&Sedimentree> {
        if let Some(sedimentree) = self.sedimentrees.get(&id) {
            tracing::info!("Found sedimentree with id {:?}", id);

            // let mut u8s = Vec::new();
            // for commit in sedimentree.commits() {
            //     u8s.extend_from_slice(commit);
            // }

            return Some(sedimentree);
        }

        for (_name, adapter) in &self.network_adapters {
            adapter.send(Message {
                action: "find".to_string(),
                sender_id: "peer-id".to_string().into(),
                target_id: "me".to_string().into(),
            });
        }

        todo!("FIXME")

        // let mut am = automerge::AutoCommit::new();
        // am.put(automerge::ROOT, "foo", "bar").unwrap();
        // Some(vec![am.save()])
    }

    pub fn on_change(&self, id: SedimentreeId, data: &[u8]) -> Result<(), String> {
        // self.sedimentrees.get_mut(&id).ok_or("FIXME".to_string())?;
        todo!("FIXME");
        // .fixme_apply_change(data);

        Ok(())
    }

    pub fn on_bundle_required(&self, id: SedimentreeId, start: Digest, end: Digest) {}

    // #[tracing::instrument]
    // fn entry(&mut self, doc_id: SedimentreeId) -> Entry<'_, SedimentreeId, HashSet<Callback>> {
    //     self.adapters.entry(doc_id)
    // }

    #[tracing::instrument(skip(self))]
    pub async fn new_commit(&mut self, _id: SedimentreeId, hash: Digest, data: Vec<u8>) {
        todo!()
    }
}
