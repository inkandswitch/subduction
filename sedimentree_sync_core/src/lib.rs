#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(
    clippy::dbg_macro,
    clippy::expect_used,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::todo,
    clippy::unwrap_used,
    future_incompatible,
    let_underscore,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    rust_2021_compatibility
)]
#![deny(
    clippy::all,
    clippy::cargo,
    clippy::pedantic,
    rust_2018_idioms,
    unreachable_pub,
    unused_extern_crates
)]
#![forbid(unsafe_code)]

use automerge::{Automerge, transaction::Transactable};
use futures::Future;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeerId(String);

impl PeerId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for PeerId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DocumentId(String);

impl DocumentId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for DocumentId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for DocumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StorageId(String);

impl StorageId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StorageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Message {
    pub action: String, // FIXME enum
    pub sender_id: PeerId,
    pub target_id: PeerId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerMetadata {
    pub storage_id: Option<StorageId>,
    pub is_ephemeral: Option<bool>,
}

pub trait NetworkAdapter {
    fn peer_id(&self) -> PeerId;
    fn peer_metadata(&self) -> Option<PeerMetadata>;
    fn is_ready(&self) -> bool;
    fn when_ready(&self) -> impl Future<Output = Result<(), String>>; // FIXME error type
    fn connect(&self, peer_id: &PeerId, peer_metadata: &Option<PeerMetadata>);
    fn send(&self, message: Message);
    fn disconnect(&mut self);
}

#[derive(Debug, Clone)]
pub struct SedimentreeSync<N: NetworkAdapter> {
    docs: HashMap<DocumentId, Automerge>,
    network_adapters: HashMap<String, N>,
}

impl<N: NetworkAdapter> SedimentreeSync<N> {
    #[tracing::instrument(skip_all)]
    pub fn new(docs: HashMap<DocumentId, Automerge>, network_adapters: HashMap<String, N>) -> Self {
        Self {
            docs,
            network_adapters
        }
    }

    // FIXME return more info about unstarted adapters
    #[tracing::instrument(skip_all)]
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
    pub fn on<F: Fn() -> ()>(&mut self, event: String, callback: &F) -> Result<(), String> {
        todo!();
        Ok(())
    }

    // FIXME closure tyep?
    // FIXME error type
    pub fn off<F: Fn() -> ()>(&mut self, doc_id: DocumentId, callback: &F) -> Result<(), String> {
        todo!();
        Ok(())
    }

    // fn entry(&mut self, doc_id: DocumentId) -> Entry<'_, DocumentId, HashSet<Callback>> {
    //     self.adapters.entry(doc_id)
    // }

    pub async fn stop(&mut self) {
        todo!()
    }

    pub async fn when_ready(&self) -> bool {
        true
    }


    pub async fn new_commit(&mut self, _document_id: DocumentId, hash: String, data: Vec<u8>) {
        todo!()
    }
}
