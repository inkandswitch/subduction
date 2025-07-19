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

use std::{collections::{hash_map::Entry, HashMap, HashSet}, hash::{DefaultHasher, Hash, Hasher}};

use wasm_bindgen::prelude::*;
use js_sys::Function;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocumentId([u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Callback {
    function: Function,
    source_digest: u64
}

impl From<Function> for Callback {
    fn from(function: Function) -> Self {
        let fn_str: String = function.to_string().into();

        let mut hasher = DefaultHasher::new();
        fn_str.hash(&mut hasher);
        let source_digest = hasher.finish();

        Self { function, source_digest }
    }
}

impl Hash for Callback {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.source_digest.hash(state);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SedimentreeNetwork {
    adapters: HashMap<DocumentId, HashSet<Callback>>, // Or something
}

impl SedimentreeNetwork {
    pub fn register(&mut self, doc_id: DocumentId, js_callback: &Function) -> Result<(), JsValue> {
        self.entry(doc_id)
            .and_modify(|set| {
                set.insert(js_callback.clone().into());
            })
            .or_insert_with(|| {
                HashSet::from_iter([js_callback.clone().into()])
            });

        Ok(())
    }

    pub fn unregister(&mut self, doc_id: DocumentId, js_callback: &Function) -> Result<(), JsValue> {
        self.entry(doc_id)
            .and_modify(|set| {
                set.remove(&js_callback.clone().into());
            });

        if let Some(doc_adapters) = self.adapters.get(&doc_id)  {
            if doc_adapters.is_empty() {
                self.adapters.remove(&doc_id);
            }
        }

        Ok(())
    }

    pub fn entry(&mut self, doc_id: DocumentId) -> Entry<'_, DocumentId, HashSet<Callback>> {
        self.adapters.entry(doc_id)
    }

    pub async fn stop() -> Result<(), JsValue> {
        todo!()
    }

    pub async fn whenRead() -> bool {
        true
    }
}




// TODO use a hardcoded docid for now, and hardcode the doc content
// send a chunk to automegre
//



pub trait SedimentreeNetworkAdapter {
    // TODO Fails if DocID not availavle
    fn on_change(documentId: DocumentId, callback: Function) -> Result<(), JsValue>;
    fn unsubscribe(documentId: DocumentId, callback: Function) -> Result<(), JsValue>;

    // fn send_bundle
    //

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none() {
        assert!(true);
    }
}
