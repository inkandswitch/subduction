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

pub type DocumentId = String;

#[wasm_bindgen]
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

#[wasm_bindgen]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SedimentreeNetwork {
    adapters: HashMap<DocumentId, HashSet<Callback>>, // TODO Or something
}

#[wasm_bindgen]
impl SedimentreeNetwork {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        web_sys::console::log_1(&"Creating SedimentreeNetwork".into());
        Self {
            adapters: HashMap::new(),
        }
    }

    pub fn start(&self, _adapters: Vec<u8>) {
        web_sys::console::log_1(&"!! start".into());
    }

    pub async fn find(&self, doc_id: DocumentId) -> Option<Vec<u8>> {
        web_sys::console::log_1(&format!("Finding adapters for doc_id: {:?}", doc_id).into());
        web_sys::console::log_1(&"(returning blank doc for now)".into());
        Some(automerge::Automerge::new().save())
    }

    pub fn  on(&mut self, doc_id: DocumentId, js_callback: &Function) -> Result<(), JsValue> {
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
        self.entry(doc_id.clone())
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

    fn entry(&mut self, doc_id: DocumentId) -> Entry<'_, DocumentId, HashSet<Callback>> {
        self.adapters.entry(doc_id)
    }

    pub async fn stop() -> Result<(), JsValue> {
        todo!()
    }

    pub async fn when_read() -> bool {
        true
    }
}




// TODO use a hardcoded docid for now, and hardcode the doc content
// send a chunk to automegre



pub trait SedimentreeNetworkAdapter {
    // TODO Fails if DocID not availavle
    fn on_change(docId: DocumentId, callback: Function) -> Result<(), JsValue>;
    fn unsubscribe(docId: DocumentId, callback: Function) -> Result<(), JsValue>;

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
