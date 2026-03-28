//! Wasm wrapper for [`Topic`].

use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use subduction_ephemeral::topic::Topic;
use thiserror::Error;
use wasm_bindgen::prelude::*;

/// A Wasm wrapper around the ephemeral [`Topic`] type.
///
/// Topics are opaque 32-byte identifiers for ephemeral pubsub channels.
/// A [`SedimentreeId`] can be used as a topic, but topics are not
/// limited to sedimentrees.
///
/// [`SedimentreeId`]: sedimentree_core::id::SedimentreeId
#[wasm_bindgen(js_name = Topic)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
pub struct WasmTopic(Topic);

#[wasm_bindgen(js_class = Topic)]
impl WasmTopic {
    /// Create a topic from a 32-byte array.
    ///
    /// # Errors
    ///
    /// Returns an error if the provided byte array is not exactly 32 bytes.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TopicNot32Bytes> {
        let raw: [u8; 32] = bytes.try_into().map_err(|_| TopicNot32Bytes)?;
        Ok(Self(Topic::new(raw)))
    }

    /// Returns the raw 32 bytes of this topic.
    #[must_use]
    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Returns the hex string representation.
    #[must_use]
    #[wasm_bindgen(js_name = toString)]
    pub fn js_to_string(&self) -> String {
        alloc::format!("{}", self.0)
    }
}

impl From<Topic> for WasmTopic {
    fn from(t: Topic) -> Self {
        Self(t)
    }
}

impl From<WasmTopic> for Topic {
    fn from(t: WasmTopic) -> Self {
        t.0
    }
}

/// Error indicating that the provided byte array is not exactly 32 bytes.
#[derive(Debug, Error)]
#[error("Topic must be exactly 32 bytes")]
#[allow(missing_copy_implementations)]
pub struct TopicNot32Bytes;

impl From<TopicNot32Bytes> for JsValue {
    fn from(err: TopicNot32Bytes) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("TopicNot32Bytes");
        js_err.into()
    }
}
