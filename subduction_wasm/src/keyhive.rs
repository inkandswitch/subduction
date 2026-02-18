//! Wasm bindings for Keyhive types.
//!
//! This module provides `Wasm*` newtypes that expose Keyhive functionality
//! through the Subduction Wasm API.

use alloc::{
    format,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};

use async_lock::Mutex;
use keyhive_core::{
    access::Access,
    cgka::operation::CgkaOperation,
    contact_card::ContactCard,
    crypto::signed::Signed,
    keyhive::Keyhive,
    listener::{cgka::CgkaListener, membership::MembershipListener, prekey::PrekeyListener},
    principal::{
        document::id::DocumentId,
        group::{delegation::Delegation, revocation::Revocation},
        identifier::Identifier,
        individual::{
            id::IndividualId,
            op::{add_key::AddKeyOp, rotate_key::RotateKeyOp},
        },
    },
    store::ciphertext::memory::MemoryCiphertextStore,
};
use nonempty::NonEmpty;
use rand::rngs::OsRng;
use wasm_bindgen::prelude::*;

use crate::signer::JsSigner;

// ============================================================================
// Change ID (Content Reference)
// ============================================================================

/// Change ID (content reference) for keyhive documents.
///
/// This is equivalent to `keyhive_wasm::JsChangeId` but defined locally
/// to avoid circular dependencies.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ChangeId(pub(crate) Vec<u8>);

impl ChangeId {
    /// Create a new change ID from bytes.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Get the bytes of this change ID.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
}



// ============================================================================
// Event Handler
// ============================================================================

/// Event handler that calls a JavaScript function for keyhive events.
#[derive(Debug, Clone)]
pub struct WasmEventHandler(pub(crate) js_sys::Function);

impl WasmEventHandler {
    /// Create a new event handler from a JavaScript function.
    #[must_use]
    pub const fn new(func: js_sys::Function) -> Self {
        Self(func)
    }

    fn call(&self, event_name: &str, data: &JsValue) {
        let name = JsValue::from_str(event_name);
        // Best effort - ignore errors from JS callback
        drop(self.0.call2(&JsValue::NULL, &name, data));
    }
}

impl PrekeyListener for WasmEventHandler {
    async fn on_prekeys_expanded(&self, _e: &Arc<Signed<AddKeyOp>>) {
        self.call("prekeys_expanded", &JsValue::NULL);
    }

    async fn on_prekey_rotated(&self, _e: &Arc<Signed<RotateKeyOp>>) {
        self.call("prekey_rotated", &JsValue::NULL);
    }
}

impl MembershipListener<JsSigner, ChangeId> for WasmEventHandler {
    async fn on_delegation(&self, _data: &Arc<Signed<Delegation<JsSigner, ChangeId, Self>>>) {
        self.call("delegation", &JsValue::NULL);
    }

    async fn on_revocation(&self, _data: &Arc<Signed<Revocation<JsSigner, ChangeId, Self>>>) {
        self.call("revocation", &JsValue::NULL);
    }
}

impl CgkaListener for WasmEventHandler {
    async fn on_cgka_op(&self, _data: &Arc<Signed<CgkaOperation>>) {
        self.call("cgka_op", &JsValue::NULL);
    }
}

// ============================================================================
// Ciphertext Store
// ============================================================================

/// Ciphertext store for encrypted content.
#[wasm_bindgen(js_name = CiphertextStore)]
#[derive(Debug, Clone)]
pub struct WasmCiphertextStore(pub(crate) MemoryCiphertextStore<ChangeId, Vec<u8>>);

#[wasm_bindgen(js_class = CiphertextStore)]
impl WasmCiphertextStore {
    /// Create a new in-memory ciphertext store.
    #[wasm_bindgen(js_name = newInMemory)]
    #[must_use]
    pub fn new_in_memory() -> Self {
        Self(MemoryCiphertextStore::new())
    }
}

// ============================================================================
// Internal Keyhive Type
// ============================================================================

/// The internal keyhive type used by Subduction.
pub(crate) type InternalKeyhive = Keyhive<
    JsSigner,
    ChangeId,
    Vec<u8>,
    MemoryCiphertextStore<ChangeId, Vec<u8>>,
    WasmEventHandler,
    OsRng,
>;

/// Shared reference to the internal keyhive.
pub(crate) type SharedKeyhive = Arc<Mutex<InternalKeyhive>>;

// ============================================================================
// WasmKeyhive
// ============================================================================

/// Keyhive instance for access control and encryption.
#[wasm_bindgen(js_name = Keyhive)]
pub struct WasmKeyhive {
    inner: SharedKeyhive,
}

impl WasmKeyhive {
    /// Create a new `WasmKeyhive` from a shared keyhive reference.
    pub(crate) fn new(inner: SharedKeyhive) -> Self {
        Self { inner }
    }
}

impl core::fmt::Debug for WasmKeyhive {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WasmKeyhive").finish_non_exhaustive()
    }
}

#[wasm_bindgen(js_class = Keyhive)]
impl WasmKeyhive {
    /// Get this keyhive's individual ID.
    #[wasm_bindgen(getter)]
    pub async fn id(&self) -> WasmIndividualId {
        let kh = self.inner.lock().await;
        WasmIndividualId(kh.id())
    }

    /// Get this keyhive's individual ID as a hex string.
    #[wasm_bindgen(getter, js_name = idString)]
    pub async fn id_string(&self) -> String {
        let kh = self.inner.lock().await;
        kh.id()
            .as_slice()
            .iter()
            .fold(String::from("0x"), |mut acc, byte| {
                acc.push_str(&format!("{byte:02x}"));
                acc
            })
    }

    /// Get this keyhive's contact card for sharing with others.
    #[wasm_bindgen(js_name = contactCard)]
    pub async fn contact_card(&self) -> Result<WasmContactCard, JsValue> {
        let kh = self.inner.lock().await;
        kh.contact_card()
            .await
            .map(WasmContactCard)
            .map_err(|e| {
                let js_err = js_sys::Error::new(&e.to_string());
                js_err.set_name("SigningError");
                js_err.into()
            })
    }

    /// Get the existing contact card (does not generate a new one).
    #[wasm_bindgen(js_name = getExistingContactCard)]
    pub async fn get_existing_contact_card(&self) -> WasmContactCard {
        let kh = self.inner.lock().await;
        WasmContactCard(kh.get_existing_contact_card().await)
    }

    /// Receive a contact card from another user.
    #[wasm_bindgen(js_name = receiveContactCard)]
    pub async fn receive_contact_card(
        &self,
        contact_card: &WasmContactCard,
    ) -> Result<WasmIndividualId, JsValue> {
        let kh = self.inner.lock().await;
        match kh.receive_contact_card(&contact_card.0).await {
            Ok(individual) => {
                let id = individual.lock().await.id();
                Ok(WasmIndividualId(id))
            }
            Err(err) => {
                let js_err = js_sys::Error::new(&err.to_string());
                js_err.set_name("ReceiveContactCardError");
                Err(js_err.into())
            }
        }
    }

    /// Generate a new keyhive-protected document.
    #[wasm_bindgen(js_name = generateDocument)]
    pub async fn generate_document(
        &self,
        initial_content_ref: WasmChangeId,
    ) -> Result<WasmDocumentId, JsValue> {
        let kh = self.inner.lock().await;
        let doc = kh
            .generate_doc(
                Vec::new(), // no coparents for now
                NonEmpty::new(initial_content_ref.0),
            )
            .await
            .map_err(|e| {
                let js_err = js_sys::Error::new(&e.to_string());
                js_err.set_name("GenerateDocError");
                JsValue::from(js_err)
            })?;

        let doc_id = doc.lock().await.doc_id();
        Ok(WasmDocumentId(doc_id))
    }

    /// Get a document by its ID.
    #[wasm_bindgen(js_name = getDocument)]
    pub async fn get_document(&self, doc_id: &WasmDocumentId) -> Option<WasmDocumentId> {
        let kh = self.inner.lock().await;
        kh.get_document(doc_id.0).await.map(|_| doc_id.clone())
    }

    /// Check access level for an agent on a document.
    #[wasm_bindgen(js_name = accessForDoc)]
    pub async fn access_for_doc(
        &self,
        agent_id: &WasmIdentifier,
        doc_id: &WasmDocumentId,
    ) -> Option<WasmAccess> {
        let kh = self.inner.lock().await;
        let doc = kh.get_document(doc_id.0).await?;
        let mems = doc.lock().await.transitive_members().await;
        mems.get(&agent_id.0).map(|(_, access)| WasmAccess(*access))
    }

    /// Get keyhive statistics.
    #[wasm_bindgen]
    pub async fn stats(&self) -> WasmStats {
        let kh = self.inner.lock().await;
        WasmStats(kh.stats().await)
    }

    /// Expand prekeys for this keyhive.
    #[wasm_bindgen(js_name = expandPrekeys)]
    pub async fn expand_prekeys(&self) -> Result<(), JsValue> {
        let kh = self.inner.lock().await;
        kh.expand_prekeys().await.map_err(|e| {
            let js_err = js_sys::Error::new(&e.to_string());
            js_err.set_name("SigningError");
            JsValue::from(js_err)
        })?;
        Ok(())
    }
}

// ============================================================================
// Supporting Types
// ============================================================================

/// Contact card for sharing keyhive identity.
#[wasm_bindgen(js_name = ContactCard)]
#[derive(Debug, Clone)]
pub struct WasmContactCard(pub(crate) ContactCard);

#[wasm_bindgen(js_class = ContactCard)]
impl WasmContactCard {
    /// Get the individual ID from this contact card.
    #[wasm_bindgen(getter, js_name = individualId)]
    pub fn individual_id(&self) -> WasmIndividualId {
        WasmIndividualId(self.0.id())
    }

    /// Get the identifier from this contact card.
    #[wasm_bindgen(getter)]
    pub fn id(&self) -> WasmIdentifier {
        WasmIdentifier(self.0.id().into())
    }
}

/// Individual ID (user identity).
#[wasm_bindgen(js_name = IndividualId)]
#[derive(Debug, Clone, Copy)]
pub struct WasmIndividualId(pub(crate) IndividualId);

#[wasm_bindgen(js_class = IndividualId)]
impl WasmIndividualId {
    /// Get the raw bytes of this ID.
    #[wasm_bindgen(getter)]
    pub fn bytes(&self) -> Vec<u8> {
        self.0.as_slice().to_vec()
    }

    /// Get the ID as a hex string.
    #[wasm_bindgen(js_name = toString)]
    pub fn to_hex_string(&self) -> String {
        self.0
            .as_slice()
            .iter()
            .fold(String::from("0x"), |mut acc, byte| {
                acc.push_str(&format!("{byte:02x}"));
                acc
            })
    }
}

/// General identifier (can be individual, group, or document).
#[wasm_bindgen(js_name = Identifier)]
#[derive(Debug, Clone, Copy)]
pub struct WasmIdentifier(pub(crate) Identifier);

#[wasm_bindgen(js_class = Identifier)]
impl WasmIdentifier {
    /// Get the raw bytes of this identifier.
    #[wasm_bindgen(getter)]
    pub fn bytes(&self) -> Vec<u8> {
        self.0.to_bytes().to_vec()
    }
}

/// Document ID.
#[wasm_bindgen(js_name = DocumentId)]
#[derive(Debug, Clone, Copy)]
pub struct WasmDocumentId(pub(crate) DocumentId);

#[wasm_bindgen(js_class = DocumentId)]
impl WasmDocumentId {
    /// Get the raw bytes of this document ID.
    #[wasm_bindgen(getter)]
    pub fn bytes(&self) -> Vec<u8> {
        self.0.as_slice().to_vec()
    }

    /// Get the document ID as an identifier.
    #[wasm_bindgen(getter)]
    pub fn id(&self) -> WasmIdentifier {
        WasmIdentifier(self.0.into())
    }

    /// Get the document ID as a hex string.
    #[wasm_bindgen(js_name = toString)]
    pub fn to_hex_string(&self) -> String {
        self.0
            .as_slice()
            .iter()
            .fold(String::from("0x"), |mut acc, byte| {
                acc.push_str(&format!("{byte:02x}"));
                acc
            })
    }
}

/// Change ID (content reference).
#[wasm_bindgen(js_name = ChangeId)]
#[derive(Debug, Clone)]
pub struct WasmChangeId(pub(crate) ChangeId);

#[wasm_bindgen(js_class = ChangeId)]
impl WasmChangeId {
    /// Create a new change ID from bytes.
    #[wasm_bindgen(constructor)]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(ChangeId::new(bytes))
    }

    /// Get the raw bytes of this change ID.
    #[wasm_bindgen(getter)]
    pub fn bytes(&self) -> Vec<u8> {
        self.0.0.clone()
    }
}

/// Access level for a document.
#[wasm_bindgen(js_name = Access)]
#[derive(Debug, Clone, Copy)]
pub struct WasmAccess(pub(crate) Access);

#[wasm_bindgen(js_class = Access)]
impl WasmAccess {
    /// Create an access level from a string.
    #[wasm_bindgen(js_name = tryFromString)]
    pub fn try_from_string(s: String) -> Option<WasmAccess> {
        match s.as_str() {
            "pull" => Some(WasmAccess(Access::Pull)),
            "read" => Some(WasmAccess(Access::Read)),
            "write" => Some(WasmAccess(Access::Write)),
            "admin" => Some(WasmAccess(Access::Admin)),
            _ => None,
        }
    }

    /// Convert to string representation.
    #[wasm_bindgen(js_name = toString)]
    pub fn to_js_string(&self) -> String {
        match self.0 {
            Access::Pull => String::from("pull"),
            Access::Read => String::from("read"),
            Access::Write => String::from("write"),
            Access::Admin => String::from("admin"),
        }
    }

    /// Pull access level.
    #[wasm_bindgen(getter)]
    pub fn pull() -> WasmAccess {
        WasmAccess(Access::Pull)
    }

    /// Read access level.
    #[wasm_bindgen(getter)]
    pub fn read() -> WasmAccess {
        WasmAccess(Access::Read)
    }

    /// Write access level.
    #[wasm_bindgen(getter)]
    pub fn write() -> WasmAccess {
        WasmAccess(Access::Write)
    }

    /// Admin access level.
    #[wasm_bindgen(getter)]
    pub fn admin() -> WasmAccess {
        WasmAccess(Access::Admin)
    }
}

/// Keyhive statistics.
#[wasm_bindgen(js_name = KeyhiveStats)]
#[derive(Debug, Clone)]
pub struct WasmStats(pub(crate) keyhive_core::stats::Stats);

#[wasm_bindgen(js_class = KeyhiveStats)]
impl WasmStats {
    /// Number of documents.
    #[wasm_bindgen(getter, js_name = documentCount)]
    #[allow(clippy::cast_possible_truncation)]
    pub fn document_count(&self) -> usize {
        self.0.docs as usize
    }

    /// Number of groups.
    #[wasm_bindgen(getter, js_name = groupCount)]
    #[allow(clippy::cast_possible_truncation)]
    pub fn group_count(&self) -> usize {
        self.0.groups as usize
    }

    /// Number of individuals.
    #[wasm_bindgen(getter, js_name = individualCount)]
    #[allow(clippy::cast_possible_truncation)]
    pub fn individual_count(&self) -> usize {
        self.0.individuals as usize
    }
}
