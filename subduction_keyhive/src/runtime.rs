//! A `keyhive_core` keyhive for multi-threaded (`Send + Sync`) runtimes.

use alloc::{format, vec::Vec};

use keyhive_content::store::memory::MemoryCiphertextStore;
use keyhive_core::{
    contact_card::ContactCard, content::heads::ContentHeads, keyhive::Keyhive,
    listener::no_listener::NoListener,
};
use keyhive_crypto::{identifier::Identifier, signer::memory::MemorySigner};
use keyline_memory::MemoryKeyline;
use rand::rngs::OsRng;

use crate::{KeyhivePeerId, change_id::ChangeId};

/// The runtime's Keyline in-memory store, refilled from the stored archive
/// and events on startup.
pub type RuntimeKeyline = MemoryKeyline<ContentHeads<ChangeId>>;

/// Concrete keyhive type for multi-threaded (`Send + Sync`) runtimes.
pub type SendableRuntimeKeyhive = Keyhive<
    future_form::Sendable,
    MemorySigner,
    ChangeId,
    Vec<u8>,
    MemoryCiphertextStore<ChangeId, Vec<u8>>,
    NoListener,
    OsRng,
    RuntimeKeyline,
>;

/// Initialize a `Sendable` keyhive instance.
///
/// # Errors
///
/// Returns an error if keyhive generation fails.
pub async fn init_sendable_keyhive(
    signer: MemorySigner,
) -> Result<(SendableRuntimeKeyhive, KeyhivePeerId, ContactCard), alloc::string::String> {
    let keyhive = Keyhive::generate(
        signer,
        MemoryCiphertextStore::<ChangeId, Vec<u8>>::new(),
        NoListener,
        RuntimeKeyline::new(),
        OsRng,
    )
    .await
    .map_err(|e| format!("failed to generate keyhive: {e}"))?;

    let contact_card = keyhive
        .generate_contact_card()
        .await
        .map_err(|e| format!("failed to generate keyhive contact card: {e}"))?;

    let kh_id: Identifier = keyhive.id().into();
    let peer_id = KeyhivePeerId::from_bytes(kh_id.to_bytes());

    Ok((keyhive, peer_id, contact_card))
}
