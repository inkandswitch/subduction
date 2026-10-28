//! `keyhive_wasm` interop: the server reads what a browser client sends.
//!
//! A browser client builds its keyhive traffic with `keyhive_wasm`. The
//! vectors in `fixtures/keyhive_wasm/vectors.txt` are bytes `keyhive_wasm`
//! produced (see `generate.cjs` there), so these tests catch the server drifting from
//! the release clients run. In particular, the events include a revocation
//! whose retained content heads hold `JsChangeId`s, which the server reads as
//! [`ChangeId`]s.

use keyhive_codec::traits::{Decode, Encode};
use keyhive_core::{
    contact_card::ContactCard, event::static_event::StaticEvent,
    principal::document::id::DocumentId,
};
use keyhive_crypto::identifier::Identifier;
use keyline::power::Power;
use sedimentree_core::loose_commit::id::CommitId;
use subduction_keyhive::{
    KeyhivePeerId, SignedMessage, change_id::ChangeId, runtime::init_sendable_keyhive,
};
use testresult::TestResult;

const VECTORS: &str = include_str!("fixtures/keyhive_wasm/vectors.txt");

/// A client's signed bytes and contact card, carried in a `SUK` envelope,
/// verify as from the client.
#[test]
fn signed_bytes_verify_as_a_keyhive_message() -> TestResult {
    let v = Vectors::parse()?;
    let card = ContactCard::from_bytes(&v.contact_card)?;
    let sent = SignedMessage::with_contact_card(v.signed, card);
    let received = SignedMessage::from_cbor(&sent.to_cbor()?)?;

    let verified = received.verify(&KeyhivePeerId::from_bytes(v.alice))?;
    assert_eq!(verified.payload, v.payload);
    assert!(verified.contact_card.is_some());
    Ok(())
}

/// Every event decodes and re-encodes to the same bytes, `ChangeId`s
/// included.
#[test]
fn events_round_trip_byte_for_byte() -> TestResult {
    let v = Vectors::parse()?;
    for bytes in &v.events {
        let event = StaticEvent::<ChangeId>::decode(bytes)?;
        assert_eq!(&event.encode().into_bytes(), bytes);
    }

    let retained = ChangeId::new(CommitId::new([2; 32])).encode().into_bytes();
    assert!(
        v.events
            .iter()
            .any(|e| e.windows(retained.len()).any(|w| w == retained)),
        "the vectors carry the revocation's retained heads"
    );
    Ok(())
}

/// A server that ingests the events, and the client's contact card, sees the
/// membership the client set up: Alice owns the document, the server relays
/// it, and Bob was revoked.
#[tokio::test]
async fn events_carry_the_membership() -> TestResult {
    let v = Vectors::parse()?;
    let signer: keyhive_crypto::signer::memory::MemorySigner =
        ed25519_dalek::SigningKey::from_bytes(&[0x5e; 32]).into();
    let (server, _, _) = init_sendable_keyhive(signer).await?;

    let alice = server
        .receive_contact_card(&ContactCard::from_bytes(&v.contact_card)?)
        .await?;
    assert_eq!(Identifier::from(alice).to_bytes(), v.alice);

    let events = v
        .events
        .iter()
        .map(|bytes| StaticEvent::<ChangeId>::decode(bytes))
        .collect::<Result<Vec<_>, _>>()?;
    let pending = server.ingest_unsorted_static_events(events).await;
    assert!(pending.is_empty(), "{} events pending", pending.len());

    let doc = DocumentId::from(Identifier::from_bytes(v.document)?);
    let access = async |who: [u8; 32]| -> TestResult<Option<Power>> {
        Ok(server
            .access_for_doc(Identifier::from_bytes(who)?, doc)
            .await?)
    };
    assert_eq!(access(v.alice).await?, Some(Power::Admin));
    assert_eq!(access(v.server).await?, Some(Power::Relay));
    assert_eq!(access(v.bob).await?, None);
    Ok(())
}

struct Vectors {
    alice: [u8; 32],
    bob: [u8; 32],
    server: [u8; 32],
    document: [u8; 32],
    payload: Vec<u8>,
    signed: Vec<u8>,
    contact_card: Vec<u8>,
    events: Vec<Vec<u8>>,
}

impl Vectors {
    fn parse() -> TestResult<Self> {
        let mut fields = std::collections::HashMap::new();
        let mut events = Vec::new();
        for line in VECTORS.lines().filter(|l| !l.starts_with('#')) {
            let (key, value) = line.split_once('=').ok_or("a key=hex line")?;
            let bytes = unhex(value)?;
            if key == "event" {
                events.push(bytes);
            } else {
                fields.insert(key, bytes);
            }
        }
        let mut take = |key: &str| fields.remove(key).ok_or(format!("no {key}"));
        let id = |bytes: Vec<u8>| <[u8; 32]>::try_from(bytes).map_err(|_| "a 32-byte ID");
        Ok(Self {
            alice: id(take("alice")?)?,
            bob: id(take("bob")?)?,
            server: id(take("server")?)?,
            document: id(take("document")?)?,
            payload: take("payload")?,
            signed: take("signed")?,
            contact_card: take("contact_card")?,
            events,
        })
    }
}

fn unhex(hex: &str) -> TestResult<Vec<u8>> {
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            let pair = hex.get(i..i + 2).ok_or("odd-length hex")?;
            Ok(u8::from_str_radix(pair, 16)?)
        })
        .collect()
}
