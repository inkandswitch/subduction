//! [`KeyhiveAccess`] over a keyhive that lives in JavaScript.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    format,
    string::String,
    sync::Arc,
    vec::Vec,
};
use core::fmt;

use dupe::Dupe;
use future_form::{FutureForm, Local};
use futures::FutureExt;
use js_sys::{Array, Map, Promise, Reflect, Uint8Array};
use keyhive_crypto::identifier::Identifier;
use subduction_keyhive::{
    AgentEventIndex, AgentId, KeyhiveAccess, KeyhiveCodec, message::EventHash,
};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(typescript_custom_section)]
const TS_KEYHIVE_ACCESS: &str = r#"
/**
 * A keyhive, as Subduction's keyhive sync protocol uses it.
 *
 * Every argument and result is bytes. Identifiers and event hashes are 32
 * bytes, events are keyhive `KH` frames as `StaticEvent` encodes them, and
 * archives are `Archive.toBytes()`. `id()` returns its result directly and
 * every other method returns a promise. A method reports a failure by
 * throwing or rejecting.
 */
export interface KeyhiveAccess {
    /** This keyhive's 32-byte identifier. */
    id(): Uint8Array;
    /** This keyhive's contact card, as `ContactCard.toBytes()`. */
    contactCard(): Promise<Uint8Array>;
    /** `Stats.totalOps`. */
    totalOps(): Promise<bigint>;
    /** Hashes of the events waiting for their dependencies. */
    pendingEventHashes(): Promise<Iterable<Uint8Array>>;
    /** Whether `id` is this keyhive, or an individual, document or group it knows. */
    isKnownAgent(id: Uint8Array): Promise<boolean>;
    /** The events `id` is sent, by hash. */
    eventsForAgent(id: Uint8Array): Promise<Map<Uint8Array, Uint8Array>>;
    /** Every event each agent is sent, in the shape of `Keyhive.allAgentEvents()`. */
    allAgentEvents(): Promise<KeyhiveAgentEvents>;
    /** Sign `payload`, returning `SignedBytes.toBytes()`. */
    signBytes(payload: Uint8Array): Promise<Uint8Array>;
    /** Ingest events in any order, returning the ones still waiting for their dependencies. */
    ingestEvents(events: Uint8Array[]): Promise<Uint8Array[]>;
    /** Receive a contact card, returning its prekey operation as an event. */
    receiveContactCard(card: Uint8Array): Promise<Uint8Array>;
    /** A snapshot of keyhive state. */
    toArchive(): Promise<Uint8Array>;
    /** Merge a snapshot into keyhive state. */
    ingestArchive(archive: Uint8Array): Promise<void>;
}

/**
 * Every event each agent is sent, indexed by source.
 *
 * An agent is sent the events of every source that `agentMembershipSources`,
 * `agentPrekeySources` and `agentCgkaSources` list for it plus its
 * `explanations`. An agent with no key in any of those three maps is sent
 * nothing.
 */
export interface KeyhiveAgentEvents {
    /** Each event once, by hash. */
    events: Map<Uint8Array, Uint8Array>;
    /** Event hashes of each membership source. */
    membershipSources: Map<Uint8Array, Uint8Array[]>;
    /** Event hashes of each prekey source. */
    prekeySources: Map<Uint8Array, Uint8Array[]>;
    /** Event hashes of each CGKA source. */
    cgkaSources: Map<Uint8Array, Uint8Array[]>;
    /** Membership sources of each agent. */
    agentMembershipSources: Map<Uint8Array, Uint8Array[]>;
    /** Prekey sources of each agent. */
    agentPrekeySources: Map<Uint8Array, Uint8Array[]>;
    /** CGKA sources of each agent. */
    agentCgkaSources: Map<Uint8Array, Uint8Array[]>;
    /** Event hashes that explain each agent's removals. */
    explanations: Map<Uint8Array, Uint8Array[]>;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// A keyhive in JavaScript, as the `KeyhiveAccess` TypeScript interface
    /// emitted by this module describes it.
    #[wasm_bindgen(js_name = KeyhiveAccess, typescript_type = "KeyhiveAccess")]
    #[derive(Clone)]
    pub type JsKeyhiveAccess;

    #[wasm_bindgen(method, catch, js_name = id)]
    fn js_id(this: &JsKeyhiveAccess) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = contactCard)]
    fn js_contact_card(this: &JsKeyhiveAccess) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = totalOps)]
    fn js_total_ops(this: &JsKeyhiveAccess) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = pendingEventHashes)]
    fn js_pending_event_hashes(this: &JsKeyhiveAccess) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = isKnownAgent)]
    fn js_is_known_agent(this: &JsKeyhiveAccess, id: Uint8Array) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = eventsForAgent)]
    fn js_events_for_agent(this: &JsKeyhiveAccess, id: Uint8Array) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = allAgentEvents)]
    fn js_all_agent_events(this: &JsKeyhiveAccess) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = signBytes)]
    fn js_sign_bytes(this: &JsKeyhiveAccess, payload: Uint8Array) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = ingestEvents)]
    fn js_ingest_events(this: &JsKeyhiveAccess, events: Array) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = receiveContactCard)]
    fn js_receive_contact_card(
        this: &JsKeyhiveAccess,
        card: Uint8Array,
    ) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = toArchive)]
    fn js_to_archive(this: &JsKeyhiveAccess) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(method, catch, js_name = ingestArchive)]
    fn js_ingest_archive(this: &JsKeyhiveAccess, archive: Uint8Array) -> Result<JsValue, JsValue>;
}

impl fmt::Debug for JsKeyhiveAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("JsKeyhiveAccess").finish()
    }
}

/// An error from a [`JsKeyhiveAccess`] call or a result of the wrong shape.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct KeyhiveAccessError(String);

impl KeyhiveAccessError {
    /// The error a JavaScript method threw or rejected with, prefixed with
    /// `KeyhiveAccess.<method>`.
    fn thrown(method: &str, error: &JsValue) -> Self {
        let detail = error
            .dyn_ref::<js_sys::Error>()
            .map(|e| String::from(e.to_string()))
            .or_else(|| error.as_string())
            .unwrap_or_else(|| format!("{error:?}"));
        Self(format!("KeyhiveAccess.{method}: {detail}"))
    }

    /// A result from `method` that does not have the documented shape.
    fn shape(method: &str, expected: &str) -> Self {
        Self(format!("KeyhiveAccess.{method}: expected {expected}"))
    }
}

/// An encoded keyhive event (a `KH` frame).
///
/// The protocol never looks inside an event. Keyhive decodes and verifies it
/// on ingest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBytes(pub Vec<u8>);

/// An encoded keyhive archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveBytes(pub Vec<u8>);

/// [`KeyhiveCodec`] and [`KeyhiveAccess`] over a [`JsKeyhiveAccess`].
#[derive(Debug, Clone)]
pub struct WasmKeyhive(JsKeyhiveAccess);

/// Cloning copies a reference to the JavaScript object.
impl Dupe for WasmKeyhive {}

impl From<JsKeyhiveAccess> for WasmKeyhive {
    fn from(access: JsKeyhiveAccess) -> Self {
        Self(access)
    }
}

type AccessResult<T> = Result<T, KeyhiveAccessError>;

impl WasmKeyhive {
    /// This keyhive's identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if `id()` throws or does not return 32 bytes.
    pub fn id(&self) -> AccessResult<AgentId> {
        let id = self
            .0
            .js_id()
            .map_err(|e| KeyhiveAccessError::thrown("id", &e))?;
        array32(&id, "id")
    }

    /// This keyhive's contact card.
    ///
    /// # Errors
    ///
    /// Returns an error if `contactCard()` fails or does not return bytes.
    pub async fn contact_card(&self) -> AccessResult<Vec<u8>> {
        let card = settle("contactCard", self.0.js_contact_card()).await?;
        bytes(&card, "contactCard")
    }
}

/// Await `result`, which is a value or a promise of one.
async fn settle(method: &str, result: Result<JsValue, JsValue>) -> AccessResult<JsValue> {
    let value = result.map_err(|e| KeyhiveAccessError::thrown(method, &e))?;
    JsFuture::from(Promise::resolve(&value))
        .await
        .map_err(|e| KeyhiveAccessError::thrown(method, &e))
}

fn bytes(value: &JsValue, method: &str) -> AccessResult<Vec<u8>> {
    value
        .dyn_ref::<Uint8Array>()
        .map(Uint8Array::to_vec)
        .ok_or_else(|| KeyhiveAccessError::shape(method, "a Uint8Array"))
}

fn array32(value: &JsValue, method: &str) -> AccessResult<[u8; 32]> {
    bytes(value, method)?
        .try_into()
        .map_err(|_| KeyhiveAccessError::shape(method, "32 bytes"))
}

/// The values an iterable yields.
fn iterate(value: &JsValue, method: &str) -> AccessResult<Vec<JsValue>> {
    let iter = js_sys::try_iter(value)
        .map_err(|e| KeyhiveAccessError::thrown(method, &e))?
        .ok_or_else(|| KeyhiveAccessError::shape(method, "an iterable"))?;
    iter.map(|item| item.map_err(|e| KeyhiveAccessError::thrown(method, &e)))
        .collect()
}

/// The `[key, value]` pairs of a `Map`.
fn entries(value: &JsValue, method: &str) -> AccessResult<Vec<(JsValue, JsValue)>> {
    let map = value
        .dyn_ref::<Map>()
        .ok_or_else(|| KeyhiveAccessError::shape(method, "a Map"))?;
    Ok(iterate(&map.entries(), method)?
        .into_iter()
        .map(|entry| {
            let pair: Array = entry.unchecked_into();
            (pair.get(0), pair.get(1))
        })
        .collect())
}

/// A `Map<Uint8Array, Uint8Array[]>` of 32-byte keys and values.
fn id_lists(value: &JsValue, method: &str) -> AccessResult<BTreeMap<[u8; 32], Vec<[u8; 32]>>> {
    entries(value, method)?
        .into_iter()
        .map(|(key, list)| {
            let ids = iterate(&list, method)?
                .iter()
                .map(|id| array32(id, method))
                .collect::<AccessResult<Vec<_>>>()?;
            Ok((array32(&key, method)?, ids))
        })
        .collect()
}

/// The hashes each agent is sent, as keyhive's `AllAgentEvents::agents` and
/// `AllAgentEvents::digests_for` compute them.
fn agent_hashes(all: &JsValue) -> AccessResult<BTreeMap<AgentId, BTreeSet<EventHash>>> {
    const METHOD: &str = "allAgentEvents";
    let field = |name: &str| -> AccessResult<JsValue> {
        Reflect::get(all, &JsValue::from_str(name))
            .map_err(|e| KeyhiveAccessError::thrown(METHOD, &e))
    };
    let lists = |name: &str| id_lists(&field(name)?, &format!("{METHOD}().{name}"));

    let mut out: BTreeMap<AgentId, BTreeSet<EventHash>> = BTreeMap::new();
    for (index, sources) in [
        ("agentMembershipSources", "membershipSources"),
        ("agentPrekeySources", "prekeySources"),
        ("agentCgkaSources", "cgkaSources"),
    ] {
        let sources = lists(sources)?;
        for (agent, source_ids) in lists(index)? {
            let hashes = out.entry(agent).or_default();
            for source in source_ids {
                hashes.extend(sources.get(&source).into_iter().flatten().copied());
            }
        }
    }

    let explanations = lists("explanations")?;
    for (agent, hashes) in &mut out {
        hashes.extend(explanations.get(agent).into_iter().flatten().copied());
    }
    Ok(out)
}

impl KeyhiveCodec for WasmKeyhive {
    type Event = EventBytes;
    type Archive = ArchiveBytes;
    type Error = KeyhiveAccessError;

    fn encode_event(event: &EventBytes) -> Vec<u8> {
        event.0.clone()
    }

    fn decode_event(bytes: &[u8]) -> AccessResult<EventBytes> {
        Ok(EventBytes(bytes.to_vec()))
    }

    fn encode_archive(archive: &ArchiveBytes) -> Vec<u8> {
        archive.0.clone()
    }

    fn decode_archive(bytes: &[u8]) -> AccessResult<ArchiveBytes> {
        Ok(ArchiveBytes(bytes.to_vec()))
    }
}

impl KeyhiveAccess<Local> for WasmKeyhive {
    fn total_ops(&self) -> <Local as FutureForm>::Future<'_, AccessResult<u64>> {
        async move {
            const METHOD: &str = "totalOps";
            let total = settle(METHOD, self.0.js_total_ops()).await?;
            total
                .dyn_into::<js_sys::BigInt>()
                .ok()
                .and_then(|n| u64::try_from(n).ok())
                .ok_or_else(|| KeyhiveAccessError::shape(METHOD, "a bigint in the range of u64"))
        }
        .boxed_local()
    }

    fn pending_event_hashes(
        &self,
    ) -> <Local as FutureForm>::Future<'_, AccessResult<Vec<EventHash>>> {
        async move {
            const METHOD: &str = "pendingEventHashes";
            let hashes = settle(METHOD, self.0.js_pending_event_hashes()).await?;
            iterate(&hashes, METHOD)?
                .iter()
                .map(|h| array32(h, METHOD))
                .collect()
        }
        .boxed_local()
    }

    fn is_known_agent(
        &self,
        id: Identifier,
    ) -> <Local as FutureForm>::Future<'_, AccessResult<bool>> {
        async move {
            const METHOD: &str = "isKnownAgent";
            let id = Uint8Array::from(id.as_bytes().as_slice());
            settle(METHOD, self.0.js_is_known_agent(id))
                .await?
                .as_bool()
                .ok_or_else(|| KeyhiveAccessError::shape(METHOD, "a boolean"))
        }
        .boxed_local()
    }

    fn events_for_agent(
        &self,
        id: Identifier,
    ) -> <Local as FutureForm>::Future<'_, AccessResult<BTreeMap<EventHash, EventBytes>>> {
        async move {
            const METHOD: &str = "eventsForAgent";
            let id = Uint8Array::from(id.as_bytes().as_slice());
            let events = settle(METHOD, self.0.js_events_for_agent(id)).await?;
            entries(&events, METHOD)?
                .into_iter()
                .map(|(hash, event)| {
                    Ok((array32(&hash, METHOD)?, EventBytes(bytes(&event, METHOD)?)))
                })
                .collect()
        }
        .boxed_local()
    }

    fn all_agent_events<'a>(
        &'a self,
        skip: &'a BTreeSet<EventHash>,
    ) -> <Local as FutureForm>::Future<'a, AccessResult<AgentEventIndex<EventBytes>>> {
        async move {
            const METHOD: &str = "allAgentEvents";
            let all = settle(METHOD, self.0.js_all_agent_events()).await?;
            let events_field = Reflect::get(&all, &JsValue::from_str("events"))
                .map_err(|e| KeyhiveAccessError::thrown(METHOD, &e))?;

            let mut events = Vec::new();
            for (hash, event) in entries(&events_field, "allAgentEvents().events")? {
                let hash = array32(&hash, METHOD)?;
                // A skipped event's bytes stay in JavaScript.
                if !skip.contains(&hash) {
                    events.push((hash, EventBytes(bytes(&event, METHOD)?)));
                }
            }

            Ok(AgentEventIndex {
                events,
                agent_hashes: agent_hashes(&all)?,
            })
        }
        .boxed_local()
    }

    fn sign_bytes(
        &self,
        payload: Vec<u8>,
    ) -> <Local as FutureForm>::Future<'_, AccessResult<Vec<u8>>> {
        async move {
            const METHOD: &str = "signBytes";
            let payload = Uint8Array::from(payload.as_slice());
            bytes(
                &settle(METHOD, self.0.js_sign_bytes(payload)).await?,
                METHOD,
            )
        }
        .boxed_local()
    }

    fn ingest_events(
        &self,
        events: Vec<EventBytes>,
    ) -> <Local as FutureForm>::Future<'_, AccessResult<Vec<Arc<EventBytes>>>> {
        async move {
            const METHOD: &str = "ingestEvents";
            let array: Array = events
                .iter()
                .map(|e| JsValue::from(Uint8Array::from(e.0.as_slice())))
                .collect();
            let pending = settle(METHOD, self.0.js_ingest_events(array)).await?;
            iterate(&pending, METHOD)?
                .iter()
                .map(|e| Ok(Arc::new(EventBytes(bytes(e, METHOD)?))))
                .collect()
        }
        .boxed_local()
    }

    fn receive_contact_card<'a>(
        &'a self,
        card: &'a [u8],
    ) -> <Local as FutureForm>::Future<'a, AccessResult<EventBytes>> {
        async move {
            const METHOD: &str = "receiveContactCard";
            let card = Uint8Array::from(card);
            let event = settle(METHOD, self.0.js_receive_contact_card(card)).await?;
            Ok(EventBytes(bytes(&event, METHOD)?))
        }
        .boxed_local()
    }

    fn to_archive(&self) -> <Local as FutureForm>::Future<'_, AccessResult<ArchiveBytes>> {
        async move {
            const METHOD: &str = "toArchive";
            let archive = settle(METHOD, self.0.js_to_archive()).await?;
            Ok(ArchiveBytes(bytes(&archive, METHOD)?))
        }
        .boxed_local()
    }

    fn ingest_archive(
        &self,
        archive: ArchiveBytes,
    ) -> <Local as FutureForm>::Future<'_, AccessResult<()>> {
        async move {
            let archive = Uint8Array::from(archive.0.as_slice());
            settle("ingestArchive", self.0.js_ingest_archive(archive))
                .await
                .map(|_| ())
        }
        .boxed_local()
    }
}
