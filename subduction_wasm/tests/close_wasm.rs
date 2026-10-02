//! Exercise the public node lifecycle on the Wasm executor.
#![cfg(target_arch = "wasm32")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
#![allow(clippy::float_cmp)] // JS numbers here are exact integer counters.

use futures::{FutureExt, future::poll_fn};
use js_sys::{Object, Reflect};
use sedimentree_wasm::sedimentree_id::WasmSedimentreeId;
use subduction_core::timeout::Timeout;
use subduction_wasm::{
    memory_signer::JsMemorySigner,
    signer::JsSigner,
    subduction::{JsSubductionOptions, WasmSubduction},
    transport::{JsTransport, authenticated::WasmAuthenticatedTransport, longpoll::JsTimeout},
};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(inline_js = r#"
export function storage() {
  return { reads: 0, released: false,
    loadAllSedimentreeIds: async () => [],
    containsSedimentreeId: async () => false,
    loadAllCommits: async () => [], loadAllFragments: async () => [],
  };
}
export function blockedStorage() {
  const s = storage();
  s.loadAllCommits = () => { s.reads++; return new Promise(resolve => { s.release = () => { s.released = true; resolve([]); }; }); };
  s.loadAllFragments = async () => { s.reads++; return []; };
  return s;
}
export function blockedWriteStorage() {
  const s = storage(); s.writes = 0; s.registrations = 0;
  s.saveCommit = () => new Promise(resolve => { s.release = () => { s.writes++; resolve(); }; });
  s.saveSedimentreeId = async () => { s.registrations++; };
  return s;
}
export function stalledTransport() {
  let rejectRecv;
  return { closes: 0, sends: 0,
    async sendBytes(_) { this.sends++; },
    recvBytes() { return new Promise((_, reject) => { rejectRecv = reject; }); },
    async disconnect() { this.closes++; if (rejectRecv) rejectRecv(new Error('closed')); },
    onDisconnect(_) {},
  };
}
export function pair() {
  function endpoint() {
    return { queue: [], waiting: [], closed: false, closes: 0,
      async sendBytes(bytes) {
        if (this.paused) { this.sentWhilePaused = true; return; }
        if (this.closed || this.other.closed) throw new Error('closed');
        const next = this.other.waiting.shift();
        if (next) next.resolve(bytes); else this.other.queue.push(bytes);
      },
      recvBytes() {
        if (this.closed) return Promise.reject(new Error('closed'));
        if (this.queue.length) return Promise.resolve(this.queue.shift());
        return new Promise((resolve, reject) => this.waiting.push({resolve,reject}));
      },
      async disconnect() {
        this.closes++; this.closed = true;
        for (const waiter of this.waiting.splice(0)) waiter.reject(new Error('closed'));
        for (const waiter of this.other.waiting.splice(0)) waiter.reject(new Error('peer closed'));
      },
      onDisconnect(_) {},
    };
  }
  const a = endpoint(), b = endpoint(); a.other = b; b.other = a; return [a,b];
}
export function policyGate() {
  const p = { authorizeFetch: async () => true, authorizePut: async () => true, filterAuthorizedFetch: async (_,ids) => ids };
  p.authorizeConnect = () => new Promise((resolve, reject) => {
    p.release = () => resolve(true);
    p.reject = () => reject(new Error('registration refused'));
  });
  return p;
}
export function togglePolicy() {
  const p = { authorizeFetch: async () => {}, authorizePut: async () => {}, filterAuthorizedFetch: async (_,ids) => ids };
  p.authorizeConnect = async () => { if (p.deny) throw new Error('registration refused'); };
  return p;
}
export function rejectRegistration(policy) { policy.reject(); }
let originalMessageChannel, trackedPorts;
export function trackMessagePorts() {
  originalMessageChannel = globalThis.MessageChannel;
  trackedPorts = [];
  globalThis.MessageChannel = class {
    constructor() {
      const channel = new originalMessageChannel();
      for (const port of [channel.port1, channel.port2]) {
        const close = port.close.bind(port);
        const counter = {closes: 0};
        trackedPorts.push(counter);
        port.close = () => { counter.closes++; close(); };
      }
      return channel;
    }
  };
}
export function bothPortsClosed() { return trackedPorts.length === 2 && trackedPorts.every(p => p.closes > 0); }
export function restoreMessageChannel() { globalThis.MessageChannel = originalMessageChannel; }
export async function tick() { await new Promise(resolve => setTimeout(resolve, 0)); }
export function release(s) { s.release(); }
let oldWebSocket;
export function mockWebSocket(open) {
  oldWebSocket = globalThis.WebSocket;
  globalThis.WebSocket = class {
    static CONNECTING = 0; static OPEN = 1;
    constructor(_) { this.readyState = open ? 1 : 0; this.closes = 0; globalThis.lastSocket = this; }
    send(_) {}
    close() { this.closes++; this.readyState = 3; }
  };
}
export function restoreWebSocket() { globalThis.WebSocket = oldWebSocket; }
export function lastSocket() { return globalThis.lastSocket; }
let oldFetch;
export function mockFetch() {
  oldFetch = globalThis.fetch;
  globalThis.fetch = request => { globalThis.lastSignal = request.signal; return new Promise((_,reject) => request.signal.addEventListener('abort', () => reject(new Error('aborted')))); };
}
export function restoreFetch() { globalThis.fetch = oldFetch; }
let pollSignals;
export function longPollServer() {
  oldFetch = globalThis.fetch;
  pollSignals = [];
  const [http, server] = pair();
  globalThis.fetch = async request => {
    const path = new URL(request.url).pathname;
    if (path === '/lp/handshake') {
      await http.sendBytes(new Uint8Array(await request.arrayBuffer()));
      const response = await http.recvBytes();
      return new Response(response, {status: 200, headers: {'x-session-id': '11'.repeat(16)}});
    }
    if (path === '/lp/send') {
      await http.sendBytes(new Uint8Array(await request.arrayBuffer()));
      return new Response(null, {status: 200});
    }
    pollSignals.push(request.signal);
    const aborted = new Promise((_, reject) => request.signal.addEventListener('abort', () => reject(new Error('aborted'))));
    const response = await Promise.race([http.recvBytes(), aborted]);
    return new Response(response, {status: 200});
  };
  return server;
}
export function pollCount() { return pollSignals.length; }
export function pollsAborted() { return pollSignals.every(signal => signal.aborted); }
export function fetchStarted() { return !!globalThis.lastSignal; }
export function fetchAborted() { return globalThis.lastSignal.aborted; }
export async function closeThroughBindings(node, id, store) {
  const reading = node.getBlobs(id).then(() => 'unexpected success', error => error.name);
  await tick(); // Allow the API promise to enter storage.
  const closing = node.close();
  if (await reading !== 'SubductionClosedError') throw new Error('active call not cancelled');
  await closing;
  await node.close();
  try { await node.getConnectedPeerIds(); throw new Error('accepted after close'); }
  catch (error) { if (error.name !== 'SubductionClosedError') throw error; }
  node.free(); // No lingering wasm-bindgen borrow from the cancelled API call.
  release(store);
}
export async function reentrantDisconnect(a, b, ta, tb) {
  await Promise.all([a.connectTransport(ta, 'test'), b.acceptTransport(tb, 'test')]);
  const disconnect = ta.disconnect.bind(ta);
  ta.disconnect = async () => { a.close(); await disconnect(); };
  await a.close();
  await b.close();
  a.free(); b.free();
}
"#)]
extern "C" {
    fn storage() -> JsValue;
    #[wasm_bindgen(js_name = blockedStorage)]
    fn blocked_storage() -> JsValue;
    #[wasm_bindgen(js_name = blockedWriteStorage)]
    fn blocked_write_storage() -> JsValue;
    #[wasm_bindgen(js_name = stalledTransport)]
    fn stalled_transport() -> JsTransport;
    fn pair() -> js_sys::Array;
    #[wasm_bindgen(js_name = policyGate)]
    fn policy_gate() -> JsValue;
    #[wasm_bindgen(js_name = togglePolicy)]
    fn toggle_policy() -> JsValue;
    #[wasm_bindgen(js_name = rejectRegistration)]
    fn reject_registration(policy: &JsValue);
    #[wasm_bindgen(js_name = trackMessagePorts)]
    fn track_message_ports();
    #[wasm_bindgen(js_name = bothPortsClosed)]
    fn both_ports_closed() -> bool;
    #[wasm_bindgen(js_name = restoreMessageChannel)]
    fn restore_message_channel();
    async fn tick();
    fn release(value: &JsValue);
    #[wasm_bindgen(js_name = mockWebSocket)]
    fn mock_websocket(open: bool);
    #[wasm_bindgen(js_name = restoreWebSocket)]
    fn restore_websocket();
    #[wasm_bindgen(js_name = lastSocket)]
    fn last_socket() -> JsValue;
    #[wasm_bindgen(js_name = mockFetch)]
    fn mock_fetch();
    #[wasm_bindgen(js_name = restoreFetch)]
    fn restore_fetch();
    #[wasm_bindgen(js_name = longPollServer)]
    fn long_poll_server() -> JsTransport;
    #[wasm_bindgen(js_name = pollCount)]
    fn poll_count() -> u32;
    #[wasm_bindgen(js_name = pollsAborted)]
    fn polls_aborted() -> bool;
    #[wasm_bindgen(js_name = fetchStarted)]
    fn fetch_started() -> bool;
    #[wasm_bindgen(js_name = fetchAborted)]
    fn fetch_aborted() -> bool;
    #[wasm_bindgen(js_name = closeThroughBindings)]
    fn close_through_bindings(node: JsValue, id: JsValue, store: &JsValue) -> js_sys::Promise;
    #[wasm_bindgen(js_name = reentrantDisconnect)]
    fn reentrant_disconnect(a: JsValue, b: JsValue, ta: JsValue, tb: JsValue) -> js_sys::Promise;
}

fn node(seed: u8, storage: &JsValue, policy: Option<&JsValue>) -> WasmSubduction {
    let opts = Object::new();
    Reflect::set(&opts, &"storage".into(), storage).unwrap();
    Reflect::set(
        &opts,
        &"signer".into(),
        &JsMemorySigner::from_bytes(&[seed; 32]).unwrap().into(),
    )
    .unwrap();
    if let Some(policy) = policy {
        Reflect::set(&opts, &"policy".into(), policy).unwrap();
    }
    WasmSubduction::new(opts.unchecked_ref::<JsSubductionOptions>()).unwrap()
}
fn count(value: &JsValue, field: &str) -> f64 {
    Reflect::get(value, &field.into())
        .unwrap()
        .as_f64()
        .unwrap()
}
fn name(error: &JsValue) -> String {
    Reflect::get(error, &"name".into())
        .unwrap()
        .as_string()
        .unwrap()
}
async fn close(node: &WasmSubduction) {
    JsFuture::from(node.close()).await.unwrap();
}

#[wasm_bindgen_test]
async fn js_bindings_release_cancelled_borrows_before_free() {
    let store = blocked_storage();
    let node = node(1, &store, None);
    let id = WasmSedimentreeId::from_bytes(&[1; 32]).unwrap();
    JsFuture::from(close_through_bindings(node.into(), id.into(), &store))
        .await
        .unwrap();
}

#[wasm_bindgen_test]
async fn disconnect_callback_can_reenter_close() {
    let a = node(1, &storage(), None);
    let b = node(2, &storage(), None);
    let transports = pair();
    JsFuture::from(reentrant_disconnect(
        a.into(),
        b.into(),
        transports.get(0),
        transports.get(1),
    ))
    .await
    .unwrap();
}

#[wasm_bindgen_test]
async fn close_before_background_start_is_idempotent_and_rejects_new_work() {
    let node = node(1, &storage(), None);
    let first = node.close(); // Admission stops synchronously, before the promise is polled.
    assert_eq!(
        name(&node.sedimentree_ids().await.unwrap_err()),
        "SubductionClosedError"
    );
    let second = node.close();
    let (a, b) = futures::join!(JsFuture::from(first), JsFuture::from(second));
    a.unwrap();
    b.unwrap();
    close(&node).await;
    assert_eq!(
        name(
            &node
                .store_built_batch(
                    &WasmSedimentreeId::from_bytes(&[1; 32]).unwrap(),
                    vec![],
                    vec![]
                )
                .await
                .unwrap_err()
        ),
        "SubductionClosedError"
    );
}

#[wasm_bindgen_test]
async fn close_cancels_handshake_and_cleans_unregistered_transport() {
    let node = node(1, &storage(), None);
    let transport = stalled_transport();
    let mut connect = node
        .connect_transport(transport.clone(), "test".into())
        .boxed_local();
    poll_fn(|cx| {
        assert!(connect.as_mut().poll(cx).is_pending());
        core::task::Poll::Ready(())
    })
    .await;
    tick().await;
    let ((), result) = futures::join!(close(&node), connect);
    assert_eq!(name(&result.unwrap_err()), "SubductionClosedError");
    assert_eq!(count(transport.as_ref(), "closes"), 1.0);
    // A rejected post-close call does not take ownership of its argument.
    let unused = stalled_transport();
    assert!(
        node.connect_transport(unused.clone(), "test".into())
            .await
            .is_err()
    );
    assert_eq!(count(unused.as_ref(), "closes"), 0.0);
}

#[wasm_bindgen_test]
async fn close_cancels_storage_wait_but_does_not_claim_to_cancel_javascript() {
    let store = blocked_storage();
    let node = node(1, &store, None);
    let id = WasmSedimentreeId::from_bytes(&[1; 32]).unwrap();
    let mut read = node.get_blobs(&id).boxed_local();
    poll_fn(|cx| {
        assert!(read.as_mut().poll(cx).is_pending());
        core::task::Poll::Ready(())
    })
    .await;
    let close = node.close();
    // Poll the Rust operation as wasm-bindgen's executor does for JS-facing calls.
    assert_eq!(name(&read.await.unwrap_err()), "SubductionClosedError");
    JsFuture::from(close).await.unwrap();
    let reads = count(&store, "reads");
    release(&store); // The foreign promise can still complete after close.
    tick().await;
    assert_eq!(count(&store, "reads"), reads); // No continuation starts another storage call.
}

#[wasm_bindgen_test]
async fn close_does_not_rollback_an_already_started_storage_write() {
    let store = blocked_write_storage();
    let node = node(1, &store, None);
    let id = WasmSedimentreeId::from_bytes(&[1; 32]).unwrap();
    let head = sedimentree_wasm::commit_id::WasmCommitId::from_bytes(&[2; 32]).unwrap();
    let blob = js_sys::Uint8Array::from(&[1u8][..]);
    let writing = node.store_commit(&id, &head, vec![], &blob);
    let closing = async {
        while Reflect::get(&store, &"release".into())
            .unwrap()
            .is_undefined()
        {
            tick().await;
        }
        close(&node).await;
    };
    let (result, ()) = futures::join!(writing, closing);
    assert_eq!(name(&result.unwrap_err()), "SubductionClosedError");
    release(&store);
    tick().await;
    assert_eq!(count(&store, "writes"), 1.0); // The JS side effect still happens.
    assert_eq!(count(&store, "registrations"), 0.0); // Rust continuation was cancelled.
}

#[wasm_bindgen_test]
async fn duplicate_registration_does_not_disconnect_an_existing_connection() {
    let policy = toggle_policy();
    let a = node(1, &storage(), Some(&policy));
    let b = node(2, &storage(), None);
    let endpoints = pair();
    let ta: JsTransport = endpoints.get(0).unchecked_into();
    let tb: JsTransport = endpoints.get(1).unchecked_into();
    let signer: JsSigner =
        JsValue::from(JsMemorySigner::from_bytes(&[1; 32]).unwrap()).unchecked_into();
    let (authenticated, accepted) = futures::join!(
        WasmAuthenticatedTransport::setup_discover(ta.clone(), &signer, Some("test".into()), None),
        b.accept_transport(tb, "test".into()),
    );
    let authenticated = authenticated.unwrap();
    accepted.unwrap();
    assert!(a.add_connection(&authenticated).await.unwrap());
    assert!(!a.add_connection(&authenticated).await.unwrap());
    Reflect::set(&policy, &"deny".into(), &JsValue::TRUE).unwrap();
    let error = a.add_connection(&authenticated).await.unwrap_err();
    assert_eq!(name(&error), "AddConnectionError");
    tick().await; // Give an incorrectly armed cleanup guard a chance to run.
    assert_eq!(count(ta.as_ref(), "closes"), 0.0);
    assert_eq!(a.connected_peer_ids().await.unwrap().len(), 1);
    let id = WasmSedimentreeId::from_bytes(&[1; 32]).unwrap();
    assert!(
        a.sync_with_peer(&authenticated.peer_id(), &id, false, None)
            .await
            .unwrap()
            .success()
    );
    close(&a).await;
    close(&b).await;
    assert_eq!(count(ta.as_ref(), "closes"), 1.0);
}

async fn wait_for_peer_count(node: &WasmSubduction, count: usize) {
    JsTimeout
        .timeout(
            core::time::Duration::from_secs(2),
            async {
                while node.connected_peer_ids().await.unwrap().len() != count {
                    tick().await;
                }
            }
            .boxed_local(),
        )
        .await
        .expect("peer count should settle");
}

async fn partial_link_cleanup(cancel: bool) {
    track_message_ports();
    let policy = policy_gate();
    let a = node(1, &storage(), None);
    let b = node(2, &storage(), Some(&policy));
    let linking = WasmSubduction::link(&a, &b);
    let interrupt = async {
        // One side has actually registered before the other side fails/cancels.
        wait_for_peer_count(&a, 1).await;
        if cancel {
            close(&a).await;
        } else {
            reject_registration(&policy);
        }
    };
    let (result, ()) = futures::join!(linking, interrupt);
    assert_eq!(
        name(&result.unwrap_err()),
        if cancel {
            "SubductionClosedError"
        } else {
            "ConnectError"
        }
    );
    if cancel {
        release(&policy);
    } else {
        wait_for_peer_count(&a, 0).await;
    }
    wait_for_peer_count(&b, 0).await;
    // Cleanup must happen because link failed, not because we close both nodes.
    JsTimeout
        .timeout(
            core::time::Duration::from_secs(2),
            async {
                while !both_ports_closed() {
                    tick().await;
                }
            }
            .boxed_local(),
        )
        .await
        .expect("both link guards should disconnect");
    close(&a).await;
    close(&b).await;
    restore_message_channel();
}

#[wasm_bindgen_test]
async fn link_registration_failure_disconnects_both_sides() {
    partial_link_cleanup(false).await;
}

#[wasm_bindgen_test]
async fn link_cancellation_disconnects_both_sides() {
    partial_link_cleanup(true).await;
}

#[wasm_bindgen_test]
async fn link_owns_and_closes_real_message_ports() {
    let a = node(1, &storage(), None);
    let b = node(2, &storage(), None);
    WasmSubduction::link(&a, &b).await.unwrap();
    assert_eq!(a.connected_peer_ids().await.unwrap().len(), 1);
    let ((), ()) = futures::join!(close(&a), close(&b));
}

#[wasm_bindgen_test]
async fn connected_nodes_close_all_transports_and_dropped_waiter_does_not_stop_close() {
    let a = node(1, &storage(), None);
    let b = node(2, &storage(), None);
    let transports = pair();
    let ta: JsTransport = transports.get(0).unchecked_into();
    let tb: JsTransport = transports.get(1).unchecked_into();
    let (a_peer, b_peer) = futures::join!(
        a.connect_transport(ta.clone(), "test".into()),
        b.accept_transport(tb.clone(), "test".into())
    );
    let peer = a_peer.unwrap();
    b_peer.unwrap();
    assert_eq!(a.connected_peer_ids().await.unwrap().len(), 1);
    let id = WasmSedimentreeId::from_bytes(&[1; 32]).unwrap();
    let result = a.sync_with_peer(&peer, &id, false, None).await.unwrap();
    assert!(result.success()); // The empty exchange still completes normally.
    drop(a.close());
    tick().await;
    close(&a).await;
    close(&b).await;
    assert_eq!(count(ta.as_ref(), "closes"), 1.0);
    assert_eq!(count(tb.as_ref(), "closes"), 1.0);
}

#[wasm_bindgen_test]
async fn freeing_node_initiates_transport_cleanup() {
    let a = node(1, &storage(), None);
    let b = node(2, &storage(), None);
    let transports = pair();
    let ta: JsTransport = transports.get(0).unchecked_into();
    let tb: JsTransport = transports.get(1).unchecked_into();
    let (x, y) = futures::join!(
        a.connect_transport(ta.clone(), "test".into()),
        b.accept_transport(tb, "test".into())
    );
    x.unwrap();
    y.unwrap();
    drop(a);
    tick().await;
    assert_eq!(count(ta.as_ref(), "closes"), 1.0);
    close(&b).await;
}

#[wasm_bindgen_test]
async fn close_cancels_websocket_dial_and_handshake_without_dangling_callbacks() {
    for open in [false, true] {
        mock_websocket(open);
        let node = node(1, &storage(), None);
        let url = web_sys::Url::new("ws://example.test").unwrap();
        let connecting = node.connect_discover(&url, None);
        let closing = async {
            tick().await;
            close(&node).await;
        };
        let (result, ()) = futures::join!(connecting, closing);
        assert_eq!(name(&result.unwrap_err()), "SubductionClosedError");
        let socket = last_socket();
        assert!(count(&socket, "closes") >= 1.0);
        for event in ["onopen", "onerror", "onmessage", "onclose"] {
            let callback = Reflect::get(&socket, &event.into()).unwrap();
            assert!(callback.is_null() || callback.is_undefined());
        }
        restore_websocket();
    }
}

#[wasm_bindgen_test]
async fn close_during_longpoll_connect_aborts_the_http_request() {
    mock_fetch();
    let node = node(1, &storage(), None);
    let connecting = node.connect_discover_long_poll("https://example.test", None);
    let closing = async {
        while !fetch_started() {
            tick().await;
        }
        close(&node).await;
    };
    let (result, ()) = futures::join!(connecting, closing);
    assert_eq!(name(&result.unwrap_err()), "SubductionClosedError");
    assert!(fetch_aborted());
    restore_fetch();
}

#[wasm_bindgen_test]
async fn close_stops_established_longpoll_drivers_and_aborts_pending_fetch() {
    let server_transport = long_poll_server();
    let a = node(1, &storage(), None);
    let b = node(2, &storage(), None);
    let (connected, accepted) = futures::join!(
        a.connect_discover_long_poll("https://example.test", Some("test".into())),
        b.accept_transport(server_transport, "test".into()),
    );
    let peer = connected.unwrap();
    accepted.unwrap();
    let id = WasmSedimentreeId::from_bytes(&[1; 32]).unwrap();
    assert!(
        a.sync_with_peer(&peer, &id, false, None)
            .await
            .unwrap()
            .success()
    );
    while poll_count() == 0 {
        tick().await;
    }
    close(&a).await;
    assert!(polls_aborted());
    let count = poll_count();
    tick().await;
    assert_eq!(poll_count(), count);
    close(&b).await;
    restore_fetch();
}

#[wasm_bindgen_test]
async fn close_wakes_a_pending_sync() {
    let a = node(1, &storage(), None);
    let b = node(2, &storage(), None);
    let transports = pair();
    let ta: JsTransport = transports.get(0).unchecked_into();
    let tb: JsTransport = transports.get(1).unchecked_into();
    let (a_peer, b_peer) = futures::join!(
        a.connect_transport(ta.clone(), "test".into()),
        b.accept_transport(tb, "test".into())
    );
    let peer = a_peer.unwrap();
    b_peer.unwrap();
    Reflect::set(ta.as_ref(), &"paused".into(), &JsValue::TRUE).unwrap();
    let id = WasmSedimentreeId::from_bytes(&[1; 32]).unwrap();
    let syncing = a.sync_with_peer(&peer, &id, false, Some(60_000));
    let closing = async {
        while Reflect::get(ta.as_ref(), &"sentWhilePaused".into())
            .unwrap()
            .is_undefined()
        {
            tick().await;
        }
        close(&a).await;
    };
    let (result, ()) = futures::join!(syncing, closing);
    assert_eq!(name(&result.unwrap_err()), "SubductionClosedError");
    close(&b).await;
}

#[wasm_bindgen_test]
async fn close_during_registration_cannot_admit_a_late_connection() {
    let policy = policy_gate();
    let a = node(1, &storage(), Some(&policy));
    let b = node(2, &storage(), None);
    let transports = pair();
    let ta: JsTransport = transports.get(0).unchecked_into();
    let tb: JsTransport = transports.get(1).unchecked_into();
    let connecting = async {
        futures::join!(
            a.connect_transport(ta.clone(), "test".into()),
            b.accept_transport(tb, "test".into())
        )
    };
    let closing = async {
        while Reflect::get(&policy, &"release".into())
            .unwrap()
            .is_undefined()
        {
            tick().await;
        }
        close(&a).await;
        release(&policy);
    };
    let ((result, _), ()) = futures::join!(connecting, closing);
    assert_eq!(name(&result.unwrap_err()), "SubductionClosedError");
    assert_eq!(count(ta.as_ref(), "closes"), 1.0);
    close(&b).await;
}
