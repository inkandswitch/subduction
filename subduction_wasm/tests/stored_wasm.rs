//! Public async-iterator behavior, recovery, and late JavaScript writes.
#![cfg(target_arch = "wasm32")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use futures::{
    FutureExt,
    future::{AbortHandle, Abortable},
};
use js_sys::{Object, Reflect, Uint8Array};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use sedimentree_wasm::storage::JsStorage;
use std::{collections::BTreeSet, time::Duration};
use subduction_core::timeout::Timeout;
use subduction_wasm::{
    memory_signer::JsMemorySigner,
    subduction::{JsSubductionOptions, Node},
    transport::longpoll::JsTimeout,
};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

// Exercise the same JS backend as the public wrapper, without exporting a
// redundant Rust -> JS -> Rust factory from the library bindings.
#[wasm_bindgen(module = "/js/storage.js")]
extern "C" {
    #[wasm_bindgen(js_name = memoryStorage)]
    fn create_memory_storage() -> JsStorage;
}

#[wasm_bindgen(inline_js = r#"
export function nextEvent(iterator) {
    if (iterator[Symbol.asyncIterator]() !== iterator) throw new Error('not an async iterator');
    return iterator.next();
}
export function closeIterator(iterator) { iterator.close(); }
export function reentrantNext(iterator) {
    const original = Object.getOwnPropertyDescriptor(Object.prototype, 'type');
    Object.defineProperty(Object.prototype, 'type', {
        configurable: true,
        set(value) {
            Object.defineProperty(this, 'type', { value, writable: true, configurable: true, enumerable: true });
            iterator.close();
        }
    });
    return iterator.next().finally(() => {
        if (original) Object.defineProperty(Object.prototype, 'type', original);
        else delete Object.prototype.type;
    });
}
export async function breakIterator(iterator) { for await (const item of iterator) { return item.type; } }
export async function tick() { await new Promise(resolve => setTimeout(resolve, 0)); }
export function gateWrite(storage) {
    const save = storage.saveCommit.bind(storage);
    storage.saveCommit = (...args) => {
        storage.entered = true;
        return new Promise((resolve, reject) => {
            storage.release = async () => { try { await save(...args); resolve(); } catch(e) { reject(e); } };
        });
    };
}
export function partialBatch(storage) {
    storage.saveBatchAll = async (id, commits, fragments) => {
        const commit = commits[0];
        await storage.saveCommit(id, commit.commitId, commit.signedCommit, commit.blob);
        throw new Error('failed after first item persisted');
    };
}
export function gateScan(storage) {
    const load = storage.loadAllCommits.bind(storage);
    let first = true;
    storage.loadAllCommits = async (...args) => {
        const snapshot = await load(...args);
        if (first) {
            first = false;
            storage.entered = true;
            await new Promise(resolve => { storage.release = resolve; });
        }
        return snapshot;
    };
}
export function release(storage) { return Promise.resolve(storage.release()); }
export function abortOptions() {
    const controller = new AbortController();
    const signal = controller.signal;
    const listeners = new Set();
    const add = signal.addEventListener.bind(signal);
    const remove = signal.removeEventListener.bind(signal);
    signal.addEventListener = (type, callback, options) => {
        if (type === 'abort') listeners.add(callback);
        add(type, callback, options);
    };
    signal.removeEventListener = (type, callback, options) => {
        if (type === 'abort') listeners.delete(callback);
        remove(type, callback, options);
    };
    return { signal, controller, listeners };
}
export function abortOptionsNow(options, reason) { options.controller.abort(reason); }
export function abortListenerCount(options) { return options.listeners.size; }
export function timeoutOptions() { return { signal: AbortSignal.timeout(200) }; }
export function abortDuringNext(iterator, options, reason) {
    const original = Object.getOwnPropertyDescriptor(Object.prototype, 'type');
    Object.defineProperty(Object.prototype, 'type', {
        configurable: true,
        set(value) {
            Object.defineProperty(this, 'type', { value, configurable: true, enumerable: true });
            options.controller.abort(reason);
        }
    });
    return iterator.next().finally(() => {
        if (original) Object.defineProperty(Object.prototype, 'type', original);
        else delete Object.prototype.type;
    });
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = nextEvent)]
    fn next_event(iterator: &JsValue) -> js_sys::Promise;
    #[wasm_bindgen(js_name = closeIterator)]
    fn close_iterator(iterator: &JsValue);
    #[wasm_bindgen(js_name = reentrantNext)]
    fn reentrant_next(iterator: &JsValue) -> js_sys::Promise;
    #[wasm_bindgen(js_name = breakIterator)]
    fn break_iterator(iterator: &JsValue) -> js_sys::Promise;
    #[wasm_bindgen(js_name = gateWrite)]
    fn gate_write(storage: &JsValue);
    #[wasm_bindgen(js_name = partialBatch)]
    fn partial_batch(storage: &JsValue);
    #[wasm_bindgen(js_name = gateScan)]
    fn gate_scan(storage: &JsValue);
    fn release(storage: &JsValue) -> js_sys::Promise;
    async fn tick();
    #[wasm_bindgen(js_name = abortOptions)]
    fn abort_options() -> JsValue;
    #[wasm_bindgen(js_name = abortOptionsNow)]
    fn abort_options_now(options: &JsValue, reason: &JsValue);
    #[wasm_bindgen(js_name = abortListenerCount)]
    fn abort_listener_count(options: &JsValue) -> u32;
    #[wasm_bindgen(js_name = timeoutOptions)]
    fn timeout_options() -> JsValue;
    #[wasm_bindgen(js_name = abortDuringNext)]
    fn abort_during_next(
        iterator: &JsValue,
        options: &JsValue,
        reason: &JsValue,
    ) -> js_sys::Promise;
}
fn node(seed: u8, storage: &JsValue) -> Node {
    let opts = Object::new();
    Reflect::set(&opts, &"storage".into(), storage).unwrap();
    Reflect::set(
        &opts,
        &"signer".into(),
        &JsMemorySigner::from_bytes(&[seed; 32]).unwrap().into(),
    )
    .unwrap();
    Node::new(opts.unchecked_ref::<JsSubductionOptions>()).unwrap()
}
const fn id() -> SedimentreeId {
    SedimentreeId::new([33; 32])
}
fn kind(event: &JsValue) -> String {
    Reflect::get(event, &"type".into())
        .unwrap()
        .as_string()
        .unwrap()
}
async fn event(iterator: &JsValue) -> JsValue {
    let result = JsTimeout
        .timeout(
            Duration::from_secs(3),
            JsFuture::from(next_event(iterator)).boxed_local(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        Reflect::get(&result, &"done".into()).unwrap(),
        JsValue::FALSE
    );
    Reflect::get(&result, &"value".into()).unwrap()
}
async fn entered(storage: &JsValue) {
    JsTimeout
        .timeout(
            Duration::from_secs(3),
            async {
                while !Reflect::get(storage, &"entered".into())
                    .unwrap()
                    .is_truthy()
                {
                    tick().await;
                }
            }
            .boxed_local(),
        )
        .await
        .unwrap();
}
async fn close(node: &Node) {
    JsFuture::from(node.close()).await.unwrap();
}

#[wasm_bindgen_test]
async fn iterator_markers_stay_open_and_close_interrupts_next() {
    let storage = JsValue::from(create_memory_storage());
    let node = node(1, &storage);
    let iterator: JsValue = node.watch_stored(&id()).unwrap().into();
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    node.store_commit(&id(), &CommitId::new([2; 32]), vec![], Blob::new(vec![4]))
        .await
        .unwrap();
    let commit = event(&iterator).await;
    assert_eq!(kind(&commit), "commit");
    assert_eq!(
        Uint8Array::new(&Reflect::get(&commit, &"blob".into()).unwrap()).to_vec(),
        vec![4]
    );
    let waiting = JsFuture::from(next_event(&iterator));
    tick().await;
    close_iterator(&iterator);
    let result = waiting.await.unwrap();
    assert_eq!(
        Reflect::get(&result, &"done".into()).unwrap(),
        JsValue::TRUE
    );
    close(&node).await;
}

#[wasm_bindgen_test]
async fn for_await_break_and_node_close_release_iterators() {
    let storage = JsValue::from(create_memory_storage());
    let node = node(2, &storage);
    let iterator: JsValue = node.watch_stored(&id()).unwrap().into();
    assert_eq!(
        JsFuture::from(break_iterator(&iterator))
            .await
            .unwrap()
            .as_string()
            .unwrap(),
        "storageChecked"
    );
    assert_eq!(
        Reflect::get(
            &JsFuture::from(next_event(&iterator)).await.unwrap(),
            &"done".into()
        )
        .unwrap(),
        JsValue::TRUE
    );
    let iterator: JsValue = node.watch_stored(&id()).unwrap().into();
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    let waiting = JsFuture::from(next_event(&iterator));
    tick().await;
    close(&node).await;
    assert_eq!(
        Reflect::get(&waiting.await.unwrap_err(), &"name".into()).unwrap(),
        "SubductionClosedError"
    );
}

#[wasm_bindgen_test]
async fn cancelled_caller_does_not_hide_a_later_storage_completion() {
    let storage = JsValue::from(create_memory_storage());
    gate_write(&storage);
    let node = node(3, &storage);
    let iterator: JsValue = node.watch_stored(&id()).unwrap().into();
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    let id = id();
    let head = CommitId::new([3; 32]);
    let blob = Blob::new(vec![5]);
    let (abort, registration) = AbortHandle::new_pair();
    let writing = Abortable::new(node.store_commit(&id, &head, vec![], blob), registration);
    let cancel = async {
        entered(&storage).await;
        abort.abort();
    };
    let (result, ()) = futures::join!(writing, cancel);
    assert!(result.is_err());
    let mut pending_event = JsFuture::from(next_event(&iterator));
    assert!((&mut pending_event).now_or_never().is_none());
    JsFuture::from(release(&storage)).await.unwrap();
    let next = pending_event.await.unwrap();
    assert_eq!(
        kind(&Reflect::get(&next, &"value".into()).unwrap()),
        "commit"
    );
    close(&node).await;
}

#[wasm_bindgen_test]
async fn partial_batch_failure_rescans_persisted_items() {
    let storage = JsValue::from(create_memory_storage());
    partial_batch(&storage);
    let node = node(4, &storage);
    let iterator: JsValue = node.watch_stored(&id()).unwrap().into();
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    let commits = (1..=2)
        .map(|seed| {
            let blob = Blob::new(vec![seed]);
            let commit = LooseCommit::new(
                SedimentreeId::new([33; 32]),
                CommitId::new([seed; 32]),
                BTreeSet::new(),
                BlobMeta::new(&blob),
            )
            .unwrap();
            (commit, blob)
        })
        .collect();
    assert!(
        node.store_built_batch(&id(), commits, vec![])
            .await
            .is_err()
    );
    assert_eq!(kind(&event(&iterator).await), "rescanRequired");
    assert_eq!(kind(&event(&iterator).await), "commit");
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    close(&node).await;
}

#[wasm_bindgen_test]
async fn overlapping_scan_and_write_cannot_miss_the_item() {
    let storage = JsValue::from(create_memory_storage());
    gate_scan(&storage);
    let node = node(5, &storage);
    let iterator: JsValue = node.watch_stored(&id()).unwrap().into();
    entered(&storage).await;
    node.store_commit(&id(), &CommitId::new([6; 32]), vec![], Blob::new(vec![6]))
        .await
        .unwrap();
    JsFuture::from(release(&storage)).await.unwrap();
    assert_eq!(kind(&event(&iterator).await), "commit");
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    close(&node).await;
}

#[wasm_bindgen_test]
async fn close_settles_concurrent_reads_and_is_reentrant_during_conversion() {
    let node = node(8, &JsValue::from(create_memory_storage()));
    let iterator: JsValue = node.watch_stored(&id()).unwrap().into();
    let result = JsFuture::from(reentrant_next(&iterator)).await.unwrap();
    assert_eq!(
        Reflect::get(&result, &"done".into()).unwrap(),
        JsValue::TRUE
    );
    let iterator: JsValue = node.watch_stored(&id()).unwrap().into();
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    let first = JsFuture::from(next_event(&iterator));
    let second = JsFuture::from(next_event(&iterator));
    tick().await;
    close_iterator(&iterator);
    close_iterator(&iterator);
    for result in [first.await.unwrap(), second.await.unwrap()] {
        assert_eq!(
            Reflect::get(&result, &"done".into()).unwrap(),
            JsValue::TRUE
        );
    }
    close(&node).await;
}

#[wasm_bindgen_test]
async fn empty_loading_waits_until_shutdown_and_closed_nodes_refuse_iterators() {
    let node = node(9, &JsValue::from(create_memory_storage()));
    let iterator: JsValue = node.load_and_watch(&id(), None).unwrap().into();
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    let pending = JsFuture::from(next_event(&iterator));
    tick().await;
    close(&node).await;
    let error = pending.await.unwrap_err();
    assert_eq!(
        Reflect::get(&error, &"name".into()).unwrap(),
        "SubductionClosedError"
    );
    assert_eq!(
        Reflect::get(
            &JsFuture::from(next_event(&iterator)).await.unwrap(),
            &"done".into()
        )
        .unwrap(),
        JsValue::TRUE
    );
    close(&node).await;
    for error in [
        node.watch_stored(&id()).err().unwrap(),
        node.load_and_watch(&id(), None).err().unwrap(),
    ] {
        assert_eq!(
            Reflect::get(&error, &"name".into()).unwrap(),
            "SubductionClosedError"
        );
    }
}

#[wasm_bindgen_test]
async fn already_aborted_load_throws_reason_without_starting_storage() {
    let storage = JsValue::from(create_memory_storage());
    gate_scan(&storage);
    let node = node(12, &storage);
    let options = abort_options();
    let reason = Object::new().into();
    abort_options_now(&options, &reason);
    let error = node
        .load_and_watch(&id(), Some(options.clone().unchecked_into()))
        .err()
        .unwrap();
    assert_eq!(error, reason);
    tick().await;
    assert!(
        !Reflect::get(&storage, &"entered".into())
            .unwrap()
            .is_truthy()
    );
    assert_eq!(abort_listener_count(&options), 0);
    close(&node).await;
}

#[wasm_bindgen_test]
async fn abort_interrupts_storage_wait_and_all_queued_reads_with_exact_reason() {
    let storage = JsValue::from(create_memory_storage());
    gate_scan(&storage);
    let node = node(13, &storage);
    let options = abort_options();
    let iterator: JsValue = node
        .load_and_watch(&id(), Some(options.clone().unchecked_into()))
        .unwrap()
        .into();
    let first = JsFuture::from(next_event(&iterator));
    let second = JsFuture::from(next_event(&iterator));
    entered(&storage).await;
    assert_eq!(abort_listener_count(&options), 1);
    let reason = Object::new().into();
    abort_options_now(&options, &reason);
    assert_eq!(first.await.unwrap_err(), reason);
    assert_eq!(second.await.unwrap_err(), reason);
    assert_eq!(
        JsFuture::from(next_event(&iterator)).await.unwrap_err(),
        reason
    );
    assert_eq!(abort_listener_count(&options), 0);
    // Abort cancels observation, not the already-started JS storage promise.
    JsFuture::from(release(&storage)).await.unwrap();
    close(&node).await;
}

#[wasm_bindgen_test]
async fn caller_timeout_signal_rejects_with_timeout_reason() {
    let storage = JsValue::from(create_memory_storage());
    gate_scan(&storage);
    let node = node(14, &storage);
    let options = timeout_options();
    let iterator: JsValue = node
        .load_and_watch(&id(), Some(options.clone().unchecked_into()))
        .unwrap()
        .into();
    let error = JsTimeout
        .timeout(
            Duration::from_secs(3),
            JsFuture::from(next_event(&iterator)).boxed_local(),
        )
        .await
        .unwrap()
        .unwrap_err();
    let signal = Reflect::get(&options, &"signal".into()).unwrap();
    assert_eq!(error, Reflect::get(&signal, &"reason".into()).unwrap());
    assert_eq!(
        Reflect::get(&error, &"name".into()).unwrap(),
        "TimeoutError"
    );
    JsFuture::from(release(&storage)).await.unwrap();
    close(&node).await;
}

#[wasm_bindgen_test]
async fn signal_listeners_are_removed_on_close_break_and_shutdown() {
    let node = node(15, &JsValue::from(create_memory_storage()));
    for break_loop in [false, true] {
        let options = abort_options();
        let iterator: JsValue = node
            .load_and_watch(&id(), Some(options.clone().unchecked_into()))
            .unwrap()
            .into();
        assert_eq!(abort_listener_count(&options), 1);
        if break_loop {
            JsFuture::from(break_iterator(&iterator)).await.unwrap();
        } else {
            close_iterator(&iterator);
            close_iterator(&iterator);
        }
        assert_eq!(abort_listener_count(&options), 0);
        abort_options_now(&options, &"too late".into());
        let result = JsFuture::from(next_event(&iterator)).await.unwrap();
        assert_eq!(
            Reflect::get(&result, &"done".into()).unwrap(),
            JsValue::TRUE
        );
    }
    let options = abort_options();
    let iterator: JsValue = node
        .load_and_watch(&id(), Some(options.clone().unchecked_into()))
        .unwrap()
        .into();
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    let pending = JsFuture::from(next_event(&iterator));
    close(&node).await;
    assert!(pending.await.is_err());
    assert_eq!(abort_listener_count(&options), 0);
}

#[wasm_bindgen_test]
async fn abort_can_reenter_during_event_conversion() {
    let node = node(17, &JsValue::from(create_memory_storage()));
    let options = abort_options();
    let iterator: JsValue = node
        .load_and_watch(&id(), Some(options.clone().unchecked_into()))
        .unwrap()
        .into();
    let reason = Object::new().into();
    assert_eq!(
        JsFuture::from(abort_during_next(&iterator, &options, &reason))
            .await
            .unwrap_err(),
        reason
    );
    assert_eq!(abort_listener_count(&options), 0);
    close(&node).await;
}

#[wasm_bindgen_test]
async fn node_shutdown_rejects_pending_signal_reads_and_removes_listener() {
    let storage = JsValue::from(create_memory_storage());
    gate_scan(&storage);
    let node = node(16, &storage);
    let options = abort_options();
    let iterator: JsValue = node
        .load_and_watch(&id(), Some(options.clone().unchecked_into()))
        .unwrap()
        .into();
    let pending = JsFuture::from(next_event(&iterator));
    entered(&storage).await;
    close(&node).await;
    let error = pending.await.unwrap_err();
    assert_eq!(
        Reflect::get(&error, &"name".into()).unwrap(),
        "SubductionClosedError"
    );
    assert_eq!(abort_listener_count(&options), 0);
    JsFuture::from(release(&storage)).await.unwrap();
}

#[wasm_bindgen_test]
async fn cached_load_syncs_with_peer_and_receives_subscribed_updates() {
    let a = node(10, &JsValue::from(create_memory_storage()));
    let b = node(11, &JsValue::from(create_memory_storage()));
    for (node, seed) in [(&a, 10_u8), (&b, 11_u8)] {
        node.store_commit(
            &id(),
            &CommitId::new([seed; 32]),
            vec![],
            Blob::new(vec![seed]),
        )
        .await
        .unwrap();
    }
    Node::link(&a, &b).await.unwrap();
    let options = abort_options();
    let iterator: JsValue = a
        .load_and_watch(&id(), Some(options.clone().unchecked_into()))
        .unwrap()
        .into();
    assert_eq!(kind(&event(&iterator).await), "commit");
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    let remote = event(&iterator).await;
    assert_eq!(kind(&remote), "commit");
    assert_eq!(
        Uint8Array::new(&Reflect::get(&remote, &"blob".into()).unwrap()).to_vec(),
        vec![11]
    );
    assert_eq!(kind(&event(&iterator).await), "peerSynced");
    b.add_commit(&id(), &CommitId::new([12; 32]), vec![], Blob::new(vec![12]))
        .await
        .unwrap();
    let pushed = event(&iterator).await;
    assert_eq!(kind(&pushed), "commit");
    assert_eq!(
        Uint8Array::new(&Reflect::get(&pushed, &"blob".into()).unwrap()).to_vec(),
        vec![12]
    );
    let pending = JsFuture::from(next_event(&iterator));
    tick().await;
    abort_options_now(&options, &"stop watching".into());
    assert_eq!(
        pending.await.unwrap_err(),
        JsValue::from_str("stop watching")
    );
    assert_eq!(abort_listener_count(&options), 0);
    close(&a).await;
    close(&b).await;
}

async fn follows_connections_for_stream_lifetime(cached: bool) {
    let a = node(20, &JsValue::from(create_memory_storage()));
    let b = node(21, &JsValue::from(create_memory_storage()));
    if cached {
        a.store_commit(&id(), &CommitId::new([1; 32]), vec![], Blob::new(vec![1]))
            .await
            .unwrap();
    }
    let iterator: JsValue = a.load_and_watch(&id(), None).unwrap().into();
    if cached {
        assert_eq!(kind(&event(&iterator).await), "commit");
    }
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    tick().await;
    b.store_commit(&id(), &CommitId::new([2; 32]), vec![], Blob::new(vec![2]))
        .await
        .unwrap();
    Node::link(&a, &b).await.unwrap();
    assert_eq!(kind(&event(&iterator).await), "commit");
    assert_eq!(kind(&event(&iterator).await), "peerSynced");
    b.add_commit(&id(), &CommitId::new([3; 32]), vec![], Blob::new(vec![3]))
        .await
        .unwrap();
    assert_eq!(kind(&event(&iterator).await), "commit");

    a.disconnect_all().await.unwrap();
    b.disconnect_all().await.unwrap();
    b.store_commit(&id(), &CommitId::new([4; 32]), vec![], Blob::new(vec![4]))
        .await
        .unwrap();
    Node::link(&a, &b).await.unwrap();
    let recovered = event(&iterator).await;
    assert_eq!(kind(&recovered), "commit");
    assert_eq!(
        Uint8Array::new(&Reflect::get(&recovered, &"blob".into()).unwrap()).to_vec(),
        vec![4]
    );
    assert_eq!(kind(&event(&iterator).await), "peerSynced");
    b.add_commit(&id(), &CommitId::new([5; 32]), vec![], Blob::new(vec![5]))
        .await
        .unwrap();
    assert_eq!(kind(&event(&iterator).await), "commit");

    let c = node(22, &JsValue::from(create_memory_storage()));
    c.store_commit(&id(), &CommitId::new([6; 32]), vec![], Blob::new(vec![6]))
        .await
        .unwrap();
    Node::link(&a, &c).await.unwrap();
    assert_eq!(kind(&event(&iterator).await), "commit");
    assert_eq!(kind(&event(&iterator).await), "peerSynced");
    close_iterator(&iterator);
    close(&a).await;
    close(&b).await;
    close(&c).await;
}

#[wasm_bindgen_test]
async fn initially_empty_load_follows_late_peers_and_reconnects() {
    follows_connections_for_stream_lifetime(false).await;
}

#[wasm_bindgen_test]
async fn cached_load_follows_late_peers_and_reconnects() {
    follows_connections_for_stream_lifetime(true).await;
}

#[wasm_bindgen_test]
async fn load_from_real_peer_and_fetch_unknown_tree() {
    let a = node(6, &JsValue::from(create_memory_storage()));
    let b = node(7, &JsValue::from(create_memory_storage()));
    b.store_commit(&id(), &CommitId::new([7; 32]), vec![], Blob::new(vec![7]))
        .await
        .unwrap();
    Node::link(&a, &b).await.unwrap();
    let iterator: JsValue = a.load_and_watch(&id(), None).unwrap().into();
    assert_eq!(kind(&event(&iterator).await), "storageChecked");
    assert_eq!(kind(&event(&iterator).await), "commit");
    assert_eq!(kind(&event(&iterator).await), "peerSynced");
    let other_id = SedimentreeId::new([34; 32]);
    b.store_commit(
        &other_id,
        &CommitId::new([8; 32]),
        vec![],
        Blob::new(vec![8]),
    )
    .await
    .unwrap();
    assert_eq!(
        a.fetch_blobs(&other_id, Some(2_000))
            .await
            .unwrap()
            .unwrap()
            .len(),
        1
    );
    close(&a).await;
    close(&b).await;
}
