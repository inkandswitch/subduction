//! JavaScript async iteration over the core coordinator; no exported Rust borrow
//! survives a Promise wait. Lifecycle and validity: see
//! [the shared contract](../../SYNC_COMPLETION.md).

use crate::{peer_id::WasmPeerId, runtime::Runtime};
use alloc::{rc::Rc, string::ToString};
use async_lock::Mutex;
use core::cell::Cell;
use futures::{StreamExt, future::AbortHandle};
use js_sys::{Object, Promise, Reflect, Uint8Array};
use sedimentree_wasm::{fragment::WasmFragment, loose_commit::WasmLooseCommit};
use subduction_core::stored::{LoadError, StoredEvent, StoredItem, StoredUpdates};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const TYPES: &str = r#"
export type StoredUpdate =
    | { type: "commit"; metadata: LooseCommit; blob: Uint8Array }
    | { type: "fragment"; metadata: Fragment; blob: Uint8Array }
    | { type: "storageChecked" }
    | { type: "peerSynced"; peerId: PeerId }
    | { type: "rescanRequired" };

export interface LoadAndWatchOptions {
    /** Cancel observation and outstanding sync attempts. Pending reads reject with signal.reason. */
    signal?: AbortSignal;
}

export interface StoredUpdates extends AsyncIterableIterator<StoredUpdate> {
    /** Cancel observation. Started storage writes are not rolled back. */
    close(): void;
}
"#;

#[wasm_bindgen(inline_js = r#"
export function storedSignal(options) {
    const signal = options?.signal;
    if (signal?.aborted) throw signal.reason;
    return signal;
}
export function storedIterator(driver, signal) {
    let aborted = false;
    let reason;
    let finished = false;
    const cleanup = () => signal?.removeEventListener('abort', onAbort);
    const onAbort = () => {
        aborted = true;
        reason = signal.reason;
        cleanup();
        driver.close();
    };
    const close = () => {
        finished = true;
        cleanup();
        driver.close();
    };
    try {
        signal?.addEventListener('abort', onAbort, { once: true });
        // Also handle a signal aborted between admission and registration.
        if (signal?.aborted) onAbort();
    } catch (error) {
        driver.close();
        throw error;
    }
    return {
        async next() {
            if (aborted) throw reason;
            if (finished) return { done: true };
            try {
                const result = await driver.next();
                if (aborted) throw reason;
                if (result.done) { finished = true; cleanup(); }
                return result;
            } catch (error) {
                finished = true;
                cleanup();
                if (aborted) throw reason;
                throw error;
            }
        },
        return() { close(); return Promise.resolve({ done: true }); },
        close,
        [Symbol.asyncIterator]() { return this; }
    };
}
"#)]
extern "C" {
    /// An async iterator of stored items and ordered completion markers.
    #[wasm_bindgen(typescript_type = "StoredUpdates")]
    pub type JsStoredUpdates;
    /// Options for caller-controlled cancellation of `loadAndWatch`.
    #[wasm_bindgen(typescript_type = "LoadAndWatchOptions")]
    pub type JsLoadAndWatchOptions;
    #[wasm_bindgen(catch, js_name = storedSignal)]
    pub(crate) fn signal(options: Option<JsLoadAndWatchOptions>) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(catch, js_name = storedIterator)]
    fn stored_iterator(
        driver: StoredUpdatesDriver,
        signal: JsValue,
    ) -> Result<JsStoredUpdates, JsValue>;
}

pub(crate) fn wrap(
    stream: StoredUpdates,
    runtime: Runtime,
    signal: JsValue,
) -> Result<JsStoredUpdates, JsValue> {
    let abort = stream.abort_handle();
    stored_iterator(
        StoredUpdatesDriver {
            stream: Rc::new(Mutex::new(stream)),
            runtime,
            abort,
            closed: Rc::new(Cell::new(false)),
        },
        signal,
    )
}

/// Implementation detail of `watchStored` and `loadAndWatch`. Use the returned
/// iterator's `close()` or `for await` cancellation rather than this driver.
#[wasm_bindgen]
pub struct StoredUpdatesDriver {
    stream: Rc<Mutex<StoredUpdates>>,
    runtime: Runtime,
    abort: AbortHandle,
    closed: Rc<Cell<bool>>,
}
impl core::fmt::Debug for StoredUpdatesDriver {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StoredUpdatesDriver")
            .field("closed", &self.closed.get())
            .finish_non_exhaustive()
    }
}
#[wasm_bindgen]
impl StoredUpdatesDriver {
    /// Serialize concurrent reads without retaining an exported-object borrow.
    pub fn next(&self) -> Promise {
        let stream = self.stream.clone();
        let runtime = self.runtime.clone();
        let closed = self.closed.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            if closed.get() {
                return iterator_result(None);
            }
            runtime
                .run(async {
                    let mut stream = stream.lock().await;
                    let next = stream.next().await;
                    if closed.get() {
                        stream.close();
                        return iterator_result(None);
                    }
                    let result = match next {
                        Some(Ok(event)) => iterator_result(Some(event)),
                        Some(Err(error)) => Err(load_error(&error)),
                        None => iterator_result(None),
                    };
                    // Conversion may reenter close through JavaScript accessors.
                    if closed.get() {
                        stream.close();
                        iterator_result(None)
                    } else {
                        result
                    }
                })
                .await
        })
    }

    /// Cancel observation, including a read currently waiting for data.
    pub fn close(&self) {
        self.closed.set(true);
        self.abort.abort();
        if let Some(mut stream) = self.stream.try_lock() {
            stream.close();
        }
        // A pending read releases the lock after abort wakes it; it then clears
        // the stream before returning done. Queued reads observe the same flag.
    }
}
impl Drop for StoredUpdatesDriver {
    fn drop(&mut self) {
        self.close();
    }
}

fn load_error(error: &LoadError) -> JsValue {
    let name = match error {
        LoadError::Unavailable => "DocumentUnavailableError",
        LoadError::TimedOut => "LoadTimeoutError",
        LoadError::Closed => "SubductionClosedError",
        LoadError::Operation(_) => "StoredDataError",
    };
    let js = js_sys::Error::new(&error.to_string());
    js.set_name(name);
    js.into()
}
fn set(object: &Object, key: &str, value: &JsValue) -> Result<(), JsValue> {
    Reflect::set(object, &key.into(), value).map(|_| ())
}
fn iterator_result(event: Option<StoredEvent>) -> Result<JsValue, JsValue> {
    let result = Object::new();
    set(&result, "done", &JsValue::from_bool(event.is_none()))?;
    if let Some(event) = event {
        let value = Object::new();
        let kind = match event {
            StoredEvent::Item(StoredItem::Commit(item)) => {
                set(
                    &value,
                    "metadata",
                    &JsValue::from(WasmLooseCommit::from(item.payload().clone())),
                )?;
                set(&value, "blob", &Uint8Array::from(item.blob().as_slice()))?;
                "commit"
            }
            StoredEvent::Item(StoredItem::Fragment(item)) => {
                set(
                    &value,
                    "metadata",
                    &JsValue::from(WasmFragment::from(item.payload().clone())),
                )?;
                set(&value, "blob", &Uint8Array::from(item.blob().as_slice()))?;
                "fragment"
            }
            StoredEvent::StorageChecked => "storageChecked",
            StoredEvent::PeerSynced { peer_id } => {
                set(&value, "peerId", &JsValue::from(WasmPeerId::from(peer_id)))?;
                "peerSynced"
            }
            StoredEvent::RescanRequired => "rescanRequired",
        };
        set(&value, "type", &kind.into())?;
        set(&result, "value", &value)?;
    }
    Ok(result.into())
}
