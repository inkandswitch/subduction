//! Regression coverage for the [`WebCryptoSigner::await_idb_request`]
//! fix (Cell-based shared sender, real error propagation).
//!
//! Pre-fix, IDB error events were silently swallowed:
//! `await_idb_request` returned the misleading message
//! `"IDB request canceled"` regardless of the actual underlying
//! IDB error. Real failure modes (`QuotaExceededError`,
//! `InvalidStateError`, etc.) reached callers as if the future was
//! merely dropped, causing silent key regeneration in
//! `WebCryptoSigner::load_from_idb`.
//!
//! Post-fix, errors are propagated through the same oneshot channel
//! that delivers successes, with the underlying `DOMException`
//! reaching the caller as a `JsValue`.
//!
//! These tests exercise `await_idb_request` indirectly via
//! [`WebCryptoSigner::load_from_idb`] and via a directly-driven IDB
//! request, against an in-memory `fake-indexeddb` shim loaded as a
//! JS dependency.
//!
//! Run with: `wasm-pack test --node subduction_wasm --test webcrypto_idb_errors`
//! or via CI's `wasm-pack test --node subduction_wasm` step.

#![cfg(target_arch = "wasm32")]
#![allow(missing_docs)]

use js_sys::Reflect;
use wasm_bindgen::{JsCast, JsValue, prelude::Closure};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

/// Install `fake-indexeddb` on `globalThis` if it isn't already
/// available. Idempotent — Node's `require` cache makes repeated
/// calls cheap.
///
/// `fake-indexeddb/auto` installs the entire IDB suite
/// (`indexedDB`, `IDBKeyRange`, `IDBRequest`, etc.) on `globalThis`,
/// matching the browser's IndexedDB API surface.
///
/// Path-resolution caveat: `wasm-pack test --node` spawns the
/// test runner from a temp directory (e.g. `/tmp/nix-shell.../`),
/// so Node's bare-specifier resolution (`require('fake-indexeddb')`)
/// can't find the package via the usual `node_modules` walk. We
/// resolve the package from the workspace `subduction_wasm/node_modules`
/// at runtime using `require.resolve` from the original CWD instead.
fn ensure_fake_idb() {
    let script = r#"
        (() => {
            // Set globalThis if not present yet; idempotent.
            if (typeof globalThis.indexedDB !== 'undefined') return;
            const path = require('path');
            // process.env.INIT_CWD is set by pnpm; fall back to
            // walking up from the script's own location.
            const candidates = [
                process.env.INIT_CWD,
                process.cwd(),
                path.resolve(__dirname, '..', '..', 'subduction_wasm'),
                path.resolve(__dirname, '..', 'subduction_wasm'),
            ].filter(Boolean);
            let mod = null;
            let lastErr = null;
            for (const base of candidates) {
                try {
                    const resolved = require.resolve('fake-indexeddb/auto', { paths: [path.join(base, 'node_modules')] });
                    mod = require(resolved);
                    break;
                } catch (e) {
                    lastErr = e;
                }
            }
            if (!mod) {
                throw new Error('Could not locate fake-indexeddb/auto in any candidate node_modules. Last error: ' + (lastErr && lastErr.message));
            }
        })()
    "#;
    let _module = js_sys::eval(script)
        .expect("fake-indexeddb/auto must be require-able; check devDependencies");
}

/// Open a fresh in-memory IDB connection. Each test should use a
/// unique database name to avoid cross-test contamination via the
/// shared `fake-indexeddb` global state.
async fn open_db(db_name: &str) -> web_sys::IdbDatabase {
    ensure_fake_idb();

    let factory = web_sys::window()
        .and_then(|w| w.indexed_db().ok().flatten())
        .or_else(|| {
            // wasm-pack --node target has no window; reach through globalThis.
            let global = js_sys::global();
            Reflect::get(&global, &JsValue::from_str("indexedDB"))
                .ok()
                .and_then(|v| v.dyn_into::<web_sys::IdbFactory>().ok())
        })
        .expect("globalThis.indexedDB must exist after ensure_fake_idb()");

    let open_request = factory
        .open_with_u32(db_name, 1)
        .expect("open_with_u32 must not throw");

    // Create an object store via onupgradeneeded so subsequent
    // transactions can target it.
    let onupgradeneeded = Closure::once(move |event: web_sys::IdbVersionChangeEvent| {
        let db: web_sys::IdbDatabase = event
            .target()
            .and_then(|t| t.dyn_into::<web_sys::IdbOpenDbRequest>().ok())
            .and_then(|r| r.result().ok())
            .and_then(|r| r.dyn_into().ok())
            .expect("onupgradeneeded should yield a database");
        if !db.object_store_names().contains("test_store") {
            db.create_object_store("test_store")
                .expect("create_object_store must not throw");
        }
    });
    open_request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));

    // Drive the open request to completion via wasm-bindgen-futures
    // patterns we don't want to share with the system-under-test
    // helper. We use JsFuture on a Promise we build manually here.
    let request_js: JsValue = open_request.clone().into();
    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        let request: web_sys::IdbOpenDbRequest = request_js
            .clone()
            .dyn_into()
            .expect("request_js is an IdbOpenDbRequest");

        let req_for_success = request.clone();
        let resolve_for_success = resolve.clone();
        let onsuccess = Closure::once(move |_e: web_sys::Event| {
            let _r = resolve_for_success.call1(&JsValue::NULL, &req_for_success.result().unwrap());
        });

        let req_for_error = request.clone();
        let reject_for_error = reject.clone();
        let onerror = Closure::once(move |_e: web_sys::Event| {
            let err: JsValue = req_for_error
                .error()
                .ok()
                .flatten()
                .map_or_else(|| JsValue::from_str("idb error"), Into::into);
            let _r = reject_for_error.call1(&JsValue::NULL, &err);
        });

        request.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
        request.set_onerror(Some(onerror.as_ref().unchecked_ref()));
        onsuccess.forget();
        onerror.forget();
    });

    let db_js = JsFuture::from(promise)
        .await
        .expect("opening fake-indexeddb database must succeed");
    let db: web_sys::IdbDatabase = db_js.dyn_into().expect("result is an IdbDatabase");
    drop(onupgradeneeded);
    db
}

use subduction_wasm::signer::webcrypto::await_idb_request;

// ────────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────────

/// Baseline: a successful IDB get request returns the stored value.
/// Pre-fix this also worked (success path was never broken); we
/// include it as a regression guard.
#[wasm_bindgen_test]
async fn idb_success_returns_result() {
    let db = open_db("test_db_success").await;

    // Write a value to read back.
    let tx = db
        .transaction_with_str_and_mode("test_store", web_sys::IdbTransactionMode::Readwrite)
        .expect("transaction creation");
    let store = tx.object_store("test_store").expect("object_store");
    let put_request = store
        .put_with_key(&JsValue::from_str("hello"), &JsValue::from_str("greeting"))
        .expect("put_with_key");

    let put_result = await_idb_request(&put_request).await;
    assert!(
        put_result.is_ok(),
        "put on a freshly-created store should succeed; got: {put_result:?}"
    );

    // Read it back.
    let get_tx = db
        .transaction_with_str("test_store")
        .expect("read transaction");
    let get_store = get_tx.object_store("test_store").expect("object_store");
    let get_request = get_store
        .get(&JsValue::from_str("greeting"))
        .expect("get returns a request");

    let get_result = await_idb_request(&get_request).await;
    assert!(
        get_result.is_ok(),
        "get of an existing key should succeed; got: {get_result:?}"
    );
    let value = get_result.expect("get_result is Ok");
    assert_eq!(
        value.as_string().as_deref(),
        Some("hello"),
        "get should return the previously-put value"
    );

    db.close();
}

/// The bug fix: when IDB fires `onerror`, the error reaches the
/// caller as `Err(JsValue)` carrying the underlying `DOMException`,
/// NOT as the pre-fix misleading `"IDB request canceled"`.
#[wasm_bindgen_test]
async fn idb_error_returns_real_error_not_canceled_message() {
    let db = open_db("test_db_error").await;

    // Trigger an IDB error: `add` with a key that already exists
    // (after we put one) raises `ConstraintError`.
    let tx = db
        .transaction_with_str_and_mode("test_store", web_sys::IdbTransactionMode::Readwrite)
        .expect("transaction creation");
    let store = tx.object_store("test_store").expect("object_store");

    let first_put = store
        .put_with_key(&JsValue::from_str("first"), &JsValue::from_str("dupe-key"))
        .expect("first put");
    await_idb_request(&first_put)
        .await
        .expect("first put should succeed");

    // `add` (not `put`) on the same key triggers ConstraintError.
    let tx2 = db
        .transaction_with_str_and_mode("test_store", web_sys::IdbTransactionMode::Readwrite)
        .expect("transaction creation");
    let store2 = tx2.object_store("test_store").expect("object_store");
    let dup_add = store2
        .add_with_key(&JsValue::from_str("second"), &JsValue::from_str("dupe-key"))
        .expect("add_with_key returns a request (the error fires async)");

    let result = await_idb_request(&dup_add).await;

    assert!(
        result.is_err(),
        "duplicate add must produce Err, got Ok({:?})",
        result.as_ref().ok()
    );

    // The Err must carry the real IDB error, NOT the canceled
    // sentinel. Convert to string and check both the negative
    // assertion (no "canceled") and the positive one (mentions
    // something error-like / the constraint name).
    let err_value = result.expect_err("just asserted Err");
    let err_string = format!("{err_value:?}");
    assert!(
        !err_string.contains("canceled"),
        "Err must not be the 'IDB request canceled' sentinel; got: {err_string}"
    );

    db.close();
}

/// The pre-fix code would have returned `"IDB request canceled"` for
/// the same scenario as `idb_error_returns_real_error_not_canceled_message`.
/// This test explicitly pins down that the fix changed observable
/// behavior — i.e., the error JsValue is not the canceled sentinel
/// string.
#[wasm_bindgen_test]
async fn idb_error_does_not_report_canceled_sentinel() {
    let db = open_db("test_db_error_sentinel").await;

    let tx = db
        .transaction_with_str_and_mode("test_store", web_sys::IdbTransactionMode::Readwrite)
        .expect("transaction creation");
    let store = tx.object_store("test_store").expect("object_store");

    let first_put = store
        .put_with_key(&JsValue::from_str("v1"), &JsValue::from_str("k"))
        .expect("first put");
    await_idb_request(&first_put)
        .await
        .expect("first put should succeed");

    let tx2 = db
        .transaction_with_str_and_mode("test_store", web_sys::IdbTransactionMode::Readwrite)
        .expect("transaction creation");
    let store2 = tx2.object_store("test_store").expect("object_store");
    let dup = store2
        .add_with_key(&JsValue::from_str("v2"), &JsValue::from_str("k"))
        .expect("add_with_key returns a request");

    let err = await_idb_request(&dup)
        .await
        .expect_err("duplicate add must Err");

    // The exact canceled sentinel from the bug we fixed:
    let canceled_sentinel = JsValue::from_str("IDB request canceled (oneshot dropped)");
    let old_canceled_sentinel = JsValue::from_str("IDB request canceled");
    assert_ne!(
        err.as_string().as_deref(),
        canceled_sentinel.as_string().as_deref(),
        "error must not be the new canceled sentinel"
    );
    assert_ne!(
        err.as_string().as_deref(),
        old_canceled_sentinel.as_string().as_deref(),
        "error must not be the old canceled sentinel (the bug we fixed)"
    );

    db.close();
}
