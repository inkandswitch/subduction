#![allow(clippy::unwrap_used)]
#![allow(clippy::float_cmp)] // JS numbers here are exact integer counters.
use super::*;
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(inline_js = r#"
export function transport(mode) {
  return {
    calls: 0,
    async sendBytes(_) {}, async recvBytes() { return new Promise(() => {}); },
    disconnect() {
      this.calls++;
      if (mode === 'throw') throw new Error('cleanup failed');
      if (mode === 'reject') return Promise.reject(new Error('cleanup failed'));
      if (mode === 'hang') return new Promise(() => {});
      return Promise.resolve();
    },
    onDisconnect(_) {},
  };
}
"#)]
extern "C" {
    fn transport(mode: &str) -> JsTransport;
}
fn calls(transport: &JsTransport) -> f64 {
    js_sys::Reflect::get(transport.as_ref(), &"calls".into())
        .unwrap()
        .as_f64()
        .unwrap()
}
struct Dropped(Rc<Cell<bool>>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[wasm_bindgen_test]
async fn close_deadline_aborts_and_joins_tasks_and_has_a_cached_error() {
    let runtime = Runtime::default();
    let dropped = Rc::new(Cell::new(false));
    let guard = Dropped(dropped.clone());
    runtime.spawner().spawn(
        async move {
            let _guard = guard;
            futures::future::pending::<()>().await;
        }
        .boxed_local(),
    );
    runtime.close(|| {}, async {}.boxed_local(), Duration::from_millis(1));
    let first = runtime.closed().await.unwrap_err();
    assert!(dropped.get());
    assert!(runtime.0.spawner.tasks().handles.is_empty());
    assert_eq!(runtime.closed().await.unwrap_err(), first);
    // No new descendant task can escape after forced shutdown.
    let dropped = Rc::new(Cell::new(false));
    let guard = Dropped(dropped.clone());
    runtime.spawner().spawn(
        async move {
            let _guard = guard;
        }
        .boxed_local(),
    );
    assert!(dropped.get());
}

#[wasm_bindgen_test]
async fn graceful_close_drains_descendants_spawned_during_shutdown() {
    let runtime = Runtime::default();
    let completed = Rc::new(Cell::new(false));
    let done = completed.clone();
    let (send, recv) = futures::channel::oneshot::channel::<()>();
    let spawner = runtime.spawner();
    runtime.spawner().spawn(
        async move {
            spawner.spawn(
                async move {
                    recv.await.unwrap();
                    done.set(true);
                }
                .boxed_local(),
            );
        }
        .boxed_local(),
    );
    runtime.close(|| {}, async {}.boxed_local(), Duration::from_secs(1));
    send.send(()).unwrap();
    runtime.closed().await.unwrap();
    assert!(completed.get());
    assert!(runtime.0.spawner.tasks().handles.is_empty());
}

#[wasm_bindgen_test]
async fn every_transport_is_disconnected_even_when_one_throws_or_rejects() {
    let runtime = Runtime::default();
    let a = transport("throw");
    let b = transport("reject");
    let c = transport("ok");
    for t in [&a, &b, &c] {
        runtime.own(t.clone()).disarm_cleanup();
    }
    runtime.close(|| {}, async {}.boxed_local(), Duration::from_secs(1));
    assert!(runtime.closed().await.is_err());
    assert!(runtime.0.transports.borrow().is_empty());
    for t in [&a, &b, &c] {
        assert_eq!(calls(t), 1.0);
    }
}

#[wasm_bindgen_test]
async fn dropping_a_duplicate_lease_leaves_the_original_guard_armed() {
    let runtime = Runtime::default();
    let t = transport("ok");
    let original = runtime.own(t.clone());
    let managed = original.transport();
    drop(runtime.own(t.clone()));
    assert!(managed.0.disconnect.borrow().is_none());
    // Only the introducing attempt owns rollback, even before registration.
    drop(original);
    managed.disconnect().await.unwrap();
    assert_eq!(calls(&t), 1.0);
    runtime.close(|| {}, async {}.boxed_local(), Duration::from_secs(1));
    runtime.closed().await.unwrap();
}

#[wasm_bindgen_test]
async fn disconnect_is_shared_with_close_and_does_not_run_twice() {
    let runtime = Runtime::default();
    let t = transport("ok");
    let lease = runtime.own(t.clone());
    let managed = lease.transport();
    lease.disarm_cleanup();
    managed.on_unregistered();
    runtime.close(|| {}, async {}.boxed_local(), Duration::from_secs(1));
    managed.disconnect().await.unwrap();
    runtime.closed().await.unwrap();
    assert_eq!(calls(&t), 1.0);
}

#[wasm_bindgen_test]
async fn hanging_disconnect_is_bounded_and_other_transports_still_close() {
    let runtime = Runtime::default();
    let hung = transport("hang");
    let ok = transport("ok");
    runtime.own(hung.clone()).disarm_cleanup();
    runtime.own(ok.clone()).disarm_cleanup();
    runtime.close(|| {}, async {}.boxed_local(), Duration::from_millis(1));
    let error = runtime.closed().await.unwrap_err();
    assert_eq!(
        error.unchecked_ref::<js_sys::Error>().name(),
        "SubductionCloseError"
    );
    assert!(runtime.0.transports.borrow().is_empty());
    assert!(runtime.0.spawner.tasks().handles.is_empty());
    assert_eq!(calls(&hung), 1.0);
    assert_eq!(calls(&ok), 1.0);
}
