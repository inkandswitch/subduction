//! Shared disconnect callback for Wasm transports.

use alloc::rc::Rc;
use core::cell::RefCell;

use wasm_bindgen::{JsCast, JsValue, closure::Closure};

/// Shared, optional disconnect callback.
///
/// Stored as a [`Closure`] so it is properly dropped (not leaked) when the
/// connection is cleaned up or a new callback is registered.
#[derive(Debug, Default, Clone)]
pub(crate) struct OnDisconnect(
    #[allow(clippy::type_complexity)] Rc<RefCell<Option<Closure<dyn Fn()>>>>,
);

impl OnDisconnect {
    /// Store a disconnect callback.
    pub(crate) fn set(&self, closure: Closure<dyn Fn()>) {
        *self.0.borrow_mut() = Some(closure);
    }

    /// Take and invoke the callback (if any).
    ///
    /// The closure is consumed so it cannot fire twice, and the borrow is
    /// released before calling into JS to avoid re-entrant `RefCell` panics.
    pub(crate) fn take_and_fire(&self) {
        let closure = self.0.borrow_mut().take();
        if let Some(closure) = closure {
            let func: &js_sys::Function = closure.as_ref().unchecked_ref();
            if let Err(e) = func.call0(&JsValue::NULL) {
                tracing::error!("onDisconnect callback threw: {e:?}");
            }
        }
    }
}
