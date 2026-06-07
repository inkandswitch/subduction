//! JavaScript logger integration for capturing tracing events.
//!
//! This module provides a custom `tracing::Layer` that forwards tracing events
//! to JavaScript callbacks, allowing programmatic capture and analysis of logs
//! from Wasm code. It is installed once at module startup by [`crate::init_rich`]
//! and shared by every cdylib bundle.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use core::fmt;
use js_sys::Function;
use std::sync::RwLock;
use tracing::Level;
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};
use wasm_bindgen::prelude::*;

/// Global storage for the JavaScript logger callback.
///
/// Uses `std::sync::RwLock` for interior mutability in a `static`, allowing
/// the callback to be set and cleared while the layer remains active.
/// The layer is initialized once at module startup, and this callback is
/// checked on each tracing event.
static JS_LOGGER: RwLock<Option<Function>> = RwLock::new(None);

/// Set a JavaScript callback to receive all tracing output.
///
/// The callback is invoked with four arguments:
/// - `level`: string — one of `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`
/// - `target`: string — the Rust module path (e.g. `subduction_core::handler::sync`)
/// - `message`: string — the log message
/// - `fields`: object — additional structured fields as key-value pairs
///
/// # Notes
///
/// - The callback runs synchronously on each tracing event.
/// - Heavy processing in the callback may impact performance; collect quickly
///   and process later.
pub fn set_logger(callback: Function) {
    match JS_LOGGER.write() {
        Ok(mut guard) => *guard = Some(callback),
        Err(poisoned) => tracing::error!("JS_LOGGER RwLock poisoned: {poisoned}"),
    }
}

/// Clear the JavaScript logger callback.
///
/// After this, tracing events are no longer forwarded to JavaScript. The layer
/// remains active but becomes a no-op until a new callback is registered.
pub fn clear_logger() {
    match JS_LOGGER.write() {
        Ok(mut guard) => *guard = None,
        Err(poisoned) => tracing::error!("JS_LOGGER RwLock poisoned: {poisoned}"),
    }
}

/// Custom tracing layer that forwards events to JavaScript.
///
/// Installed once at module initialization and active for the lifetime of the
/// Wasm module. It checks for a registered callback on each event and forwards
/// the event only if one is present — so with no callback registered this is a
/// cheap early-out.
pub(crate) struct JsCallbackLayer;

impl<S> Layer<S> for JsCallbackLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let callback_opt = match JS_LOGGER.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => {
                tracing::error!("JS_LOGGER RwLock poisoned: {poisoned}");
                return;
            }
        };

        if let Some(callback) = callback_opt {
            let metadata = event.metadata();
            let level = level_to_str(metadata.level());
            let target = metadata.target();

            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);

            // callback(level, target, message, fields). Ignore JS errors — a
            // broken callback must not crash Rust.
            let this = JsValue::NULL;
            let level_js = JsValue::from_str(level);
            let target_js = JsValue::from_str(target);
            let message_js = JsValue::from_str(&visitor.message);
            let fields_js = visitor.to_js_object();

            drop(callback.call4(&this, &level_js, &target_js, &message_js, &fields_js));
        }
    }
}

/// Convert a tracing level to its string representation.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn level_to_str(level: &Level) -> &'static str {
    match *level {
        Level::TRACE => "trace",
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
    }
}

/// Visitor to extract the message and fields from a tracing event.
#[derive(Default)]
struct FieldVisitor {
    message: String,
    fields: Vec<(String, String)>,
}

impl FieldVisitor {
    /// Convert collected fields to a JavaScript object.
    fn to_js_object(&self) -> JsValue {
        let obj = js_sys::Object::new();
        for (key, value) in &self.fields {
            drop(js_sys::Reflect::set(
                &obj,
                &JsValue::from_str(key),
                &JsValue::from_str(value),
            ));
        }
        obj.into()
    }
}

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        let value_str = alloc::format!("{value:?}");

        if field.name() == "message" {
            self.message = value_str.trim_matches('"').to_string();
        } else {
            self.fields.push((field.name().to_string(), value_str));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() != "message" {
            self.fields
                .push((field.name().to_string(), alloc::format!("{value}")));
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if field.name() != "message" {
            self.fields
                .push((field.name().to_string(), alloc::format!("{value}")));
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if field.name() != "message" {
            self.fields
                .push((field.name().to_string(), alloc::format!("{value}")));
        }
    }
}
