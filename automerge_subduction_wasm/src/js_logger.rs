//! JavaScript logger integration for capturing tracing events.
//!
//! This module provides a custom `tracing::Layer` that forwards tracing events
//! to JavaScript callbacks, allowing programmatic capture and analysis of logs
//! from WASM code.

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;
use js_sys::Function;
use parking_lot::RwLock;
use tracing::Level;
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};
use wasm_bindgen::prelude::*;

/// Global storage for the JavaScript logger callback.
///
/// Uses `RwLock` for interior mutability, allowing the callback to be set
/// and cleared while the layer remains active. The layer is initialized once
/// at module startup, and this callback is checked on each tracing event.
static JS_LOGGER: RwLock<Option<Function>> = RwLock::new(None);

/// Sets a JavaScript callback to receive all tracing output.
///
/// The callback will be invoked with four arguments:
/// - `level`: string - One of "trace", "debug", "info", "warn", "error"
/// - `target`: string - The Rust module path (e.g., "automerge_subduction::protocol")
/// - `message`: string - The log message
/// - `fields`: object - Additional structured fields as key-value pairs
///
/// # Example (JavaScript)
///
/// ```javascript
/// import { set_subduction_logger } from "@automerge/automerge-subduction"
///
/// set_subduction_logger((level, target, message, fields) => {
///   console.log(`[${level}] ${target}: ${message}`, fields)
/// })
/// ```
///
/// # Notes
///
/// - The callback runs synchronously on each tracing event
/// - Heavy processing in the callback may impact performance
/// - For async processing, collect logs quickly and process them later
#[wasm_bindgen]
pub fn set_subduction_logger(callback: Function) {
    *JS_LOGGER.write() = Some(callback);
}

/// Clears the JavaScript logger callback.
///
/// After calling this, tracing events will no longer be forwarded to JavaScript.
/// The tracing layer remains active but becomes a no-op until a new callback
/// is registered.
#[wasm_bindgen]
pub fn clear_subduction_logger() {
    *JS_LOGGER.write() = None;
}

/// Custom tracing layer that forwards events to JavaScript.
///
/// This layer is installed once at module initialization and remains active
/// throughout the lifetime of the WASM module. It checks for a registered
/// callback on each event and forwards the event if one is present.
pub(crate) struct JsCallbackLayer;

impl<S> Layer<S> for JsCallbackLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        // Check if a JavaScript callback is registered
        let callback_opt = JS_LOGGER.read().clone();

        if let Some(callback) = callback_opt {
            let metadata = event.metadata();

            // Convert tracing level to string
            let level = level_to_str(metadata.level());

            // Get the target (Rust module path)
            let target = metadata.target();

            // Extract message and fields using a visitor
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);

            // Call the JavaScript function
            // Signature: callback(level, target, message, fields)
            let this = JsValue::NULL;
            let level_js = JsValue::from_str(level);
            let target_js = JsValue::from_str(target);
            let message_js = JsValue::from_str(&visitor.message);
            let fields_js = visitor.to_js_object();

            // Ignore any errors from calling JavaScript - we don't want
            // a broken callback to crash the Rust code
            drop(callback.call4(&this, &level_js, &target_js, &message_js, &fields_js));
        }
    }
}

/// Converts a tracing level to its string representation.
fn level_to_str(level: &Level) -> &'static str {
    match *level {
        Level::TRACE => "trace",
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
    }
}

/// Visitor to extract fields from tracing events.
///
/// Tracing events contain structured fields beyond just the message.
/// This visitor collects the message field separately and puts all
/// other fields into a vector for conversion to a JavaScript object.
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

        // The "message" field gets special handling
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
            self.fields.push((field.name().to_string(), value.to_string()));
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
