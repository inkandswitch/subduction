//! Shared, fallible conversions for the plain-value JavaScript boundary.
//!
//! Inputs are converted directly into owned core values, without intermediate
//! Wasm wrappers. No reference to application data survives conversion.
//!
//! # Errors
//! Every fallible conversion returns a path-qualified JS `TypeError` for invalid
//! input, or propagates the exception from a property getter.
#![allow(clippy::missing_errors_doc)] // Shared conversion error contract above.

use alloc::{
    collections::BTreeSet,
    format,
    string::{String, ToString},
    vec::Vec,
};
use js_sys::{Array, Object, Reflect, Uint8Array};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    crypto::digest::Digest,
    fragment::{Fragment, checkpoint::Checkpoint},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const TYPES: &str = include_str!("pojo.d.ts");

/// Construct a path-qualified validation error.
#[must_use]
pub fn invalid(path: &str, message: &str) -> JsValue {
    js_sys::TypeError::new(&format!("{path}: {message}")).into()
}

/// Read a property, preserving exceptions thrown by getters/proxies.
pub fn field(value: &JsValue, name: &str) -> Result<JsValue, JsValue> {
    Reflect::get(value, &name.into())
}

/// Validate a record before reading its properties.
pub fn record(value: &JsValue, path: &str) -> Result<(), JsValue> {
    if value.is_object() && !value.is_null() && !is_array(value)? {
        Ok(())
    } else {
        Err(invalid(path, "expected an object"))
    }
}

#[wasm_bindgen]
extern "C" {
    // Array.isArray itself can throw for a revoked proxy.
    #[wasm_bindgen(catch, js_namespace = Array, js_name = isArray)]
    fn is_array(value: &JsValue) -> Result<bool, JsValue>;
}

/// Read an actual array without invoking unchecked index getters. Only JS
/// references are collected here; each element is then converted into core data.
pub fn array(value: &JsValue, path: &str) -> Result<Vec<JsValue>, JsValue> {
    if !is_array(value)? {
        return Err(invalid(path, "expected an array"));
    }
    let length = u32_value(&field(value, "length")?, &format!("{path}.length"))?;
    (0..length)
        .map(|i| Reflect::get(value, &i.into()))
        .collect()
}

/// Decode a fixed-length, unprefixed hex string. Input case is insignificant.
pub fn unhex<const N: usize>(value: &JsValue, path: &str) -> Result<[u8; N], JsValue> {
    let error = || invalid(path, &format!("expected {} hex characters", N * 2));
    let text = value.as_string().ok_or_else(error)?;
    if text.len() != N * 2 {
        return Err(error());
    }
    let mut bytes = [0; N];
    for (out, &[high, low]) in bytes.iter_mut().zip(text.as_bytes().as_chunks::<2>().0) {
        let nibble = |c: u8| match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        };
        *out = (nibble(high).ok_or_else(error)? << 4) | nibble(low).ok_or_else(error)?;
    }
    Ok(bytes)
}

/// Encode canonical lowercase hex.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    let digit = |n: u8| char::from(if n < 10 { b'0' + n } else { b'a' + n - 10 });
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(digit(byte >> 4));
        text.push(digit(byte & 15));
    }
    text
}

/// Read a tree identifier.
pub fn tree(value: &JsValue, path: &str) -> Result<SedimentreeId, JsValue> {
    Ok(SedimentreeId::new(unhex(value, path)?))
}

/// Read a commit identifier.
pub fn commit_id(value: &JsValue, path: &str) -> Result<CommitId, JsValue> {
    Ok(CommitId::new(unhex(value, path)?))
}

#[wasm_bindgen(inline_js = r#"
// wasm-bindgen's ordinary Uint8Array conversion does not enforce this API's
// ownership/validation contract: accept real Uint8Arrays (including foreign-realm
// arrays and Node Buffer slices), not coercible array-likes or overridden buffer/
// offset getters. Return a local view; Rust immediately copies it into owned memory
// before scheduling any work. Shared memory cannot give us that stable snapshot:
// another agent could modify it during the copy, so reject it rather than race.
export function copyPojoBytes(value, path) {
    // These accessors inspect internal slots, not instanceof/prototype identity.
    const proto = Object.getPrototypeOf(Uint8Array.prototype);
    const tag = Object.getOwnPropertyDescriptor(proto, Symbol.toStringTag).get;
    if (tag.call(value) !== 'Uint8Array') throw new TypeError(`${path}: expected Uint8Array`);
    const buffer = Object.getOwnPropertyDescriptor(proto, 'buffer').get.call(value);
    // The ArrayBuffer byteLength getter rejects SharedArrayBuffer (including cross-realm).
    try { Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, 'byteLength').get.call(buffer); }
    catch { throw new TypeError(`${path}: expected ordinary ArrayBuffer backing (shared-memory buffers are not supported)`); }
    // A new local view uses intrinsic offsets, not application overrides.
    const offset = Object.getOwnPropertyDescriptor(proto, 'byteOffset').get.call(value);
    const length = Object.getOwnPropertyDescriptor(proto, 'byteLength').get.call(value);
    return new Uint8Array(buffer, offset, length);
}
"#)]
extern "C" {
    #[wasm_bindgen(catch, js_name = copyPojoBytes)]
    fn byte_view(value: &JsValue, path: &str) -> Result<Uint8Array, JsValue>;
}

/// Copy bytes into Rust at admission, rejecting shared memory and coercions.
pub fn bytes(value: &JsValue, path: &str) -> Result<Vec<u8>, JsValue> {
    Ok(byte_view(value, path)?.to_vec())
}

/// Validate a number without coercion or truncation.
pub fn u32_value(value: &JsValue, path: &str) -> Result<u32, JsValue> {
    let n = value
        .as_f64()
        .ok_or_else(|| invalid(path, "expected a u32 integer"))?;
    if n < 0.0 || n > f64::from(u32::MAX) || n.fract() != 0.0 {
        return Err(invalid(path, "expected a u32 integer"));
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok(n as u32)
}

/// Construct an ordinary object. All keys here are library-owned constants or hex.
#[must_use]
pub fn object(fields: &[(&str, JsValue)]) -> JsValue {
    let result = Object::new();
    for (key, value) in fields {
        // Fresh, extensible object and own data properties: no user code runs.
        let descriptor = Object::new();
        Reflect::set(&descriptor, &"value".into(), value).unwrap_throw();
        Reflect::set(&descriptor, &"enumerable".into(), &JsValue::TRUE).unwrap_throw();
        Reflect::set(&descriptor, &"writable".into(), &JsValue::TRUE).unwrap_throw();
        Reflect::set(&descriptor, &"configurable".into(), &JsValue::TRUE).unwrap_throw();
        Object::define_property(&result, &(*key).into(), &descriptor);
    }
    result.into()
}

/// Convert a sequence of owned values into a JS array.
pub fn values(items: impl IntoIterator<Item = JsValue>) -> JsValue {
    items.into_iter().collect::<Array>().into()
}

/// Read and deduplicate commit identifiers.
pub fn ids(value: &JsValue, path: &str) -> Result<BTreeSet<CommitId>, JsValue> {
    array(value, path)?
        .into_iter()
        .enumerate()
        .map(|(i, value)| commit_id(&value, &format!("{path}[{i}]")))
        .collect()
}

/// Read one unsigned commit and derive its blob metadata.
pub fn commit_input(
    id: SedimentreeId,
    value: &JsValue,
    path: &str,
) -> Result<(LooseCommit, Blob), JsValue> {
    record(value, path)?;
    let head = commit_id(&field(value, "head")?, &format!("{path}.head"))?;
    let parents = ids(&field(value, "parents")?, &format!("{path}.parents"))?;
    let blob = Blob::new(bytes(&field(value, "blob")?, &format!("{path}.blob"))?);
    let commit = LooseCommit::new(id, head, parents, BlobMeta::new(&blob))
        .map_err(|e| invalid(path, &e.to_string()))?;
    Ok((commit, blob))
}

/// Read one unsigned fragment using 12-byte checkpoint prefixes.
pub fn fragment_input(
    id: SedimentreeId,
    value: &JsValue,
    path: &str,
) -> Result<(Fragment, Blob), JsValue> {
    record(value, path)?;
    let head = commit_id(&field(value, "head")?, &format!("{path}.head"))?;
    let boundary = ids(&field(value, "boundary")?, &format!("{path}.boundary"))?;
    let checkpoints = array(
        &field(value, "checkpoints")?,
        &format!("{path}.checkpoints"),
    )?
    .into_iter()
    .enumerate()
    .map(|(i, v)| {
        Ok(Checkpoint::from(unhex::<12>(
            &v,
            &format!("{path}.checkpoints[{i}]"),
        )?))
    })
    .collect::<Result<BTreeSet<_>, JsValue>>()?;
    let blob = Blob::new(bytes(&field(value, "blob")?, &format!("{path}.blob"))?);
    let fragment = Fragment::from_parts(id, head, boundary, checkpoints, BlobMeta::new(&blob))
        .map_err(|e| invalid(path, &e.to_string()))?;
    Ok((fragment, blob))
}

/// Parse blob metadata, retaining the full u64 range without numeric coercion.
fn read_blob_meta(value: &JsValue, path: &str) -> Result<BlobMeta, JsValue> {
    record(value, path)?;
    let digest = digest(&field(value, "digest")?, &format!("{path}.digest"))?;
    let size = field(value, "sizeBytes")?;
    let size = u64::try_from(size).map_err(|_| {
        invalid(
            &format!("{path}.sizeBytes"),
            "expected an unsigned 64-bit bigint",
        )
    })?;
    Ok(BlobMeta::from_digest_size(digest, size))
}

/// Parse a commit metadata record.
pub fn read_commit(value: &JsValue, path: &str) -> Result<LooseCommit, JsValue> {
    record(value, path)?;
    LooseCommit::new(
        tree(
            &field(value, "sedimentreeId")?,
            &format!("{path}.sedimentreeId"),
        )?,
        commit_id(&field(value, "head")?, &format!("{path}.head"))?,
        ids(&field(value, "parents")?, &format!("{path}.parents"))?,
        read_blob_meta(&field(value, "blobMeta")?, &format!("{path}.blobMeta"))?,
    )
    .map_err(|e| invalid(path, &e.to_string()))
}

/// Parse a fragment metadata record.
pub fn read_fragment(value: &JsValue, path: &str) -> Result<Fragment, JsValue> {
    record(value, path)?;
    let checkpoints = array(
        &field(value, "checkpoints")?,
        &format!("{path}.checkpoints"),
    )?
    .into_iter()
    .enumerate()
    .map(|(i, v)| {
        Ok(Checkpoint::from(unhex::<12>(
            &v,
            &format!("{path}.checkpoints[{i}]"),
        )?))
    })
    .collect::<Result<BTreeSet<_>, JsValue>>()?;
    Fragment::from_parts(
        tree(
            &field(value, "sedimentreeId")?,
            &format!("{path}.sedimentreeId"),
        )?,
        commit_id(&field(value, "head")?, &format!("{path}.head"))?,
        ids(&field(value, "boundary")?, &format!("{path}.boundary"))?,
        checkpoints,
        read_blob_meta(&field(value, "blobMeta")?, &format!("{path}.blobMeta"))?,
    )
    .map_err(|e| invalid(path, &e.to_string()))
}

/// Parse and check a built commit before signing or storage.
pub fn built_commit_input(
    id: SedimentreeId,
    value: &JsValue,
    path: &str,
) -> Result<(LooseCommit, Blob), JsValue> {
    record(value, path)?;
    let metadata = read_commit(&field(value, "metadata")?, &format!("{path}.metadata"))?;
    let blob = Blob::new(bytes(&field(value, "blob")?, &format!("{path}.blob"))?);
    if metadata.sedimentree_id() != id {
        return Err(invalid(path, "metadata belongs to a different sedimentree"));
    }
    if *metadata.blob_meta() != BlobMeta::new(&blob) {
        return Err(invalid(path, "blob does not match metadata"));
    }
    Ok((metadata, blob))
}

/// Parse and check a built fragment before signing or storage.
pub fn built_fragment_input(
    id: SedimentreeId,
    value: &JsValue,
    path: &str,
) -> Result<(Fragment, Blob), JsValue> {
    record(value, path)?;
    let metadata = read_fragment(&field(value, "metadata")?, &format!("{path}.metadata"))?;
    let blob = Blob::new(bytes(&field(value, "blob")?, &format!("{path}.blob"))?);
    if metadata.sedimentree_id() != id {
        return Err(invalid(path, "metadata belongs to a different sedimentree"));
    }
    if metadata.summary().blob_meta() != BlobMeta::new(&blob) {
        return Err(invalid(path, "blob does not match metadata"));
    }
    Ok((metadata, blob))
}

/// Plain blob metadata, with an exact unsigned 64-bit size.
#[must_use]
fn blob_meta(meta: &BlobMeta) -> JsValue {
    object(&[
        ("digest", hex(meta.digest().as_bytes()).into()),
        ("sizeBytes", meta.size_bytes().into()),
    ])
}

/// Plain unsigned commit metadata.
#[must_use]
pub fn commit_metadata(commit: &LooseCommit) -> JsValue {
    object(&[
        (
            "sedimentreeId",
            hex(commit.sedimentree_id().as_bytes()).into(),
        ),
        ("head", hex(commit.head().as_bytes()).into()),
        (
            "parents",
            values(commit.parents().iter().map(|id| hex(id.as_bytes()).into())),
        ),
        ("blobMeta", blob_meta(commit.blob_meta())),
    ])
}

/// Plain unsigned fragment metadata.
#[must_use]
pub fn fragment_metadata(fragment: &Fragment) -> JsValue {
    object(&[
        (
            "sedimentreeId",
            hex(fragment.sedimentree_id().as_bytes()).into(),
        ),
        ("head", hex(fragment.head().as_bytes()).into()),
        (
            "boundary",
            values(
                fragment
                    .boundary()
                    .iter()
                    .map(|id| hex(id.as_bytes()).into()),
            ),
        ),
        (
            "checkpoints",
            values(
                fragment
                    .checkpoints()
                    .iter()
                    .map(|cp| hex(cp.as_bytes()).into()),
            ),
        ),
        ("blobMeta", blob_meta(&fragment.summary().blob_meta())),
    ])
}

/// Decode a digest into the core type.
pub fn digest<T>(value: &JsValue, path: &str) -> Result<Digest<T>, JsValue> {
    Ok(Digest::force_from_bytes(unhex(value, path)?))
}
