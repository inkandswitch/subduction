//! atproto (Bluesky) **service-auth** verification for admission control.
//!
//! A client authenticates by presenting an inter-service auth JWT minted with
//! `com.atproto.server.getServiceAuth`: `iss` is the caller's DID, `aud` is our
//! service DID, and it is signed by the caller's atproto signing key. We verify
//! it **offline** by resolving the issuer DID to its signing key and checking
//! the signature — no OAuth server, no session state.
//!
//! The security-critical logic (JWT decode, claim checks, multikey decode,
//! signature verification) is pure and **host-tested**. Only DID resolution
//! (network I/O via the Workers runtime) is `wasm32`-gated.
//!
//! atproto repo signing keys are `p256` (JOSE `ES256`) or `k256` (`ES256K`);
//! `ed25519` (`EdDSA`) is also accepted for completeness with other DID methods.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::Deserialize;

/// Default allowed clock skew when checking `exp`/`nbf`, in seconds.
pub const DEFAULT_SKEW_SECS: i64 = 60;

/// Multicodec prefixes for the public-key types atproto uses (varint-encoded in
/// a `Multikey`/`did:key`).
const MULTICODEC_P256_PUB: u64 = 0x1200;
const MULTICODEC_K256_PUB: u64 = 0xe7;
const MULTICODEC_ED25519_PUB: u64 = 0xed;

/// Why an admission attempt was rejected. Kept coarse on purpose: the client
/// only needs "no" plus a category, not a verification oracle.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("missing auth token")]
    Missing,
    #[error("malformed jwt")]
    MalformedJwt,
    #[error("unsupported jwt alg: {0}")]
    UnsupportedAlg(String),
    #[error("token has no expiry")]
    NoExpiry,
    #[error("token expired")]
    Expired,
    #[error("token not yet valid")]
    NotYetValid,
    #[error("wrong audience")]
    WrongAudience,
    #[error("issuer is not a DID")]
    BadIssuer,
    #[error("unsupported DID method")]
    UnsupportedDidMethod,
    #[error("DID document has no atproto signing key")]
    NoSigningKey,
    #[error("malformed public key")]
    MalformedKey,
    #[error("signature verification failed")]
    BadSignature,
    #[error("DID resolution failed: {0}")]
    Resolution(String),
}

/// The JOSE algorithm of the presented JWT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JwtAlg {
    /// ECDSA over P-256 with SHA-256.
    Es256,
    /// ECDSA over secp256k1 with SHA-256.
    Es256k,
    /// Edwards-curve DSA over Ed25519.
    EdDsa,
}

impl JwtAlg {
    fn parse(alg: &str) -> Result<Self, AuthError> {
        match alg {
            "ES256" => Ok(Self::Es256),
            "ES256K" => Ok(Self::Es256k),
            "EdDSA" => Ok(Self::EdDsa),
            other => Err(AuthError::UnsupportedAlg(other.to_string())),
        }
    }
}

/// A decoded public key from a DID document verification method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SigningKey {
    /// Compressed SEC1 P-256 point (33 bytes).
    P256([u8; 33]),
    /// Compressed SEC1 secp256k1 point (33 bytes).
    Secp256k1([u8; 33]),
    /// Ed25519 public key (32 bytes).
    Ed25519([u8; 32]),
}

/// A JWT that has passed structural + claim validation but **not** signature
/// verification. Hold the material needed to finish verification once the
/// issuer's key is resolved.
#[derive(Debug, Clone)]
pub struct PrevalidatedToken {
    /// The issuer DID (`iss`) — the authenticated identity if verification passes.
    pub issuer: String,
    /// The `jti` claim, if present (usable for replay dedupe).
    pub jti: Option<String>,
    alg: JwtAlg,
    /// The `header.payload` ASCII bytes that the signature covers.
    signing_input: Vec<u8>,
    /// The raw (JOSE, `r || s` for ECDSA) signature bytes.
    signature: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct JwtHeader {
    alg: String,
}

#[derive(Debug, Deserialize)]
struct ServiceAuthClaims {
    iss: String,
    aud: String,
    exp: Option<i64>,
    nbf: Option<i64>,
    #[serde(default)]
    jti: Option<String>,
}

/// Decode and validate a JWT's structure and claims **without** checking the
/// signature. Verifies audience and time bounds; returns the issuer and the
/// bytes/alg needed for signature verification.
///
/// # Errors
///
/// Returns an [`AuthError`] for a malformed token, unsupported alg, wrong
/// audience, or an expired / not-yet-valid token.
pub fn prevalidate(
    token: &str,
    now_unix: i64,
    expected_aud: &str,
    skew_secs: i64,
) -> Result<PrevalidatedToken, AuthError> {
    if token.is_empty() {
        return Err(AuthError::Missing);
    }
    let mut parts = token.split('.');
    let (Some(h_b64), Some(p_b64), Some(s_b64), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(AuthError::MalformedJwt);
    };

    let header: JwtHeader = decode_json(h_b64)?;
    let alg = JwtAlg::parse(&header.alg)?;
    let claims: ServiceAuthClaims = decode_json(p_b64)?;

    if claims.aud != expected_aud {
        return Err(AuthError::WrongAudience);
    }
    // Service-auth tokens are always short-lived; a missing `exp` means we can't
    // bound replay, so reject rather than trust indefinitely.
    let exp = claims.exp.ok_or(AuthError::NoExpiry)?;
    if now_unix > exp + skew_secs {
        return Err(AuthError::Expired);
    }
    if let Some(nbf) = claims.nbf {
        if now_unix + skew_secs < nbf {
            return Err(AuthError::NotYetValid);
        }
    }
    if !claims.iss.starts_with("did:") {
        return Err(AuthError::BadIssuer);
    }

    let signature = URL_SAFE_NO_PAD
        .decode(s_b64)
        .map_err(|_| AuthError::MalformedJwt)?;
    let signing_input = format!("{h_b64}.{p_b64}").into_bytes();

    Ok(PrevalidatedToken {
        issuer: claims.iss,
        jti: claims.jti,
        alg,
        signing_input,
        signature,
    })
}

/// Verify a prevalidated token's signature against a resolved signing key.
///
/// The JWT `alg` must match the key type; a mismatch is treated as a bad
/// signature rather than a distinct error so it can't be used to probe key types.
///
/// # Errors
///
/// Returns [`AuthError::BadSignature`] if verification fails.
pub fn verify_signature(token: &PrevalidatedToken, key: &SigningKey) -> Result<(), AuthError> {
    let msg = &token.signing_input;
    let sig = &token.signature;
    match (token.alg, key) {
        (JwtAlg::Es256, SigningKey::P256(pk)) => {
            use p256::ecdsa::signature::Verifier as _;
            let vk = p256::ecdsa::VerifyingKey::from_sec1_bytes(pk)
                .map_err(|_| AuthError::MalformedKey)?;
            let signature =
                p256::ecdsa::Signature::from_slice(sig).map_err(|_| AuthError::BadSignature)?;
            vk.verify(msg, &signature)
                .map_err(|_| AuthError::BadSignature)
        }
        (JwtAlg::Es256k, SigningKey::Secp256k1(pk)) => {
            use k256::ecdsa::signature::Verifier as _;
            let vk = k256::ecdsa::VerifyingKey::from_sec1_bytes(pk)
                .map_err(|_| AuthError::MalformedKey)?;
            let signature =
                k256::ecdsa::Signature::from_slice(sig).map_err(|_| AuthError::BadSignature)?;
            vk.verify(msg, &signature)
                .map_err(|_| AuthError::BadSignature)
        }
        (JwtAlg::EdDsa, SigningKey::Ed25519(pk)) => {
            let vk =
                ed25519_dalek::VerifyingKey::from_bytes(pk).map_err(|_| AuthError::MalformedKey)?;
            let signature =
                ed25519_dalek::Signature::from_slice(sig).map_err(|_| AuthError::BadSignature)?;
            vk.verify_strict(msg, &signature)
                .map_err(|_| AuthError::BadSignature)
        }
        // alg / key-type mismatch.
        _ => Err(AuthError::BadSignature),
    }
}

/// Decode a `Multikey` / `did:key` `publicKeyMultibase` string (base58-btc,
/// `z`-prefixed, multicodec-tagged) into a [`SigningKey`].
///
/// # Errors
///
/// Returns [`AuthError::MalformedKey`] for a bad encoding or unsupported key
/// type.
pub fn decode_multikey(multibase: &str) -> Result<SigningKey, AuthError> {
    // Multibase base58-btc is the only encoding atproto emits here.
    let rest = multibase.strip_prefix('z').ok_or(AuthError::MalformedKey)?;
    let bytes = bs58::decode(rest)
        .into_vec()
        .map_err(|_| AuthError::MalformedKey)?;
    let (codec, key) = read_varint(&bytes).ok_or(AuthError::MalformedKey)?;
    match codec {
        MULTICODEC_P256_PUB => Ok(SigningKey::P256(
            key.try_into().map_err(|_| AuthError::MalformedKey)?,
        )),
        MULTICODEC_K256_PUB => Ok(SigningKey::Secp256k1(
            key.try_into().map_err(|_| AuthError::MalformedKey)?,
        )),
        MULTICODEC_ED25519_PUB => Ok(SigningKey::Ed25519(
            key.try_into().map_err(|_| AuthError::MalformedKey)?,
        )),
        _ => Err(AuthError::MalformedKey),
    }
}

/// Read an unsigned LEB128 varint from the front of `bytes`, returning the value
/// and the remaining bytes. Bounded to a `u64` (multicodec prefixes are tiny).
fn read_varint(bytes: &[u8]) -> Option<(u64, &[u8])> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    for (i, &b) in bytes.iter().enumerate() {
        if shift >= 64 {
            return None; // overlong / malformed
        }
        value |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some((value, &bytes[i + 1..]));
        }
        shift += 7;
    }
    None
}

fn decode_json<T: for<'de> Deserialize<'de>>(b64: &str) -> Result<T, AuthError> {
    let raw = URL_SAFE_NO_PAD
        .decode(b64)
        .map_err(|_| AuthError::MalformedJwt)?;
    serde_json::from_slice(&raw).map_err(|_| AuthError::MalformedJwt)
}

// --- DID resolution + end-to-end admission (Workers runtime only) -----------

#[cfg(target_arch = "wasm32")]
mod resolve {
    use super::{decode_multikey, AuthError, SigningKey};
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct DidDocument {
        #[serde(default, rename = "verificationMethod")]
        verification_method: Vec<VerificationMethod>,
    }

    #[derive(Debug, Deserialize)]
    struct VerificationMethod {
        id: String,
        #[serde(default, rename = "publicKeyMultibase")]
        public_key_multibase: Option<String>,
    }

    /// Resolve a DID to its atproto signing key (the `#atproto` verification
    /// method). Supports `did:plc` (via plc.directory) and `did:web`.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Resolution`] on network/parse failure, or
    /// [`AuthError::NoSigningKey`] if the document lacks an `#atproto` key.
    pub async fn resolve_signing_key(did: &str) -> Result<SigningKey, AuthError> {
        let url = did_document_url(did)?;
        let doc: DidDocument = fetch_json(&url).await?;

        let multibase = doc
            .verification_method
            .into_iter()
            .find(|vm| vm.id.ends_with("#atproto"))
            .and_then(|vm| vm.public_key_multibase)
            .ok_or(AuthError::NoSigningKey)?;
        decode_multikey(&multibase)
    }

    fn did_document_url(did: &str) -> Result<String, AuthError> {
        if did.starts_with("did:plc:") {
            // plc.directory serves the resolved DID document at /<did>.
            Ok(format!("https://plc.directory/{did}"))
        } else if let Some(rest) = did.strip_prefix("did:web:") {
            // did:web:<host> -> https://<host>/.well-known/did.json
            // did:web:<host>:<a>:<b> -> https://<host>/<a>/<b>/did.json
            match rest.split_once(':') {
                None => Ok(format!("https://{rest}/.well-known/did.json")),
                Some((host, path)) => Ok(format!(
                    "https://{host}/{}/did.json",
                    path.replace(':', "/")
                )),
            }
        } else {
            Err(AuthError::UnsupportedDidMethod)
        }
    }

    async fn fetch_json<T: for<'de> Deserialize<'de>>(url: &str) -> Result<T, AuthError> {
        let parsed = worker::Url::parse(url).map_err(|e| AuthError::Resolution(e.to_string()))?;
        let mut resp = worker::Fetch::Url(parsed)
            .send()
            .await
            .map_err(|e| AuthError::Resolution(e.to_string()))?;
        if resp.status_code() != 200 {
            return Err(AuthError::Resolution(format!(
                "status {}",
                resp.status_code()
            )));
        }
        let text = resp
            .text()
            .await
            .map_err(|e| AuthError::Resolution(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| AuthError::Resolution(e.to_string()))
    }
}

/// Verify a service-auth token end to end: prevalidate claims, resolve the
/// issuer's signing key, and check the signature. Returns the authenticated
/// issuer DID on success.
///
/// # Errors
///
/// Returns an [`AuthError`] if any step fails.
#[cfg(target_arch = "wasm32")]
pub async fn admit(token: &str, now_unix: i64, service_did: &str) -> Result<String, AuthError> {
    let prevalidated = prevalidate(token, now_unix, service_did, DEFAULT_SKEW_SECS)?;
    let key = resolve::resolve_signing_key(&prevalidated.issuer).await?;
    verify_signature(&prevalidated, &key)?;
    Ok(prevalidated.issuer)
}

#[cfg(test)]
mod tests {
    use super::*;

    const AUD: &str = "did:web:subduct.io";
    const NOW: i64 = 1_000_000;

    fn b64(bytes: &[u8]) -> String {
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// Assemble a signed JWT from header/claims JSON and a signer closure.
    fn make_jwt(alg: &str, claims: &serde_json::Value, sign: impl Fn(&[u8]) -> Vec<u8>) -> String {
        let header = serde_json::json!({ "alg": alg, "typ": "JWT" });
        let h = b64(serde_json::to_string(&header).unwrap().as_bytes());
        let p = b64(serde_json::to_string(claims).unwrap().as_bytes());
        let signing_input = format!("{h}.{p}");
        let sig = sign(signing_input.as_bytes());
        format!("{h}.{p}.{}", b64(&sig))
    }

    fn default_claims() -> serde_json::Value {
        serde_json::json!({
            "iss": "did:plc:example",
            "aud": AUD,
            "exp": NOW + 300,
            "iat": NOW,
        })
    }

    // ---- P-256 (ES256) --------------------------------------------------

    fn p256_keypair() -> (p256::ecdsa::SigningKey, SigningKey) {
        let sk = p256::ecdsa::SigningKey::random(&mut rand_core::OsRng);
        let vk = sk.verifying_key();
        let compressed: [u8; 33] = vk.to_encoded_point(true).as_bytes().try_into().unwrap();
        (sk, SigningKey::P256(compressed))
    }

    #[test]
    fn accepts_valid_es256_token() {
        let (sk, pk) = p256_keypair();
        let jwt = make_jwt("ES256", &default_claims(), |msg| {
            use p256::ecdsa::signature::Signer as _;
            let sig: p256::ecdsa::Signature = sk.sign(msg);
            sig.to_bytes().to_vec()
        });
        let pv = prevalidate(&jwt, NOW, AUD, DEFAULT_SKEW_SECS).expect("prevalidate");
        assert_eq!(pv.issuer, "did:plc:example");
        verify_signature(&pv, &pk).expect("verify");
    }

    // ---- secp256k1 (ES256K) --------------------------------------------

    fn k256_keypair() -> (k256::ecdsa::SigningKey, SigningKey) {
        let sk = k256::ecdsa::SigningKey::random(&mut rand_core::OsRng);
        let vk = sk.verifying_key();
        let compressed: [u8; 33] = vk.to_encoded_point(true).as_bytes().try_into().unwrap();
        (sk, SigningKey::Secp256k1(compressed))
    }

    #[test]
    fn accepts_valid_es256k_token() {
        let (sk, pk) = k256_keypair();
        let jwt = make_jwt("ES256K", &default_claims(), |msg| {
            use k256::ecdsa::signature::Signer as _;
            let sig: k256::ecdsa::Signature = sk.sign(msg);
            sig.to_bytes().to_vec()
        });
        let pv = prevalidate(&jwt, NOW, AUD, DEFAULT_SKEW_SECS).expect("prevalidate");
        verify_signature(&pv, &pk).expect("verify");
    }

    // ---- Ed25519 (EdDSA) ------------------------------------------------

    fn ed25519_keypair() -> (ed25519_dalek::SigningKey, SigningKey) {
        use rand_core::RngCore as _;
        let mut seed = [0u8; 32];
        rand_core::OsRng.fill_bytes(&mut seed);
        let sk = ed25519_dalek::SigningKey::from_bytes(&seed);
        let pk = SigningKey::Ed25519(sk.verifying_key().to_bytes());
        (sk, pk)
    }

    #[test]
    fn accepts_valid_eddsa_token() {
        let (sk, pk) = ed25519_keypair();
        let jwt = make_jwt("EdDSA", &default_claims(), |msg| {
            use ed25519_dalek::Signer as _;
            sk.sign(msg).to_bytes().to_vec()
        });
        let pv = prevalidate(&jwt, NOW, AUD, DEFAULT_SKEW_SECS).expect("prevalidate");
        verify_signature(&pv, &pk).expect("verify");
    }

    // ---- rejections -----------------------------------------------------

    #[test]
    fn rejects_wrong_audience() {
        let (sk, _) = p256_keypair();
        let mut claims = default_claims();
        claims["aud"] = serde_json::json!("did:web:evil.example");
        let jwt = make_jwt("ES256", &claims, |msg| {
            use p256::ecdsa::signature::Signer as _;
            let sig: p256::ecdsa::Signature = sk.sign(msg);
            sig.to_bytes().to_vec()
        });
        assert_eq!(
            prevalidate(&jwt, NOW, AUD, DEFAULT_SKEW_SECS).unwrap_err(),
            AuthError::WrongAudience
        );
    }

    #[test]
    fn rejects_expired_token() {
        let (sk, _) = p256_keypair();
        let mut claims = default_claims();
        claims["exp"] = serde_json::json!(NOW - 1000);
        let jwt = make_jwt("ES256", &claims, |msg| {
            use p256::ecdsa::signature::Signer as _;
            let sig: p256::ecdsa::Signature = sk.sign(msg);
            sig.to_bytes().to_vec()
        });
        assert_eq!(
            prevalidate(&jwt, NOW, AUD, DEFAULT_SKEW_SECS).unwrap_err(),
            AuthError::Expired
        );
    }

    #[test]
    fn rejects_missing_expiry() {
        let (sk, _) = p256_keypair();
        let claims = serde_json::json!({ "iss": "did:plc:example", "aud": AUD, "iat": NOW });
        let jwt = make_jwt("ES256", &claims, |msg| {
            use p256::ecdsa::signature::Signer as _;
            let sig: p256::ecdsa::Signature = sk.sign(msg);
            sig.to_bytes().to_vec()
        });
        assert_eq!(
            prevalidate(&jwt, NOW, AUD, DEFAULT_SKEW_SECS).unwrap_err(),
            AuthError::NoExpiry
        );
    }

    #[test]
    fn rejects_tampered_payload() {
        let (sk, pk) = p256_keypair();
        let jwt = make_jwt("ES256", &default_claims(), |msg| {
            use p256::ecdsa::signature::Signer as _;
            let sig: p256::ecdsa::Signature = sk.sign(msg);
            sig.to_bytes().to_vec()
        });
        // Swap the payload for a different (still valid-aud) one; signature no
        // longer matches the signing input.
        let mut parts: Vec<&str> = jwt.split('.').collect();
        let forged = b64(serde_json::to_string(&serde_json::json!({
            "iss": "did:plc:attacker", "aud": AUD, "exp": NOW + 300
        }))
        .unwrap()
        .as_bytes());
        parts[1] = &forged;
        let tampered = parts.join(".");
        let pv = prevalidate(&tampered, NOW, AUD, DEFAULT_SKEW_SECS).expect("prevalidate");
        assert_eq!(
            verify_signature(&pv, &pk).unwrap_err(),
            AuthError::BadSignature
        );
    }

    #[test]
    fn rejects_alg_key_mismatch() {
        // Token says ES256 but we hand it a k256 key.
        let (sk, _) = p256_keypair();
        let (_, k256_pk) = k256_keypair();
        let jwt = make_jwt("ES256", &default_claims(), |msg| {
            use p256::ecdsa::signature::Signer as _;
            let sig: p256::ecdsa::Signature = sk.sign(msg);
            sig.to_bytes().to_vec()
        });
        let pv = prevalidate(&jwt, NOW, AUD, DEFAULT_SKEW_SECS).expect("prevalidate");
        assert_eq!(
            verify_signature(&pv, &k256_pk).unwrap_err(),
            AuthError::BadSignature
        );
    }

    #[test]
    fn rejects_unsupported_alg() {
        let claims = default_claims();
        let jwt = make_jwt("HS256", &claims, |_| vec![0u8; 32]);
        assert!(matches!(
            prevalidate(&jwt, NOW, AUD, DEFAULT_SKEW_SECS).unwrap_err(),
            AuthError::UnsupportedAlg(_)
        ));
    }

    #[test]
    fn rejects_malformed_jwt() {
        assert_eq!(
            prevalidate("not-a-jwt", NOW, AUD, DEFAULT_SKEW_SECS).unwrap_err(),
            AuthError::MalformedJwt
        );
        assert_eq!(
            prevalidate("", NOW, AUD, DEFAULT_SKEW_SECS).unwrap_err(),
            AuthError::Missing
        );
    }

    // ---- multikey decoding ---------------------------------------------

    #[test]
    fn multikey_roundtrip_k256() {
        let (_, pk) = k256_keypair();
        let SigningKey::Secp256k1(raw) = pk else {
            unreachable!()
        };
        // z + base58btc(varint(0xe7) || key)
        let mut tagged = vec![0xe7, 0x01];
        tagged.extend_from_slice(&raw);
        let multibase = format!("z{}", bs58::encode(tagged).into_string());
        assert_eq!(
            decode_multikey(&multibase).unwrap(),
            SigningKey::Secp256k1(raw)
        );
    }

    #[test]
    fn multikey_roundtrip_p256() {
        let (_, pk) = p256_keypair();
        let SigningKey::P256(raw) = pk else {
            unreachable!()
        };
        // p256-pub multicodec 0x1200 -> varint 0x80 0x24
        let mut tagged = vec![0x80, 0x24];
        tagged.extend_from_slice(&raw);
        let multibase = format!("z{}", bs58::encode(tagged).into_string());
        assert_eq!(decode_multikey(&multibase).unwrap(), SigningKey::P256(raw));
    }

    #[test]
    fn multikey_rejects_non_z_prefix() {
        assert_eq!(
            decode_multikey("Qabc").unwrap_err(),
            AuthError::MalformedKey
        );
    }

    #[test]
    fn varint_reads_two_byte_prefix() {
        // 0x1200 encodes as 0x80 0x24.
        let (v, rest) = read_varint(&[0x80, 0x24, 0xaa]).unwrap();
        assert_eq!(v, 0x1200);
        assert_eq!(rest, &[0xaa]);
    }
}
