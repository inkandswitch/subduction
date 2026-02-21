//! Encode/decode `ed25519_dalek::VerifyingKey` as CBOR bytes.

/// Encode a `VerifyingKey` as CBOR bytes.
///
/// # Errors
///
/// Returns an error if encoding fails.
pub fn encode<Ctx, W: minicbor::encode::Write>(
    key: &ed25519_dalek::VerifyingKey,
    e: &mut minicbor::Encoder<W>,
    _ctx: &mut Ctx,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    e.bytes(key.as_bytes())?;
    Ok(())
}

/// Decode a `VerifyingKey` from CBOR bytes.
///
/// # Errors
///
/// Returns an error if decoding fails or the bytes are not a valid key.
pub fn decode<Ctx>(
    d: &mut minicbor::Decoder<'_>,
    _ctx: &mut Ctx,
) -> Result<ed25519_dalek::VerifyingKey, minicbor::decode::Error> {
    let arr: [u8; 32] = d
        .bytes()?
        .try_into()
        .map_err(|_| minicbor::decode::Error::message("VerifyingKey must be exactly 32 bytes"))?;

    ed25519_dalek::VerifyingKey::from_bytes(&arr)
        .map_err(|_| minicbor::decode::Error::message("invalid VerifyingKey"))
}
