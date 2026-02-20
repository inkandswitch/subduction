//! Encode/decode `ed25519_dalek::Signature` as CBOR bytes.

/// Encode a `Signature` as CBOR bytes.
///
/// # Errors
///
/// Returns an error if encoding fails.
pub fn encode<Ctx, W: minicbor::encode::Write>(
    sig: &ed25519_dalek::Signature,
    e: &mut minicbor::Encoder<W>,
    _ctx: &mut Ctx,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    e.bytes(&sig.to_bytes())?;
    Ok(())
}

/// Decode a `Signature` from CBOR bytes.
///
/// # Errors
///
/// Returns an error if decoding fails or the bytes are not a valid signature.
pub fn decode<Ctx>(
    d: &mut minicbor::Decoder<'_>,
    _ctx: &mut Ctx,
) -> Result<ed25519_dalek::Signature, minicbor::decode::Error> {
    let arr: [u8; 64] = d
        .bytes()?
        .try_into()
        .map_err(|_| minicbor::decode::Error::message("Signature must be exactly 64 bytes"))?;
    Ok(ed25519_dalek::Signature::from_bytes(&arr))
}
