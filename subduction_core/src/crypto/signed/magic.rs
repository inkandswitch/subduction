use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Magic;

pub const MAGIC: &[u8] = b"SDN";

impl TryFrom<&[u8]> for Magic {
    type Error = InvalidMagicError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value == MAGIC {
            Ok(Magic)
        } else {
            Err(InvalidMagicError)
        }
    }
}

impl From<Magic> for &[u8] {
    fn from(_: Magic) -> Self {
        MAGIC
    }
}

#[derive(Debug, Clone, Error)]
#[error("invalid magic bytes")]
pub struct InvalidMagicError;

impl<Ctx> minicbor::Encode<Ctx> for Magic {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        e.bytes(MAGIC)?;
        Ok(())
    }
}

impl<'b, Ctx> minicbor::Decode<'b, Ctx> for Magic {
    fn decode(d: &mut minicbor::Decoder<'b>, _: &mut Ctx) -> Result<Self, minicbor::decode::Error> {
        let bytes = d.bytes()?;
        if bytes == MAGIC {
            Ok(Magic)
        } else {
            Err(minicbor::decode::Error::message("invalid magic bytes"))
        }
    }
}
