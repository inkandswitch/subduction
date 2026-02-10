//! Error types for FFI operations.

use crate::abi::FfiResult;

#[derive(Debug, thiserror::Error)]
pub enum FfiError {
    #[error("CBOR encode error: {0}")]
    Encode(String),

    #[error("CBOR decode error: {0}")]
    Decode(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("null pointer")]
    NullPointer,

    #[error("protocol error: {0}")]
    Protocol(String),
}

impl From<minicbor::encode::Error<std::io::Error>> for FfiError {
    fn from(e: minicbor::encode::Error<std::io::Error>) -> Self {
        Self::Encode(e.to_string())
    }
}

impl From<minicbor::decode::Error> for FfiError {
    fn from(e: minicbor::decode::Error) -> Self {
        Self::Decode(e.to_string())
    }
}

impl FfiError {
    fn status_code(&self) -> i32 {
        match self {
            Self::Encode(_) => -1,
            Self::Decode(_) => -2,
            Self::Storage(_) => -3,
            Self::NullPointer => -4,
            Self::Protocol(_) => -5,
        }
    }

    pub fn into_ffi_result(self) -> FfiResult {
        FfiResult::err(self.status_code(), &self.to_string())
    }
}

/// Convert a `Result<Vec<u8>, FfiError>` into an `FfiResult`.
pub fn result_to_ffi(result: Result<Vec<u8>, FfiError>) -> FfiResult {
    match result {
        Ok(data) => FfiResult::ok(data),
        Err(e) => e.into_ffi_result(),
    }
}

/// Convert a `Result<(), FfiError>` into an `FfiResult` (no data on success).
pub fn unit_result_to_ffi(result: Result<(), FfiError>) -> FfiResult {
    match result {
        Ok(()) => FfiResult::ok_empty(),
        Err(e) => e.into_ffi_result(),
    }
}
