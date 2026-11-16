//! The signing capability.

use future_form::FutureForm;
use subduction_crypto::signer::Signer;

/// The node's signing identity.
pub trait HasSigner<Async: FutureForm> {
    /// The signer type.
    type Signer: Signer<Async>;
}
