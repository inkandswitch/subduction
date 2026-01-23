// // going to need to keep the exact bytes around for sharing with others
//
// pub struct Verified<T>{
//     author: ed25519_dalek::VerifyingKey,
//     payload: T,
// }; // NEVER send over the wire
//
// impl Verified<T> {
//     pub fn payload(&self) -> &T {
//         &self.0.payload
//     }
//
//     pub fn author(&self) -> &ed25519_dalek::VerifyingKey {
//         &self.0.author
//     }
//
//     pub fn signature(&self) -> &ed25519_dalek::Signature {
//         &self.0.signature
//     }
// }
//
// pub struct Signed<T> {
//     protocol_id: Version,
//     author: ed25519_dalek::VerifyingKey,
//     signature: ed25519_dalek::Signature,
//     payload: T,
//     payload_bytes: Vec<u8>,
// }
//
// impl Signed<T> {
//     pub fn verify(&self) -> Result<Verified<T>, ed25519_dalek::SignatureError> {
//         let payload_bytes = bincode::serialize(&self.payload).map_err(|_| {
//             ed25519_dalek::SignatureError::from(ed25519_dalek::SignatureError::InvalidSignature)
//         })?;
//         self.author.verify_strict(&payload_bytes, &self.signature)?;
//         Ok(Verified(self.clone()))
//     }
// }
//
// pub struct Challenge(Signed<Timestamp>);
//
// pub struct Witness(PeerId, SediementreeId);
