//! Approach B: Full sans-I/O state machine.
//!
//! No async runtime. The host language drives every I/O operation via
//! an effect/response protocol:
//!
//! 1. Create a driver for a specific operation
//! 2. Call `driver_next_effect()` to get the next I/O request
//! 3. Perform the I/O in the host language
//! 4. Call `driver_provide_response()` with the result
//! 5. Repeat until `driver_next_effect()` returns `EFFECT_TAG_COMPLETE`
//! 6. Call `driver_finish()` to extract the final result

pub mod effect;
pub mod driver;
pub mod ffi;
