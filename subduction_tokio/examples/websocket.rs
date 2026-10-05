//! Ephemeral connectivity example:
//!
//! ```text
//! cargo run -p subduction_tokio --features websocket --example websocket -- \
//!     ws://localhost:8080 <64-hex-digit-peer-id>
//! ```
use std::{error::Error, time::Duration};
use subduction_core::{peer::id::PeerId, policy::open::OpenPolicy, storage::memory::MemoryStorage};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_tokio::{ExpectedPeer, ReconnectPolicy, TokioClient, websocket::WebSocketConnector};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1);
    let url = arguments.next().ok_or("expected WebSocket URL")?;
    let identity = arguments.next().ok_or("expected 64-hex-digit peer ID")?;
    if identity.len() != 64 || !identity.is_ascii() {
        return Err("peer ID must be 64 ASCII hex digits".into());
    }
    let mut bytes = [0_u8; 32];
    for (byte, pair) in bytes.iter_mut().zip(identity.as_bytes().chunks_exact(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    // These are explicit local-test choices; applications should select their
    // own persistent storage, identity, and authorization policy.
    let client = TokioClient::builder()
        .storage(MemoryStorage::default())
        .policy(OpenPolicy)
        .signer(MemorySigner::generate())
        .start()?;
    let connection = client
        .handle()
        .connect(
            WebSocketConnector::new(url),
            ExpectedPeer::Known(PeerId::new(bytes)),
        )
        .reconnect(ReconnectPolicy::exponential_backoff())
        .await?;
    println!("connected: {:?}", connection.status());
    tokio::time::sleep(Duration::from_secs(1)).await;
    connection.close().await?;
    let report = client.close().await;
    println!("shutdown: {report:?}");
    if report.forced || report.unfinished_tasks != 0 || report.failure.is_some() {
        return Err("client did not close gracefully".into());
    }
    Ok(())
}
