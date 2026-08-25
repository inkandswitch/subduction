//! The server must not outlive its Subduction listener/manager futures:
//! an accept loop that keeps completing handshakes nobody will ever service
//! is worse than a stopped server. The supervised spawns in
//! `setup_with_keepalive` stop the whole server when either critical future
//! exits outside an orderly shutdown.

use core::time::Duration;

use sedimentree_core::depth::CountLeadingZeroBytes;
use subduction_core::{
    nonce_cache::NonceCache, policy::open::OpenPolicy, storage::memory::MemoryStorage,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket_legacy::{
    DEFAULT_MAX_MESSAGE_SIZE,
    tokio::{TimeoutTokio, server::TokioWebSocketServer},
};
use testresult::TestResult;

/// If the core pipeline shuts down behind the server's back (standing in for
/// a listener/manager death), the accept loop must stop accepting rather
/// than zombify.
#[tokio::test]
async fn server_stops_accepting_when_core_pipeline_dies() -> TestResult {
    let server = TokioWebSocketServer::setup(
        "127.0.0.1:0".parse()?,
        TimeoutTokio,
        Duration::from_secs(60),
        DEFAULT_MAX_MESSAGE_SIZE,
        MemorySigner::from_bytes(&[0xAA; 32]),
        None,
        MemoryStorage::new(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await?;
    let address = server.address();

    // Sanity: the server accepts TCP while healthy.
    tokio::net::TcpStream::connect(address).await?;

    // Kill the core pipeline without going through `stop()`: the listener
    // and manager futures exit via their channel-close paths, as if they had
    // died unexpectedly.
    server.subduction().shutdown();

    // The supervisor must cancel the accept loop; poll until connections
    // are refused. The deadline only bounds the failure case.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::net::TcpStream::connect(address).await.is_err() {
            break; // accept loop is gone
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "server kept accepting connections after its core pipeline died"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    Ok(())
}
