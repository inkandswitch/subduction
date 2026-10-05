use super::*;
use crate::tasks::Tasks;
use std::{
    error::Error as _,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Default)]
struct EqualBackend(Arc<AtomicUsize>);
impl PartialEq for EqualBackend {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Transport<Sendable> for EqualBackend {
    type SendError = std::io::Error;
    type RecvError = std::io::Error;
    type DisconnectionError = std::io::Error;
    fn send_bytes(&self, _: &[u8]) -> BoxFuture<'_, Result<(), Self::SendError>> {
        Box::pin(async {
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "backend failure",
            ))
        })
    }
    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        Box::pin(std::future::pending())
    }
    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        Box::pin(async {
            self.0.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            Ok(())
        })
    }
}

#[tokio::test]
async fn identity_and_error_sources_survive_erasure() -> Result<(), Box<dyn std::error::Error>> {
    let tasks = Tasks::new(CancellationToken::new());
    let first = AttemptScope::new(&tasks, 0, Duration::from_secs(1));
    let second = AttemptScope::new(&tasks, 1, Duration::from_secs(1));
    let backend = EqualBackend::default();
    let a = ErasedTransport::new(backend.clone(), first.clone());
    let b = ErasedTransport::new(backend.clone(), second.clone());
    assert_eq!(a, a.clone());
    assert_ne!(a, b);
    let Err(error) = a.send_bytes(b"bytes").await else {
        return Err("expected backend error".into());
    };
    assert_eq!(error.kind(), ErrorKind::Transport);
    let original = error
        .source()
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .ok_or("lost original error")?;
    assert_eq!(original.kind(), std::io::ErrorKind::BrokenPipe);
    // Core removal and attempt cleanup may disconnect concurrently. They must
    // poll the same backend disconnect, including after it has completed.
    let (direct, cleanup) = tokio::join!(a.disconnect(), a.cleanup());
    direct?;
    cleanup?;
    a.disconnect().await?;
    assert_eq!(backend.0.load(Ordering::SeqCst), 1);
    first.cleanup().await?;
    second.cleanup().await?;
    tasks.wait().await;
    Ok(())
}
