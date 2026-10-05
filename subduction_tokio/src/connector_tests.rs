use super::*;

#[tokio::test]
async fn aborted_cleanup_breaks_cycles_even_before_first_poll() -> Result<(), Error> {
    for poll_first in [false, true] {
        let tasks = Tasks::new(CancellationToken::new());
        let scope = AttemptScope::new(&tasks, 0, Duration::from_secs(1));
        let weak = Arc::downgrade(&scope.0);
        let retained = scope.clone();
        scope.on_cleanup(async move {
            retained.cancelled().await;
            Ok(())
        })?;
        if poll_first {
            tokio::task::yield_now().await;
        }
        tasks.abort_all();
        tasks.wait().await;
        assert_eq!(
            scope.cleanup().await.err().map(|e| e.kind()),
            Some(ErrorKind::Cleanup)
        );
        drop(scope);
        assert!(
            weak.upgrade().is_none(),
            "aborted cleanup retained its scope"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sealed_runtime_rejects_attempt_resources() {
    let tasks = Tasks::new(CancellationToken::new());
    tasks.seal();
    let scope = AttemptScope::new(&tasks, 0, Duration::from_secs(1));
    assert!(scope.on_cleanup(async { Ok(()) }).is_err());
    assert_eq!(
        scope.cleanup().await.err().map(|e| e.kind()),
        Some(ErrorKind::Cleanup)
    );
}
