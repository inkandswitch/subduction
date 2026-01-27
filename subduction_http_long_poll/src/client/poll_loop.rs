//! Background polling task for receiving server messages.

use alloc::{sync::Arc, vec::Vec};
use core::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use async_lock::Mutex;
use futures::channel::oneshot;
use reqwest::Client;
use sedimentree_core::collections::Map;
use subduction_core::connection::message::{BatchSyncResponse, Message, RequestId};
use tracing::{debug, error, info, warn};
use url::Url;

use crate::{SESSION_ID_HEADER, session::SessionId};

/// Run the poll loop, fetching messages from the server.
///
/// This function runs until the connection is closed or an unrecoverable error occurs.
const MAX_CONSECUTIVE_ERRORS: u32 = 5;
const ERROR_BACKOFF_MS: u64 = 1000;

pub(super) async fn poll_loop(
    client: Client,
    poll_url: Url,
    session_id: SessionId,
    inbound_writer: async_channel::Sender<Message>,
    pending: Arc<Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>>,
    closed: Arc<AtomicBool>,
    poll_timeout: Duration,
) {
    info!(session_id = %session_id, "starting poll loop");

    let mut consecutive_errors = 0u32;

    loop {
        if closed.load(Ordering::SeqCst) {
            info!("poll loop: connection closed, exiting");
            break;
        }

        let result = poll_once(
            &client,
            &poll_url,
            session_id,
            &inbound_writer,
            &pending,
            poll_timeout,
        )
        .await;

        match result {
            Ok(count) => {
                consecutive_errors = 0;
                if count > 0 {
                    debug!(count, "poll received messages");
                }
            }
            Err(PollError::SessionExpired) => {
                error!("poll loop: session expired, closing connection");
                closed.store(true, Ordering::SeqCst);
                break;
            }
            Err(PollError::ChannelClosed) => {
                info!("poll loop: inbound channel closed, exiting");
                break;
            }
            Err(e) => {
                consecutive_errors += 1;
                warn!(
                    error = %e,
                    consecutive_errors,
                    "poll error"
                );

                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    error!("poll loop: too many consecutive errors, closing");
                    closed.store(true, Ordering::SeqCst);
                    break;
                }

                tokio::time::sleep(Duration::from_millis(
                    ERROR_BACKOFF_MS * u64::from(consecutive_errors),
                ))
                .await;
            }
        }
    }

    inbound_writer.close();
    info!("poll loop exited");
}

#[derive(Debug, thiserror::Error)]
enum PollError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Session expired")]
    SessionExpired,

    #[error("CBOR decode error: {0}")]
    Decode(#[from] minicbor::decode::Error),

    #[error("Channel closed")]
    ChannelClosed,
}

async fn poll_once(
    client: &Client,
    poll_url: &Url,
    session_id: SessionId,
    inbound_writer: &async_channel::Sender<Message>,
    pending: &Arc<Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>>,
    poll_timeout: Duration,
) -> Result<usize, PollError> {
    let response = client
        .get(poll_url.clone())
        .header(SESSION_ID_HEADER, session_id.to_base58())
        .timeout(poll_timeout + Duration::from_secs(5))
        .send()
        .await?;

    let status = response.status().as_u16();
    if status == 401 || status == 404 {
        return Err(PollError::SessionExpired);
    }

    if status == 204 {
        return Ok(0);
    }

    if !response.status().is_success() {
        warn!(status, "poll returned error status");
        return Ok(0);
    }

    let bytes = response.bytes().await?;
    if bytes.is_empty() {
        return Ok(0);
    }

    let messages: Vec<Message> = minicbor::decode(&bytes)?;
    let count = messages.len();

    for msg in messages {
        if let Message::BatchSyncResponse(ref resp) = msg {
            let req_id = resp.req_id;
            if let Some(tx) = pending.lock().await.remove(&req_id) {
                debug!(req_id = ?req_id, "dispatching response to waiter");
                if tx.send(resp.clone()).is_err() {
                    warn!(req_id = ?req_id, "response channel already closed");
                }
                continue;
            }
        }

        if inbound_writer.send(msg).await.is_err() {
            return Err(PollError::ChannelClosed);
        }
    }

    Ok(count)
}
