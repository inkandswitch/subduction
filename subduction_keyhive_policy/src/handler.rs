//! Keyhive message handler for Subduction.
//!
//! Provides [`KeyhiveProtocolHandle`], a `Send + Sync` handle that
//! bridges the [`Handler`] trait to an existing [`KeyhiveProtocol`]
//! running on a `!Send` task (e.g., a `tokio::task::LocalSet`).
//!
//! # Architecture
//!
//! ```text
//! KeyhiveProtocolHandle (Send + Sync, Clone)
//!   │  impl Handler<Sendable, C>
//!   │
//!   ▼ async_channel
//! KeyhiveActor (runs on LocalSet, owns !Send KeyhiveProtocol)
//!   │  loops on KeyhiveCommand
//!   ▼
//! KeyhiveProtocol::handle_message(...)
//! ```
//!
//! When `keyhive_core` becomes `Send`, the actor can be removed and
//! `KeyhiveProtocolHandle` can hold an `Arc<KeyhiveProtocol>` directly.
//!
//! [`Handler`]: subduction_core::handler::Handler
//! [`KeyhiveProtocol`]: subduction_keyhive::KeyhiveProtocol

use alloc::string::String;

use async_channel::{Receiver, Sender};
use futures::{FutureExt, future::BoxFuture};
use subduction_core::{
    authenticated::Authenticated,
    handler::Handler,
    peer::id::PeerId,
};
use subduction_keyhive::{
    KeyhiveMessage, SignedMessage,
    signed_message::CborError,
};
                // Best-effort reply; handle may have timed out.
                let _ = reply.send(result).await;
            }
            KeyhiveCommand::PeerDisconnect { peer_id } => {
                on_disconnect(peer_id).await;
            }
        }
    }

    tracing::debug!("keyhive actor shutting down (channel closed)");
}

// ── Handler impl ────────────────────────────────────────────────────────

impl<C> Handler<future_form::Sendable, C> for KeyhiveProtocolHandle
where
    C: Clone + Send + Sync + 'static,
{
    type Message = KeyhiveMessage;
    type HandlerError = HandleError;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, future_form::Sendable>,
        message: KeyhiveMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        let peer_id = conn.peer_id();
        let tx = self.tx.clone();

        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);

            tx.send(KeyhiveCommand::HandleInbound {
                peer_id,
                message,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HandleError::ActorGone)?;

            reply_rx.recv().await.map_err(|_| HandleError::ActorGone)?
        }
        .boxed()
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> BoxFuture<'_, ()> {
        let tx = self.tx.clone();
        async move {
            let _ = tx
                .send(KeyhiveCommand::PeerDisconnect { peer_id: peer })
                .await;
        }
        .boxed()
    }
}
