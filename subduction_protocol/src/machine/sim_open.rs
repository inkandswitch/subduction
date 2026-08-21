//! Simultaneous open: both sides dialed and both sent challenges.
//!
//! Ported from `legacy/subduction_core/src/handshake.rs::initiate`'s
//! crossed-challenge branch. Verification is inline (ADR-014), so the
//! whole dance needs only two machine states beyond the shared ones:
//!
//! ```text
//!  AwaitingResponse                 ← both sides sit here after sending
//!    │ MessageReceived(SignedChallenge)   their own challenges
//!    ├─ identical bytes to ours → Fault::ReflectedChallenge
//!    ├─ verify inline; signed by our own key → Fault::ReflectionAttack
//!    ├─ audience/freshness (SIMULTANEOUS_OPEN_MAX_DRIFT) + pin check
//!    │ tie-break: our signed bytes > theirs ⇒ we win
//!    ├─ WINNER ─────────────────────┐  ├─ LOSER ──────────────────┐
//!    ▼                              │  ▼                          │
//!  SimOpenAwaitTheirResponse        │  SimOpenLoserSign           │
//!    │ (owed: their challenge)      │    │ Sign✓ → send response  │
//!    │ their response: verify       │    ▼                        │
//!    │ inline + splice check        │  SimOpenAwaitTheirResponse  │
//!    ▼                              │    │ (owed: none)           │
//!  AwaitingResponseSign             │    │ their response: verify │
//!    │ Sign✓ → send response        │    │ inline + splice check  │
//!    ▼                              │    ▼                        │
//!  Authenticated ◀──────────────────┘  Authenticated ◀────────────┘
//! ```
//!
//! Deliberate strengthening vs legacy (ADR-013): if we dialed
//! `Audience::Known(p)`, the crossed challenge's issuer must *be* `p`
//! ([`Fault::PeerMismatch`]) — legacy only pinned the response signer.

use alloc::vec::Vec;

use subduction_crypto::signed::Signed;

use super::{HandshakeState, Machine, Now, pinned_peer, signed_preimage};
use crate::{
    effect::{CryptoOp, Effect},
    handshake::{
        HandshakeMessage, SIMULTANEOUS_OPEN_MAX_DRIFT, audience::Audience, challenge::Challenge,
        response::Response,
    },
    id::ConnId,
    outcome::{Fault, IgnoreReason, Outcome},
    peer_id::PeerId,
};

impl Machine {
    /// A crossed challenge arrived while we were awaiting a response.
    /// Verification and all guards run inline (ADR-014).
    pub(super) fn on_sim_open_challenge(
        &mut self,
        now: Now,
        conn: ConnId,
        raw: &[u8],
        signed: &Signed<Challenge>,
    ) -> Outcome {
        let Some(entry) = self.conns.get(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let HandshakeState::AwaitingResponse {
            challenge: our_challenge,
            signed_bytes,
        } = &entry.state
        else {
            return self.fault(conn, Fault::UnexpectedMessage);
        };
        let our_challenge = *our_challenge;

        // Reflection guard: our exact bytes replayed back at us.
        if signed_bytes.as_slice() == raw {
            return self.fault(conn, Fault::ReflectedChallenge);
        }
        let we_win = signed_bytes.as_slice() > raw;

        // Inline signature verification, then guards (as legacy: signature
        // first, so spoofed issuer fields cannot trigger cheap rejections).
        let Ok(verified) = signed.try_verify() else {
            return self.fault(conn, Fault::HandshakeVerificationFailed);
        };
        let their_challenge = *verified.payload();
        let their_peer = PeerId::from(verified.issuer());

        if their_peer == self.config.local_peer {
            return self.fault(conn, Fault::ReflectionAttack);
        }

        // Audience: us — Known(local), or the discovery audience WE dialed
        // with (legacy fallback rule) — plus freshness under the sim-open
        // drift budget.
        let known = Audience::known(self.config.local_peer);
        let dialed_discovery = matches!(our_challenge.audience, Audience::Discover(_))
            .then_some(our_challenge.audience);
        let audience_ok = their_challenge.audience == known
            || dialed_discovery.is_some_and(|d| their_challenge.audience == d);
        if !audience_ok || !their_challenge.is_fresh(now.wall, SIMULTANEOUS_OPEN_MAX_DRIFT) {
            return self.fault(conn, Fault::HandshakeVerificationFailed);
        }

        // Our strengthening: a dialed Known audience pins even sim-open.
        if let Some(pinned) = pinned_peer(&our_challenge)
            && pinned != their_peer
        {
            return self.fault(conn, Fault::PeerMismatch);
        }

        if we_win {
            // Winner: receive and verify their response first; we owe them
            // a response afterwards.
            let Some(entry) = self.conns.get_mut(&conn) else {
                return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
            };
            entry.state = HandshakeState::SimOpenAwaitTheirResponse {
                our_challenge,
                owed: Some(their_challenge),
                expected: their_peer,
            };
            Outcome::Progressed
        } else {
            // Loser: sign and send our response to their challenge first.
            let response = Response::for_challenge(&their_challenge, now.wall);
            let preimage = signed_preimage(&self.config.local_peer, &response);
            let Some(entry) = self.conns.get_mut(&conn) else {
                return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
            };
            let ticket = entry.issue_ticket(conn);
            entry.state = HandshakeState::SimOpenLoserSign {
                ticket,
                preimage: preimage.clone(),
                our_challenge,
                expected: their_peer,
            };
            self.effects.push_back(Effect::Crypto {
                ticket,
                op: CryptoOp::Sign { payload: preimage },
            });
            Outcome::Progressed
        }
    }

    /// Loser: our response to their challenge is signed — send it, then
    /// wait for their response to ours.
    pub(super) fn on_sim_open_loser_signed(
        &mut self,
        conn: ConnId,
        preimage: Vec<u8>,
        our_challenge: Challenge,
        expected: PeerId,
        signature: [u8; 64],
    ) -> Outcome {
        let mut bytes = preimage;
        bytes.extend_from_slice(&signature);
        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        entry.state = HandshakeState::SimOpenAwaitTheirResponse {
            our_challenge,
            owed: None,
            expected,
        };
        self.effects.push_back(Effect::SendMessage { conn, bytes });
        Outcome::Progressed
    }

    /// A message arrived while awaiting their response to our challenge.
    /// Verification and the splice check run inline (ADR-014).
    pub(super) fn on_sim_open_message(&mut self, now: Now, conn: ConnId, bytes: &[u8]) -> Outcome {
        let Ok(msg) = HandshakeMessage::try_decode(bytes) else {
            self.stats.malformed_messages = self.stats.malformed_messages.saturating_add(1);
            return self.fault(conn, Fault::MalformedMessage);
        };
        match msg {
            HandshakeMessage::SignedResponse(signed) => {
                let Some(entry) = self.conns.get(&conn) else {
                    return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
                };
                let HandshakeState::SimOpenAwaitTheirResponse {
                    our_challenge,
                    owed,
                    expected,
                } = &entry.state
                else {
                    return self.fault(conn, Fault::UnexpectedMessage);
                };
                let (our_challenge, owed, expected) = (*our_challenge, *owed, *expected);

                let Ok(verified) = signed.try_verify() else {
                    return self.fault(conn, Fault::HandshakeVerificationFailed);
                };
                if verified.payload().validate(&our_challenge).is_err() {
                    return self.fault(conn, Fault::HandshakeVerificationFailed);
                }
                // The challenge signer and the response signer must be the
                // same peer — otherwise a MITM spliced the exchanges.
                let responder = PeerId::from(verified.issuer());
                if responder != expected {
                    return self.fault(conn, Fault::SimultaneousOpenPeerMismatch);
                }

                match owed {
                    // Winner: we still owe our response; the shared
                    // responder-side sign state finishes (send + auth).
                    Some(their_challenge) => {
                        let response = Response::for_challenge(&their_challenge, now.wall);
                        let preimage = signed_preimage(&self.config.local_peer, &response);
                        let Some(entry) = self.conns.get_mut(&conn) else {
                            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
                        };
                        let ticket = entry.issue_ticket(conn);
                        entry.state = HandshakeState::AwaitingResponseSign {
                            ticket,
                            preimage: preimage.clone(),
                            initiator: expected,
                        };
                        self.effects.push_back(Effect::Crypto {
                            ticket,
                            op: CryptoOp::Sign { payload: preimage },
                        });
                        Outcome::Progressed
                    }
                    // Loser: already sent ours; done.
                    None => self.authenticate(conn, expected),
                }
            }
            HandshakeMessage::Rejection(rejection) => {
                self.fault(conn, Fault::HandshakeRejected(rejection.reason))
            }
            HandshakeMessage::SignedChallenge(_) => self.fault(conn, Fault::UnexpectedMessage),
        }
    }
}
