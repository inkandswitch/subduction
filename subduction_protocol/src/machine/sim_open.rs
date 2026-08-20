//! Simultaneous open: both sides dialed and both sent challenges.
//!
//! Ported from `legacy/subduction_core/src/handshake.rs::initiate`'s
//! crossed-challenge branch. The async sequencing inverts into the
//! `SimOpen*` states of [`HandshakeState`](super::HandshakeState):
//!
//! ```text
//!  AwaitingResponse                 ← both sides sit here after sending
//!    │ MessageReceived(SignedChallenge)   their own challenges
//!    ├─ identical bytes to ours → Fault::ReflectedChallenge
//!    │ pure checks (audience, freshness @ SIMULTANEOUS_OPEN_MAX_DRIFT)
//!    ▼
//!  SimOpenChallengeVerify           (their signature at the driver)
//!    │ Valid; signed by our own key → Fault::ReflectionAttack
//!    │ tie-break: our signed bytes > theirs ⇒ we win
//!    ├─ WINNER ──────────────────┐  ├─ LOSER ────────────────────┐
//!    ▼                           │  ▼                            │
//!  SimOpenAwaitTheirResponse     │  SimOpenLoserSign             │
//!    │ (owed: their challenge)   │    │ send our response        │
//!    │ their response arrives    │    ▼                          │
//!    ▼                           │  SimOpenAwaitTheirResponse    │
//!  SimOpenResponseVerify         │    │ (owed: none)             │
//!    │ Valid + peer matches      │    ▼                          │
//!    ▼                           │  SimOpenResponseVerify        │
//!  AwaitingResponseSign          │    │ Valid + peer matches     │
//!    │ send our response         │    ▼                          │
//!    ▼                           │  Authenticated                │
//!  Authenticated ◀───────────────┘                               │
//! ```
//!
//! Deliberate strengthening vs legacy: if we dialed `Audience::Known(p)`,
//! the crossed challenge's issuer must *be* `p`
//! ([`Fault::PeerMismatch`]) — legacy only pinned the response signer.

use alloc::vec::Vec;

use subduction_crypto::signed::Signed;

use super::{
    HandshakeState, Machine, Now, pinned_peer, signed_preimage, try_decode_payload, verify_item,
};
use crate::{
    effect::{CryptoOp, CryptoResult, Effect, SignatureCheck},
    handshake::{
        HandshakeMessage, SIMULTANEOUS_OPEN_MAX_DRIFT,
        audience::Audience,
        challenge::Challenge,
        response::Response,
    },
    id::ConnId,
    outcome::{Fault, IgnoreReason, Outcome},
    peer_id::PeerId,
};

/// The verified-challenge snapshot handed from the crypto-completion
/// dispatcher to [`Machine::on_sim_open_challenge_verified`].
pub(super) struct CrossedChallenge {
    pub(super) our_challenge: Challenge,
    pub(super) their_challenge: Challenge,
    pub(super) their_peer: PeerId,
    pub(super) we_win: bool,
}

impl Machine {
    /// Crypto completions for the sim-open states. The caller has already
    /// validated connection and generation.
    pub(super) fn sim_open_crypto_done(
        &mut self,
        now: Now,
        conn: ConnId,
        ticket: crate::ticket::CryptoTicket,
        result: CryptoResult,
    ) -> Outcome {
        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        match (&entry.state, result) {
            (
                HandshakeState::SimOpenChallengeVerify {
                    ticket: expected,
                    our_challenge,
                    their_challenge,
                    their_peer,
                    we_win,
                },
                CryptoResult::Verified(check),
            ) if *expected == ticket => {
                let crossed = CrossedChallenge {
                    our_challenge: *our_challenge,
                    their_challenge: *their_challenge,
                    their_peer: *their_peer,
                    we_win: *we_win,
                };
                self.on_sim_open_challenge_verified(now, conn, &crossed, check)
            }
            (
                HandshakeState::SimOpenLoserSign {
                    ticket: expected,
                    preimage,
                    our_challenge,
                    expected: expected_peer,
                },
                CryptoResult::Signed { signature },
            ) if *expected == ticket => {
                let (preimage, our_challenge, expected_peer) =
                    (preimage.clone(), *our_challenge, *expected_peer);
                self.on_sim_open_loser_signed(conn, preimage, our_challenge, expected_peer, signature)
            }
            (
                HandshakeState::SimOpenResponseVerify {
                    ticket: expected,
                    owed,
                    expected: expected_peer,
                    responder,
                },
                CryptoResult::Verified(check),
            ) if *expected == ticket => {
                let (owed, expected_peer, responder) = (*owed, *expected_peer, *responder);
                self.on_sim_open_response_verified(now, conn, owed, expected_peer, responder, check)
            }
            // Duplicate or mismatched completion: consume-once semantics.
            (
                HandshakeState::AwaitingChallenge
                | HandshakeState::AwaitingChallengeSign { .. }
                | HandshakeState::AwaitingResponse { .. }
                | HandshakeState::AwaitingResponseVerify { .. }
                | HandshakeState::AwaitingChallengeVerify { .. }
                | HandshakeState::AwaitingResponseSign { .. }
                | HandshakeState::SimOpenChallengeVerify { .. }
                | HandshakeState::SimOpenLoserSign { .. }
                | HandshakeState::SimOpenAwaitTheirResponse { .. }
                | HandshakeState::SimOpenResponseVerify { .. }
                | HandshakeState::Authenticated
                | HandshakeState::Closing,
                CryptoResult::Signed { .. }
                | CryptoResult::Verified(_)
                | CryptoResult::BatchVerified(_),
            ) => {
                self.stats.unknown_tickets = self.stats.unknown_tickets.saturating_add(1);
                Outcome::Ignored(IgnoreReason::UnknownTicket)
            }
        }
    }

    /// A crossed challenge arrived while we were awaiting a response.
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

        // Pure checks: audience is us — Known(local), or the discovery
        // audience WE dialed with (legacy fallback rule) — and freshness
        // under the sim-open drift budget.
        let Ok((their_challenge, _)) = try_decode_payload::<Challenge>(signed) else {
            return self.fault(conn, Fault::MalformedMessage);
        };
        let known = Audience::known(self.config.local_peer);
        let dialed_discovery = matches!(our_challenge.audience, Audience::Discover(_))
            .then_some(our_challenge.audience);
        let audience_ok = their_challenge.audience == known
            || dialed_discovery.is_some_and(|d| their_challenge.audience == d);
        if !audience_ok
            || !their_challenge.is_fresh(now.wall, SIMULTANEOUS_OPEN_MAX_DRIFT)
        {
            return self.fault(conn, Fault::HandshakeVerificationFailed);
        }

        let their_peer = PeerId::from(signed.issuer());
        let item = verify_item(signed);

        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let ticket = entry.issue_ticket(conn);
        entry.state = HandshakeState::SimOpenChallengeVerify {
            ticket,
            our_challenge,
            their_challenge,
            their_peer,
            we_win,
        };
        self.effects.push_back(Effect::Crypto {
            ticket,
            op: CryptoOp::Verify(item),
        });
        Outcome::Progressed
    }

    /// Their crossed challenge's signature checked out (or not).
    fn on_sim_open_challenge_verified(
        &mut self,
        now: Now,
        conn: ConnId,
        crossed: &CrossedChallenge,
        check: SignatureCheck,
    ) -> Outcome {
        let CrossedChallenge {
            our_challenge,
            their_challenge,
            their_peer,
            we_win,
        } = *crossed;
        if matches!(check, SignatureCheck::Invalid) {
            return self.fault(conn, Fault::HandshakeVerificationFailed);
        }
        // Checked AFTER signature verification (DoS: spoofed issuer
        // fields must not be able to trigger this cheaply), as legacy.
        if their_peer == self.config.local_peer {
            return self.fault(conn, Fault::ReflectionAttack);
        }
        // Our strengthening: a dialed Known audience pins even sim-open.
        if let Some(pinned) = pinned_peer(&our_challenge)
            && pinned != their_peer
        {
            return self.fault(conn, Fault::PeerMismatch);
        }

        if we_win {
            // Winner: receive and verify their response first.
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
    fn on_sim_open_loser_signed(
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
    pub(super) fn on_sim_open_message(&mut self, conn: ConnId, bytes: &[u8]) -> Outcome {
        let Ok(msg) = HandshakeMessage::try_decode(bytes) else {
            self.stats.malformed_messages = self.stats.malformed_messages.saturating_add(1);
            return self.fault(conn, Fault::MalformedMessage);
        };
        match msg {
            HandshakeMessage::SignedResponse(signed) => {
                let Some(entry) = self.conns.get_mut(&conn) else {
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

                let Ok((response, _)) = try_decode_payload::<Response>(&signed) else {
                    return self.fault(conn, Fault::MalformedMessage);
                };
                if response.validate(&our_challenge).is_err() {
                    return self.fault(conn, Fault::HandshakeVerificationFailed);
                }

                let responder = PeerId::from(signed.issuer());
                let item = verify_item(&signed);
                let ticket = match self.conns.get_mut(&conn) {
                    Some(entry) => entry.issue_ticket(conn),
                    None => return Outcome::Ignored(IgnoreReason::UnknownConnection(conn)),
                };
                if let Some(entry) = self.conns.get_mut(&conn) {
                    entry.state = HandshakeState::SimOpenResponseVerify {
                        ticket,
                        owed,
                        expected,
                        responder,
                    };
                }
                self.effects.push_back(Effect::Crypto {
                    ticket,
                    op: CryptoOp::Verify(item),
                });
                Outcome::Progressed
            }
            HandshakeMessage::Rejection(rejection) => {
                self.fault(conn, Fault::HandshakeRejected(rejection.reason))
            }
            HandshakeMessage::SignedChallenge(_) => self.fault(conn, Fault::UnexpectedMessage),
        }
    }

    /// Their response's signature checked out (or not).
    fn on_sim_open_response_verified(
        &mut self,
        now: Now,
        conn: ConnId,
        owed: Option<Challenge>,
        expected: PeerId,
        responder: PeerId,
        check: SignatureCheck,
    ) -> Outcome {
        if matches!(check, SignatureCheck::Invalid) {
            return self.fault(conn, Fault::HandshakeVerificationFailed);
        }
        // The challenge signer and the response signer must be the same
        // peer — otherwise a MITM spliced the exchanges (as legacy).
        if responder != expected {
            return self.fault(conn, Fault::SimultaneousOpenPeerMismatch);
        }

        match owed {
            // Winner: we still owe them a response to their challenge;
            // the existing responder-side sign state finishes the job
            // (send + authenticate).
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
}
