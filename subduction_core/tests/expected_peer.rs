//! A valid signature is not sufficient when dialing a known identity.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use future_form::Sendable;
use futures::future::BoxFuture;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use subduction_core::{
    handshake::{
        self, AuthenticateError, Handshake, HandshakeError, HandshakeMessage, audience::Audience,
        challenge::Challenge,
    },
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::memory::MemorySigner};

struct WrongPeer {
    signer: MemorySigner,
    challenge: Option<Challenge>,
}
impl Handshake<Sendable> for WrongPeer {
    type Error = std::io::Error;
    fn send(&mut self, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), Self::Error>> {
        let HandshakeMessage::SignedChallenge(challenge) =
            HandshakeMessage::try_decode(&bytes).unwrap()
        else {
            panic!("expected challenge")
        };
        self.challenge = Some(*challenge.try_verify().unwrap().payload());
        Box::pin(async { Ok(()) })
    }
    fn recv(&mut self) -> BoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        Box::pin(async move {
            let response = handshake::create_response::<Sendable, _>(
                &self.signer,
                &self.challenge.unwrap(),
                TimestampSeconds::new(100),
            )
            .await;
            Ok(HandshakeMessage::SignedResponse(response).encode())
        })
    }
}

#[tokio::test]
async fn wrong_known_peer_rejected_before_connection_factory() {
    let local = MemorySigner::from_bytes(&[1; 32]);
    let expected = PeerId::from(MemorySigner::from_bytes(&[2; 32]).verifying_key());
    let signer = MemorySigner::from_bytes(&[3; 32]);
    let actual = PeerId::from(signer.verifying_key());
    let called = Arc::new(AtomicBool::new(false));
    let factory_called = called.clone();
    let result = handshake::initiate::<Sendable, _, _, _, _>(
        WrongPeer {
            signer,
            challenge: None,
        },
        move |_, _| {
            factory_called.store(true, Ordering::SeqCst);
            ((), ())
        },
        &local,
        Audience::Known(expected),
        TimestampSeconds::new(100),
        Nonce::from_u128(10),
    )
    .await;
    assert!(
        matches!(result, Err(AuthenticateError::Handshake(HandshakeError::PeerMismatch { expected: e, actual: a })) if e == expected && a == actual)
    );
    assert!(!called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn discovery_still_accepts_a_valid_response_signer() {
    let local = MemorySigner::from_bytes(&[1; 32]);
    let signer = MemorySigner::from_bytes(&[3; 32]);
    let peer = PeerId::from(signer.verifying_key());
    let (authenticated, ()) = handshake::initiate::<Sendable, _, _, _, _>(
        WrongPeer {
            signer,
            challenge: None,
        },
        |_, _| ((), ()),
        &local,
        Audience::discover(b"service"),
        TimestampSeconds::new(100),
        Nonce::from_u128(10),
    )
    .await
    .unwrap();
    assert_eq!(authenticated.peer_id(), peer);
}

struct SimultaneousWrongPeer {
    signer: MemorySigner,
    remote_challenge: Vec<u8>,
    original: Option<Challenge>,
    sent_challenge: bool,
    local_responses: Arc<std::sync::atomic::AtomicUsize>,
}
impl Handshake<Sendable> for SimultaneousWrongPeer {
    type Error = std::io::Error;
    fn send(&mut self, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), Self::Error>> {
        match HandshakeMessage::try_decode(&bytes).unwrap() {
            HandshakeMessage::SignedChallenge(challenge) => {
                self.original = Some(*challenge.try_verify().unwrap().payload());
            }
            HandshakeMessage::SignedResponse(_) => {
                self.local_responses.fetch_add(1, Ordering::SeqCst);
            }
            HandshakeMessage::Rejection(_) => panic!("unexpected rejection"),
        }
        Box::pin(async { Ok(()) })
    }
    fn recv(&mut self) -> BoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        Box::pin(async move {
            if !self.sent_challenge {
                self.sent_challenge = true;
                return Ok(self.remote_challenge.clone());
            }
            let response = handshake::create_response::<Sendable, _>(
                &self.signer,
                &self.original.unwrap(),
                TimestampSeconds::new(100),
            )
            .await;
            Ok(HandshakeMessage::SignedResponse(response).encode())
        })
    }
}

#[tokio::test]
async fn wrong_known_peer_rejected_on_both_simultaneous_open_branches() {
    use std::sync::atomic::AtomicUsize;
    use subduction_crypto::signed::Signed;
    let mut roles = Vec::new();
    for (our_seed, their_seed) in [(1, 2), (2, 1)] {
        let local = MemorySigner::from_bytes(&[our_seed; 32]);
        let remote = MemorySigner::from_bytes(&[their_seed; 32]);
        let expected = PeerId::from(MemorySigner::from_bytes(&[99; 32]).verifying_key());
        let actual = PeerId::from(remote.verifying_key());
        let their_challenge = Challenge::new(
            Audience::Known(PeerId::from(local.verifying_key())),
            TimestampSeconds::new(100),
            Nonce::from_u128(20),
        );
        let theirs = Signed::seal::<Sendable, _>(&remote, their_challenge)
            .await
            .into_signed();
        let ours = Signed::seal::<Sendable, _>(
            &local,
            Challenge::new(
                Audience::Known(expected),
                TimestampSeconds::new(100),
                Nonce::from_u128(10),
            ),
        )
        .await
        .into_signed();
        let we_win = ours.as_bytes() > theirs.as_bytes();
        roles.push(we_win);
        let responses = Arc::new(AtomicUsize::new(0));
        let called = Arc::new(AtomicBool::new(false));
        let factory_called = called.clone();
        let result = handshake::initiate::<Sendable, _, _, _, _>(
            SimultaneousWrongPeer {
                signer: remote,
                remote_challenge: HandshakeMessage::SignedChallenge(theirs).encode(),
                original: None,
                sent_challenge: false,
                local_responses: responses.clone(),
            },
            move |_, _| {
                factory_called.store(true, Ordering::SeqCst);
                ((), ())
            },
            &local,
            Audience::Known(expected),
            TimestampSeconds::new(100),
            Nonce::from_u128(10),
        )
        .await;
        assert!(
            matches!(result, Err(AuthenticateError::Handshake(HandshakeError::PeerMismatch { expected: e, actual: a })) if e == expected && a == actual)
        );
        assert!(!called.load(Ordering::SeqCst));
        assert_eq!(responses.load(Ordering::SeqCst), usize::from(!we_win));
    }
    assert!(roles.contains(&true) && roles.contains(&false));
}
