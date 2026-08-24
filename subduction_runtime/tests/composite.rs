//! A toy echo extension riding authenticated connections through the
//! [`Composite`] combinator: ping crosses, pong comes back, and the
//! extension can only reach peers through granted capabilities.
#![allow(clippy::wildcard_enum_match_arm)] // tests match a growing event enum on purpose

mod common;

use std::{cell::RefCell, rc::Rc};

use common::{stack, wait_for, Stack};
use future_form::Local;
use futures::{executor::LocalPool, task::LocalSpawnExt as _};
use subduction_protocol::{
    effect::AppEvent,
    event::Direction,
    handshake::audience::Audience,
    id::ConnId,
    peer_id::PeerId,
    timestamp::{Now, Timestamp},
    wall_clock::TimestampSeconds,
};
use subduction_runtime::{
    composite::{Composite, ExtensionMachine},
    memory::transport::MemoryTransport,
};
use testresult::TestResult;

const ECHO_SCHEMA: [u8; 4] = *b"ECH\0";

/// What an [`Echo`] machine observed — shared with the test for asserts.
#[derive(Debug, Default)]
struct Observed {
    peers: Vec<PeerId>,
    heard: Vec<Vec<u8>>,
}

/// Replies to any `ping:` message with `pong:` + the same payload.
#[derive(Debug, Default)]
struct Echo {
    observed: Rc<RefCell<Observed>>,
    outbox: Vec<(ConnId, Vec<u8>)>,
}

impl Echo {
    fn new() -> (Self, Rc<RefCell<Observed>>) {
        let observed = Rc::new(RefCell::new(Observed::default()));
        (
            Self {
                observed: Rc::clone(&observed),
                outbox: Vec::new(),
            },
            observed,
        )
    }
}

impl ExtensionMachine for Echo {
    fn schema(&self) -> [u8; 4] {
        ECHO_SCHEMA
    }

    fn peer_up(&mut self, _conn: ConnId, peer: PeerId) {
        self.observed.borrow_mut().peers.push(peer);
    }

    fn peer_down(&mut self, _conn: ConnId, peer: PeerId) {
        self.observed.borrow_mut().peers.retain(|p| *p != peer);
    }

    fn on_message(&mut self, conn: ConnId, _peer: PeerId, bytes: &[u8]) {
        self.observed.borrow_mut().heard.push(bytes.to_vec());
        if let Some(payload) = bytes.get(4..).and_then(|body| body.strip_prefix(b"ping:")) {
            let mut reply = ECHO_SCHEMA.to_vec();
            reply.extend_from_slice(b"pong:");
            reply.extend_from_slice(payload);
            self.outbox.push((conn, reply));
        }
    }

    fn wake(&mut self, _now: Now) {}

    fn poll_send(&mut self) -> Option<(ConnId, Vec<u8>)> {
        if self.outbox.is_empty() {
            None
        } else {
            Some(self.outbox.remove(0))
        }
    }
}

const fn now() -> Now {
    Now {
        monotonic: Timestamp::from_millis(0),
        wall: TimestampSeconds::new(0),
    }
}

/// Wait for the next extension message and feed it to `composite`,
/// asserting it was consumed.
async fn route_next_extension(
    side: &Stack,
    composite: &mut Composite<MemoryTransport>,
) -> Result<(), String> {
    let event = wait_for(side, |event| match event {
        AppEvent::ExtensionMessage { .. } => Some(event.clone()),
        _ => None,
    })
    .await?;
    let consumed = composite
        .dispatch(now(), &event)
        .await
        .map_err(|e| e.to_string())?;
    if consumed {
        Ok(())
    } else {
        Err("extension message not consumed by the composite".into())
    }
}

#[test]
fn echo_extension_rides_authenticated_connections() -> TestResult {
    let mut pool = LocalPool::new();
    let spawner = pool.spawner();

    let (driver_a, a) = stack(1);
    let (driver_b, b) = stack(2);
    spawner.spawn_local(driver_a.run())?;
    spawner.spawn_local(driver_b.run())?;

    let result: Result<(), String> = pool.run_until(async {
        let (ta, tb) = MemoryTransport::pair();
        let (pending_a, pump_a) = a
            .handle
            .connect::<Local>(ta, Direction::Outbound, Some(Audience::known(b.peer)))
            .await
            .map_err(|e| e.to_string())?;
        let (pending_b, pump_b) = b
            .handle
            .connect::<Local>(tb, Direction::Inbound, None)
            .await
            .map_err(|e| e.to_string())?;
        spawner.spawn_local(pump_a).map_err(|e| e.to_string())?;
        spawner.spawn_local(pump_b).map_err(|e| e.to_string())?;

        let conn_a = pending_a.authenticated().await.map_err(|e| e.to_string())?;
        let conn_b = pending_b.authenticated().await.map_err(|e| e.to_string())?;

        // Host an echo machine on each side; grant the capabilities.
        // Cloning before the grant is delegation: the app keeps a
        // capability of its own to bootstrap with.
        let app_conn_a = conn_a.clone();
        let (echo_a, observed_a) = Echo::new();
        let mut composite_a = Composite::new();
        composite_a.register(Box::new(echo_a));
        composite_a.grant(conn_a);
        let (echo_b, observed_b) = Echo::new();
        let mut composite_b = Composite::new();
        composite_b.register(Box::new(echo_b));
        composite_b.grant(conn_b);

        assert_eq!(observed_a.borrow().peers, vec![b.peer], "PeerUp on grant");
        assert_eq!(observed_b.borrow().peers, vec![a.peer], "PeerUp on grant");

        // A pings over the extension schema.
        let mut ping = ECHO_SCHEMA.to_vec();
        ping.extend_from_slice(b"ping:hello");
        app_conn_a
            .send_extension(ping)
            .await
            .map_err(|e| e.to_string())?;

        // B routes the ping into its composite; the echo machine's
        // queued pong flushes back through B's granted capability.
        route_next_extension(&b, &mut composite_b).await?;
        assert_eq!(
            observed_b.borrow().heard,
            vec![b"ECH\0ping:hello".to_vec()],
            "b's echo heard the ping"
        );

        // A routes the pong into its composite.
        route_next_extension(&a, &mut composite_a).await?;
        assert_eq!(
            observed_a.borrow().heard,
            vec![b"ECH\0pong:hello".to_vec()],
            "a's echo heard the pong"
        );

        Ok(())
    });
    result?;
    Ok(())
}
