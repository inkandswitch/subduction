//! Multiplexers indexed by peer and physical connection.

use alloc::{sync::Arc, vec::Vec};
use future_form::FutureForm;
use sedimentree_core::collections::Map;

use crate::{authenticated::Authenticated, multiplexer::Multiplexer, peer::id::PeerId};

#[derive(Debug)]
struct Entry<Conn: Clone, Async: FutureForm> {
    connection: Authenticated<Conn, Async>,
    mux: Arc<Multiplexer>,
}

/// One mux per registered physical connection, grouped by authenticated peer.
#[derive(Debug)]
pub(super) struct MultiplexerMap<Conn: Clone, Async: FutureForm> {
    by_peer: Map<PeerId, Vec<Entry<Conn, Async>>>,
}

impl<Conn: Clone + PartialEq, Async: FutureForm> MultiplexerMap<Conn, Async> {
    pub(super) fn new() -> Self {
        Self {
            by_peer: Map::new(),
        }
    }

    /// Register a newly admitted connection. The caller excludes duplicates
    /// through the connection registry before inserting its mux here.
    ///
    /// ## Panics
    ///
    /// - If a mux is already registered for the same physical connection to the same peer.
    pub(super) fn insert(&mut self, connection: Authenticated<Conn, Async>, mux: Arc<Multiplexer>) {
        let peer_id = connection.peer_id();
        assert!(
            !self.by_peer.get(&peer_id).is_some_and(|entries| {
                entries.iter().any(|entry| entry.connection == connection)
            }),
            "duplicate muxes for the same physical connection to peer {peer_id:?}"
        );
        self.by_peer
            .entry(connection.peer_id())
            .or_default()
            .push(Entry { connection, mux });
    }

    /// Clone the matching mux so it can be used after releasing the map lock.
    pub(super) fn get(&self, connection: &Authenticated<Conn, Async>) -> Option<Arc<Multiplexer>> {
        self.by_peer
            .get(&connection.peer_id())?
            .iter()
            .find(|entry| &entry.connection == connection)
            .map(|entry| entry.mux.clone())
    }

    /// Detach only this physical connection's muxes, preserving its siblings.
    pub(super) fn detach_connection(
        &mut self,
        connection: &Authenticated<Conn, Async>,
    ) -> Vec<Arc<Multiplexer>> {
        let peer_id = connection.peer_id();
        let entries = self.by_peer.remove(&peer_id).unwrap_or_default();
        let mut detached = Vec::new();
        let mut retained = Vec::new();
        for entry in entries {
            if &entry.connection == connection {
                detached.push(entry.mux);
            } else {
                retained.push(entry);
            }
        }
        if !retained.is_empty() {
            self.by_peer.insert(peer_id, retained);
        }
        detached
    }

    /// Detach every mux for a peer, leaving other peers untouched.
    pub(super) fn detach_peer(&mut self, peer_id: &PeerId) -> Vec<Arc<Multiplexer>> {
        self.by_peer
            .remove(peer_id)
            .unwrap_or_default()
            .into_iter()
            .map(|entry| entry.mux)
            .collect()
    }

    /// Empty the registry and return its muxes for post-lock cancellation.
    pub(super) fn detach_all(&mut self) -> Vec<Arc<Multiplexer>> {
        core::mem::take(&mut self.by_peer)
            .into_values()
            .flatten()
            .map(|entry| entry.mux)
            .collect()
    }

    #[cfg(any(feature = "test_utils", test))]
    pub(super) fn count(&self, peer_id: &PeerId) -> usize {
        self.by_peer.get(peer_id).map_or(0, Vec::len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authenticated::Direction;
    use core::time::Duration;
    use future_form::Sendable;

    // Physical identity needs only equality; no transport or executor is needed.
    fn connection(id: u8, peer: u8) -> Authenticated<u8, Sendable> {
        Authenticated::new_for_test(id, PeerId::new([peer; 32]), Direction::Dialed)
    }
    fn mux(conn: &Authenticated<u8, Sendable>) -> Arc<Multiplexer> {
        Arc::new(Multiplexer::new(conn.peer_id(), Duration::from_secs(1)))
    }

    #[test]
    fn allows_three_distinct_connections_to_the_same_peer() {
        let mut map = MultiplexerMap::new();
        for id in 1..=3 {
            let conn = connection(id, 10);
            map.insert(conn.clone(), mux(&conn));
        }
        assert_eq!(map.count(&PeerId::new([10; 32])), 3);
    }

    #[test]
    #[should_panic(expected = "duplicate muxes for the same physical connection")]
    fn rejects_a_duplicate_physical_connection() {
        let mut map = MultiplexerMap::new();
        let conn = connection(1, 10);
        map.insert(conn.clone(), mux(&conn));
        map.insert(conn.clone(), mux(&conn));
    }

    #[test]
    fn lookup_distinguishes_physical_connections_to_the_same_peer() {
        let mut map = MultiplexerMap::new();
        let a = connection(1, 10);
        let b = connection(2, 10);
        let a_mux = mux(&a);
        let b_mux = mux(&b);
        map.insert(a.clone(), a_mux.clone());
        map.insert(b.clone(), b_mux.clone());

        assert!(map.get(&a).is_some_and(|found| Arc::ptr_eq(&found, &a_mux)));
        assert!(map.get(&b).is_some_and(|found| Arc::ptr_eq(&found, &b_mux)));
        assert!(map.get(&connection(3, 10)).is_none());
        assert!(map.get(&connection(4, 20)).is_none());
        assert_eq!(map.count(&a.peer_id()), 2);
    }

    #[test]
    fn detaching_one_connection_preserves_siblings_and_other_peers() {
        let mut map = MultiplexerMap::new();
        let a = connection(1, 10);
        let b = connection(2, 10);
        let other = connection(3, 20);
        let a_mux = mux(&a);
        let b_mux = mux(&b);
        let other_mux = mux(&other);
        map.insert(a.clone(), a_mux.clone());
        map.insert(b.clone(), b_mux.clone());
        map.insert(other.clone(), other_mux.clone());

        let detached = map.detach_connection(&a);
        assert_eq!(detached.len(), 1);
        assert!(detached.iter().any(|found| Arc::ptr_eq(found, &a_mux)));
        assert!(map.get(&a).is_none());
        assert!(map.get(&b).is_some_and(|found| Arc::ptr_eq(&found, &b_mux)));
        assert!(
            map.get(&other)
                .is_some_and(|found| Arc::ptr_eq(&found, &other_mux))
        );
        assert!(map.detach_connection(&a).is_empty());
        assert!(map.detach_connection(&connection(4, 10)).is_empty());
        assert_eq!(map.count(&b.peer_id()), 1);

        assert_eq!(map.detach_connection(&b).len(), 1);
        assert!(
            !map.by_peer.contains_key(&b.peer_id()),
            "remove empty peer buckets"
        );
        assert_eq!(map.count(&other.peer_id()), 1);
    }

    #[test]
    fn late_detachment_cannot_remove_a_replacement() {
        let mut map = MultiplexerMap::new();
        let old = connection(1, 10);
        let replacement = connection(2, 10);
        map.insert(old.clone(), mux(&old));
        assert_eq!(map.detach_peer(&old.peer_id()).len(), 1);
        let replacement_mux = mux(&replacement);
        map.insert(replacement.clone(), replacement_mux.clone());

        assert!(map.detach_connection(&old).is_empty());
        assert!(map.get(&old).is_none());
        assert!(
            map.get(&replacement)
                .is_some_and(|found| Arc::ptr_eq(&found, &replacement_mux))
        );
    }

    #[test]
    fn peer_detachment_and_global_drain_have_distinct_scopes() {
        let mut map = MultiplexerMap::new();
        let a = connection(1, 10);
        let b = connection(2, 10);
        let other = connection(3, 20);
        let a_mux = mux(&a);
        let b_mux = mux(&b);
        let other_mux = mux(&other);
        map.insert(a.clone(), a_mux.clone());
        map.insert(b, b_mux.clone());
        map.insert(other.clone(), other_mux.clone());

        assert!(map.detach_peer(&PeerId::new([30; 32])).is_empty());
        let detached = map.detach_peer(&a.peer_id());
        assert_eq!(detached.len(), 2);
        assert!(detached.iter().any(|found| Arc::ptr_eq(found, &a_mux)));
        assert!(detached.iter().any(|found| Arc::ptr_eq(found, &b_mux)));
        assert_eq!(map.count(&a.peer_id()), 0);
        assert_eq!(map.count(&other.peer_id()), 1);
        assert!(map.detach_peer(&a.peer_id()).is_empty());

        let rest = map.detach_all();
        assert_eq!(rest.len(), 1);
        assert!(rest.iter().any(|found| Arc::ptr_eq(found, &other_mux)));
        assert!(map.by_peer.is_empty());
        assert!(map.detach_all().is_empty());
    }
}
