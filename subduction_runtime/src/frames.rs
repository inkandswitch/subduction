//! The driver's frame table: custody of blob bytes the node references.
//!
//! Machines route [`BlobRef`]s — references into frames retained here —
//! instead of blob bytes. The table obeys the node's data-plane contract:
//!
//! 1. _Retain on delivery._ Every inbound wire message and every
//!    storage-fetched blob becomes a retained frame.
//! 2. _`ReleaseFrame`_: no refs escaped this frame; free it.
//! 3. _`ReleaseBlob`_: one escaped ref is done; the frame is freed when
//!    its last escaped ref is released.
//! 4. _Epoch bulk-free._ When a connection dies, every frame owned by it
//!    is freed in one sweep. Refs into swept frames resolve as `None`
//!    ("gone", never wrong bytes — [`FrameId`]s are never reused), and
//!    the operations holding them fail cleanly.

use sedimentree_core::collections::Map;

use subduction_protocol::{
    blob_ref::{BlobRef, FrameId},
    id::ConnId,
};

/// One retained frame.
#[derive(Debug)]
struct Slot {
    bytes: Vec<u8>,

    /// The connection this frame's lifecycle is keyed to, if any.
    /// `None` for frames minted from local storage fetches, which are
    /// freed only by their refs.
    owner: Option<ConnId>,

    /// Escaped refs not yet released.
    outstanding: u32,

    /// Whether any ref ever escaped (a frame with escapes is freed by
    /// its last `ReleaseBlob`, not by `ReleaseFrame`).
    saw_refs: bool,
}

/// The driver-owned buffer table. See the [module docs](self).
#[derive(Debug, Default)]
pub struct FrameTable {
    slots: Map<u64, Slot>,
    next: u64,
}

impl FrameTable {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Retain `bytes` as a new frame, keyed to `owner`'s epoch when given.
    pub fn retain(&mut self, owner: Option<ConnId>, bytes: Vec<u8>) -> FrameId {
        let id = self.next;
        self.next += 1;
        let _previous = self.slots.insert(
            id,
            Slot {
                bytes,
                owner,
                outstanding: 0,
                saw_refs: false,
            },
        );
        FrameId::new(id)
    }

    /// The bytes a ref points at, or `None` if the frame is gone or the
    /// ref is out of bounds.
    #[must_use]
    pub fn resolve(&self, r: BlobRef) -> Option<&[u8]> {
        let slot = self.slots.get(&r.frame.as_u64())?;
        let start = usize::try_from(r.offset).ok()?;
        let end = usize::try_from(r.offset.checked_add(r.len)?).ok()?;
        slot.bytes.get(start..end)
    }

    /// Record that a ref into this frame escaped into an operation or
    /// message; the frame now outlives `ReleaseFrame` until every escaped
    /// ref is released.
    pub fn note_escape(&mut self, r: BlobRef) {
        if let Some(slot) = self.slots.get_mut(&r.frame.as_u64()) {
            slot.outstanding = slot.outstanding.saturating_add(1);
            slot.saw_refs = true;
        }
    }

    /// The node saw no escaping refs in this frame; free it. A no-op for
    /// frames with escapes (their refs still resolve; `release_blob`
    /// frees them) and for already-swept frames.
    pub fn release_frame(&mut self, frame: FrameId) {
        let free = self
            .slots
            .get(&frame.as_u64())
            .is_some_and(|slot| !slot.saw_refs);
        if free {
            let _slot = self.slots.remove(&frame.as_u64());
        }
    }

    /// One escaped ref is done; frees the frame when it was the last.
    pub fn release_blob(&mut self, r: BlobRef) {
        let Some(slot) = self.slots.get_mut(&r.frame.as_u64()) else {
            return;
        };
        slot.outstanding = slot.outstanding.saturating_sub(1);
        if slot.outstanding == 0 {
            let _slot = self.slots.remove(&r.frame.as_u64());
        }
    }

    /// Epoch bulk-free: drop every frame owned by `conn`.
    pub fn sweep_conn(&mut self, conn: ConnId) {
        self.slots.retain(|_, slot| slot.owner != Some(conn));
    }

    /// Retained frame count (telemetry / tests).
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether no frames are retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob_ref(frame: FrameId, offset: u32, len: u32) -> BlobRef {
        BlobRef { frame, offset, len }
    }

    #[test]
    fn retain_resolve_roundtrip() {
        let mut table = FrameTable::new();
        let frame = table.retain(None, vec![1, 2, 3, 4]);
        assert_eq!(table.resolve(blob_ref(frame, 1, 2)), Some(&[2u8, 3][..]));
        assert_eq!(table.resolve(blob_ref(frame, 3, 2)), None, "out of bounds");
    }

    #[test]
    fn release_frame_frees_only_without_escapes() {
        let mut table = FrameTable::new();
        let plain = table.retain(None, vec![1]);
        let escaped = table.retain(None, vec![2]);
        table.note_escape(blob_ref(escaped, 0, 1));

        table.release_frame(plain);
        table.release_frame(escaped);
        assert_eq!(table.resolve(blob_ref(plain, 0, 1)), None);
        assert!(
            table.resolve(blob_ref(escaped, 0, 1)).is_some(),
            "escaped refs keep the frame alive past ReleaseFrame"
        );

        table.release_blob(blob_ref(escaped, 0, 1));
        assert_eq!(table.resolve(blob_ref(escaped, 0, 1)), None);
        assert!(table.is_empty());
    }

    #[test]
    fn sweep_frees_only_the_dead_conn() {
        let mut table = FrameTable::new();
        let dead = ConnId::new(1);
        let alive = ConnId::new(2);
        let f1 = table.retain(Some(dead), vec![1]);
        let f2 = table.retain(Some(alive), vec![2]);
        let f3 = table.retain(None, vec![3]);

        table.sweep_conn(dead);
        assert_eq!(table.resolve(blob_ref(f1, 0, 1)), None, "swept");
        assert!(table.resolve(blob_ref(f2, 0, 1)).is_some());
        assert!(table.resolve(blob_ref(f3, 0, 1)).is_some());
    }

    #[test]
    fn frame_ids_never_reused_after_sweep() {
        let mut table = FrameTable::new();
        let conn = ConnId::new(1);
        let f1 = table.retain(Some(conn), vec![1]);
        table.sweep_conn(conn);
        let f2 = table.retain(Some(conn), vec![2]);
        assert_ne!(f1, f2);
    }
}
