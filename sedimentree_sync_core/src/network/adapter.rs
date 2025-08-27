use crate::message::Message;
use crate::peer::{id::PeerId, metadata::PeerMetadata};
use futures::Future;

pub trait NetworkAdapter {
    fn peer_id(&self) -> PeerId;
    fn peer_metadata(&self) -> Option<PeerMetadata>;
    fn is_ready(&self) -> bool;

    fn when_ready(&self) -> impl Future<Output = Result<(), String>>; // FIXME error type
    fn connect(
        &self,
        peer_id: &PeerId,
        peer_metadata: &Option<PeerMetadata>,
    ) -> impl Future<Output = Result<(), String>>; // FIXME error type
    fn send(&self, message: Message) -> impl Future<Output = Result<(), String>>; // FIXME error type
    fn recv(&self, message: Message) -> impl Future<Output = Result<(), String>>; // FIXME error type
    fn disconnect(&mut self) -> impl Future<Output = Result<(), String>>; // FIXME error type
}
