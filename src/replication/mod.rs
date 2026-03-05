//! Hypercore to Hypercore replication
pub mod events;
#[cfg(feature = "shared-core")]
pub mod shared_core;

use futures::Stream;
use futures_lite::future::FutureExt;
use hypercore_handshake::CipherTrait;
use hypercore_protocol::{Protocol, discovery_key};
#[cfg(feature = "shared-core")]
pub use shared_core::SharedCore;
use tracing::{error, trace, warn};

use crate::{AppendOutcome, Hypercore, HypercoreError, Info, PartialKeypair};

use hypercore_schema::{Proof, RequestBlock, RequestSeek, RequestUpgrade};

pub use events::Event;

use async_broadcast::Receiver;
use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

/// Methods related to just this core's information
pub trait CoreInfo {
    /// Get core info (see: [`crate::Hypercore::info`]
    fn info(&self) -> impl Future<Output = Info> + Send;
    /// Get the key_pair (see: [`crate::Hypercore::key_pair`]
    fn key_pair(&self) -> impl Future<Output = PartialKeypair> + Send;
}

/// Error for ReplicationMethods trait
#[derive(thiserror::Error, Debug)]
pub enum ReplicationMethodsError {
    /// Error from hypercore
    #[error("Got a hypercore error: [{0}]")]
    HypercoreError(#[from] HypercoreError),
    /// Error from CoreMethods
    #[error("Got a CoreMethods error: [{0}]")]
    CoreMethodsError(#[from] CoreMethodsError),
}

/// Methods needed for replication
pub trait ReplicationMethods: CoreInfo + Send {
    /// ref Core::verify_and_apply_proof
    fn verify_and_apply_proof(
        &self,
        proof: Proof,
    ) -> impl Future<Output = Result<bool, ReplicationMethodsError>> + Send;
    /// ref Core::missing_nodes
    fn missing_nodes(
        &self,
        index: u64,
    ) -> impl Future<Output = Result<u64, ReplicationMethodsError>> + Send;
    /// ref Core::create_proof
    fn create_proof(
        &self,
        block: Option<RequestBlock>,
        hash: Option<RequestBlock>,
        seek: Option<RequestSeek>,
        upgrade: Option<RequestUpgrade>,
    ) -> impl Future<Output = Result<Option<Proof>, ReplicationMethodsError>> + Send;
    /// subscribe to core events
    fn event_subscribe(&self) -> impl Future<Output = Receiver<Event>>;
}

/// Error for CoreMethods trait
#[derive(thiserror::Error, Debug)]
pub enum CoreMethodsError {
    /// Error from hypercore
    #[error("Got a hypercore error [{0}]")]
    HypercoreError(#[from] HypercoreError),
}

/// Trait for things that consume [`crate::Hypercore`] can instead use this trait
/// so they can use all Hypercore-like things such as `SharedCore`.
pub trait CoreMethods: CoreInfo {
    /// Check if the core has the block at the given index locally
    fn has(&self, index: u64) -> impl Future<Output = bool> + Send;

    /// get a block
    fn get(
        &self,
        index: u64,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, CoreMethodsError>> + Send;

    /// Append data to the core
    fn append(
        &self,
        data: &[u8],
    ) -> impl Future<Output = Result<AppendOutcome, CoreMethodsError>> + Send;

    /// Append a batch of data to the core
    fn append_batch<A: AsRef<[u8]>, B: AsRef<[A]> + Send>(
        &self,
        batch: B,
    ) -> impl Future<Output = Result<AppendOutcome, CoreMethodsError>> + Send;
}

pub struct Peer {
    protocol: Protocol,
    _pending_open: Option<Pin<Box<dyn Future<Output = Result<(), std::io::Error>>>>>,
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("protocol", &self.protocol)
            //.field("pending_open", &self.pending_open)
            .finish()
    }
}
impl Peer {
    fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            _pending_open: Default::default(),
        }
    }

    fn _poll_peer(
        &mut self,
        core: &Hypercore,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), HypercoreError>> {
        if let Some(mut fut) = self._pending_open.take() {
            match fut.poll(cx) {
                Poll::Ready(res) => match res {
                    Ok(_) => {
                        trace!("protocol opened");
                    }
                    Err(e) => {
                        error!(error =? e, "protocol open failed");
                    }
                },
                Poll::Pending => {
                    _ = self._pending_open.insert(fut);
                    return Poll::Pending;
                }
            }
        }
        let event = match Pin::new(&mut self.protocol).poll_next(cx) {
            Poll::Ready(res) => match res {
                Some(Ok(e)) => e,
                Some(Err(e)) => return Poll::Ready(Err(e.into())),
                None => return Poll::Pending,
            },
            Poll::Pending => todo!(),
        };
        match event {
            hypercore_protocol::Event::Handshake(_) => {
                if self.protocol.is_initiator() {
                    let key = core.key_pair().public.to_bytes();
                    self._pending_open = Some(Box::pin(self.protocol.open(key)));
                }
            }
            hypercore_protocol::Event::DiscoveryKey(dkey) => {
                let key = core.key_pair().public.to_bytes();
                let this_dkey = discovery_key(&key);
                if this_dkey == dkey {
                    self._pending_open = Some(Box::pin(self.protocol.open(key)));
                } else {
                    warn!("Got discovery key for different core: {dkey:?}");
                }
            }
            hypercore_protocol::Event::Channel(_channel) => todo!(),
            hypercore_protocol::Event::Close(_) => {}
            _ => todo!(),
        }
        todo!()
    }
}

impl Hypercore {
    pub fn replicate(&mut self, stream: impl CipherTrait + 'static) {
        let protocol = Protocol::new(Box::new(stream));
        self.peers.push(Mutex::new(Peer::new(protocol)));
    }
}

impl Stream for Hypercore {
    type Item = Result<(), HypercoreError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        //for peer in self.peers.iter() {
        //    if let Poll::Ready(_) = peer.lock().unwrap().poll_peer(self.deref_mut(), cx) {
        //        cx.waker().wake_by_ref();
        //    }
        //}
        Poll::Pending
    }
}
