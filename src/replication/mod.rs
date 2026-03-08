//! Hypercore to Hypercore replication
pub mod events;
#[cfg(feature = "shared-core")]
pub mod shared_core;

use std::{
    collections::{BTreeSet, VecDeque},
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use hypercore_handshake::CipherTrait;
use hypercore_protocol::{
    Channel, Protocol, discovery_key,
    schema::{Data, Range, Request, Synchronize},
    Message,
};
#[cfg(feature = "shared-core")]
pub use shared_core::SharedCore;
use tracing::{error, warn};

use crate::{
    AppendOutcome, Hypercore, HypercoreError, Info, PartialKeypair,
    core::inner::{CreateProofFuture, HypercoreInner, MissingNodesFuture, VerifyAndApplyProofFuture},
};
use hypercore_schema::{RequestBlock, RequestSeek, RequestUpgrade};

pub use events::Event;

use async_broadcast::Receiver;

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
        proof: hypercore_schema::Proof,
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
    ) -> impl Future<Output = Result<Option<hypercore_schema::Proof>, ReplicationMethodsError>> + Send;
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

// ── Remote bitfield ────────────────────────────────────────────────────────────

struct RemoteBitfield(BTreeSet<u64>);

impl RemoteBitfield {
    fn new() -> Self {
        Self(BTreeSet::new())
    }

    fn get(&self, index: u64) -> bool {
        self.0.contains(&index)
    }

    fn set_range(&mut self, start: u64, length: u64, value: bool) {
        for i in start..(start + length) {
            if value {
                self.0.insert(i);
            } else {
                self.0.remove(&i);
            }
        }
    }
}

// ── PeerState ──────────────────────────────────────────────────────────────────

struct PeerState {
    can_upgrade: bool,
    remote_fork: u64,
    remote_length: u64,
    remote_bitfield: RemoteBitfield,
    remote_can_upgrade: bool,
    remote_uploading: bool,
    remote_downloading: bool,
    remote_synced: bool,
    length_acked: u64,
}

impl Default for PeerState {
    fn default() -> Self {
        Self {
            can_upgrade: true,
            remote_fork: 0,
            remote_length: 0,
            remote_bitfield: RemoteBitfield::new(),
            remote_can_upgrade: false,
            remote_uploading: true,
            remote_downloading: true,
            remote_synced: false,
            length_acked: 0,
        }
    }
}

// ── DataMeta ───────────────────────────────────────────────────────────────────

/// Saved from a Data message; used after verify_and_apply completes to decide
/// what to request next.
struct DataMeta {
    has_upgrade: bool,
    pre_length: u64,
    remote_length: u64,
    block_index: Option<u64>,
}

// ── ChannelState ───────────────────────────────────────────────────────────────

struct ChannelState {
    channel: Channel,
    state: PeerState,
    synced: bool,

    // Outgoing batches; drained one at a time through pending_send.
    outgoing: VecDeque<Vec<Message>>,
    pending_send: Option<Pin<Box<dyn Future<Output = io::Result<()>>>>>,

    // Incoming Request → create_proof
    pending_create_proof: Option<CreateProofFuture>,
    pending_create_proof_id: u64,
    pending_create_proof_fork: u64,

    // Incoming Data → verify_and_apply_proof
    pending_verify_apply: Option<VerifyAndApplyProofFuture>,
    pending_data_meta: Option<DataMeta>,

    // Block request queue: each index needs a missing_nodes call before we
    // can send the Request message.
    pending_request_indices: VecDeque<u64>,
    pending_missing_nodes: Option<(u64, MissingNodesFuture)>,

    // Core events (Get / Have / DataUpgrade)
    core_events: async_broadcast::Receiver<events::Event>,
}

impl ChannelState {
    fn new(channel: Channel, core_events: async_broadcast::Receiver<events::Event>) -> Self {
        Self {
            channel,
            state: PeerState::default(),
            synced: false,
            outgoing: VecDeque::new(),
            pending_send: None,
            pending_create_proof: None,
            pending_create_proof_id: 0,
            pending_create_proof_fork: 0,
            pending_verify_apply: None,
            pending_data_meta: None,
            pending_request_indices: VecDeque::new(),
            pending_missing_nodes: None,
            core_events,
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        inner: &HypercoreInner,
    ) -> Poll<Result<(), HypercoreError>> {
        // ── Drive pending send ─────────────────────────────────────────────────
        if let Some(ref mut fut) = self.pending_send {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => self.pending_send = None,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
                Poll::Pending => {}
            }
        }
        if self.pending_send.is_none() {
            if let Some(batch) = self.outgoing.pop_front() {
                let channel = self.channel.clone();
                self.pending_send = Some(Box::pin(async move {
                    channel.send_batch(&batch).await
                }));
                cx.waker().wake_by_ref();
            }
        }

        // ── Initial sync ───────────────────────────────────────────────────────
        if !self.synced {
            let info = inner.info();
            let remote_length = if info.fork == self.state.remote_fork {
                self.state.remote_length
            } else {
                0
            };
            let mut msgs = vec![Message::Synchronize(Synchronize {
                fork: info.fork,
                length: info.length,
                remote_length,
                can_upgrade: self.state.can_upgrade,
                uploading: true,
                downloading: true,
            })];
            if info.contiguous_length > 0 {
                msgs.push(Message::Range(Range {
                    drop: false,
                    start: 0,
                    length: info.contiguous_length,
                }));
            }
            self.outgoing.push_back(msgs);
            self.synced = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        // ── Poll core events ───────────────────────────────────────────────────
        loop {
            match Pin::new(&mut self.core_events).poll_next(cx) {
                Poll::Ready(Some(event)) => self.on_core_event(event, inner),
                Poll::Ready(None) => break,
                Poll::Pending => break,
            }
        }

        // ── Drive pending_missing_nodes ────────────────────────────────────────
        if let Some((index, ref mut fut)) = self.pending_missing_nodes {
            let index = index;
            match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(nodes)) => {
                    self.pending_missing_nodes = None;
                    if self.state.remote_bitfield.get(index)
                        && self.state.remote_length > index
                    {
                        let info = inner.info();
                        self.outgoing.push_back(vec![Message::Request(Request {
                            id: index + 1,
                            fork: info.fork,
                            block: Some(RequestBlock { index, nodes }),
                            hash: None,
                            seek: None,
                            upgrade: None,
                            manifest: false,
                            priority: 42,
                        })]);
                        cx.waker().wake_by_ref();
                    }
                    if let Some(next) = self.pending_request_indices.pop_front() {
                        self.pending_missing_nodes = Some((
                            next,
                            inner.missing_nodes_from_merkle_tree_index(next),
                        ));
                        cx.waker().wake_by_ref();
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
        } else if let Some(index) = self.pending_request_indices.pop_front() {
            self.pending_missing_nodes = Some((
                index,
                inner.missing_nodes_from_merkle_tree_index(index),
            ));
            cx.waker().wake_by_ref();
        }

        // ── Drive pending_create_proof ─────────────────────────────────────────
        if let Some(ref mut fut) = self.pending_create_proof {
            match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(maybe_proof)) => {
                    let req_id = self.pending_create_proof_id;
                    let req_fork = self.pending_create_proof_fork;
                    self.pending_create_proof = None;
                    if let Some(proof) = maybe_proof {
                        self.outgoing.push_back(vec![Message::Data(Data {
                            request: req_id,
                            fork: req_fork,
                            hash: proof.hash,
                            block: proof.block,
                            seek: proof.seek,
                            upgrade: proof.upgrade,
                        })]);
                        cx.waker().wake_by_ref();
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
        }

        // ── Drive pending_verify_apply ─────────────────────────────────────────
        if let Some(ref mut fut) = self.pending_verify_apply {
            match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(_applied)) => {
                    self.pending_verify_apply = None;
                    if let Some(meta) = self.pending_data_meta.take() {
                        let next_index = if meta.has_upgrade {
                            (meta.pre_length < meta.remote_length).then_some(meta.pre_length)
                        } else {
                            meta.block_index.filter(|&i| i < meta.remote_length.saturating_sub(1))
                                .map(|i| i + 1)
                        };
                        if let Some(idx) = next_index {
                            self.pending_request_indices.push_front(idx);
                            cx.waker().wake_by_ref();
                        }
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
        }

        // ── Poll channel for incoming messages ─────────────────────────────────
        match Pin::new(&mut self.channel).poll_next(cx) {
            Poll::Ready(Some(message)) => {
                self.on_message(message, inner);
                cx.waker().wake_by_ref();
            }
            Poll::Ready(None) => return Poll::Ready(Ok(())),
            Poll::Pending => {}
        }

        Poll::Pending
    }

    fn on_message(&mut self, message: Message, inner: &HypercoreInner) {
        match message {
            Message::Synchronize(msg) => self.on_synchronize(msg, inner),
            Message::Request(msg) => {
                if self.pending_create_proof.is_none() {
                    self.pending_create_proof_id = msg.id;
                    self.pending_create_proof_fork = msg.fork;
                    self.pending_create_proof =
                        Some(inner.create_proof(msg.block, msg.hash, msg.seek, msg.upgrade));
                }
            }
            Message::Data(msg) => {
                if self.pending_verify_apply.is_none() {
                    let info = inner.info();
                    self.pending_data_meta = Some(DataMeta {
                        has_upgrade: msg.upgrade.is_some(),
                        pre_length: info.length,
                        remote_length: self.state.remote_length,
                        block_index: msg.block.as_ref().map(|b| b.index),
                    });
                    self.pending_verify_apply =
                        Some(inner.verify_and_apply_proof(msg.into_proof()));
                }
            }
            Message::Range(Range { start, length, .. }) => {
                self.state.remote_bitfield.set_range(start, length, true);
            }
            _ => {}
        }
    }

    fn on_synchronize(&mut self, msg: Synchronize, inner: &HypercoreInner) {
        let info = inner.info();
        let peer_length_changed = msg.length != self.state.remote_length;
        let first_sync = !self.state.remote_synced;
        let same_fork = msg.fork == info.fork;

        self.state.remote_fork = msg.fork;
        self.state.remote_length = msg.length;
        self.state.remote_can_upgrade = msg.can_upgrade;
        self.state.remote_uploading = msg.uploading;
        self.state.remote_downloading = msg.downloading;
        self.state.remote_synced = true;
        self.state.length_acked = if same_fork { msg.remote_length } else { 0 };

        let mut messages = vec![];
        if first_sync {
            messages.push(Message::Synchronize(Synchronize {
                fork: info.fork,
                length: info.length,
                remote_length: self.state.remote_length,
                can_upgrade: self.state.can_upgrade,
                uploading: true,
                downloading: true,
            }));
        }
        if self.state.remote_length > info.length
            && self.state.length_acked == info.length
            && peer_length_changed
        {
            messages.push(Message::Request(Request {
                id: 1,
                fork: info.fork,
                hash: None,
                block: None,
                seek: None,
                upgrade: Some(RequestUpgrade {
                    start: info.length,
                    length: self.state.remote_length - info.length,
                }),
                manifest: false,
                priority: 42,
            }));
        }
        if !messages.is_empty() {
            self.outgoing.push_back(messages);
        }
    }

    fn on_core_event(&mut self, event: events::Event, inner: &HypercoreInner) {
        match event {
            events::Event::Get(evt) => {
                if self.state.remote_length > evt.index
                    && self.state.remote_bitfield.get(evt.index)
                {
                    self.pending_request_indices.push_back(evt.index);
                }
            }
            events::Event::Have(evt) => {
                self.outgoing.push_back(vec![Message::Range(Range {
                    drop: evt.drop,
                    start: evt.start,
                    length: evt.length,
                })]);
            }
            events::Event::DataUpgrade(_) => {
                let info = inner.info();
                self.outgoing.push_back(vec![Message::Synchronize(Synchronize {
                    fork: info.fork,
                    length: info.length,
                    remote_length: self.state.remote_length,
                    downloading: true,
                    uploading: true,
                    can_upgrade: self.state.can_upgrade,
                })]);
            }
        }
    }
}

// ── Replicator ─────────────────────────────────────────────────────────────────

/// Drives replication for a single peer connection.
///
/// Created by [`Hypercore::replicate`]. Poll it as a `Future` to drive
/// replication; it resolves when the connection closes.
pub struct Replicator {
    inner: HypercoreInner,
    protocol: Protocol,
    discovery_key: [u8; 32],
    public_key: [u8; 32],
    pending_open: Option<Pin<Box<dyn Future<Output = io::Result<()>>>>>,
    channel_state: Option<ChannelState>,
}

impl std::fmt::Debug for Replicator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replicator")
            .field("discovery_key", &self.discovery_key)
            .finish()
    }
}

impl Replicator {
    fn new(inner: HypercoreInner, stream: impl CipherTrait + 'static) -> Self {
        let protocol = Protocol::new(Box::new(stream));
        let public_key = inner.key_pair().public.to_bytes();
        let discovery_key = discovery_key(&public_key);
        Self {
            inner,
            protocol,
            discovery_key,
            public_key,
            pending_open: None,
            channel_state: None,
        }
    }

    fn poll_replicator(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), HypercoreError>> {
        // Drive pending protocol.open()
        if let Some(ref mut fut) = self.pending_open {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => self.pending_open = None,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
                Poll::Pending => return Poll::Pending,
            }
        }

        // Poll protocol for the next handshake/channel event
        match Pin::new(&mut self.protocol).poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => {
                self.on_protocol_event(event);
                cx.waker().wake_by_ref();
            }
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e.into())),
            Poll::Ready(None) => return Poll::Ready(Ok(())),
            Poll::Pending => {}
        }

        // Drive channel state
        if let Some(ref mut cs) = self.channel_state {
            match cs.poll(cx, &self.inner) {
                Poll::Ready(Ok(())) => self.channel_state = None,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }

    fn on_protocol_event(&mut self, event: hypercore_protocol::Event) {
        match event {
            hypercore_protocol::Event::Handshake(_) => {
                if self.protocol.is_initiator() {
                    self.pending_open =
                        Some(Box::pin(self.protocol.open(self.public_key)));
                }
            }
            hypercore_protocol::Event::DiscoveryKey(dkey) => {
                if self.discovery_key == dkey {
                    self.pending_open =
                        Some(Box::pin(self.protocol.open(self.public_key)));
                } else {
                    warn!("Got discovery key for different core: {dkey:?}");
                }
            }
            hypercore_protocol::Event::Channel(channel) => {
                if self.discovery_key == *channel.discovery_key() {
                    let core_events = self.inner.event_subscribe();
                    self.channel_state = Some(ChannelState::new(channel, core_events));
                } else {
                    error!("Wrong discovery key?");
                }
            }
            hypercore_protocol::Event::Close(_) => {}
            _ => {}
        }
    }
}

impl Future for Replicator {
    type Output = Result<(), HypercoreError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_replicator(cx)
    }
}

// ── Hypercore::replicate ───────────────────────────────────────────────────────

impl Hypercore {
    /// Begin replicating with a remote peer over the given encrypted stream.
    ///
    /// Returns a [`Replicator`] that must be driven to completion (e.g. via
    /// `.await`) to perform replication. Multiple replicators can be active
    /// simultaneously.
    pub fn replicate(&self, stream: impl CipherTrait + 'static) -> Replicator {
        Replicator::new(self.inner.clone(), stream)
    }
}
