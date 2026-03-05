use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use ed25519_dalek::Signature;
use futures::future::Either;
use random_access_storage::BoxFuture;
use std::sync::Mutex;
use tracing::instrument;

use crate::{
    bitfield::Bitfield,
    common::{BitfieldUpdate, HypercoreError, NodeByteRange, StoreInfo, ValuelessProof},
    crypto::{PartialKeypair, generate_signing_key},
    data::BlockStore,
    oplog::{Header, MAX_OPLOG_ENTRIES_BYTE_SIZE, Oplog},
    storage::Storage,
    tree::{MerkleTree, MerkleTreeChangeset},
};
use hypercore_schema::{Proof, RequestBlock, RequestSeek, RequestUpgrade};

use super::{AppendOutcome, HypercoreOptions, Info};

#[derive(Debug)]
pub(crate) struct HypercoreInnerInner {
    pub(crate) key_pair: PartialKeypair,
    pub(crate) storage: Storage,
    pub(crate) oplog: Oplog,
    pub(crate) tree: MerkleTree,
    pub(crate) block_store: BlockStore,
    pub(crate) bitfield: Bitfield,
    pub(crate) skip_flush_count: u8,
    pub(crate) header: Header,
    #[cfg(feature = "replication")]
    pub(crate) events: crate::replication::events::Events,
}

impl HypercoreInnerInner {
    pub(crate) async fn new(
        storage: Storage,
        mut options: HypercoreOptions,
    ) -> Result<Self, HypercoreError> {
        let key_pair: Option<PartialKeypair> = if options.open {
            if options.key_pair.is_some() {
                return Err(HypercoreError::BadArgument {
                    context: "Key pair can not be used when building an openable hypercore"
                        .to_string(),
                });
            }
            None
        } else {
            Some(options.key_pair.take().unwrap_or_else(|| {
                let signing_key = generate_signing_key();
                PartialKeypair {
                    public: signing_key.verifying_key(),
                    secret: Some(signing_key),
                }
            }))
        };

        // Open/create oplog
        let mut oplog_open_outcome = match Oplog::open(&key_pair, None)? {
            Either::Right(value) => value,
            Either::Left(instruction) => {
                let info = storage.read_info(instruction).await?;
                match Oplog::open(&key_pair, Some(info))? {
                    Either::Right(value) => value,
                    Either::Left(_) => {
                        return Err(HypercoreError::InvalidOperation {
                            context: "Could not open oplog".to_string(),
                        });
                    }
                }
            }
        };
        storage
            .flush_infos(Vec::from(oplog_open_outcome.infos_to_flush))
            .await?;

        // Open/create tree
        let mut tree = match MerkleTree::open(
            &oplog_open_outcome.header.tree,
            None,
            #[cfg(feature = "cache")]
            &options.node_cache_options,
        )? {
            Either::Right(value) => value,
            Either::Left(instructions) => {
                let infos = storage.read_infos(Vec::from(instructions)).await?;
                match MerkleTree::open(
                    &oplog_open_outcome.header.tree,
                    Some(&infos),
                    #[cfg(feature = "cache")]
                    &options.node_cache_options,
                )? {
                    Either::Right(value) => value,
                    Either::Left(_) => {
                        return Err(HypercoreError::InvalidOperation {
                            context: "Could not open tree".to_string(),
                        });
                    }
                }
            }
        };

        // Create block store instance
        let block_store = BlockStore::default();

        // Open bitfield
        let mut bitfield = match Bitfield::open(None) {
            Either::Right(value) => value,
            Either::Left(instruction) => {
                let info = storage.read_info(instruction).await?;
                match Bitfield::open(Some(info)) {
                    Either::Right(value) => value,
                    Either::Left(instruction) => {
                        let info = storage.read_info(instruction).await?;
                        match Bitfield::open(Some(info)) {
                            Either::Right(value) => value,
                            Either::Left(_) => {
                                return Err(HypercoreError::InvalidOperation {
                                    context: "Could not open bitfield".to_string(),
                                });
                            }
                        }
                    }
                }
            }
        };

        // Process entries stored only to the oplog and not yet flushed into bitfield or tree
        if let Some(entries) = oplog_open_outcome.entries {
            for entry in entries.iter() {
                for node in &entry.tree_nodes {
                    tree.add_node(node.clone());
                }

                if let Some(bitfield_update) = &entry.bitfield {
                    bitfield.update(bitfield_update);
                    update_contiguous_length(
                        &mut oplog_open_outcome.header,
                        &bitfield,
                        bitfield_update,
                    );
                }
                if let Some(tree_upgrade) = &entry.tree_upgrade {
                    let mut changeset =
                        match tree.truncate(tree_upgrade.length, tree_upgrade.fork, None)? {
                            Either::Right(value) => value,
                            Either::Left(instructions) => {
                                let infos = storage.read_infos(Vec::from(instructions)).await?;
                                match tree.truncate(
                                    tree_upgrade.length,
                                    tree_upgrade.fork,
                                    Some(&infos),
                                )? {
                                    Either::Right(value) => value,
                                    Either::Left(_) => {
                                        return Err(HypercoreError::InvalidOperation {
                                            context: format!(
                                                "Could not truncate tree to length {}",
                                                tree_upgrade.length
                                            ),
                                        });
                                    }
                                }
                            }
                        };
                    changeset.ancestors = tree_upgrade.ancestors;
                    changeset.hash = Some(changeset.hash());
                    changeset.signature =
                        Some(Signature::try_from(&*tree_upgrade.signature).map_err(|_| {
                            HypercoreError::InvalidSignature {
                                context: "Could not parse changeset signature".to_string(),
                            }
                        })?);

                    oplog_open_outcome.oplog.update_header_with_changeset(
                        &changeset,
                        None,
                        &mut oplog_open_outcome.header,
                    )?;

                    tree.commit(changeset)?;
                }
            }
        }

        let oplog = oplog_open_outcome.oplog;
        let header = oplog_open_outcome.header;
        let key_pair = header.key_pair.clone();

        Ok(Self {
            key_pair,
            storage,
            oplog,
            tree,
            block_store,
            bitfield,
            header,
            skip_flush_count: 0,
            #[cfg(feature = "replication")]
            events: crate::replication::events::Events::new(),
        })
    }

    pub(crate) fn info(&self) -> Info {
        Info {
            length: self.tree.length,
            byte_length: self.tree.byte_length,
            contiguous_length: self.header.hints.contiguous_length,
            fork: self.tree.fork,
            writeable: self.key_pair.secret.is_some(),
        }
    }

    pub(crate) fn key_pair(&self) -> &PartialKeypair {
        &self.key_pair
    }

    #[instrument(ret, skip(self))]
    pub(crate) fn has(&self, index: u64) -> bool {
        self.bitfield.get(index)
    }

    #[cfg(feature = "replication")]
    pub(crate) fn event_subscribe(
        &self,
    ) -> async_broadcast::Receiver<crate::replication::events::Event> {
        self.events.channel.new_receiver()
    }

    pub(crate) fn append_outcome(&self) -> AppendOutcome {
        AppendOutcome {
            length: self.tree.length,
            byte_length: self.tree.byte_length,
        }
    }

    pub(crate) fn should_flush_bitfield_and_tree_and_oplog(&mut self) -> bool {
        if self.skip_flush_count == 0
            || self.oplog.entries_byte_length >= MAX_OPLOG_ENTRIES_BYTE_SIZE
        {
            self.skip_flush_count = 3;
            true
        } else {
            self.skip_flush_count -= 1;
            false
        }
    }

    pub(crate) fn flush_bitfield_and_tree_and_oplog(
        &mut self,
        clear_traces: bool,
    ) -> BoxFuture<Result<(), HypercoreError>> {
        let mut infos = vec![];
        infos.extend(self.bitfield.flush());
        infos.extend(self.tree.flush());
        match self.oplog.flush(&self.header, clear_traces) {
            Ok(opinfo) => infos.extend(opinfo),
            Err(e) => return Box::pin(async { Err(e) }),
        }

        self.storage.flush_infos(infos)
    }

    pub(crate) async fn verify_proof(
        &self,
        proof: &Proof,
    ) -> Result<MerkleTreeChangeset, HypercoreError> {
        match self.tree.verify_proof(proof, &self.key_pair.public, None)? {
            Either::Right(value) => Ok(value),
            Either::Left(instructions) => {
                let infos = self
                    .storage
                    .read_infos_to_vec(Vec::from(instructions))
                    .await?;
                match self
                    .tree
                    .verify_proof(proof, &self.key_pair.public, Some(&infos))?
                {
                    Either::Right(value) => Ok(value),
                    Either::Left(_) => Err(HypercoreError::InvalidOperation {
                        context: "Could not verify proof from tree".to_string(),
                    }),
                }
            }
        }
    }

    #[instrument(err, skip(self))]
    pub(crate) async fn missing_nodes_from_merkle_tree_index(
        &self,
        merkle_tree_index: u64,
    ) -> Result<u64, HypercoreError> {
        match self.tree.missing_nodes(merkle_tree_index, None)? {
            Either::Right(value) => Ok(value),
            Either::Left(instructions) => {
                let mut instructions = instructions;
                let mut infos: Vec<StoreInfo> = vec![];
                loop {
                    infos.extend(
                        self.storage
                            .read_infos_to_vec(Vec::from(instructions))
                            .await?,
                    );
                    match self.tree.missing_nodes(merkle_tree_index, Some(&infos))? {
                        Either::Right(value) => {
                            return Ok(value);
                        }
                        Either::Left(new_instructions) => {
                            instructions = new_instructions;
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn byte_range(
        &self,
        index: u64,
        initial_infos: Option<&[StoreInfo]>,
    ) -> Result<NodeByteRange, HypercoreError> {
        match self.tree.byte_range(index, initial_infos)? {
            Either::Right(value) => Ok(value),
            Either::Left(instructions) => {
                let mut instructions = instructions;
                let mut infos: Vec<StoreInfo> = vec![];
                loop {
                    infos.extend(
                        self.storage
                            .read_infos_to_vec(Vec::from(instructions))
                            .await?,
                    );
                    match self.tree.byte_range(index, Some(&infos))? {
                        Either::Right(value) => {
                            return Ok(value);
                        }
                        Either::Left(new_instructions) => {
                            instructions = new_instructions;
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn create_valueless_proof(
        &self,
        block: Option<RequestBlock>,
        hash: Option<RequestBlock>,
        seek: Option<RequestSeek>,
        upgrade: Option<RequestUpgrade>,
    ) -> Result<ValuelessProof, HypercoreError> {
        match self.tree.create_valueless_proof(
            block.as_ref(),
            hash.as_ref(),
            seek.as_ref(),
            upgrade.as_ref(),
            None,
        )? {
            Either::Right(value) => Ok(value),
            Either::Left(instructions) => {
                let mut instructions = instructions;
                let mut infos: Vec<StoreInfo> = vec![];
                loop {
                    infos.extend(
                        self.storage
                            .read_infos_to_vec(Vec::from(instructions))
                            .await?,
                    );
                    match self.tree.create_valueless_proof(
                        block.as_ref(),
                        hash.as_ref(),
                        seek.as_ref(),
                        upgrade.as_ref(),
                        Some(&infos),
                    )? {
                        Either::Right(value) => {
                            return Ok(value);
                        }
                        Either::Left(new_instructions) => {
                            instructions = new_instructions;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct HypercoreInner {
    pub(crate) inner: Arc<Mutex<HypercoreInnerInner>>,
}

impl HypercoreInner {
    pub(crate) async fn new(
        storage: Storage,
        options: HypercoreOptions,
    ) -> Result<Self, HypercoreError> {
        Ok(Self {
            inner: Arc::new(Mutex::new(
                HypercoreInnerInner::new(storage, options).await?,
            )),
        })
    }
    pub(crate) fn info(&self) -> Info {
        self.inner.lock().unwrap().info()
    }
    pub(crate) fn key_pair(&self) -> PartialKeypair {
        self.inner.lock().unwrap().key_pair().clone()
    }

    pub(crate) fn has(&self, index: u64) -> bool {
        self.inner.lock().unwrap().has(index)
    }

    #[cfg(feature = "replication")]
    pub(crate) fn event_subscribe(
        &self,
    ) -> async_broadcast::Receiver<crate::replication::events::Event> {
        self.inner.lock().unwrap().event_subscribe()
    }
    pub(crate) fn append_outcome(&self) -> AppendOutcome {
        self.inner.lock().unwrap().append_outcome()
    }
    pub(crate) fn should_flush_bitfield_and_tree_and_oplog(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .should_flush_bitfield_and_tree_and_oplog()
    }
    pub(crate) fn flush_bitfield_and_tree_and_oplog(
        &self,
        clear_traces: bool,
    ) -> BoxFuture<Result<(), HypercoreError>> {
        self.inner
            .lock()
            .unwrap()
            .flush_bitfield_and_tree_and_oplog(clear_traces)
    }

    pub(crate) fn verify_proof(&self, proof: Proof) -> VerifyProofFuture {
        VerifyProofFuture {
            inner: self.inner.clone(),
            proof,
            infos: None,
            pending_read: None,
        }
    }
    pub(crate) fn missing_nodes_from_merkle_tree_index(
        &self,
        merkle_tree_index: u64,
    ) -> MissingNodesFuture {
        MissingNodesFuture {
            inner: self.inner.clone(),
            merkle_tree_index,
            infos: Vec::new(),
            pending_read: None,
        }
    }
    pub(crate) fn byte_range(&self, index: u64, initial_infos: Vec<StoreInfo>) -> ByteRangeFuture {
        ByteRangeFuture {
            inner: self.inner.clone(),
            index,
            infos: initial_infos,
            pending_read: None,
        }
    }

    pub(crate) fn get(&self, index: u64) -> GetFuture {
        GetFuture {
            inner: self.inner.clone(),
            index,
            byte_range_fut: None,
            byte_range: None,
            block_read_fut: None,
        }
    }

    pub(crate) fn create_valueless_proof(
        &self,
        block: Option<RequestBlock>,
        hash: Option<RequestBlock>,
        seek: Option<RequestSeek>,
        upgrade: Option<RequestUpgrade>,
    ) -> ValuelessProofFuture {
        ValuelessProofFuture {
            inner: self.inner.clone(),
            block,
            hash,
            seek,
            upgrade,
            infos: Vec::new(),
            pending_read: None,
        }
    }

    pub(crate) fn create_proof(
        &self,
        block: Option<RequestBlock>,
        hash: Option<RequestBlock>,
        seek: Option<RequestSeek>,
        upgrade: Option<RequestUpgrade>,
    ) -> CreateProofFuture {
        CreateProofFuture {
            inner: self.inner.clone(),
            block,
            hash,
            seek,
            upgrade,
            valueless_proof_fut: None,
            valueless_proof: None,
            get_fut: None,
        }
    }
}

pub(crate) struct CreateProofFuture {
    inner: Arc<Mutex<HypercoreInnerInner>>,
    block: Option<RequestBlock>,
    hash: Option<RequestBlock>,
    seek: Option<RequestSeek>,
    upgrade: Option<RequestUpgrade>,
    // Phase 1: build the proof structure (without block data)
    valueless_proof_fut: Option<ValuelessProofFuture>,
    valueless_proof: Option<ValuelessProof>,
    // Phase 2: fetch the block value (only when proof.block is Some)
    get_fut: Option<GetFuture>,
}

impl Future for CreateProofFuture {
    type Output = Result<Option<Proof>, HypercoreError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // Phase 2: fetch block value.
            if let Some(fut) = this.get_fut.as_mut() {
                match Pin::new(fut).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(data)) => {
                        this.get_fut = None;
                        let vp = this.valueless_proof.take().unwrap();
                        return match data {
                            // Block not present locally — can't serve the proof.
                            None => Poll::Ready(Ok(None)),
                            Some(bytes) => {
                                Poll::Ready(Ok(Some(vp.into_proof(Some(bytes.into_vec())))))
                            }
                        };
                    }
                }
            }

            // Phase 1: build the valueless proof.
            if let Some(fut) = this.valueless_proof_fut.as_mut() {
                match Pin::new(fut).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(vp)) => {
                        this.valueless_proof_fut = None;
                        if let Some(block) = vp.block.as_ref() {
                            let index = block.index;
                            this.valueless_proof = Some(vp);
                            this.get_fut = Some(GetFuture {
                                inner: this.inner.clone(),
                                index,
                                byte_range_fut: None,
                                byte_range: None,
                                block_read_fut: None,
                            });
                            continue;
                        }
                        return Poll::Ready(Ok(Some(vp.into_proof(None))));
                    }
                }
            }

            // Initial: start the valueless proof future.
            this.valueless_proof_fut = Some(ValuelessProofFuture {
                inner: this.inner.clone(),
                block: this.block.take(),
                hash: this.hash.take(),
                seek: this.seek.take(),
                upgrade: this.upgrade.take(),
                infos: Vec::new(),
                pending_read: None,
            });
        }
    }
}

pub(crate) struct GetFuture {
    inner: Arc<Mutex<HypercoreInnerInner>>,
    index: u64,
    // Phase 1: resolve byte range
    byte_range_fut: Option<ByteRangeFuture>,
    // Phase 2: read block from storage (only needed if block_store has no cached value)
    byte_range: Option<NodeByteRange>,
    block_read_fut: Option<BoxFuture<Result<StoreInfo, HypercoreError>>>,
}

impl Future for GetFuture {
    type Output = Result<Option<Box<[u8]>>, HypercoreError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // TODO: we really need to generalize the Either response stack
        loop {
            // Phase 2: storage read for block data (highest priority when active).
            if let Some(fut) = this.block_read_fut.as_mut() {
                match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(info)) => {
                        this.block_read_fut = None;
                        let inner = this.inner.lock().unwrap();
                        let byte_range = this.byte_range.as_ref().unwrap();
                        return match inner.block_store.read(byte_range, Some(info)) {
                            Either::Right(data) => Poll::Ready(Ok(Some(data))),
                            Either::Left(_) => Poll::Ready(Err(HypercoreError::InvalidOperation {
                                context: "Could not read block storage range".to_string(),
                            })),
                        };
                    }
                }
            }

            // Phase 1: resolve the byte range.
            if let Some(fut) = this.byte_range_fut.as_mut() {
                match Pin::new(fut).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(byte_range)) => {
                        this.byte_range_fut = None;
                        let inner = this.inner.lock().unwrap();
                        match inner.block_store.read(&byte_range, None) {
                            Either::Right(data) => return Poll::Ready(Ok(Some(data))),
                            Either::Left(instruction) => {
                                let storage = inner.storage.clone();
                                this.block_read_fut = Some(storage.read_info(instruction));
                                this.byte_range = Some(byte_range);
                                // Loop to poll block_read_fut immediately.
                            }
                        }
                    }
                }
                continue;
            }

            // Initial: check bitfield, then start byte range resolution.
            {
                let inner = this.inner.lock().unwrap();
                if !inner.bitfield.get(this.index) {
                    #[cfg(feature = "replication")]
                    inner.events.send_on_get(this.index);
                    return Poll::Ready(Ok(None));
                }
            }
            this.byte_range_fut = Some(ByteRangeFuture {
                inner: this.inner.clone(),
                index: this.index,
                infos: Vec::new(),
                pending_read: None,
            });
            // Loop to poll byte_range_fut immediately.
        }
    }
}

pub(crate) struct MissingNodesFuture {
    inner: Arc<Mutex<HypercoreInnerInner>>,
    merkle_tree_index: u64,
    infos: Vec<StoreInfo>,
    pending_read: Option<BoxFuture<Result<Vec<StoreInfo>, HypercoreError>>>,
}

impl Future for MissingNodesFuture {
    type Output = Result<u64, HypercoreError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            if let Some(fut) = this.pending_read.as_mut() {
                match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(new_infos)) => {
                        this.infos.extend(new_infos);
                        this.pending_read = None;
                    }
                }
            }

            let result = {
                let inner = this.inner.lock().unwrap();
                let infos_opt = if this.infos.is_empty() {
                    None
                } else {
                    Some(this.infos.as_slice())
                };
                inner.tree.missing_nodes(this.merkle_tree_index, infos_opt)
                // Lock is dropped here.
            };

            match result {
                Err(e) => return Poll::Ready(Err(e)),
                Ok(Either::Right(value)) => return Poll::Ready(Ok(value)),
                Ok(Either::Left(instructions)) => {
                    let storage = this.inner.lock().unwrap().storage.clone();
                    this.pending_read = Some(storage.read_infos_to_vec(Vec::from(instructions)));
                }
            }
        }
    }
}

pub(crate) struct ByteRangeFuture {
    inner: Arc<Mutex<HypercoreInnerInner>>,
    index: u64,
    infos: Vec<StoreInfo>,
    pending_read: Option<BoxFuture<Result<Vec<StoreInfo>, HypercoreError>>>,
}

impl Future for ByteRangeFuture {
    type Output = Result<NodeByteRange, HypercoreError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            if let Some(fut) = this.pending_read.as_mut() {
                match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(new_infos)) => {
                        this.infos.extend(new_infos);
                        this.pending_read = None;
                    }
                }
            }

            let result = {
                let inner = this.inner.lock().unwrap();
                let infos_opt = if this.infos.is_empty() {
                    None
                } else {
                    Some(this.infos.as_slice())
                };
                inner.tree.byte_range(this.index, infos_opt)
                // Lock is dropped here.
            };

            match result {
                Err(e) => return Poll::Ready(Err(e)),
                Ok(Either::Right(value)) => return Poll::Ready(Ok(value)),
                Ok(Either::Left(instructions)) => {
                    let storage = this.inner.lock().unwrap().storage.clone();
                    this.pending_read = Some(storage.read_infos_to_vec(Vec::from(instructions)));
                }
            }
        }
    }
}

pub(crate) struct VerifyProofFuture {
    inner: Arc<Mutex<HypercoreInnerInner>>,
    proof: Proof,
    // None = first attempt (no read done yet), Some = read completed
    infos: Option<Vec<StoreInfo>>,
    pending_read: Option<BoxFuture<Result<Vec<StoreInfo>, HypercoreError>>>,
}

impl Future for VerifyProofFuture {
    type Output = Result<MerkleTreeChangeset, HypercoreError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // Phase 1: if there's a pending storage read, drive it to completion.
            if let Some(fut) = this.pending_read.as_mut() {
                match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(infos)) => {
                        this.infos = Some(infos);
                        this.pending_read = None;
                        // Fall through to retry verify_proof.
                    }
                }
            }

            // Phase 2: call tree.verify_proof synchronously under the lock.
            let result = {
                let inner = this.inner.lock().unwrap();
                let public_key = inner.key_pair.public;
                let infos_opt = this.infos.as_deref();
                inner.tree.verify_proof(&this.proof, &public_key, infos_opt)
                // Lock is dropped here.
            };

            match result {
                Err(e) => return Poll::Ready(Err(e)),
                Ok(Either::Right(value)) => return Poll::Ready(Ok(value)),
                Ok(Either::Left(_)) if this.infos.is_some() => {
                    // We already read infos and still got Left — the proof can't
                    // be satisfied, which is an error.
                    return Poll::Ready(Err(HypercoreError::InvalidOperation {
                        context: "Could not verify proof from tree".to_string(),
                    }));
                }
                Ok(Either::Left(instructions)) => {
                    let storage = this.inner.lock().unwrap().storage.clone();
                    this.pending_read = Some(storage.read_infos_to_vec(Vec::from(instructions)));
                    // Loop to poll the new future immediately.
                }
            }
        }
    }
}

pub(crate) struct ValuelessProofFuture {
    inner: Arc<Mutex<HypercoreInnerInner>>,
    block: Option<RequestBlock>,
    hash: Option<RequestBlock>,
    seek: Option<RequestSeek>,
    upgrade: Option<RequestUpgrade>,
    infos: Vec<StoreInfo>,
    pending_read: Option<BoxFuture<Result<Vec<StoreInfo>, HypercoreError>>>,
}

impl Future for ValuelessProofFuture {
    type Output = Result<ValuelessProof, HypercoreError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // ValuelessProofFuture is Unpin (all fields are Unpin), so this is safe.
        let this = self.get_mut();

        loop {
            // Phase 1: if there's a pending storage read, drive it to completion.
            if let Some(fut) = this.pending_read.as_mut() {
                match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(new_infos)) => {
                        this.infos.extend(new_infos);
                        this.pending_read = None;
                        // Fall through to retry create_valueless_proof.
                    }
                }
            }

            // Phase 2: call tree.create_valueless_proof synchronously under the lock.
            let result = {
                let inner = this.inner.lock().unwrap();
                let infos_opt = if this.infos.is_empty() {
                    None
                } else {
                    Some(this.infos.as_slice())
                };
                inner.tree.create_valueless_proof(
                    this.block.as_ref(),
                    this.hash.as_ref(),
                    this.seek.as_ref(),
                    this.upgrade.as_ref(),
                    infos_opt,
                )
                // Lock is dropped here.
            };

            match result {
                Err(e) => return Poll::Ready(Err(e)),
                Ok(Either::Right(value)) => return Poll::Ready(Ok(value)),
                Ok(Either::Left(instructions)) => {
                    // Need more nodes from storage. Clone storage (cheap Arc clone)
                    // outside the lock so we don't hold it across the async read.
                    let storage = this.inner.lock().unwrap().storage.clone();
                    this.pending_read = Some(storage.read_infos_to_vec(Vec::from(instructions)));
                    // Loop to poll the new future immediately.
                }
            }
        }
    }
}

pub(crate) fn update_contiguous_length(
    header: &mut Header,
    bitfield: &Bitfield,
    bitfield_update: &BitfieldUpdate,
) {
    let end = bitfield_update.start + bitfield_update.length;
    let mut c = header.hints.contiguous_length;
    if bitfield_update.drop {
        if c <= end && c > bitfield_update.start {
            c = bitfield_update.start;
        }
    } else if c <= end && c >= bitfield_update.start {
        c = end;
        while bitfield.get(c) {
            c += 1;
        }
    }

    if c != header.hints.contiguous_length {
        header.hints.contiguous_length = c;
    }
}
