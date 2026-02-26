use ed25519_dalek::Signature;
use futures::future::Either;
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
pub(crate) struct HypercoreInner {
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

impl HypercoreInner {
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
                                let infos =
                                    storage.read_infos(Vec::from(instructions)).await?;
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

    pub(crate) async fn flush_bitfield_and_tree_and_oplog(
        &mut self,
        clear_traces: bool,
    ) -> Result<(), HypercoreError> {
        let infos = self.bitfield.flush();
        self.storage.flush_infos(Vec::from(infos)).await?;
        let infos = self.tree.flush();
        self.storage.flush_infos(Vec::from(infos)).await?;
        let infos = self.oplog.flush(&self.header, clear_traces)?;
        self.storage.flush_infos(Vec::from(infos)).await?;
        Ok(())
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
