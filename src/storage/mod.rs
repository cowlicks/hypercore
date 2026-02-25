//! Save data to a desired storage backend.

use futures::future::FutureExt;
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::{RandomAccess, RandomAccessError};
use std::fmt::Debug;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
use tracing::instrument;

use crate::{
    HypercoreError,
    common::{Store, StoreInfo, StoreInfoInstruction, StoreInfoType},
};

/// Supertrait for Storage
pub trait StorageTraits: RandomAccess + Debug {}
impl<T: RandomAccess + Debug> StorageTraits for T {}

/// Save data to a desired storage backend.
#[derive(Debug)]
pub struct Storage {
    tree: Box<dyn StorageTraits + Send>,
    data: Box<dyn StorageTraits + Send>,
    bitfield: Box<dyn StorageTraits + Send>,
    oplog: Box<dyn StorageTraits + Send>,
}

impl Storage {
    /// Create a new instance. Takes a callback to create new storage instances and overwrite flag.
    pub async fn open<Cb>(create: Cb, overwrite: bool) -> Result<Self, HypercoreError>
    where
        Cb: Fn(
            Store,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Box<dyn StorageTraits + Send>, RandomAccessError>,
                    > + Send,
            >,
        >,
    {
        let tree = create(Store::Tree).await?;
        let data = create(Store::Data).await?;
        let bitfield = create(Store::Bitfield).await?;
        let oplog = create(Store::Oplog).await?;

        if overwrite {
            if tree.len() > 0 {
                tree.truncate(0).await?;
            }
            if data.len() > 0 {
                data.truncate(0).await?;
            }
            if bitfield.len() > 0 {
                bitfield.truncate(0).await?;
            }
            if oplog.len() > 0 {
                oplog.truncate(0).await?;
            }
        }

        let instance = Self {
            tree,
            data,
            bitfield,
            oplog,
        };

        Ok(instance)
    }

    /// Read info from store based on given instruction. Convenience method to `read_infos`.
    pub(crate) async fn read_info(
        &self,
        info_instruction: StoreInfoInstruction,
    ) -> Result<StoreInfo, HypercoreError> {
        let mut infos = self.read_infos_to_vec(&[info_instruction]).await?;
        Ok(infos
            .pop()
            .expect("Should have gotten one info with one instruction"))
    }

    /// Read infos from stores based on given instructions
    pub(crate) async fn read_infos(
        &self,
        info_instructions: &[StoreInfoInstruction],
    ) -> Result<Box<[StoreInfo]>, HypercoreError> {
        let infos = self.read_infos_to_vec(info_instructions).await?;
        Ok(infos.into_boxed_slice())
    }

    /// Reads infos but retains them as a Vec
    pub(crate) async fn read_infos_to_vec(
        &self,
        info_instructions: &[StoreInfoInstruction],
    ) -> Result<Vec<StoreInfo>, HypercoreError> {
        if info_instructions.is_empty() {
            return Ok(vec![]);
        }
        let mut current_store: Store = info_instructions[0].store.clone();
        let mut storage = self.get_random_access(&current_store);
        let mut infos: Vec<StoreInfo> = Vec::with_capacity(info_instructions.len());
        for instruction in info_instructions.iter() {
            if instruction.store != current_store {
                current_store = instruction.store.clone();
                storage = self.get_random_access(&current_store);
            }
            match instruction.info_type {
                StoreInfoType::Content => {
                    let read_length = match instruction.length {
                        Some(length) => length,
                        None => storage.len(),
                    };
                    let read_result = storage.read(instruction.index, read_length).await;
                    let info: StoreInfo = match read_result {
                        Ok(buf) => Ok(StoreInfo::new_content(
                            instruction.store.clone(),
                            instruction.index,
                            &buf,
                        )),
                        Err(RandomAccessError::OutOfBounds { length, .. }) => {
                            if instruction.allow_miss {
                                Ok(StoreInfo::new_content_miss(
                                    instruction.store.clone(),
                                    instruction.index,
                                ))
                            } else {
                                Err(HypercoreError::InvalidOperation {
                                    context: format!(
                                        "Could not read from store {}, index {} / length {} is out of bounds for store length {}",
                                        current_store, instruction.index, read_length, length
                                    ),
                                })
                            }
                        }
                        Err(e) => Err(e.into()),
                    }?;
                    infos.push(info);
                }
                StoreInfoType::Size => {
                    let length = storage.len();
                    infos.push(StoreInfo::new_size(
                        instruction.store.clone(),
                        instruction.index,
                        length - instruction.index,
                    ));
                }
            }
        }
        Ok(infos)
    }

    /// Flush info to storage. Convenience method to `flush_infos`.
    pub(crate) async fn flush_info(&self, slice: StoreInfo) -> Result<(), HypercoreError> {
        self.flush_infos(&[slice]).await
    }

    /// Flush infos to storage
    pub(crate) async fn flush_infos(&self, infos: &[StoreInfo]) -> Result<(), HypercoreError> {
        if infos.is_empty() {
            return Ok(());
        }
        let mut current_store: Store = infos[0].store.clone();
        let mut storage = self.get_random_access(&current_store);
        for info in infos.iter() {
            if info.store != current_store {
                current_store = info.store.clone();
                storage = self.get_random_access(&current_store);
            }
            match info.info_type {
                StoreInfoType::Content => {
                    if !info.miss {
                        if let Some(data) = &info.data {
                            storage.write(info.index, data).await?;
                        }
                    } else {
                        storage
                            .del(
                                info.index,
                                info.length.expect("When deleting, length must be given"),
                            )
                            .await?;
                    }
                }
                StoreInfoType::Size => {
                    if info.miss {
                        storage.truncate(info.index).await?;
                    } else {
                        panic!("Flushing a size that isn't miss, is not supported");
                    }
                }
            }
        }
        Ok(())
    }

    fn get_random_access(&self, store: &Store) -> &Box<dyn StorageTraits + Send> {
        match store {
            Store::Tree => &self.tree,
            Store::Data => &self.data,
            Store::Bitfield => &self.bitfield,
            Store::Oplog => &self.oplog,
        }
    }

    /// New storage backed by a `RandomAccessMemory` instance.
    #[instrument(err)]
    pub async fn new_memory() -> Result<Self, HypercoreError> {
        let create = |_| {
            async { Ok(Box::new(RandomAccessMemory::default()) as Box<dyn StorageTraits + Send>) }
                .boxed()
        };
        // No reason to overwrite, as this is a new memory segment
        Self::open(create, false).await
    }

    /// New storage backed by a `RandomAccessDisk` instance.
    #[cfg(not(target_arch = "wasm32"))]
    #[instrument(err)]
    pub async fn new_disk(dir: &PathBuf, overwrite: bool) -> Result<Self, HypercoreError> {
        let storage = |store: Store| {
            let dir = dir.clone();
            async move {
                let name = match store {
                    Store::Tree => "tree",
                    Store::Data => "data",
                    Store::Bitfield => "bitfield",
                    Store::Oplog => "oplog",
                };
                Ok(
                    Box::new(RandomAccessDisk::open(dir.as_path().join(name)).await?)
                        as Box<dyn StorageTraits + Send>,
                )
            }
            .boxed()
        };
        Self::open(storage, overwrite).await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::{StoreInfo, StoreInfoInstruction};

    #[tokio::test]
    async fn test_storage() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Storage::new_memory().await?;

        let data = b"hello hypercore";

        // Write to tree store
        storage
            .flush_info(StoreInfo::new_content(Store::Tree, 0, data))
            .await?;

        // Read it back
        let info = storage
            .read_info(StoreInfoInstruction::new_content(
                Store::Tree,
                0,
                data.len() as u64,
            ))
            .await?;

        assert_eq!(info.data.as_deref(), Some(data.as_slice()));

        // Write to two different stores, read back together
        storage
            .flush_infos(&[
                StoreInfo::new_content(Store::Data, 0, b"block0"),
                StoreInfo::new_content(Store::Bitfield, 0, b"bits"),
            ])
            .await?;

        let infos = storage
            .read_infos(&[
                StoreInfoInstruction::new_content(Store::Data, 0, 6),
                StoreInfoInstruction::new_content(Store::Bitfield, 0, 4),
            ])
            .await?;

        assert_eq!(infos[0].data.as_deref(), Some(b"block0".as_slice()));
        assert_eq!(infos[1].data.as_deref(), Some(b"bits".as_slice()));

        Ok(())
    }
}
