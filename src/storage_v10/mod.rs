//! Save data to a desired storage backend.

use anyhow::{anyhow, Result};
use ed25519_dalek::{PublicKey, SecretKey};
use futures::future::FutureExt;
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::path::PathBuf;

use crate::common::{Store, StoreInfo, StoreInfoInstruction, StoreInfoType};

/// Key pair where for read-only hypercores the secret key can also be missing.
#[derive(Debug)]
pub struct PartialKeypair {
    /// Public key
    pub public: PublicKey,
    /// Secret key. If None, the hypercore is read-only.
    pub secret: Option<SecretKey>,
}

impl Clone for PartialKeypair {
    fn clone(&self) -> Self {
        let secret: Option<SecretKey> = match &self.secret {
            Some(secret) => {
                let bytes = secret.to_bytes();
                Some(SecretKey::from_bytes(&bytes).unwrap())
            }
            None => None,
        };
        PartialKeypair {
            public: self.public.clone(),
            secret,
        }
    }
}

/// Save data to a desired storage backend.
#[derive(Debug)]
pub struct Storage<T>
where
    T: RandomAccess + Debug,
{
    tree: T,
    data: T,
    bitfield: T,
    oplog: T,
}

impl<T> Storage<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    /// Create a new instance. Takes a callback to create new storage instances and overwrite flag.
    pub async fn open<Cb>(create: Cb, overwrite: bool) -> Result<Self>
    where
        Cb: Fn(Store) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send>>,
    {
        if overwrite {
            unimplemented!("Clearing storage not implemented");
        }
        let tree = create(Store::Tree).await?;
        let data = create(Store::Data).await?;
        let bitfield = create(Store::Bitfield).await?;
        let oplog = create(Store::Oplog).await?;

        let instance = Self {
            tree,
            data,
            bitfield,
            oplog,
        };

        Ok(instance)
    }

    /// Read info from store based on given instruction. Convenience method to `read_infos`.
    pub async fn read_info(&mut self, info_instruction: StoreInfoInstruction) -> Result<StoreInfo> {
        let mut infos = self.read_infos_to_vec(&[info_instruction]).await?;
        Ok(infos
            .pop()
            .expect("Should have gotten one info with one instruction"))
    }

    /// Read infos from stores based on given instructions
    pub async fn read_infos(
        &mut self,
        info_instructions: &[StoreInfoInstruction],
    ) -> Result<Box<[StoreInfo]>> {
        let infos = self.read_infos_to_vec(info_instructions).await?;
        Ok(infos.into_boxed_slice())
    }

    /// Reads infos but retains them as a Vec
    pub async fn read_infos_to_vec(
        &mut self,
        info_instructions: &[StoreInfoInstruction],
    ) -> Result<Vec<StoreInfo>> {
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
                    let length = match instruction.length {
                        Some(length) => length,
                        None => storage.len().await.map_err(|e| anyhow!(e))?,
                    };
                    let read_result = storage.read(instruction.index, length).await;
                    let info: StoreInfo = match read_result {
                        Ok(buf) => Ok(StoreInfo::new_content(
                            instruction.store.clone(),
                            instruction.index,
                            &buf,
                        )),
                        Err(e) => {
                            if instruction.allow_miss {
                                Ok(StoreInfo::new_content_miss(
                                    instruction.store.clone(),
                                    instruction.index,
                                ))
                            } else {
                                Err(anyhow!(e))
                            }
                        }
                    }?;
                    infos.push(info);
                }
                StoreInfoType::Size => {
                    let length = storage.len().await.map_err(|e| anyhow!(e))?;
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
    pub async fn flush_info(&mut self, slice: StoreInfo) -> Result<()> {
        self.flush_infos(&[slice]).await
    }

    /// Flush infos to storage
    pub async fn flush_infos(&mut self, infos: &[StoreInfo]) -> Result<()> {
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
                            storage
                                .write(info.index, &data.to_vec())
                                .await
                                .map_err(|e| anyhow!(e))?;
                        }
                    } else {
                        storage
                            .del(
                                info.index,
                                info.length.expect("When deleting, length must be given"),
                            )
                            .await
                            .map_err(|e| anyhow!(e))?;
                    }
                }
                StoreInfoType::Size => {
                    if info.miss {
                        storage.truncate(info.index).await.map_err(|e| anyhow!(e))?;
                    } else {
                        panic!("Flushing a size that isn't miss, is not supported");
                    }
                }
            }
        }
        Ok(())
    }

    fn get_random_access(&mut self, store: &Store) -> &mut T {
        match store {
            Store::Tree => &mut self.tree,
            Store::Data => &mut self.data,
            Store::Bitfield => &mut self.bitfield,
            Store::Oplog => &mut self.oplog,
        }
    }
}

impl Storage<RandomAccessMemory> {
    /// New storage backed by a `RandomAccessMemory` instance.
    pub async fn new_memory() -> Result<Self> {
        let create = |_| async { Ok(RandomAccessMemory::default()) }.boxed();
        // No reason to overwrite, as this is a new memory segment
        Ok(Self::open(create, false).await?)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Storage<RandomAccessDisk> {
    /// New storage backed by a `RandomAccessDisk` instance.
    pub async fn new_disk(dir: &PathBuf, overwrite: bool) -> Result<Self> {
        let storage = |store: Store| {
            let name = match store {
                Store::Tree => "tree",
                Store::Data => "data",
                Store::Bitfield => "bitfield",
                Store::Oplog => "oplog",
            };
            RandomAccessDisk::open(dir.as_path().join(name)).boxed()
        };
        Ok(Self::open(storage, overwrite).await?)
    }
}
