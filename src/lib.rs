#![forbid(unsafe_code, bad_style, future_incompatible)]
#![forbid(rust_2018_idioms, rust_2018_compatibility)]
#![forbid(missing_debug_implementations)]
#![forbid(missing_docs)]
#![warn(unreachable_pub)]
#![cfg_attr(test, deny(warnings))]
#![doc(test(attr(deny(warnings))))]

//! ## Introduction
//!
//! Hypercore is a secure, distributed append-only log. Built for sharing
//! large datasets and streams of real time data as part of the [Dat] project.
//! This is a rust port of [the original Javascript version][holepunch-hypercore]
//! aiming for interoperability with LTS version. The primary way to use this
//! crate is through the [Hypercore] struct, which can be created using the
//! [HypercoreBuilder].
//!
//! This crate supports WASM with `cargo build --target=wasm32-unknown-unknown`.
//!
//! ## Features
//!
//! ### `sparse` (default)
//!
//! When using disk storage, clearing values may create sparse files. On by default.
//!
//! ### `async-std` (default)
//!
//! Use the async-std runtime, on by default. Either this or `tokio` is mandatory.
//!
//! ### `tokio`
//!
//! Use the tokio runtime. Either this or `async_std` is mandatory.
//!
//! ### `cache`
//!
//! Use a moka cache for merkle tree nodes to speed-up reading.
//!
//! ## Example
//! ```rust
//! # #[cfg(feature = "tokio")]
//! # tokio_test::block_on(async {
//! # example().await;
//! # });
//! # #[cfg(feature = "async-std")]
//! # async_std::task::block_on(async {
//! # example().await;
//! # });
//! # async fn example() {
//! use hypercore::{HypercoreBuilder, Storage};
//!
//! // Create an in-memory hypercore using a builder
//! let mut hypercore = HypercoreBuilder::new(Storage::new_memory().await.unwrap())
//!     .build()
//!     .await
//!     .unwrap();
//!
//! // Append entries to the log
//! hypercore.append(b"Hello, ").await.unwrap();
//! hypercore.append(b"world!").await.unwrap();
//!
//! // Read entries from the log
//! assert_eq!(hypercore.get(0).await.unwrap().unwrap(), b"Hello, ");
//! assert_eq!(hypercore.get(1).await.unwrap().unwrap(), b"world!");
//! # }
//! ```
//!
//! Find more examples in the [examples] folder.
//!
//! [Dat]: https://github.com/datrs
//! [holepunch-hypercore]: https://github.com/holepunchto/hypercore
//! [Hypercore]: crate::core::Hypercore
//! [HypercoreBuilder]: crate::builder::HypercoreBuilder
//! [examples]: https://github.com/datrs/hypercore/tree/master/examples

pub mod encoding;
pub mod prelude;

mod bitfield;
mod builder;
mod common;
mod core;
mod crypto;
mod data;
mod oplog;
mod storage;
mod tree;

#[cfg(feature = "cache")]
pub use crate::builder::CacheOptionsBuilder;
pub use crate::builder::HypercoreBuilder;
pub use crate::common::{
    DataBlock, DataHash, DataSeek, DataUpgrade, HypercoreError, Node, Proof, RequestBlock,
    RequestSeek, RequestUpgrade, Store,
};
pub use crate::core::{AppendOutcome, Hypercore, Info};
pub use crate::crypto::{generate_signing_key, sign, verify, PartialKeypair};
pub use crate::storage::{Storage, StorageTraits};
pub use ed25519_dalek::{
    SecretKey, Signature, SigningKey, VerifyingKey, KEYPAIR_LENGTH, PUBLIC_KEY_LENGTH,
    SECRET_KEY_LENGTH,
};
use tokio::sync::Mutex;

use std::future::Future;
use std::sync::Arc;

/// Hypercore that can be shared across threads
#[derive(Debug, Clone)]
pub struct SharedCore(Arc<Mutex<Hypercore>>);

impl SharedCore {
    /// create a shared core from a hypercore
    pub fn from_hypercore(core: Hypercore) -> Self {
        SharedCore(Arc::new(Mutex::new(core)))
    }
}

/// methods taht replication needs
pub trait ReplicationMethods {
    /// TODO err
    type Error: std::error::Error;

    /// ref Core::verify_and_apply_proof
    fn verify_and_apply_proof(
        &self,
        proof: &Proof,
    ) -> impl Future<Output = Result<bool, Self::Error>>;
    /// ref Core::missing_nodes
    fn missing_nodes(&self, index: u64) -> impl Future<Output = Result<u64, Self::Error>>;
    /// ref Core::create_proof
    fn create_proof(
        &self,
        block: Option<RequestBlock>,
        hash: Option<RequestBlock>,
        seek: Option<RequestSeek>,
        upgrade: Option<RequestUpgrade>,
    ) -> impl Future<Output = Result<Option<Proof>, Self::Error>>;
}

impl ReplicationMethods for SharedCore {
    type Error = HypercoreError;
    fn verify_and_apply_proof(
        &self,
        proof: &Proof,
    ) -> impl Future<Output = Result<bool, HypercoreError>> {
        async move {
            let mut core = self.0.lock().await;
            Ok(core.verify_and_apply_proof(proof).await?)
        }
    }

    fn missing_nodes(&self, index: u64) -> impl Future<Output = Result<u64, HypercoreError>> {
        async move {
            let mut core = self.0.lock().await;
            Ok(core.missing_nodes(index).await?)
        }
    }

    fn create_proof(
        &self,
        block: Option<RequestBlock>,
        hash: Option<RequestBlock>,
        seek: Option<RequestSeek>,
        upgrade: Option<RequestUpgrade>,
    ) -> impl Future<Output = Result<Option<Proof>, HypercoreError>> {
        async move {
            let mut core = self.0.lock().await;
            Ok(core.create_proof(block, hash, seek, upgrade).await?)
        }
    }
}

/// Things that consume Hypercore's can provide this interface to them
pub trait CoreMethods {
    /// Errors from Hypercore results
    type Error: std::error::Error;

    /// get a block
    fn get(&self, index: u64) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;
    ///TODO rm result Get info for the core
    fn info(&self) -> impl Future<Output = Info> + Send;
    /// Append data to the core
    fn append(
        &self,
        data: &[u8],
    ) -> impl Future<Output = Result<AppendOutcome, HypercoreError>> + Send;
}

impl CoreMethods for SharedCore {
    type Error = HypercoreError;

    fn get(&self, index: u64) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send {
        async move {
            let mut core = self.0.lock().await;
            Ok(core.get(index).await?)
        }
    }

    fn info(&self) -> impl Future<Output = Info> + Send {
        async move {
            let core = &self.0.lock().await;
            core.info()
        }
    }

    fn append(
        &self,
        data: &[u8],
    ) -> impl Future<Output = Result<AppendOutcome, HypercoreError>> + Send {
        async move {
            let mut core = self.0.lock().await;
            Ok(core.append(data).await?)
        }
    }
}
