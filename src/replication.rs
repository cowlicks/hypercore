//! External interface for replication
use crate::{
    AppendOutcome, EventMsg, Hypercore, HypercoreError, Info, PartialKeypair, Proof, RequestBlock,
    RequestSeek, RequestUpgrade,
};
use tokio::sync::{broadcast::Receiver, Mutex};

use std::future::Future;
use std::sync::Arc;
/// Hypercore that can be shared across threads
#[derive(Debug, Clone)]
pub struct SharedCore(pub Arc<Mutex<Hypercore>>);

impl From<Hypercore> for SharedCore {
    fn from(core: Hypercore) -> Self {
        SharedCore(Arc::new(Mutex::new(core)))
    }
}
impl SharedCore {
    /// create a shared core from a hypercore
    pub fn from_hypercore(core: Hypercore) -> Self {
        SharedCore(Arc::new(Mutex::new(core)))
    }
}

/// Has [`Info`]
pub trait CoreInfo {
    /// Get info for the core
    /// Get core info (see: [`Hypercore::info`]
    fn info(&self) -> impl Future<Output = Info> + Send;
    /// Get the key_pair (see: [`Hypercore::key_pair`]
    fn key_pair(&self) -> impl Future<Output = PartialKeypair> + Send;
}

impl CoreInfo for SharedCore {
    fn info(&self) -> impl Future<Output = Info> + Send {
        async move {
            let core = &self.0.lock().await;
            core.info()
        }
    }

    fn key_pair(&self) -> impl Future<Output = PartialKeypair> + Send {
        async move {
            let core = &self.0.lock().await;
            core.key_pair().clone()
        }
    }
}

/// Error for ReplicationMethods trait
#[derive(thiserror::Error, Debug)]
pub enum ReplicationMethodsError {
    /// Error from hypercore
    #[error("Got a hypercore error: [{0}]")]
    HypercoreError(#[from] HypercoreError),
}

/// methods needed for replication
pub trait ReplicationMethods: CoreInfo + Send {
    /// ref Core::verify_and_apply_proof
    fn verify_and_apply_proof(
        &self,
        proof: &Proof,
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
    fn event_subscribe(&self) -> impl Future<Output = Receiver<EventMsg>>;
}

impl ReplicationMethods for SharedCore {
    fn verify_and_apply_proof(
        &self,
        proof: &Proof,
    ) -> impl Future<Output = Result<bool, ReplicationMethodsError>> {
        async move {
            let mut core = self.0.lock().await;
            Ok(core.verify_and_apply_proof(proof).await?)
        }
    }

    fn missing_nodes(
        &self,
        index: u64,
    ) -> impl Future<Output = Result<u64, ReplicationMethodsError>> {
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
    ) -> impl Future<Output = Result<Option<Proof>, ReplicationMethodsError>> {
        async move {
            let mut core = self.0.lock().await;
            Ok(core.create_proof(block, hash, seek, upgrade).await?)
        }
    }

    fn event_subscribe(&self) -> impl Future<Output = Receiver<EventMsg>> {
        async move { self.0.lock().await.event_subscribe() }
    }
}

/// Error for ReplicationMethods trait
#[derive(thiserror::Error, Debug)]
pub enum CoreMethodsError {
    /// Error from hypercore
    #[error("Got a hypercore error [{0}]")]
    HypercoreError(#[from] HypercoreError),
}

/// Things that consume Hypercore's can provide this interface to them
pub trait CoreMethods: CoreInfo {
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
}

impl CoreMethods for SharedCore {
    fn get(
        &self,
        index: u64,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, CoreMethodsError>> + Send {
        async move {
            let mut core = self.0.lock().await;
            Ok(core.get(index).await?)
        }
    }

    fn append(
        &self,
        data: &[u8],
    ) -> impl Future<Output = Result<AppendOutcome, CoreMethodsError>> + Send {
        async move {
            let mut core = self.0.lock().await;
            Ok(core.append(data).await?)
        }
    }
}
