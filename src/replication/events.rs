//! events related to replication
use crate::{common::BitfieldUpdate, HypercoreError};
use tokio::sync::broadcast;

static MAX_EVENT_QUEUE_CAPACITY: usize = 32;

/// Event emeitted by on_append
#[derive(Debug, Clone)]
/// Emitted when [`Hypercore::get`] is called when the block is missing.
pub struct Get {
    /// Index of the requested block
    pub index: u64,
    /// when the block is gotton the emits an event
    pub get_result: broadcast::Sender<()>,
}

/// Emitted when
#[derive(Debug, Clone)]
pub struct DataUpgrade {}

/// Emitted when core gets new blocks
#[derive(Debug, Clone)]
pub struct Have {
    /// Start
    pub start: u64,
    /// TODO
    pub length: u64,
    /// TODO
    pub drop: bool,
}

impl From<&BitfieldUpdate> for Have {
    fn from(
        BitfieldUpdate {
            start,
            length,
            drop,
        }: &BitfieldUpdate,
    ) -> Self {
        Have {
            start: *start,
            length: *length,
            drop: *drop,
        }
    }
}

#[derive(Debug, Clone)]
/// Core events relevant to replication
pub enum Event {
    /// emmited when core.get(i) happens for a missing block
    Get(Get),
    /// emmitted when data.upgrade applied
    DataUpgrade(DataUpgrade),
    /// emmitted when core gets new blocks
    Have(Have),
}

/// derive From<msg> for Enum where enum variant and msg have the same name
macro_rules! impl_from_for_enum_variant {
    ($enum_name:ident, $variant_and_msg_name:ident) => {
        impl From<$variant_and_msg_name> for $enum_name {
            fn from(value: $variant_and_msg_name) -> Self {
                $enum_name::$variant_and_msg_name(value)
            }
        }
    };
}

impl_from_for_enum_variant!(Event, Get);
impl_from_for_enum_variant!(Event, DataUpgrade);
impl_from_for_enum_variant!(Event, Have);

#[derive(Debug)]
#[cfg(feature = "tokio")]
pub(crate) struct Events {
    /// Channel for core events
    pub(crate) channel: broadcast::Sender<Event>,
}

#[cfg(feature = "tokio")]
impl Events {
    pub(crate) fn new() -> Self {
        Self {
            channel: broadcast::channel(MAX_EVENT_QUEUE_CAPACITY).0,
        }
    }

    pub(crate) fn send<T: Into<Event>>(&self, evt: T) -> Result<(), HypercoreError> {
        // TODO error should be ignored it just means no replicator subscribed to events
        let _ = self.channel.send(evt.into());
        Ok(())
    }

    pub(crate) fn send_on_get(&self, index: u64) -> broadcast::Receiver<()> {
        let (tx, rx) = broadcast::channel(1);
        let _ = self.send(Get {
            index,
            get_result: tx,
        });
        rx
    }
}
