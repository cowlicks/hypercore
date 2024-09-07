//! events related to replication
use crate::{common::BitfieldUpdate, AppendOutcome, DataBlock, HypercoreError};
use tokio::sync::broadcast;

static MAX_EVENT_QUEUE_CAPACITY: usize = 32;

/// Event emeitted by on_append
#[derive(Debug, Clone)]
/// Emitted when [`Hypercore::get`] is called when the block is missing.
pub struct OnGetEvent {
    /// Index of the requested block
    pub index: u64,
    /// when the block is gotton the emits an event
    pub get_result: broadcast::Sender<()>,
}

/// When an upgrade is applied
#[derive(Debug, Clone)]
pub struct OnDataUpgradeEvent {}

/// When an upgrade is applied
/// rename it to onhave
#[derive(Debug, Clone)]
pub struct OnHaveEvent {
    /// TODO
    pub start: u64,
    /// TODO
    pub length: u64,
    /// TODO
    pub drop: bool,
}

impl From<&BitfieldUpdate> for OnHaveEvent {
    fn from(
        BitfieldUpdate {
            start,
            length,
            drop,
        }: &BitfieldUpdate,
    ) -> Self {
        OnHaveEvent {
            start: *start,
            length: *length,
            drop: *drop,
        }
    }
}

#[derive(Debug, Clone)]
/// Core events relative to the replicator
pub enum EventMsg {
    /// emmited when core.get(i) happens for a missing block
    OnGet(OnGetEvent),
    /// emmitted when data.upgrade applied
    OnDataUgrade(OnDataUpgradeEvent),
    /// emmitted when core gets new blocks
    OnHave(OnHaveEvent),
}

impl From<OnGetEvent> for EventMsg {
    fn from(value: OnGetEvent) -> Self {
        EventMsg::OnGet(value)
    }
}

impl From<OnDataUpgradeEvent> for EventMsg {
    fn from(value: OnDataUpgradeEvent) -> Self {
        EventMsg::OnDataUgrade(value)
    }
}
impl From<OnHaveEvent> for EventMsg {
    fn from(value: OnHaveEvent) -> Self {
        EventMsg::OnHave(value)
    }
}

#[derive(Debug)]
#[cfg(feature = "tokio")]
pub(crate) struct Events {
    /// Channel for core events
    pub(crate) channel: broadcast::Sender<EventMsg>,
}

#[cfg(feature = "tokio")]
impl Events {
    pub(crate) fn new() -> Self {
        Self {
            channel: broadcast::channel(MAX_EVENT_QUEUE_CAPACITY).0,
        }
    }

    pub(crate) fn send<T: Into<EventMsg>>(&self, evt: T) -> Result<(), HypercoreError> {
        // TODO error should be ignored it just means no replicator subscribed to events
        let _ = self.channel.send(evt.into());
        Ok(())
    }

    pub(crate) fn send_on_get(&self, index: u64) -> broadcast::Receiver<()> {
        let (tx, rx) = broadcast::channel(1);
        let _ = self.send(OnGetEvent {
            index,
            get_result: tx,
        });
        rx
    }
}
