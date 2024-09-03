//! events related to replication
//!
use crate::{AppendOutcome, HypercoreError};
use tokio::sync::broadcast;

static MAX_EVENT_QUEUE_CAPACITY: usize = 32;

#[derive(Clone, Debug)]
/// Event emeitted by on_append
pub struct OnAppendEvent {
    /// outcome of append
    pub append_outcome: AppendOutcome,
    /// startind block index of append
    pub start: u64,
    /// length of apppend (in blocks )
    pub length: u64,
}

#[derive(Debug, Clone)]
/// Emitted when [`Hypercore::get`] is called when the block is missing.
pub struct OnGetEvent {
    /// Index of the requested block
    pub index: u64,
    /// when the block is gotton the emits an event
    pub get_result: broadcast::Sender<()>,
}

#[derive(Debug, Clone)]
/// Core events relative to the replicator
pub enum EventMsg {
    /// emmited when core.append happens
    OnAppend(OnAppendEvent),
    /// emmited when core.get(i) happens for a missing block
    OnGet(OnGetEvent),
}

impl From<OnAppendEvent> for EventMsg {
    fn from(value: OnAppendEvent) -> Self {
        EventMsg::OnAppend(value)
    }
}

impl From<OnGetEvent> for EventMsg {
    fn from(value: OnGetEvent) -> Self {
        EventMsg::OnGet(value)
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
