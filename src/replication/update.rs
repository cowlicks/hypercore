//! Fetching the newest state of a core from its peers.
//!
//! See [`Hypercore::update`].

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::Stream as _;

use crate::{
    Hypercore, HypercoreError, Info,
    core::inner::{BackgroundFuture, HypercoreInner},
    replication::{PeerSyncState, events},
};

/// Options for [`Hypercore::update`].
#[derive(Debug, Clone)]
pub struct UpdateOptions {
    /// Wait for peers to connect and tell us their length.
    ///
    /// When `false`, [`Hypercore::update`] answers from what currently-connected peers have
    /// already said and never blocks on a peer arriving — so with no peers it resolves
    /// `Ok(false)` immediately.
    ///
    /// Defaults to `true`.
    pub wait: bool,
    /// Run even on a writable core, which otherwise resolves `Ok(false)` without doing
    /// anything. Defaults to `false`.
    pub force: bool,
    /// Give up after this long and return [`HypercoreError::Timeout`].
    ///
    /// `None` (the default) means wait indefinitely. Note that `wait: true` with no timeout
    /// and no peers really does block forever, exactly like Javascript's
    /// `core.update({ wait: true })`.
    pub timeout: Option<Duration>,
}

impl Default for UpdateOptions {
    fn default() -> Self {
        Self {
            wait: true,
            force: false,
            timeout: None,
        }
    }
}

/// State captured on the first poll, against which "did we grow?" is judged.
struct Snapshot {
    fork: u64,
    length: u64,
}

/// Future returned by [`Hypercore::update`].
///
/// Resolves `Ok(true)` when the core's verified length (or fork) moved, `Ok(false)` when the
/// peers confirmed there is nothing newer, and [`HypercoreError::Timeout`] when
/// [`UpdateOptions::timeout`] elapsed with nobody having answered.
pub struct UpdateFuture {
    inner: HypercoreInner,
    opts: UpdateOptions,
    /// `None` until the first poll, which is also what makes the first poll special.
    snapshot: Option<Snapshot>,
    events: Option<async_broadcast::Receiver<events::Event>>,
    /// Replicator attached via [`Hypercore::attach_replicator`], if any. Driven in-band, the
    /// same way [`crate::Hypercore::get`] does it.
    background: BackgroundFuture,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl std::fmt::Debug for UpdateFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateFuture")
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

impl UpdateFuture {
    pub(crate) fn new(inner: HypercoreInner, opts: UpdateOptions) -> Self {
        let background = inner.background.clone();
        Self {
            inner,
            opts,
            snapshot: None,
            events: None,
            background,
            sleep: None,
        }
    }

    /// Drive an attached replicator, so that `update` works under
    /// [`Hypercore::attach_replicator`] as well as under an externally driven one.
    ///
    /// `Ok(None)` means no replicator is attached — replication, if any, is driven elsewhere
    /// and we can conclude nothing from that. `Ok(Some(true))` means one was attached and has
    /// now run to completion, so no further peer will ever appear through it.
    fn poll_background(&self, cx: &mut Context<'_>) -> Result<Option<bool>, HypercoreError> {
        let mut background = self.background.lock().unwrap();
        let Some(fut) = background.as_mut() else {
            return Ok(None);
        };
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => {
                *background = None;
                Ok(Some(true))
            }
            Poll::Ready(Err(e)) => Err(e),
            Poll::Pending => Ok(Some(false)),
        }
    }

    /// Drain pending events. Payloads are ignored on purpose: the event channel drops
    /// messages under load, so events are treated purely as "something changed, look again".
    fn drain_events(&mut self, cx: &mut Context<'_>) {
        let Some(events) = self.events.as_mut() else {
            return;
        };
        while let Poll::Ready(Some(_)) = Pin::new(&mut *events).poll_next(cx) {}
    }

    /// Whether some peer might still upgrade us, i.e. whether it is too early to answer
    /// `false`. A port of Javascript's `_checkUpgradeIfAvailable` (`lib/replicator.js`).
    ///
    /// Javascript only consults the first `MAX_PEERS_UPGRADE` (3) peers; we check them all,
    /// since a core here has a handful of peers at most.
    fn any_peer_may_upgrade(&self, peers: &[Arc<PeerSyncState>], info: &Info) -> bool {
        let snapshot = self.snapshot.as_ref().expect("snapshot taken on first poll");
        for peer in peers {
            // Hasn't told us anything yet. Under `wait: false` we don't hang around for it.
            if !peer.remote_synced() {
                if self.opts.wait {
                    return true;
                }
                continue;
            }
            // We have nothing at all and the peer does: it can certainly upgrade us.
            if info.length == 0 && peer.remote_length() > 0 {
                return true;
            }
            // This peer has nothing we don't already have, or is on another fork.
            if peer.remote_length() <= snapshot.length || peer.remote_fork() != snapshot.fork {
                continue;
            }
            // It hasn't acked our current length, so its `can_upgrade` claim is stale.
            if peer.length_acked() != info.length && peer.remote_fork() == info.fork {
                return true;
            }
            if peer.remote_can_upgrade() || peer.upgrade_inflight() {
                return true;
            }
        }
        false
    }
}

impl Future for UpdateFuture {
    type Output = Result<bool, HypercoreError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // ── First poll ─────────────────────────────────────────────────────────
        if this.snapshot.is_none() {
            let info = this.inner.info();
            if info.writeable && !this.opts.force {
                return Poll::Ready(Ok(false));
            }
            // Subscribe before reading any state, so no wake-up can slip between the two.
            this.events = Some(this.inner.event_subscribe());
            this.snapshot = Some(Snapshot {
                fork: info.fork,
                length: info.length,
            });
            // Nudge replicators that are connected but idle into re-checking whether they
            // should ask for an upgrade.
            this.inner.send_event(events::Upgrade {});
            this.sleep = this
                .opts
                .timeout
                .map(|timeout| Box::pin(tokio::time::sleep(timeout)));
        }

        let background = match this.poll_background(cx) {
            Ok(background) => background,
            Err(e) => return Poll::Ready(Err(e)),
        };

        this.drain_events(cx);

        // ── Did we grow? ───────────────────────────────────────────────────────
        let info = this.inner.info();
        let snapshot = this.snapshot.as_ref().expect("set above");
        if info.length != snapshot.length || info.fork != snapshot.fork {
            return Poll::Ready(Ok(true));
        }

        // ── Could anyone still grow us? ────────────────────────────────────────
        let peers = this.inner.peers();
        if peers.is_empty() && !this.opts.wait {
            return Poll::Ready(Ok(false));
        }
        if !peers.is_empty() && !this.any_peer_may_upgrade(&peers, &info) {
            return Poll::Ready(Ok(false));
        }

        // An attached replicator that ran to completion means every connection it owned has
        // closed, so no peer is ever going to show up through it.
        if background == Some(true) && peers.is_empty() {
            return Poll::Ready(Ok(false));
        }

        // ── Give up? ───────────────────────────────────────────────────────────
        if let Some(sleep) = this.sleep.as_mut()
            && sleep.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(HypercoreError::Timeout {
                context: "Hypercore::update timed out waiting for a peer".to_string(),
            }));
        }

        Poll::Pending
    }
}

impl Hypercore {
    /// Fetch the newest state of this core from its peers.
    ///
    /// Resolves `Ok(true)` if the core's verified length grew, `Ok(false)` if the peers
    /// confirmed there is nothing newer (or the core is writable — pass
    /// [`UpdateOptions::force`] to update one anyway), and [`HypercoreError::Timeout`] if
    /// [`UpdateOptions::timeout`] elapsed before any peer answered.
    ///
    /// This is the "is there anything new?" primitive: it asks peers to grow this core's
    /// *verified* length, which is a different operation from downloading blocks. Use
    /// [`Hypercore::get`] for the latter.
    ///
    /// Replication must be driven by someone: either a spawned [`Replicator`](super::Replicator)
    /// / channel future, or one attached with [`Hypercore::attach_replicator`], in which case
    /// this future drives it while it waits.
    ///
    /// ```rust,ignore
    /// // Block until a peer connects and tells us where it is, but no longer than 5s.
    /// let grew = core.update(UpdateOptions {
    ///     timeout: Some(Duration::from_secs(5)),
    ///     ..Default::default()
    /// }).await?;
    /// ```
    pub fn update(&self, opts: UpdateOptions) -> UpdateFuture {
        UpdateFuture::new(self.inner.clone(), opts)
    }

    /// The number of peers currently replicating this core.
    pub fn peer_count(&self) -> usize {
        self.inner.peers().len()
    }
}
