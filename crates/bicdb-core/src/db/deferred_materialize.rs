//! Experimental commit durability where the transaction log is the only
//! synchronous write before acknowledgment (`BICDB_COMMIT_DURABILITY=
//! transaction-log`). Off by default. See `docs/commit-durability-invariants.md`.
//!
//! A row commit's page `Commit` record is appended without a sync, and its
//! `CommitMaterialized` marker is queued here instead of being written. The
//! materializer later syncs the page WAL through the queued LSNs and only then
//! appends the markers, so a durable marker never claims pages that a crash
//! could still lose (invariant I3).
//!
//! Markers are written only for the contiguous applied prefix of commit
//! sequences, in sequence order. Every commit queues its marker before it
//! advances the applied watermark, so once the watermark reads `w`, every
//! commit at or below `w` that needs a marker has queued one. The durable
//! marker set therefore stays a commit-sequence prefix (I4), and recovery
//! replays an unmarked suffix in order over a state that contains the marked
//! prefix. A torn marker batch leaves a shorter prefix. A batch whose marker
//! write fails poisons the materializer: writing later markers past an
//! unwritten gap would break the prefix.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use super::TxLogHandle;
use crate::error::{BicDbError, Result};
use crate::paged_collection::PagedRecords;

/// Interval between background materializer passes.
fn materialize_interval() -> Duration {
    let millis = std::env::var("BICDB_DEFERRED_MATERIALIZE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10)
        .clamp(1, 10_000);
    Duration::from_millis(millis)
}

/// Whether the experimental transaction-log-only commit durability is
/// requested through the environment.
pub(crate) fn transaction_log_durability_default() -> bool {
    std::env::var("BICDB_COMMIT_DURABILITY").is_ok_and(|value| value == "transaction-log")
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DeferredMaterializeStats {
    pub passes: u64,
    pub wal_syncs: u64,
    pub markers_written: u64,
    pub queued: u64,
    pub poisoned: bool,
}

pub(crate) struct DeferredMaterializer {
    /// Queued markers by commit sequence: (page commit LSN, framed marker).
    pending: Mutex<BTreeMap<u64, (bicdb_page::Lsn, Vec<u8>)>>,
    /// Serializes passes, so two passes never write overlapping batches out
    /// of order.
    pass: Mutex<()>,
    paged: Arc<PagedRecords>,
    tx_log: TxLogHandle,
    applied_watermark: Arc<AtomicU64>,
    poisoned: AtomicBool,
    stop: Mutex<bool>,
    wake: Condvar,
    passes: AtomicU64,
    wal_syncs: AtomicU64,
    markers_written: AtomicU64,
}

impl DeferredMaterializer {
    pub(crate) fn new(
        paged: Arc<PagedRecords>,
        tx_log: TxLogHandle,
        applied_watermark: Arc<AtomicU64>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(BTreeMap::new()),
            pass: Mutex::new(()),
            paged,
            tx_log,
            applied_watermark,
            poisoned: AtomicBool::new(false),
            stop: Mutex::new(false),
            wake: Condvar::new(),
            passes: AtomicU64::new(0),
            wal_syncs: AtomicU64::new(0),
            markers_written: AtomicU64::new(0),
        })
    }

    /// Queue `commit_seq`'s marker. Must run before the commit advances the
    /// applied watermark.
    pub(crate) fn enqueue(&self, commit_seq: u64, lsn: bicdb_page::Lsn, marker: Vec<u8>) {
        self.pending.lock().insert(commit_seq, (lsn, marker));
    }

    /// Sync the page WAL through every queued commit at or below the applied
    /// watermark, then write their markers. Returns the number written.
    pub(crate) fn materialize(&self) -> Result<u64> {
        let _pass = self.pass.lock();
        self.passes.fetch_add(1, AtomicOrdering::Relaxed);
        let watermark = self.applied_watermark.load(AtomicOrdering::SeqCst);
        let batch = {
            let mut pending = self.pending.lock();
            let rest = pending.split_off(&watermark.saturating_add(1));
            std::mem::replace(&mut *pending, rest)
        };
        let Some(max_lsn) = batch.values().map(|(lsn, _)| *lsn).max() else {
            return Ok(0);
        };
        if let Err(error) = self.paged.store().wal().sync_through(max_lsn) {
            // Nothing was written: requeue, so a later pass writes the same
            // prefix once the log syncs.
            self.pending.lock().extend(batch);
            return Err(BicDbError::PagedStorage(format!(
                "deferred materialization could not sync the page log: {error}"
            )));
        }
        self.wal_syncs.fetch_add(1, AtomicOrdering::Relaxed);
        bicdb_page::test_faults::crash_point("materialize.after_wal_sync");
        if self.poisoned.load(AtomicOrdering::Acquire) {
            // Recovery replays every unmarked commit; markers are only an
            // optimization, so a poisoned materializer just stops writing them.
            return Ok(0);
        }
        let count = batch.len() as u64;
        let mut bytes = Vec::with_capacity(batch.values().map(|(_, m)| m.len()).sum());
        for (_, marker) in batch.values() {
            bytes.extend_from_slice(marker);
        }
        if bicdb_page::test_faults::crash_point_armed("materialize.torn_markers") {
            let _ = self.tx_log.append_auxiliary(&bytes[..bytes.len() / 2]);
        }
        bicdb_page::test_faults::crash_point("materialize.torn_markers");
        if let Err(error) = self.tx_log.append_auxiliary(&bytes) {
            self.poisoned.store(true, AtomicOrdering::Release);
            return Err(error);
        }
        self.markers_written
            .fetch_add(count, AtomicOrdering::Relaxed);
        bicdb_page::test_faults::crash_point("materialize.after_markers");
        Ok(count)
    }

    /// Barrier for anything that relies on every applied commit being
    /// page-durable: transaction-log cuts, sidecar writes, checkpoints,
    /// close. Syncs the whole page log even when no marker is queued.
    pub(crate) fn barrier(&self) -> Result<()> {
        self.materialize()?;
        self.paged
            .store()
            .wal()
            .sync()
            .map_err(|error| BicDbError::PagedStorage(error.to_string()))
    }

    pub(crate) fn stats(&self) -> DeferredMaterializeStats {
        DeferredMaterializeStats {
            passes: self.passes.load(AtomicOrdering::Relaxed),
            wal_syncs: self.wal_syncs.load(AtomicOrdering::Relaxed),
            markers_written: self.markers_written.load(AtomicOrdering::Relaxed),
            queued: self.pending.lock().len() as u64,
            poisoned: self.poisoned.load(AtomicOrdering::Acquire),
        }
    }

    fn run(self: Arc<Self>) {
        let interval = materialize_interval();
        loop {
            {
                let mut stop = self.stop.lock();
                if *stop {
                    return;
                }
                self.wake.wait_for(&mut stop, interval);
                if *stop {
                    return;
                }
            }
            if let Err(error) = self.materialize() {
                eprintln!("bicdb deferred materialization: {error}");
            }
        }
    }
}

impl super::BicDb {
    /// Make every applied commit page-durable and write its marker. A no-op
    /// unless the experimental transaction-log-only durability is on.
    pub(crate) fn deferred_materialize_barrier(&self) -> Result<()> {
        match &self.deferred_materializer {
            Some(handle) => handle.materializer.barrier(),
            None => Ok(()),
        }
    }

    /// Materializer counters, `None` unless the experimental durability is on.
    pub fn deferred_materialize_stats(&self) -> Option<DeferredMaterializeStats> {
        self.deferred_materializer
            .as_ref()
            .map(|handle| handle.materializer.stats())
    }
}

/// Owns the background thread; dropping it stops and joins the thread.
pub(crate) struct DeferredMaterializerHandle {
    pub(crate) materializer: Arc<DeferredMaterializer>,
    thread: Option<JoinHandle<()>>,
}

impl DeferredMaterializerHandle {
    pub(crate) fn start(materializer: Arc<DeferredMaterializer>) -> Self {
        let worker = materializer.clone();
        let thread = std::thread::Builder::new()
            .name("bicdb-materialize".into())
            .spawn(move || worker.run())
            .ok();
        Self {
            materializer,
            thread,
        }
    }
}

impl Drop for DeferredMaterializerHandle {
    fn drop(&mut self) {
        *self.materializer.stop.lock() = true;
        self.materializer.wake.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        // Best effort: a missing marker only means a longer replay.
        let _ = self.materializer.materialize();
    }
}

impl std::fmt::Debug for DeferredMaterializerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredMaterializerHandle")
            .field("stats", &self.materializer.stats())
            .finish()
    }
}
