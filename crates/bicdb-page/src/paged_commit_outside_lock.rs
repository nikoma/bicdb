//! Row commits whose page-log sync runs outside the store's `write_lock`.
//!
//! EXPERIMENTAL, off by default (`PagedStoreOptions::commit_sync_outside_lock`).
//! Design and qualification: `docs/commit-fsync-outside-locks.md`.
//!
//! A row commit appends its page images and its `Commit` record under the
//! writer lock, registers as **committing** and releases the lock
//! ([`PagedStore::commit_append`]). Outside the lock it waits for the page log
//! to be durable through its record, sharing the `fdatasync` with every other
//! committer, and only then is it **published** (marked committed in page
//! MVCC), always as a prefix in commit-record LSN order
//! ([`PagedStore::await_commit`]).
//!
//! Three rules keep this equivalent to the locked commit:
//!
//! - **Durable before visible.** Publication pops only entries at or below
//!   `durable_lsn`, and nothing after a durability failure.
//! - **Committing barrier.** Every other writer-lock holder (checkpoints,
//!   maintenance, non-row page transactions, backup capture) first drains
//!   the queue, so it sees the state it sees today: no committing
//!   transaction. Checkpoint truncation additionally refuses to run while
//!   the queue is non-empty.
//! - **Overlap safety net.** A row commit that touches a key a committing
//!   transaction wrote waits for that transaction's publication instead of
//!   raising a write conflict. Core row locks make this unreachable; the
//!   counter `overlap_waits` proves it in qualification.

use std::sync::atomic::Ordering;

use rustc_hash::{FxHashMap, FxHasher};

use super::PagedStore;
use crate::error::{PageError, Result};
use crate::mvcc::{Snapshot, TxStatus, Xid};
use crate::wal::Lsn;

/// Committing transactions, ordered by their `Commit` record LSN.
#[derive(Debug, Default)]
pub(crate) struct CommittingQueue {
    by_lsn: std::collections::BTreeMap<Lsn, Xid>,
    lsn_of: FxHashMap<Xid, Lsn>,
    /// Key hash -> committing writer, for the overlap safety net.
    keys: FxHashMap<u64, Xid>,
    keys_of: FxHashMap<Xid, Vec<u64>>,
    /// Live (not yet appended) row-commit transactions and the key hashes
    /// they wrote so far.
    live: FxHashMap<Xid, Vec<u64>>,
}

/// Counters for qualification and observability.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CommittingStats {
    /// Row commits that registered as committing.
    pub enqueued: u64,
    /// Committing transactions published (made visible).
    pub published: u64,
    /// Publications done by a thread other than the committer.
    pub published_by_other: u64,
    /// Times a writer-lock holder drained the queue (the barrier).
    pub barrier_drains: u64,
    /// Overlap safety-net waits. Expected 0 under core row locks.
    pub overlap_waits: u64,
    /// Largest queue depth seen.
    pub max_depth: u64,
    /// Entries currently committing.
    pub depth: u64,
}

#[derive(Debug, Default)]
pub(crate) struct CommittingCounters {
    pub(crate) count: std::sync::atomic::AtomicU64,
    enqueued: std::sync::atomic::AtomicU64,
    published: std::sync::atomic::AtomicU64,
    published_by_other: std::sync::atomic::AtomicU64,
    barrier_drains: std::sync::atomic::AtomicU64,
    overlap_waits: std::sync::atomic::AtomicU64,
    max_depth: std::sync::atomic::AtomicU64,
}

/// What [`PagedStore::commit_append`] hands back: the commit to wait for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitTicket {
    pub xid: Xid,
    pub lsn: Lsn,
    /// Already durable and published (the free-adoption fallback).
    pub published: bool,
}

fn key_hash(key: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = FxHasher::default();
    hasher.write(key);
    hasher.finish()
}

impl PagedStore {
    /// Whether this store runs row-commit syncs outside the writer lock.
    pub fn commit_sync_outside_lock(&self) -> bool {
        self.commit_sync_outside_lock
    }

    /// Counters of the committing queue. All zero when the gate is off.
    pub fn committing_stats(&self) -> CommittingStats {
        let c = &self.committing_counters;
        CommittingStats {
            enqueued: c.enqueued.load(Ordering::Acquire),
            published: c.published.load(Ordering::Acquire),
            published_by_other: c.published_by_other.load(Ordering::Acquire),
            barrier_drains: c.barrier_drains.load(Ordering::Acquire),
            overlap_waits: c.overlap_waits.load(Ordering::Acquire),
            max_depth: c.max_depth.load(Ordering::Acquire),
            depth: c.count.load(Ordering::Acquire),
        }
    }

    pub(crate) fn committing_is_empty(&self) -> bool {
        self.committing_counters.count.load(Ordering::Acquire) == 0
    }

    /// The writer lock for everything except a row commit. Gate off: exactly
    /// `write_lock.lock()`. Gate on, at outermost depth: also drain the
    /// committing queue (the barrier), so the holder never coexists with a
    /// committing transaction.
    pub(crate) fn writer_lock(&self) -> parking_lot::ReentrantMutexGuard<'_, ()> {
        if !self.commit_sync_outside_lock {
            return self.write_lock.lock();
        }
        let outermost = !self.write_lock.is_owned_by_current_thread();
        let guard = self.write_lock.lock();
        if outermost {
            self.drain_committing();
        }
        guard
    }

    /// The writer lock for a core row commit: no barrier, because the row
    /// commit is the thing that may run alongside committing transactions.
    pub fn row_commit_guard(&self) -> parking_lot::ReentrantMutexGuard<'_, ()> {
        self.write_lock.lock()
    }

    /// Begin a row-commit page transaction (gate on). Its writes are tracked
    /// for the overlap safety net.
    ///
    /// Every transaction committing when it begins is ordered before it: the
    /// host decided that order (their durable decisions and page work came
    /// first, under the same lock), and with the locked commit this
    /// transaction would only have begun after their publication. So they
    /// are removed from its snapshot's `in_flight` set. They still count as
    /// committed only once published (`TxStatus::Committed`), which is what
    /// the overlap safety net waits for.
    pub fn begin_row_commit(&self) -> Xid {
        let xid = self.begin();
        if self.commit_sync_outside_lock {
            let mut queue = self.committing.lock();
            queue.live.insert(xid, Vec::new());
            if !queue.by_lsn.is_empty() {
                let mut snapshots = self.snapshots.lock();
                if let Some(snapshot) = snapshots.get_mut(&xid) {
                    let mut in_flight = (*snapshot.in_flight).clone();
                    for committing in queue.by_lsn.values() {
                        in_flight.remove(committing);
                    }
                    *snapshot = Snapshot {
                        xid: snapshot.xid,
                        xmax: snapshot.xmax,
                        in_flight: std::sync::Arc::new(in_flight),
                    };
                }
            }
        }
        xid
    }

    /// Resolve every committing transaction. Called with the writer lock
    /// held, so no new entry can appear. Stops (leaving the queue) on a
    /// durability failure: the store is fenced and nothing may publish.
    pub(crate) fn drain_committing(&self) {
        if self.committing_is_empty() {
            return;
        }
        self.committing_counters
            .barrier_drains
            .fetch_add(1, Ordering::AcqRel);
        loop {
            let max = self
                .committing
                .lock()
                .by_lsn
                .last_key_value()
                .map(|(lsn, _)| *lsn);
            let Some(max) = max else { break };
            if self.wal.sync_through(max).is_err() {
                break;
            }
            self.publish_durable(None);
            if self.wal.health().check().is_err() {
                break;
            }
        }
        crate::test_faults::crash_point("commit.barrier.drained");
    }

    /// Publish the durable prefix of the queue, in LSN order. Publishes
    /// nothing once the store's durability has failed.
    fn publish_durable(&self, own: Option<Xid>) {
        let mut queue = self.committing.lock();
        if self.wal.health().check().is_err() {
            return;
        }
        let durable = self.wal.durable_lsn();
        while let Some((&lsn, &xid)) = queue.by_lsn.first_key_value() {
            if lsn > durable {
                break;
            }
            queue.by_lsn.remove(&lsn);
            queue.lsn_of.remove(&xid);
            if let Some(hashes) = queue.keys_of.remove(&xid) {
                for hash in hashes {
                    if queue.keys.get(&hash) == Some(&xid) {
                        queue.keys.remove(&hash);
                    }
                }
            }
            self.transactions.commit(xid);
            self.snapshots.lock().remove(&xid);
            self.committing_counters
                .published
                .fetch_add(1, Ordering::AcqRel);
            if own != Some(xid) {
                self.committing_counters
                    .published_by_other
                    .fetch_add(1, Ordering::AcqRel);
            }
        }
        self.committing_counters
            .count
            .store(queue.by_lsn.len() as u64, Ordering::Release);
    }

    /// Record a row commit's write of `key` and wait out any committing
    /// writer of the same key (the overlap safety net). Caller holds the
    /// writer lock.
    pub(crate) fn note_row_commit_write(&self, transaction: Xid, key: &[u8]) -> Result<()> {
        if !self.commit_sync_outside_lock {
            return Ok(());
        }
        let hash = key_hash(key);
        let other = {
            let mut queue = self.committing.lock();
            let Some(live) = queue.live.get_mut(&transaction) else {
                return Ok(());
            };
            live.push(hash);
            queue.keys.get(&hash).copied()
        };
        if let Some(other) = other {
            self.wait_for_committing(other)?;
        }
        Ok(())
    }

    /// For a "decided before xmax" read (`xid == 0`, empty `in_flight`, the
    /// shape the host uses for pre-image reads): wait for a committing writer
    /// of `key`, so the read does not miss a decided commit.
    pub(crate) fn wait_for_committing_key_read(&self, snapshot: &Snapshot, key: &[u8]) -> Result<()> {
        if !self.commit_sync_outside_lock
            || self.committing_is_empty()
            || snapshot.xid != 0
            || !snapshot.in_flight.is_empty()
        {
            return Ok(());
        }
        let other = self.committing.lock().keys.get(&key_hash(key)).copied();
        match other {
            Some(other) if other < snapshot.xmax => self.wait_for_committing(other),
            _ => Ok(()),
        }
    }

    fn wait_for_committing(&self, other: Xid) -> Result<()> {
        let lsn = self.committing.lock().lsn_of.get(&other).copied();
        let Some(lsn) = lsn else {
            return Ok(()); // already published
        };
        self.committing_counters
            .overlap_waits
            .fetch_add(1, Ordering::AcqRel);
        self.wal.sync_through(lsn)?;
        self.publish_durable(None);
        self.wal.health().check()?;
        if self.transactions.status(other) != TxStatus::Committed {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!("committing transaction {other} was not published"),
            });
        }
        Ok(())
    }

    /// Forget a live row commit that aborted.
    pub(crate) fn forget_row_commit(&self, transaction: Xid) {
        if self.commit_sync_outside_lock {
            self.committing.lock().live.remove(&transaction);
        }
    }

    /// Append a row commit's page images and `Commit` record and register it
    /// as committing. Caller holds [`Self::row_commit_guard`] and releases it
    /// before [`Self::await_commit`]. Gate off: the ordinary synced commit.
    pub fn commit_append(&self, transaction: Xid) -> Result<CommitTicket> {
        if !self.commit_sync_outside_lock {
            let lsn = self.commit_with_sync(transaction, true)?;
            return Ok(CommitTicket {
                xid: transaction,
                lsn,
                published: true,
            });
        }
        let _guard = self.write_lock.lock();
        let result = (|| {
            let free_adoption = self.prepare_free_adoption(transaction, &[])?;
            self.log_dirty_pages(transaction)?;
            if free_adoption.is_empty() {
                let lsn = self.wal.commit_unsynced(transaction)?;
                self.enqueue_committing(transaction, lsn);
                return Ok(CommitTicket {
                    xid: transaction,
                    lsn,
                    published: false,
                });
            }
            // A commit that frees pages keeps the synced path under the lock
            // (free-map adoption must not run ahead of the log), and publishes
            // through the queue so visibility stays in LSN order.
            let lsn = self.wal.commit(transaction)?;
            self.enqueue_committing(transaction, lsn);
            self.pool.flush_all()?;
            self.adopt_free_segments(&free_adoption)?;
            self.publish_durable(Some(transaction));
            self.require_published(transaction)?;
            Ok(CommitTicket {
                xid: transaction,
                lsn,
                published: true,
            })
        })();
        if result.is_err() {
            self.forget_row_commit(transaction);
        }
        result
    }

    fn enqueue_committing(&self, transaction: Xid, lsn: Lsn) {
        let mut queue = self.committing.lock();
        let hashes = queue.live.remove(&transaction).unwrap_or_default();
        for hash in &hashes {
            queue.keys.insert(*hash, transaction);
        }
        queue.keys_of.insert(transaction, hashes);
        queue.by_lsn.insert(lsn, transaction);
        queue.lsn_of.insert(transaction, lsn);
        let depth = queue.by_lsn.len() as u64;
        self.committing_counters.count.store(depth, Ordering::Release);
        self.committing_counters
            .enqueued
            .fetch_add(1, Ordering::AcqRel);
        self.committing_counters
            .max_depth
            .fetch_max(depth, Ordering::AcqRel);
    }

    fn require_published(&self, transaction: Xid) -> Result<()> {
        // After a durability failure nothing is reported committed, even a
        // commit a successful sync covered before the failure.
        self.wal.health().check()?;
        if self.transactions.status(transaction) == TxStatus::Committed {
            return Ok(());
        }
        Err(PageError::InvalidMaintenanceLimits {
            reason: format!("committing transaction {transaction} was not published"),
        })
    }

    /// Wait, without any store lock, until the ticket's commit is durable and
    /// published. An error means the outcome is unknown: the record may be
    /// on disk. The entry is then never published on this handle.
    pub fn await_commit(&self, ticket: CommitTicket) -> Result<()> {
        if ticket.published {
            // Gate off: `commit_with_sync` already ran the maintenance step.
            if !self.commit_sync_outside_lock {
                return Ok(());
            }
            return self.after_commit_maintenance();
        }
        debug_assert!(
            !self.write_lock.is_owned_by_current_thread(),
            "await_commit must run outside the writer lock"
        );
        crate::test_faults::crash_point("commit.page.appended_unlocked");
        self.wal.sync_through(ticket.lsn)?;
        crate::test_faults::crash_point("commit.page.synced_unpublished");
        self.publish_durable(Some(ticket.xid));
        self.require_published(ticket.xid)?;
        crate::test_faults::crash_point("commit.page.published");
        self.after_commit_maintenance()
    }
}
