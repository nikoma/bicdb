//! Live commit-stream replication and synchronous quorum commit.
//!
//! A primary ships every commit sequence number to its standbys, in order and
//! with no gaps: commits that wrote nothing (read-only transactions, revoked
//! commits) travel as empty frames, because a standby applies a strictly
//! contiguous sequence. Frames are built from the durable transaction log.
//! [`ReplicationCursor`] remembers where a sender stopped, so each round reads
//! only what was appended since; the cursor re-verifies the frame it stopped
//! at, so a log truncated by a checkpoint is detected and rescanned instead of
//! being read at a stale offset.
//!
//! Only commit sequence numbers that are both durable and fully applied are
//! shipped. The persisted commit-sequence floor tells a fresh scan which
//! missing sequence numbers were truncated away (a standby that far behind
//! needs a snapshot) and which simply wrote nothing.
//!
//! [`SyncQuorum`] holds a commit's acknowledgement until enough standbys have
//! durably applied it (RPO = 0). Callers wait *after* releasing database
//! locks: senders need a read lock to build frames.
use super::*;
use std::sync::atomic::AtomicBool;

thread_local! {
    static THREAD_COMMIT_SEQ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Records that the current thread committed `seq` (called by every commit).
pub(crate) fn note_thread_commit(seq: u64) {
    THREAD_COMMIT_SEQ.with(|cell| cell.set(cell.get().max(seq)));
}

/// The highest commit sequence number the current thread committed since the
/// last call, if any. A server clears it before handling a client message and
/// reads it before acknowledging, so synchronous replication covers every
/// commit path (explicit COMMIT, autocommit statements, DDL) uniformly.
pub fn take_thread_commit_seq() -> Option<u64> {
    let seq = THREAD_COMMIT_SEQ.with(|cell| cell.replace(0));
    (seq > 0).then_some(seq)
}

/// Offset of the first frame belonging to a commit after `seq` (its first
/// write, or the commit frame), or the log length when there is none.
fn first_frame_offset_after(path: &Path, seq: u64, encryption: &EncryptionRuntime) -> Result<u64> {
    let frames = storage::read_frames_readonly_from(path, 0, FrameKind::Transaction, encryption)?;
    let mut first_offset_of_tx: FxHashMap<u64, u64> = FxHashMap::default();
    let mut earliest: Option<u64> = None;
    let mut end = 0;
    for (start, frame_end, payload) in &frames {
        end = *frame_end;
        match decode_tx_frame(payload)? {
            TxFrame::Write { tx_id, .. } | TxFrame::Truncate { tx_id, .. } => {
                first_offset_of_tx.entry(tx_id.0).or_insert(*start);
            }
            TxFrame::Commit {
                tx_id, commit_seq, ..
            } if commit_seq > seq => {
                let offset = first_offset_of_tx.get(&tx_id.0).copied().unwrap_or(*start);
                earliest = Some(earliest.map_or(offset, |current: u64| current.min(offset)));
            }
            _ => {}
        }
    }
    Ok(earliest.unwrap_or(end))
}

/// Wakes replication senders when the durable sequence advances.
#[derive(Debug, Default)]
pub(crate) struct DurableNotifier {
    durable: Mutex<u64>,
    changed: Condvar,
}

impl DurableNotifier {
    pub(crate) fn publish(&self, seq: u64) {
        let mut durable = self.durable.lock();
        if seq > *durable {
            *durable = seq;
        }
        drop(durable);
        self.changed.notify_all();
    }

    fn wait_beyond(&self, seq: u64, timeout: Duration) {
        let mut durable = self.durable.lock();
        if *durable <= seq {
            self.changed.wait_for(&mut durable, timeout);
        }
    }
}

/// Lock-free handle a replication sender waits on between rounds.
#[derive(Clone, Debug)]
pub struct ReplicationFeed {
    tx_log: TxLogHandle,
}

impl ReplicationFeed {
    /// Highest durable commit sequence number.
    pub fn durable_seq(&self) -> u64 {
        self.tx_log.written_seq.load(AtomicOrdering::Acquire)
    }

    /// Blocks until the durable sequence passes `seq` or `timeout` elapses.
    pub fn wait_beyond(&self, seq: u64, timeout: Duration) -> u64 {
        let durable = self.durable_seq();
        if durable > seq {
            return durable;
        }
        self.tx_log.durable_notifier.wait_beyond(seq, timeout);
        self.durable_seq()
    }
}

/// Where a replication sender stopped in the transaction log.
#[derive(Clone, Debug)]
pub struct ReplicationCursor {
    sent_seq: u64,
    offset: u64,
    /// Start offset and header of the frame ending at `offset`.
    anchor: Option<(u64, Vec<u8>)>,
}

impl ReplicationCursor {
    /// A cursor for a standby that has applied everything up to `applied_seq`.
    pub fn new(applied_seq: u64) -> Self {
        Self {
            sent_seq: applied_seq,
            offset: 0,
            anchor: None,
        }
    }

    pub fn sent_seq(&self) -> u64 {
        self.sent_seq
    }
}

impl BicDb {
    /// Handle for waiting on durable progress without holding the database.
    pub fn replication_feed(&self) -> ReplicationFeed {
        ReplicationFeed {
            tx_log: self.tx_log.clone(),
        }
    }

    /// Next frames after `cursor` (at most `limit` commit sequence numbers),
    /// advancing the cursor past them. Empty when nothing new is shippable.
    pub fn replication_frames_after(
        &self,
        cursor: &mut ReplicationCursor,
        limit: usize,
    ) -> Result<Vec<CommitFrame>> {
        self.ensure_replication_frames_retained()?;
        let shippable = self
            .tx_log
            .written_seq
            .load(AtomicOrdering::Acquire)
            .min(self.last_committed_tx.load(AtomicOrdering::Acquire));
        if cursor.sent_seq >= shippable {
            return Ok(Vec::new());
        }
        let target = shippable.min(cursor.sent_seq.saturating_add(limit.max(1) as u64));
        let path = self.tx_log_path();
        if let Some((start, header)) = &cursor.anchor {
            let still_there = storage::read_bytes_at(&path, *start, storage::HEADER_LEN)?;
            if still_there.as_deref() != Some(header.as_slice()) {
                // The log was truncated or rewritten under us: rescan it.
                cursor.offset = 0;
                cursor.anchor = None;
            }
        }
        let from_start = cursor.offset == 0;
        let frames = storage::read_frames_readonly_from(
            &path,
            cursor.offset,
            FrameKind::Transaction,
            &self.encryption,
        )?;

        let mut writes: FxHashMap<u64, Vec<TxWrite>> = FxHashMap::default();
        // commit_seq -> (tx_id, timestamp)
        let mut commits: BTreeMap<u64, (u64, i64)> = BTreeMap::new();
        let mut tx_of_seq: FxHashMap<u64, u64> = FxHashMap::default();
        let mut new_anchor: Option<(u64, u64)> = None;
        for (start, end, payload) in &frames {
            match decode_tx_frame(payload)? {
                TxFrame::Write { tx_id, write } => {
                    writes.entry(tx_id.0).or_default().push(*write);
                }
                TxFrame::Patch { .. } => {
                    return Err(replication_error(
                        "transaction log contains compact update patches; replication requires full row images",
                    ));
                }
                TxFrame::Truncate { tx_id, len } => {
                    if let Some(tx_writes) = writes.get_mut(&tx_id.0) {
                        tx_writes.truncate(len);
                    }
                }
                TxFrame::Commit {
                    tx_id,
                    timestamp,
                    commit_seq,
                } => {
                    if commit_seq <= target {
                        new_anchor = Some((*start, *end));
                    }
                    if commit_seq > cursor.sent_seq && commit_seq <= target {
                        commits.insert(commit_seq, (tx_id.0, timestamp));
                        tx_of_seq.insert(commit_seq, tx_id.0);
                    } else {
                        writes.remove(&tx_id.0);
                    }
                }
                TxFrame::Abort { tx_id, .. } => {
                    writes.remove(&tx_id.0);
                    commits.retain(|_, (committed, _)| *committed != tx_id.0);
                }
                TxFrame::Begin { .. }
                | TxFrame::CommitMaterialized { .. }
                | TxFrame::BrokerPublish { .. }
                | TxFrame::RecordAudit { .. }
                | TxFrame::RecordAuditPublished { .. } => {}
            }
        }

        let base = read_wal_base(&self.path)?;
        let mut out = Vec::with_capacity((target - cursor.sent_seq) as usize);
        for seq in cursor.sent_seq + 1..=target {
            let (tx_id, timestamp, tx_writes) = match commits.get(&seq) {
                Some((tx_id, timestamp)) => {
                    (*tx_id, *timestamp, writes.remove(tx_id).unwrap_or_default())
                }
                None if from_start && seq <= base => {
                    return Err(replication_error(format!(
                        "commit_seq {seq} is no longer in the transaction log; the standby needs a snapshot bootstrap"
                    )));
                }
                None => (0, unix_timestamp(), Vec::new()),
            };
            let tx_writes = tx_writes
                .iter()
                .map(|write| {
                    let collection_meta = self
                        .collections
                        .get(&write.collection)
                        .map(|state| state.read().meta.clone());
                    tx_write_to_replication_write(collection_meta, write)
                })
                .collect::<Result<Vec<_>>>()?;
            out.push(CommitFrame::new(
                self.config.replication.cluster_id.clone(),
                self.config.replication.node_id.clone(),
                DEFAULT_REPLICATION_STREAM_ID,
                seq,
                tx_id,
                timestamp,
                tx_writes,
            ));
        }
        cursor.sent_seq = target;
        if let Some((start, end)) = new_anchor {
            cursor.offset = end;
            cursor.anchor = storage::read_bytes_at(&path, start, storage::HEADER_LEN)?
                .map(|header| (start, header));
        }
        Ok(out)
    }

    /// True on a read-only standby (legacy HA standby or replication standby).
    pub fn is_read_only_standby(&self) -> bool {
        self.ensure_writable("probe").is_err()
    }

    /// True when this node is not the primary of its group: a standby, or a
    /// former primary that was fenced.
    pub fn is_standby_role(&self) -> bool {
        self.ha_state.role == HaRole::Standby
            || (self.config.replication.enabled && self.ha_node.role() != NodeRole::Primary)
    }

    /// Drops the transaction log before `keep_from` (`u64::MAX`: all of it),
    /// where `dropped_through` is the highest commit sequence whose frames all
    /// lie before `keep_from`. Never drops a frame a standby has not yet
    /// acknowledged: the cut moves back to that standby's position instead.
    pub(crate) fn cut_transaction_log(&self, keep_from: u64, dropped_through: u64) -> Result<()> {
        // Retention must survive a restart: persist the positions it is based on.
        self.persist_standby_slots()?;
        let path = self.tx_log_path();
        let log_len = file_len(&path)?;
        let (keep_from, wal_base) = match self.sync_quorum.retention_floor() {
            Some(acked) if acked < dropped_through => (
                keep_from.min(first_frame_offset_after(&path, acked, &self.encryption)?),
                acked,
            ),
            _ => (keep_from, dropped_through),
        };
        persist_commit_seq_floor(
            &self.path,
            self.commit_seq.load(AtomicOrdering::SeqCst),
            wal_base,
            self.config.fsync,
        )?;
        if keep_from >= log_len {
            truncate_log_file(&path, self.config.fsync)
        } else if keep_from > 0 {
            copy_transaction_log_tail(&path, keep_from, self.config.fsync)
        } else {
            Ok(())
        }
    }

    /// The synchronous-quorum tracker; pgwire senders record standby
    /// acknowledgements in it and committers wait on it.
    pub fn sync_quorum(&self) -> Arc<SyncQuorum> {
        Arc::clone(&self.sync_quorum)
    }
}

/// What a commit does when its synchronous standbys do not confirm it in time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncTimeoutPolicy {
    /// Report an error to the client (the commit stays durable locally and
    /// replicates when a standby reconnects).
    Fail,
    /// Acknowledge the commit and fall back to asynchronous replication until
    /// the standbys catch up again (RPO > 0 meanwhile).
    Degrade,
}

#[derive(Debug)]
struct SyncQuorumState {
    required: usize,
    timeout: Duration,
    policy: SyncTimeoutPolicy,
    acked: BTreeMap<String, u64>,
    degraded: bool,
    /// Highest sequence a committer asked to have confirmed.
    max_requested: u64,
    /// When each standby was last heard from (acknowledgements).
    heard: BTreeMap<String, Instant>,
    /// Write lease: (duration, peers that must have been heard within it).
    lease: Option<(Duration, usize)>,
    /// Log positions held for standbys that are being bootstrapped: they
    /// retain the log but confirm nothing.
    pins: BTreeMap<String, u64>,
    /// Log positions held by local consumers (the commit log archive):
    /// persisted, never confirmations.
    holds: BTreeMap<String, u64>,
    /// Set when no confirmation may be awaited any more (shutdown, fencing):
    /// waiting commits fail instead of being acknowledged.
    closed: Option<String>,
}

/// Holds commit acknowledgements until `required` standbys have durably
/// applied the commit.
#[derive(Debug)]
pub struct SyncQuorum {
    state: Mutex<SyncQuorumState>,
    changed: Condvar,
    /// Mirror of `state.required` so asynchronous servers skip the lock on
    /// every commit.
    required_fast: AtomicUsize,
    /// Whether a write lease is configured (skips the lock otherwise).
    lease_fast: AtomicBool,
    stats: SyncWaitStats,
}

impl Default for SyncQuorum {
    fn default() -> Self {
        Self {
            state: Mutex::new(SyncQuorumState {
                required: 0,
                timeout: Duration::from_secs(10),
                policy: SyncTimeoutPolicy::Fail,
                acked: BTreeMap::new(),
                degraded: false,
                max_requested: 0,
                pins: BTreeMap::new(),
                heard: BTreeMap::new(),
                lease: None,
                holds: BTreeMap::new(),
                closed: None,
            }),
            changed: Condvar::new(),
            required_fast: AtomicUsize::new(0),
            lease_fast: AtomicBool::new(false),
            stats: SyncWaitStats::default(),
        }
    }
}

/// A point-in-time view of synchronous replication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncQuorumStatus {
    pub required: usize,
    pub quorum_seq: u64,
    pub degraded: bool,
    pub standbys: BTreeMap<String, u64>,
    /// Milliseconds since each standby was last heard from.
    pub last_heard_ms: BTreeMap<String, u64>,
    /// Log held for local consumers (the commit log archive).
    pub holds: BTreeMap<String, u64>,
    /// Why writes are refused under the write lease, if they are.
    pub lease_refusal: Option<String>,
    pub lease_configured: bool,
    /// Commits that waited for confirmations, and for how long in total.
    pub waits: u64,
    pub wait_micros: u64,
    /// Waits that ended without the confirmations (timeout, shutdown,
    /// fencing, lost lease).
    pub failed_waits: u64,
    /// Times synchronous replication degraded to asynchronous.
    pub degraded_events: u64,
}

#[derive(Debug, Default)]
struct SyncWaitStats {
    waits: AtomicU64,
    wait_micros: AtomicU64,
    failed: AtomicU64,
    degraded: AtomicU64,
}

impl SyncQuorum {
    /// `required` = number of standbys that must confirm each commit (0 =
    /// asynchronous). `timeout` zero waits forever.
    pub fn configure(&self, required: usize, timeout: Duration, policy: SyncTimeoutPolicy) {
        let mut state = self.state.lock();
        state.required = required;
        state.timeout = timeout;
        state.policy = policy;
        self.required_fast.store(required, AtomicOrdering::Release);
        drop(state);
        self.changed.notify_all();
    }

    /// Requires a write lease: writes are admitted only while at least
    /// `peers` standbys were heard from within `duration` (a primary cut off
    /// from the majority of its group stops writing before the group can
    /// elect a successor).
    pub fn configure_lease(&self, duration: Duration, peers: usize) {
        self.state.lock().lease = Some((duration, peers));
        self.lease_fast.store(true, AtomicOrdering::Release);
    }

    /// Why writes are refused under the write lease, if they are.
    pub fn lease_refusal(&self) -> Option<String> {
        if !self.lease_fast.load(AtomicOrdering::Acquire) {
            return None;
        }
        let state = self.state.lock();
        let (duration, peers) = state.lease?;
        let now = Instant::now();
        let heard = state
            .heard
            .values()
            .filter(|at| now.saturating_duration_since(**at) <= duration)
            .count();
        (heard < peers).then(|| {
            format!(
                "this primary heard from {heard} of the {peers} standby(s) its write lease needs within {duration:?}"
            )
        })
    }

    pub fn required(&self) -> usize {
        self.state.lock().required
    }

    /// Records that `standby` has durably applied everything up to `seq`.
    pub fn record_ack(&self, standby: &str, seq: u64) {
        let mut state = self.state.lock();
        state.heard.insert(standby.to_string(), Instant::now());
        let entry = state.acked.entry(standby.to_string()).or_insert(0);
        if seq > *entry {
            *entry = seq;
        }
        if state.pins.get(standby).is_some_and(|pin| seq >= *pin) {
            state.pins.remove(standby);
        }
        if state.degraded && Self::quorum_seq_of(&state) >= state.max_requested {
            state.degraded = false;
            eprintln!(
                "bicdb synchronous replication: standbys caught up; synchronous commit restored"
            );
        }
        drop(state);
        self.changed.notify_all();
    }

    fn quorum_seq_of(state: &SyncQuorumState) -> u64 {
        if state.required == 0 {
            return u64::MAX;
        }
        let mut acked = state.acked.values().copied().collect::<Vec<_>>();
        acked.sort_unstable_by(|a, b| b.cmp(a));
        acked.get(state.required - 1).copied().unwrap_or(0)
    }

    /// Fails every current and future wait for confirmations with `reason`.
    pub fn close(&self, reason: &str) {
        let mut state = self.state.lock();
        if state.closed.is_none() {
            state.closed = Some(reason.to_string());
        }
        self.changed.notify_all();
    }

    /// Records a (re)connecting standby's actual position, replacing what was
    /// known before: it may be lower after the standby lost data.
    pub fn set_ack(&self, standby: &str, seq: u64) {
        let mut state = self.state.lock();
        state.acked.insert(standby.to_string(), seq);
        state.heard.insert(standby.to_string(), Instant::now());
        drop(state);
        self.changed.notify_all();
    }

    /// Retains the log after `seq` for `standby` without counting it as a
    /// confirmation (the standby is being bootstrapped from a snapshot).
    pub fn pin(&self, standby: &str, seq: u64) {
        let mut state = self.state.lock();
        state.acked.remove(standby);
        state.pins.insert(standby.to_string(), seq);
    }

    /// Keeps the log after `seq` for a local consumer such as the commit log
    /// archive. Holds are persisted and never count as confirmations.
    pub fn hold(&self, holder: &str, seq: u64) {
        self.state.lock().holds.insert(holder.to_string(), seq);
    }

    /// Releases a hold.
    pub fn release(&self, holder: &str) {
        self.state.lock().holds.remove(holder);
    }

    pub fn holds(&self) -> BTreeMap<String, u64> {
        self.state.lock().holds.clone()
    }

    pub(crate) fn seed_holds(&self, holds: BTreeMap<String, u64>) {
        self.state.lock().holds.extend(holds);
    }

    /// Seeds acknowledged positions persisted by an earlier run.
    pub(crate) fn seed(&self, acked: BTreeMap<String, u64>) {
        self.state.lock().acked.extend(acked);
    }

    /// Acknowledged position per known standby.
    pub fn acknowledged(&self) -> BTreeMap<String, u64> {
        self.state.lock().acked.clone()
    }

    /// Forgets standbys not in `allowed`.
    pub fn retain_standbys(&self, allowed: &[String]) {
        self.state
            .lock()
            .acked
            .retain(|standby, _| allowed.iter().any(|allowed| allowed == standby));
    }

    /// Oldest position any known standby has acknowledged: the transaction
    /// log must keep every commit after it.
    pub fn retention_floor(&self) -> Option<u64> {
        let state = self.state.lock();
        state
            .acked
            .values()
            .chain(state.pins.values())
            .chain(state.holds.values())
            .copied()
            .min()
    }

    pub fn status(&self) -> SyncQuorumStatus {
        let lease_refusal = self.lease_refusal();
        let state = self.state.lock();
        SyncQuorumStatus {
            required: state.required,
            quorum_seq: Self::quorum_seq_of(&state),
            degraded: state.degraded,
            standbys: state.acked.clone(),
            last_heard_ms: state
                .heard
                .iter()
                .map(|(standby, at)| (standby.clone(), at.elapsed().as_millis() as u64))
                .collect(),
            holds: state.holds.clone(),
            lease_refusal,
            lease_configured: state.lease.is_some(),
            waits: self.stats.waits.load(AtomicOrdering::Relaxed),
            wait_micros: self.stats.wait_micros.load(AtomicOrdering::Relaxed),
            failed_waits: self.stats.failed.load(AtomicOrdering::Relaxed),
            degraded_events: self.stats.degraded.load(AtomicOrdering::Relaxed),
        }
    }

    /// Blocks until `required` standbys have durably applied `seq`.
    pub fn wait_for(&self, seq: u64) -> Result<()> {
        if self.required_fast.load(AtomicOrdering::Acquire) == 0
            && !self.lease_fast.load(AtomicOrdering::Acquire)
        {
            return Ok(());
        }
        let started = Instant::now();
        let result = self.wait_for_confirmations(seq);
        self.stats.waits.fetch_add(1, AtomicOrdering::Relaxed);
        self.stats.wait_micros.fetch_add(
            started.elapsed().as_micros() as u64,
            AtomicOrdering::Relaxed,
        );
        if result.is_err() {
            self.stats.failed.fetch_add(1, AtomicOrdering::Relaxed);
        }
        result
    }

    fn wait_for_confirmations(&self, seq: u64) -> Result<()> {
        if let Some(refusal) = self.lease_refusal() {
            return Err(BicDbError::HighAvailability(format!(
                "commit {seq} is durable on this node but is not acknowledged: {refusal}"
            )));
        }
        if self.required_fast.load(AtomicOrdering::Acquire) == 0 {
            return Ok(());
        }
        let mut state = self.state.lock();
        if state.required == 0 || seq == 0 {
            return Ok(());
        }
        state.max_requested = state.max_requested.max(seq);
        if state.degraded && state.policy == SyncTimeoutPolicy::Degrade {
            return Ok(());
        }
        let deadline = (!state.timeout.is_zero()).then(|| Instant::now() + state.timeout);
        while Self::quorum_seq_of(&state) < seq {
            if let Some(reason) = &state.closed {
                return Err(BicDbError::HighAvailability(format!(
                    "commit {seq} is durable on this node but was not confirmed by a synchronous standby: {reason}"
                )));
            }
            match deadline {
                Some(deadline) => {
                    if self.changed.wait_until(&mut state, deadline).timed_out()
                        && Self::quorum_seq_of(&state) < seq
                    {
                        return match state.policy {
                            SyncTimeoutPolicy::Degrade => {
                                if !state.degraded {
                                    state.degraded = true;
                                    self.stats.degraded.fetch_add(1, AtomicOrdering::Relaxed);
                                    eprintln!(
                                        "bicdb synchronous replication: no quorum within {:?}; degraded to asynchronous",
                                        state.timeout
                                    );
                                }
                                Ok(())
                            }
                            SyncTimeoutPolicy::Fail => Err(BicDbError::HighAvailability(format!(
                                "commit {seq} is durable on the primary but was not confirmed by {} synchronous standby(s) within {:?}; it will replicate when a standby reconnects",
                                state.required, state.timeout
                            ))),
                        };
                    }
                }
                None => self.changed.wait(&mut state),
            }
        }
        Ok(())
    }
}
