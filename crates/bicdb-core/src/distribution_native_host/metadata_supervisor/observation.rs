//! TEST-ONLY bounded passive facts retain no Error, ACK, token, store or
//! authority owner. A SEPARATE armed synchronization hook can queue the
//! original round CancellationToken into the actual controlled reader; it
//! never renews that token. Protocol results are emitted after scopes return.
use super::*;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

const CAP: usize = 256;
pub(super) type SnapshotHook = dyn Fn(&CancellationToken) -> Result<()> + Send + Sync;
#[derive(Clone, Serialize)]
struct Fact {
    stage: &'static str,
    peer: Option<String>,
    term: u64,
    role: Option<&'static str>,
    voting_actor: Option<bool>,
    anchor_committed: Option<bool>,
    index: u64,
    request_entries: Option<usize>,
    request_prev_index: Option<u64>,
    request_last_index: Option<u64>,
    response_term: Option<u64>,
    response_success: Option<bool>,
    result_committed: Option<bool>,
    elapsed_us: u64,
    remaining_us: Option<u64>,
    error_prefix: Option<String>,
    error_prefix_truncated: bool,
    error_display_sha256: Option<String>,
    error_display_complete: bool,
}
pub(crate) struct Observation {
    facts: Mutex<Vec<Fact>>,
    omitted: AtomicU64,
    pub(super) initial_busy: AtomicU64,
    pub(super) max_lock_wait_us: AtomicU64,
    pub(super) completed_snapshots: AtomicU64,
    pub(super) canceled_admission_refusals: AtomicU64,
    pub(super) expired_admission_refusals: AtomicU64,
    hook: Mutex<Option<Arc<SnapshotHook>>>,
}
impl Observation {
    pub(super) fn new() -> Self {
        Self {
            facts: Mutex::new(Vec::new()),
            omitted: AtomicU64::new(0),
            initial_busy: AtomicU64::new(0),
            max_lock_wait_us: AtomicU64::new(0),
            completed_snapshots: AtomicU64::new(0),
            canceled_admission_refusals: AtomicU64::new(0),
            expired_admission_refusals: AtomicU64::new(0),
            hook: Mutex::new(None),
        }
    }
    pub(super) fn observe_admission_denial(&self, error: &crate::BicDbError) {
        match error {
            crate::BicDbError::QueryCanceled => {
                self.canceled_admission_refusals
                    .fetch_add(1, Ordering::Relaxed);
            }
            crate::BicDbError::QueryTimedOut => {
                self.expired_admission_refusals
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
    pub(super) fn before_snapshot(&self, parent: &CancellationToken) -> Result<()> {
        let hook = self.hook.lock().clone();
        if let Some(hook) = hook {
            hook(parent)?;
        }
        Ok(())
    }
    pub(crate) fn initial_busy_count(&self) -> u64 {
        self.initial_busy.load(Ordering::Acquire)
    }
    pub(crate) fn set_snapshot_hook(&self, hook: Option<Arc<SnapshotHook>>) -> Result<()> {
        // Setup/cleanup only, outside every engine/service/MStore guard.
        // This mutex contains only an Arc to a test synchronization hook.
        *self.hook.lock() = hook;
        Ok(())
    }
    pub(super) fn record(
        &self,
        stage: &'static str,
        peer: Option<&ClusterNodeId>,
        term: u64,
        index: u64,
        response: Option<(u64, bool)>,
        committed: Option<bool>,
        start: Instant,
        parent: &CancellationToken,
        error: Option<&crate::BicDbError>,
    ) {
        // Streaming full DISPLAY hash; once prefix is incomplete it never
        // appends a later UTF8 chunk. No source-chain recursion/allocation.
        struct Writer {
            hash: sha2::Sha256,
            prefix: String,
            truncated: bool,
        }
        impl std::fmt::Write for Writer {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                use sha2::Digest;
                self.hash.update(text.as_bytes());
                if !self.truncated {
                    let mut n = (512 - self.prefix.len()).min(text.len());
                    while !text.is_char_boundary(n) {
                        n -= 1;
                    }
                    self.prefix.push_str(&text[..n]);
                    self.truncated = n != text.len();
                }
                Ok(())
            }
        }
        use sha2::Digest;
        let (prefix, truncated, hash, complete) = if let Some(error) = error {
            let mut writer = Writer {
                hash: sha2::Sha256::new(),
                prefix: String::new(),
                truncated: false,
            };
            let complete = std::fmt::write(&mut writer, format_args!("{error}")).is_ok();
            (
                Some(writer.prefix),
                writer.truncated,
                Some(hex::encode(writer.hash.finalize())),
                complete,
            )
        } else {
            (None, false, None, true)
        };
        let peer = peer.map(|p| p.as_str().chars().take(128).collect());
        let fact = Fact {
            stage,
            peer,
            term,
            role: None,
            voting_actor: None,
            anchor_committed: None,
            index,
            request_entries: None,
            request_prev_index: None,
            request_last_index: None,
            response_term: response.map(|r| r.0),
            response_success: response.map(|r| r.1),
            result_committed: committed,
            elapsed_us: start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            remaining_us: parent.deadline().map(|d| {
                d.saturating_duration_since(Instant::now())
                    .as_micros()
                    .min(u64::MAX as u128) as u64
            }),
            error_prefix: prefix,
            error_prefix_truncated: truncated,
            error_display_sha256: hash,
            error_display_complete: complete,
        };
        self.push(fact);
    }
    fn push(&self, fact: Fact) {
        if let Some(mut held) = self.facts.try_lock() {
            if held.len() < CAP {
                held.push(fact);
                return;
            }
        }
        self.omitted.fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn record_snapshot(
        &self,
        snapshot: &NativeMetadataSupervisorSnapshot,
        start: Instant,
        parent: &CancellationToken,
    ) {
        let role = match snapshot.status.role {
            MetadataConsensusRole::Follower => "follower",
            MetadataConsensusRole::Candidate => "candidate",
            MetadataConsensusRole::Leader => "leader",
        };
        self.push(Fact {
            stage: "full_snapshot_returned",
            peer: None,
            term: snapshot.status.current_term,
            role: Some(role),
            voting_actor: Some(snapshot.voting_actor),
            anchor_committed: Some(snapshot.anchored),
            index: snapshot.status.last_log_index,
            request_entries: None,
            request_prev_index: None,
            request_last_index: None,
            response_term: None,
            response_success: None,
            result_committed: None,
            elapsed_us: start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            remaining_us: parent.deadline().map(|d| {
                d.saturating_duration_since(Instant::now())
                    .as_micros()
                    .min(u64::MAX as u128) as u64
            }),
            error_prefix: None,
            error_prefix_truncated: false,
            error_display_sha256: None,
            error_display_complete: true,
        });
    }
    pub(super) fn record_append_dispatch(
        &self,
        peer: &ClusterNodeId,
        request: &crate::distribution_consensus::MetadataAppendRequest,
        start: Instant,
        parent: &CancellationToken,
    ) {
        self.push(Fact {
            stage: "append_dispatch",
            peer: Some(peer.as_str().chars().take(128).collect()),
            term: request.term,
            role: None,
            voting_actor: None,
            anchor_committed: None,
            index: request.leader_commit,
            request_entries: Some(request.entries.len()),
            request_prev_index: Some(request.prev_log_index),
            request_last_index: Some(
                request
                    .entries
                    .last()
                    .map_or(request.prev_log_index, |e| e.index),
            ),
            response_term: None,
            response_success: None,
            result_committed: None,
            elapsed_us: start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            remaining_us: parent.deadline().map(|d| {
                d.saturating_duration_since(Instant::now())
                    .as_micros()
                    .min(u64::MAX as u128) as u64
            }),
            error_prefix: None,
            error_prefix_truncated: false,
            error_display_sha256: None,
            error_display_complete: true,
        });
    }
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let facts = self.facts.try_lock().map(|held| held.clone());
        serde_json::json!({"recorded":facts,"capacity":CAP,"omitted":self.omitted.load(Ordering::Relaxed),
            "initial_try_lock_busy":self.initial_busy_count(),"max_lock_wait_us":self.max_lock_wait_us.load(Ordering::Relaxed),
            "completed_full_snapshots":self.completed_snapshots.load(Ordering::Relaxed),
            "error_display_prefix_max_bytes":512,"peer_max_chars":128,
            "snapshot_buffer_available":facts.is_some(),"global_capture_complete":false,
            "background_quiescence_not_proven":true,"passive_facts_retain_no_authority":true,
            "separate_test_hook_can_queue_original_round_token":true,
            "canceled_admission_refusals":self.canceled_admission_refusals.load(Ordering::Relaxed),
            "expired_admission_refusals":self.expired_admission_refusals.load(Ordering::Relaxed)})
    }
}
