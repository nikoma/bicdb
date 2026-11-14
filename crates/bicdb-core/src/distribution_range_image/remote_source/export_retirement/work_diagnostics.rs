//! Private test observation of the ORIGINAL recipient function's lifetime.
//! No token/guard/permit is returned to the fixture; status is DATA only.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default)]
pub(crate) struct ExportWorkDiagnostics {
    entries: Mutex<BTreeMap<Uuid, Vec<Arc<Work>>>>,
}
struct Work {
    exact_request: Vec<u8>,
    identity: NativeSourceExportRequestV1,
    entered: Instant,
    original_server_parent_deadline: Option<Instant>,
    server_parent_remaining_at_entry_us: Option<u64>,
    finished: AtomicBool,
    finished_after_us: AtomicU64,
    finished_parent_cancelled: AtomicBool,
    finished_parent_expired: AtomicBool,
    panicked: AtomicBool,
}
pub(crate) struct OriginalExportWorkGuard {
    work: Arc<Work>,
    original_server_parent: CancellationToken,
}
impl Drop for OriginalExportWorkGuard {
    fn drop(&mut self) {
        // FIRST function local -> LAST local dropped. All nested source,
        // session and discovery-claim guards have already unwound. No locks,
        // serialization, callbacks, I/O or admission checks in this probe.
        let now = Instant::now();
        self.work.finished_after_us.store(
            self.work
                .entered
                .elapsed()
                .as_micros()
                .min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        self.work.finished_parent_cancelled.store(
            self.original_server_parent.is_cancel_requested(),
            Ordering::Relaxed,
        );
        self.work.finished_parent_expired.store(
            self.original_server_parent
                .deadline()
                .is_some_and(|d| now >= d),
            Ordering::Relaxed,
        );
        self.work
            .panicked
            .store(std::thread::panicking(), Ordering::Relaxed);
        self.work.finished.store(true, Ordering::Release);
    }
}
impl NativeRemoteImageSourceRegistry {
    pub(crate) fn test_enter_original_export_work(
        &self,
        request: &NativeSourceExportRequestV1,
        parent: &CancellationToken,
    ) -> Result<OriginalExportWorkGuard> {
        let bytes = bounded_json(request, 64 * 1024)?;
        let mut entries = self
            .export_work_diagnostics
            .entries
            .try_lock()
            .ok_or_else(|| invalid("original-work diagnostic table busy"))?;
        if !entries.contains_key(&request.challenge) && entries.len() >= 64 {
            return Err(invalid("original-work diagnostic request capacity"));
        }
        let attempts = entries.entry(request.challenge).or_default();
        if attempts.len() >= 8 || attempts.iter().any(|w| w.exact_request != bytes) {
            return Err(invalid("original-work diagnostic changed/capacity request"));
        }
        let work = Arc::new(Work {
            exact_request: bytes,
            identity: request.clone(),
            entered: Instant::now(),
            original_server_parent_deadline: parent.deadline(),
            server_parent_remaining_at_entry_us: parent.deadline().map(|d| {
                d.saturating_duration_since(Instant::now())
                    .as_micros()
                    .min(u64::MAX as u128) as u64
            }),
            finished: AtomicBool::new(false),
            finished_after_us: AtomicU64::new(0),
            finished_parent_cancelled: AtomicBool::new(false),
            finished_parent_expired: AtomicBool::new(false),
            panicked: AtomicBool::new(false),
        });
        // Retain the exact entry BEFORE any business request/current checks.
        attempts.push(work.clone());
        Ok(OriginalExportWorkGuard {
            work,
            original_server_parent: parent.clone(),
        })
    }
    pub(crate) fn test_original_export_work_status(
        &self,
        request: &NativeSourceExportRequestV1,
    ) -> Result<serde_json::Value> {
        let bytes = bounded_json(request, 64 * 1024)?;
        let entries = self
            .export_work_diagnostics
            .entries
            .try_lock()
            .ok_or_else(|| invalid("original-work diagnostic table busy"))?;
        let Some(attempts) = entries.get(&request.challenge) else {
            return Ok(
                serde_json::json!({"entries":0,"all_completed":false,"work":[],"not_entered_is_not_proof_not_dispatched":true}),
            );
        };
        if attempts.iter().any(|w| w.exact_request != bytes) {
            return Err(invalid("original-work diagnostic request identity differs"));
        }
        let work=attempts.iter().map(|w| {
            let finished=w.finished.load(Ordering::Acquire);
            serde_json::json!({"actual_recipient_function_entered":true,"exact_request":w.identity,
                "completed_after_all_business_guards_dropped":finished,"elapsed_since_entry_us":w.entered.elapsed().as_micros(),
                "server_parent_deadline_remaining_us":w.original_server_parent_deadline.map(|d|d.saturating_duration_since(Instant::now()).as_micros()),
                "server_parent_remaining_at_entry_us":w.server_parent_remaining_at_entry_us,
                "finished_after_us":finished.then(||w.finished_after_us.load(Ordering::Relaxed)),
                "server_parent_cancelled_at_completion":finished.then(||w.finished_parent_cancelled.load(Ordering::Relaxed)),
                "server_parent_expired_at_completion":finished.then(||w.finished_parent_expired.load(Ordering::Relaxed)),
                "panicked":finished.then(||w.panicked.load(Ordering::Relaxed)),"not_full_network_worker_completion":true})
        }).collect::<Vec<_>>();
        Ok(
            serde_json::json!({"entries":work.len(),"all_completed":work.iter().all(|w|w["completed_after_all_business_guards_dropped"]==true),"work":work,"observation_not_authority":true}),
        )
    }
}
