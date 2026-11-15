//! One-shot observations at actual commit boundaries.
//! Test callbacks receive sequence DATA, never a counter setter or authority.
use super::*;
use std::cell::RefCell;
use std::rc::Rc;

mod visibility_wait_tests;

type Hook = Box<dyn FnOnce(u64)>;
#[derive(Clone, Copy)]
enum Point {
    RealWal,
    ActualApply,
}
thread_local! {
    static REAL_WAL: RefCell<Option<Hook>> = const { RefCell::new(None) };
    static ACTUAL_APPLY: RefCell<Option<Hook>> = const { RefCell::new(None) };
}
struct HookGuard {
    point: Point,
    // The hook belongs only to the original thread. Cleanup cannot migrate to
    // another committer and clear that thread's selected observation.
    _not_send: PhantomData<Rc<()>>,
}
impl Drop for HookGuard {
    fn drop(&mut self) {
        slot(self.point, |value| {
            value.borrow_mut().take();
        });
    }
}
fn slot<R>(point: Point, f: impl FnOnce(&RefCell<Option<Hook>>) -> R) -> R {
    match point {
        Point::RealWal => REAL_WAL.with(f),
        Point::ActualApply => ACTUAL_APPLY.with(f),
    }
}
fn arm(point: Point, callback: Hook) -> HookGuard {
    slot(point, |value| {
        let mut value = value.borrow_mut();
        assert!(
            value.is_none(),
            "selected commit visibility hook already armed"
        );
        *value = Some(callback);
    });
    HookGuard {
        point,
        _not_send: PhantomData,
    }
}
pub(super) fn arm_after_real_wal(callback: Hook) -> impl Drop {
    arm(Point::RealWal, callback)
}
pub(super) fn arm_after_actual_apply(callback: Hook) -> impl Drop {
    arm(Point::ActualApply, callback)
}
pub(super) fn after_real_wal(sequence: u64) {
    call(Point::RealWal, sequence);
}
pub(super) fn after_actual_apply(sequence: u64) {
    call(Point::ActualApply, sequence);
}
fn call(point: Point, sequence: u64) {
    // Release the RefCell borrow before the callback pauses the real caller.
    let callback = slot(point, |value| value.borrow_mut().take());
    if let Some(callback) = callback {
        callback(sequence);
    }
}

struct NativePublicProbe {
    registration: Uuid,
    armed_thread: std::thread::ThreadId,
    callback: Box<dyn FnOnce(serde_json::Value) + Send>,
}
// A real follower applies on the quorum fan-out worker, rather than the
// origin thread arming this diagnostic. Select only the exact actual DB open.
// Entries contain observation callbacks, never transactions or capabilities.
fn native_public_probes(
) -> &'static parking_lot::Mutex<std::collections::BTreeMap<String, NativePublicProbe>> {
    static PROBES: std::sync::OnceLock<
        parking_lot::Mutex<std::collections::BTreeMap<String, NativePublicProbe>>,
    > = std::sync::OnceLock::new();
    PROBES.get_or_init(Default::default)
}
struct NativePublicProbeGuard {
    expected_open: String,
    registration: Uuid,
}
impl Drop for NativePublicProbeGuard {
    fn drop(&mut self) {
        let removed = {
            let mut probes = native_public_probes().lock();
            if probes
                .get(&self.expected_open)
                .is_some_and(|probe| probe.registration == self.registration)
            {
                probes.remove(&self.expected_open)
            } else {
                None
            }
        };
        drop(removed);
    }
}
pub(super) fn arm_native_public_finalizer_probe(
    expected_actual_open: String,
    callback: Box<dyn FnOnce(serde_json::Value) + Send>,
) -> impl Drop {
    let registration = Uuid::now_v7();
    let mut probes = native_public_probes().lock();
    assert!(
        !probes.contains_key(&expected_actual_open),
        "native public-finalizer probe already armed for actual open"
    );
    probes.insert(
        expected_actual_open.clone(),
        NativePublicProbe {
            registration,
            armed_thread: std::thread::current().id(),
            callback,
        },
    );
    NativePublicProbeGuard {
        expected_open: expected_actual_open,
        registration,
    }
}

/// Called ONLY after the real private native preparation proved actual apply.
/// The fixture receives DATA, never a transaction/grant/permit or state setter.
pub(super) fn probe_native_public_finalizer(tx: &mut Transaction) {
    let db = unsafe { tx.db.as_ref() };
    // Drop the registry guard before observing or invoking the same Tx's
    // original public API. A different open cannot consume this one-shot.
    let probe = native_public_probes().lock().remove(&db.incarnation);
    let Some(probe) = probe else {
        return;
    };
    let observer = tx.observe_outcome();
    let observe = |tx: &Transaction| {
        serde_json::json!({
            "transaction_id":tx.id.0,"sequence":tx.committed_seq,
            "incarnation":db.incarnation,"global_prefix":db.current_visibility_watermark(),
            "wait_exhausted":tx.visibility_wait_exhausted,
            "admission_ticket_present":tx.commit_admission_ticket.is_some(),
            "wal_pin_present":tx.native_wal_retention.is_some(),
            "core_assigned_sequence":db.current_commit_seq(),
            "native_next_xid":db.paged_records.as_ref().map(|p|p.store().transactions().next_xid()),
            "native_next_lsn":db.paged_records.as_ref().map(|p|p.store().wal().next_lsn()),
            "memory_index_count":db.memory_indexes.read().len(),
            "memory_job_count":db.memory_index_jobs.read().len(),
            "receipt":format!("{:?}",observer.outcome()),
        })
    };
    let before = observe(tx);
    let result = tx.finalize_commit_admission();
    let exact_unknown = matches!(&result, Err(BicDbError::CommitOutcomeUnknown {
        sequence, transaction_id, incarnation, ..
    }) if *sequence == tx.committed_seq && *transaction_id == Some(tx.id.0) && incarnation == &db.incarnation);
    let after = observe(tx);
    (probe.callback)(serde_json::json!({
        "before":before,"after":after,
        "probe_armed_thread":format!("{:?}",probe.armed_thread),
        "probe_actual_finalizer_thread":format!("{:?}",std::thread::current().id()),
        "public_returned_ok":result.is_ok(),
        "public_exact_original_unknown":exact_unknown,
        "public_result":format!("{result:?}"),
        "scope":"current public API on actual typed native Tx; selector DATA is not authority",
    }));
}

// Captures only actual selected job IDs. The callback runs AFTER the jobs
// read guard was dropped and BEFORE per-job claims/current-status admission.
type JobSelectionHook = Box<dyn FnOnce(Vec<Uuid>)>;
thread_local! {
    static JOB_SELECTION: RefCell<Option<JobSelectionHook>> = const { RefCell::new(None) };
}
struct JobSelectionGuard(PhantomData<Rc<()>>);
impl Drop for JobSelectionGuard {
    fn drop(&mut self) {
        JOB_SELECTION.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
pub(super) fn arm_after_memory_job_selection(callback: JobSelectionHook) -> impl Drop {
    JOB_SELECTION.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(slot.is_none(), "memory job selection hook already armed");
        *slot = Some(callback);
    });
    JobSelectionGuard(PhantomData)
}
pub(super) fn after_memory_job_selection(ids: Vec<Uuid>) {
    let callback = JOB_SELECTION.with(|slot| slot.borrow_mut().take());
    if let Some(callback) = callback {
        callback(ids);
    }
}

// Actual per-job claims remain owned by the caller, but no claim mutex or job
// metadata guard survives into this callback. Only selected actual IDs escape.
thread_local! {
    static JOB_CLAIM: RefCell<Option<JobSelectionHook>> = const { RefCell::new(None) };
}
struct JobClaimGuard(PhantomData<Rc<()>>);
impl Drop for JobClaimGuard {
    fn drop(&mut self) {
        JOB_CLAIM.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}
pub(super) fn arm_after_memory_job_claim(callback: JobSelectionHook) -> impl Drop {
    JOB_CLAIM.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(slot.is_none(), "memory job claim hook already armed");
        *slot = Some(callback);
    });
    JobClaimGuard(PhantomData)
}
pub(super) fn after_memory_job_claim(ids: Vec<Uuid>) {
    let callback = JOB_CLAIM.with(|slot| slot.borrow_mut().take());
    if let Some(callback) = callback {
        callback(ids);
    }
}
