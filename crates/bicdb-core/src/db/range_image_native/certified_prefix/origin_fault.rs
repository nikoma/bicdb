//! Test-only actual receiver fault/exit observations. No record here is a
//! commit certificate, native visibility proof, role or recovery permission.
use super::*;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Weak;

#[derive(Debug, Default)]
struct Observation {
    span_id: Option<Uuid>,
    transaction_id: Option<u64>,
    ticket_command_id: Option<String>,
    ticket_authority: Option<String>,
    core_error: Option<String>,
    unknown_sequence: Option<u64>,
    unknown_transaction_id: Option<u64>,
}
#[derive(Debug)]
pub(crate) struct NativeOriginFaultProbe {
    database_incarnation: String,
    actual_open: Weak<()>,
    range_id: RangeId,
    record_id: String,
    armed: AtomicBool,
    claimed: AtomicBool,
    fired: AtomicBool,
    core_result: AtomicU8,
    release_result: AtomicU8,
    span_ended: AtomicBool,
    normal_handler_exit: AtomicBool,
    panicked: AtomicBool,
    diagnostic_contention: AtomicBool,
    observation: Mutex<Observation>,
}
impl NativeOriginFaultProbe {
    /// Call only from the external test after dropping engine guards. This
    /// projection is diagnostic DATA; it cannot authorize any engine action.
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let observed = self.observation.lock();
        serde_json::json!({
            "scope":"cfg(test) actual receiver observations only; no authority",
            "database_open_incarnation":self.database_incarnation,
            "actual_open_alive":self.actual_open.upgrade().is_some(),
            "requested_range_id":self.range_id,
            "requested_record_id":self.record_id,
            "armed":self.armed.load(Ordering::Acquire),
            "claimed":self.claimed.load(Ordering::Acquire),
            "fired":self.fired.load(Ordering::Acquire),
            "actual_transaction_id":observed.transaction_id,
            "actual_ticket_command_id":observed.ticket_command_id,
            "actual_ticket_authority":observed.ticket_authority,
            "core_result":match self.core_result.load(Ordering::Acquire) {0=>"not_returned",1=>"ok",2=>"commit_outcome_unknown",_=>"other_error"},
            "actual_core_error":observed.core_error,
            "unknown_sequence":observed.unknown_sequence,
            "unknown_transaction_id":observed.unknown_transaction_id,
            "release_callback_returned":self.release_result.load(Ordering::Acquire)!=0,
            "release_callback_ok":self.release_result.load(Ordering::Acquire)==1,
            "app_span_ended":self.span_ended.load(Ordering::Acquire),
            "normal_app_handler_exit":self.normal_handler_exit.load(Ordering::Acquire),
            "panicked":self.panicked.load(Ordering::Acquire),
            "diagnostic_contention":self.diagnostic_contention.load(Ordering::Acquire),
        })
    }
}

/// The arm owner, NOT a cloned observation, controls lifetime. Revocation never
/// waits for a registry borrow or checks DB health/filesystem/role authority.
#[derive(Debug)]
pub(crate) struct NativeOriginFaultArm {
    probe: Arc<NativeOriginFaultProbe>,
    registry: Weak<Mutex<NativeRangeScopeRegistry>>,
}
fn disarm(probe: &Arc<NativeOriginFaultProbe>, registry: &Weak<Mutex<NativeRangeScopeRegistry>>) {
    probe.armed.store(false, Ordering::Release);
    if let Some(registry) = registry.upgrade() {
        if let Some(mut registry) = registry.try_lock() {
            if registry
                .origin_fault
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, probe))
            {
                registry.origin_fault = None;
            }
        }
    }
    // A failed try-borrow can retain ONE stale, already-disarmed slot. A new
    // actual arm may replace it; no observation can re-arm it.
}
impl NativeOriginFaultArm {
    pub(crate) fn observation(&self) -> Arc<NativeOriginFaultProbe> {
        Arc::clone(&self.probe)
    }
}
impl Drop for NativeOriginFaultArm {
    fn drop(&mut self) {
        disarm(&self.probe, &self.registry);
    }
}

#[derive(Clone)]
struct Active {
    probe: Arc<NativeOriginFaultProbe>,
    span_id: Uuid,
}
thread_local! {
    static ACTIVE: RefCell<Option<Active>> = const { RefCell::new(None) };
}
pub(crate) struct NativeOriginAppSpan {
    active: Active,
    previous: Option<Active>,
    registry: Weak<Mutex<NativeRangeScopeRegistry>>,
    _not_send: PhantomData<Rc<()>>,
}
impl NativeOriginAppSpan {
    pub(crate) fn note_result(&self, result: &crate::Result<crate::ClusterEventResponse>) {
        let probe = &self.active.probe;
        let Some(mut observed) = probe.observation.try_lock() else {
            probe.diagnostic_contention.store(true, Ordering::Release);
            return;
        };
        if observed.span_id != Some(self.active.span_id) {
            probe.diagnostic_contention.store(true, Ordering::Release);
            return;
        }
        match result {
            Ok(_) => probe.core_result.store(1, Ordering::Release),
            Err(error) => {
                if let BicDbError::CommitOutcomeUnknown {
                    sequence,
                    transaction_id,
                    ..
                } = error
                {
                    observed.unknown_sequence = *sequence;
                    observed.unknown_transaction_id = *transaction_id;
                    probe.core_result.store(2, Ordering::Release);
                } else {
                    probe.core_result.store(3, Ordering::Release);
                }
                let detail = error.to_string();
                if detail.len() <= 4096 {
                    observed.core_error = Some(detail);
                } else {
                    probe.diagnostic_contention.store(true, Ordering::Release);
                }
            }
        }
    }
}
impl Drop for NativeOriginAppSpan {
    fn drop(&mut self) {
        let probe = &self.active.probe;
        let panicked = std::thread::panicking();
        probe.panicked.store(panicked, Ordering::Release);
        // This span is declared BEFORE the actual APP DB guard. Rust drops
        // that guard first, after execute_cluster_event consumed/dropped its
        // transaction and release callback. No mutation follows this marker.
        probe.normal_handler_exit.store(
            !panicked && probe.core_result.load(Ordering::Acquire) != 0,
            Ordering::Release,
        );
        disarm(probe, &self.registry);
        ACTIVE.with(|slot| {
            slot.replace(self.previous.take());
        });
        probe.span_ended.store(true, Ordering::Release);
    }
}

impl BicDb {
    pub(crate) fn test_arm_native_origin_pre_wal_fault(
        &self,
        range_id: RangeId,
        namespace: &str,
        key: &str,
    ) -> Result<NativeOriginFaultArm> {
        let record_id = crate::cluster_event_record_id(namespace, key)?;
        let mut registry = self
            .native_range_command_scopes
            .try_lock()
            .ok_or_else(|| invalid("test origin fault registry busy"))?;
        if registry
            .origin_fault
            .as_ref()
            .is_some_and(|p| p.armed.load(Ordering::Acquire))
        {
            return Err(invalid("test origin fault already armed"));
        }
        let probe = Arc::new(NativeOriginFaultProbe {
            database_incarnation: self.incarnation.clone(),
            actual_open: self.native_metadata_owner_open_lifetime(),
            range_id,
            record_id,
            armed: AtomicBool::new(true),
            claimed: AtomicBool::new(false),
            fired: AtomicBool::new(false),
            core_result: AtomicU8::new(0),
            release_result: AtomicU8::new(0),
            span_ended: AtomicBool::new(false),
            normal_handler_exit: AtomicBool::new(false),
            panicked: AtomicBool::new(false),
            diagnostic_contention: AtomicBool::new(false),
            observation: Mutex::new(Observation::default()),
        });
        registry.origin_fault = Some(Arc::clone(&probe));
        Ok(NativeOriginFaultArm {
            probe,
            registry: Arc::downgrade(&self.native_range_command_scopes),
        })
    }
    pub(crate) fn test_begin_native_origin_app_span(
        &self,
        range_id: RangeId,
        operation: &crate::ClusterEventOperation,
    ) -> Option<NativeOriginAppSpan> {
        let crate::ClusterEventOperation::Append { namespace, key, .. } = operation else {
            return None;
        };
        let record_id = crate::cluster_event_record_id(namespace, key).ok()?;
        let probe = self
            .native_range_command_scopes
            .try_lock()?
            .origin_fault
            .clone()?;
        if probe.database_incarnation != self.incarnation
            || !Weak::ptr_eq(
                &probe.actual_open,
                &self.native_metadata_owner_open_lifetime(),
            )
            || probe.actual_open.upgrade().is_none()
            || probe.range_id != range_id
            || probe.record_id != record_id
            || !probe.armed.load(Ordering::Acquire)
            || probe
                .claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return None;
        }
        let span_id = Uuid::now_v7();
        if let Some(mut observed) = probe.observation.try_lock() {
            observed.span_id = Some(span_id);
        } else {
            probe.diagnostic_contention.store(true, Ordering::Release);
            probe.armed.store(false, Ordering::Release);
            return None;
        }
        let active = Active { probe, span_id };
        let previous = ACTIVE.with(|slot| slot.replace(Some(active.clone())));
        Some(NativeOriginAppSpan {
            active,
            previous,
            registry: Arc::downgrade(&self.native_range_command_scopes),
            _not_send: PhantomData,
        })
    }
    pub(in crate::db) fn test_consume_receiver_origin_fault(
        &self,
        transaction: &Transaction,
    ) -> bool {
        let Some(active) = ACTIVE.with(|slot| slot.borrow().clone()) else {
            return false;
        };
        let probe = &active.probe;
        let Some((_, ticket)) = transaction.commit_admission_ticket.as_ref() else {
            return false;
        };
        if transaction.native_admission_recovery_release.is_none()
            || probe.database_incarnation != self.incarnation
            || !Weak::ptr_eq(
                &probe.actual_open,
                &self.native_metadata_owner_open_lifetime(),
            )
            || probe.actual_open.upgrade().is_none()
            || transaction.writes.len() != 1
            || transaction.writes[0].collection != crate::CLUSTER_EVENTS_COLLECTION
            || transaction.writes[0].record_id != probe.record_id
            || ticket.command_id.len() > 128
            || ticket.authority.len() > 256
        {
            return false;
        }
        let Some(mut observed) = probe.observation.try_lock() else {
            probe.diagnostic_contention.store(true, Ordering::Release);
            probe.armed.store(false, Ordering::Release);
            return false;
        };
        if observed.span_id != Some(active.span_id)
            || probe
                .armed
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return false;
        }
        observed.transaction_id = Some(transaction.id().0);
        observed.ticket_command_id = Some(ticket.command_id.clone());
        observed.ticket_authority = Some(ticket.authority.clone());
        probe.fired.store(true, Ordering::Release);
        true
    }
}

pub(in crate::db) fn test_record_origin_release(
    ticket: &CommitAdmissionTicket,
    result: &Result<()>,
) {
    let Some(active) = ACTIVE.with(|slot| slot.borrow().clone()) else {
        return;
    };
    let probe = &active.probe;
    if !probe.fired.load(Ordering::Acquire) {
        return;
    }
    let Some(observed) = probe.observation.try_lock() else {
        probe.diagnostic_contention.store(true, Ordering::Release);
        return;
    };
    if observed.span_id == Some(active.span_id)
        && observed.ticket_command_id.as_deref() == Some(ticket.command_id.as_str())
    {
        probe
            .release_result
            .store(if result.is_ok() { 1 } else { 2 }, Ordering::Release);
    }
}
