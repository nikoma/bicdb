//! Selected actual manual turn only. Pause bypass changes test scheduling,
//! never a Role/native result. Every real receiver and final publisher runs.
use super::*;
use std::cell::{Cell, RefCell};
use std::io::Write;
use std::rc::Rc;
thread_local! {static SELECTED:RefCell<Option<Selection>>=const{RefCell::new(None)};}
struct Facts {
    nonce: Uuid,
    phase: &'static str,
    position: usize,
    members: usize,
    node: ClusterNodeId,
    parent_live: bool,
    role_live: bool,
    effective_live: bool,
    slot_live: bool,
    parent_deadline: Option<Instant>,
    role_deadline: Instant,
    effective_deadline: Option<Instant>,
    effective_not_after_parent: bool,
}
impl Facts {
    fn data(&self) -> serde_json::Value {
        serde_json::json!({"nonce":self.nonce,"phase":self.phase,"position":self.position,"members":self.members,"node":self.node,
            "parent_live":self.parent_live,"role_live":self.role_live,"effective_live":self.effective_live,"slot_live":self.slot_live,
            "parent_deadline":self.parent_deadline.map(|d|format!("{d:?}")),"role_deadline":format!("{:?}",self.role_deadline),"effective_deadline":self.effective_deadline.map(|d|format!("{d:?}")),"effective_not_after_parent":self.effective_not_after_parent,
            "actual_sealed_success_and_same_open_validation_preceded_cut":true,"no_quorum_or_grant_from_DATA":true})
    }
}
struct Selection {
    owner: usize,
    mode: u8,
    original: CancellationToken,
    facts: Rc<Cell<Option<Facts>>>,
}
struct Selected;
impl Drop for Selected {
    fn drop(&mut self) {
        SELECTED.with(|s| {
            s.borrow_mut().take();
        });
    }
}
fn arm(
    owner: &NativeHotRecoveryMaintenance,
    mode: u8,
    original: &CancellationToken,
) -> (Selected, Rc<Cell<Option<Facts>>>) {
    let facts = Rc::new(Cell::new(None));
    SELECTED.with(|s| {
        assert!(s.borrow().is_none());
        *s.borrow_mut() = Some(Selection {
            owner: owner as *const _ as usize,
            mode,
            original: original.clone(),
            facts: facts.clone(),
        });
    });
    (Selected, facts)
}
/// Expired/missing selection never broadens the normal worker's pause branch.
pub(super) fn selected(owner: &NativeHotRecoveryMaintenance) -> bool {
    SELECTED.with(|s| {
        s.borrow()
            .as_ref()
            .is_some_and(|s| s.owner == owner as *const _ as usize && s.original.check().is_ok())
    })
}
pub(super) fn after_actual_validated_success(
    owner: &NativeHotRecoveryMaintenance,
    job: &AllocationJob,
    role: &RecoveryRoleAdmission,
    parent: &CancellationToken,
    effective: &CancellationToken,
    actual_slot_check: impl FnOnce() -> Result<()>,
) {
    let selected = SELECTED.with(|s| {
        let mut s = s.borrow_mut();
        if s.as_ref()
            .is_some_and(|s| s.owner == owner as *const _ as usize)
        {
            s.take()
        } else {
            None
        }
    });
    let Some(selected) = selected else {
        return;
    };
    let facts = Facts {
        nonce: job.scope.recovery_nonce,
        phase: job.phase.name(),
        position: job.position,
        members: job.members.len(),
        node: job.members[job.position - 1].clone(),
        parent_live: parent.check().is_ok() && selected.original.check().is_ok(),
        role_live: role.require_live_request().is_ok(),
        effective_live: effective.check().is_ok(),
        slot_live: actual_slot_check().is_ok(),
        parent_deadline: parent.deadline(),
        role_deadline: role.deadline(),
        effective_deadline: effective.deadline(),
        effective_not_after_parent: effective
            .deadline()
            .zip(parent.deadline())
            .is_some_and(|(a, b)| a <= b),
    };
    let fire = facts.parent_live && facts.role_live && facts.effective_live && facts.slot_live;
    // Immutable observations BEFORE action. No formatting/JSON/channel/IO.
    selected.facts.set(Some(facts));
    // Actual State and range reservation remain owned by the original turn.
    if fire && selected.mode == 1 {
        selected.original.cancel();
    }
    if fire && selected.mode == 2 {
        while effective.check().is_ok() {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
impl NativeHotRecoveryMaintenance {
    fn progress(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let state = loop {
            parent.check()?;
            if let Some(state) = self.state.try_lock_for(Duration::from_millis(5)) {
                break state;
            }
        };
        let actual = state.allocation_jobs.get(&range).map(|job| {
            (
                job.phase.name(),
                job.position,
                job.members.clone(),
                job.scope.recovery_nonce,
                job.last_actual.clone(),
                job.peer_opens.len(),
                job.first_error_kind,
            )
        });
        drop(state); // no JSON/formatter/IO under the actual State latch
        let job=actual.map(|(phase,position,members,nonce,last_actual,opens,error)|serde_json::json!({"phase":phase,"position":position,"members":members,"nonce":nonce,"last_actual":last_actual,"same_peer_opens":opens,"first_error_kind":error}));
        Ok(
            serde_json::json!({"owner":self as *const _ as usize,"state":&self.state as *const _ as usize,"job":job}),
        )
    }
}
impl NativeMetadataControlHost {
    pub(in crate::distribution_native_host) fn test_actual_allocation_phase_return(
        &self,
        range: crate::RangeId,
        mode: u8,
        parent: &CancellationToken,
    ) -> Result<(Result<()>, Result<serde_json::Value>)> {
        parent.check()?;
        if parent.deadline().is_none() {
            return Err(invalid(
                "actual phase control requires original finite caller",
            ));
        }
        if !matches!(mode, 1 | 2) {
            return Err(invalid("actual allocation phase control mode invalid"));
        }
        let data = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("actual phase control INITIAL DATA absent"))?;
        let hot = data
            .hot_recovery
            .as_ref()
            .ok_or_else(|| invalid("actual phase control original maintenance absent"))?;
        let before = hot.progress(range, parent); // diagnostic failure does not hide call
        let (selection, facts) = arm(hot, mode, parent);
        let actual = hot.tick_for_request(parent);
        let returned_at = Instant::now();
        let parent_dead_at_return = parent.check().is_err();
        drop(selection);
        // Keep original typed Result BEFORE any fallible diagnostic/formatting.
        let _ = writeln!(
            std::io::stderr().lock(),
            "NATIVE_ALLOCATION_PHASE_ORIGINAL mode={mode} actual={actual:?}"
        );
        let observed = facts.take();
        // Fresh finite DIAGNOSTIC context only; never renew the actual turn.
        let diagnostic = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_millis(500));
        let after = hot.progress(range, &diagnostic);
        let diagnostics = (|| -> Result<serde_json::Value> {
            let fact = observed.as_ref().map(Facts::data);
            let before = before?;
            let after = after?;
            Ok(
                serde_json::json!({"before":before,"after":after,"actual_cut":fact,"mode":mode,"original_parent_expired_or_canceled_at_return":parent_dead_at_return,"effective_expired_at_return":observed.as_ref().and_then(|f|f.effective_deadline).is_some_and(|d|returned_at>=d),"returned_at":format!("{returned_at:?}"),"control_scope":"exact selected real maintenance owner; State and range reservation retained by actual call","DATA_only":true}),
            )
        })();
        Ok((actual, diagnostics))
    }
}
