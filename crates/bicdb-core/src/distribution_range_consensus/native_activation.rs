//! Durable same-voter activation. No decoder, caller list or stage alone arms
//! an open: Runtime must still consume the actual owned pin/freeze LAST.
use super::*;
use crate::distribution_range_recovery::{NativeCompletionQuorum, RecoveryRoleAdmission};
use native_ready_completion::NativeCompletedPoint;
use recovery_state::RecoveryFencePhase;
pub(super) const NATIVE_ACTIVATED_FORMAT: u32 = 9;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeActivatedPoint {
    pub(super) completed: NativeCompletedPoint,
    pub(super) completed_catalog_sha256: String,
    pub(super) completion_quorum_sha256: String,
    pub(super) native_activation_sha256: String,
}
impl NativeActivatedPoint {
    pub(super) fn validate(&self) -> Result<()> {
        self.completed.validate()?;
        if [
            &self.completed_catalog_sha256,
            &self.completion_quorum_sha256,
            &self.native_activation_sha256,
        ]
        .iter()
        .any(|s| s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(range_write_error("damaged native activation commitment"));
        }
        Ok(())
    }
    pub(super) fn checksum(&self) -> Result<String> {
        self.validate()?;
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(self)?)))
    }
}
#[derive(Debug)]
pub(crate) struct NativeActivationReceipt {
    store_open_id: Uuid,
    scope: RecoveryFenceScope,
    activated: NativeActivatedPoint,
    native: crate::db::NativeRecoveredServingReceipt,
    native_decision: crate::db::NativeActivationDecisionReceipt,
    quorum: NativeCompletionQuorum,
}
impl NativeActivationReceipt {
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn activation_sha256(&self) -> Result<String> {
        self.activated.checksum()
    }
    pub(crate) fn native_serving_sha256(&self) -> &str {
        &self.activated.completed.native_serving_sha256
    }
    pub(crate) fn require_current(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        prepared: &PreparedNativeRecoveryReady,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.quorum
            .require_prepared_identity_point(prepared, fresh)?;
        self.scope.require_role(fresh)?;
        self.native.require_current(db)?;
        self.native_decision.require_current(db)?;
        if self.native_decision.checksum_sha256() != self.activated.native_activation_sha256
            || self.native_decision.completion_quorum_sha256()
                != self.activated.completion_quorum_sha256
            || self.native_decision.native_serving_sha256() != self.native.checksum_sha256()
            || !self.native_decision.scope().same_session(&self.scope)
        {
            return Err(range_write_error(
                "activation receipt differs from its protected native-WAL decision",
            ));
        }
        prepared.require_activated_materialization(db, store, fresh)?;
        store.require_native_activation_control(self.scope.range_id)?;
        if self.store_open_id != store.open_id
            || !self.scope.same_session(prepared.scope())
            || !matches!(&store.catalog.recovery_fences[&self.scope.range_id].phase,RecoveryFencePhase::NativeActivated(a) if a==&self.activated)
        {
            return Err(range_write_error(
                "activation receipt has stale actual native/store/session identity",
            ));
        }
        Ok(())
    }
}
impl RangeWriteStore {
    fn require_native_activation_control(&self, range: RangeId) -> Result<()> {
        self.require_durable_recovery_catalog()?;
        self.require_no_image_install(range)?;
        if self.catalog.handoff_promises.contains_key(&range)
            || self.catalog.backup_fences.contains_key(&range)
        {
            return Err(range_write_error(
                "same-voter activation cannot clear handoff or backup refusal",
            ));
        }
        Ok(())
    }
    pub(crate) fn activate_native_recovery(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<NativeActivationReceipt> {
        self.activate_native_recovery_with(
            db,
            prepared,
            quorum,
            fresh,
            crate::storage::write_atomic,
        )
    }
    #[cfg(test)]
    pub(crate) fn test_activate_native_recovery_with(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<NativeActivationReceipt> {
        self.activate_native_recovery_with(db, prepared, quorum, fresh, write)
    }
    fn activate_native_recovery_with(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<NativeActivationReceipt> {
        quorum.require_prepared_identity_point(prepared, fresh)?;
        let range = prepared.scope().range_id;
        if self.recovery_persistence_uncertain
            && matches!(
                self.catalog.recovery_fences.get(&range).map(|r| &r.phase),
                Some(RecoveryFencePhase::NativeActivated(_))
            )
        {
            prepared.validate_activated_refsync(db, self, fresh)?;
            self.persist_recovery_catalog()?;
        }
        self.require_native_activation_control(range)?;
        let (activated, native, native_decision) = match &self
            .catalog
            .recovery_fences
            .get(&range)
            .ok_or_else(|| range_write_error("activation has no local completed record"))?
            .phase
        {
            RecoveryFencePhase::NativeCompleted(completed) => {
                let native = prepared.require_completed_for_activation(db, self, fresh)?;
                let native_decision =
                    db.persist_native_activation_decision(prepared, self, &quorum, fresh)?;
                let activated = NativeActivatedPoint {
                    completed: completed.clone(),
                    completed_catalog_sha256: self.recovery_range_catalog_sha256(range)?,
                    completion_quorum_sha256: native_decision.completion_quorum_sha256().to_owned(),
                    native_activation_sha256: native_decision.checksum_sha256().to_owned(),
                };
                (activated, native, native_decision)
            }
            RecoveryFencePhase::NativeActivated(activated) => {
                // Exact idempotent re-fsync before any new open admission. The
                // same immutable completion is not replaced by another list.
                let activated = activated.clone();
                if self.recovery_persistence_uncertain {
                    self.persist_recovery_catalog()?;
                }
                let native = prepared.require_activated_materialization(db, self, fresh)?;
                let p = &activated.completed.point;
                let native_decision = db.observe_native_activation_decision_for_role(
                    fresh,
                    prepared.scope(),
                    &activated.native_activation_sha256,
                    &activated.completed.native_serving_sha256,
                    p.range_index(),
                    p.logical_genesis_sha256(),
                    p.logical_history_sha256(),
                )?;
                if native_decision.completion_quorum_sha256() != activated.completion_quorum_sha256
                {
                    return Err(range_write_error(
                        "actual native/cataloque activation quorum commitments disagree",
                    ));
                }
                (activated, native, native_decision)
            }
            _ => {
                return Err(range_write_error(
                    "activation requires actual owned CompletedFrozen state",
                ))
            }
        };
        activated.validate()?;
        self.native_activated_opens.remove(&range);
        self.recovery_persistence_uncertain = true;
        self.catalog.recovery_fences.get_mut(&range).unwrap().phase =
            RecoveryFencePhase::NativeActivated(activated.clone());
        self.catalog.format_version = self.catalog.format_version.max(NATIVE_ACTIVATED_FORMAT);
        native_ready_prepare::validate_native_prepared_catalog(&self.catalog)?;
        self.persist_recovery_catalog_with(write)?;
        let receipt = NativeActivationReceipt {
            store_open_id: self.open_id,
            scope: prepared.scope().clone(),
            activated,
            native,
            native_decision,
            quorum,
        };
        receipt.require_current(db, self, prepared, fresh)?;
        Ok(receipt)
    }
    pub(crate) fn arm_native_activation_open(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        receipt: &NativeActivationReceipt,
        fresh: &RecoveryRoleAdmission,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<()> {
        receipt.require_current(db, self, prepared, fresh)?;
        let guard = db.native_activated_open_guard(self, prepared, receipt, fresh, frozen)?;
        self.native_activated_opens
            .insert(receipt.scope.range_id, guard);
        Ok(())
    }
    pub(super) fn native_activated_open_allowed(&self, range: RangeId) -> Result<()> {
        self.require_native_activation_control(range)?;
        self.require_native_phase_format(range)?;
        let saved = self
            .catalog
            .recovery_fences
            .get(&range)
            .ok_or_else(|| range_write_error("no native activation record"))?;
        let RecoveryFencePhase::NativeActivated(activated) = &saved.phase else {
            return Err(range_write_error("range is not natively activated"));
        };
        let guard = self.native_activated_opens.get(&range).ok_or_else(|| {
            range_write_error("native activation requires actual current-open release proof")
        })?;
        guard.require_current(self.open_id, &saved.scope, &activated.checksum()?)
    }
    pub(crate) fn require_current_native_activation_decision(
        &self,
        quorum: &NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        quorum.require_recipient_role(fresh)?;
        self.native_activated_open_allowed(quorum.scope().range_id)?;
        let saved = &self.catalog.recovery_fences[&quorum.scope().range_id];
        let RecoveryFencePhase::NativeActivated(a) = &saved.phase else {
            unreachable!("checked activation")
        };
        if !saved.scope.same_session(quorum.scope())
            || a.completed.point.range_index() != quorum.range_index()
            || a.completed.point.logical_genesis_sha256() != quorum.logical_genesis_sha256()
            || a.completed.point.logical_history_sha256() != quorum.logical_history_sha256()
        {
            return Err(range_write_error(
                "durable local activation contradicts the actual completion quorum",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_actual_native_activated_open(&self, range: RangeId) -> Result<()> {
        self.native_activated_open_allowed(range)
    }
    pub(crate) fn invalidate_native_activation_open(&mut self, range: RangeId) {
        self.native_activated_opens.remove(&range);
    }
}
