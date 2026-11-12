//! Durable completion remains FENCED. A separate actual completion quorum and
//! independently fresh authority are required for later activation. No method
//! in this module releases the owned range freeze or authorizes strong reads.
use super::*;
use crate::db::NativeRecoveredServingReceipt;
use crate::distribution_range_recovery::{ClosedNativeReadyQuorum, RecoveryRoleAdmission};
use native_ready_prepare::NativePreparedPoint;
use recovery_state::RecoveryFencePhase;

pub(super) const NATIVE_COMPLETED_CATALOG_FORMAT: u32 = 8;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeCompletedPoint {
    pub(super) point: NativePreparedPoint,
    pub(super) native_serving_sha256: String,
}
impl NativeCompletedPoint {
    pub(super) fn validate(&self) -> Result<()> {
        self.point.validate()?;
        if self.native_serving_sha256.len() != 64
            || !self
                .native_serving_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(range_write_error(
                "damaged native-completed serving binding",
            ));
        }
        Ok(())
    }
}

/// Actual native-WAL + catalogue persistence, STILL FROZEN. There is no serde,
/// Clone or constructor accepting a decoded record or caller's ready-voter list.
#[derive(Debug)]
pub(crate) struct NativeRecoveryCompletedFrozenReceipt {
    store_open_id: Uuid,
    scope: RecoveryFenceScope,
    completed: NativeCompletedPoint,
    native: NativeRecoveredServingReceipt,
}
impl NativeRecoveryCompletedFrozenReceipt {
    pub(crate) fn native_serving_sha256(&self) -> &str {
        &self.completed.native_serving_sha256
    }
    pub(crate) fn require_current(
        &self,
        db: &crate::BicDb,
        store: &RangeWriteStore,
        prepared: &PreparedNativeRecoveryReady,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<()> {
        self.scope.require_role(fresh)?;
        prepared.require_current_under_role(db, store, fresh)?;
        self.native.require_current(db)?;
        if self.store_open_id != store.open_id
            || !prepared.scope().same_session(&self.scope)
            || self.native.checksum_sha256() != self.completed.native_serving_sha256
            || !matches!(&store.catalog.recovery_fences[&self.scope.range_id].phase,
                RecoveryFencePhase::NativeCompleted(saved) if saved == &self.completed)
        {
            return Err(range_write_error(
                "completed-frozen receipt is stale or from another actual store",
            ));
        }
        Ok(())
    }
}
enum NativeCompletionDecision {
    Closed(ClosedNativeReadyQuorum),
    Completed(crate::distribution_range_recovery::NativeCompletionQuorum),
    Activated(crate::distribution_range_recovery::ActivatedNativeCompletionDecision),
}
impl NativeCompletionDecision {
    fn require_recipient_role(&self, f: &RecoveryRoleAdmission) -> Result<()> {
        match self {
            Self::Closed(q) => q.require_recipient_role(f),
            Self::Completed(q) => q.require_recipient_role(f),
            Self::Activated(q) => q.require_recipient_role(f),
        }
    }
    fn require_local_prepared(
        &self,
        p: &PreparedNativeRecoveryReady,
        db: &crate::BicDb,
        s: &RangeWriteStore,
        f: &RecoveryRoleAdmission,
    ) -> Result<()> {
        match self {
            Self::Closed(q) => q.require_local_prepared(p, db, s, f),
            Self::Completed(q) => q.require_local_prepared(p, db, s, f),
            Self::Activated(q) => q.require_local_prepared(p, db, s, f),
        }
    }
    fn persist_native(
        &self,
        p: &PreparedNativeRecoveryReady,
        db: &crate::BicDb,
        s: &RangeWriteStore,
        f: &RecoveryRoleAdmission,
    ) -> Result<NativeRecoveredServingReceipt> {
        match self {
            Self::Closed(q) => db.persist_native_recovered_serving_configuration(p, s, q, f),
            Self::Completed(q) => db.persist_native_serving_from_completion_decision(p, s, q, f),
            Self::Activated(q) => db.persist_native_serving_from_activated_decision(p, s, q, f),
        }
    }
}
impl RangeWriteStore {
    pub(crate) fn complete_native_recovery_frozen(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: ClosedNativeReadyQuorum,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<NativeRecoveryCompletedFrozenReceipt> {
        self.complete_native_recovery_frozen_with(
            db,
            prepared,
            quorum,
            fresh,
            crate::storage::write_atomic,
        )
    }
    #[cfg(test)]
    pub(crate) fn test_complete_native_recovery_frozen_with(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: ClosedNativeReadyQuorum,
        fresh: &RecoveryRoleAdmission,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<NativeRecoveryCompletedFrozenReceipt> {
        self.complete_native_recovery_frozen_with(db, prepared, quorum, fresh, write)
    }
    fn complete_native_recovery_frozen_with(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: ClosedNativeReadyQuorum,
        fresh: &RecoveryRoleAdmission,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<NativeRecoveryCompletedFrozenReceipt> {
        self.persist_native_completion_decision_with(
            db,
            prepared,
            NativeCompletionDecision::Closed(quorum),
            fresh,
            write,
        )
    }
    pub(crate) fn adopt_native_completion_decision(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: crate::distribution_range_recovery::NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<NativeRecoveryCompletedFrozenReceipt> {
        self.persist_native_completion_decision_with(
            db,
            prepared,
            NativeCompletionDecision::Completed(quorum),
            fresh,
            crate::storage::write_atomic,
        )
    }
    pub(crate) fn adopt_native_activated_decision(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        decision: crate::distribution_range_recovery::ActivatedNativeCompletionDecision,
        fresh: &RecoveryRoleAdmission,
    ) -> Result<NativeRecoveryCompletedFrozenReceipt> {
        self.persist_native_completion_decision_with(
            db,
            prepared,
            NativeCompletionDecision::Activated(decision),
            fresh,
            crate::storage::write_atomic,
        )
    }
    fn persist_native_completion_decision_with(
        &mut self,
        db: &crate::BicDb,
        prepared: &PreparedNativeRecoveryReady,
        quorum: NativeCompletionDecision,
        fresh: &RecoveryRoleAdmission,
        write: impl FnOnce(&Path, &[u8], bool) -> Result<()>,
    ) -> Result<NativeRecoveryCompletedFrozenReceipt> {
        #[cfg(test)]
        let mut quorum_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-final-local-quorum-check",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        quorum.require_local_prepared(prepared, db, self, fresh)?;
        #[cfg(test)]
        {
            quorum_trace.complete();
            drop(quorum_trace);
        }
        // FIRST: actual native-WAL configuration, while Prepared blocks writes.
        #[cfg(test)]
        let mut native_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-native-serving-producer",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        let native = quorum.persist_native(prepared, db, self, fresh)?;
        #[cfg(test)]
        {
            native_trace.complete();
            drop(native_trace);
            eprintln!(
                "ACTUAL_RECOVERY_COMPLETE_NATIVE_PERSISTED node={} nonce={} R={} serving_sha={}",
                fresh.local_node_id(),
                prepared.scope().recovery_nonce,
                prepared.range_index(),
                native.checksum_sha256()
            );
        }
        let scope = prepared.scope().clone();
        let completed = NativeCompletedPoint {
            point: prepared.actual_prepared_point(),
            native_serving_sha256: native.checksum_sha256().to_owned(),
        };
        completed.validate()?;
        #[cfg(test)]
        let mut pre_catalog_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-before-catalogue-local-quorum-check",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        quorum.require_local_prepared(prepared, db, self, fresh)?;
        #[cfg(test)]
        {
            pre_catalog_trace.complete();
            drop(pre_catalog_trace);
        }
        self.recovery_persistence_uncertain = true;
        self.catalog
            .recovery_fences
            .get_mut(&scope.range_id)
            .ok_or_else(|| range_write_error("completion lost its actual refusal"))?
            .phase = RecoveryFencePhase::NativeCompleted(completed.clone());
        self.catalog.format_version = self
            .catalog
            .format_version
            .max(NATIVE_COMPLETED_CATALOG_FORMAT);
        native_ready_prepare::validate_native_prepared_catalog(&self.catalog)?;
        // SECOND: completion is durable, still an ordinary-write refusal.
        // On any error retain this phase + uncertain flag; never roll it back
        // to Active or infer durability from the visible JSON rename.
        #[cfg(test)]
        let mut persist_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-catalogue-fsync",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        self.persist_recovery_catalog_with(write)?;
        #[cfg(test)]
        {
            persist_trace.complete();
            drop(persist_trace);
            eprintln!("ACTUAL_RECOVERY_COMPLETE_CATALOGUE_PERSISTED node={} nonce={} point={:?} serving_sha={} uncertain={}", fresh.local_node_id(), scope.recovery_nonce, completed.point, completed.native_serving_sha256, self.recovery_persistence_uncertain);
        }
        #[cfg(test)]
        let mut refresh_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-refresh-prepared-binding",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        prepared.refresh_after_durable_native_completion(db, self, fresh)?;
        #[cfg(test)]
        {
            refresh_trace.complete();
            drop(refresh_trace);
        }
        let receipt = NativeRecoveryCompletedFrozenReceipt {
            store_open_id: self.open_id,
            scope,
            completed,
            native,
        };
        #[cfg(test)]
        let mut receipt_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::from_deadline(
                "recovery-complete-receipt-current",
                fresh.local_node_id(),
                fresh.deadline(),
            );
        receipt.require_current(db, self, prepared, fresh)?;
        quorum.require_recipient_role(fresh)?;
        #[cfg(test)]
        {
            receipt_trace.complete();
            drop(receipt_trace);
        }
        Ok(receipt)
    }
}
