//! Live source witness for ONLY the retained-C1 closed learner-image lane.
//! Weak owners and original cancellation cannot be replaced by audit bytes.
use super::*;
use crate::distribution_range_handoff::NativeJointOldImageProof;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use std::sync::Weak;

#[derive(Debug)]
pub(crate) struct ClosedNativeImageSource {
    capture: Weak<()>,
    database: Weak<RwLock<BicDb>>,
    database_incarnation: String,
    frozen: Weak<FrozenRangeMutationPermit>,
    pin: Weak<SourceCaptureReadPin>,
    old: Arc<NativeJointOldImageProof>,
    cancellation: CancellationToken,
    r: u64,
    h0: String,
    h: String,
    receipt: String,
    reserved: u64,
    namespaces: Vec<(String, Uuid)>,
    schema: String,
}
impl TentativeNativeRangeImageSourceAuthorization<'_> {
    pub(crate) fn closed_native_image_source(
        &self,
        role: &RecoveryRoleAdmission,
    ) -> Result<ClosedNativeImageSource> {
        self.require_current()?;
        let NativeRangeImageSourceCut::CommittedHandoff(cut) = self.cut() else {
            return Err(invalid(
                "closed C1 image requires actual committed handoff source",
            ));
        };
        let old = cut
            .native_old_image_proof()
            .ok_or_else(|| invalid("closed C1 image requires genuine native OLD closure quorum"))?
            .clone();
        old.require_current(role, self.lineage())?;
        let result = ClosedNativeImageSource {
            capture: Arc::downgrade(&self.capture.closed_image_lifetime),
            database: Arc::downgrade(&self.capture.db),
            database_incarnation: read_database(&self.capture.db)?.incarnation_id().to_owned(),
            frozen: Arc::downgrade(&self.capture.frozen),
            pin: Arc::downgrade(&self.capture.source_read_pin),
            old,
            cancellation: self.capture.original_cancellation.clone(),
            r: self.lineage().range_index(),
            h0: self.lineage().logical_genesis_sha256().into(),
            h: self.lineage().logical_history_sha256().into(),
            receipt: self.native_receipt_sha256().into(),
            reserved: self.capture.reserved.0,
            namespaces: self.capture.bindings.namespace_ids(),
            schema: self.capture.bindings.schema_sha256().into(),
        };
        result.require_current(role)?;
        Ok(result)
    }
}
impl ClosedNativeImageSource {
    pub(crate) fn require_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        self.cancellation.check()?;
        self.old.require_role(role)
    }
    /// Re-derive the actual source point on every row/transaction boundary.
    /// This returns predecessor configuration DATA only, never APP/Ready.
    pub(crate) fn require_current(
        &self,
        role: &RecoveryRoleAdmission,
    ) -> Result<crate::RangeDescriptor> {
        self.cancellation.check()?;
        role.require_live_request()?;
        let _capture = self
            .capture
            .upgrade()
            .ok_or_else(|| invalid("original closed image capture ended"))?;
        let database = self
            .database
            .upgrade()
            .ok_or_else(|| invalid("closed image source DB owner ended"))?;
        let frozen = self
            .frozen
            .upgrade()
            .ok_or_else(|| invalid("closed image source freeze ended"))?;
        let pin = self
            .pin
            .upgrade()
            .ok_or_else(|| invalid("closed image source pin ended"))?;
        let lineage = {
            let db = read_database(&database)?;
            if db.incarnation_id() != self.database_incarnation {
                return Err(invalid("closed image source DB open changed"));
            }
            let gate = db.range_mutation_gate();
            gate.validate_quiescent_frozen(role.current_range().id, &frozen)?;
            gate.validate_source_capture_read_pin(role.current_range().id, &pin)?;
            let actual = db.current_native_range_source_lineage(role.current_range().id)?;
            let bindings = db.capture_range_image_bindings()?;
            if actual.range_index() != self.r
                || actual.logical_genesis_sha256() != self.h0
                || actual.logical_history_sha256() != self.h
                || actual.native_receipt_sha256() != self.receipt
                || bindings.namespace_ids() != self.namespaces
                || bindings.schema_sha256() != self.schema
                || db.replication_shippable_seq() < self.reserved
            {
                return Err(invalid(
                    "closed image source point/schema/UID/shippable prefix changed",
                ));
            }
            // Source origin slots are covered ONLY by actual materialized
            // committed commands in this frozen spine. Promise/no-effect or
            // other unfinished domains need a distinct qualified producer.
            for slot in db
                .native_abort_slot_inventory()?
                .iter()
                .filter(|slot| slot.command.range_id == role.current_range().id)
            {
                if !slot.ever_commit
                    || slot.no_effect_sha256.is_some()
                    || slot.abort_promise_sha256.is_some()
                    || slot.command.index > self.r
                {
                    return Err(invalid(
                        "closed C1 image source has unsupported protected slot obligations",
                    ));
                }
                db.require_native_command_materialized(&actual, &slot.command)?;
            }
            actual
        };
        // No source DB or native journal guard is held across service checks.
        self.old.require_current(role, &lineage)?;
        self.cancellation.check()?;
        role.require_live_request()?;
        Ok(lineage.native_write_range()?.clone())
    }
}
