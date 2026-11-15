//! Permissionless collection over an ALREADY counted actual preparation.
//! This borrow cannot attest physical preparation/current-history validity.
//! Those complete checks remain mandatory at scoped publication/recovery.
use super::*;
#[cfg(test)]
mod faults;

/// Comparison identity only. No getter exposes a writer, Frozen, DB or pin.
pub(crate) struct NativeJointPreparationCollectionIdentity {
    owner: crate::db::NativeJointPhaseOwner,
}
impl NativeJointPreparationCollectionIdentity {
    pub(crate) fn require_actual_lifetime(
        &self,
        actual: &ActualServiceNativeJointPreparedEvidence,
    ) -> Result<()> {
        actual.require_phase_writer_lifetime(&self.owner)?;
        actual.origin.require_actual(actual)
    }
    pub(crate) fn same_comparison(&self, other: &Self) -> bool {
        self.owner.same_actual(&other.owner)
    }
    pub(crate) fn is_same_owner(&self, actual: &crate::db::NativeJointPhaseOwner) -> bool {
        self.owner.same_actual(actual)
    }
}

/// The real checked DB borrow and SAME service/Store stay lexical. This type
/// is never a publication grant and may not cross metadata/network acquisition.
pub(crate) struct NativeJointPreparationCollectionBorrow<'a> {
    actual: &'a ActualServiceNativeJointPreparedEvidence,
    database: parking_lot::RwLockReadGuard<'a, BicDb>,
    fresh: &'a CommittedNativeJointDecisionObservation,
    history: PreparedHistory<'a>,
    owner: crate::db::NativeJointPhaseOwner,
}
impl ActualServiceNativeJointPreparedEvidence {
    pub(crate) fn borrow_preparation_collection<'a>(
        &'a self,
        service: &'a ClusterDataNodeService,
        fresh: &'a CommittedNativeJointDecisionObservation,
        custody: Option<&'a crate::distribution_range_handoff::NativeJointPreparationCustody>,
    ) -> Result<NativeJointPreparationCollectionBorrow<'a>> {
        let history = match custody {
            Some(custody) => PreparedHistory::Custody(custody),
            None => PreparedHistory::OriginalRequest,
        };
        history.require_collection_origin(self)?;
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        // Actual State/registration, root/open/Store, range/domain and A checks
        // are NOT replaced by a hash or an old successful preparation receipt.
        let owner =
            self.require_original_phase_owner_for_collection(&database, &service.range_writes)?;
        fresh.require_current(&database)?; // unchanged native DECISION proof
        let old = self.committed.metadata_anchor();
        let current = fresh.metadata_anchor();
        let prepared = self.prepared.metadata_anchor();
        if fresh.data() != self.committed.data()
            || fresh.native_receipt().checksum_sha256()
                != self.committed.native_receipt().checksum_sha256()
            || fresh.native_receipt().native_database_incarnation()
                != self.prepared.native_database_incarnation()
            || self.prepared.local_node_id() != &service.node_id
            || current.0 < old.0
            || current.1 < old.1
            || current.2 == old.2
            || current.0 < prepared.0
            || current.1 < prepared.1
            || current.2 == prepared.2
        {
            return Err(data_error(
                "fresh collection changed original decision/open/member/metadata",
            ));
        }
        let result = NativeJointPreparationCollectionBorrow {
            actual: self,
            database,
            fresh,
            history,
            owner,
        };
        result.require_request_memory()?;
        Ok(result)
    }
}
impl NativeJointPreparationCollectionBorrow<'_> {
    fn require_request_memory(&self) -> Result<()> {
        self.history.require_collection_origin(self.actual)?;
        self.fresh.require_current_metadata_only()
    }
    pub(crate) fn into_comparison_identity(
        self,
    ) -> Result<NativeJointPreparationCollectionIdentity> {
        self.require_request_memory()?;
        Ok(NativeJointPreparationCollectionIdentity { owner: self.owner })
    }
    /// Only the pin owned by this original evidence can be removed. Frozen
    /// stays borrowed/retained, and this creates no Controlled writer/receipt.
    pub(crate) fn drain_owned_capture_pin(self) -> Result<()> {
        let mut pin = self
            .actual
            .pin
            .try_lock()
            .ok_or_else(|| data_error("collection owned pin transition busy"))?;
        let gate = self.database.range_mutation_gate();
        let range = self.fresh.data().old_range().id;
        let frozen = self.actual.lease.try_frozen()?;
        gate.validate_quiescent_frozen(range, frozen)?;
        if let Some(actual_pin) = pin.as_ref() {
            gate.validate_source_capture_read_pin(range, actual_pin)?;
        }
        // No I/O or blocking lock acquisition follows before taking ownership.
        self.require_request_memory()?;
        let dropped = pin.take();
        drop(pin);
        // Its Drop locks the actual gate: NEVER hold that gate across Drop.
        drop(dropped);
        gate.validate_quiescent_frozen(range, frozen)?;
        self.require_request_memory()
    }
}
