//! Actual remote ALL-prepared native bridge. Retention is private attempt
//! custody, not a receipt derived from CURRENT bytes or an unknown IO result.
use super::*;
use crate::distribution_range_handoff::{
    NativeRemoteBridgeAdmission, NativeRemoteBridgeReadAdmission,
};

enum NativeOutcome {
    Fresh,
    Entered,
    Known(u64),
    Unknown,
}
pub(crate) struct NativeRemoteJointBridgeAttempt {
    database_open: String,
    store_open: Uuid,
    frozen_owner: Uuid,
    range_id: RangeId,
    decision_sha256: String,
    preparation_sha256: String,
    candidate: Option<NativeJointServingBridgeRecord>,
    outcome: NativeOutcome,
    issued_xid: Option<u64>,
    error: Option<super::super::remote_preparation::NativePreparationError>,
    #[cfg(test)]
    mode: u8,
    #[cfg(test)]
    cancel_fired: u64,
    #[cfg(test)]
    sync_fired: u64,
    #[cfg(test)]
    cancel_event: Option<serde_json::Value>,
}
impl NativeRemoteJointBridgeAttempt {
    pub(crate) fn retain_original(
        db: &BicDb,
        store: &RangeWriteStore,
        prepared: &NativeJointServingPreparation,
        frozen: &FrozenRangeMutationPermit,
        admission: &NativeRemoteBridgeAdmission<'_>,
    ) -> Result<Self> {
        admission.require_live()?;
        admission.require_native_local(db, store, prepared, frozen)?;
        Ok(Self {
            database_open: db.incarnation_id().into(),
            store_open: store.native_writer_store_open_id(),
            frozen_owner: frozen.owner(),
            range_id: admission.committed().data().old_range().id,
            decision_sha256: admission.committed().data().checksum_sha256().into(),
            preparation_sha256: prepared.checksum_sha256().into(),
            candidate: None,
            outcome: NativeOutcome::Fresh,
            issued_xid: None,
            error: None,
            #[cfg(test)]
            mode: 0,
            #[cfg(test)]
            cancel_fired: 0,
            #[cfg(test)]
            sync_fired: 0,
            #[cfg(test)]
            cancel_event: None,
        })
    }
    fn require_original(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        frozen: &FrozenRangeMutationPermit,
        prepared: &NativeJointServingPreparation,
        committed: &CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        if self.database_open != db.incarnation_id()
            || self.store_open != store.native_writer_store_open_id()
            || self.frozen_owner != frozen.owner()
            || self.range_id != committed.data().old_range().id
            || self.decision_sha256 != committed.data().checksum_sha256()
            || self.preparation_sha256 != prepared.checksum_sha256()
        {
            return Err(invalid(
                "remote bridge changed original DB/Store/Frozen attempt",
            ));
        }
        Ok(())
    }
    fn require_fresh(&self) -> Result<()> {
        if !matches!(self.outcome, NativeOutcome::Fresh)
            || self.candidate.is_some()
            || self.issued_xid.is_some()
        {
            return Err(invalid(
                "remote bridge native attempt already entered; no WAL replay",
            ));
        }
        Ok(())
    }
    fn retain_issued(&mut self, xid: u64) -> Result<()> {
        self.require_fresh()?;
        self.issued_xid = Some(xid);
        self.outcome = NativeOutcome::Entered;
        Ok(())
    }
    fn retain_candidate(&mut self, record: &NativeJointServingBridgeRecord) -> Result<()> {
        if self.candidate.is_some()
            || self.issued_xid != Some(record.native_page_xid)
            || !matches!(self.outcome, NativeOutcome::Entered)
        {
            return Err(invalid(
                "remote bridge candidate differs from actual issued transaction",
            ));
        }
        record.validate()?;
        self.candidate = Some(record.clone());
        Ok(())
    }
    fn retain_return(&mut self, result: &Result<Option<u64>>) {
        match result {
            Ok(Some(xid))
                if self.issued_xid == Some(*xid)
                    && self
                        .candidate
                        .as_ref()
                        .is_some_and(|r| r.native_page_xid == *xid) =>
            {
                self.outcome = NativeOutcome::Known(*xid)
            }
            Err(error) => {
                self.outcome = NativeOutcome::Unknown;
                self.error =
                    Some(super::super::remote_preparation::NativePreparationError::capture(error));
            }
            _ => self.outcome = NativeOutcome::Unknown,
        }
    }
    fn known(&self) -> Result<&NativeJointServingBridgeRecord> {
        match (&self.outcome,&self.candidate) {
            (NativeOutcome::Known(xid),Some(record)) if record.native_page_xid==*xid && self.issued_xid==Some(*xid)=>Ok(record),
            _=>Err(invalid("remote bridge refuses native Unknown/unframed outcome; visible bytes are not IO success")),
        }
    }
}
impl BicDb {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_remote_native_joint_bridge(
        &self,
        admission: &NativeRemoteBridgeAdmission<'_>,
        prepared: &NativeJointServingPreparation,
        store: &RangeWriteStore,
        catalogue: &NativeJointBridgeCatalogue,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
        attempt: &mut NativeRemoteJointBridgeAttempt,
    ) -> Result<NativeJointServingBridgeReceipt> {
        parent.check()?;
        admission.require_live()?;
        attempt.require_original(self, store, frozen, prepared, admission.committed())?;
        admission.require_native_local(self, store, prepared, frozen)?;
        let original = admission.committed();
        original.require_current(self)?;
        let scope =
            prepared.publication_read_scope(self, store, original, catalogue, image, frozen)?;
        admission.require_scope(&scope, store)?;
        original.require_native_publication_metadata(self, store)?;
        if !self.config.fsync || !format::native_joint_data_handoff_active(self.data_path())? {
            return Err(invalid(
                "remote bridge requires actual fsync and existing reader fence",
            ));
        }
        // An earlier known native return is recovered only by the separate
        // read method. Missing CURRENT cannot authorize replay after effects.
        attempt.require_fresh()?;
        if scope.actual_existing_bridge()?.is_some() {
            return Err(invalid(
                "remote bridge CURRENT exists without this original known attempt",
            ));
        }
        let (term, index, context) = original.metadata_anchor();
        let mut record = NativeJointServingBridgeRecord {
            version: 1,
            decision: original.data().clone(),
            prepared: prepared.record.clone(),
            new_prepared: admission.votes(),
            post_quorum_metadata_context_id: context,
            post_quorum_metadata_term: term,
            post_quorum_metadata_read_index: index,
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        parent.check()?;
        admission.require_scope(&scope, store)?;
        let controlled = self.range_mutations.try_controlled_mutation(frozen)?;
        self.range_mutations.validate(
            record.prepared.range_id,
            &RangeMutationAuthority::Controlled(&controlled),
        )?;
        admission.require_scope(&scope, store)?;
        #[cfg(test)]
        let mut actual_sync_fault = None;
        let actual = self.in_paged_transaction(|paged, xid| {
            attempt.retain_issued(xid)?;
            parent.check()?;
            admission.require_live()?;
            original.require_native_publication_metadata(self, store)?;
            record.native_page_xid = xid;
            record.checksum_sha256 = record.checksum()?;
            record.validate()?;
            let archive = archive_key(record.prepared.range_id, &record.checksum_sha256);
            if self
                .read_native_range_image_record::<NativeJointServingBridgeRecord>(&archive)?
                .is_some()
            {
                return Err(invalid(
                    "remote bridge immutable archive already exists without its CURRENT",
                ));
            }
            let bytes = bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?;
            attempt.retain_candidate(&record)?;
            for name in [archive, key(record.prepared.range_id)] {
                paged
                    .store()
                    .put(xid, &native_key(&name), &bytes)
                    .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
            }
            #[cfg(test)]
            if attempt.mode == 2 {
                attempt.mode = 0;
                actual_sync_fault = Some(bicdb_page::test_faults::arm_io_failure(
                    "page_log.sync",
                    self.data_path(),
                    1,
                    false,
                ));
            }
            Ok(())
        });
        // Actual API return is retained before any outer readback or check.
        attempt.retain_return(&actual);
        #[cfg(test)]
        {
            attempt.sync_fired = attempt
                .sync_fired
                .saturating_add(actual_sync_fault.as_ref().map(|f| f.fired()).unwrap_or(0));
            if attempt.mode == 1 && matches!(attempt.outcome, NativeOutcome::Known(_)) {
                attempt.mode = 0;
                attempt.cancel_fired = attempt.cancel_fired.saturating_add(1);
                attempt.cancel_event = Some(admission.test_cancel_original_server_request());
            }
        }
        #[cfg(test)]
        drop(actual_sync_fault);
        // Cancellation is observed before receipt construction/readback; the
        // exact actual API return above remains retained in either case.
        actual?.ok_or_else(|| invalid("remote bridge requires actual native page store"))?;
        parent.check()?;
        admission.require_live()?;
        let receipt = scope.receipt_from_actual(record)?;
        drop(controlled);
        scope.require_bridge_postcondition(&receipt, None)?;
        admission.require_scope(&scope, store)?;
        original.require_native_publication_metadata(self, store)?;
        self.range_mutations
            .validate_quiescent_frozen(prepared.record.range_id, frozen)?;
        parent.check()?;
        admission.require_live()?;
        let guard = original.native_publication_guard()?;
        parent.check()?;
        admission.require_live()?;
        drop(guard);
        Ok(receipt)
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn read_known_remote_native_joint_bridge(
        &self,
        admission: &NativeRemoteBridgeReadAdmission<'_>,
        prepared: &NativeJointServingPreparation,
        store: &RangeWriteStore,
        catalogue: &NativeJointBridgeCatalogue,
        image: Option<&dyn NativeJointImageMaterialization>,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
        attempt: &NativeRemoteJointBridgeAttempt,
    ) -> Result<NativeJointServingBridgeReceipt> {
        parent.check()?;
        admission.require_live()?;
        attempt.require_original(self, store, frozen, prepared, admission.committed())?;
        let original = admission.committed();
        let record = attempt.known()?.clone();
        admission.require_native_local(self, store, prepared, frozen)?;
        if record.decision != *original.data() || record.prepared != prepared.record {
            return Err(invalid(
                "known remote bridge changed original D/preparation",
            ));
        }
        let scope =
            prepared.publication_read_scope(self, store, original, catalogue, image, frozen)?;
        admission.require_scope(&scope, store)?;
        let actual = scope
            .actual_existing_bridge()?
            .ok_or_else(|| invalid("known remote bridge CURRENT missing"))?;
        if actual != record {
            return Err(invalid(
                "known remote bridge original CURRENT/archive audit changed",
            ));
        }
        let receipt = scope.receipt_from_actual(record)?;
        scope.require_bridge_postcondition(&receipt, None)?;
        admission.require_scope(&scope, store)?;
        original.require_native_publication_metadata(self, store)?;
        parent.check()?;
        admission.require_live()?;
        let guard = original.native_publication_guard()?;
        parent.check()?;
        admission.require_live()?;
        drop(guard);
        Ok(receipt)
    }
}

#[cfg(test)]
impl NativeRemoteJointBridgeAttempt {
    pub(crate) fn test_outcome(&self) -> serde_json::Value {
        let outcome = match self.outcome {
            NativeOutcome::Fresh => "fresh",
            NativeOutcome::Entered => "native_entered_unknown",
            NativeOutcome::Known(_) => "actual_native_returned_known",
            NativeOutcome::Unknown => "actual_native_returned_unknown",
        };
        serde_json::json!({"actual_return":outcome,"actual_issued_xid":self.issued_xid,
            "original_candidate":self.candidate,"actual_error":self.error,
            "actual_cancel_after_native_return_fired":self.cancel_fired,"actual_cancel_event":self.cancel_event,
            "actual_commit_sync_fault_fired":self.sync_fired,"DATA_only":true})
    }
}

impl NativeRemoteJointBridgeAttempt {
    pub(crate) fn require_known_receipt(
        &self,
        receipt: &NativeJointServingBridgeReceipt,
    ) -> Result<()> {
        let original = self.known()?;
        if &receipt.record != original {
            return Err(invalid(
                "bridge resolution substituted exact original native candidate/receipt",
            ));
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_arm(&mut self, mode: u8) -> Result<()> {
        self.require_fresh()?;
        if !matches!(mode, 1 | 2) || self.mode != 0 {
            return Err(invalid(
                "bridge fault must arm untouched original native attempt once",
            ));
        }
        self.mode = mode;
        Ok(())
    }
}
