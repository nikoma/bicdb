//! Original native preparation attempt custody. No decoding, clone, native
//! admission, or preparation vote can be obtained from this record alone.
use super::*;

enum NativeReturn {
    NotEntered,
    NativeEntered,
    PutEntered,
    ReturnedOk(Option<u64>),
    ReturnedErr {
        details: Option<NativePreparationError>,
    },
}
#[derive(Serialize)]
pub(super) struct NativePreparationError {
    prefix: String,
    prefix_complete: bool,
    formatted_complete: bool,
    formatted_sha256: String,
}
impl NativePreparationError {
    pub(super) fn capture(error: &BicDbError) -> Self {
        use std::fmt::Write;
        struct Bounded {
            prefix: String,
            complete: bool,
            hash: sha2::Sha256,
        }
        impl std::fmt::Write for Bounded {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                self.hash.update(text.as_bytes());
                if !self.complete {
                    return Ok(());
                }
                let mut end = (4096 - self.prefix.len()).min(text.len());
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                self.prefix.push_str(&text[..end]);
                self.complete = end == text.len();
                Ok(())
            }
        }
        let mut actual = Bounded {
            prefix: String::new(),
            complete: true,
            hash: sha2::Sha256::new(),
        };
        let formatted_complete = write!(&mut actual, "{error}").is_ok();
        Self {
            prefix: actual.prefix,
            prefix_complete: actual.complete,
            formatted_complete,
            formatted_sha256: hex::encode(actual.hash.finalize()),
        }
    }
}
pub(crate) struct NativeRemoteJointPreparationAttempt {
    database_open: String,
    store_open: Uuid,
    frozen_owner: Uuid,
    decision_sha256: String,
    local_decision_sha256: String,
    candidate: Option<NativeJointServingPreparationRecord>,
    issued_xid: Option<u64>,
    actual_return: NativeReturn,
    verified_readback: bool,
    #[cfg(test)]
    test_mode: u8,
    #[cfg(test)]
    test_cancel_fired: u64,
    #[cfg(test)]
    test_sync_fault_fired: u64,
    #[cfg(test)]
    test_cancel_event: Option<serde_json::Value>,
}
impl NativeRemoteJointPreparationAttempt {
    /// Called only with the actual local committed decision, Store and lease.
    /// Retention is not a preparation or permission to cross a later gate.
    pub(crate) fn retain_original(
        db: &BicDb,
        store: &RangeWriteStore,
        committed: &CommittedNativeJointDecisionObservation,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<Self> {
        committed.require_current(db)?;
        committed.require_native_publication_metadata(db, store)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(committed.data().old_range().id, frozen)?;
        Ok(Self {
            database_open: db.incarnation_id().into(),
            store_open: store.native_writer_store_open_id(),
            frozen_owner: frozen.owner(),
            decision_sha256: committed.data().checksum_sha256().into(),
            local_decision_sha256: committed.native_receipt().checksum_sha256().into(),
            candidate: None,
            issued_xid: None,
            actual_return: NativeReturn::NotEntered,
            verified_readback: false,
            #[cfg(test)]
            test_mode: 0,
            #[cfg(test)]
            test_cancel_fired: 0,
            #[cfg(test)]
            test_sync_fault_fired: 0,
            #[cfg(test)]
            test_cancel_event: None,
        })
    }
    pub(super) fn require_entry(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        committed: &CommittedNativeJointDecisionObservation,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        if self.database_open != db.incarnation_id()
            || self.store_open != store.native_writer_store_open_id()
            || self.frozen_owner != frozen.owner()
            || self.decision_sha256 != committed.data().checksum_sha256()
            || self.local_decision_sha256 != committed.native_receipt().checksum_sha256()
        {
            return Err(invalid(
                "remote preparation replaced original DB/Store/lease/native decision",
            ));
        }
        match &self.actual_return {
            NativeReturn::NotEntered
                if (self.candidate.is_none() && self.issued_xid.is_none())
                    || self.verified_readback =>
            {
                Ok(())
            }
            NativeReturn::ReturnedOk(Some(xid))
                if self.issued_xid == Some(*xid)
                    && self
                        .candidate
                        .as_ref()
                        .is_some_and(|r| r.native_page_xid == *xid) =>
            {
                Ok(())
            }
            _ => Err(invalid(
                "remote preparation original native outcome unknown or unframed; no WAL replay",
            )),
        }
    }
    pub(super) fn require_candidate(
        &self,
        candidate: &NativeJointServingPreparationRecord,
    ) -> Result<()> {
        if self
            .candidate
            .as_ref()
            .is_some_and(|old| !old.same_fact(candidate))
        {
            return Err(invalid(
                "remote preparation changed original physical candidate",
            ));
        }
        Ok(())
    }
    pub(super) fn require_no_previous_effect(&self) -> Result<()> {
        if self.candidate.is_some()
            || self.issued_xid.is_some()
            || !matches!(self.actual_return, NativeReturn::NotEntered)
        {
            return Err(invalid(
                "remote preparation original CURRENT absent; cannot replay native attempt",
            ));
        }
        Ok(())
    }
    pub(super) fn retain_issued_xid(&mut self, xid: u64) -> Result<()> {
        self.require_no_previous_effect()?;
        self.issued_xid = Some(xid);
        self.actual_return = NativeReturn::NativeEntered;
        Ok(())
    }
    pub(super) fn enter_native(
        &mut self,
        db: &BicDb,
        frozen: &FrozenRangeMutationPermit,
        record: &NativeJointServingPreparationRecord,
    ) -> Result<()> {
        if self.candidate.is_some()
            || self.issued_xid != Some(record.native_page_xid)
            || !matches!(self.actual_return, NativeReturn::NativeEntered)
        {
            return Err(invalid(
                "remote preparation candidate differs from actual issued native transaction",
            ));
        }
        if self.database_open != db.incarnation_id() || self.frozen_owner != frozen.owner() {
            return Err(invalid(
                "remote preparation native callback changed original open/lease",
            ));
        }
        record.validate()?;
        self.candidate = Some(record.clone());
        self.actual_return = NativeReturn::PutEntered;
        Ok(())
    }
    pub(super) fn retain_native_return(&mut self, actual: &Result<Option<u64>>) {
        match actual {
            Ok(xid) => self.actual_return = NativeReturn::ReturnedOk(*xid),
            Err(error) => {
                // Record actual API Err BEFORE formatting/capture; a failed
                // formatter can never turn an unretained outcome into Fresh.
                self.actual_return = NativeReturn::ReturnedErr { details: None };
                let actual = NativePreparationError::capture(error);
                if let NativeReturn::ReturnedErr { details } = &mut self.actual_return {
                    *details = Some(actual);
                }
            }
        }
    }
    pub(super) fn original_known_record(&self) -> Result<&NativeJointServingPreparationRecord> {
        let candidate = self
            .candidate
            .as_ref()
            .ok_or_else(|| invalid("known preparation read lacks original native candidate"))?;
        if !self.verified_readback
            && !matches!(&self.actual_return,NativeReturn::ReturnedOk(Some(xid)) if self.issued_xid==Some(*xid) && *xid==candidate.native_page_xid)
        {
            return Err(invalid(
                "known preparation read refuses actual original native Unknown/unframed return",
            ));
        }
        Ok(candidate)
    }
    pub(super) fn require_readback(
        &self,
        record: &NativeJointServingPreparationRecord,
    ) -> Result<()> {
        if self
            .candidate
            .as_ref()
            .is_some_and(|original| original != record)
        {
            return Err(invalid(
                "remote preparation CURRENT differs from exact original issued candidate",
            ));
        }
        Ok(())
    }
    /// Only AFTER actual CURRENT/archive/native transaction verification.
    /// This does not rewrite the actual helper return or certify old fsync.
    pub(super) fn retain_verified_readback(
        &mut self,
        db: &BicDb,
        frozen: &FrozenRangeMutationPermit,
        record: &NativeJointServingPreparationRecord,
    ) -> Result<()> {
        self.require_readback(record)?;
        if self.database_open != db.incarnation_id() || self.frozen_owner != frozen.owner() {
            return Err(invalid(
                "remote preparation readback changed original open/lease",
            ));
        }
        if self.candidate.is_none() {
            if !matches!(self.actual_return, NativeReturn::NotEntered) {
                return Err(invalid(
                    "remote preparation cannot frame an unknown native return from readback",
                ));
            }
            self.candidate = Some(record.clone());
        }
        self.verified_readback = true;
        Ok(())
    }
    pub(crate) fn require_original_known_native_outcome(&self) -> Result<()> {
        self.original_known_record().map(|_| ())
    }
    #[cfg(test)]
    pub(crate) fn test_arm(&mut self, mode: u8) -> Result<()> {
        if !matches!(mode, 1 | 2) || self.test_mode != 0 || self.candidate.is_some() {
            return Err(invalid(
                "preparation native fault must arm original untouched actual attempt once",
            ));
        }
        self.test_mode = mode;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn test_take_sync_fault(&mut self) -> bool {
        if self.test_mode == 2 {
            self.test_mode = 0;
            true
        } else {
            false
        }
    }
    #[cfg(test)]
    pub(super) fn test_record_sync_fault(&mut self, fired: u64) {
        self.test_sync_fault_fired += fired;
    }
    #[cfg(test)]
    pub(super) fn test_record_cancel_event(&mut self, event: serde_json::Value) {
        self.test_cancel_event = Some(event);
    }
    #[cfg(test)]
    pub(super) fn test_take_cancel_after_return(&mut self) -> bool {
        if self.test_mode == 1
            && matches!(&self.actual_return,NativeReturn::ReturnedOk(Some(xid)) if self.candidate.as_ref().is_some_and(|r|r.native_page_xid==*xid))
        {
            self.test_mode = 0;
            self.test_cancel_fired += 1;
            true
        } else {
            false
        }
    }
    #[cfg(test)]
    pub(crate) fn test_outcome(&self) -> serde_json::Value {
        let outcome = match &self.actual_return {
            NativeReturn::NotEntered => serde_json::json!({"state":"native_not_entered"}),
            NativeReturn::NativeEntered => {
                serde_json::json!({"state":"actual_native_entered_before_candidate_unknown"})
            }
            NativeReturn::PutEntered => serde_json::json!({"state":"native_put_entered_unknown"}),
            NativeReturn::ReturnedOk(xid) => {
                serde_json::json!({"state":"actual_native_returned_ok","returned_xid":xid})
            }
            NativeReturn::ReturnedErr { details } => {
                serde_json::json!({"state":"actual_native_returned_error_unknown_commit","actual_error":details})
            }
        };
        serde_json::json!({"actual_return":outcome,"actual_issued_xid":self.issued_xid,"issued_candidate":self.candidate,
            "actual_protected_readback_completed":self.verified_readback,"native_permission":false,
            "actual_cancel_after_native_return_fired":self.test_cancel_fired,"actual_cancel_event":self.test_cancel_event,"actual_commit_sync_fault_fired":self.test_sync_fault_fired})
    }
}
