//! DISTINCT remote native DECISION attempt. A real participant admission plus
//! original Registered exclusion owns writes; historical DATA grants nothing.
use super::*;
#[cfg(test)]
mod reader_controls;
use crate::distribution_range_handoff::NativeRemoteDecisionAdmission;

#[derive(Debug)]
enum RemoteOutcome {
    Began,
    Candidate,
    PutEntered,
    Aborted { error: String },
    Unknown { error: String },
    Committed { xid: u64 },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct RemoteDecisionProvenance {
    version: u32,
    correlation: crate::distribution_range_handoff::NativeRemoteDecisionBindingV1,
    decision_sha256: String,
    native_record_sha256: String,
    native_page_xid: u64,
    checksum_sha256: String,
}
impl RemoteDecisionProvenance {
    fn binding_sha256(&self) -> Result<String> {
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &(
                "native-remote-decision-provenance-binding-v1",
                &self.correlation,
                &self.decision_sha256,
                self.native_page_xid,
            ),
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn checksum(&self) -> Result<String> {
        let mut copy = self.clone();
        copy.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &copy,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self, record: &NativeJointDecisionRecord) -> Result<()> {
        self.correlation.validate()?;
        record
            .decision
            .require_remote_source_data(&self.correlation.source_proof)?;
        if self.version != 1
            || self.native_page_xid != record.native_page_xid
            || self.native_page_xid == 0
            || self.native_record_sha256 != record.checksum_sha256
            || self.decision_sha256 != record.decision.checksum_sha256()
            || record.remote_provenance_sha256.as_deref() != Some(self.binding_sha256()?.as_str())
            || self.correlation.participant.node_id != record.local_node_id
            || self.correlation.participant.birth.birth_id() != record.birth_id
            || self.correlation.participant.birth.birth_sha256() != record.birth_sha256
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid(
                "protected remote DECISION provenance/body/xid mismatch",
            ));
        }
        Ok(())
    }
}
fn remote_current(range: RangeId) -> String {
    format!("joint-handoff/remote-decision/current/{range}")
}
fn remote_archive(range: RangeId, binding: &str) -> String {
    format!("joint-handoff/remote-decision/history/{range}/{binding}")
}
struct RemoteNativeAttempt {
    record: NativeJointDecisionRecord,
    bytes: Vec<u8>,
    provenance: Option<RemoteDecisionProvenance>,
    provenance_bytes: Vec<u8>,
    outcome: RemoteOutcome,
}
pub(crate) struct NativeRemoteJointDecisionAttempt {
    expected: NativeJointDecisionData,
    native: Option<RemoteNativeAttempt>,
    issued_xid: Option<u64>,
    #[cfg(test)]
    sync_fault: bool,
    #[cfg(test)]
    sync_fault_count: u64,
}
impl NativeRemoteJointDecisionAttempt {
    pub(crate) fn retain_expected(data: &NativeJointDecisionData) -> Result<Self> {
        data.validate()?;
        Ok(Self {
            expected: data.clone(),
            native: None,
            issued_xid: None,
            #[cfg(test)]
            sync_fault: false,
            #[cfg(test)]
            sync_fault_count: 0,
        })
    }
    #[cfg(test)]
    pub(crate) fn test_arm_commit_sync_failure(&mut self) {
        self.sync_fault = true;
    }
    pub(crate) fn actual_provenance_sha256(&self) -> Result<&str> {
        self.actual_committed_xid()?;
        Ok(&self
            .native
            .as_ref()
            .unwrap()
            .provenance
            .as_ref()
            .ok_or_else(|| invalid("remote native provenance unframed"))?
            .checksum_sha256)
    }
    pub(crate) fn actual_committed_xid(&self) -> Result<u64> {
        match self.native.as_ref().map(|n| &n.outcome) {
            Some(RemoteOutcome::Committed { xid }) if self.issued_xid == Some(*xid) => Ok(*xid),
            _ => Err(invalid(
                "remote native DECISION original outcome not known committed",
            )),
        }
    }
    #[cfg(test)]
    pub(crate) fn test_evidence(&self) -> serde_json::Value {
        let actual = self.native.as_ref().map(|n| {
            let outcome=match &n.outcome {
                RemoteOutcome::Began => serde_json::json!({"state":"began_before_candidate"}),
                RemoteOutcome::Candidate => serde_json::json!({"state":"candidate_before_put"}),
                RemoteOutcome::PutEntered => serde_json::json!({"state":"put_entered"}),
                RemoteOutcome::Aborted {error} => serde_json::json!({"state":"actual_abort_returned","error":error,"replacement_allowed":false}),
                RemoteOutcome::Unknown {error} => serde_json::json!({"state":"unknown_effect","error":error}),
                RemoteOutcome::Committed {xid} => serde_json::json!({"state":"actual_commit_returned","actual_xid":xid}),
            };
            serde_json::json!({
                "record":n.record,"bytes_sha256":hex::encode(sha2::Sha256::digest(&n.bytes)),
                "encoded_bytes":n.bytes.len(),"provenance":n.provenance,"provenance_bytes_sha256":hex::encode(sha2::Sha256::digest(&n.provenance_bytes)),"outcome":outcome,
                "native_xid":n.record.native_page_xid,
            })
        });
        serde_json::json!({"expected":self.expected,"actual_native_attempt":actual,"actual_issued_xid":self.issued_xid,
            "native_attempt_limit":1,"actual_commit_sync_fault_count":self.sync_fault_count,"no_catalogue_vote_or_serving":true})
    }
}
// Both variants are actual lexical physical owners, not deserialized policy.
enum RemotePhysicalScope<'a> {
    Registered(crate::db::NativeRegisteredJointExclusionScope<'a>),
    Old(crate::db::NativeRemoteOldDecisionWriterScope<'a>),
}
struct RemoteLocalPoint<'a> {
    native: &'a NativeRecoveryAdmission,
    receipt: &'a str,
    r: u64,
    h0: &'a str,
    h: &'a str,
    b: u64,
    frozen: &'a FrozenRangeMutationPermit,
}
impl RemotePhysicalScope<'_> {
    fn require_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
    ) -> Result<()> {
        match self {
            Self::Registered(scope) => {
                admission.require_target(scope.target())?;
                scope.require_current(db, store)
            }
            Self::Old(scope) => scope.require_current(db, store, admission),
        }
    }
    fn local_point(&self) -> RemoteLocalPoint<'_> {
        match self {
            Self::Registered(scope) => {
                let target = scope.target();
                let base = target.base();
                RemoteLocalPoint {
                    native: target.native(),
                    receipt: base.native_receipt_sha256(),
                    r: base.range_index(),
                    h0: base.logical_genesis_sha256(),
                    h: base.logical_history_sha256(),
                    b: base.target_snapshot_bound().get(),
                    frozen: scope.frozen(),
                }
            }
            Self::Old(scope) => {
                let close = scope.native_close();
                RemoteLocalPoint {
                    native: close.native_admission(),
                    receipt: close.checksum_sha256(),
                    r: close.range_index(),
                    h0: close.logical_genesis_sha256(),
                    h: close.logical_history_sha256(),
                    b: close.local_visibility_sequence(),
                    frozen: scope.frozen(),
                }
            }
        }
    }
}
impl BicDb {
    pub(super) fn require_remote_native_decision_profile(
        &self,
        record: &NativeJointDecisionRecord,
    ) -> Result<()> {
        #[cfg(test)]
        let profile_observation = reader_controls::profile_record(record);
        #[cfg(test)]
        let record = profile_observation.as_ref().unwrap_or(record);
        if record.remote_provenance_sha256.is_some() {
            self.require_remote_native_decision_provenance(record)?;
        } else if format::native_remote_joint_decision_active(self.data_path())?
            && self
                .read_native_range_image_record::<RemoteDecisionProvenance>(&remote_current(
                    record.decision.old_range().id,
                ))?
                .is_some()
        {
            return Err(invalid(
                "remote DECISION provenance pointer erased from common CURRENT",
            ));
        }
        Ok(())
    }
    pub(super) fn require_remote_native_decision_provenance(
        &self,
        record: &NativeJointDecisionRecord,
    ) -> Result<RemoteDecisionProvenance> {
        if !format::native_remote_joint_decision_active(self.data_path())? {
            return Err(invalid("remote DECISION required reader fence absent"));
        }
        let pointer = record
            .remote_provenance_sha256
            .as_ref()
            .ok_or_else(|| invalid("remote DECISION provenance pointer absent"))?;
        let mut current = self.read_native_range_image_record::<RemoteDecisionProvenance>(
            &remote_current(record.decision.old_range().id),
        )?;
        #[cfg(test)]
        reader_controls::current_observation(&mut current)?;
        let current = current
            .ok_or_else(|| invalid("remote DECISION protected provenance CURRENT absent"))?;
        let mut archive = self.read_native_range_image_record::<RemoteDecisionProvenance>(
            &remote_archive(record.decision.old_range().id, pointer),
        )?;
        #[cfg(test)]
        reader_controls::archive_observation(&mut archive);
        let archive = archive
            .ok_or_else(|| invalid("remote DECISION immutable provenance archive absent"))?;
        current.validate(record)?;
        if current != archive {
            return Err(invalid(
                "remote DECISION provenance CURRENT/archive mismatch",
            ));
        }
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("remote DECISION native page store absent"))?;
        if paged.store().transactions().status(record.native_page_xid)
            != bicdb_page::TxStatus::Committed
        {
            return Err(invalid(
                "remote DECISION provenance native xid not committed",
            ));
        }
        Ok(current) // Historical integrity only; no Role/A or cold grant.
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_remote_registered_native_decision(
        &self,
        target: &crate::distribution_native_host::NativeRegisteredRemoteJointTarget,
        frozen: &FrozenRangeMutationPermit,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        attempt: &mut NativeRemoteJointDecisionAttempt,
        parent: &CancellationToken,
    ) -> Result<NativeJointDecisionReceipt> {
        admission.require_target(target)?;
        admission.require_current()?;
        let scope = self.registered_joint_exclusion_scope(target, frozen, parent)?;
        self.persist_remote_native_decision_body(
            RemotePhysicalScope::Registered(scope),
            store,
            admission,
            attempt,
            parent,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_remote_old_native_decision(
        &self,
        writer: &crate::db::NativeWriterHostState,
        native: &NativeJointOldCloseReceipt,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        frozen: &FrozenRangeMutationPermit,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        attempt: &mut NativeRemoteJointDecisionAttempt,
        parent: &CancellationToken,
    ) -> Result<NativeJointDecisionReceipt> {
        admission.require_old_parts(native, promise, frozen, writer)?;
        admission.require_current()?;
        let scope = self.remote_old_decision_writer_scope(
            writer, native, promise, frozen, store, admission, parent,
        )?;
        self.persist_remote_native_decision_body(
            RemotePhysicalScope::Old(scope),
            store,
            admission,
            attempt,
            parent,
        )
    }
    fn persist_remote_native_decision_body(
        &self,
        physical: RemotePhysicalScope<'_>,
        store: &RangeWriteStore,
        admission: &NativeRemoteDecisionAdmission<'_>,
        attempt: &mut NativeRemoteJointDecisionAttempt,
        parent: &CancellationToken,
    ) -> Result<NativeJointDecisionReceipt> {
        physical.require_current(self, store, admission)?;
        let metadata = admission.metadata();
        metadata.require_native_store_point(self, store)?;
        if attempt.expected != *admission.data() {
            return Err(invalid("remote native DECISION changed original body"));
        }
        let point = physical.local_point();
        let native = point.native;
        let frozen = point.frozen;
        admission.data().require_actual_local_point(
            native,
            point.receipt,
            point.r,
            point.h0,
            point.h,
            point.b,
        )?;
        if native.local_node_id() != admission.role().local_node_id()
            || native.local_member_incarnation() != admission.role().local_member_incarnation()
        {
            return Err(invalid(
                "remote DECISION actual native recipient/role changed",
            ));
        }
        self.require_registered_joint_decision_storage_healthy()?;
        store.require_native_catalog_durability(self.data_path())?;
        format::enable_native_joint_data_handoff(self.data_path())?;
        format::enable_native_remote_joint_decision(self.data_path())?;
        let controlled = self.range_mutation_gate().try_controlled_mutation(frozen)?;
        self.range_mutations.validate(
            admission.data().old_range().id,
            &RangeMutationAuthority::Controlled(&controlled),
        )?;
        if attempt.native.is_none() {
            if attempt.issued_xid.is_some() {
                return Err(invalid("remote DECISION issued original native xid has incomplete custody; no replacement"));
            }
            if self
                .read_native_range_image_record::<NativeJointDecisionRecord>(&key(admission
                    .data()
                    .old_range()
                    .id))?
                .is_some()
            {
                return Err(invalid("remote DECISION existing record has no original owned native attempt; resolver required"));
            }
            let role = admission.role();
            let mut record = NativeJointDecisionRecord {
                version: 1,
                decision: admission.data().clone(),
                birth_id: native.birth_id(),
                birth_sha256: native.birth_sha256().into(),
                local_node_id: native.local_node_id().clone(),
                local_incarnation: native.local_member_incarnation(),
                local_native_receipt_sha256: point.receipt.into(),
                local_range_index: point.r,
                local_logical_genesis_sha256: point.h0.into(),
                local_logical_history_sha256: point.h.into(),
                actual_local_visibility_sequence: point.b,
                post_quorum_metadata_context_id: role.context_id(),
                post_quorum_metadata_term: role.metadata_term(),
                post_quorum_metadata_read_index: role.metadata_read_index(),
                native_page_xid: 0,
                remote_provenance_sha256: None,
                checksum_sha256: String::new(),
            };
            let correlation = admission.durable_binding()?;
            let paged = self
                .paged_records
                .as_ref()
                .ok_or_else(|| invalid("remote DECISION native page store absent"))?;
            let _writer = paged.write_guard();
            self.require_registered_joint_decision_storage_healthy()?;
            admission.require_memory_current()?;
            let (xid, _) = paged.begin();
            attempt.issued_xid = Some(xid);
            record.native_page_xid = xid;
            // FIRST operation after actual begin: retain issued xid/template.
            attempt.native = Some(RemoteNativeAttempt {
                record: record.clone(),
                bytes: Vec::new(),
                provenance: None,
                provenance_bytes: Vec::new(),
                outcome: RemoteOutcome::Began,
            });
            let write = (|| -> Result<()> {
                let mut provenance = RemoteDecisionProvenance {
                    version: 1,
                    correlation: correlation.clone(),
                    decision_sha256: record.decision.checksum_sha256().to_string(),
                    native_record_sha256: String::new(),
                    native_page_xid: xid,
                    checksum_sha256: String::new(),
                };
                record.remote_provenance_sha256 = Some(provenance.binding_sha256()?);
                record.checksum_sha256 = record.checksum()?;
                record.validate()?;
                let bytes = bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?;
                provenance.native_record_sha256 = record.checksum_sha256.clone();
                provenance.checksum_sha256 = provenance.checksum()?;
                provenance.validate(&record)?;
                let provenance_bytes = bounded_json(&provenance, MAX_NATIVE_RECEIPT_BYTES)?;
                let actual = attempt.native.as_mut().unwrap();
                actual.record = record.clone();
                actual.bytes = bytes.clone();
                actual.provenance = Some(provenance.clone());
                actual.provenance_bytes = provenance_bytes.clone();
                actual.outcome = RemoteOutcome::Candidate;
                #[cfg(test)]
                before_put::fire();
                admission.require_current()?;
                metadata.require_native_store_point(self, store)?;
                physical.require_current(self, store, admission)?;
                parent.check()?;
                attempt.native.as_mut().unwrap().outcome = RemoteOutcome::PutEntered;
                for name in [
                    archive_key(admission.data().old_range().id, &record.checksum_sha256),
                    key(admission.data().old_range().id),
                ] {
                    paged
                        .store()
                        .put(xid, &native_key(&name), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                for name in [
                    remote_archive(
                        admission.data().old_range().id,
                        record.remote_provenance_sha256.as_ref().unwrap(),
                    ),
                    remote_current(admission.data().old_range().id),
                ] {
                    paged
                        .store()
                        .put(xid, &native_key(&name), &provenance_bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                Ok(())
            })();
            if let Err(primary) = write {
                match paged.abort(xid) {
                    Ok(()) => {
                        attempt.native.as_mut().unwrap().outcome = RemoteOutcome::Aborted {
                            error: primary.to_string(),
                        }
                    }
                    Err(abort) => {
                        self.fence_durability("remote.joint.decision.abort", &abort);
                        let detail =
                            format!("remote native xid {xid}: write={primary}; abort={abort}");
                        attempt.native.as_mut().unwrap().outcome = RemoteOutcome::Unknown {
                            error: detail.clone(),
                        };
                        return Err(BicDbError::CommitOutcomeUnknown {
                            sequence: None,
                            transaction_id: None,
                            incarnation: self.incarnation.clone(),
                            detail,
                        });
                    }
                }
                return Err(primary); // Abort custody remains; no automatic replacement policy.
            }
            #[cfg(test)]
            let fault = std::mem::take(&mut attempt.sync_fault).then(|| {
                bicdb_page::test_faults::arm_io_failure("page_log.sync", self.data_path(), 1, false)
            });
            let commit = paged.commit(xid);
            #[cfg(test)]
            {
                attempt.sync_fault_count += fault.as_ref().map(|f| f.fired()).unwrap_or(0);
                drop(fault);
            }
            if let Err(error) = commit {
                self.fence_durability("remote.joint.decision.commit", &error);
                let detail = format!("remote native DECISION xid {xid} commit={error}");
                attempt.native.as_mut().unwrap().outcome = RemoteOutcome::Unknown {
                    error: detail.clone(),
                };
                return Err(BicDbError::CommitOutcomeUnknown {
                    sequence: None,
                    transaction_id: None,
                    incarnation: self.incarnation.clone(),
                    detail,
                });
            }
            attempt.native.as_mut().unwrap().outcome = RemoteOutcome::Committed { xid };
            #[cfg(test)]
            after_commit::fire();
        }
        let actual = attempt.native.as_ref().unwrap();
        let xid = attempt.actual_committed_xid()?;
        if actual.record.decision != *admission.data()
            || actual.record.native_page_xid != xid
            || admission.role().metadata_term() < actual.record.post_quorum_metadata_term
            || admission.role().metadata_read_index()
                < actual.record.post_quorum_metadata_read_index
        {
            return Err(invalid(
                "remote DECISION changed retained native body/metadata lower bound",
            ));
        }
        let receipt = self.read_native_joint_decision(
            native,
            admission.data().old_range().id,
            admission.data().checksum_sha256(),
        )?;
        if receipt.record != actual.record
            || bounded_json(&receipt.record, MAX_NATIVE_RECEIPT_BYTES)? != actual.bytes
            || self
                .paged_records
                .as_ref()
                .unwrap()
                .store()
                .transactions()
                .status(xid)
                != bicdb_page::TxStatus::Committed
        {
            return Err(invalid(
                "remote native DECISION exact CURRENT/archive/xid readback not proven",
            ));
        }
        let provenance = self.require_remote_native_decision_provenance(&receipt.record)?;
        if actual.provenance.as_ref() != Some(&provenance)
            || bounded_json(&provenance, MAX_NATIVE_RECEIPT_BYTES)? != actual.provenance_bytes
            || provenance.correlation != admission.durable_binding()?
        {
            return Err(invalid(
                "remote DECISION original protected provenance bytes changed",
            ));
        }
        self.require_registered_joint_decision_storage_healthy()?;
        metadata.require_native_store_point(self, store)?;
        admission.require_current()?;
        physical.require_current(self, store, admission)?;
        parent.check()?;
        drop(controlled);
        self.range_mutation_gate()
            .validate_quiescent_frozen(admission.data().old_range().id, frozen)?;
        let _version = admission.publication_guard()?;
        admission.require_memory_current()?;
        Ok(receipt) // A native fact only. No catalogue acknowledgement/vote.
    }
}
#[cfg(test)]
mod before_put {
    pub(super) fn fire() {
        super::cut::fire(false);
    }
}
#[cfg(test)]
mod after_commit {
    pub(super) fn fire() {
        super::cut::fire(true);
    }
}
#[cfg(test)]
mod cut {
    use std::cell::RefCell;
    type Cut = (bool, Box<dyn FnOnce()>);
    thread_local! { static CUT: RefCell<Option<Cut>>=const { RefCell::new(None) }; }
    pub(crate) struct Guard(Option<Cut>);
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|s| {
                s.replace(self.0.take());
            });
        }
    }
    pub(crate) fn install(after_commit: bool, action: Box<dyn FnOnce()>) -> Guard {
        Guard(CUT.with(|s| s.replace(Some((after_commit, action)))))
    }
    pub(super) fn fire(point: bool) {
        let action = CUT.with(|s| {
            let mut v = s.borrow_mut();
            if v.as_ref().is_some_and(|(p, _)| *p == point) {
                v.take()
            } else {
                None
            }
        });
        if let Some((_, action)) = action {
            action();
        }
    }
}
#[cfg(test)]
pub(crate) use cut::install as test_remote_joint_decision_cut;
