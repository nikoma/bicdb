//! Original-A exact native decision attempt custody BEFORE first put. Unknown
//! remains unknown; only an ACTUAL successful abort permits another native xid.
//! Committed retries read the SAME retained record/xid, never fabricate a vote.
use super::*;
use crate::db::NativeRegisteredJointExclusionScope;
use crate::distribution_data::ActualNativeJointMetadataPoint;

#[derive(Debug)]
enum NativeOutcome {
    BeganBeforeCandidate,
    PreparedBeforePut,
    PutEntered,
    Committed {
        actual_xid: u64,
    },
    Aborted {
        actual_xid: u64,
        original_error: String,
        retry_allowed: bool,
        storage_health_error: Option<String>,
    },
    Unknown {
        exact_error: String,
    },
}
#[derive(Debug)]
struct NativeAttempt {
    record: NativeJointDecisionRecord,
    bytes: Vec<u8>,
    outcome: NativeOutcome,
}
pub(crate) struct NativeRegisteredJointDecisionAttempt {
    expected: NativeJointDecisionData,
    native: Vec<NativeAttempt>,
    catalogue:
        Option<crate::distribution_range_consensus::NativeRegisteredDecisionCataloguePublication>,
}
impl NativeRegisteredJointDecisionAttempt {
    /// Custody only; DATA cannot publish through this type without a fresh
    /// actual metadata point plus original native exclusion/physical proof.
    pub(crate) fn retain_expected(expected: &NativeJointDecisionData) -> Result<Self> {
        expected.validate()?;
        Ok(Self {
            expected: expected.clone(),
            native: Vec::new(),
            catalogue: None,
        })
    }
    pub(crate) fn has_retained_catalogue(&self) -> bool {
        self.catalogue.is_some()
    }
    fn require_expected(&self, admission: &NativeJointDecisionPublicationAdmission) -> Result<()> {
        if self.expected != *admission.data() {
            return Err(invalid(
                "Registered decision attempt changed original complete body",
            ));
        }
        Ok(())
    }
    fn require_can_begin(&self) -> Result<()> {
        if self.native.len() >= 8 {
            return Err(invalid(
                "Registered decision native attempt bound reached; retained outcomes not discarded",
            ));
        }
        if self.native.last().is_some_and(|n| {
            !matches!(
                n.outcome,
                NativeOutcome::Aborted {
                    retry_allowed: true,
                    ..
                }
            )
        }) {
            return Err(invalid(
                "Registered decision original native outcome unresolved; cannot create another xid",
            ));
        }
        Ok(())
    }
    fn require_retained_committed(&self, record: &NativeJointDecisionRecord) -> Result<()> {
        let actual = self.native.last().ok_or_else(|| {
            invalid("Registered existing native decision has no original retained candidate")
        })?;
        match &actual.outcome {
            NativeOutcome::Committed { actual_xid } if *actual_xid == record.native_page_xid && actual.record == *record
                && actual.bytes == bounded_json(record, MAX_NATIVE_RECEIPT_BYTES)? => Ok(()),
            _ => Err(invalid("Registered native record is not the SAME known committed attempt/xid; resolver required")),
        }
    }
    #[cfg(test)]
    pub(crate) fn test_catalogue_evidence(&self) -> Option<serde_json::Value> {
        self.catalogue.as_ref().map(|c| c.test_evidence())
    }
    #[cfg(test)]
    pub(crate) fn test_evidence(&self) -> Result<serde_json::Value> {
        let attempts = self.native.iter().map(|n| {
            let outcome = match &n.outcome {
                NativeOutcome::BeganBeforeCandidate => serde_json::json!({"state":"actual_native_begin_retained_candidate_not_yet_complete"}),
                NativeOutcome::PreparedBeforePut => serde_json::json!({"state":"candidate_retained_before_first_put"}),
                NativeOutcome::PutEntered => serde_json::json!({"state":"put_entered_outcome_not_known"}),
                NativeOutcome::Committed { actual_xid } => serde_json::json!({"state":"actual_commit_returned","actual_xid":actual_xid}),
                NativeOutcome::Aborted { actual_xid, original_error, retry_allowed, storage_health_error } => serde_json::json!({"state":"actual_abort_returned","actual_xid":actual_xid,"original_error":original_error,"retry_allowed":retry_allowed,"storage_health_error":storage_health_error}),
                NativeOutcome::Unknown { exact_error } => serde_json::json!({"state":"unknown_effect","exact_error":exact_error}),
            };
            serde_json::json!({"record":n.record,"native_xid":n.record.native_page_xid,"bytes":n.bytes,
                "bytes_sha256":hex::encode(sha2::Sha256::digest(&n.bytes)),"outcome":outcome})
        }).collect::<Vec<_>>();
        Ok(
            serde_json::json!({"expected":self.expected,"native_attempts":attempts,"data_not_authority":true}),
        )
    }
}
/// This actual post-IO proof borrows the still-held original DB/journal/hook/A
/// exclusion AND actual Store. It cannot be deserialized, cloned or retagged.
struct DecisionPublicationPoint<'s, 'db> {
    _scope: &'s NativeRegisteredJointExclusionScope<'db>,
    _store: &'s RangeWriteStore,
    receipt: NativeJointDecisionReceipt,
}
impl DecisionPublicationPoint<'_, '_> {
    fn count(
        self,
        metadata: &ActualNativeJointMetadataPoint,
        admission: &NativeJointDecisionPublicationAdmission,
        durable: &mut BTreeMap<ClusterNodeId, NativeJointDecisionReceipt>,
    ) -> Result<()> {
        let node = self.receipt.local_node_id().clone();
        if durable
            .get(&node)
            .is_some_and(|old| old.checksum_sha256() != self.receipt.checksum_sha256())
        {
            return Err(invalid(
                "Registered decision duplicate actual native receipt conflicts",
            ));
        }
        let version = metadata.try_publication_guard()?;
        version.require_current_memory()?;
        admission.role().require_live_request()?;
        // NO full require_live/IO/callback after the gate: only the concrete
        // bounded vote insertion. All actual lexical physical guards remain.
        durable.insert(node, self.receipt);
        Ok(())
    }
}
impl BicDb {
    fn require_registered_joint_decision_storage_healthy(&self) -> Result<()> {
        self.check_recovery_state()?;
        let paged = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("Registered DECISION actual native page store ended"))?;
        paged
            .store()
            .durability_health()
            .check()
            .map_err(crate::error::durability_error)?;
        paged
            .store()
            .wal()
            .health()
            .check()
            .map_err(crate::error::durability_error)?;
        self.check_recovery_state()
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_registered_joint_decision_and_count(
        &self,
        target: &Arc<crate::distribution_native_host::NativeRegisteredRemoteJointTarget>,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
        store: &mut RangeWriteStore,
        admission: &NativeJointDecisionPublicationAdmission,
        metadata: &ActualNativeJointMetadataPoint,
        attempt: &mut NativeRegisteredJointDecisionAttempt,
        durable: &mut BTreeMap<ClusterNodeId, NativeJointDecisionReceipt>,
    ) -> Result<()> {
        let scope = self.registered_joint_exclusion_scope(target, frozen, parent)?;
        admission.require_registered_own()?;
        let (own, business) = admission.registered_metadata_request()?;
        metadata.require_request(own, business)?;
        metadata.require_native_store_point(self, store)?;
        scope.require_current(self, store)?;
        attempt.require_expected(admission)?;
        let controlled = self.range_mutation_gate().try_controlled_mutation(frozen)?;
        let receipt = self.persist_registered_joint_decision(
            admission,
            &scope,
            store,
            &controlled,
            metadata,
            attempt,
        )?;
        if attempt.catalogue.is_none() {
            // Retain the exact whole candidate BEFORE any catalogue memory/IO.
            attempt.catalogue = Some(crate::distribution_range_consensus::NativeRegisteredDecisionCataloguePublication::begin(
                store, self, target.clone(), admission, &receipt, &scope, metadata)?);
        }
        attempt.catalogue.as_mut().unwrap().advance(self, store,
            crate::distribution_range_consensus::NativeRegisteredDecisionCatalogueAuthority::Initial(admission),
            &receipt, &scope, metadata)?;
        drop(controlled);
        // Complete physical/readback/postchecks BEFORE the short final gate.
        scope.require_current(self, store)?;
        self.range_mutation_gate()
            .validate_quiescent_frozen(admission.data().old_range().id, frozen)?;
        receipt.require_current(self)?;
        admission.require_registered_own()?;
        metadata.require_native_store_point(self, store)?;
        parent.check()?;
        let point = DecisionPublicationPoint {
            _scope: &scope,
            _store: store,
            receipt,
        };
        point.count(metadata, admission, durable)
    }
    /// No native begin/put/commit here. Only the original retained candidate's
    /// exact metadata file can be reconciled under the SAME actual prepared owners.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reconcile_registered_joint_decision_catalogue(
        &self,
        target: &Arc<crate::distribution_native_host::NativeRegisteredRemoteJointTarget>,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
        store: &mut RangeWriteStore,
        retry: &crate::distribution_range_handoff::NativeRegisteredDecisionCatalogueRetry<'_>,
        metadata: &ActualNativeJointMetadataPoint,
        attempt: &mut NativeRegisteredJointDecisionAttempt,
    ) -> Result<()> {
        if !attempt.has_retained_catalogue() {
            return Ok(());
        }
        if attempt.expected != *retry.data() {
            return Err(invalid(
                "Registered catalogue retry changed original expected decision",
            ));
        }
        let scope = self.registered_joint_exclusion_scope(target, frozen, parent)?;
        scope.require_identity_point(self, store)?;
        retry.require_native_point(self, store, target, metadata)?;
        let receipt = self.read_native_joint_decision(
            target.native(),
            retry.data().old_range().id,
            retry.data().checksum_sha256(),
        )?;
        attempt.require_retained_committed(&receipt.record)?;
        let controlled = self.range_mutation_gate().try_controlled_mutation(frozen)?;
        self.range_mutations.validate(
            retry.data().old_range().id,
            &RangeMutationAuthority::Controlled(&controlled),
        )?;
        attempt.catalogue.as_mut().unwrap().advance(
            self,
            store,
            crate::distribution_range_consensus::NativeRegisteredDecisionCatalogueAuthority::Retry(
                retry,
            ),
            &receipt,
            &scope,
            metadata,
        )?;
        drop(controlled);
        scope.require_current(self, store)?;
        parent.check()
    }
    fn persist_registered_joint_decision(
        &self,
        admission: &NativeJointDecisionPublicationAdmission,
        scope: &NativeRegisteredJointExclusionScope<'_>,
        store: &RangeWriteStore,
        controlled: &ControlledFrozenMutationPermit,
        metadata: &ActualNativeJointMetadataPoint,
        attempt: &mut NativeRegisteredJointDecisionAttempt,
    ) -> Result<NativeJointDecisionReceipt> {
        admission.require_registered_own()?;
        let (own, business) = admission.registered_metadata_request()?;
        metadata.require_request(own, business)?;
        metadata.require_native_store_point(self, store)?;
        scope.require_current(self, store)?;
        attempt.require_expected(admission)?;
        let target = scope.target();
        let point = target.base();
        let native = target.native();
        admission.data().require_actual_local_point(
            native,
            point.native_receipt_sha256(),
            point.range_index(),
            point.logical_genesis_sha256(),
            point.logical_history_sha256(),
            point.target_snapshot_bound().get(),
        )?;
        if native.local_node_id() != admission.role().local_node_id()
            || native.local_member_incarnation() != admission.role().local_member_incarnation()
        {
            return Err(invalid(
                "Registered decision changed actual native/role recipient",
            ));
        }
        self.range_mutations.validate(
            admission.data().old_range().id,
            &RangeMutationAuthority::Controlled(controlled),
        )?;
        store.require_native_catalog_durability(self.data_path())?;
        format::enable_native_joint_data_handoff(self.data_path())?;
        let role = admission.role();
        let mut record = NativeJointDecisionRecord {
            version: 1,
            decision: admission.data().clone(),
            birth_id: native.birth_id(),
            birth_sha256: native.birth_sha256().into(),
            local_node_id: native.local_node_id().clone(),
            local_incarnation: native.local_member_incarnation(),
            local_native_receipt_sha256: point.native_receipt_sha256().into(),
            local_range_index: point.range_index(),
            local_logical_genesis_sha256: point.logical_genesis_sha256().into(),
            local_logical_history_sha256: point.logical_history_sha256().into(),
            actual_local_visibility_sequence: point.target_snapshot_bound().get(),
            post_quorum_metadata_context_id: role.context_id(),
            post_quorum_metadata_term: role.metadata_term(),
            post_quorum_metadata_read_index: role.metadata_read_index(),
            native_page_xid: 0,
            checksum_sha256: String::new(),
        };
        if let Some(existing) =
            self.read_native_range_image_record::<NativeJointDecisionRecord>(&key(admission
                .data()
                .old_range()
                .id))?
        {
            existing.validate()?;
            attempt.require_retained_committed(&existing)?;
            if !existing.same_fact(&record)
                || role.metadata_term() < existing.post_quorum_metadata_term
                || role.metadata_read_index() < existing.post_quorum_metadata_read_index
            {
                return Err(invalid("Registered decision fresh retry changed actual same committed body/metadata floor"));
            }
            record = existing;
        } else {
            attempt.require_can_begin()?;
            self.require_registered_joint_decision_storage_healthy()?;
            metadata.require_native_store_point(self, store)?;
            let paged = self
                .paged_records
                .as_ref()
                .ok_or_else(|| invalid("Registered decision requires actual native page WAL"))?;
            let _write = paged.write_guard();
            self.require_registered_joint_decision_storage_healthy()?;
            // Known stale before begin cannot consume a native xid.
            metadata.require_current_memory()?;
            let (xid, _) = paged.begin();
            record.native_page_xid = xid;
            // Actual xid custody precedes even fallible candidate framing.
            attempt.native.push(NativeAttempt {
                record: record.clone(),
                bytes: Vec::new(),
                outcome: NativeOutcome::BeganBeforeCandidate,
            });
            let index = attempt.native.len() - 1;
            let write = (|| -> Result<()> {
                record.checksum_sha256 = record.checksum()?;
                record.validate()?;
                let bytes = bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?;
                attempt.native[index].record = record.clone();
                attempt.native[index].bytes = bytes.clone();
                attempt.native[index].outcome = NativeOutcome::PreparedBeforePut;
                #[cfg(test)]
                before_put::fire();
                admission.require_registered_own()?;
                metadata.require_native_store_point(self, store)?;
                scope.require_current(self, store)?;
                attempt.native[index].outcome = NativeOutcome::PutEntered;
                for name in [
                    archive_key(admission.data().old_range().id, &record.checksum_sha256),
                    key(admission.data().old_range().id),
                ] {
                    paged
                        .store()
                        .put(xid, &native_key(&name), &bytes)
                        .map_err(|e| BicDbError::PagedStorage(e.to_string()))?;
                }
                Ok(())
            })();
            if let Err(error) = write {
                match paged.abort(xid) {
                    Ok(()) => {
                        // Actual abort status is retained even when another
                        // storage layer is fenced. Ok(abort) alone cannot
                        // authorize allocating a replacement native xid.
                        let healthy = self.require_registered_joint_decision_storage_healthy();
                        let storage_health_error = healthy.as_ref().err().map(ToString::to_string);
                        attempt.native[index].outcome = NativeOutcome::Aborted {
                            actual_xid: xid,
                            original_error: error.to_string(),
                            retry_allowed: healthy.is_ok(),
                            storage_health_error,
                        };
                        if let Err(health) = healthy {
                            self.fence_durability(
                                "registered.joint.decision.abort.storage_health",
                                &health,
                            );
                        }
                    }
                    Err(abort) => {
                        self.fence_durability("registered.joint.decision.abort", &abort);
                        attempt.native[index].outcome = NativeOutcome::Unknown {
                            exact_error: format!("write={error}; abort={abort}"),
                        };
                        return Err(BicDbError::CommitOutcomeUnknown { sequence:None, transaction_id:None, incarnation:self.incarnation.clone(), detail:format!("Registered decision native xid {xid}: write={error}; abort={abort}") });
                    }
                }
                return Err(error);
            }
            if let Err(error) = paged.commit(xid) {
                self.fence_durability("registered.joint.decision.commit", &error);
                attempt.native[index].outcome = NativeOutcome::Unknown {
                    exact_error: error.to_string(),
                };
                return Err(BicDbError::CommitOutcomeUnknown {
                    sequence: None,
                    transaction_id: None,
                    incarnation: self.incarnation.clone(),
                    detail: format!("Registered decision native xid {xid} commit={error}"),
                });
            }
            // Actual API commit outcome retained BEFORE every later fallible
            // native readback, metadata/version, catalogue or reply check.
            attempt.native[index].outcome = NativeOutcome::Committed { actual_xid: xid };
            #[cfg(test)]
            after_commit::fire();
        }
        let receipt = NativeJointDecisionReceipt {
            native: self.native_recovery_admission(
                native.cluster_id(),
                native.bootstrap_id(),
                native.local_node_id(),
                native.local_member_incarnation(),
            )?,
            record,
        };
        receipt.require_current(self)?;
        metadata.require_native_store_point(self, store)?;
        admission.require_registered_own()?;
        Ok(receipt)
    }
}
#[cfg(test)]
mod before_put {
    use std::cell::RefCell;
    type Cut = Box<dyn FnOnce()>;
    thread_local! { static CUT: RefCell<Option<Cut>> = const { RefCell::new(None) }; }
    pub(crate) struct Guard(Option<Cut>);
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|cut| {
                *cut.borrow_mut() = self.0.take();
            });
        }
    }
    pub(crate) fn install(cut: Cut) -> Guard {
        Guard(CUT.with(|slot| slot.borrow_mut().replace(cut)))
    }
    pub(super) fn fire() {
        if let Some(cut) = CUT.with(|slot| slot.borrow_mut().take()) {
            cut();
        }
    }
}
#[cfg(test)]
mod after_commit {
    use std::cell::RefCell;
    type Cut = Box<dyn FnOnce()>;
    thread_local! { static CUT: RefCell<Option<Cut>> = const { RefCell::new(None) }; }
    pub(crate) struct Guard(Option<Cut>);
    impl Drop for Guard {
        fn drop(&mut self) {
            CUT.with(|cut| {
                *cut.borrow_mut() = self.0.take();
            });
        }
    }
    pub(crate) fn install(cut: Cut) -> Guard {
        Guard(CUT.with(|slot| slot.borrow_mut().replace(cut)))
    }
    pub(super) fn fire() {
        if let Some(cut) = CUT.with(|slot| slot.borrow_mut().take()) {
            cut();
        }
    }
}
#[cfg(test)]
impl BicDb {
    pub(crate) fn test_registered_joint_decision_record_evidence(
        &self,
        range: crate::RangeId,
        candidate_sha: &str,
    ) -> Result<serde_json::Value> {
        if candidate_sha.len() != 64 || !candidate_sha.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(invalid("test exact decision candidate hash invalid"));
        }
        let _journal = self.try_native_image_journal()?;
        let pointer =
            self.read_native_range_image_record::<NativeJointDecisionRecord>(&key(range))?;
        let archive = self.read_native_range_image_record::<NativeJointDecisionRecord>(
            &archive_key(range, candidate_sha),
        )?;
        for record in pointer.iter().chain(archive.iter()) {
            record.validate()?;
        }
        Ok(
            serde_json::json!({"range":range,"candidate_sha256":candidate_sha,
            "actual_pointer":pointer,"actual_candidate_archive":archive,"is_permission":false}),
        )
    }
    pub(crate) fn test_registered_joint_decision_before_put(cut: Box<dyn FnOnce()>) -> impl Drop {
        before_put::install(cut)
    }
    pub(crate) fn test_registered_joint_decision_after_commit(cut: Box<dyn FnOnce()>) -> impl Drop {
        after_commit::install(cut)
    }
}
