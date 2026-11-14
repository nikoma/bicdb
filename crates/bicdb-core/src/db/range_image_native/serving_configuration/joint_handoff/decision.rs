//! Protected COMPLETE native decision body. Only an actual typed joint
//! publisher can write it. Reading bytes yields a historical local WAL fact,
//! never reconstructed quorums or permission to release a freeze.
use super::*;
#[cfg(feature = "tls-replication")]
mod registered;
#[cfg(feature = "tls-replication")]
mod remote;
use crate::distribution_range_handoff::{
    NativeJointDecisionData, NativeJointDecisionPublicationAdmission,
};
#[cfg(feature = "tls-replication")]
pub(crate) use registered::NativeRegisteredJointDecisionAttempt;
#[cfg(all(test, feature = "tls-replication"))]
pub(crate) use remote::test_remote_joint_decision_cut;
#[cfg(feature = "tls-replication")]
pub(crate) use remote::NativeRemoteJointDecisionAttempt;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NativeJointDecisionRecord {
    version: u32,
    decision: NativeJointDecisionData,
    birth_id: Uuid,
    birth_sha256: String,
    local_node_id: ClusterNodeId,
    local_incarnation: u64,
    local_native_receipt_sha256: String,
    local_range_index: u64,
    local_logical_genesis_sha256: String,
    local_logical_history_sha256: String,
    actual_local_visibility_sequence: u64,
    post_quorum_metadata_context_id: Uuid,
    post_quorum_metadata_term: u64,
    post_quorum_metadata_read_index: u64,
    native_page_xid: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote_provenance_sha256: Option<String>,
    checksum_sha256: String,
}
fn key(range: RangeId) -> String {
    format!("joint-handoff/decision/current/{range}")
}
fn archive_key(range: RangeId, sha: &str) -> String {
    format!("joint-handoff/decision/history/{range}/{sha}")
}
impl NativeJointDecisionRecord {
    fn checksum(&self) -> Result<String> {
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(bounded_json(
            &unsigned,
            MAX_NATIVE_RECEIPT_BYTES,
        )?)))
    }
    fn validate(&self) -> Result<()> {
        self.decision.validate()?;
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if self.version != 1
            || self.birth_id.is_nil()
            || self.local_incarnation == 0
            || self.post_quorum_metadata_context_id.is_nil()
            || self.post_quorum_metadata_term == 0
            || self.post_quorum_metadata_read_index == 0
            || self.native_page_xid == 0
            || self.local_range_index > self.decision.range_index()
            || self.local_logical_genesis_sha256 != self.decision.logical_genesis_sha256()
            || !digest(&self.birth_sha256)
            || !digest(&self.local_native_receipt_sha256)
            || !digest(&self.local_logical_history_sha256)
            || self
                .remote_provenance_sha256
                .as_ref()
                .is_some_and(|hash| !digest(hash))
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged protected complete joint decision record"));
        }
        Ok(())
    }
    fn same_fact(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        for fact in [&mut left, &mut right] {
            fact.post_quorum_metadata_context_id = Uuid::nil();
            fact.post_quorum_metadata_term = 0;
            fact.post_quorum_metadata_read_index = 0;
            fact.native_page_xid = 0;
            fact.checksum_sha256.clear();
        }
        left == right
    }
}
#[derive(Debug)]
pub(crate) struct NativeJointDecisionReceipt {
    native: NativeRecoveryAdmission,
    record: NativeJointDecisionRecord,
}
impl NativeJointDecisionReceipt {
    pub(crate) fn native_admission(&self) -> &NativeRecoveryAdmission {
        &self.native
    }
    pub(crate) fn native_metadata_initialization_version(&self) -> u32 {
        self.native.metadata_initialization_version()
    }
    pub(crate) fn native_database_incarnation(&self) -> &str {
        self.native.database_incarnation()
    }
    pub(crate) fn metadata_context_id(&self) -> Uuid {
        self.record.post_quorum_metadata_context_id
    }
    pub(crate) fn metadata_anchor(&self) -> (u64, u64) {
        (
            self.record.post_quorum_metadata_term,
            self.record.post_quorum_metadata_read_index,
        )
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.record.checksum_sha256
    }
    pub(crate) fn decision_sha256(&self) -> &str {
        self.record.decision.checksum_sha256()
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.record.decision
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.record.local_node_id
    }
    pub(crate) fn actual_local_visibility_sequence(&self) -> u64 {
        self.record.actual_local_visibility_sequence
    }
    pub(crate) fn require_current(&self, database: &BicDb) -> Result<()> {
        database.require_native_joint_decision_reader_profile()?;
        self.native.require_current(database)?;
        self.record.validate()?;
        self.record.decision.require_actual_native(&self.native)?;
        if !format::native_joint_data_handoff_active(database.data_path())?
            || self.native.local_node_id() != &self.record.local_node_id
            || self.native.birth_id() != self.record.birth_id
            || self.native.birth_sha256() != self.record.birth_sha256
            || self.native.local_member_incarnation() != self.record.local_incarnation
        {
            return Err(invalid(
                "joint decision changed actual native birth/reader domain",
            ));
        }
        let actual: NativeJointDecisionRecord = database
            .read_native_range_image_record(&key(self.record.decision.old_range().id))?
            .ok_or_else(|| invalid("protected complete joint decision missing"))?;
        actual.validate()?;
        let archived: NativeJointDecisionRecord = database
            .read_native_range_image_record(&archive_key(
                self.record.decision.old_range().id,
                &self.record.checksum_sha256,
            ))?
            .ok_or_else(|| invalid("immutable complete joint decision archive missing"))?;
        #[cfg(feature = "tls-replication")]
        database.require_remote_native_decision_profile(&self.record)?;
        #[cfg(not(feature = "tls-replication"))]
        if self.record.remote_provenance_sha256.is_some()
            || format::native_remote_joint_decision_active(database.data_path())?
        {
            return Err(invalid(
                "remote decision provenance requires tls-replication reader",
            ));
        }
        if actual != self.record || archived != self.record {
            return Err(invalid("protected current/archive joint decision differ"));
        }
        Ok(())
    }
}
impl BicDb {
    fn require_native_joint_decision_reader_profile(&self) -> Result<()> {
        #[cfg(not(feature = "tls-replication"))]
        if format::native_remote_joint_decision_active(self.data_path())? {
            return Err(invalid(
                "remote decision semantic profile requires tls-replication reader",
            ));
        }
        Ok(())
    }
    /// Actual protected CURRENT read, not a checksum/DTO authority constructor.
    pub(in crate::db::range_image_native) fn read_current_native_joint_writer_decision(
        &self,
        native: &NativeRecoveryAdmission,
        range: RangeId,
    ) -> Result<NativeJointDecisionReceipt> {
        self.require_native_joint_decision_reader_profile()?;
        native.require_current(self)?;
        let actual: NativeJointDecisionRecord = self
            .read_native_range_image_record(&key(range))?
            .ok_or_else(|| {
            invalid("writer DATA transition lacks actual protected joint decision")
        })?;
        actual.validate()?;
        self.read_native_joint_decision(native, range, actual.decision.checksum_sha256())
    }

    pub(crate) fn persist_native_joint_decision_from_old(
        &self,
        admission: &NativeJointDecisionPublicationAdmission,
        native: &NativeJointOldCloseReceipt,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        store: &RangeWriteStore,
        controlled: &ControlledFrozenMutationPermit,
    ) -> Result<NativeJointDecisionReceipt> {
        admission.require_live()?;
        native.require_current(self, admission.plan(), admission.role(), promise, store)?;
        admission.data().require_actual_local_point(
            native.native_admission(),
            native.checksum_sha256(),
            native.range_index(),
            native.logical_genesis_sha256(),
            native.logical_history_sha256(),
            native.local_visibility_sequence(),
        )?;
        self.persist_native_joint_decision_inner(
            admission,
            store,
            native.native_admission(),
            native.checksum_sha256(),
            native.range_index(),
            native.logical_genesis_sha256(),
            native.logical_history_sha256(),
            native.local_visibility_sequence(),
            controlled,
        )
    }
    pub(crate) fn persist_native_joint_decision_from_image(
        &self,
        admission: &NativeJointDecisionPublicationAdmission,
        image: &crate::distribution_range_image_install::NativeImageCatalogueFinalization,
        store: &RangeWriteStore,
        frozen: &FrozenRangeMutationPermit,
        controlled: &ControlledFrozenMutationPermit,
    ) -> Result<NativeJointDecisionReceipt> {
        admission.require_live()?;
        image.require_materialized_fact_current(self, store, frozen)?;
        let point = image.base();
        admission.data().require_actual_local_point(
            image.native_admission(),
            point.native_receipt_sha256(),
            point.range_index(),
            point.logical_genesis_sha256(),
            point.logical_history_sha256(),
            point.target_snapshot_bound().get(),
        )?;
        self.persist_native_joint_decision_inner(
            admission,
            store,
            image.native_admission(),
            point.native_receipt_sha256(),
            point.range_index(),
            point.logical_genesis_sha256(),
            point.logical_history_sha256(),
            point.target_snapshot_bound().get(),
            controlled,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn persist_native_joint_decision_inner(
        &self,
        admission: &NativeJointDecisionPublicationAdmission,
        store: &RangeWriteStore,
        native: &NativeRecoveryAdmission,
        receipt: &str,
        r: u64,
        h0: &str,
        h: &str,
        b: u64,
        controlled: &ControlledFrozenMutationPermit,
    ) -> Result<NativeJointDecisionReceipt> {
        admission.require_live()?;
        native.require_current(self)?;
        admission.require_native_metadata_point(self, store, native)?;
        admission.data().require_actual_native(native)?;
        store.require_native_catalog_durability(self.data_path())?;
        store.require_native_writer_host_base()?;
        if native.local_node_id() != admission.role().local_node_id()
            || native.local_member_incarnation() != admission.role().local_member_incarnation()
        {
            return Err(invalid(
                "joint publication role differs from actual native writer",
            ));
        }
        self.range_mutations.validate(
            admission.data().old_range().id,
            &RangeMutationAuthority::Controlled(controlled),
        )?;
        format::enable_native_joint_data_handoff(self.data_path())?;
        let role = admission.role();
        let mut record = NativeJointDecisionRecord {
            version: 1,
            decision: admission.data().clone(),
            birth_id: native.birth_id(),
            birth_sha256: native.birth_sha256().into(),
            local_node_id: native.local_node_id().clone(),
            local_incarnation: native.local_member_incarnation(),
            local_native_receipt_sha256: receipt.into(),
            local_range_index: r,
            local_logical_genesis_sha256: h0.into(),
            local_logical_history_sha256: h.into(),
            actual_local_visibility_sequence: b,
            post_quorum_metadata_context_id: role.context_id(),
            post_quorum_metadata_term: role.metadata_term(),
            post_quorum_metadata_read_index: role.metadata_read_index(),
            native_page_xid: 0,
            remote_provenance_sha256: None,
            checksum_sha256: String::new(),
        };
        let _journal = self.try_native_image_journal()?;
        if let Some(existing) =
            self.read_native_range_image_record::<NativeJointDecisionRecord>(&key(admission
                .data()
                .old_range()
                .id))?
        {
            existing.validate()?;
            if !existing.same_fact(&record)
                || role.metadata_term() < existing.post_quorum_metadata_term
                || role.metadata_read_index() < existing.post_quorum_metadata_read_index
            {
                return Err(invalid(
                    "joint decision retry cannot replace its protected fact/context",
                ));
            }
            record = existing;
        } else {
            self.in_paged_transaction(|paged, xid| {
                admission.require_live()?;
                admission.require_native_metadata_point(self, store, native)?;
                record.native_page_xid = xid;
                record.checksum_sha256 = record.checksum()?;
                record.validate()?;
                let bytes = bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?;
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
            })?
            .ok_or_else(|| invalid("complete joint decision has no actual native page WAL"))?;
        }
        drop(_journal);
        let actual = self.native_recovery_admission(
            native.cluster_id(),
            native.bootstrap_id(),
            native.local_node_id(),
            native.local_member_incarnation(),
        )?;
        let result = NativeJointDecisionReceipt {
            native: actual,
            record,
        };
        result.require_current(self)?;
        admission.require_live()?;
        let _physical_journal = self.try_native_image_journal()?;
        result.require_current(self)?;
        admission.require_native_metadata_point(self, store, native)?;
        let _version = admission.native_publication_guard(native)?;
        Ok(result)
    }
    /// Actual protected record reader. Expected checksum binds a committed
    /// correlation; it does not manufacture a quorum or serving permission.
    pub(crate) fn read_native_joint_decision(
        &self,
        native: &NativeRecoveryAdmission,
        range: RangeId,
        expected: &str,
    ) -> Result<NativeJointDecisionReceipt> {
        self.require_native_joint_decision_reader_profile()?;
        native.require_current(self)?;
        let record: NativeJointDecisionRecord =
            self.read_native_range_image_record(&key(range))?
                .ok_or_else(|| invalid("protected joint decision unavailable"))?;
        record.validate()?;
        if record.decision.old_range().id != range || record.decision.checksum_sha256() != expected
        {
            return Err(invalid(
                "actual protected joint decision differs from committed correlation",
            ));
        }
        let result = NativeJointDecisionReceipt {
            native: self.native_recovery_admission(
                native.cluster_id(),
                native.bootstrap_id(),
                native.local_node_id(),
                native.local_member_incarnation(),
            )?,
            record,
        };
        result.require_current(self)?;
        Ok(result)
    }
}

#[cfg(all(test, not(feature = "tls-replication")))]
mod unsupported_remote_reader_tests {
    use super::*;
    #[test]
    fn native_remote_decision_current_reader_without_tls_refuses_required_profile_before_lookup() {
        let (_root, database, topology, spec) =
            crate::distribution_range_consensus::native_origin_fixture();
        let db = database.read();
        let native = db
            .native_recovery_admission(
                &spec.cluster_id,
                spec.bootstrap_id,
                &spec.node_id,
                spec.member_incarnation,
            )
            .unwrap();
        let range = topology.ranges.values().next().unwrap().id;
        let expected = "a".repeat(64);
        let before = db
            .read_native_joint_decision(&native, range, &expected)
            .unwrap_err()
            .to_string();
        assert!(before.contains("protected joint decision unavailable"));
        // Refusal-only format fixture: no remote DECISION/sidecar is minted.
        crate::format::test_publish_remote_joint_decision_reader_refusal_only(db.data_path())
            .unwrap();
        let format_error = crate::format::plan_migration(db.data_path(), false)
            .unwrap_err()
            .to_string();
        let direct = db
            .read_native_joint_decision(&native, range, &expected)
            .unwrap_err()
            .to_string();
        let current = db
            .read_current_native_joint_writer_decision(&native, range)
            .unwrap_err()
            .to_string();
        eprintln!(
            "NON_TLS_REMOTE_DECISION_ACTUAL_READER_REFUSAL {}",
            serde_json::json!({
                "genuine_native_birth":native.birth_id(),"actual_before_flag_error":before,
                "actual_direct_reader_error":direct,"actual_current_reader_error":current,"actual_format_validation_error":format_error,
                "remote_committed_record_not_created":true,"format_refusal_only_not_remote_qualification":true,
            })
        );
        assert!(format_error.contains("requires tls-replication reader"));
        assert!(direct.contains("requires tls-replication reader"));
        assert!(current.contains("requires tls-replication reader"));
    }
}
