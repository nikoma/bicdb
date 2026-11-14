//! Historical DATA only. Exact native close bytes bind the outer closure
//! receipt to its distinct selected-range physical base and certified prefix.
//! Decoding/validating this type never creates a local closure or authority.
use super::*;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJointOldCloseRecordData {
    encoded: Vec<u8>,
}
impl std::fmt::Debug for NativeJointOldCloseRecordData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeJointOldCloseRecordData")
            .field("bytes", &self.encoded.len())
            .field(
                "bytes_sha256",
                &hex::encode(sha2::Sha256::digest(&self.encoded)),
            )
            .finish_non_exhaustive()
    }
}
impl NativeJointOldCloseRecordData {
    pub(crate) fn require_byte_bound(&self) -> Result<()> {
        if self.encoded.is_empty() || self.encoded.len() > MAX_NATIVE_RECEIPT_BYTES {
            return Err(invalid("native OLD exact record DATA exceeds byte bound"));
        }
        Ok(())
    }
    fn record(&self) -> Result<NativeJointOldCloseRecord> {
        self.require_byte_bound()?;
        let record: NativeJointOldCloseRecord = serde_json::from_slice(&self.encoded)?;
        record.validate()?;
        if bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)? != self.encoded {
            return Err(invalid(
                "native OLD record DATA is not exact canonical native bytes",
            ));
        }
        Ok(record)
    }
    pub(crate) fn require_materialized_facts(
        &self,
        facts: &crate::distribution_range_handoff::NativeJointMaterializedFacts,
    ) -> Result<()> {
        let r = self.record()?;
        if r.checksum_sha256 != facts.native_receipt_sha256
            || r.scope_sha256 != facts.scope_sha256
            || r.local_node_id != facts.node_id
            || r.birth_id != facts.birth_id
            || r.birth_sha256 != facts.birth_sha256
            || r.range_index != facts.range_index
            || r.logical_genesis_sha256 != facts.logical_genesis_sha256
            || r.logical_history_sha256 != facts.logical_history_sha256
            || r.actual_local_visibility_sequence != facts.actual_local_visibility_sequence
            || r.metadata_term > facts.metadata_term
            || r.metadata_read_index > facts.metadata_read_index
            || facts.source_snapshot_db_sequence.is_some()
            || facts.image_root_sha256.is_some()
        {
            return Err(invalid(
                "exact native OLD record differs from authenticated closure facts",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_plan(&self, plan: &NativeJointHandoffPlan) -> Result<()> {
        let r = self.record()?;
        if r.scope_sha256 != plan.checksum_sha256()?
            || !r.promise.same_request(plan.promise())
            || !plan.old_members().contains_key(&r.local_node_id)
            || plan.enrolled_birth(&r.local_node_id)?.node_incarnation() != r.local_incarnation
            || plan.enrolled_birth(&r.local_node_id)?.birth_id() != r.birth_id
            || plan.enrolled_birth(&r.local_node_id)?.birth_sha256() != r.birth_sha256
        {
            return Err(invalid(
                "exact native OLD record changed original plan/birth/session",
            ));
        }
        Ok(())
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn require_source_manifest(
        &self,
        manifest: &crate::distribution_range_image::NativeRemoteImageManifestV1,
    ) -> Result<()> {
        let r = self.record()?;
        let facts = manifest
            .actual_old_closures
            .get(&manifest.source_id)
            .ok_or_else(|| invalid("source manifest lacks selected actual OLD closure"))?;
        self.require_materialized_facts(facts)?;
        if r.local_node_id != manifest.source_id
            || r.birth_id != manifest.source_birth.birth_id()
            || r.birth_sha256 != manifest.source_birth.birth_sha256()
            || r.local_incarnation != manifest.source_birth.node_incarnation()
            || r.promise.session_id != manifest.handoff_session
            || r.promise.nonce != manifest.nonce
            || r.promise.relocation_id != manifest.relocation_id
            || r.promise.plan_id != manifest.plan_id
            || r.promise.range_id != manifest.capture_range.id
            || r.promise.old_epoch != manifest.capture_range.epoch
            || r.promise.activation_epoch != manifest.activation_range.epoch
            || r.promise.old_config_sha256 != manifest.old_config_sha256
            || r.promise.new_config_sha256 != manifest.new_config_sha256
            || r.physical_base_sha256 != manifest.source_physical_base_receipt
            || r.native_prefix_sha256 != manifest.source_native_receipt
            || r.range_index != manifest.range_index
            || r.logical_genesis_sha256 != manifest.h0
            || r.logical_history_sha256 != manifest.h
            || r.actual_local_visibility_sequence != manifest.source_native_local_b
            || manifest.source_reserved_b < manifest.source_native_local_b
            || r.schema_sha256 != manifest.schema_sha256
            || r.reader_features != manifest.readers
            || r.local_namespace_ids != manifest.source_namespace_ids
            || r.history_generation != manifest.source_history_generation
        {
            return Err(invalid(
                "source manifest differs from exact protected OLD record/prefix/base",
            ));
        }
        Ok(())
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn require_source_seal(
        &self,
        seal: &crate::distribution_range_image::LocalRangeImageSealV1,
    ) -> Result<()> {
        let r = self.record()?;
        if r.local_node_id != seal.source_id
            || r.local_incarnation != seal.source_member_incarnation
            || r.promise.session_id != seal.session_id
            || r.promise.nonce != seal.nonce
            || r.promise.range_id != seal.range_id
            || r.promise.old_epoch != seal.capture_epoch
            || r.promise.activation_epoch != seal.activation_epoch
            || r.promise.old_config_sha256 != seal.old_config_sha256
            || r.promise.new_config_sha256 != seal.new_config_sha256
            || r.native_prefix_sha256 != seal.source_native_receipt_sha256
            || r.range_index != seal.range_index
            || r.logical_genesis_sha256 != seal.logical_genesis_sha256
            || r.logical_history_sha256 != seal.logical_history_sha256
            || seal.reserved_db_upper_bound < r.actual_local_visibility_sequence
            || r.schema_sha256 != seal.schema_sha256
            || r.reader_features != seal.reader_features
            || r.local_namespace_ids != seal.local_namespace_ids
        {
            return Err(invalid(
                "source seal differs from exact protected OLD record/prefix",
            ));
        }
        Ok(())
    }
}
impl NativeJointOldCloseReceipt {
    pub(crate) fn actual_record_data(
        &self,
        database: &BicDb,
        plan: &NativeJointHandoffPlan,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        store: &RangeWriteStore,
    ) -> Result<NativeJointOldCloseRecordData> {
        self.require_closed_fact_current(database, plan, promise, store)?;
        let paged = database
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("actual OLD record DATA has no original paged store"))?;
        let snapshot = paged.latest_snapshot();
        let read = |key: String| -> Result<Vec<u8>> {
            paged
                .store()
                .get_as_of(&snapshot, &native_key(&key))
                .map_err(|e| BicDbError::PagedStorage(e.to_string()))?
                .ok_or_else(|| invalid("actual OLD current/history bytes absent"))
        };
        let encoded = read(old_close_key(plan.old_range().id))?;
        if encoded.is_empty() || encoded.len() > MAX_NATIVE_RECEIPT_BYTES {
            return Err(invalid("actual OLD record DATA exceeds byte bound"));
        }
        let archive = read(old_close_history_key(
            plan.old_range().id,
            &self.record.checksum_sha256,
        ))?;
        if encoded != archive || encoded != bounded_json(&self.record, MAX_NATIVE_RECEIPT_BYTES)? {
            return Err(invalid(
                "actual OLD current/history bytes differ from original native receipt",
            ));
        }
        let data = NativeJointOldCloseRecordData { encoded };
        if data.record()? != self.record {
            return Err(invalid(
                "actual OLD exact record changed during observation",
            ));
        }
        self.require_closed_fact_current(database, plan, promise, store)?;
        Ok(data)
    }
}

#[cfg(all(test, feature = "tls-replication"))]
impl NativeJointOldCloseRecordData {
    /// DATA-only negative controls derived from an actual authenticated fixture
    /// record. Even recomputed DTO checksums cannot replace its original fact.
    pub(crate) fn test_manifest_refusals(
        &self,
        manifest: &crate::distribution_range_image::NativeRemoteImageManifestV1,
    ) -> Result<serde_json::Value> {
        let original = self.record()?;
        let facts = manifest
            .actual_old_closures
            .get(&manifest.source_id)
            .ok_or_else(|| invalid("actual fixture selected OLD fact absent"))?;
        let positive = self.require_source_manifest(manifest);
        let mut cases = Vec::new();
        let mut record = |name: &str, result: Result<()>| {
            cases.push(
                serde_json::json!({"case":name,"actual_error":result.err().map(|e|e.to_string())}),
            );
        };
        record(
            "malformed_native_record_bytes",
            Self {
                encoded: b"{".to_vec(),
            }
            .require_materialized_facts(facts),
        );
        record(
            "oversized_native_record_bytes",
            Self {
                encoded: vec![0; MAX_NATIVE_RECEIPT_BYTES + 1],
            }
            .require_materialized_facts(facts),
        );
        let mut noncanonical = self.clone();
        noncanonical.encoded.push(b' ');
        record(
            "noncanonical_native_record_bytes",
            noncanonical.require_materialized_facts(facts),
        );
        let mut stale = facts.clone();
        stale.range_index = stale.range_index.saturating_sub(1);
        record(
            "stale_closed_prefix",
            self.require_materialized_facts(&stale),
        );
        let mut scope = facts.clone();
        scope.scope_sha256 = "0".repeat(64);
        record("other_scope", self.require_materialized_facts(&scope));
        let mut changed = original.clone();
        changed.native_prefix_sha256 = "0".repeat(64);
        changed.checksum_sha256 = changed.checksum()?;
        let changed = Self {
            encoded: bounded_json(&changed, MAX_NATIVE_RECEIPT_BYTES)?,
        };
        record(
            "rechecksummed_record_cannot_replace_authenticated_original",
            changed.require_materialized_facts(facts),
        );
        for name in [
            "other_birth",
            "source_prefix",
            "physical_base",
            "namespace",
            "session",
            "nonce",
        ] {
            let mut changed = manifest.clone();
            match name {
                "other_birth" => changed.source_birth = manifest.target_birth.clone(),
                "source_prefix" => changed.source_native_receipt = "0".repeat(64),
                "physical_base" => changed.source_physical_base_receipt = "0".repeat(64),
                "namespace" => {
                    let first = changed
                        .source_namespace_ids
                        .first_mut()
                        .ok_or_else(|| invalid("actual fixture has no namespace identity"))?;
                    first.1 = Uuid::now_v7();
                }
                "session" => changed.handoff_session = Uuid::now_v7(),
                "nonce" => changed.nonce = Uuid::now_v7(),
                _ => unreachable!(),
            }
            changed.checksum_sha256 = changed.checksum()?;
            record(name, changed.require_shape());
        }
        Ok(serde_json::json!({
            "original_validation_error":positive.err().map(|e|e.to_string()),
            "exact_original_native_record_bytes":self.encoded.len(),
            "exact_original_native_record_bytes_sha256":hex::encode(sha2::Sha256::digest(&self.encoded)),
            "outer_native_old_close_receipt_sha256":original.checksum_sha256,
            "inner_native_prefix_receipt_sha256":original.native_prefix_sha256,
            "physical_base_receipt_sha256":original.physical_base_sha256,
            "source_id":manifest.source_id,"range_id":manifest.capture_range.id,
            "manifest_sha256":manifest.checksum_sha256,"original_target_attempt":manifest.target_attempt,
            "data_validation_is_not_native_authority":true,"tampered_DATA_was_never_dispatched":true,
            "cases":cases,
        }))
    }
}
