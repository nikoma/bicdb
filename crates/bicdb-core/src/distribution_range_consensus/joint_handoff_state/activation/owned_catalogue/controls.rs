//! DATA-only access to actual retained publication and real persistence controls.
use super::*;
impl RangeWriteStore {
    pub(crate) fn test_owned_activation_snapshot(&self) -> Result<serde_json::Value> {
        // Original memory/IO return copied BEFORE diagnostic disk observation.
        let uncertain = self.recovery_persistence_uncertain;
        let history = serde_json::json!({"chain":hex::encode(self.activation_io_history.chain),"events":self.activation_io_history.events,"count_overflow":self.activation_io_history.count_overflow});
        let Some(actual) = self.owned_joint_activation.as_ref() else {
            return Ok(
                serde_json::json!({"retained":false,"uncertain_at_snapshot_entry":uncertain,"history":history}),
            );
        };
        let actual = actual
            .try_lock()
            .ok_or_else(|| range_write_error("actual activation snapshot owner busy"))?;
        let outcomes = actual
            .outcomes
            .iter()
            .map(|o| match o {
                IoReturn::Entered => serde_json::json!({"actual_return":"not_returned"}),
                IoReturn::ReturnedOk => serde_json::json!({"actual_return":"returned_ok"}),
                IoReturn::ReturnedErr { sha256 } => {
                    serde_json::json!({"actual_return":"returned_error","error_sha256":sha256})
                }
            })
            .collect::<Vec<_>>();
        let original_bytes = super::super::super::super::native_limits::validate_native_json_size(
            &actual.original,
            MAX_CATALOGUE_HEAD_BYTES,
        )?;
        let candidate_bytes = super::super::super::super::native_limits::validate_native_json_size(
            &actual.candidate,
            MAX_CATALOGUE_HEAD_BYTES,
        )?;
        let mut value = serde_json::json!({"retained":true,"uncertain_at_snapshot_entry":uncertain,"history":history,
            "actual_io_returns":outcomes,"write_generation":actual.write_generation,"store_write_generation":self.current_catalogue_write_generation(),"owns_uncertainty":actual.owns_uncertainty,"verified_at_snapshot_entry":actual.verified,"original_store":actual.store,
            "native_bridge_sha256":actual.native.checksum_sha256(),"native_preparation_sha256":actual.native.native_preparation_sha256(),
            "original_serialized_bytes":original_bytes,"candidate_serialized_bytes":candidate_bytes,"candidate_envelope_bytes":actual.bytes.len(),"existing_head_bound_bytes":MAX_CATALOGUE_HEAD_BYTES,"resident_memory_measured":false,
            "candidate_bytes_sha256":hex::encode(Sha256::digest(&actual.bytes)),"original_session_live_at_snapshot":actual.session.check().is_ok(),
            "memory_is_original":self.catalog==actual.original,"memory_is_candidate":self.catalog==actual.candidate,
            "snapshot_not_whole_RPC_coverage":true});
        let disk = NativeJointActivationPublication::read_head(self);
        value["disk_is_original"] = disk.as_ref().ok().map(|d| d == &actual.original).into();
        value["disk_is_candidate"] = disk.as_ref().ok().map(|d| d == &actual.candidate).into();
        value["disk_read_failed"] = disk.is_err().into();
        Ok(value)
    }
    pub(crate) fn test_owned_activation_foreign_bindings(
        &self,
        db: &crate::BicDb,
        native: &NativeJointServingBridgeReceipt,
        foreign: &NativeJointWriterDomain,
        foreign_frozen: &FrozenRangeMutationPermit,
    ) -> Result<serde_json::Value> {
        let actual = self
            .owned_joint_activation
            .as_ref()
            .ok_or_else(|| range_write_error("actual retained activation absent"))?
            .try_lock()
            .ok_or_else(|| range_write_error("actual retained activation busy"))?;
        let owner = actual.require_binding(db, self, native, foreign);
        let gate = actual.frozen.require_same_current(foreign_frozen);
        Ok(
            serde_json::json!({"actual_foreign_owner_returned_ok":owner.is_ok(),"owner_error":owner.as_ref().err().map(ToString::to_string),
            "actual_foreign_gate_returned_ok":gate.is_ok(),"gate_error":gate.as_ref().err().map(ToString::to_string),"IO_attempts":actual.outcomes.len(),"uncertain":self.recovery_persistence_uncertain,"read_only_no_grant":true}),
        )
    }
}

impl RangeWriteStore {
    /// Genuine unchanged-head refusal persistence through the normal catalogue
    /// sink, under the original gate. The caller arms a real storage fault;
    /// this does not synthesize Unknown or manufacture a competing R2 producer.
    pub(crate) fn test_later_same_bytes_activation_io(
        &mut self,
        db: &crate::BicDb,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<serde_json::Value> {
        let owner = self
            .owned_joint_activation
            .clone()
            .ok_or_else(|| range_write_error("actual activation owner absent"))?;
        let owner = owner
            .try_lock()
            .ok_or_else(|| range_write_error("actual activation owner busy"))?;
        if !owner.verified || self.recovery_persistence_uncertain || self.catalog != owner.candidate
        {
            return Err(range_write_error(
                "later same-byte control requires actual completed activation",
            ));
        }
        owner.native.require_current(db)?;
        owner.frozen.require_same_current(frozen)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(owner.native.data().old_range().id, frozen)?;
        let controlled = db.range_mutation_gate().try_controlled_mutation(frozen)?;
        db.range_mutation_gate().validate(
            owner.native.data().old_range().id,
            &RangeMutationAuthority::Controlled(&controlled),
        )?;
        let before = self.current_catalogue_write_generation();
        let result = self.persist_recovery_catalog_with(|path, bytes, fsync| {
            crate::storage::replace_durably(
                path,
                crate::storage::ReplaceContents::Plain(bytes),
                fsync,
                "test_activation_later_same_bytes",
                crate::storage::FailClosed::No,
            )
        });
        let error = result.as_ref().err().map(ToString::to_string);
        let after = self.current_catalogue_write_generation();
        let current = db.range_mutation_gate().validate(
            owner.native.data().old_range().id,
            &RangeMutationAuthority::Controlled(&controlled),
        );
        drop(controlled);
        Ok(
            serde_json::json!({"real_catalogue_returned_ok":result.is_ok(),"exact_error":error,"generation_before":before,"generation_after":after,"pending_activation_owner":owner.owns_uncertainty,"old_verified_activation":owner.verified,"global_uncertain":self.recovery_persistence_uncertain,"actual_controlled_postcheck_ok":current.is_ok(),"same_memory_head":self.catalog==owner.candidate,"native_transaction_replayed":false}),
        )
    }
}

impl RangeWriteStore {
    pub(crate) fn test_completed_activation_owner_against_current(
        &self,
        db: &crate::BicDb,
        admission: &NativeJointOpenAdmission,
        domain: &NativeJointWriterDomain,
    ) -> Result<serde_json::Value> {
        admission.require_live()?;
        admission
            .committed()
            .require_native_publication_metadata(db, self)?;
        admission.native_receipt().require_current(db)?;
        self.require_durable_recovery_catalog()?;
        let actual = self
            .owned_joint_activation
            .as_ref()
            .ok_or_else(|| range_write_error("actual completed activation owner absent"))?
            .try_lock()
            .ok_or_else(|| range_write_error("actual completed activation owner busy"))?;
        let comparison = actual.require_binding(db, self, &admission.native_receipt(), domain);
        Ok(
            serde_json::json!({"fresh_actual_admission_live":true,"selected_native_bridge_current":true,"global_healthy":true,"comparison_returned_ok":comparison.is_ok(),"comparison_error":comparison.as_ref().err().map(ToString::to_string),"same_memory_head":self.catalog==actual.candidate,"io_attempts":actual.outcomes.len(),"saved_generation":actual.write_generation,"current_generation":self.current_catalogue_write_generation()}),
        )
    }
}

impl RangeWriteStore {
    /// Read actual retained ordinary commands; never constructs catalogue DATA.
    pub(crate) fn test_large_activation_catalogue_entries(
        &self,
        range: RangeId,
    ) -> Result<serde_json::Value> {
        self.require_durable_recovery_catalog()?;
        let log = self
            .catalog
            .ranges
            .get(&range)
            .ok_or_else(|| range_write_error("large-entry actual range absent"))?;
        let mut entries = Vec::new();
        for (index, entry) in &log.entries {
            super::super::super::super::native_limits::validate_native_recovery_entry_size(entry)?;
            let bytes = super::super::super::super::native_limits::validate_native_json_size(
                entry,
                crate::distribution_range_consensus::MAX_NATIVE_RECOVERY_ENTRY_BYTES,
            )?;
            entries.push(serde_json::json!({"index":index,"state":entry.state,"complete_entry_bytes":bytes,"actual_checksum":entry.command.checksum_sha256,"record_ids":entry.command.mutations.iter().filter(|m|m.collection=="items").map(|m|m.record_id.as_str()).collect::<Vec<_>>()}));
        }
        let model_bytes = super::super::super::super::native_limits::validate_native_json_size(
            &self.catalog,
            MAX_CATALOGUE_HEAD_BYTES,
        )?;
        let file = open_range_catalog_no_follow(&self.root.join(DEFAULT_RANGE_WRITE_LOG))?;
        Ok(
            serde_json::json!({"actual_catalogue_file_bytes":file.metadata()?.len(),"catalogue_model_bytes":model_bytes,"actual_open":self.open_id,"entries":entries,"ordinary_entry_limit_bytes":crate::distribution_range_consensus::MAX_NATIVE_RECOVERY_ENTRY_BYTES,"existing_head_bound_bytes":MAX_CATALOGUE_HEAD_BYTES,"read_only_not_admission":true}),
        )
    }
}

#[cfg(test)]
mod capacity_counting_tests {
    use super::*;
    struct Repeated<'a> {
        chunk: &'a str,
        count: usize,
    }
    impl serde::Serialize for Repeated<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            use serde::ser::SerializeSeq;
            let mut sequence = serializer.serialize_seq(Some(self.count))?;
            for _ in 0..self.count {
                sequence.serialize_element(self.chunk)?;
            }
            sequence.end()
        }
    }
    #[test]
    fn activation_head_counting_preserves_existing_64mib_boundary_without_scratch_vec() {
        assert_eq!(MAX_CATALOGUE_HEAD_BYTES, 64 * 1024 * 1024);
        assert_eq!(MAX_RETAINED_ENCODED_BYTES, 3 * MAX_CATALOGUE_HEAD_BYTES);
        // Each actual serialized string is 1024 bytes; each array separator
        // adds one. Only a 1022-byte input string exists, not a 64MiB Vec.
        let chunk = "x".repeat(1022);
        let count = (MAX_CATALOGUE_HEAD_BYTES - 1) / 1025;
        let fit = Repeated {
            chunk: &chunk,
            count,
        };
        let actual = super::super::super::super::super::native_limits::validate_native_json_size(
            &fit,
            MAX_CATALOGUE_HEAD_BYTES,
        )
        .unwrap();
        assert_eq!(actual, count * 1025 + 1);
        assert!(actual > 8 * 1024 * 1024);
        assert!(
            super::super::super::super::super::native_limits::validate_native_json_size(
                &Repeated {
                    chunk: &chunk,
                    count: count + 1
                },
                MAX_CATALOGUE_HEAD_BYTES
            )
            .is_err()
        );
    }
}
