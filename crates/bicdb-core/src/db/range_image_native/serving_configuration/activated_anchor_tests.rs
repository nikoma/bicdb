//! Actual hidden-metadata WAL fault injection for the historical-anchor test.
//! This returns diagnostic DATA, never a serving/Ready/role capability.
use super::*;

impl BicDb {
    pub(crate) fn test_remove_current_joint_serving_archive(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        parent.check()?;
        let _journal = self.try_native_image_journal()?;
        let actual = self.current_native_range_source_lineage(range)?;
        let joint = actual
            .current_joint_decision_sha256()
            .ok_or_else(|| invalid("serving archive fault needs actual protected joint domain"))?;
        let record = actual
            .serving
            .as_ref()
            .ok_or_else(|| invalid("serving archive fault needs actual current serving pointer"))?;
        record.validate_historical_floor(&actual.birth, &actual.base)?;
        self.require_recovered_serving_joint_binding(&actual.birth, &actual.base, record)?;
        let current: NativeRecoveredServingConfiguration = self
            .read_native_range_image_record(&key(range))?
            .ok_or_else(|| invalid("serving archive fault current pointer missing"))?;
        let archive_key = history_key(range, &record.checksum_sha256);
        let archived: NativeRecoveredServingConfiguration = self
            .read_native_range_image_record(&archive_key)?
            .ok_or_else(|| {
                invalid("serving archive fault prerequisite immutable archive missing")
            })?;
        archived.validate_historical_floor(&actual.birth, &actual.base)?;
        if current != *record
            || archived != *record
            || !record.binds_current_base(&actual.base)
            || record.joint_decision_sha256.as_deref() != Some(joint)
        {
            return Err(invalid(
                "serving archive fault prerequisite current/archive/joint binding differs",
            ));
        }
        parent.check()?;
        self.in_paged_transaction(|paged, xid| {
            parent.check()?;
            paged
                .store()
                .delete(xid, &native_key(&archive_key))
                .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
            Ok(())
        })?
        .ok_or_else(|| invalid("serving archive fault has no actual native page store"))?;
        // Prove the exact current record survived and ONLY its archive ended.
        let after: NativeRecoveredServingConfiguration = self
            .read_native_range_image_record(&key(range))?
            .ok_or_else(|| invalid("serving archive fault unexpectedly lost current pointer"))?;
        if after != current
            || self
                .read_native_range_image_record::<NativeRecoveredServingConfiguration>(
                    &archive_key,
                )?
                .is_some()
        {
            return Err(invalid(
                "serving archive fault did not preserve pointer/delete exact archive",
            ));
        }
        parent.check()?;
        Ok(serde_json::json!({
            "scope":"actual metadata-WAL fault DATA only; no grant", "range":range,
            "current_record":current,"removed_archive":archived,"archive_key":archive_key,
            "joint_decision_sha256":joint,"actual_native_R":actual.range_index(),
            "actual_native_H":actual.logical_history_sha256(),
            "actual_local_B":actual.actual_local_visibility_sequence(),
            "current_pointer_unchanged":true,"actual_archive_absent":true,
        }))
    }
}
