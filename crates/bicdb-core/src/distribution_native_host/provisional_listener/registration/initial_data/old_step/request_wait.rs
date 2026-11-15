//! Actual original table selection before a finite OLD service observation.
//! No table/entry/service/DB/MStore guard survives into a blocking acquisition.
use super::*;

impl NativeInitialDataAdmission {
    pub(in crate::distribution_native_host::provisional_listener::registration::initial_data) fn retained_actual_old_source_for_request(
        &self,
        range: crate::RangeId,
        parent: &CancellationToken,
    ) -> Result<Arc<crate::distribution_range_handoff::NativeJointOldObservation>> {
        parent.check()?;
        self.require_live()?;
        let original_writer = self
            .writer
            .try_read()
            .ok_or_else(|| invalid("actual OLD original writer identity busy"))?
            .clone();
        let table = self
            .old_closures
            .upgrade()
            .ok_or_else(|| invalid("actual OLD donor table ended"))?;
        let entry = table
            .try_lock()
            .ok_or_else(|| invalid("actual OLD donor table busy"))?
            .get(&range)
            .cloned()
            .ok_or_else(|| invalid("actual OLD donor has no retained local closure"))?;
        let (actual, writer, plan_sha) = {
            let owner = entry
                .try_lock()
                .ok_or_else(|| invalid("actual OLD donor closure still active"))?;
            if owner.remote_retired || !Arc::ptr_eq(&owner.writer, &original_writer) {
                return Err(invalid("retained OLD source original writer replaced"));
            }
            (
                owner
                    .actual
                    .clone()
                    .ok_or_else(|| invalid("actual OLD donor outcome remains uncertain"))?,
                owner.writer.clone(),
                owner.plan_sha256.clone(),
            )
        };
        if actual.node_id()
            != self
                .old_transport
                .upgrade()
                .ok_or_else(|| invalid("actual OLD donor transport ended"))?
                .local_metadata_read_participant()
                .node_id()
            || actual.plan().checksum_sha256()? != plan_sha
        {
            return Err(invalid(
                "actual OLD donor changed original local identity/scope",
            ));
        }
        actual.require_current_for_request(parent)?;
        // Waiting never permits a table entry, writer or native observation
        // to be adopted after replacement, even if its serialized bytes match.
        self.require_live()?;
        parent.check()?;
        let current_writer = self
            .writer
            .try_read()
            .ok_or_else(|| invalid("actual OLD writer identity busy after observation"))?
            .clone();
        let selected = table
            .try_lock()
            .ok_or_else(|| invalid("actual OLD donor table busy after observation"))?
            .get(&range)
            .cloned()
            .ok_or_else(|| invalid("actual OLD donor entry ended during observation"))?;
        if !Arc::ptr_eq(&entry, &selected) {
            return Err(invalid(
                "actual OLD donor entry replaced during observation",
            ));
        }
        let owner = selected
            .try_lock()
            .ok_or_else(|| invalid("actual OLD donor entry busy after observation"))?;
        if owner.remote_retired
            || owner.plan_sha256 != plan_sha
            || !Arc::ptr_eq(&owner.writer, &writer)
            || !Arc::ptr_eq(&writer, &current_writer)
            || owner
                .actual
                .as_ref()
                .is_none_or(|v| !Arc::ptr_eq(v, &actual))
        {
            return Err(invalid(
                "actual OLD donor custody changed during observation",
            ));
        }
        Ok(actual)
    }
}
