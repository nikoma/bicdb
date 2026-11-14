//! Genuine retained-owner test access returns only DATA or an opaque hold.
use super::*;
impl ActualServiceNativeJointPreparedEvidence {
    pub(crate) fn test_prepublication_owner_snapshot(
        &self,
        service: &ClusterDataNodeService,
    ) -> Result<serde_json::Value> {
        // Diagnostics deliberately do not claim current OWN/metadata validity.
        let database = service.database();
        let database = database
            .try_read()
            .ok_or_else(|| data_error("prepublication diagnostic DB busy"))?;
        let range = self.committed.data().old_range().id;
        let frozen_owner = match &self.lease {
            #[cfg(feature = "tls-replication")]
            NativeJointServingLease::Registered { frozen, .. } => match frozen.as_ref() {
                NativeImageInstallFreeze::RegisteredRemote(actual) => {
                    actual.test_retained_frozen_owner()
                }
                _ => None,
            },
            _ => self.lease.try_frozen().ok().map(|frozen| frozen.owner()),
        };
        // Strict validation remains diagnostic DATA, not a prerequisite to
        // observing the retained owner or invoking the real refusing sink.
        let mut lease_current_error_truncated = false;
        let lease_current_error = self.lease.try_frozen().err().map(|error| {
            let detail = error.to_string();
            lease_current_error_truncated = detail.chars().count() > 4096;
            detail.chars().take(4096).collect::<String>()
        });
        Ok(serde_json::json!({
            "storage":database.test_joint_prepublication_storage(range)?,
            "gate":database.range_mutation_gate().test_source_capture_pin_state(range)?,
            "frozen_owner":frozen_owner,
            "lease_current_error":lease_current_error,
            "lease_current_error_truncated":lease_current_error_truncated,
            "owner_observation_is_retained_custody_not_authority":true,
            "producer_session_error":self.require_original_preparation_session().err().map(|e|e.to_string()),
        }))
    }
    pub(crate) fn test_post_collection_archive_fault(
        &self,
        service: &ClusterDataNodeService,
        parent: &CancellationToken,
    ) -> Result<serde_json::Value> {
        let database = service.database();
        let database = database
            .try_read()
            .ok_or_else(|| data_error("archive fault DB busy"))?;
        let gate = database.range_mutation_gate();
        let range = self.committed.data().old_range().id;
        let controlled = gate.try_controlled_mutation(self.lease.try_frozen()?)?;
        gate.validate(
            range,
            &crate::distribution_range_mutation::RangeMutationAuthority::Controlled(&controlled),
        )?;
        let result = database.test_remove_joint_preparation_archive(self.prepared(), parent);
        let post = gate.validate(
            range,
            &crate::distribution_range_mutation::RangeMutationAuthority::Controlled(&controlled),
        );
        drop(controlled); // before the later counter baseline / actual publisher
        match result {
            Ok(mut value) => {
                post?;
                value["actual_controlled_admission_validated_before_and_after"] = true.into();
                Ok(value)
            }
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
        }
    }
    pub(crate) fn test_post_collection_controlled_hold(
        &self,
        service: &ClusterDataNodeService,
    ) -> Result<impl Send> {
        let database = service.database();
        let database = database
            .try_read()
            .ok_or_else(|| data_error("controlled fault DB busy"))?;
        database
            .range_mutation_gate()
            .try_controlled_mutation(self.lease.try_frozen()?)
    }
}
