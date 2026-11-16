//! Finite same-owner OLD observation. No source capture/native mutation is
//! retried, and the generic instantaneous currentness API remains unchanged.
use super::*;

impl NativeJointOldObservation {
    #[cfg(test)]
    pub(crate) fn test_actual_old_request_wait_owner(&self) -> Result<serde_json::Value> {
        let frozen_owner = {
            let _actual_current = self.frozen.try_current_publication()?;
            self.frozen.owner()
        };
        // The actual Frozen guard is gone before any hashing/serialization.
        Ok(serde_json::json!({"actual_frozen_owner":frozen_owner,
            "actual_closed_native_sha256":self.native.checksum_sha256(),
            "actual_retained_range_index":self.native.range_index(),
            "actual_node":self.node_id(),"actual_plan_sha256":self.plan.checksum_sha256()?,
            "same_actual_Frozen_current":true,"DATA_only_not_authority":true}))
    }
    /// Only guard-free request acquisitions use this method. The caller keeps
    /// its actual OWN/Role checks and links their live window when applicable.
    /// No OLD pin/service/DB/MStore/publication guard is held while waiting.
    pub(crate) fn require_current_for_request(
        &self,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        if parent.deadline().is_none() {
            return Err(invalid(
                "waiting OLD observation needs original finite caller",
            ));
        }
        loop {
            parent.check()?;
            if let Some(service) = self
                .service
                .try_lock_for(std::time::Duration::from_millis(5))
            {
                parent.check()?;
                if let Some(pin) = self.pin.try_lock() {
                    // Same complete original predicate and actual borrowed
                    // objects as require_current; time is not physical proof.
                    service.require_native_joint_old_observation(
                        &self.plan,
                        &self.native,
                        &self.promise,
                        &self.frozen,
                        pin.as_ref().map(|pin| &**pin),
                    )?;
                    parent.check()?;
                    return Ok(());
                }
                drop(service);
                // Nothing remains held while pacing. Generic pin-first
                // observers use try_lock, so cannot deadlock this acquisition.
                std::thread::sleep(std::time::Duration::from_millis(5));
                continue;
            }
            #[cfg(all(test, feature = "tls-replication"))]
            ClusterDataNodeService::test_after_actual_request_service_contention(
                &self.service,
                parent,
            )?;
        }
    }
}
