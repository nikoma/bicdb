//! Same-open typed receiver identity for JOINT-ONLY catalogue readers. This
//! never relaxes ordinary native writer admission or reconstructs an owner
//! from persisted bytes. Only the original host transfers an actual owner.
use super::*;
impl RangeWriteStore {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn retain_registered_joint_identity(
        &mut self,
        owner: &Arc<crate::distribution_native_host::NativeRegisteredRemoteJointOwner>,
    ) -> Result<()> {
        let target = owner.target();
        target.require_joint_lifetime(
            &self.root.canonicalize()?,
            self.open_id,
            target.base().intended_activation_range(),
        )?;
        let range = target.base().capture_range().id;
        if let Some(old) = self.registered_joint_owners.get(&range) {
            if !std::sync::Weak::ptr_eq(old, &Arc::downgrade(owner)) {
                return Err(range_write_error(
                    "joint Store cannot replace original Registered identity",
                ));
            }
            return Ok(());
        }
        if self.registered_joint_owners.len() >= 128 {
            return Err(range_write_error(
                "actual Registered joint identity bound exceeded",
            ));
        }
        self.registered_joint_owners
            .insert(range, Arc::downgrade(owner));
        Ok(())
    }
    /// Necessary host identity only. Actual phase MQ, native records,
    /// physical history and Frozen/dropped/Open proofs remain mandatory.
    pub(crate) fn require_native_joint_host_base(
        &self,
        db: &crate::BicDb,
        range: RangeId,
    ) -> Result<()> {
        match self.require_native_writer_host_base() {
            Ok(()) => Ok(()),
            Err(legacy_error) => {
                #[cfg(feature = "tls-replication")]
                if let Some(owner) = self
                    .registered_joint_owners
                    .get(&range)
                    .and_then(std::sync::Weak::upgrade)
                {
                    let domain =
                        crate::db::NativeJointWriterDomain::Registered(Arc::downgrade(&owner));
                    return domain.require_current(
                        db,
                        self,
                        owner.target().base().intended_activation_range(),
                    );
                }
                let _ = (db, range);
                Err(legacy_error)
            }
        }
    }
}
