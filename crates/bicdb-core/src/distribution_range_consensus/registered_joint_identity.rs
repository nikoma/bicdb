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
    /// Joint activation catalogue write. An installed writer host passes the
    /// ordinary gate; a registered joint member that is not yet a writer is
    /// authorised only by its live registered owner for this exact range,
    /// re-proved before and after the write. Anything else is refused.
    ///
    /// The registered owner's proof also demands a durable catalogue. During
    /// this write the store's uncertain marker is this operation's own (or
    /// the exact one its re-fsync resolves), and only this write can clear
    /// it, so the identity is proved with that marker set aside and the
    /// marker is restored unchanged; the caller clears it after success.
    pub(super) fn persist_native_joint(&mut self, db: &crate::BicDb, range: RangeId) -> Result<()> {
        self.require_native_joint_identity_for_persist(db, range)?;
        self.persist_gated(|_| Ok(()), crate::storage::write_atomic)?;
        self.require_native_joint_identity_for_persist(db, range)
    }
    fn require_native_joint_identity_for_persist(
        &mut self,
        db: &crate::BicDb,
        range: RangeId,
    ) -> Result<()> {
        self.with_own_uncertainty_set_aside(|store| store.require_native_joint_host_base(db, range))
    }
    /// Run an identity proof for a joint activation write or its exact
    /// re-fsync with the store's uncertain marker set aside. Callers must
    /// already have established that any uncertainty is this activation's
    /// own. The marker is restored on every exit, including a panic.
    pub(super) fn with_own_uncertainty_set_aside<T>(
        &mut self,
        proof: impl FnOnce(&Self) -> Result<T>,
    ) -> Result<T> {
        let uncertain = std::mem::replace(&mut self.recovery_persistence_uncertain, false);
        let proved = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| proof(self)));
        self.recovery_persistence_uncertain = uncertain;
        match proved {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}
