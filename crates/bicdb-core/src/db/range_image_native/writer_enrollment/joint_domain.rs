use super::*;
/// Distinct typed necessary policy at joint-only boundaries. A CLOSED variant
/// can be constructed only by the retained native publication above. It is
/// never accepted by ordinary APP/Strong range checks or metadata-only refresh.
#[derive(Clone)]
pub(crate) enum NativeJointWriterDomain {
    Installed(Arc<NativeWriterHostState>),
    #[cfg(feature = "tls-replication")]
    Registered(std::sync::Weak<crate::distribution_native_host::NativeRegisteredRemoteJointOwner>),
    #[cfg(feature = "tls-replication")]
    Closed(Arc<data_domains::NativeClosedWriterDomain>),
}
impl NativeJointWriterDomain {
    pub(crate) fn actual_writer(&self) -> Result<&Arc<NativeWriterHostState>> {
        match self {
            Self::Installed(w) => Ok(w),
            #[cfg(feature = "tls-replication")]
            Self::Closed(c) => Ok(c.actual_writer()),
            #[cfg(feature = "tls-replication")]
            Self::Registered(_) => Err(invalid(
                "original Registered joint domain has no installed writer",
            )),
        }
    }
    pub(crate) fn phase_owner(&self) -> NativeJointPhaseOwner {
        match self {
            Self::Installed(w) => NativeJointPhaseOwner::Writer(w.clone()),
            #[cfg(feature = "tls-replication")]
            Self::Closed(c) => NativeJointPhaseOwner::Writer(c.actual_writer().clone()),
            #[cfg(feature = "tls-replication")]
            Self::Registered(owner) => NativeJointPhaseOwner::Registered(owner.clone()),
        }
    }
    pub(crate) fn require_lifetime(
        &self,
        root: &Path,
        store: Uuid,
        range: &RangeDescriptor,
    ) -> Result<()> {
        self.phase_owner().require_store(root, store)?;
        match self {
            Self::Installed(w) => w.require_range(range),
            #[cfg(feature = "tls-replication")]
            Self::Registered(owner) => owner
                .upgrade()
                .ok_or_else(|| invalid("original Registered joint owner ended"))?
                .target()
                .require_joint_lifetime(root, store, range),
            #[cfg(feature = "tls-replication")]
            Self::Closed(c) => {
                c.require_lifetime()?;
                if crate::distribution_native_read::canonical_range(range) != *c.range() {
                    return Err(invalid(
                        "CLOSED joint writer binding changed selected domain",
                    ));
                }
                Ok(())
            }
        }
    }
    pub(crate) fn require_current(
        &self,
        db: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        range: &RangeDescriptor,
    ) -> Result<()> {
        #[cfg(feature = "tls-replication")]
        if let Self::Registered(owner) = self {
            let owner = owner
                .upgrade()
                .ok_or_else(|| invalid("original Registered joint owner ended"))?;
            let target = owner.target();
            self.require_lifetime(db.data_path(), store.native_writer_store_open_id(), range)?;
            if let Some(writer) = owner.terminal_writer() {
                writer.require_database(db, range)?;
                if !Arc::ptr_eq(writer, &store.native_writer_host_state()?) {
                    return Err(invalid(
                        "Registered successor changed its actual first writer State",
                    ));
                }
            } else {
                target.require_joint_identity(db, store)?;
                // Necessary identity only. An owned first-policy attempt may
                // already have exact native audit bytes before hook publication;
                // ONLY its retained installer can reconcile those bytes. The
                // original never-writer getters themselves remain unchanged.
                target.original_hook().require_current(db)?;
            }
            return self.require_lifetime(
                db.data_path(),
                store.native_writer_store_open_id(),
                range,
            );
        }
        let writer = self.actual_writer()?;
        store.require_native_writer_host_base()?;
        self.require_lifetime(db.data_path(), store.native_writer_store_open_id(), range)?;
        if !Arc::ptr_eq(writer, &store.native_writer_host_state()?) {
            return Err(invalid("joint writer domain changed original actual State"));
        }
        match self {
            Self::Installed(w) => w.require_database(db, range)?,
            #[cfg(feature = "tls-replication")]
            Self::Registered(_) => unreachable!("handled original actual receiver above"),
            #[cfg(feature = "tls-replication")]
            Self::Closed(c) => {
                c.require_current_for_store(db, store, range)?;
            }
        }
        self.require_lifetime(db.data_path(), store.native_writer_store_open_id(), range)
    }
}
