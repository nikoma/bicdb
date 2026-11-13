use super::*;
/// Distinct typed necessary policy at joint-only boundaries. A CLOSED variant
/// can be constructed only by the retained native publication above. It is
/// never accepted by ordinary APP/Strong range checks or metadata-only refresh.
#[derive(Clone)]
pub(crate) enum NativeJointWriterDomain {
    Installed(Arc<NativeWriterHostState>),
    #[cfg(feature = "tls-replication")]
    Closed(Arc<data_domains::NativeClosedWriterDomain>),
}
impl NativeJointWriterDomain {
    pub(crate) fn actual_writer(&self) -> &Arc<NativeWriterHostState> {
        match self {
            Self::Installed(w) => w,
            #[cfg(feature = "tls-replication")]
            Self::Closed(c) => c.actual_writer(),
        }
    }
    pub(crate) fn require_lifetime(
        &self,
        root: &Path,
        store: Uuid,
        range: &RangeDescriptor,
    ) -> Result<()> {
        self.actual_writer().require_store(root, store)?;
        match self {
            Self::Installed(w) => w.require_range(range),
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
        let writer = self.actual_writer();
        store.require_native_writer_host_base()?;
        self.require_lifetime(db.data_path(), store.native_writer_store_open_id(), range)?;
        if !Arc::ptr_eq(writer, &store.native_writer_host_state()?) {
            return Err(invalid("joint writer domain changed original actual State"));
        }
        match self {
            Self::Installed(w) => w.require_database(db, range)?,
            #[cfg(feature = "tls-replication")]
            Self::Closed(c) => {
                c.require_current_for_store(db, store, range)?;
            }
        }
        self.require_lifetime(db.data_path(), store.native_writer_store_open_id(), range)
    }
}
