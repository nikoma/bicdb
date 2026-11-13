//! Required existing decision custody. No policy, zero counter or pathname can
//! create bootstrap authority; only the private actual NEW factory can do so.
use super::*;
use std::io::Write;

#[cfg(test)]
thread_local! {
    static BOOTSTRAP_FAILURE: std::cell::Cell<Option<FreshCatalogueTestCut>> = const { std::cell::Cell::new(None) };
}
#[cfg(test)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum FreshCatalogueTestCut {
    BeforeWrite,
    BeforeFileSync,
    AfterDurable,
}
#[cfg(test)]
struct ClearCut;
#[cfg(test)]
impl Drop for ClearCut {
    fn drop(&mut self) {
        BOOTSTRAP_FAILURE.with(|cut| cut.set(None));
    }
}
impl RangeWriteStore {
    pub(crate) fn initialize_fresh_native_catalogue(
        fresh: &crate::db::FreshNativeCatalogueBootstrap<'_>,
    ) -> Result<()> {
        let (root, cluster_id, node_id) = fresh.require_current()?;
        fresh.require_non_abort_profile()?;
        let catalogue = RangeWriteCatalog {
            format_version: NATIVE_WRITER_CATALOG_FORMAT,
            native_writer_enforcement_version: Some(1),
            cluster_id,
            node_id,
            ranges: BTreeMap::new(),
            backup_fences: BTreeMap::new(),
            handoff_promises: BTreeMap::new(),
            image_install_intents: BTreeMap::new(),
            recovery_fences: BTreeMap::new(),
            native_image_bases: BTreeMap::new(),
            sparse_committed_decisions: Some(BTreeMap::new()),
            installing_committed_certifications: None,
            installing_recovery_handovers: None,
            native_joint_handoffs: None,
            native_abort_protocol: None,
            current_joint_recoveries: None,
        };
        let envelope = RangeWriteEnvelope::new(catalogue)?;
        envelope.clone().verify()?;
        Self::write_new_native_catalogue(&root, &envelope)?;
        fresh.require_current()?;
        Ok(())
    }

    // Byte publication ONLY. Callers must already own either the private NEW
    // birth capability or the actual protected Pending StoreUUID capability.
    pub(super) fn write_new_native_catalogue(
        root: &Path,
        envelope: &RangeWriteEnvelope,
    ) -> Result<()> {
        #[cfg(test)]
        let failure = BOOTSTRAP_FAILURE.with(|cut| cut.take());
        #[cfg(test)]
        if failure == Some(FreshCatalogueTestCut::BeforeWrite) {
            return Err(range_write_error(
                "injected fresh catalogue before durable creation",
            ));
        }
        // create_new never overwrites a partial/existing decision file. Any
        // failure leaves this factory unexposed; retry cannot reset the root.
        let path = root.join(DEFAULT_RANGE_WRITE_LOG);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.write_all(&serde_json::to_vec_pretty(&envelope)?)?;
        #[cfg(test)]
        if failure == Some(FreshCatalogueTestCut::BeforeFileSync) {
            return Err(range_write_error(
                "injected fresh catalogue file sync failure",
            ));
        }
        file.sync_all()?;
        drop(file);
        File::open(root)?.sync_all()?;
        #[cfg(test)]
        if failure == Some(FreshCatalogueTestCut::AfterDurable) {
            return Err(range_write_error(
                "injected fresh catalogue after durable creation",
            ));
        }
        Ok(())
    }

    /// Called by the actual service while sealing fresh Host policy, before
    /// native policy WAL. A visible prior file is not fsync evidence. Re-sync
    /// only unchanged verified custody under that SAME Store's borrow/owner;
    /// generic open does not gain a new unconditional write path.
    pub(crate) fn sync_verified_native_catalogue_before_policy(&self, root: &Path) -> Result<()> {
        self.require_verified_native_catalogue_custody(root)?;
        self.persist_verified_open_catalog()?;
        self.require_verified_native_catalogue_custody(root)
    }

    pub(crate) fn require_verified_native_catalogue_custody(&self, root: &Path) -> Result<()> {
        if !self.catalogue_loaded_at_open {
            return Err(BicDbError::NativeRangeCatalogueCustodyMissing);
        }
        if !self.fsync
            || self.recovery_persistence_uncertain
            || self.root.canonicalize()? != root.canonicalize()?
        {
            return Err(range_write_error(
                "native catalogue custody open/root/durability changed",
            ));
        }
        let file = match open_range_catalog_no_follow(&self.root.join(DEFAULT_RANGE_WRITE_LOG)) {
            Ok(file) => file,
            Err(BicDbError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(BicDbError::NativeRangeCatalogueCustodyMissing);
            }
            Err(error) => return Err(error),
        };
        const MAX_CUSTODY_BYTES: u64 = 64 * 1024 * 1024;
        if file.metadata()?.len() > MAX_CUSTODY_BYTES {
            return Err(range_write_error(
                "native catalogue custody exceeds bounded read",
            ));
        }
        let mut bytes = Vec::new();
        file.take(MAX_CUSTODY_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_CUSTODY_BYTES {
            return Err(range_write_error(
                "native catalogue custody grew beyond bounded read",
            ));
        }
        let stored = serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()?;
        if stored != self.catalog {
            return Err(range_write_error(
                "native catalogue custody differs from actual verified store state",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn test_with_fresh_catalogue_cut<T>(
        cut: FreshCatalogueTestCut,
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let _clear = ClearCut;
        BOOTSTRAP_FAILURE.with(|slot| assert!(slot.replace(Some(cut)).is_none()));
        operation()
    }
}
