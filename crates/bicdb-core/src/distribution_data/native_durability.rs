//! Necessary native catalog durability policy, never a quorum/floor proof.
//! Its actual DB root/open is captured before stores open. Checks under an
//! originating DB.write guard do not reacquire that guard.
use super::*;

#[derive(Debug)]
pub(super) struct ServiceStoragePolicy {
    database_root: PathBuf,
    database_incarnation: String,
    directory: Option<std::sync::Weak<bicdb_page::DirectoryLock>>,
    native_format_at_open: bool,
}
impl ServiceStoragePolicy {
    pub(super) fn capture(database: &BicDb) -> Result<Self> {
        Ok(Self {
            database_root: database.data_path().canonicalize()?,
            database_incarnation: database.incarnation_id().into(),
            directory: database.native_service_directory_lifetime(),
            native_format_at_open: database.native_range_image_birth_present()
                || crate::format::range_image_install_active(database.data_path())?,
        })
    }
    fn requires_native_catalog(&self, catalog_root: &Path) -> Result<bool> {
        Ok(self.native_format_at_open
            || crate::format::range_image_install_active(&self.database_root)?
            || crate::format::range_image_install_active(catalog_root)?)
    }
    pub(super) fn validate_configuration(&self, catalog_root: &Path, fsync: bool) -> Result<()> {
        if !self.requires_native_catalog(catalog_root)? {
            return Ok(());
        }
        if !fsync {
            return Err(data_error("native range catalog requires fsync=true"));
        }
        if self.directory.as_ref().and_then(|d| d.upgrade()).is_none() {
            return Err(data_error(
                "native range catalog actual database lifetime ended",
            ));
        }
        if catalog_root.canonicalize()? != self.database_root {
            return Err(data_error(
                "native range catalog root differs from actual database root",
            ));
        }
        if !crate::format::range_image_install_active(&self.database_root)? {
            return Err(data_error(
                "native range catalog lost its actual image reader fence",
            ));
        }
        Ok(())
    }
    pub(super) fn require_database(&self, database: &BicDb) -> Result<()> {
        if self.database_root != database.data_path().canonicalize()?
            || self.database_incarnation != database.incarnation_id()
        {
            return Err(data_error(
                "native catalog service belongs to another actual database/open",
            ));
        }
        database.check_recovery_state()
    }
}
impl ClusterDataNodeService {
    pub(crate) fn require_catalog_storage_policy(&self) -> Result<()> {
        self.range_writes.require_service_catalog_durability(
            &self.storage_policy.database_root,
            self.storage_policy.native_format_at_open,
        )?;
        // No database Arc lock acquisition: source admission already owns its
        // actual borrowed DB context. The weak open only fences old handles.
        if self
            .storage_policy
            .requires_native_catalog(&self.storage_policy.database_root)?
            && self
                .storage_policy
                .directory
                .as_ref()
                .and_then(|d| d.upgrade())
                .is_none()
        {
            return Err(data_error("native catalog service database lifetime ended"));
        }
        Ok(())
    }
    pub(crate) fn require_native_catalog_database(&self, database: &BicDb) -> Result<()> {
        self.storage_policy.require_database(database)?;
        self.range_writes
            .require_native_catalog_durability(database.data_path())
    }
}

#[cfg(test)]
mod tests;
