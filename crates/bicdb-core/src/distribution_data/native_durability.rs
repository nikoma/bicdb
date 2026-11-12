//! Necessary native catalog durability policy, never a quorum/floor proof.
//! Its actual DB root/open is captured before stores open. Checks under an
//! originating DB.write guard do not reacquire that guard.
use super::*;

/// Necessary refusal fence for one actual BicDb open. This is neither a
/// quorum proof nor a replacement for validating the borrowed database.
#[derive(Clone, Debug)]
pub(crate) struct ClusterServiceOpenGuard {
    open: std::sync::Weak<()>,
    health: Arc<bicdb_page::DurabilityHealth>,
    database_root: PathBuf,
    database_incarnation: String,
}
impl ClusterServiceOpenGuard {
    fn capture(database: &BicDb) -> Result<Self> {
        Ok(Self {
            open: database.native_metadata_owner_open_lifetime(),
            health: database.native_metadata_owner_health()?,
            database_root: database.data_path().canonicalize()?,
            database_incarnation: database.incarnation_id().into(),
        })
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        if self.open.upgrade().is_none() {
            return Err(data_error("cluster service actual database open ended"));
        }
        self.health.check().map_err(crate::error::durability_error)
    }
    pub(crate) fn require_database(&self, database: &BicDb) -> Result<()> {
        self.require_live()?;
        if self.database_root != database.data_path().canonicalize()?
            || self.database_incarnation != database.incarnation_id()
        {
            return Err(data_error(
                "cluster service belongs to another actual database/open",
            ));
        }
        database.check_recovery_state()
    }
}

#[derive(Debug)]
pub(super) struct ServiceStoragePolicy {
    actual_open: ClusterServiceOpenGuard,
    database_root: PathBuf,
    database_incarnation: String,
    directory: Option<std::sync::Weak<bicdb_page::DirectoryLock>>,
    native_format_at_open: bool,
    writer_enforcement_version: u32,
}
impl ServiceStoragePolicy {
    pub(super) fn capture(database: &BicDb) -> Result<Self> {
        Ok(Self {
            actual_open: ClusterServiceOpenGuard::capture(database)?,
            database_root: database.data_path().canonicalize()?,
            database_incarnation: database.incarnation_id().into(),
            directory: database.native_service_directory_lifetime(),
            writer_enforcement_version: database.actual_native_writer_enforcement_version()?,
            native_format_at_open: database.native_range_image_birth_present()
                || crate::format::range_image_install_active(database.data_path())?,
        })
    }
    pub(super) fn native_writer_enforcement_version(&self) -> Result<u32> {
        self.actual_open.require_live()?;
        let flag = crate::format::native_writer_enrollment_active(&self.database_root)?;
        if flag != (self.writer_enforcement_version == 1)
            || (flag && self.directory.as_ref().and_then(|d| d.upgrade()).is_none())
        {
            return Err(data_error("native writer service birth/flag/open changed"));
        }
        Ok(self.writer_enforcement_version)
    }
    fn requires_native_catalog(&self, catalog_root: &Path) -> Result<bool> {
        Ok(self.native_format_at_open
            || crate::format::range_image_install_active(&self.database_root)?
            || crate::format::range_image_install_active(catalog_root)?)
    }
    pub(super) fn validate_configuration(&self, catalog_root: &Path, fsync: bool) -> Result<()> {
        self.actual_open.require_live()?;
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
        self.actual_open.require_database(database)?;
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
    pub(crate) fn listener_open_guard(&self) -> ClusterServiceOpenGuard {
        self.storage_policy.actual_open.clone()
    }
    pub(crate) fn require_catalog_storage_policy(&self) -> Result<()> {
        self.storage_policy.actual_open.require_live()?;
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
