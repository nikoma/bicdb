//! Necessary fsync/root/reader-fence policy. This grants no birth, quorum,
//! recovery role, physical materialization or read authority.
use super::*;
impl RangeWriteStore {
    pub(crate) fn require_native_catalog_durability(
        &self,
        actual_database_root: &Path,
    ) -> Result<()> {
        if !self.fsync {
            return Err(range_write_error(
                "native range catalog requires fsync=true",
            ));
        }
        if self.root.canonicalize()? != actual_database_root.canonicalize()? {
            return Err(range_write_error(
                "native range catalog root differs from actual database root",
            ));
        }
        if !crate::format::range_image_install_active(actual_database_root)? {
            return Err(range_write_error(
                "native range catalog requires its actual image reader fence",
            ));
        }
        self.require_durable_recovery_catalog()
    }
    /// Conditional only for explicitly legacy callers. Actual persisted format
    /// flags are reread, so a service opened before activation cannot keep an
    /// async writer policy after its real DB/storage becomes image-aware.
    pub(crate) fn require_service_catalog_durability(
        &self,
        actual_database_root: &Path,
        native_format_at_open: bool,
    ) -> Result<()> {
        if native_format_at_open
            || crate::format::range_image_install_active(actual_database_root)?
            || crate::format::range_image_install_active(&self.root)?
        {
            self.require_native_catalog_durability(actual_database_root)?;
        }
        Ok(())
    }
}
