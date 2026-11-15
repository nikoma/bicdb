//! Same-service validation of the real Registered remote-image request.
//! No Store accessor, writer installation or row admission is exported.
use super::*;
impl ClusterDataNodeService {
    pub(crate) fn require_registered_remote_image_step(
        &self,
        step: &crate::distribution_native_host::NativeRegisteredRemoteImageStep<'_>,
        database: &BicDb,
    ) -> Result<()> {
        self.require_native_catalog_database(database)?;
        step.require_database_store(database, &self.range_writes)
    }
}
