//! Dedicated original Registered catalogue sink. No general mutable Store
//! callback and no exception to ordinary writer/durability admission.
use super::*;
use crate::db::{NativeRegisteredRemoteAttemptScope, NativeRegisteredRemoteProposalReceipt};
use crate::distribution_native_host::NativeRegisteredRemoteInstallPreflight;
use crate::distribution_native_host::NativeRegisteredRemoteProposalPoint;
use crate::distribution_range_consensus::{
    NativeRegisteredRemoteInstallingFence, NativeRegisteredRemoteInstallingPublication,
};
impl ClusterDataNodeService {
    pub(crate) fn require_retained_registered_remote_catalogue(
        &self,
        db: &BicDb,
        publication: &NativeRegisteredRemoteInstallingPublication,
        preflight: &NativeRegisteredRemoteInstallPreflight,
    ) -> Result<()> {
        self.storage_policy.require_database(db)?;
        self.range_writes.require_actual_abort_catalogue(db)?;
        publication.require_request_catalogue(&self.range_writes, db, preflight)
    }
    pub(crate) fn persist_registered_remote_installing(
        &mut self,
        db: &BicDb,
        point: &NativeRegisteredRemoteProposalPoint<'_>,
        scope: &NativeRegisteredRemoteAttemptScope<'_>,
        receipt: &NativeRegisteredRemoteProposalReceipt,
        retained: &mut Option<NativeRegisteredRemoteInstallingPublication>,
    ) -> Result<Arc<NativeRegisteredRemoteInstallingFence>> {
        self.storage_policy.require_database(db)?;
        self.range_writes.require_actual_abort_catalogue(db)?;
        point.require_original_boundary(db, scope)?;
        let result = {
            let store = self.range_writes.current_mut();
            if retained.is_none() {
                // This first prepare requires the original durable CLOSED
                // catalogue. Exact retries instead consume the retained owner,
                // so uncertain persistence cannot prevent reaching re-fsync.
                *retained = Some(NativeRegisteredRemoteInstallingPublication::begin(
                    store, db, point, scope, receipt,
                )?);
            }
            retained
                .as_mut()
                .unwrap()
                .advance(store, db, point, scope, receipt)
        };
        let post = self
            .storage_policy
            .require_database(db)
            .and_then(|()| self.range_writes.require_actual_abort_catalogue(db));
        match result {
            Ok(fence) => {
                post?;
                self.require_native_catalog_database(db)?;
                Ok(fence)
            }
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
        }
    }
}
