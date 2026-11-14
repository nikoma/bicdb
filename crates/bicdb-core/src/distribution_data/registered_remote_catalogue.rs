//! Dedicated actual mutable Store sink, without a general recovery callback.
use super::*;
use crate::distribution_native_host::NativeRegisteredRemoteInstallPreflight;
use crate::distribution_range_consensus::{
    NativeRegisteredRemoteCataloguePoint, NativeRegisteredRemoteCataloguePublication,
};
impl ClusterDataNodeService {
    pub(crate) fn require_retained_registered_remote_adoption(
        &self,
        db: &BicDb,
        publication: &NativeRegisteredRemoteCataloguePublication,
        preflight: &NativeRegisteredRemoteInstallPreflight,
    ) -> Result<()> {
        self.storage_policy.require_database(db)?;
        self.range_writes.require_actual_abort_catalogue(db)?;
        publication.require_request_catalogue(&self.range_writes, db, preflight)
    }
    pub(crate) fn persist_registered_remote_catalogue_floor(
        &mut self,
        db: &BicDb,
        point: &NativeRegisteredRemoteCataloguePoint<'_>,
        retained: &mut Option<NativeRegisteredRemoteCataloguePublication>,
    ) -> Result<crate::db::NativeRegisteredRemoteImageProgress> {
        self.storage_policy.require_database(db)?;
        self.range_writes.require_actual_abort_catalogue(db)?;
        let result = {
            let store = self.range_writes.current_mut();
            if retained.is_none() {
                *retained = Some(NativeRegisteredRemoteCataloguePublication::begin(
                    store, db, point,
                )?);
            }
            retained.as_mut().unwrap().advance(store, db, point)
        };
        let post = self
            .storage_policy
            .require_database(db)
            .and_then(|()| self.range_writes.require_actual_abort_catalogue(db));
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(progress) => {
                post?;
                self.require_native_catalog_database(db)?;
                Ok(progress)
            }
        }
    }
}
