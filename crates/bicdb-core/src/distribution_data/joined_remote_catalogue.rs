//! Dedicated original Joined selected adoption. No generic mutable Store
//! callback, uncertainty-clear escape or ordinary durability relaxation.
use super::*;
use crate::distribution_range_consensus::{
    NativeJoinedRemoteCataloguePoint, NativeJoinedRemoteCataloguePublication,
};
impl ClusterDataNodeService {
    pub(crate) fn persist_joined_remote_catalogue_floor(
        &mut self,
        db: &BicDb,
        point: &NativeJoinedRemoteCataloguePoint<'_, '_>,
        retained: &mut Option<NativeJoinedRemoteCataloguePublication>,
    ) -> Result<crate::db::NativeJoinedRemoteImageProgress> {
        self.storage_policy.require_database(db)?;
        self.range_writes.require_actual_abort_catalogue(db)?;
        let result = {
            let store = self.range_writes.current_mut();
            if retained.is_none() {
                *retained = Some(NativeJoinedRemoteCataloguePublication::begin(
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

impl ClusterDataNodeService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_joined_remote_catalogue_from_original(
        &mut self,
        db: &BicDb,
        point: &crate::distribution_native_host::NativeJoinedRemoteImagePoint<'_>,
        scope: &crate::db::NativeJoinedRemoteMutationScope<'_>,
        fence: &Arc<crate::distribution_range_consensus::NativeJoinedRemoteInstallingFence>,
        physical: &crate::db::NativeInstalledRangeImageBase,
        boundary: &Arc<crate::db::JoinedRemoteBaseBoundary>,
        final_image: &crate::db::NativeJoinedRemoteFinalImageReceipt,
        retained: &mut Option<NativeJoinedRemoteCataloguePublication>,
    ) -> Result<crate::db::NativeJoinedRemoteImageProgress> {
        self.storage_policy.require_database(db)?;
        self.range_writes.require_actual_abort_catalogue(db)?;
        let proposal = db.read_native_joined_remote_proposal_for_catalogue(
            &self.range_writes,
            scope,
            fence.protected_proposal(),
        )?;
        let actual = NativeJoinedRemoteCataloguePoint::new(
            point,
            scope,
            fence,
            physical,
            boundary,
            final_image,
            &proposal,
        );
        self.persist_joined_remote_catalogue_floor(db, &actual, retained)
    }
}
