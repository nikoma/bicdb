//! Dedicated additional-range Installing sink. No general mutable Store
//! callback and no reuse of the Registered/FIRST authority constructor.
use super::*;
use crate::db::{NativeJoinedRemoteAttemptScope, NativeJoinedRemoteProposalReceipt};
use crate::distribution_range_consensus::{
    NativeJoinedRemoteInstallingFence, NativeJoinedRemoteInstallingPublication,
};
impl ClusterDataNodeService {
    pub(crate) fn persist_joined_remote_installing(
        &mut self,
        db: &BicDb,
        scope: &NativeJoinedRemoteAttemptScope<'_>,
        receipt: &NativeJoinedRemoteProposalReceipt,
        retained: &mut Option<NativeJoinedRemoteInstallingPublication>,
    ) -> Result<Arc<NativeJoinedRemoteInstallingFence>> {
        self.storage_policy.require_database(db)?;
        scope.require_store_identity(db, &self.range_writes)?;
        let result = {
            let store = self.range_writes.current_mut();
            if retained.is_none() {
                // Retain the actual selected candidate owner before its first
                // possible persistence. Retry does not create another intent.
                *retained = Some(NativeJoinedRemoteInstallingPublication::begin(
                    store, db, scope, receipt,
                )?);
            }
            retained
                .as_mut()
                .unwrap()
                .advance(store, db, scope, receipt)
        };
        let post = self
            .storage_policy
            .require_database(db)
            .and_then(|()| scope.require_store_identity(db, &self.range_writes));
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(fence) => {
                post?;
                self.require_native_catalog_database(db)?;
                Ok(fence)
            }
        }
    }
}
