//! Metadata-owner catalogue comparison only. No range-leader plan, recovery
//! Role, OLD/NEW local quorum, writer, Frozen or activation can be produced.
use super::*;
use crate::distribution_consensus::{HostMetadataReadObservation, MetadataConsensusStore};
use crate::distribution_data::{ActualNativeJointMetadataPoint, ClusterServiceOpenGuard};
use std::sync::Arc;

pub(crate) struct NativeMetadataOwnerCatalogueScope {
    data: NativeJointScopeData,
    own: HostMetadataReadObservation,
    business: Uuid,
    point: Arc<ActualNativeJointMetadataPoint>,
    open: ClusterServiceOpenGuard,
    caller: CancellationToken,
}
impl NativeMetadataOwnerCatalogueScope {
    /// All inputs are actual sealed observations/owners. The held MStore is
    /// checked against the SAME protected point without recursively locking it.
    pub(crate) fn from_actual_metadata_owner(
        own: HostMetadataReadObservation,
        business: Uuid,
        point: Arc<ActualNativeJointMetadataPoint>,
        open: ClusterServiceOpenGuard,
        store: &MetadataConsensusStore,
        manifest: &NativeRemoteImageManifestV1,
        parent: &CancellationToken,
    ) -> Result<Arc<Self>> {
        parent.check()?;
        own.require_live_request(business)?;
        store.require_listener_database(&open)?;
        store.require_native_control_leader(&own, business)?;
        point.require_request(&own, business)?;
        point.require_current_metadata_store(store)?;
        let data = NativeJointScopeData::from_actual_catalogue_owner(
            &own,
            business,
            manifest.relocation_id,
        )?;
        let caller = parent.child_with_deadline(own.deadline());
        let result = Arc::new(Self {
            data,
            own,
            business,
            point,
            open,
            caller,
        });
        result.require_manifest(manifest)?;
        result.require_live()?;
        Ok(result)
    }
    /// Test-only necessary original-request check, not full currentness and
    /// never a native/metadata permission or a raw token/OWN accessor.
    #[cfg(test)]
    pub(crate) fn test_original_request_live(&self) -> Result<()> {
        self.caller.check()?;
        self.own.require_live_request(self.business)
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.caller.check()?;
        self.own.require_live_request(self.business)?;
        self.open.require_live()?;
        self.point.require_request(&self.own, self.business)?;
        self.data.validate()
    }
    pub(crate) fn require_fresh_proposal_owner(
        &self,
        own: &HostMetadataReadObservation,
        business: Uuid,
        point: &ActualNativeJointMetadataPoint,
        open: &ClusterServiceOpenGuard,
        store: &MetadataConsensusStore,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.require_live()?;
        parent.check()?;
        own.require_live_request(business)?;
        store.require_listener_database(open)?;
        store.require_native_control_leader(own, business)?;
        // Full original owner/open/UUID/committed state and publication version,
        // not node labels or a freshly reconstructed same-purpose context.
        self.point.require_current_metadata_store(store)?;
        point.require_request(own, business)?;
        point.require_current_metadata_store(store)?;
        if own.requester_node_id() != self.own.requester_node_id()
            || own.requester_incarnation() != self.own.requester_incarnation()
            || own.metadata_leader_id() != self.own.metadata_leader_id()
            || own.term() < self.own.term()
            || own.read_index() < self.own.read_index()
            || NativeJointScopeData::from_actual_catalogue_owner(
                own,
                business,
                self.data.promise.relocation_id,
            )?
            .checksum()?
                != self.data.checksum()?
        {
            return Err(invalid(
                "catalogue scope changed original metadata owner or selected configuration",
            ));
        }
        parent.check()?;
        self.require_live()
    }
    pub(crate) fn require_manifest(&self, mf: &NativeRemoteImageManifestV1) -> Result<()> {
        mf.require_shape()?;
        NativeSourceExportBindingV1::from_manifest(mf)?
            .require_metadata_owner_catalogue_scope(self)?;
        mf.source_old_close_record
            .require_metadata_owner_catalogue_scope(self)?;
        if canonical_range(&mf.capture_range) != canonical_range(self.old_range())
            || canonical_range(&mf.activation_range) != canonical_range(self.activation_range())
            || canonical_range(&mf.native_predecessor)
                != canonical_range(&self.old_native_predecessor_data()?)
            || &mf.old_members != self.old_members()
            || &mf.new_members != self.new_members()
            || mf.schema_sha256 != self.constraint_schema_sha256()
            || mf.restore.as_ref() != self.constraint_restore_activation()
            || mf.old_config_sha256 != self.promise().old_config_sha256
            || mf.new_config_sha256 != self.promise().new_config_sha256
        {
            return Err(invalid(
                "remote NEW manifest changes actual complete pending plan/configuration",
            ));
        }
        Ok(())
    }
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        self.data.checksum()
    }
    pub(crate) fn promise(&self) -> &RangeHandoffPromise {
        &self.data.promise
    }
    pub(crate) fn old_range(&self) -> &crate::RangeDescriptor {
        &self.data.old_range
    }
    pub(crate) fn activation_range(&self) -> &crate::RangeDescriptor {
        &self.data.activation_range
    }
    pub(crate) fn old_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        &self.data.old_members
    }
    pub(crate) fn new_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        &self.data.new_members
    }
    pub(crate) fn enrolled_birth(&self, node: &ClusterNodeId) -> Result<&NativeStoreBirthV1> {
        self.data
            .enrolled_births
            .get(node)
            .ok_or_else(|| invalid("catalogue comparison actual enrolled birth absent"))
    }
    fn old_native_predecessor_data(&self) -> Result<crate::RangeDescriptor> {
        self.data.validate()?;
        let mut expected = self.data.old_range.clone();
        expected.replicas.retain(|r| r.node_id != self.data.target);
        expected.epoch = expected
            .epoch
            .checked_sub(1)
            .ok_or_else(|| invalid("joint expected native epoch underflow"))?;
        Ok(expected)
    }
    fn constraint_schema_sha256(&self) -> &str {
        &self.data.schema_sha256
    }
    fn constraint_restore_activation(
        &self,
    ) -> Option<&crate::distribution_consensus::MetadataRestoreActivation> {
        self.data.restore_activation.as_ref()
    }
}
