//! Read-only fact from an ACTUAL protected metadata owner. It cannot initialize
//! a store, promote a node, or bless an unwritten factory under a Voter role.
use super::*;

pub(crate) struct NativeOwnedMetadataMemberObservation {
    pub(super) store_id: Uuid,
    node: ClusterNodeId,
    birth: crate::distribution::NativeStoreBirthV1,
    topology: ClusterTopology,
    restore: Option<MetadataRestoreActivation>,
    term: u64,
    read_index: u64,
    actual_open: std::sync::Weak<()>,
    cancellation: crate::CancellationToken,
}
impl NativeOwnedMetadataMemberObservation {
    pub(crate) fn store_id(&self) -> Uuid {
        self.store_id
    }
    pub(crate) fn topology(&self) -> &ClusterTopology {
        &self.topology
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.cancellation.check()?;
        if !std::sync::Weak::ptr_eq(&self.actual_open, &db.native_metadata_owner_open_lifetime())
            || self.actual_open.upgrade().is_none()
        {
            return Err(metadata_error(
                "owned metadata observation belongs to another actual DB open",
            ));
        }
        let native = db.native_recovery_admission(
            &self.topology.cluster_id,
            self.topology
                .range_image_bootstrap_id
                .ok_or_else(|| metadata_error("owned member bootstrap absent"))?,
            &self.node,
            self.birth.node_incarnation(),
        )?;
        if native.birth_id() != self.birth.birth_id()
            || native.birth_sha256() != self.birth.birth_sha256()
            || native.restore_activation() != self.restore.as_ref()
        {
            return Err(metadata_error(
                "owned metadata member native birth/restore changed",
            ));
        }
        let record = db.read_native_metadata_store(&self.topology, &self.node)?;
        if !record.registered() || record.store_id() != self.store_id {
            return Err(metadata_error(
                "actual protected Registered metadata owner differs",
            ));
        }
        // Preserve original learner namespace protection even AFTER promotion.
        record.require_namespaces_current(db)?;
        db.check_recovery_state()?;
        self.cancellation.check()
    }
    pub(crate) fn require_current(&self, store: &MetadataConsensusStore) -> Result<()> {
        self.cancellation.check()?;
        let id = store.registered_native_metadata_member_store_id()?;
        let owner = store
            .native_owner
            .as_ref()
            .ok_or_else(|| metadata_error("actual owned metadata store absent"))?;
        if id != self.store_id
            || store.state.node_id != self.node
            || store.state.current_term != self.term
            || store.state.commit_index != self.read_index
            || store.state.committed_topology != self.topology
            || store.state.committed_restore_activation != self.restore
            || !std::sync::Weak::ptr_eq(&owner.actual_open, &self.actual_open)
        {
            return Err(metadata_error(
                "actual owned metadata member state changed during validation",
            ));
        }
        Ok(())
    }
}
impl MetadataConsensusStore {
    pub(crate) fn registered_native_metadata_member_store_id(&self) -> Result<Uuid> {
        self.require_native_metadata_actor_current(false)?;
        let owner = self
            .native_owner
            .as_ref()
            .ok_or_else(|| metadata_error("actual native metadata owner absent"))?;
        let id = owner.registered_store_id.ok_or_else(|| {
            metadata_error("Prepared native metadata cannot become an owned member")
        })?;
        if self.state.native_store_id != Some(id) {
            return Err(metadata_error(
                "actual protected metadata StoreUUID differs",
            ));
        }
        Ok(id)
    }
    pub(crate) fn observe_owned_native_metadata_member(
        &self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<NativeOwnedMetadataMemberObservation> {
        observed.require_live_request(business)?;
        parent.check()?;
        let id = self.registered_native_metadata_member_store_id()?;
        if observed.requester_node_id() != &self.state.node_id
            || observed.term() != self.state.current_term
            || observed.read_index() != self.state.commit_index
            || observed.committed_topology(business)? != &self.state.committed_topology
            || observed.restore_activation(business)?
                != self.state.committed_restore_activation.as_ref()
        {
            return Err(metadata_error(
                "owned metadata member requires actual independently fresh OWN installed CONFIG",
            ));
        }
        let member = self
            .state
            .committed_topology
            .nodes
            .get(&self.state.node_id)
            .ok_or_else(|| metadata_error("owned member absent"))?;
        if member.lifecycle != ClusterNodeLifecycle::Active
            || member.incarnation != observed.requester_incarnation()
        {
            return Err(metadata_error("owned metadata member is retired/replaced"));
        }
        if member.metadata_role == MetadataMemberRole::Voter {
            self.require_native_metadata_actor_current(true)?;
        }
        let birth = member
            .native_store_birth_v1
            .clone()
            .ok_or_else(|| metadata_error("owned metadata member birth unbound"))?;
        let owner = self.native_owner.as_ref().unwrap();
        crate::distribution_range_consensus::validate_native_json_size(
            &self.state.committed_topology,
            2 * 1024 * 1024,
        )?;
        Ok(NativeOwnedMetadataMemberObservation {
            store_id: id,
            node: self.state.node_id.clone(),
            birth,
            topology: self.state.committed_topology.clone(),
            restore: self.state.committed_restore_activation.clone(),
            term: self.state.current_term,
            read_index: self.state.commit_index,
            actual_open: owner.actual_open.clone(),
            cancellation: parent.child_with_deadline(observed.deadline()),
        })
    }
}
