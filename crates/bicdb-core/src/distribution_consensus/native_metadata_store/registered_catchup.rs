//! Lower authority than owned-member READY: the actual Registered store may
//! still retain an older Learner C. This grants only listener/catch-up custody.
use super::*;

pub(crate) struct NativeRegisteredMetadataCatchupObservation<'a> {
    observed: &'a HostMetadataReadObservation,
    business: Uuid,
    store_id: Uuid,
    local_sha256: String,
    local: ClusterTopology,
    global: ClusterTopology,
    installed_current: bool,
    actual_open: std::sync::Weak<()>,
    cancellation: CancellationToken,
}
impl NativeRegisteredMetadataCatchupObservation<'_> {
    /// Necessary refusal/diagnostic only; READY still needs its separate
    /// exact owned-member producer and actual DB validation.
    pub(crate) fn local_installed_current(&self) -> Result<bool> {
        self.require_live()?;
        Ok(self.installed_current)
    }
    pub(crate) fn store_id(&self) -> Uuid {
        self.store_id
    }
    pub(crate) fn topology(&self) -> Result<&ClusterTopology> {
        self.require_live()?;
        Ok(&self.global)
    }
    fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.observed.require_live_request(self.business)
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.require_live()?;
        if !std::sync::Weak::ptr_eq(&self.actual_open, &db.native_metadata_owner_open_lifetime())
            || self.actual_open.upgrade().is_none()
        {
            return Err(metadata_error(
                "Registered catch-up belongs to another actual DB open",
            ));
        }
        let node = self.observed.requester_node_id();
        let member = &self.global.nodes[node];
        let native = db.native_recovery_admission(
            &self.global.cluster_id,
            self.global
                .range_image_bootstrap_id
                .ok_or_else(|| metadata_error("Registered catch-up bootstrap absent"))?,
            node,
            member.incarnation,
        )?;
        if member.native_store_birth_v1.as_ref()
            != Some(&crate::distribution::NativeStoreBirthV1::from_actual_native(&native)?)
            || native.restore_activation() != self.observed.restore_activation(self.business)?
            || db.current_commit_seq() != 0
        {
            return Err(metadata_error(
                "Registered catch-up actual birth/restore/business history differs",
            ));
        }
        let record = db.read_native_metadata_store(&self.global, node)?;
        if !record.registered()
            || record.store_id() != self.store_id
            || record.initial_namespace_ids().is_none()
        {
            return Err(metadata_error(
                "Registered catch-up needs actual protected UUID and original namespace binding",
            ));
        }
        record.require_namespaces_current(db)?;
        let schema = db.schema_compatibility_fingerprint()?.sha256;
        if [&self.local, &self.global].iter().any(|topology| {
            topology
                .ranges
                .values()
                .any(|range| range.required_schema_sha256(&topology.nodes) != Some(schema.as_str()))
        }) {
            return Err(metadata_error(
                "Registered catch-up actual local/current schema changed",
            ));
        }
        db.check_recovery_state()?;
        self.require_live()
    }
    pub(crate) fn require_current(&self, store: &MetadataConsensusStore) -> Result<()> {
        self.require_live()?;
        let owner = store
            .native_owner
            .as_ref()
            .ok_or_else(|| metadata_error("Registered catch-up owner absent"))?;
        if store.registered_native_metadata_member_store_id()? != self.store_id
            || !std::sync::Weak::ptr_eq(&owner.actual_open, &self.actual_open)
            || state_checksum(&store.state)? != self.local_sha256
            || store.state.committed_topology != self.local
        {
            return Err(metadata_error(
                "Registered catch-up actual persisted owner/state changed",
            ));
        }
        Ok(())
    }
}
impl MetadataConsensusStore {
    pub(crate) fn observe_registered_native_metadata_catchup<'a>(
        &self,
        observed: &'a HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<NativeRegisteredMetadataCatchupObservation<'a>> {
        observed.require_live_request(business)?;
        parent.check()?;
        let id = self.registered_native_metadata_member_store_id()?;
        let global = observed.committed_topology(business)?;
        let local = &self.state.committed_topology;
        let node = &self.state.node_id;
        let old = local
            .nodes
            .get(node)
            .ok_or_else(|| metadata_error("Registered local actor absent"))?;
        let current = global
            .nodes
            .get(node)
            .ok_or_else(|| metadata_error("Registered current actor absent"))?;
        if observed.requester_node_id() != node
            || observed.requester_incarnation() != current.incarnation
            || global.cluster_id != self.state.cluster_id
            || global.range_image_bootstrap_id != local.range_image_bootstrap_id
            || global.generation < local.generation
            || (global.generation == local.generation && global != local)
            || observed.term() < self.state.current_term
            || observed.read_index() < self.state.commit_index
            || observed.restore_activation(business)?
                != self.state.committed_restore_activation.as_ref()
            || old.lifecycle != ClusterNodeLifecycle::Active
            || current.lifecycle != ClusterNodeLifecycle::Active
            || old.incarnation != current.incarnation
            || old.tls_certificate_sha256 != current.tls_certificate_sha256
            || old.pending_tls_certificate_sha256 != current.pending_tls_certificate_sha256
            || old.native_store_birth_v1.is_none()
            || old.native_store_birth_v1 != current.native_store_birth_v1
            || [local, global].iter().any(|topology| {
                topology
                    .ranges
                    .values()
                    .any(|r| r.replicas.iter().any(|p| &p.node_id == node))
            })
            || !local.is_metadata_voter(observed.metadata_leader_id())
        {
            return Err(metadata_error("Registered catch-up needs unchanged actual actor/no-data/restore and a current leader in retained local C; exact snapshot admission otherwise unsupported"));
        }
        crate::distribution_range_consensus::validate_native_json_size(local, 2 * 1024 * 1024)?;
        crate::distribution_range_consensus::validate_native_json_size(global, 2 * 1024 * 1024)?;
        let result = NativeRegisteredMetadataCatchupObservation {
            observed,
            business,
            store_id: id,
            local_sha256: state_checksum(&self.state)?,
            local: local.clone(),
            global: global.clone(),
            installed_current: observed.term() == self.state.current_term
                && observed.read_index() == self.state.commit_index
                && global == local,
            actual_open: self.native_owner.as_ref().unwrap().actual_open.clone(),
            cancellation: parent.child_with_deadline(observed.deadline()),
        };
        result.require_current(self)?;
        Ok(result)
    }
    /// Registered means reopen/refsync only. This path cannot allocate a UUID,
    /// write original-C/Prepare, initialize a blank voter, or reset any log state.
    pub(crate) fn recover_registered_native_metadata_retaining(
        root: &Path,
        database: Arc<RwLock<BicDb>>,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
        retained: &mut Option<Self>,
    ) -> Result<()> {
        observed.require_live_request(business)?;
        let token = parent.child_with_deadline(observed.deadline());
        let topology = observed.committed_topology(business)?.clone();
        let node = observed.requester_node_id().clone();
        {
            let db = database
                .try_read()
                .ok_or_else(|| metadata_error("Registered reopen DB busy"))?;
            let record = db.read_native_metadata_store(&topology, &node)?;
            if !record.registered() || record.initial_namespace_ids().is_none() {
                return Err(metadata_error(
                    "Registered reopen refuses missing/Prepared/old-unbound native identity",
                ));
            }
            record.require_namespaces_current(&db)?;
        }
        if retained.is_none() {
            Self::reopen_native_owned_retaining(
                root,
                Arc::clone(&database),
                node,
                topology,
                &token,
                retained,
            )?;
        }
        let store = retained
            .as_ref()
            .ok_or_else(|| metadata_error("actual Registered retained owner absent"))?;
        let proof = store.observe_registered_native_metadata_catchup(observed, business, &token)?;
        {
            let db = database.try_read().ok_or_else(|| {
                metadata_error("Registered reopen DB busy validating actual record")
            })?;
            proof.require_database(&db)?;
        }
        proof.require_current(store)?;
        drop(proof);
        // Resolves exact retained same-UUID persistence ambiguity. Never resets
        // current term/vote/log/C or constructs a new identity from status.
        retained.as_ref().unwrap().persist()?;
        observed.require_live_request(business)?;
        token.check()
    }
}
