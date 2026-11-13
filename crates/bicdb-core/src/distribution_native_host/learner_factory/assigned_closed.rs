//! Original actual factory continuation ONLY; never blank creation or voting.
use super::*;
impl NativeUnassignedLearnerFactory {
    pub(in crate::distribution_native_host) fn require_assigned_closed_database(
        &self,
        db: &BicDb,
        proof: &crate::distribution_consensus::NativeOwnedMetadataMemberObservation,
        observed: &HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        observed.require_live_request(business)?;
        let topology = observed.committed_topology(business)?;
        if !self.spec.initial_ranges.is_empty()
            || proof.topology() != topology
            || observed.requester_node_id() != &self.spec.node_id
            || observed.requester_incarnation() != self.spec.member_incarnation
            || observed.restore_activation(business)? != self.spec.restore_activation.as_ref()
            || topology
                .nodes
                .get(&self.spec.node_id)
                .and_then(|n| n.native_store_birth_v1.as_ref())
                != Some(&self.manifest.birth)
        {
            return Err(invalid(
                "assigned CLOSED receiver changed original factory/owned birth",
            ));
        }
        proof.require_database(db)?;
        let native = db.native_recovery_admission(
            &self.spec.cluster_id,
            self.spec.bootstrap_id,
            &self.spec.node_id,
            self.spec.member_incarnation,
        )?;
        if native.metadata_initialization_version() != 1
            || native.abort_protocol_version() != 1
            || native.writer_enforcement_version() != 1
            || NativeStoreBirthV1::from_actual_native(&native)? != self.manifest.birth
            || native.restore_activation() != self.spec.restore_activation.as_ref()
            || db.schema_compatibility_fingerprint()?.sha256 != self.manifest.schema_sha256
            || topology.ranges.values().any(|range| {
                range.required_schema_sha256(&topology.nodes)
                    != Some(self.manifest.schema_sha256.as_str())
            })
        {
            return Err(invalid(
                "assigned CLOSED receiver original DB/birth/schema/no-writer differs",
            ));
        }
        // Explicit initial-fence lane; not the eventual populated receiver
        // contract. No current range floor may be silently adopted/reset.
        if db.current_commit_seq() != 0 || !db.native_abort_slot_inventory()?.is_empty() {
            return Err(invalid("assigned CLOSED initial receiver has protected business obligations; populated continuation unsupported"));
        }
        db.require_original_registered_receiver_domain(&native)?;
        db.check_recovery_state()?;
        observed.require_live_request(business)
    }
}
