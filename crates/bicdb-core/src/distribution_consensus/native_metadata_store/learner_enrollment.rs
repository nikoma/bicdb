//! Existing registered learner enrollment: authenticated birth FACTS are
//! rebased only onto this actual owned leader's independently fresh Host MQ.
//! Registration, catch-up, promotion and data admission are separate steps.
use super::*;
use crate::distribution_native_host::NativeLearnerBirthObservation;

impl MetadataConsensusStore {
    pub(crate) fn propose_observed_native_learner_enrollment(
        &mut self,
        birth: &NativeLearnerBirthObservation,
        current: &HostMetadataReadObservation,
        business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<Option<MetadataLogEntry>> {
        parent.check()?;
        birth.require_live()?;
        self.require_native_learner_enrollment_leader(current, business)?;
        birth.require_enrollment_rebase(
            &self.state.committed_topology,
            &self.state.node_id,
            self.state.committed_restore_activation.as_ref(),
        )?;
        // Every old voter is an enrolled actual native member. An unbound
        // learner contributes neither an old vote nor numeric installed proof.
        for member in self.state.committed_topology.nodes.values().filter(|m| {
            m.lifecycle == ClusterNodeLifecycle::Active
                && m.metadata_role == MetadataMemberRole::Voter
        }) {
            let actual = member.native_store_birth_v1.as_ref().ok_or_else(|| {
                metadata_error("native learner enrollment requires an enrolled native voter cohort")
            })?;
            actual.validate_for(
                self.state.committed_topology.range_image_bootstrap_id,
                member.incarnation,
            )?;
            if actual.writer_enforcement_version() != 1 {
                return Err(metadata_error(
                    "native learner enrollment requires enforced native voters",
                ));
            }
        }
        let node = birth.node()?;
        let actual = birth.birth()?;
        let target = self
            .state
            .committed_topology
            .nodes
            .get(node)
            .ok_or_else(|| metadata_error("registered learner disappeared"))?;
        if let Some(existing) = &target.native_store_birth_v1 {
            if self.state.log.iter().any(|entry| !entry.leadership_barrier) {
                return Err(metadata_error(
                    "native learner enrollment conflicts with another pending topology proposal",
                ));
            }
            if existing != actual {
                return Err(metadata_error(
                    "native learner enrollment refuses overwrite of another actual birth",
                ));
            }
            parent.check()?;
            birth.require_live()?;
            current.require_live_request(business)?;
            return Ok(None);
        }
        let mut proposed = self.state.committed_topology.clone();
        proposed
            .nodes
            .get_mut(node)
            .ok_or_else(|| metadata_error("registered learner disappeared"))?
            .native_store_birth_v1 = Some(actual.clone());
        proposed.generation = proposed
            .generation
            .checked_add(1)
            .ok_or_else(|| metadata_error("metadata generation exhausted"))?;
        // No old snapshot, caller-provided index or proposal DTO is installed.
        // Normal metadata proposal validation and actual old-voter commit apply.
        parent.check()?;
        birth.require_live()?;
        current.require_live_request(business)?;
        let entry = if let Some(pending) = self
            .state
            .log
            .iter()
            .find(|entry| !entry.leadership_barrier)
            .cloned()
        {
            if pending.topology != proposed
                || pending.restore_activation.is_some()
                || pending.term != self.state.current_term
            {
                return Err(metadata_error(
                    "native learner enrollment conflicts with another pending topology proposal",
                ));
            }
            // Retry only an ACTUAL persisted identical proposal. An in-memory
            // entry left by a failed pre-publication write is not that proof.
            let persisted = decode_state(&fs::read(
                self.root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
            )?)?;
            validate_state(&persisted)?;
            if persisted.cluster_id != self.state.cluster_id
                || persisted.node_id != self.state.node_id
                || persisted.native_store_id != self.state.native_store_id
                || persisted.current_term != self.state.current_term
                || persisted.commit_index != self.state.commit_index
                || persisted.committed_topology != self.state.committed_topology
                || persisted.committed_restore_activation != self.state.committed_restore_activation
                || persisted
                    .log
                    .iter()
                    .find(|entry| entry.index == pending.index)
                    != Some(&pending)
            {
                return Err(metadata_error(
                    "native learner enrollment pending entry is not the actual persisted identity",
                ));
            }
            // Re-fsync the exact owned entry without allocating a new index.
            self.persist()?;
            pending
        } else {
            self.propose_topology(proposed)?
        };
        // Persistence may finish after cancellation. Report no successful
        // controller result from an expired fact; any durable proposal remains
        // ordinary metadata state to be reconciled, never silently removed.
        parent.check()?;
        birth.require_live()?;
        current.require_live_request(business)?;
        Ok(Some(entry))
    }

    fn require_native_learner_enrollment_leader(
        &self,
        current: &HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        current.require_live_request(business)?;
        self.require_native_metadata_actor_current(true)?;
        if self.native_owner.is_none()
            || self.state.role != MetadataConsensusRole::Leader
            || current.requester_node_id() != &self.state.node_id
            || current.metadata_leader_id() != &self.state.node_id
            || current.requester_incarnation()
                != self.state.committed_topology.nodes[&self.state.node_id].incarnation
            || current.term() != self.state.current_term
            || current.read_index() != self.state.commit_index
            || current.committed_topology(business)? != &self.state.committed_topology
            || current.restore_activation(business)?
                != self.state.committed_restore_activation.as_ref()
        {
            return Err(metadata_error("native learner enrollment requires independently fresh actual owned leader C/term/readIndex"));
        }
        Ok(())
    }
}
