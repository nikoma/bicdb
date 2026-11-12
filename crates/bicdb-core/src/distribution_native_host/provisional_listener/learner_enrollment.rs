//! Finite existing-learner controller; no registration queue or automatic join.
use super::*;
use crate::distribution::NativeStoreBirthV1;
use crate::distribution_consensus::MetadataConsensusRole;
use std::time::Instant;

/// A committed identity-binding result, never a vote/owned-backend/Ready grant.
#[derive(Debug)]
pub(crate) struct NativeLearnerMetadataEnrollment {
    node: ClusterNodeId,
    birth: NativeStoreBirthV1,
    committed_index: u64,
    metadata_term: u64,
}
impl NativeLearnerMetadataEnrollment {
    pub(crate) fn node(&self) -> &ClusterNodeId {
        &self.node
    }
    pub(crate) fn birth(&self) -> &NativeStoreBirthV1 {
        &self.birth
    }
    pub(crate) fn committed_index(&self) -> u64 {
        self.committed_index
    }
    pub(crate) fn metadata_term(&self) -> u64 {
        self.metadata_term
    }
}
impl NativeInitialConsensusLaunch {
    pub(crate) fn enroll_registered_native_learner(
        &self,
        target: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeLearnerMetadataEnrollment> {
        let token = parent.child_with_deadline(Instant::now() + Duration::from_secs(5));
        token.check()?;
        let leader = &self.seed.spec.node_id;
        // All network work occurs outside service/DB/metadata latches. These
        // are distinct MQ contexts, both bounded by the ORIGINAL operation.
        let first_business = Uuid::now_v7();
        let first = self.transport.acquire_host_metadata_observation(
            leader,
            first_business,
            Duration::from_secs(5),
            &token,
        )?;
        self.transport
            .install_topology(first.committed_topology(first_business)?.clone())?;
        let birth = self
            .transport
            .acquire_learner_birth(target, first, first_business, &token)?;
        let current_business = Uuid::now_v7();
        let current = self.transport.acquire_host_metadata_observation(
            leader,
            current_business,
            Duration::from_secs(5),
            &token,
        )?;
        token.check()?;
        birth.require_live()?;
        current.require_live_request(current_business)?;
        let actual_open = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("enrollment service busy"))?
            .listener_open_guard();
        let proposed = {
            let mut store = self.lock_initial_metadata(&token)?;
            store.require_listener_database(&actual_open)?;
            store.propose_observed_native_learner_enrollment(
                &birth,
                &current,
                current_business,
                &token,
            )?
        };
        let proposed_index = proposed.as_ref().map(|entry| entry.index);
        self.heartbeat_initial_metadata(&token)?;
        token.check()?;
        birth.require_live()?;
        actual_open.require_live()?;
        let store = self.lock_initial_metadata(&token)?;
        store.require_listener_database(&actual_open)?;
        let status = store.status();
        if status.role != MetadataConsensusRole::Leader
            || status.current_term != current.term()
            || proposed_index.is_some_and(|index| status.commit_index < index)
            || store
                .committed_topology()
                .nodes
                .get(target)
                .and_then(|n| n.native_store_birth_v1.as_ref())
                != Some(birth.birth()?)
        {
            return Err(invalid(
                "actual old-voter metadata quorum has not committed the observed learner birth",
            ));
        }
        birth.require_enrollment_rebase(
            store.committed_topology(),
            leader,
            store.committed_restore_activation(),
        )?;
        token.check()?;
        Ok(NativeLearnerMetadataEnrollment {
            node: target.clone(),
            birth: birth.birth()?.clone(),
            committed_index: status.commit_index,
            metadata_term: status.current_term,
        })
    }
}
