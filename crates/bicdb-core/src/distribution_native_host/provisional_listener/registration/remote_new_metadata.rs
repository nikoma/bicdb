//! Fresh recipient metadata for one bounded remote NEW operation. Acquisition
//! never holds a participant table, service, DB, native or publication guard.
use super::*;
use crate::distribution_consensus::{
    MetadataConsensusStore, MetadataReadParticipant, NativeOwnedMetadataMemberObservation,
};
use crate::distribution_data::{ActualNativeJointMetadataPoint, ClusterServiceOpenGuard};
use crate::distribution_range_handoff::{NativeRemoteNewOperationV1, NativeRemoteNewRequestV1};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use std::time::Duration;

pub(super) struct RemoteNewRecipientMetadata {
    pub(super) role: Option<Arc<RecoveryRoleAdmission>>,
    pub(super) own: Arc<HostMetadataReadObservation>,
    pub(super) business: Uuid,
    pub(super) point: Arc<ActualNativeJointMetadataPoint>,
    member: NativeOwnedMetadataMemberObservation,
    metadata: Arc<Mutex<MetadataConsensusStore>>,
    database: ClusterServiceOpenGuard,
}
impl RemoteNewRecipientMetadata {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn acquire(
        provider: &Arc<crate::distribution_metadata_read_provider::MetadataReadProvider>,
        transport: &Arc<TcpClusterRelocationTransport>,
        metadata: Arc<Mutex<MetadataConsensusStore>>,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        peer: &MetadataReadParticipant,
        request: &NativeRemoteNewRequestV1,
        mutation: bool,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        request.require_shape()?;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let local = transport.local_metadata_read_participant();
        let role = if mutation {
            let purpose = request.purpose();
            let witness = provider.witness_recovery_role(
                peer,
                business,
                request.manifest.capture_range.id,
                request.manifest.capture_range.epoch,
                request.manifest.nonce,
                challenge,
                purpose.clone(),
                parent,
            )?;
            Some(Arc::new(RecoveryRoleAdmission::from_witness(
                witness,
                peer,
                local.node_id(),
                business,
                request.manifest.capture_range.id,
                request.manifest.capture_range.epoch,
                request.manifest.nonce,
                challenge,
                &purpose,
            )?))
        } else {
            None
        };
        let leader = provider
            .current_metadata_leader_hint(parent)?
            .ok_or_else(|| invalid("remote NEW current metadata leader unavailable"))?;
        let remaining = parent
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or(BicDbError::QueryTimedOut)?;
        let own = Arc::new(transport.acquire_host_metadata_observation(
            &leader,
            business,
            remaining.min(Duration::from_secs(5)),
            parent,
        )?);
        request.require_current_own(&own, business, local.node_id())?;
        let topology = own.committed_topology(business)?;
        let actor = topology
            .nodes
            .get(peer.node_id())
            .ok_or_else(|| invalid("remote NEW authenticated actor absent"))?;
        let listed = request.manifest.old_members.contains_key(peer.node_id())
            || request.manifest.new_members.contains_key(peer.node_id());
        if peer.plaintext_loopback()
            || actor.lifecycle != crate::distribution::ClusterNodeLifecycle::Active
            || actor.incarnation != peer.member_incarnation()
            || peer
                .certificate_sha256()
                .is_none_or(|pin| !actor.accepts_tls_certificate_sha256(pin))
            || (matches!(&request.operation, NativeRemoteNewOperationV1::Observe) && !listed)
        {
            return Err(invalid(
                "remote NEW actual peer/credential/readonly participant differs",
            ));
        }
        if role.is_none() && !matches!(&request.operation, NativeRemoteNewOperationV1::Observe) {
            // No witnessed Role covers this acquisition (a decision read, or
            // the first half of a preparation). Before any participant state
            // is looked up, retained or read, the recipient's OWN committed
            // observation must admit the peer.
            let range = topology
                .range_by_id(request.manifest.capture_range.id)
                .ok_or_else(|| invalid("remote NEW current range absent"))?;
            if !unwitnessed_peer_admitted(
                listed,
                range.replicas.iter().any(|r| &r.node_id == peer.node_id()),
                &range.leader,
                own.metadata_leader_id(),
                peer.node_id(),
            ) {
                return Err(invalid(
                    "remote NEW peer is neither a member of this handoff nor its permitted initiator",
                ));
            }
        }
        if let Some(role) = &role {
            role.require_own_host_metadata(&own, business)?;
            let plan = crate::distribution_range_handoff::NativeJointHandoffPlan::from_actual_role_and_own(role, &own, business)?;
            request.require_plan(&plan)?;
        }
        let database = service
            .try_lock()
            .ok_or_else(|| invalid("remote NEW actual service busy before MStore"))?
            .listener_open_guard();
        let member = {
            let store = loop {
                parent.check()?;
                own.require_live_request(business)?;
                if let Some(store) = metadata.try_lock_for(Duration::from_millis(5)) {
                    break store;
                }
            };
            store.require_listener_database(&database)?;
            store.observe_owned_native_metadata_member(&own, business, parent)?
        };
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            service, &own, business, parent,
        )?
        .ok_or_else(|| invalid("remote NEW requires real current native MStore point"))?;
        let result = Self {
            role,
            own,
            business,
            point,
            member,
            metadata,
            database,
        };
        result.require_current()?;
        Ok(result)
    }
    pub(super) fn metadata_store_id(&self) -> Uuid {
        self.member.store_id()
    }
    pub(super) fn require_current(&self) -> Result<()> {
        self.own.require_live_request(self.business)?;
        if let Some(role) = &self.role {
            role.require_own_host_metadata(&self.own, self.business)?;
        }
        {
            let store = loop {
                self.own.require_live_request(self.business)?;
                if let Some(store) = self.metadata.try_lock_for(Duration::from_millis(5)) {
                    break store;
                }
            };
            store.require_listener_database(&self.database)?;
            self.member.require_current(&store)?;
        }
        self.point.require_external_current()?;
        self.own.require_live_request(self.business)
    }
}

/// Who may send an operation that no witnessed Role covers: a committed
/// replica of the range that the manifest also lists as an OLD or NEW member,
/// or the node the committed-handoff initiator rule admits for mutations (the
/// range leader or the metadata leader). Every input except `listed` comes
/// from the recipient's own committed metadata observation.
fn unwitnessed_peer_admitted(
    listed: bool,
    committed_replica: bool,
    range_leader: &ClusterNodeId,
    metadata_leader: &ClusterNodeId,
    peer: &ClusterNodeId,
) -> bool {
    (listed && committed_replica) || range_leader == peer || metadata_leader == peer
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_remote_decision_certificate_unwitnessed_peer_rule_admits_members_and_initiators_only()
    {
        let id = |name: &str| ClusterNodeId::new(name.to_string()).unwrap();
        let (range_leader, metadata_leader, outsider) = (id("n1"), id("n0"), id("n9"));
        let admitted = |listed, replica, peer: &ClusterNodeId| {
            unwitnessed_peer_admitted(listed, replica, &range_leader, &metadata_leader, peer)
        };
        // An Active cluster node outside the plan that may not initiate it.
        assert!(!admitted(false, false, &outsider));
        // Request DATA alone never admits: a manifest listing without the
        // recipient's own committed replica, or the reverse.
        assert!(!admitted(true, false, &outsider));
        assert!(!admitted(false, true, &outsider));
        assert!(admitted(true, true, &id("n2")));
        // The initiator rule: range leader or metadata leader, listed or not.
        assert!(admitted(false, false, &metadata_leader));
        assert!(admitted(false, false, &range_leader));
    }
}
