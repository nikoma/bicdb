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
        if peer.plaintext_loopback()
            || actor.lifecycle != crate::distribution::ClusterNodeLifecycle::Active
            || actor.incarnation != peer.member_incarnation()
            || peer
                .certificate_sha256()
                .is_none_or(|pin| !actor.accepts_tls_certificate_sha256(pin))
            || (matches!(&request.operation, NativeRemoteNewOperationV1::Observe)
                && !request.manifest.old_members.contains_key(peer.node_id())
                && !request.manifest.new_members.contains_key(peer.node_id()))
        {
            return Err(invalid(
                "remote NEW actual peer/credential/readonly participant differs",
            ));
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
