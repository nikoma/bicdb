//! One genuinely fresh phase witness, independent OWN and attached MStore.
//! This module supplies request evidence only; actual private native/physical
//! owners must still validate under the recipient's serialized host consumer.
use super::*;
use crate::distribution_consensus::{MetadataConsensusStore, MetadataReadParticipant};
use crate::distribution_data::ActualNativeJointMetadataPoint;
use crate::distribution_native_host::NativeRemoteServingRequestV1;
use crate::distribution_range_recovery::RecoveryRoleAdmission;

pub(super) struct RemoteServingRecipientMetadata {
    pub(super) role: RecoveryRoleAdmission,
    pub(super) own: HostMetadataReadObservation,
    pub(super) business: Uuid,
    pub(super) point: Arc<ActualNativeJointMetadataPoint>,
}
impl RemoteServingRecipientMetadata {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn acquire(
        provider: &Arc<crate::distribution_metadata_read_provider::MetadataReadProvider>,
        transport: &Arc<TcpClusterRelocationTransport>,
        metadata: &Arc<Mutex<MetadataConsensusStore>>,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        peer: &MetadataReadParticipant,
        request: &NativeRemoteServingRequestV1,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        request.require_shape()?;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let selected = &request.manifest.activation_range;
        let purpose = request.purpose();
        let local = transport.local_metadata_read_participant();
        let witness = provider.witness_recovery_role(
            peer,
            business,
            selected.id,
            selected.epoch,
            request.manifest.nonce,
            challenge,
            purpose.clone(),
            parent,
        )?;
        let role = RecoveryRoleAdmission::from_witness(
            witness,
            peer,
            local.node_id(),
            business,
            selected.id,
            selected.epoch,
            request.manifest.nonce,
            challenge,
            &purpose,
        )?;
        let leader = provider
            .current_metadata_leader_hint(parent)?
            .ok_or_else(|| invalid("serving current metadata leader unavailable"))?;
        let remaining = parent
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or(BicDbError::QueryTimedOut)?;
        let own =
            transport.acquire_host_metadata_observation(&leader, business, remaining, parent)?;
        role.require_own_host_metadata(&own, business)?;
        let topology = own.committed_topology(business)?;
        let actor = topology
            .nodes
            .get(peer.node_id())
            .ok_or_else(|| invalid("serving actual TLS actor absent"))?;
        let record = topology
            .relocations
            .get(&request.manifest.relocation_id)
            .and_then(|p| p.native_joint_handoff_v1.as_ref())
            .ok_or_else(|| invalid("serving actual committed native decision absent"))?;
        if peer.plaintext_loopback()
            || actor.lifecycle != ClusterNodeLifecycle::Active
            || actor.incarnation != peer.member_incarnation()
            || peer
                .certificate_sha256()
                .is_none_or(|p| !actor.accepts_tls_certificate_sha256(p))
            || record.stage() != crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
            || record.decision_sha256() != request.decision_sha256
            || record.session_id() != request.manifest.handoff_session
            || record.nonce() != request.manifest.nonce
            || topology
                .range_by_id(selected.id)
                .map(crate::distribution_native_read::canonical_range)
                != Some(crate::distribution_native_read::canonical_range(selected))
            || own.restore_activation(business)? != request.manifest.restore.as_ref()
        {
            return Err(invalid(
                "serving changed actual TLS/current phase/domain binding",
            ));
        }
        let open = service
            .try_lock()
            .ok_or_else(|| invalid("serving actual service busy before MStore"))?
            .listener_open_guard();
        {
            let store = loop {
                parent.check()?;
                own.require_live_request(business)?;
                if let Some(store) = metadata.try_lock_for(Duration::from_millis(5)) {
                    break store;
                }
            };
            store.require_listener_database(&open)?;
            let member = store.observe_owned_native_metadata_member(&own, business, parent)?;
            member.require_current(&store)?;
        }
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            service, &own, business, parent,
        )?
        .ok_or_else(|| invalid("serving requires actual attached current native MStore"))?;
        point.require_external_current()?;
        parent.check()?;
        Ok(Self {
            role,
            own,
            business,
            point,
        })
    }
}
