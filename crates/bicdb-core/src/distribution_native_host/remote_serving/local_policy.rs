//! Actual finite self-origin authority for the retained released owner. It
//! cannot be converted to a generic recovery Role or a remote mutation intent.
use super::*;
use crate::distribution_range_recovery::NativeLocalServingPolicyRoleAdmission;

pub(crate) struct NativeRemoteLocalPolicyAuthority {
    role: NativeLocalServingPolicyRoleAdmission,
    own: HostMetadataReadObservation,
    point: Arc<ActualNativeJointMetadataPoint>,
    business: Uuid,
    owner: Arc<crate::distribution_data::NativeRemoteReleaseOwner>,
    process: Arc<NativeRemotePreparationProcess>,
    local: ClusterNodeId,
    incarnation: u64,
    token: CancellationToken,
}
impl NativeRemoteLocalPolicyAuthority {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::distribution_native_host) fn acquire(
        provider: &Arc<crate::distribution_metadata_read_provider::MetadataReadProvider>,
        transport: &Arc<crate::distribution_transport::TcpClusterRelocationTransport>,
        service: &Arc<Mutex<crate::distribution_data::ClusterDataNodeService>>,
        manifest: &NativeRemoteImageManifestV1,
        owner: Arc<crate::distribution_data::NativeRemoteReleaseOwner>,
        process: Arc<NativeRemotePreparationProcess>,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        process.require_memory()?;
        owner.require_process(&process)?;
        owner.require_original_manifest_identity(manifest)?;
        let actual_self = transport.local_metadata_read_participant();
        let local = actual_self.node_id().clone();
        let incarnation = actual_self.member_incarnation();
        let member = manifest
            .new_members
            .get(&local)
            .ok_or_else(|| invalid("local policy actual recipient not final NEW"))?;
        if incarnation != member.incarnation
            || owner.native().local_node_id() != &local
            || !Arc::ptr_eq(service, owner.service())
        {
            return Err(invalid(
                "local policy changed actual bound self/service/incarnation",
            ));
        }
        let token = process.bound_request(parent);
        let purpose = MetadataRecoveryRolePurpose::NativeJointLocalServingPolicy {
            relocation_id: manifest.relocation_id,
            session_id: manifest.handoff_session,
            nonce: manifest.nonce,
            decision_sha256: owner.native().data().checksum_sha256().into(),
        };
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let witness = provider.witness_recovery_role(
            &actual_self,
            business,
            manifest.activation_range.id,
            manifest.activation_range.epoch,
            manifest.nonce,
            challenge,
            purpose.clone(),
            &token,
        )?;
        let role = NativeLocalServingPolicyRoleAdmission::from_actual_witness(
            witness,
            &actual_self,
            business,
            &manifest.activation_range,
            manifest.nonce,
            challenge,
            &purpose,
        )?;
        let leader = provider
            .current_metadata_leader_hint(&token)?
            .ok_or_else(|| invalid("local policy metadata leader absent"))?;
        let remaining = token
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or(BicDbError::QueryTimedOut)?;
        let own =
            transport.acquire_host_metadata_observation(&leader, business, remaining, &token)?;
        role.require_own(&own, business, &local, incarnation)?;
        owner.native().data().require_metadata_domain(
            own.committed_topology(business)?,
            own.restore_activation(business)?,
        )?;
        let point =
            crate::distribution_data::ClusterDataNodeService::capture_actual_joint_metadata_point(
                service, &own, business, &token,
            )?
            .ok_or_else(|| {
                invalid("local policy requires actual attached current native MStore")
            })?;
        point.require_external_current()?;
        let token = token
            .child_linked_to(&own.original_request_cancellation())
            .child_with_deadline(role.deadline().min(own.deadline()));
        let result = Self {
            role,
            own,
            point,
            business,
            owner,
            process,
            local,
            incarnation,
            token,
        };
        result.require_memory_current()?;
        Ok(result)
    }
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.token.check()?;
        self.role.require_live_request()?;
        self.own.require_live_request(self.business)?;
        self.point.require_current_memory()?;
        self.owner.require_memory_lifetime()?;
        self.owner.require_process(&self.process)
    }
    pub(crate) fn require_current(
        &self,
        post: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.require_memory_current()?;
        self.role
            .require_own(&self.own, self.business, &self.local, self.incarnation)?;
        self.point.require_request(&self.own, self.business)?;
        post.require_current_metadata_only()?;
        if post.data() != self.owner.native().data()
            || post.native_receipt().local_node_id() != &self.local
        {
            return Err(invalid(
                "local policy changed same original native/recipient",
            ));
        }
        self.require_memory_current()
    }
    pub(crate) fn require_scope(
        &self,
        post: &crate::distribution_range_handoff::CommittedNativeJointDecisionObservation,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.require_current(post)?;
        self.point
            .require_native_store_point(scope.database(), store)?;
        self.process
            .require_released_native(scope.database(), post, &self.owner)?;
        self.require_memory_current()
    }
    pub(crate) fn require_successor(
        &self,
        actual: &Arc<crate::distribution_data::NativeRemoteReleaseOwner>,
    ) -> Result<()> {
        self.require_memory_current()?;
        if !Arc::ptr_eq(actual, &self.owner) {
            return Err(invalid(
                "local policy changed retained actual released successor",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_service(
        &self,
        actual: &crate::distribution_data::ClusterDataNodeService,
    ) -> Result<()> {
        self.require_memory_current()?;
        self.process.require_service(actual)
    }
    pub(crate) fn bound_request(&self) -> CancellationToken {
        self.token.child()
    }
}
