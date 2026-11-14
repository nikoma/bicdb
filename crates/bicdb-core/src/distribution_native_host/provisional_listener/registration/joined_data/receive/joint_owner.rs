//! Same-attempt Joined A custody for later joint phases. This transfer grants
//! no NEW vote, decision, Open, Serving, APP or Strong permission. The host
//! retains the actual owner before any post-transfer check can fail.
use super::*;
use crate::distribution_data::ActualNativeJointMetadataPoint;
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_recovery::JoinedRemoteImageInstallFreeze;

pub(crate) struct NativeJoinedRemoteJointOwner {
    target: Arc<NativeJoinedRemoteMaterializedOwner>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    frozen: Mutex<Option<Arc<JoinedRemoteImageInstallFreeze>>>,
}
impl NativeJoinedRemoteJointOwner {
    pub(crate) fn target(&self) -> &Arc<NativeJoinedRemoteMaterializedOwner> {
        &self.target
    }
    pub(crate) fn service(&self) -> &Arc<Mutex<ClusterDataNodeService>> {
        &self.service
    }
    pub(crate) fn retained_freeze(&self) -> Result<Arc<JoinedRemoteImageInstallFreeze>> {
        let frozen = self
            .frozen
            .try_lock()
            .ok_or_else(|| invalid("Joined joint original A custody busy"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| invalid("Joined joint original A moved to release continuation"))?;
        // Runtime checks run AFTER dropping the owner mutex, outside engine
        // guards. The Arc is the same lease view, never a new permit.
        self.target.require_original_lease(&frozen)?;
        Ok(frozen)
    }
    fn require_same_target(&self, target: &Arc<NativeJoinedRemoteMaterializedOwner>) -> Result<()> {
        if !Arc::ptr_eq(&self.target, target) {
            return Err(invalid(
                "Joined joint retry changed original materialized owner",
            ));
        }
        self.target.require_lifetime()
    }
}

impl NativeMetadataControlHost {
    /// Use independently fresh joint Role/OWN/current-MStore authority; the
    /// snapshot caller's finite request is deliberately not an input here.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn take_joined_remote_joint_target(
        &mut self,
        target: &Arc<NativeJoinedRemoteMaterializedOwner>,
        plan: &Arc<NativeJointHandoffPlan>,
        role: &HandoffRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<Arc<NativeJoinedRemoteJointOwner>> {
        parent.check()?;
        target.require_lifetime()?;
        plan.require_role(role.role())?;
        plan.require_actual_native(target.native())?;
        role.role().require_own_host_metadata(own, business)?;
        target.readonly_plan().require_manifest(target.manifest())?;
        target
            .readonly_plan()
            .require_fresh_target_own(own, business)?;
        if plan.checksum_sha256()? != target.readonly_plan().checksum_sha256()? {
            return Err(invalid("Joined joint phase changed original complete plan"));
        }
        let service = target.service()?;
        let metadata: Arc<ActualNativeJointMetadataPoint> =
            ClusterDataNodeService::capture_actual_joint_metadata_point(
                &service, own, business, parent,
            )?
            .ok_or_else(|| invalid("Joined joint requires actual current-MStore point"))?;
        metadata.require_external_current()?;

        let host = self.joined_receive_host_mut()?;
        target.preflight().selected().origin().require_host(host)?;
        let attempt = host
            .remote_attempts
            .get_mut(&target.base().capture_range().id)
            .ok_or_else(|| invalid("Joined joint original host attempt absent"))?;
        if !Arc::ptr_eq(&attempt.preflight, target.preflight())
            || attempt
                .materialized
                .as_ref()
                .is_none_or(|old| !Arc::ptr_eq(old, target))
        {
            return Err(invalid(
                "Joined joint requires SAME host-retained physical owner",
            ));
        }
        let existing = attempt.joint.clone();
        let retained = match &existing {
            Some(owner) => {
                owner.require_same_target(target)?;
                Some(owner.retained_freeze()?)
            }
            None => None,
        };
        let original_a = match &retained {
            Some(frozen) => frozen.as_ref(),
            None => attempt
                .lease
                .as_ref()
                .ok_or_else(|| invalid("Joined joint original A absent without continuation"))?,
        };
        target.require_original_lease(original_a)?;
        let identity = original_a.try_as_frozen()?.observe_identity()?;
        {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("Joined joint original service busy"))?;
            let database = actual.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Joined joint actual DB busy"))?;
            actual.with_writer_data_domain_store(&db, |store| {
                metadata.require_native_store_point(&db, store)?;
                target.require_joint_point(&db, store, original_a.try_as_frozen()?)?;
                plan.require_role(role.role())?;
                role.role().require_own_host_metadata(own, business)?;
                parent.check()
            })?;
        }
        metadata.require_external_current()?;
        // This is ONLY a custody move of the same runtime-retained A. No
        // native/catalogue operation or usable phase grant is produced. Store
        // the owner immediately, before the first fallible postcheck. If the
        // final fresh gate refuses, explicit retry uses THIS same owner/A.
        let owner = match existing {
            Some(owner) => owner,
            None => {
                let frozen = Arc::new(attempt.lease.take().expect("checked original Joined A"));
                let owner = Arc::new(NativeJoinedRemoteJointOwner {
                    target: target.clone(),
                    service: service.clone(),
                    frozen: Mutex::new(Some(frozen)),
                });
                attempt.joint = Some(owner.clone());
                owner
            }
        };
        let frozen = owner.retained_freeze()?;
        {
            // All allocation/physical checks precede these nonblocking short
            // guards. They protect memory-only liveness and exact identity.
            let gate = frozen.try_as_frozen()?.try_current_publication()?;
            let version = metadata.try_publication_guard()?;
            parent.check()?;
            own.require_live_request(business)?;
            owner.target.require_joint_lifetime_memory()?;
            identity.require_in_current_publication(&gate)?;
            version.require_current_memory()?;
        }
        metadata.require_external_current()?;
        parent.check()?;
        Ok(owner)
    }
}
