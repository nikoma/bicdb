//! Opaque actual old-member closure. It observes a permanent fence; it cannot
//! be deserialized, retagged, or used as a new-member/materialization vote.
use super::*;
use crate::distribution_data::{ActualServiceNativeJointOldEvidence, ClusterDataNodeService};
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, SourceCaptureReadPin};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use parking_lot::Mutex;
use std::sync::Arc;

pub(crate) struct NativeJointOldObservation {
    plan: Arc<NativeJointHandoffPlan>,
    role: Arc<RecoveryRoleAdmission>,
    native: Arc<crate::db::NativeJointOldCloseReceipt>,
    promise: Arc<crate::distribution_range_consensus::NativeJointOldPromiseReceipt>,
    frozen: Arc<FrozenRangeMutationPermit>,
    pin: Mutex<Option<Arc<SourceCaptureReadPin>>>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    #[cfg(feature = "tls-replication")]
    predecessor: Option<Arc<crate::db::NativeWriterHistoricalPredecessor>>,
}
impl NativeJointOldObservation {
    #[cfg(feature = "tls-replication")]
    pub(super) fn actual_native_birth(&self) -> Result<crate::distribution::NativeStoreBirthV1> {
        crate::distribution::NativeStoreBirthV1::from_actual_native(self.native.native_admission())
    }
    #[cfg(feature = "tls-replication")]
    pub(super) fn take_remote_new_decision_pin(
        &self,
        permit: &super::remote_new::NativeRemoteNewPinHandoffPermit<'_>,
    ) -> Result<Option<Arc<SourceCaptureReadPin>>> {
        permit.require_old(self)?;
        self.require_current()?;
        let mut pin = self
            .pin
            .try_lock()
            .ok_or_else(|| invalid("remote NEW OLD pin handoff busy"))?;
        if pin.as_ref().is_some_and(|pin| Arc::strong_count(pin) != 1) {
            return Err(invalid(
                "remote NEW OLD pin still borrowed by actual source export; retain all custody",
            ));
        }
        if pin.is_none() && !permit.old_already_detached() {
            return Err(invalid(
                "remote NEW OLD pin absent outside original retained handoff",
            ));
        }
        let dropped = {
            let _version = permit.publication_guard()?;
            permit.require_memory_current()?;
            let dropped = pin.take();
            permit.record_old_detached();
            dropped
        };
        // Return the actual pin AFTER the short version guard; its Drop may
        // acquire the range gate, and must never run under that guard.
        Ok(dropped)
    }
    pub(super) fn actual_service(&self) -> &Arc<Mutex<ClusterDataNodeService>> {
        &self.service
    }
    pub(crate) fn plan(&self) -> &Arc<NativeJointHandoffPlan> {
        &self.plan
    }
    /// Historical projection of this actual retained permanent closure only.
    pub(crate) fn actual_materialized_facts(
        &self,
    ) -> Result<super::materialized::NativeJointMaterializedFacts> {
        self.require_current()?;
        Ok(self.materialized_facts())
    }
    /// Exact native CURRENT/history bytes from this original retained owner.
    /// The returned serializable DATA cannot recreate the owner or its pin.
    pub(crate) fn actual_close_record_data(
        &self,
    ) -> Result<crate::db::NativeJointOldCloseRecordData> {
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| invalid("old exact-record pin busy"))?;
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("old exact-record service busy"))?;
        service.native_joint_old_close_record_data(
            &self.plan,
            &self.native,
            &self.promise,
            &self.frozen,
            pin.as_ref().map(|p| &**p),
        )
    }
    pub(super) fn require_remote_ancestor(
        &self,
        index: u64,
        h0: &str,
        h: &str,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| invalid("remote donor source pin busy"))?;
        let pin = pin
            .as_ref()
            .ok_or_else(|| invalid("remote donor source pin ended"))?;
        self.service
            .try_lock()
            .ok_or_else(|| invalid("remote donor source service busy"))?
            .require_native_joint_old_ancestor_with_parent(
                &self.plan,
                &self.native,
                &self.promise,
                &self.frozen,
                pin,
                index,
                h0,
                h,
                None,
                parent,
            )
    }
    pub(super) fn remote_command_at(
        &self,
        index: u64,
        parent: &crate::CancellationToken,
    ) -> Result<Option<String>> {
        parent.check()?;
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| invalid("remote donor source pin busy"))?;
        let pin = pin
            .as_ref()
            .ok_or_else(|| invalid("remote donor source pin ended"))?;
        let (_, command) = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("remote donor source service busy"))?
            .native_joint_old_history_point_with_parent(
                &self.plan,
                &self.native,
                &self.promise,
                &self.frozen,
                pin,
                index,
                parent,
            )?;
        Ok(command)
    }
    pub(super) fn retained_serving_lease(
        &self,
    ) -> Result<(
        Arc<Mutex<crate::distribution_data::ClusterDataNodeService>>,
        crate::distribution_data::NativeJointServingLease,
    )> {
        self.require_current()?;
        Ok((
            self.service.clone(),
            crate::distribution_data::NativeJointServingLease::Survivor(self.frozen.clone()),
        ))
    }
    pub(crate) fn from_actual_service(
        mut evidence: ActualServiceNativeJointOldEvidence,
        service: Arc<Mutex<ClusterDataNodeService>>,
    ) -> Result<Self> {
        {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("old closure service busy during binding"))?;
            evidence.require_current(&actual)?;
        }
        #[cfg(feature = "tls-replication")]
        let predecessor = evidence.take_historical_predecessor();
        let (plan, role, native, promise, frozen, pin) = evidence.into_parts();
        let result = Self {
            plan,
            role,
            native,
            promise,
            frozen,
            pin: Mutex::new(Some(pin)),
            service,
            #[cfg(feature = "tls-replication")]
            predecessor,
        };
        result.require_current()?;
        Ok(result)
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| invalid("old closure pin transition busy"))?;
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("old closure service busy during revalidation"))?;
        service.require_native_joint_old_observation(
            &self.plan,
            &self.native,
            &self.promise,
            &self.frozen,
            pin.as_ref().map(|pin| &**pin),
        )
    }
    pub(super) fn original_issuance_deadline(&self) -> std::time::Instant {
        self.role.deadline().max(self.plan.original_deadline())
    }
    pub(crate) fn node_id(&self) -> &ClusterNodeId {
        self.native.native_admission().local_node_id()
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.native.range_index()
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        self.native.logical_genesis_sha256()
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        self.native.logical_history_sha256()
    }
    pub(crate) fn receipt_sha256(&self) -> &str {
        self.native.checksum_sha256()
    }
}
impl NativeJointOldObservation {
    pub(super) fn require_issuance_current(&self) -> Result<()> {
        self.plan.require_live()?;
        self.role.require_live_request()?;
        self.require_current()
    }
    pub(super) fn scope_sha256(&self) -> Result<String> {
        self.plan.checksum_sha256()
    }
    pub(super) fn require_ancestor(&self, index: u64, h0: &str, h: &str) -> Result<()> {
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| invalid("old source pin busy"))?;
        let pin = pin
            .as_ref()
            .ok_or_else(|| invalid("old source capture phase ended"))?;
        self.service
            .try_lock()
            .ok_or_else(|| invalid("old selected source busy"))?
            .require_native_joint_old_ancestor(
                &self.plan,
                &self.native,
                &self.promise,
                &self.frozen,
                pin,
                index,
                h0,
                h,
                None,
            )
    }
    pub(super) fn command_at(&self, index: u64) -> Result<Option<String>> {
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| invalid("old source pin busy"))?;
        let pin = pin
            .as_ref()
            .ok_or_else(|| invalid("old source capture phase ended"))?;
        let (_, command) = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("old selected source busy"))?
            .native_joint_old_history_point(
                &self.plan,
                &self.native,
                &self.promise,
                &self.frozen,
                pin,
                index,
            )?;
        Ok(command)
    }
    pub(super) fn prepare_capture(
        &self,
        cut: CertifiedRangeImageCut,
    ) -> Result<crate::distribution_data::ActualServiceNativeJointCaptureInputs> {
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| invalid("old source pin busy"))?;
        let pin = pin
            .as_ref()
            .ok_or_else(|| invalid("old source capture phase ended"))?;
        self.service
            .try_lock()
            .ok_or_else(|| invalid("old selected source busy before capture"))?
            .prepare_native_joint_capture_inputs(
                &self.plan,
                &self.native,
                &self.promise,
                self.frozen.clone(),
                pin.clone(),
                cut,
            )
    }
    pub(super) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        (
            self.role.metadata_term(),
            self.role.metadata_read_index(),
            self.role.context_id(),
        )
    }
}

impl NativeJointOldObservation {
    pub(super) fn materialized_facts(&self) -> super::materialized::NativeJointMaterializedFacts {
        let (term, read_index, context) = self.metadata_anchor();
        super::materialized::NativeJointMaterializedFacts {
            scope_sha256: self.native.scope_sha256().into(),
            node_id: self.node_id().clone(),
            birth_id: self.native.native_admission().birth_id(),
            birth_sha256: self.native.native_admission().birth_sha256().into(),
            native_receipt_sha256: self.native.checksum_sha256().into(),
            range_index: self.range_index(),
            logical_genesis_sha256: self.logical_genesis_sha256().into(),
            logical_history_sha256: self.logical_history_sha256().into(),
            actual_local_visibility_sequence: self.native.local_visibility_sequence(),
            source_snapshot_db_sequence: None,
            image_root_sha256: None,
            metadata_term: term,
            metadata_read_index: read_index,
            metadata_context_id: context,
        }
    }
}

impl NativeJointOldObservation {
    pub(super) fn require_new_member_role(&self, role: &RecoveryRoleAdmission) -> Result<()> {
        self.plan.require_role(role)?;
        let (term, index, _) = self.metadata_anchor();
        if role.local_node_id() != self.node_id()
            || role.local_member_incarnation()
                != self.native.native_admission().local_member_incarnation()
            || role.metadata_term() < term
            || role.metadata_read_index() < index
            || !self.plan.new_members().contains_key(self.node_id())
        {
            return Err(invalid(
                "new survivor role differs from actual closed member/current scope",
            ));
        }
        self.require_current()
    }
}

impl NativeJointOldObservation {
    pub(super) fn release_capture_pin_for_decision(&self) -> Result<()> {
        self.require_current()?;
        self.pin
            .try_lock()
            .ok_or_else(|| invalid("old source capture pin busy"))?
            .take();
        self.require_current()
    }
}

impl NativeJointOldObservation {
    pub(super) fn publish_decision(
        &self,
        admission: &super::decision::NativeJointDecisionPublicationAdmission,
        parent: &crate::CancellationToken,
    ) -> Result<crate::db::NativeJointDecisionReceipt> {
        if self
            .pin
            .try_lock()
            .ok_or_else(|| invalid("old capture pin busy"))?
            .is_some()
        {
            return Err(invalid(
                "old capture phase must close before decision WAL publication",
            ));
        }
        let mut service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("old decision publisher busy"))?;
        service.publish_native_joint_decision_old(
            admission,
            &self.native,
            &self.promise,
            &self.frozen,
            parent,
        )
    }
}

impl NativeJointOldObservation {
    pub(super) fn require_decision_receipt(
        &self,
        receipt: &crate::db::NativeJointDecisionReceipt,
    ) -> Result<()> {
        self.service
            .try_lock()
            .ok_or_else(|| invalid("old decision receipt service busy"))?
            .require_native_joint_decision_receipt(receipt)
    }
}

#[cfg(feature = "tls-replication")]
impl NativeJointOldObservation {
    pub(super) fn require_remote_decision_predecessor(
        &self,
        db: &crate::BicDb,
        writer: &crate::db::NativeWriterHostState,
    ) -> Result<()> {
        self.predecessor
            .as_ref()
            .ok_or_else(|| {
                invalid("remote OLD decision has no original pre-close predecessor owner")
            })?
            .require_historical(db, writer, &self.plan)
    }
    #[cfg(test)]
    pub(crate) fn test_original_frozen_custody(&self) -> serde_json::Value {
        serde_json::json!({"allocation_diagnostic_only":format!("{:p}",Arc::as_ptr(&self.frozen)),
            "owner":self.frozen.owner()})
    }
    #[cfg(test)]
    pub(crate) fn test_remote_decision_predecessor(&self) -> Result<serde_json::Value> {
        Ok(self
            .predecessor
            .as_ref()
            .ok_or_else(|| invalid("remote OLD test predecessor absent"))?
            .test_evidence())
    }
    pub(super) fn require_remote_decision_parts(
        &self,
        native: &crate::db::NativeJointOldCloseReceipt,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        if !std::ptr::eq(self.native.as_ref(), native)
            || !std::ptr::eq(self.promise.as_ref(), promise)
            || !std::ptr::eq(self.frozen.as_ref(), frozen)
        {
            return Err(invalid(
                "remote OLD decision changed original actual closure/promise/Frozen",
            ));
        }
        Ok(())
    }
    pub(super) fn publish_remote_decision(
        &self,
        writer: &crate::db::NativeWriterHostState,
        admission: &super::remote_new::NativeRemoteDecisionAdmission<'_>,
        attempt: &mut crate::db::NativeRemoteJointDecisionAttempt,
        parent: &crate::CancellationToken,
    ) -> Result<crate::db::NativeJointDecisionReceipt> {
        admission.require_old_parts(&self.native, &self.promise, &self.frozen, writer)?;
        let pin = self
            .pin
            .try_lock()
            .ok_or_else(|| invalid("remote OLD decision original pin busy"))?;
        if pin.is_some() {
            return Err(invalid("remote OLD decision requires original pin handoff"));
        }
        let mut service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("remote OLD decision original service busy"))?;
        service.publish_remote_old_decision(
            writer,
            &self.native,
            &self.promise,
            &self.frozen,
            admission,
            attempt,
            parent,
        )
    }
}
