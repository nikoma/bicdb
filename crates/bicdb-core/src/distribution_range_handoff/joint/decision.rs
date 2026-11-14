//! One complete decision body, produced from actual OLD and NEW physical
//! quorums. Decoding its audit data never reconstructs either quorum.
use super::materialized::NativeJointMaterializedFacts;
use super::*;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(feature = "tls-replication")]
mod registered_catalogue_retry;
#[cfg(feature = "tls-replication")]
pub(crate) use registered_catalogue_retry::NativeRegisteredDecisionCatalogueRetry;

const MAX_DECISION_BYTES: usize = 2 * 1024 * 1024;
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJointDecisionData {
    version: u32,
    scope: NativeJointScopeData,
    old_closed: BTreeMap<ClusterNodeId, NativeJointMaterializedFacts>,
    observed_irrevocable: Vec<crate::distribution_data::NativeJointIrrevocableFacts>,
    new_materialized: BTreeMap<ClusterNodeId, NativeJointMaterializedFacts>,
    selected_source: ClusterNodeId,
    range_index: u64,
    h0: String,
    h: String,
    checksum_sha256: String,
}
impl NativeJointDecisionData {
    pub(crate) fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if self.version != 1
            || !digest(&self.h0)
            || !digest(&self.h)
            || self.old_closed.len() < self.scope.old_members.len() / 2 + 1
            || self.new_materialized.len() < self.scope.new_members.len() / 2 + 1
            || !self.old_closed.contains_key(&self.selected_source)
            || !self
                .new_materialized
                .contains_key(&self.scope.activation_range.leader)
            || self.observed_irrevocable.len() > 128
        {
            return Err(invalid("malformed native joint decision provenance"));
        }
        let target = self
            .new_materialized
            .get(&self.scope.target)
            .ok_or_else(|| invalid("joint decision has no actual completed target image"))?;
        if target.source_snapshot_db_sequence.is_none()
            || target
                .image_root_sha256
                .as_deref()
                .is_none_or(|root| !digest(root))
        {
            return Err(invalid(
                "joint decision target lacks actual image capture B/content root",
            ));
        }
        let scope_sha = self.scope.checksum()?;
        for (facts, members, new) in [
            (&self.old_closed, &self.scope.old_members, false),
            (&self.new_materialized, &self.scope.new_members, true),
        ] {
            for (node, fact) in facts {
                let birth = self
                    .scope
                    .enrolled_births
                    .get(node)
                    .ok_or_else(|| invalid("decision member birth absent"))?;
                if fact.node_id != *node
                    || !members.contains_key(node)
                    || fact.scope_sha256 != scope_sha
                    || fact.birth_id != birth.birth_id()
                    || fact.birth_sha256 != birth.birth_sha256()
                    || fact.logical_genesis_sha256 != self.h0
                    || fact.range_index > self.range_index
                    || (new
                        && (fact.range_index != self.range_index
                            || fact.logical_history_sha256 != self.h))
                    || !digest(&fact.logical_history_sha256)
                    || !digest(&fact.native_receipt_sha256)
                    || fact.metadata_context_id.is_nil()
                    || fact.metadata_term == 0
                    || fact.metadata_read_index == 0
                {
                    return Err(invalid(
                        "joint decision changes an actually counted native member/point",
                    ));
                }
            }
        }
        if self.old_closed[&self.selected_source].range_index != self.range_index
            || self.old_closed[&self.selected_source].logical_history_sha256 != self.h
        {
            return Err(invalid("joint decision selected source point mismatch"));
        }
        for fact in &self.observed_irrevocable {
            if fact.scope_sha256 != scope_sha
                || !self.scope.old_members.contains_key(&fact.node_id)
                || fact.actual_resolved > self.range_index
                || fact.actual_compacted > self.range_index
                || fact
                    .commands
                    .iter()
                    .any(|(i, hash)| *i > self.range_index || !digest(hash))
                || fact
                    .native_point
                    .as_ref()
                    .is_some_and(|(i, h0, h)| *i > self.range_index || h0 != &self.h0 || !digest(h))
            {
                return Err(invalid(
                    "joint decision omits/conflicts with an observed irreversible bound",
                ));
            }
        }
        let mut unsigned = self.clone();
        unsigned.checksum_sha256.clear();
        let checksum = hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(&unsigned, MAX_DECISION_BYTES)?,
        ));
        if checksum != self.checksum_sha256 {
            return Err(invalid("native joint decision body checksum mismatch"));
        }
        Ok(())
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_remote_historical_data(
        plan: &NativeJointHandoffPlan,
        manifest: &crate::distribution_range_image::NativeRemoteImageManifestV1,
        source: &super::remote_new::NativeRemoteDecisionSourceData,
        new_materialized: BTreeMap<ClusterNodeId, NativeJointMaterializedFacts>,
    ) -> Result<Self> {
        source.require_manifest(manifest)?;
        source.binding.require_plan(plan)?;
        let mut data = Self {
            version: 1,
            scope: plan.data().clone(),
            old_closed: manifest.actual_old_closures.clone(),
            observed_irrevocable: source.constraints.clone(),
            new_materialized,
            selected_source: manifest.source_id.clone(),
            range_index: manifest.range_index,
            h0: manifest.h0.clone(),
            h: manifest.h.clone(),
            checksum_sha256: String::new(),
        };
        data.checksum_sha256 = hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(&data, MAX_DECISION_BYTES)?,
        ));
        data.validate()?;
        Ok(data) // DATA only; no LocalQQ or local publication admission.
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn require_remote_source_data(
        &self,
        source: &super::remote_new::NativeRemoteDecisionSourceData,
    ) -> Result<()> {
        self.validate()?;
        source.binding.require_shape()?;
        if source.binding.scope_sha256 != self.scope.checksum()?
            || source.binding.source_id != self.selected_source
            || source.binding.target_id != self.scope.target
            || source.constraints != self.observed_irrevocable
            || source.constraints.len() != self.scope.old_members.len()
            || source.original_close_records.len() != self.old_closed.len()
            || source.checksum()? != source.checksum_sha256
        {
            return Err(invalid(
                "protected remote decision source obligations/plan changed",
            ));
        }
        for (id, fact) in &self.old_closed {
            source
                .original_close_records
                .get(id)
                .ok_or_else(|| invalid("protected remote original OLD record missing"))?
                .require_materialized_facts(fact)?;
        }
        Ok(())
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn remote_new_facts(
        &self,
    ) -> &BTreeMap<ClusterNodeId, NativeJointMaterializedFacts> {
        &self.new_materialized
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.checksum_sha256
    }
    pub(crate) fn plan_sha256(&self) -> Result<String> {
        self.scope.checksum()
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        &self.h0
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        &self.h
    }
    /// Historical protected scope/materialization DATA; no live role or QQ.
    pub(crate) fn scope_sha256(&self) -> Result<String> {
        self.scope.checksum()
    }
    pub(crate) fn old_materialized_fact(
        &self,
        node: &ClusterNodeId,
    ) -> Option<&NativeJointMaterializedFacts> {
        self.old_closed.get(node)
    }
    /// Immutable historical DATA only; this does not reconstruct an OLD quorum.
    pub(crate) fn old_materialized_facts(
        &self,
    ) -> &BTreeMap<ClusterNodeId, NativeJointMaterializedFacts> {
        &self.old_closed
    }
    pub(crate) fn promise(&self) -> &RangeHandoffPromise {
        &self.scope.promise
    }
    pub(crate) fn old_range(&self) -> &RangeDescriptor {
        &self.scope.old_range
    }
    pub(crate) fn activation_range(&self) -> &RangeDescriptor {
        &self.scope.activation_range
    }
    pub(crate) fn old_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        &self.scope.old_members
    }
    pub(crate) fn new_members(&self) -> &BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember> {
        &self.scope.new_members
    }
    /// Correlation of an existing raw refusal with the protected OLD domain.
    /// This hash is not a metadata observation or authority constructor.
    pub(crate) fn old_recovery_range_sha256(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let range = self.old_range();
        let mut replicas = range
            .replicas
            .iter()
            .map(|r| (r.id, &r.node_id, &r.role))
            .collect::<Vec<_>>();
        replicas.sort_by_key(|r| r.0);
        let members = self
            .old_members()
            .iter()
            .map(|(id, m)| {
                (
                    id.clone(),
                    crate::distribution_range_recovery::RecoveryMemberIdentity {
                        incarnation: m.incarnation,
                        active_certificate_sha256: m.tls_certificate_sha256.clone(),
                        pending_certificate_sha256: m.pending_tls_certificate_sha256.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            "bicdb-recovery-semantic-range-v1",
            self.cluster_id(),
            self.bootstrap_id(),
            range.id,
            range.start_token,
            range.end_token,
            range.epoch,
            &range.leader,
            &range.placement,
            replicas,
            &members,
            &Some(self.schema_sha256().to_owned()),
            &self.restore_activation(),
        ))?)))
    }
    /// Historical identity constraint, not an observation or new quorum grant.
    pub(crate) fn new_member_birth(
        &self,
        node: &ClusterNodeId,
    ) -> Option<&crate::distribution::NativeStoreBirthV1> {
        self.scope
            .new_members
            .contains_key(node)
            .then(|| self.scope.enrolled_births.get(node))
            .flatten()
    }
    pub(crate) fn schema_sha256(&self) -> &str {
        &self.scope.schema_sha256
    }
    pub(crate) fn restore_activation(
        &self,
    ) -> Option<&crate::distribution_consensus::MetadataRestoreActivation> {
        self.scope.restore_activation.as_ref()
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.scope.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.scope.bootstrap_id
    }
    pub(crate) fn require_actual_native(
        &self,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<()> {
        self.validate()?;
        self.scope.require_actual_native(native)
    }
    pub(crate) fn require_actual_local_point(
        &self,
        native: &crate::db::NativeRecoveryAdmission,
        receipt: &str,
        r: u64,
        h0: &str,
        h: &str,
        b: u64,
    ) -> Result<()> {
        self.require_actual_native(native)?;
        let node = native.local_node_id();
        let actual = self
            .new_materialized
            .get(node)
            .or_else(|| self.old_closed.get(node))
            .ok_or_else(|| invalid("joint publication node was not actually materialized"))?;
        if actual.native_receipt_sha256 != receipt
            || actual.range_index != r
            || actual.logical_genesis_sha256 != h0
            || actual.logical_history_sha256 != h
            || actual.actual_local_visibility_sequence != b
        {
            return Err(invalid(
                "joint publication does not match its actual local materialization",
            ));
        }
        Ok(())
    }
}

/// Complete actual quorums remain owned. This is the ONLY publisher capability;
/// its data projection is read-only and cannot be converted back to this type.
pub(crate) struct NativeJointPreparedDecision {
    pub(super) quorums: NativeJointMaterializedQuorums,
    data: NativeJointDecisionData,
}
impl NativeJointMaterializedQuorums {
    /// Bounded failure-only DATA. Original guards have returned before this
    /// formatter runs. It performs no currentness check or protocol callback.
    #[cfg(test)]
    fn report_preparation_refusal(
        &self,
        stage: &'static str,
        failed_member: Option<&ClusterNodeId>,
        entered: std::time::Instant,
        error: &BicDbError,
    ) {
        let returned = std::time::Instant::now();
        let clocks: Vec<_> = self
            .new
            .iter()
            .take(64)
            .map(|(node, observation)| {
                let bytes = node.as_str().as_bytes();
                serde_json::json!({"member_bytes":&bytes[..bytes.len().min(128)],
                "member_original_bytes":bytes.len(),"member_truncated":bytes.len()>128,
                "failed_member":failed_member==Some(node),
                "clock":observation.test_issuance_clock(entered,returned)})
            })
            .collect();
        let report = serde_json::json!({"stage":stage,"elapsed_us":returned.duration_since(entered).as_micros(),
            "members":clocks,"member_limit":64,"omitted_members":self.new.len().saturating_sub(64),
            "actual_error_discriminant":format!("{:?}",std::mem::discriminant(error)),
            "primary_error":"preserved by original Result propagation and caller unwrap",
            "scope":"failure-only copied clocks; no CPU/IO/cause attribution or renewed authority"});
        let _ = std::io::Write::write_fmt(
            &mut std::io::stderr().lock(),
            format_args!("NATIVE_DECISION_PREPARATION_REFUSAL {report}\n"),
        );
    }
    pub(crate) fn prepare_decision(self) -> Result<NativeJointPreparedDecision> {
        #[cfg(test)]
        let entered = std::time::Instant::now();
        let current = self.require_current();
        #[cfg(test)]
        if let Err(error) = &current {
            self.report_preparation_refusal("initial_actual_current", None, entered, error);
        }
        current?;
        for (_node, obs) in &self.new {
            #[cfg(test)]
            let entered = std::time::Instant::now();
            let issued = obs.require_live_issuance();
            #[cfg(test)]
            if let Err(error) = &issued {
                self.report_preparation_refusal(
                    "member_live_issuance",
                    Some(_node),
                    entered,
                    error,
                );
            }
            issued?;
        }
        let plan = self.old.plan();
        let mut data = NativeJointDecisionData {
            version: 1,
            scope: plan.data().clone(),
            old_closed: self.old.materialized_facts(),
            observed_irrevocable: self.old.constraints().to_vec(),
            new_materialized: self.new_facts(),
            selected_source: self.old.selected_source().clone(),
            range_index: self.old.range_index(),
            h0: self.old.logical_genesis_sha256().into(),
            h: self.old.logical_history_sha256().into(),
            checksum_sha256: String::new(),
        };
        data.checksum_sha256 = hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(&data, MAX_DECISION_BYTES)?,
        ));
        data.validate()?;
        // Capture/image readers must drain before counted metadata WAL writers.
        // A still-live capture retains its Arc pin and makes publication fail
        // boundedly; no exception permits writing through that source pin.
        self.old.release_capture_pins_for_decision()?;
        for obs in self.new.values() {
            obs.release_capture_pin_for_decision()?;
        }
        self.require_current()?;
        Ok(NativeJointPreparedDecision {
            quorums: self,
            data,
        })
    }
}
impl NativeJointPreparedDecision {
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.data
    }
    pub(crate) fn authorize_local_publication(
        &self,
        role: RecoveryRoleAdmission,
    ) -> Result<NativeJointDecisionPublicationAdmission> {
        self.quorums.require_current()?;
        let plan = self.quorums.old.plan();
        plan.require_role(&role)?;
        let node = role.local_node_id();
        if !self.data.old_closed.contains_key(node)
            && !self.data.new_materialized.contains_key(node)
        {
            return Err(invalid(
                "joint publisher is not an actual materialized old/new voter",
            ));
        }
        let observed = self
            .data
            .old_closed
            .values()
            .chain(self.data.new_materialized.values());
        if observed.clone().any(|f| {
            role.context_id() == f.metadata_context_id
                || role.metadata_term() < f.metadata_term
                || role.metadata_read_index() < f.metadata_read_index
        }) {
            return Err(invalid(
                "joint publisher fresh metadata regressed behind actual materialization",
            ));
        }
        Ok(NativeJointDecisionPublicationAdmission {
            data: self.data.clone(),
            plan,
            role,
            #[cfg(feature = "tls-replication")]
            registered_own: None,
            metadata_point: None,
        })
    }
}
/// Single local publication operation under an independently fresh exact
/// pending-plan witness. This does not authorize metadata finalization itself.
pub(crate) struct NativeJointDecisionPublicationAdmission {
    data: NativeJointDecisionData,
    plan: Arc<NativeJointHandoffPlan>,
    role: RecoveryRoleAdmission,
    metadata_point: Option<Arc<crate::distribution_data::ActualNativeJointMetadataPoint>>,
    #[cfg(feature = "tls-replication")]
    registered_own: Option<(
        crate::distribution_consensus::HostMetadataReadObservation,
        Uuid,
    )>,
}
impl NativeJointDecisionPublicationAdmission {
    pub(crate) fn require_native_metadata_point(
        &self,
        db: &crate::BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<()> {
        match &self.metadata_point {
            Some(point) => point.require_native_store_point(db, store),
            None if native.metadata_initialization_version() == 0 => Ok(()),
            None => Err(invalid(
                "protected native DECISION requires actual fresh OWN/current-MStore version",
            )),
        }
    }
    pub(crate) fn native_publication_guard(
        &self,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<Option<crate::distribution_data::ActualNativeJointMetadataPublicationGuard<'_>>>
    {
        let version = match &self.metadata_point {
            Some(point) => point.try_publication_guard().map(Some),
            None if native.metadata_initialization_version() == 0 => Ok(None),
            None => Err(invalid(
                "protected native DECISION has no current metadata publication gate",
            )),
        }?;
        // Original role request is a distinct memory-only cancellation/deadline.
        self.role.require_live_request()?;
        Ok(version)
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.data
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        &self.role
    }
    pub(crate) fn plan(&self) -> &NativeJointHandoffPlan {
        &self.plan
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn registered_metadata_request(
        &self,
    ) -> Result<(
        &crate::distribution_consensus::HostMetadataReadObservation,
        Uuid,
    )> {
        self.require_registered_own()?;
        let (own, business) = self.registered_own.as_ref().unwrap();
        Ok((own, *business))
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn require_registered_own(&self) -> Result<()> {
        let (own, business) = self.registered_own.as_ref().ok_or_else(|| {
            invalid("Registered joint publication requires independently fresh actual OWN")
        })?;
        self.role.require_own_host_metadata(own, *business)?;
        self.require_live()
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.plan.require_role(&self.role)?;
        #[cfg(feature = "tls-replication")]
        if let Some((own, business)) = &self.registered_own {
            self.role.require_own_host_metadata(own, *business)?;
        }
        self.data.validate()
    }
}

/// Actual same-body native WAL acknowledgements on BOTH configurations. This
/// fact authorizes only a conditional metadata stage, never user admission.
pub(crate) struct NativeJointDurableDecision {
    prepared: NativeJointPreparedDecision,
    durable: BTreeMap<ClusterNodeId, crate::db::NativeJointDecisionReceipt>,
}
pub(crate) struct NativeJointDecisionPublicationRound {
    prepared: NativeJointPreparedDecision,
    durable: BTreeMap<ClusterNodeId, crate::db::NativeJointDecisionReceipt>,
}
impl NativeJointPreparedDecision {
    pub(crate) fn publication_round(self) -> NativeJointDecisionPublicationRound {
        NativeJointDecisionPublicationRound {
            prepared: self,
            durable: BTreeMap::new(),
        }
    }
    fn require_actual_receipt(
        &self,
        receipt: &crate::db::NativeJointDecisionReceipt,
    ) -> Result<()> {
        if receipt.decision_sha256() != self.data.checksum_sha256() {
            return Err(invalid(
                "durable native decision ACK changed the complete decision body",
            ));
        }
        if let Some(new) = self.quorums.new.get(receipt.local_node_id()) {
            new.require_decision_receipt(receipt)
        } else if let Some(old) = self.quorums.old.old_observation(receipt.local_node_id()) {
            old.require_decision_receipt(receipt)
        } else {
            Err(invalid(
                "native decision ACK has no actual materialized participant",
            ))
        }
    }
}
impl NativeJointDecisionPublicationRound {
    /// Actual local collection contents; no independent vote or authority.
    #[cfg(test)]
    pub(crate) fn test_actual_durable_member_ids(&self) -> Vec<ClusterNodeId> {
        self.durable.keys().cloned().collect()
    }
    pub(crate) fn publish_actual_member(
        &mut self,
        role: RecoveryRoleAdmission,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let admission = self.prepared.authorize_local_publication(role)?;
        self.publish_admitted_member(admission, parent)
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn publish_actual_owned_member(
        &mut self,
        role: RecoveryRoleAdmission,
        own: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        role.require_own_host_metadata(&own, business)?;
        // Re-fsync only an actual retained catalogue candidate before strict
        // normal quorum/durability authorization. Never rebuild a NEW vote.
        if let Some(NativeJointNewObservation::Registered { actual }) =
            self.prepared.quorums.new.get(role.local_node_id())
        {
            actual.reconcile_retained_decision_catalogue(
                &self.prepared,
                &role,
                &own,
                business,
                parent,
            )?;
        }
        let mut admission = self.prepared.authorize_local_publication(role)?;
        let node = admission.role().local_node_id().clone();
        let service = match self.prepared.quorums.new.get(&node) {
            Some(new) => new.actual_service_for_metadata().clone(),
            None => self
                .prepared
                .quorums
                .old
                .old_observation(&node)
                .ok_or_else(|| invalid("actual owned DECISION participant absent"))?
                .actual_service()
                .clone(),
        };
        admission.metadata_point =
            crate::distribution_data::ClusterDataNodeService::capture_actual_joint_metadata_point(
                &service, &own, business, parent,
            )?;
        admission.registered_own = Some((own, business));
        admission.require_registered_own()?;
        if let Some(NativeJointNewObservation::Registered { actual }) =
            self.prepared.quorums.new.get(&node)
        {
            actual.publish_and_count_decision(&admission, parent, &mut self.durable)
        } else {
            self.publish_admitted_member(admission, parent)
        }
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn publish_actual_registered_member(
        &mut self,
        role: RecoveryRoleAdmission,
        own: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        let node = role.local_node_id();
        if !matches!(
            self.prepared.quorums.new.get(node),
            Some(NativeJointNewObservation::Registered { .. })
        ) {
            return Err(invalid(
                "Registered publication requires actual original target NEW owner",
            ));
        }
        self.publish_actual_owned_member(role, own, business, parent)
    }

    fn publish_admitted_member(
        &mut self,
        admission: NativeJointDecisionPublicationAdmission,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        let node = admission.role().local_node_id().clone();
        let receipt = if let Some(new) = self.prepared.quorums.new.get(&node) {
            new.publish_decision(&admission, parent)?
        } else {
            self.prepared
                .quorums
                .old
                .old_observation(&node)
                .ok_or_else(|| invalid("joint publisher member absent"))?
                .publish_decision(&admission, parent)?
        };
        self.prepared.require_actual_receipt(&receipt)?;
        if self
            .durable
            .get(&node)
            .is_some_and(|old| old.checksum_sha256() != receipt.checksum_sha256())
        {
            return Err(invalid(
                "same member returned conflicting protected joint receipt",
            ));
        }
        let retired = {
            let _version = admission.native_publication_guard(receipt.native_admission())?;
            self.durable.insert(node, receipt)
        };
        drop(retired);
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeJointDurableDecision> {
        self.try_finish().map_err(|(error, _original)| error)
    }
    /// On refusal, return the exact original owned round and durable receipts.
    /// No fresh attempt or decoded decision can replace uncertain custody.
    pub(crate) fn try_finish(
        self,
    ) -> std::result::Result<NativeJointDurableDecision, (BicDbError, Self)> {
        if let Err(error) = self.require_finish_current() {
            return Err((error, self));
        }
        Ok(NativeJointDurableDecision {
            prepared: self.prepared,
            durable: self.durable,
        })
    }
    fn require_finish_current(&self) -> Result<()> {
        self.prepared.quorums.require_current()?;
        let old = self
            .durable
            .keys()
            .filter(|id| self.prepared.data.old_members().contains_key(*id))
            .count();
        let new = self
            .durable
            .keys()
            .filter(|id| self.prepared.data.new_members().contains_key(*id))
            .count();
        if old < self.prepared.data.old_members().len() / 2 + 1
            || new < self.prepared.data.new_members().len() / 2 + 1
            || !self
                .durable
                .contains_key(&self.prepared.data.activation_range().leader)
        {
            return Err(invalid("complete decision must be actually durable on both OLD and NEW data majorities, including the new leader"));
        }
        for receipt in self.durable.values() {
            self.prepared.require_actual_receipt(receipt)?;
        }
        Ok(())
    }
}
impl NativeJointDurableDecision {
    /// After the actual metadata stages have consumed this durable decision,
    /// hand its REAL NEW materialization owners to the serving preparation.
    /// This transfers exclusion only; fresh committed MQ is still mandatory.
    pub(crate) fn into_new_serving_owners(
        self,
    ) -> Result<
        BTreeMap<
            ClusterNodeId,
            (
                Arc<parking_lot::Mutex<crate::distribution_data::ClusterDataNodeService>>,
                crate::distribution_data::NativeJointServingLease,
            ),
        >,
    > {
        self.retain_new_serving_owners()
    }
    /// Retry custody only: these leases share the genuine original owners.
    /// Keep this decision until the complete prepared quorum is retained.
    pub(crate) fn retain_new_serving_owners(
        &self,
    ) -> Result<
        BTreeMap<
            ClusterNodeId,
            (
                Arc<parking_lot::Mutex<crate::distribution_data::ClusterDataNodeService>>,
                crate::distribution_data::NativeJointServingLease,
            ),
        >,
    > {
        self.require_current()?;
        let mut owners = BTreeMap::new();
        for (node, observation) in &self.prepared.quorums.new {
            let value = match observation {
                NativeJointNewObservation::Survivor { old, .. } => old.retained_serving_lease()?,
                #[cfg(feature = "tls-replication")]
                NativeJointNewObservation::Registered { actual } => {
                    actual.retain_serving_lease()?
                }
                NativeJointNewObservation::Target { actual, service } => {
                    let lease = {
                        let local = service
                            .try_lock()
                            .ok_or_else(|| invalid("joint NEW owner service busy"))?;
                        actual.retain_joint_serving_lease(&local)?
                    };
                    (service.clone(), lease)
                }
            };
            owners.insert(node.clone(), value);
        }
        Ok(owners)
    }
    #[cfg(test)]
    pub(crate) fn test_preparation_custody(&self) -> serde_json::Value {
        let images = self
            .prepared
            .quorums
            .new
            .iter()
            .filter_map(|(node, observation)| match observation {
                NativeJointNewObservation::Target { actual, .. } => {
                    Some((node.clone(), actual.test_image_custody_identity()))
                }
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        let receipts = self
            .durable
            .iter()
            .map(|(node, receipt)| {
                (
                    node.clone(),
                    serde_json::json!({"checksum":receipt.checksum_sha256(),
                "page_xid":receipt.test_native_page_xid(),
                "database_open":receipt.native_database_incarnation()}),
                )
            })
            .collect::<BTreeMap<_, _>>();
        serde_json::json!({"decision":self.data(),"durable_receipts":receipts,"images":images})
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.prepared.data
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        self.prepared.quorums.require_current()?;
        for receipt in self.durable.values() {
            self.prepared.require_actual_receipt(receipt)?;
        }
        Ok(())
    }
}

impl NativeJointDecisionData {
    pub(crate) fn source_snapshot_db_sequence(&self) -> Result<u64> {
        self.new_materialized
            .get(&self.scope.target)
            .and_then(|fact| fact.source_snapshot_db_sequence)
            .ok_or_else(|| {
                invalid("joint metadata source snapshot B requires the actual completed image seal")
            })
    }
    pub(crate) fn target_local_snapshot_db_sequence(&self) -> Result<u64> {
        self.new_materialized
            .get(&self.scope.target)
            .map(|fact| fact.actual_local_visibility_sequence)
            .ok_or_else(|| {
                invalid("joint metadata publication requires the actual completed target image")
            })
    }
    pub(crate) fn require_metadata_domain(
        &self,
        topology: &crate::distribution::ClusterTopology,
        restore: Option<&crate::distribution_consensus::MetadataRestoreActivation>,
    ) -> Result<()> {
        self.validate()?;
        topology.validate()?;
        if topology.cluster_id != self.scope.cluster_id
            || topology.range_image_bootstrap_id != Some(self.scope.bootstrap_id)
            || restore != self.scope.restore_activation.as_ref()
        {
            return Err(invalid(
                "joint metadata publication changed cluster/bootstrap/restore",
            ));
        }
        for (id, birth) in &self.scope.enrolled_births {
            let node = topology
                .nodes
                .get(id)
                .ok_or_else(|| invalid("joint enrolled member disappeared"))?;
            if node.native_store_birth_v1.as_ref() != Some(birth)
                || node.incarnation != birth.node_incarnation()
                || node.lifecycle != crate::distribution::ClusterNodeLifecycle::Active
                || node
                    .labels
                    .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                    != Some(&self.scope.schema_sha256)
            {
                return Err(invalid("joint enrolled birth/incarnation/schema changed"));
            }
        }
        for (id, expected) in self
            .scope
            .old_members
            .iter()
            .chain(self.scope.new_members.iter())
        {
            let node = &topology.nodes[id];
            if node.incarnation != expected.incarnation
                || node.tls_certificate_sha256 != expected.tls_certificate_sha256
                || node.pending_tls_certificate_sha256 != expected.pending_tls_certificate_sha256
            {
                return Err(invalid("joint data-voter credentials changed"));
            }
        }
        let relocation = topology
            .relocations
            .get(&self.promise().relocation_id)
            .ok_or_else(|| invalid("joint relocation disappeared"))?;
        if relocation.range_id != self.promise().range_id
            || relocation.plan_id != self.promise().plan_id
            || relocation.source.as_ref() != Some(&self.scope.old_source)
            || relocation.target != self.scope.target
            || relocation.learner_epoch != self.promise().old_epoch
            || relocation.handoff_session.as_ref().is_none_or(|s| {
                s.session_id != self.promise().session_id || s.nonce != self.promise().nonce
            })
        {
            return Err(invalid("joint committed relocation/session changed"));
        }
        Ok(())
    }
}
