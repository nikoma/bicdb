//! Exact pending-plan scope derived only from an actual fresh metadata read.
use super::*;
use crate::distribution::{ClusterNodeLifecycle, NativeStoreBirthV1, RangeReplicaRole};
use std::collections::{BTreeMap, BTreeSet};

/// Integrity/correlation data only. Its decoder never constructs a quorum,
/// physical materialization, writer, activation, or cleanup capability.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeJointScopeData {
    pub(super) version: u32,
    pub(super) cluster_id: ClusterId,
    pub(super) bootstrap_id: Uuid,
    pub(super) promise: RangeHandoffPromise,
    pub(super) old_range: RangeDescriptor,
    pub(super) activation_range: RangeDescriptor,
    pub(super) old_members: BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>,
    pub(super) new_members: BTreeMap<ClusterNodeId, crate::db::RangeImageBirthMember>,
    pub(super) enrolled_births: BTreeMap<ClusterNodeId, NativeStoreBirthV1>,
    pub(super) old_source: ClusterNodeId,
    pub(super) target: ClusterNodeId,
    pub(super) schema_sha256: String,
    pub(super) restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
}
pub(super) fn canonical_range(range: &RangeDescriptor) -> RangeDescriptor {
    let mut range = range.clone();
    range.approximate_bytes = 0;
    range.approximate_qps = 0;
    range.replicas.sort_by_key(|r| r.id);
    range
}
impl NativeJointScopeData {
    pub(super) fn from_actual_round(round: &HandoffCutRound) -> Result<Self> {
        Self::from_actual_configuration(
            round.authority.topology()?,
            &round.range,
            &round.intended_activation_range,
            round.request.clone(),
            round.authority.restore_activation()?.cloned(),
        )
    }
    /// A recipient's paired REAL role and independent OWN HostMQ, not a
    /// decoded local plan or a controller-provided topology declaration.
    pub(super) fn from_actual_role_and_own(
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
        own: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<Self> {
        role.require_own_host_metadata(own, business)?;
        let crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id,
            ..
        } = role.purpose()
        else {
            return Err(invalid(
                "recipient OLD plan requires actual CommittedHandoff role",
            ));
        };
        let topology = own.committed_topology(business)?;
        let relocation = topology
            .relocations
            .get(relocation_id)
            .ok_or_else(|| invalid("recipient actual Pending relocation absent"))?;
        let range = topology
            .range_by_id(relocation.range_id)
            .ok_or_else(|| invalid("recipient actual Pending range absent"))?;
        if role.committed_relocation() != Some(relocation)
            || canonical_range(role.current_range()) != canonical_range(range)
            || relocation.learner_epoch != range.epoch
            || !matches!(
                relocation.phase,
                crate::distribution::RelocationPhase::LearnerAllocated
                    | crate::distribution::RelocationPhase::SnapshotCopying
                    | crate::distribution::RelocationPhase::CatchingUp
                    | crate::distribution::RelocationPhase::ReadyToPromote
            )
        {
            return Err(invalid(
                "recipient actual Pending role/configuration differs",
            ));
        }
        let (promise, activation_range, _) = super::super::pending_handoff_configuration(
            topology,
            *relocation_id,
            relocation,
            range,
            own.restore_activation(business)?,
        )?;
        let result = Self::from_actual_configuration(
            topology,
            range,
            &activation_range,
            promise,
            own.restore_activation(business)?.cloned(),
        )?;
        role.require_own_host_metadata(own, business)?;
        Ok(result)
    }
    fn from_actual_configuration(
        topology: &crate::ClusterTopology,
        range: &RangeDescriptor,
        activation_range: &RangeDescriptor,
        promise: RangeHandoffPromise,
        restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    ) -> Result<Self> {
        let relocation = topology
            .relocations
            .get(&promise.relocation_id)
            .ok_or_else(|| invalid("actual joint relocation absent"))?;
        let schema = range
            .required_schema_sha256(&topology.nodes)
            .ok_or_else(|| invalid("joint scope has no common schema"))?
            .to_owned();
        let bootstrap_id = topology
            .range_image_bootstrap_id
            .filter(|id| !id.is_nil())
            .ok_or_else(|| invalid("joint scope has no native bootstrap"))?;
        let old_source = relocation
            .source
            .clone()
            .ok_or_else(|| invalid("first native joint move requires one retiring source"))?;
        let members = |range: &RangeDescriptor| -> BTreeMap<_, _> {
            range
                .replicas
                .iter()
                .filter(|r| r.role == RangeReplicaRole::Voter)
                .map(|r| {
                    let n = &topology.nodes[&r.node_id];
                    (
                        n.id.clone(),
                        crate::db::RangeImageBirthMember {
                            incarnation: n.incarnation,
                            tls_certificate_sha256: n.tls_certificate_sha256.clone(),
                            pending_tls_certificate_sha256: n
                                .pending_tls_certificate_sha256
                                .clone(),
                        },
                    )
                })
                .collect()
        };
        let old_members = members(range);
        let new_members = members(activation_range);
        let mut required: BTreeSet<_> = topology.metadata_voters();
        required.extend(old_members.keys().cloned());
        required.extend(new_members.keys().cloned());
        let mut enrolled_births = BTreeMap::new();
        for id in required {
            let node = topology
                .nodes
                .get(&id)
                .ok_or_else(|| invalid("joint member is absent"))?;
            let birth = node
                .native_store_birth_v1
                .as_ref()
                .ok_or_else(|| invalid("joint voter has no enrolled immutable birth"))?;
            birth.validate_for(Some(bootstrap_id), node.incarnation)?;
            if birth.writer_enforcement_version() != 1
                || node.lifecycle != ClusterNodeLifecycle::Active
                || node
                    .labels
                    .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                    != Some(&schema)
            {
                return Err(invalid(
                    "joint old/new voter requires actual from-birth enforcement and common schema",
                ));
            }
            enrolled_births.insert(id, birth.clone());
        }
        let result = Self {
            version: 1,
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id,
            promise,
            old_range: canonical_range(range),
            activation_range: canonical_range(activation_range),
            old_members,
            new_members,
            enrolled_births,
            old_source,
            target: relocation.target.clone(),
            schema_sha256: schema,
            restore_activation,
        };
        result.validate()?;
        Ok(result)
    }
    pub(super) fn validate(&self) -> Result<()> {
        self.promise.validate()?;
        let voters = |range: &RangeDescriptor| {
            range
                .replicas
                .iter()
                .filter(|r| r.role == RangeReplicaRole::Voter)
                .map(|r| r.node_id.clone())
                .collect::<BTreeSet<_>>()
        };
        let exact_replicas = |range: &RangeDescriptor| {
            let ids = range.replicas.iter().map(|r| r.id).collect::<BTreeSet<_>>();
            let nodes = range
                .replicas
                .iter()
                .map(|r| r.node_id.clone())
                .collect::<BTreeSet<_>>();
            let voter_count = range
                .replicas
                .iter()
                .filter(|r| r.role == RangeReplicaRole::Voter)
                .count();
            !range.replicas.is_empty()
                && range.replicas.len() <= 65
                && ids.len() == range.replicas.len()
                && nodes.len() == range.replicas.len()
                && range.replicas.iter().all(|r| r.id.get() > 0)
                && voter_count == voters(range).len()
                && canonical_range(range) == *range
        };
        if self.version != 1
            || !exact_replicas(&self.old_range)
            || !exact_replicas(&self.activation_range)
            || self.bootstrap_id.is_nil()
            || self.old_range.id != self.promise.range_id
            || self.old_range.epoch != self.promise.old_epoch
            || self.activation_range.id != self.old_range.id
            || self.activation_range.start_token != self.old_range.start_token
            || self.activation_range.end_token != self.old_range.end_token
            || self.activation_range.placement != self.old_range.placement
            || self.activation_range.epoch != self.promise.activation_epoch
            || self.old_members.keys().cloned().collect::<BTreeSet<_>>() != voters(&self.old_range)
            || self.new_members.keys().cloned().collect::<BTreeSet<_>>()
                != voters(&self.activation_range)
            || !self.old_members.contains_key(&self.old_source)
            || self.old_members.contains_key(&self.target)
            || self.new_members.contains_key(&self.old_source)
            || !self.new_members.contains_key(&self.target)
            || self.old_members.is_empty()
            || self.new_members.len() != self.old_members.len()
            || self.old_members.len() > 64
            || self.schema_sha256.len() != 64
            || !self
                .schema_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid(
                "joint scope changes more than one exact fenced replica move",
            ));
        }
        let mut expected = self.old_range.clone();
        expected.replicas.retain(|r| r.node_id != self.old_source);
        let target = expected
            .replicas
            .iter_mut()
            .filter(|r| r.node_id == self.target)
            .collect::<Vec<_>>();
        if target.len() != 1 || target[0].role != RangeReplicaRole::Learner {
            return Err(invalid("joint target is not the exact pending learner"));
        }
        drop(target);
        for r in &mut expected.replicas {
            if r.node_id == self.target {
                r.role = RangeReplicaRole::Voter;
            }
        }
        expected.epoch = self.promise.activation_epoch;
        if expected.leader == self.old_source {
            expected.leader = self.target.clone();
        }
        if canonical_range(&expected) != self.activation_range {
            return Err(invalid(
                "joint activation changes an unrelated range placement or replica identity",
            ));
        }
        for (id, old) in &self.old_members {
            if let Some(new) = self.new_members.get(id) {
                if new != old {
                    return Err(invalid("joint survivor credentials changed"));
                }
            }
        }
        for (id, member) in self.old_members.iter().chain(self.new_members.iter()) {
            let birth = self
                .enrolled_births
                .get(id)
                .ok_or_else(|| invalid("joint member birth missing"))?;
            birth.validate_for(Some(self.bootstrap_id), member.incarnation)?;
            if birth.writer_enforcement_version() != 1 {
                return Err(invalid("joint member is not born-v2 enforced"));
            }
        }
        Ok(())
    }
    pub(super) fn checksum(&self) -> Result<String> {
        self.validate()?;
        Ok(hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(self, 256 * 1024)?,
        )))
    }
    pub(super) fn require_actual_native(
        &self,
        native: &crate::db::NativeRecoveryAdmission,
    ) -> Result<()> {
        let actual = crate::distribution::NativeStoreBirthV1::from_actual_native(native)?;
        if native.writer_enforcement_version() != 1
            || native.cluster_id() != &self.cluster_id
            || native.bootstrap_id() != self.bootstrap_id
            || self.enrolled_births.get(native.local_node_id()) != Some(&actual)
        {
            return Err(invalid(
                "joint scope differs from actual local immutable native birth",
            ));
        }
        Ok(())
    }
}
