//! Private native-prefix admission ingredients. These are not metadata/read
//! authorities, portable signatures, or source visibility by themselves.
use super::*;
use crate::db::{RangeImageBirthMember, SourceRangeImageLineage};

const MAX_NATIVE_DATA_VOTERS: usize = 64;
const MAX_NATIVE_SCOPE_BYTES: usize = 64 * 1024;

/// A bounded command/configuration snapshot. Only this parent coordinator
/// module can construct it; a decoded command DTO cannot supply its authority.
#[derive(Clone, Debug)]
pub(crate) struct NativeRangeCommandIntent {
    command_id: String,
    command_checksum_sha256: String,
    origin_transaction_id: u64,
    mutations_sha256: String,
    range_index: u64,
    cluster_id: ClusterId,
    bootstrap_id: Uuid,
    range: RangeDescriptor,
    expected_base: RangeDescriptor,
    members: BTreeMap<ClusterNodeId, RangeImageBirthMember>,
    schema_sha256: String,
    scope_sha256: String,
}

/// Actual distinct certificate ACKs collected by the coordinator. This opaque
/// value is not serializable and cannot be recreated from an ACK count.
#[derive(Debug)]
pub(crate) struct NativeRangeCommitCertificate {
    intent: NativeRangeCommandIntent,
    certified_voters: BTreeSet<ClusterNodeId>,
    required_quorum: usize,
}

fn replicas(
    range: &RangeDescriptor,
) -> BTreeMap<crate::ReplicaId, (ClusterNodeId, RangeReplicaRole)> {
    range
        .replicas
        .iter()
        .map(|replica| (replica.id, (replica.node_id.clone(), replica.role)))
        .collect()
}
fn same_range_configuration(left: &RangeDescriptor, right: &RangeDescriptor) -> bool {
    left.id == right.id
        && left.start_token == right.start_token
        && left.end_token == right.end_token
        && left.epoch == right.epoch
        && left.leader == right.leader
        && left.placement == right.placement
        && left.replicas.len() == right.replicas.len()
        && replicas(left) == replicas(right)
}

impl NativeRangeCommandIntent {
    pub(super) fn plan(
        command: &RangeWriteCommand,
        range: &RangeDescriptor,
        topology: &ClusterTopology,
    ) -> Result<Self> {
        let bootstrap_id = topology
            .range_image_bootstrap_id
            .filter(|id| !id.is_nil())
            .ok_or_else(|| {
                range_write_error("native write has no committed image-aware bootstrap domain")
            })?;
        let current = topology
            .range_by_id(range.id)
            .ok_or_else(|| range_write_error("native write range is absent"))?;
        if !same_range_configuration(current, range)
            || command.cluster_id != topology.cluster_id
            || command.range_id != range.id
            || command.range_epoch != range.epoch
            || command.leader_node_id != range.leader
        {
            return Err(range_write_error(
                "native command does not match its committed-local configuration snapshot",
            ));
        }
        command.validate()?;
        let mut members = BTreeMap::new();
        for replica in &range.replicas {
            if replica.role != RangeReplicaRole::Voter {
                continue;
            }
            let member = topology
                .nodes
                .get(&replica.node_id)
                .ok_or_else(|| range_write_error("native data voter is absent"))?;
            if members
                .insert(
                    member.id.clone(),
                    RangeImageBirthMember {
                        incarnation: member.incarnation,
                        tls_certificate_sha256: member.tls_certificate_sha256.clone(),
                        pending_tls_certificate_sha256: member
                            .pending_tls_certificate_sha256
                            .clone(),
                    },
                )
                .is_some()
            {
                return Err(range_write_error("duplicate native data voter"));
            }
        }
        if members.is_empty() || members.len() > MAX_NATIVE_DATA_VOTERS {
            return Err(range_write_error(
                "native data voter count is outside 1..=64",
            ));
        }
        let schema_sha256 = range
            .required_schema_sha256(&topology.nodes)
            .ok_or_else(|| range_write_error("native write has no committed schema fence"))?
            .to_string();
        let mut expected_base = range.clone();
        if let Some(relocation) = topology.active_relocation_for_range(range.id) {
            if relocation.learner_epoch == range.epoch {
                if relocation.expected_epoch.checked_add(1) != Some(range.epoch) {
                    return Err(range_write_error(
                        "native write learner allocation has no exact predecessor",
                    ));
                }
                let matches = range
                    .replicas
                    .iter()
                    .filter(|replica| {
                        replica.id == relocation.target_replica_id
                            && replica.node_id == relocation.target
                            && replica.role == RangeReplicaRole::Learner
                    })
                    .count();
                if matches != 1 {
                    return Err(range_write_error(
                        "native write learner allocation differs from committed plan",
                    ));
                }
                expected_base.epoch = relocation.expected_epoch;
                expected_base
                    .replicas
                    .retain(|replica| replica.id != relocation.target_replica_id);
            }
        }
        let scope = crate::distribution_range_image::bounded_json(
            &(
                &topology.cluster_id,
                bootstrap_id,
                &range.id,
                range.start_token,
                range.end_token,
                range.epoch,
                &range.leader,
                &members,
                &schema_sha256,
            ),
            MAX_NATIVE_SCOPE_BYTES,
        )?;
        Ok(Self {
            command_id: command.command_id.clone(),
            command_checksum_sha256: command.checksum_sha256.clone(),
            origin_transaction_id: command.transaction_id,
            mutations_sha256: crate::db::admission_mutations_sha256(&command.mutations)?,
            range_index: command.index,
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id,
            range: range.clone(),
            expected_base,
            members,
            schema_sha256,
            scope_sha256: hex::encode(Sha256::digest(scope)),
        })
    }
    pub(crate) fn validate_native_base(&self, lineage: &SourceRangeImageLineage) -> Result<()> {
        if lineage.cluster_id() != &self.cluster_id
            || lineage.bootstrap_id() != self.bootstrap_id
            || lineage.range_index().checked_add(1) != Some(self.range_index)
            || lineage.schema_sha256() != self.schema_sha256
            || !same_range_configuration(lineage.base_range(), &self.expected_base)
            || !self.members.contains_key(lineage.local_node_id())
            || self
                .members
                .get(lineage.local_node_id())
                .map(|member| member.incarnation)
                != Some(lineage.local_member_incarnation())
        {
            return Err(range_write_error(
                "native command cannot extend this actual native base/configuration",
            ));
        }
        for (id, member) in &self.members {
            if lineage.base_members().get(id) != Some(member) {
                return Err(range_write_error(
                    "native command voter credentials differ from its base",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn certify(
        &self,
        certified_voters: &BTreeSet<ClusterNodeId>,
    ) -> Result<NativeRangeCommitCertificate> {
        let required_quorum = self.members.len() / 2 + 1;
        if certified_voters.len() < required_quorum
            || !certified_voters.contains(&self.range.leader)
            || !certified_voters
                .iter()
                .all(|member| self.members.contains_key(member))
        {
            return Err(range_write_error(
                "native command has no distinct leader-inclusive certificate quorum",
            ));
        }
        Ok(NativeRangeCommitCertificate {
            intent: self.clone(),
            certified_voters: certified_voters.clone(),
            required_quorum,
        })
    }
    pub(crate) fn command_id(&self) -> &str {
        &self.command_id
    }
    pub(crate) fn command_checksum_sha256(&self) -> &str {
        &self.command_checksum_sha256
    }
    pub(crate) fn origin_transaction_id(&self) -> u64 {
        self.origin_transaction_id
    }
    pub(crate) fn mutations_sha256(&self) -> &str {
        &self.mutations_sha256
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.range_index
    }
    pub(crate) fn range(&self) -> &RangeDescriptor {
        &self.range
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub(crate) fn bootstrap_id(&self) -> Uuid {
        self.bootstrap_id
    }
    pub(crate) fn scope_sha256(&self) -> &str {
        &self.scope_sha256
    }
}
impl NativeRangeCommitCertificate {
    pub(crate) fn intent(&self) -> &NativeRangeCommandIntent {
        &self.intent
    }
    pub(crate) fn certified_voters(&self) -> &BTreeSet<ClusterNodeId> {
        &self.certified_voters
    }
    pub(crate) fn required_quorum(&self) -> usize {
        self.required_quorum
    }
}
