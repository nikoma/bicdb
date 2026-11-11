//! Purpose-bound logical cuts. The recovery variant deliberately has no
//! numeric/DTO constructor: declaring its shape does not implement recovery.
use std::collections::BTreeMap;

use super::*;
use crate::db::RangeImageBirthMember;
use crate::distribution::RangeDescriptor;
use crate::distribution_consensus::MetadataRestoreActivation;

pub(crate) use crate::distribution_range_image_scope::RangeImageSourcePurposeV1;

/// No Serialize/Deserialize, public fields, number constructor or fake plan.
/// Its factory consumes actual fresh fenced-current-quorum history agreement
/// AND independently materialized local native lineage. Physical repair does
/// not grant serving-configuration, leader, Apply or strong-read authority.
#[derive(Debug)]
pub(crate) struct CertifiedRecoveryImageCut {
    cluster_id: ClusterId,
    bootstrap_id: Uuid,
    range: RangeDescriptor,
    native_base_range: RangeDescriptor,
    range_index: u64,
    source_id: ClusterNodeId,
    source_incarnation: u64,
    target_id: ClusterNodeId,
    target_incarnation: u64,
    session_id: Uuid,
    nonce: Uuid,
    current_config_sha256: String,
    members: BTreeMap<ClusterNodeId, RangeImageBirthMember>,
    schema_sha256: String,
    restore_activation: Option<MetadataRestoreActivation>,
    source_native_receipt_sha256: String,
    source_logical_genesis_sha256: String,
    source_logical_history_sha256: String,
    historical_acceptance:
        crate::distribution_range_recovery::HistoricalCurrentImageSourceAcceptance,
    resume_proposal: Option<crate::distribution_range_consensus::NativeCurrentImageProposalPointer>,
}

impl CertifiedRecoveryImageCut {
    pub(crate) fn historical_acceptance(
        &self,
    ) -> &crate::distribution_range_recovery::HistoricalCurrentImageSourceAcceptance {
        &self.historical_acceptance
    }
    pub(crate) fn from_verified_source_history(
        history: crate::distribution_range_recovery::VerifiedRecoverySourceHistory,
        source: &crate::db::LocalNativeRecoveryReady,
        database: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<Self> {
        history.require_current(source, database, store)?;
        let authority = history.authority();
        let role = authority.role();
        let target = history.target();
        let target_identity = authority
            .member_credentials()
            .get(target)
            .ok_or_else(|| invalid("image repair target is no longer a current voter"))?;
        let schema_sha256 = role
            .expected_schema_sha256()
            .ok_or_else(|| invalid("image repair has no committed schema contract"))?;
        let cut = Self {
            cluster_id: role.cluster_id().clone(),
            bootstrap_id: role.bootstrap_id(),
            range: role.current_range().clone(),
            native_base_range: source.physical_base_range().clone(),
            range_index: source.range_index(),
            source_id: source.role().local_node_id().clone(),
            source_incarnation: source.role().local_member_incarnation(),
            target_id: target.clone(),
            target_incarnation: target_identity.incarnation,
            session_id: role.recovery_nonce(),
            nonce: role.recovery_nonce(),
            current_config_sha256: role.configuration_sha256()?,
            members: role
                .member_credentials()
                .iter()
                .map(|(id, identity)| {
                    (
                        id.clone(),
                        RangeImageBirthMember {
                            incarnation: identity.incarnation,
                            tls_certificate_sha256: identity.active_certificate_sha256.clone(),
                            pending_tls_certificate_sha256: identity
                                .pending_certificate_sha256
                                .clone(),
                        },
                    )
                })
                .collect(),
            schema_sha256: schema_sha256.to_string(),
            restore_activation: role.restore_activation().cloned(),
            source_native_receipt_sha256: source.native_receipt_sha256().to_string(),
            source_logical_genesis_sha256: source.logical_genesis_sha256().to_string(),
            source_logical_history_sha256: source.logical_history_sha256().to_string(),
            historical_acceptance: history
                .historical_current_image_acceptance(source, database, store)?,
            resume_proposal: None,
        };
        history.require_current(source, database, store)?;
        Ok(cut)
    }
    pub(crate) fn from_verified_resumed_source_history(
        history: crate::distribution_range_recovery::VerifiedCurrentImageResumeHistory,
        source: &crate::db::LocalNativeRecoveryReady,
        database: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<Self> {
        history.require_current(source, database, store)?;
        let authority = history.authority();
        let role = authority.role();
        let target = authority.target();
        let target_identity = authority
            .member_credentials()
            .get(target)
            .ok_or_else(|| invalid("image repair target is no longer a current voter"))?;
        let schema_sha256 = role
            .expected_schema_sha256()
            .ok_or_else(|| invalid("image repair has no committed schema contract"))?;
        let cut = Self {
            cluster_id: role.cluster_id().clone(),
            bootstrap_id: role.bootstrap_id(),
            range: role.current_range().clone(),
            native_base_range: source.physical_base_range().clone(),
            range_index: source.range_index(),
            source_id: source.role().local_node_id().clone(),
            source_incarnation: source.role().local_member_incarnation(),
            target_id: target.clone(),
            target_incarnation: target_identity.incarnation,
            session_id: role.recovery_nonce(),
            nonce: role.recovery_nonce(),
            current_config_sha256: role.configuration_sha256()?,
            members: role
                .member_credentials()
                .iter()
                .map(|(id, identity)| {
                    (
                        id.clone(),
                        RangeImageBirthMember {
                            incarnation: identity.incarnation,
                            tls_certificate_sha256: identity.active_certificate_sha256.clone(),
                            pending_tls_certificate_sha256: identity
                                .pending_certificate_sha256
                                .clone(),
                        },
                    )
                })
                .collect(),
            schema_sha256: schema_sha256.to_string(),
            restore_activation: role.restore_activation().cloned(),
            source_native_receipt_sha256: source.native_receipt_sha256().to_string(),
            source_logical_genesis_sha256: source.logical_genesis_sha256().to_string(),
            source_logical_history_sha256: source.logical_history_sha256().to_string(),
            historical_acceptance: history
                .historical_current_image_acceptance(source, database, store)?,
            resume_proposal: Some(authority.proposal().clone()),
        };
        history.require_current(source, database, store)?;
        Ok(cut)
    }
}

#[derive(Debug)]
pub(crate) enum NativeRangeImageSourceCut {
    CommittedHandoff(CertifiedRangeImageCut),
    CurrentVoterRepair(CertifiedRecoveryImageCut),
}
impl From<CertifiedRangeImageCut> for NativeRangeImageSourceCut {
    fn from(cut: CertifiedRangeImageCut) -> Self {
        Self::CommittedHandoff(cut)
    }
}
impl From<CertifiedRecoveryImageCut> for NativeRangeImageSourceCut {
    fn from(cut: CertifiedRecoveryImageCut) -> Self {
        Self::CurrentVoterRepair(cut)
    }
}

fn semantic_range(a: &RangeDescriptor, b: &RangeDescriptor) -> bool {
    let mut a = a.clone();
    let mut b = b.clone();
    a.approximate_bytes = 0;
    a.approximate_qps = 0;
    b.approximate_bytes = 0;
    b.approximate_qps = 0;
    a.replicas.sort_by_key(|replica| replica.id);
    b.replicas.sort_by_key(|replica| replica.id);
    a == b
}
impl NativeRangeImageSourceCut {
    pub(crate) fn resume_proposal(
        &self,
    ) -> Option<&crate::distribution_range_consensus::NativeCurrentImageProposalPointer> {
        match self {
            Self::CurrentVoterRepair(cut) => cut.resume_proposal.as_ref(),
            Self::CommittedHandoff(_) => None,
        }
    }
    pub(crate) fn historical_current_acceptance(
        &self,
    ) -> Option<&crate::distribution_range_recovery::HistoricalCurrentImageSourceAcceptance> {
        match self {
            Self::CurrentVoterRepair(cut) => Some(cut.historical_acceptance()),
            Self::CommittedHandoff(_) => None,
        }
    }
    pub(crate) fn purpose(&self) -> RangeImageSourcePurposeV1 {
        match self {
            Self::CommittedHandoff(cut) => RangeImageSourcePurposeV1::CommittedHandoff {
                relocation_id: cut.relocation_id(),
                plan_id: cut.plan_id().to_string(),
            },
            Self::CurrentVoterRepair(_) => RangeImageSourcePurposeV1::CurrentVoterRepair,
        }
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        match self {
            Self::CommittedHandoff(cut) => cut.cluster_id(),
            Self::CurrentVoterRepair(cut) => &cut.cluster_id,
        }
    }
    pub(crate) fn bootstrap_id(&self) -> Option<Uuid> {
        match self {
            Self::CommittedHandoff(cut) => cut.bootstrap_id(),
            Self::CurrentVoterRepair(cut) => Some(cut.bootstrap_id),
        }
    }
    pub(crate) fn range(&self) -> &RangeDescriptor {
        match self {
            Self::CommittedHandoff(cut) => cut.range(),
            Self::CurrentVoterRepair(cut) => &cut.range,
        }
    }
    pub(crate) fn range_index(&self) -> u64 {
        match self {
            Self::CommittedHandoff(cut) => cut.range_index(),
            Self::CurrentVoterRepair(cut) => cut.range_index,
        }
    }
    pub(crate) fn source_id(&self) -> &ClusterNodeId {
        match self {
            Self::CommittedHandoff(cut) => cut.source_id(),
            Self::CurrentVoterRepair(cut) => &cut.source_id,
        }
    }
    pub(crate) fn source_incarnation(&self) -> u64 {
        match self {
            Self::CommittedHandoff(cut) => cut.source_incarnation(),
            Self::CurrentVoterRepair(cut) => cut.source_incarnation,
        }
    }
    pub(crate) fn target_id(&self) -> &ClusterNodeId {
        match self {
            Self::CommittedHandoff(cut) => cut.target_id(),
            Self::CurrentVoterRepair(cut) => &cut.target_id,
        }
    }
    pub(crate) fn target_incarnation(&self) -> u64 {
        match self {
            Self::CommittedHandoff(cut) => cut.target_incarnation(),
            Self::CurrentVoterRepair(cut) => cut.target_incarnation,
        }
    }
    pub(crate) fn session_id(&self) -> Uuid {
        match self {
            Self::CommittedHandoff(cut) => cut.session_id(),
            Self::CurrentVoterRepair(cut) => cut.session_id,
        }
    }
    pub(crate) fn nonce(&self) -> Uuid {
        match self {
            Self::CommittedHandoff(cut) => cut.nonce(),
            Self::CurrentVoterRepair(cut) => cut.nonce,
        }
    }
    pub(crate) fn old_config_sha256(&self) -> &str {
        match self {
            Self::CommittedHandoff(cut) => cut.old_config_sha256(),
            Self::CurrentVoterRepair(cut) => &cut.current_config_sha256,
        }
    }
    pub(crate) fn new_config_sha256(&self) -> &str {
        match self {
            Self::CommittedHandoff(cut) => cut.new_config_sha256(),
            Self::CurrentVoterRepair(cut) => &cut.current_config_sha256,
        }
    }
    pub(crate) fn activation_epoch(&self) -> u64 {
        match self {
            Self::CommittedHandoff(cut) => cut.activation_epoch(),
            Self::CurrentVoterRepair(cut) => cut.range.epoch,
        }
    }
    pub(crate) fn intended_activation_range(&self) -> &RangeDescriptor {
        match self {
            Self::CommittedHandoff(cut) => cut.intended_activation_range(),
            Self::CurrentVoterRepair(cut) => &cut.range,
        }
    }
    pub(crate) fn intended_activation_members(
        &self,
    ) -> &BTreeMap<ClusterNodeId, RangeImageBirthMember> {
        match self {
            Self::CommittedHandoff(cut) => cut.intended_activation_members(),
            Self::CurrentVoterRepair(cut) => &cut.members,
        }
    }
    pub(crate) fn capture_members(&self) -> BTreeMap<ClusterNodeId, RangeImageBirthMember> {
        match self {
            Self::CommittedHandoff(cut) => cut.capture_members(),
            Self::CurrentVoterRepair(cut) => cut.members.clone(),
        }
    }
    pub(crate) fn schema_sha256(&self) -> Option<&str> {
        match self {
            Self::CommittedHandoff(cut) => cut.schema_sha256(),
            Self::CurrentVoterRepair(cut) => Some(&cut.schema_sha256),
        }
    }
    pub(crate) fn restore_activation(&self) -> Option<&MetadataRestoreActivation> {
        match self {
            Self::CommittedHandoff(cut) => cut.restore_activation(),
            Self::CurrentVoterRepair(cut) => cut.restore_activation.as_ref(),
        }
    }

    pub(crate) fn validate_source_lineage_configuration(
        &self,
        lineage: &SourceRangeImageLineage,
    ) -> Result<()> {
        match self {
            Self::CommittedHandoff(cut) => cut.validate_source_lineage_configuration(lineage),
            Self::CurrentVoterRepair(cut) => {
                if lineage.cluster_id() != &cut.cluster_id
                    || lineage.bootstrap_id() != cut.bootstrap_id
                    || lineage.local_node_id() != &cut.source_id
                    || lineage.local_member_incarnation() != cut.source_incarnation
                    || lineage.range_index() != cut.range_index
                    || lineage.native_receipt_sha256() != cut.source_native_receipt_sha256
                    || lineage.logical_genesis_sha256() != cut.source_logical_genesis_sha256
                    || lineage.logical_history_sha256() != cut.source_logical_history_sha256
                    || lineage.schema_sha256() != cut.schema_sha256
                    || lineage.restore_activation() != cut.restore_activation.as_ref()
                    || !semantic_range(lineage.base_range(), &cut.native_base_range)
                    || cut.native_base_range.id != cut.range.id
                    || cut.native_base_range.start_token != cut.range.start_token
                    || cut.native_base_range.end_token != cut.range.end_token
                    || cut
                        .native_base_range
                        .replicas
                        .iter()
                        .filter(|replica| {
                            replica.role == crate::distribution::RangeReplicaRole::Voter
                        })
                        .map(|replica| replica.node_id.clone())
                        .collect::<std::collections::BTreeSet<_>>()
                        != cut.members.keys().cloned().collect()
                    || cut
                        .members
                        .iter()
                        .any(|(id, member)| lineage.base_members().get(id) != Some(member))
                {
                    return Err(invalid("current-quorum image cut does not bind the selected actual native source lineage"));
                }
                Ok(())
            }
        }
    }
}
