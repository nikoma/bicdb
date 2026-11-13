//! NEW data quorum materialization. Its only observations are a retained actual
//! old native closure or an actual finalized target image. No ready DTO votes.
use super::*;
use crate::distribution_data::{ActualServiceNativeJointNewEvidence, ClusterDataNodeService};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Data for immutable decision provenance only. A decoded value cannot mint
/// an observation/quorum or authorize a membership transition.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJointMaterializedFacts {
    pub(crate) scope_sha256: String,
    pub(crate) node_id: ClusterNodeId,
    pub(crate) birth_id: Uuid,
    pub(crate) birth_sha256: String,
    pub(crate) native_receipt_sha256: String,
    pub(crate) range_index: u64,
    pub(crate) logical_genesis_sha256: String,
    pub(crate) logical_history_sha256: String,
    pub(crate) actual_local_visibility_sequence: u64,
    /// Actual completed capture reserved B; present only for the installed
    /// learner image. Old/native visibility B is a different local domain.
    pub(crate) source_snapshot_db_sequence: Option<u64>,
    pub(crate) image_root_sha256: Option<String>,
    pub(crate) metadata_term: u64,
    pub(crate) metadata_read_index: u64,
    pub(crate) metadata_context_id: Uuid,
}

pub(crate) enum NativeJointNewObservation {
    #[cfg(feature = "tls-replication")]
    Registered {
        actual: crate::distribution_data::ActualServiceNativeRegisteredJointNewEvidence,
    },
    Survivor {
        old: Arc<NativeJointOldObservation>,
        role: crate::distribution_range_recovery::RecoveryRoleAdmission,
    },
    Target {
        actual: ActualServiceNativeJointNewEvidence,
        service: Arc<Mutex<ClusterDataNodeService>>,
    },
}
impl NativeJointNewObservation {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_registered_actual_target(
        actual: crate::distribution_data::ActualServiceNativeRegisteredJointNewEvidence,
    ) -> Result<Self> {
        actual.require_current()?;
        Ok(Self::Registered { actual })
    }
    pub(crate) fn from_finalized_actual_target(
        actual: ActualServiceNativeJointNewEvidence,
        service: Arc<Mutex<ClusterDataNodeService>>,
    ) -> Result<Self> {
        {
            let local = service
                .try_lock()
                .ok_or_else(|| invalid("new target service busy"))?;
            actual.require_current(&local)?;
        }
        Ok(Self::Target { actual, service })
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        match self {
            #[cfg(feature = "tls-replication")]
            Self::Registered { actual } => actual.require_current(),
            Self::Survivor { old, .. } => old.require_current(),
            Self::Target { actual, service } => {
                let local = service
                    .try_lock()
                    .ok_or_else(|| invalid("new target service busy"))?;
                actual.require_current(&local)
            }
        }
    }
    pub(super) fn require_live_issuance(&self) -> Result<()> {
        match self {
            #[cfg(feature = "tls-replication")]
            Self::Registered { actual } => actual.require_issuance_current(),
            Self::Survivor { old, role } => old.require_new_member_role(role),
            Self::Target { actual, .. } => actual.require_issuance_current(),
        }
    }
    pub(super) fn facts(&self) -> NativeJointMaterializedFacts {
        match self {
            #[cfg(feature = "tls-replication")]
            Self::Registered { actual } => actual.facts().clone(),
            Self::Survivor { old, role } => {
                let mut facts = old.materialized_facts();
                facts.metadata_term = role.metadata_term();
                facts.metadata_read_index = role.metadata_read_index();
                facts.metadata_context_id = role.context_id();
                facts
            }
            Self::Target { actual, .. } => actual.facts().clone(),
        }
    }
}

pub(crate) struct NativeJointNewRound {
    old: NativeJointOldDecisionOwners,
    observations: BTreeMap<ClusterNodeId, NativeJointNewObservation>,
}
pub(crate) struct NativeJointMaterializedQuorums {
    pub(super) old: NativeJointOldDecisionOwners,
    pub(super) new: BTreeMap<ClusterNodeId, NativeJointNewObservation>,
}
impl NativeJointNewRound {
    pub(crate) fn begin(old: Arc<NativeJointOldClosedQuorum>) -> Result<Self> {
        old.require_current()?;
        Ok(Self {
            old: NativeJointOldDecisionOwners::Local(old),
            observations: BTreeMap::new(),
        })
    }
    /// The certificate is not cast into a local quorum. Every actual recipient
    /// owner must independently match the authenticated historical response.
    #[cfg(feature = "tls-replication")]
    pub(crate) fn begin_remote_actual(
        certificate: Arc<NativeRemoteOldCutCertificate>,
        recipients: BTreeMap<ClusterNodeId, Arc<NativeJointOldObservation>>,
    ) -> Result<Self> {
        Ok(Self {
            old: NativeJointOldDecisionOwners::from_remote_actual(certificate, recipients)?,
            observations: BTreeMap::new(),
        })
    }
    pub(crate) fn record_actual_survivor(
        &mut self,
        node: &ClusterNodeId,
        role: crate::distribution_range_recovery::RecoveryRoleAdmission,
    ) -> Result<()> {
        let actual = self.old.observation(node)?;
        actual.require_new_member_role(&role)?;
        self.record(NativeJointNewObservation::Survivor { old: actual, role })
    }
    pub(crate) fn record(&mut self, observation: NativeJointNewObservation) -> Result<()> {
        observation.require_live_issuance()?;
        observation.require_current()?;
        let facts = observation.facts();
        let plan = self.old.plan();
        if facts.scope_sha256 != plan.checksum_sha256()?
            || !plan.new_members().contains_key(&facts.node_id)
            || facts.range_index != self.old.range_index()
            || facts.logical_genesis_sha256 != self.old.logical_genesis_sha256()
            || facts.logical_history_sha256 != self.old.logical_history_sha256()
        {
            return Err(invalid("actual NEW native materialization conflicts with the closed OLD data prefix/configuration"));
        }
        if self.observations.contains_key(&facts.node_id) {
            return Err(invalid("duplicate NEW physical member cannot vote twice"));
        }
        self.observations.insert(facts.node_id, observation);
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeJointMaterializedQuorums> {
        self.old.require_current()?;
        let plan = self.old.plan();
        if self.observations.len() < plan.new_members().len() / 2 + 1
            || !self
                .observations
                .contains_key(&plan.activation_range().leader)
            || !self.observations.contains_key(&plan.data().target)
        {
            return Err(invalid(
                "actual NEW data materialization needs a distinct majority and its intended leader",
            ));
        }
        for observation in self.observations.values() {
            observation.require_live_issuance()?;
            observation.require_current()?;
        }
        Ok(NativeJointMaterializedQuorums {
            old: self.old,
            new: self.observations,
        })
    }
}
impl NativeJointMaterializedQuorums {
    pub(crate) fn require_current(&self) -> Result<()> {
        self.old.require_current()?;
        for observation in self.new.values() {
            observation.require_current()?;
        }
        Ok(())
    }
    pub(super) fn new_facts(&self) -> BTreeMap<ClusterNodeId, NativeJointMaterializedFacts> {
        self.new
            .iter()
            .map(|(id, observation)| (id.clone(), observation.facts()))
            .collect()
    }
}

impl NativeJointNewObservation {
    #[cfg(feature = "tls-replication")]
    pub(super) fn actual_service_for_metadata(
        &self,
    ) -> &Arc<parking_lot::Mutex<crate::distribution_data::ClusterDataNodeService>> {
        match self {
            Self::Registered { actual } => actual.service(),
            Self::Survivor { old, .. } => old.actual_service(),
            Self::Target { service, .. } => service,
        }
    }
    pub(super) fn release_capture_pin_for_decision(&self) -> Result<()> {
        match self {
            #[cfg(feature = "tls-replication")]
            Self::Registered { actual } => actual.release_capture_pin_for_decision(),
            Self::Survivor { old, .. } => old.release_capture_pin_for_decision(),
            Self::Target { actual, service } => {
                let service = service
                    .try_lock()
                    .ok_or_else(|| invalid("new target pin transition service busy"))?;
                actual.release_capture_pin_for_decision(&service)
            }
        }
    }
}

impl NativeJointNewObservation {
    pub(super) fn publish_decision(
        &self,
        admission: &super::decision::NativeJointDecisionPublicationAdmission,
        parent: &crate::CancellationToken,
    ) -> Result<crate::db::NativeJointDecisionReceipt> {
        match self {
            #[cfg(feature = "tls-replication")]
            Self::Registered { .. } => Err(invalid(
                "Registered decision needs original actual current-metadata publication/count path",
            )),
            Self::Survivor { old, .. } => old.publish_decision(admission, parent),
            Self::Target { actual, service } => {
                let mut service = service
                    .try_lock()
                    .ok_or_else(|| invalid("target decision publisher busy"))?;
                actual.publish_decision(&mut service, admission, parent)
            }
        }
    }
}

impl NativeJointNewObservation {
    pub(super) fn require_decision_receipt(
        &self,
        receipt: &crate::db::NativeJointDecisionReceipt,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "tls-replication")]
            Self::Registered { actual } => actual.require_decision_receipt(receipt),
            Self::Survivor { old, .. } => old.require_decision_receipt(receipt),
            Self::Target { service, .. } => service
                .try_lock()
                .ok_or_else(|| invalid("new decision receipt service busy"))?
                .require_native_joint_decision_receipt(receipt),
        }
    }
}
