//! Distinct original OLD decision ownership. A remote certificate remains
//! remote: only independently retained actual recipient objects can perform
//! native decision/phase operations. No DTO constructs a local closed quorum.
use super::*;
use crate::distribution_data::NativeJointIrrevocableFacts;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) enum NativeJointOldDecisionOwners {
    Local(Arc<NativeJointOldClosedQuorum>),
    #[cfg(feature = "tls-replication")]
    Remote {
        certificate: Arc<NativeRemoteOldCutCertificate>,
        recipients: BTreeMap<ClusterNodeId, Arc<NativeJointOldObservation>>,
        constraints: Vec<NativeJointIrrevocableFacts>,
    },
}
impl NativeJointOldDecisionOwners {
    #[cfg(feature = "tls-replication")]
    pub(super) fn from_remote_actual(
        certificate: Arc<NativeRemoteOldCutCertificate>,
        recipients: BTreeMap<ClusterNodeId, Arc<NativeJointOldObservation>>,
    ) -> Result<Self> {
        let constraints = certificate.decision_constraints();
        let owners = Self::Remote {
            certificate,
            recipients,
            constraints,
        };
        owners.require_current()?;
        Ok(owners)
    }
    pub(super) fn require_current(&self) -> Result<()> {
        match self {
            Self::Local(old) => old.require_current(),
            #[cfg(feature = "tls-replication")]
            Self::Remote {
                certificate,
                recipients,
                ..
            } => {
                certificate.require_current()?;
                let expected = certificate.source_image_materialized_facts()?;
                if recipients.len() != expected.len() {
                    return Err(invalid(
                        "remote OLD actual recipients omit original obligations",
                    ));
                }
                let plan = certificate.plan();
                for (id, facts) in &expected {
                    let actual = recipients
                        .get(id)
                        .ok_or_else(|| invalid("remote OLD actual recipient absent"))?;
                    actual.require_current()?;
                    if actual.node_id() != id
                        || actual.plan().checksum_sha256()? != plan.checksum_sha256()?
                        || actual.materialized_facts() != *facts
                    {
                        return Err(invalid(
                            "remote OLD actual recipient differs from authenticated closed fact",
                        ));
                    }
                }
                certificate.require_current()
            }
        }
    }
    pub(super) fn plan(&self) -> Arc<NativeJointHandoffPlan> {
        match self {
            Self::Local(old) => old.plan(),
            #[cfg(feature = "tls-replication")]
            Self::Remote { certificate, .. } => certificate.plan(),
        }
    }
    pub(super) fn observation(
        &self,
        node: &ClusterNodeId,
    ) -> Result<Arc<NativeJointOldObservation>> {
        self.require_current()?;
        self.old_observation(node)
            .cloned()
            .ok_or_else(|| invalid("actual OLD recipient owner absent"))
    }
    pub(super) fn old_observation(
        &self,
        node: &ClusterNodeId,
    ) -> Option<&Arc<NativeJointOldObservation>> {
        match self {
            Self::Local(old) => old.old_observation(node),
            #[cfg(feature = "tls-replication")]
            Self::Remote { recipients, .. } => recipients.get(node),
        }
    }
    pub(super) fn materialized_facts(
        &self,
    ) -> BTreeMap<ClusterNodeId, NativeJointMaterializedFacts> {
        match self {
            Self::Local(old) => old.materialized_facts(),
            #[cfg(feature = "tls-replication")]
            Self::Remote { recipients, .. } => recipients
                .iter()
                .map(|(id, actual)| (id.clone(), actual.materialized_facts()))
                .collect(),
        }
    }
    pub(super) fn constraints(&self) -> &[NativeJointIrrevocableFacts] {
        match self {
            Self::Local(old) => old.constraints(),
            #[cfg(feature = "tls-replication")]
            Self::Remote { constraints, .. } => constraints,
        }
    }
    pub(super) fn selected_source(&self) -> &ClusterNodeId {
        match self {
            Self::Local(old) => old.selected_source(),
            #[cfg(feature = "tls-replication")]
            Self::Remote { certificate, .. } => certificate.selected_source_id(),
        }
    }
    fn source(&self) -> &NativeJointOldObservation {
        // Private construction checked exact complete membership; no mutation
        // can remove an owner afterwards. This is an actual retained object.
        self.old_observation(self.selected_source())
            .expect("sealed actual source owner")
    }
    pub(super) fn range_index(&self) -> u64 {
        match self {
            Self::Local(old) => old.range_index(),
            #[cfg(feature = "tls-replication")]
            Self::Remote { .. } => self.source().range_index(),
        }
    }
    pub(super) fn logical_genesis_sha256(&self) -> &str {
        match self {
            Self::Local(old) => old.logical_genesis_sha256(),
            #[cfg(feature = "tls-replication")]
            Self::Remote { .. } => self.source().logical_genesis_sha256(),
        }
    }
    pub(super) fn logical_history_sha256(&self) -> &str {
        match self {
            Self::Local(old) => old.logical_history_sha256(),
            #[cfg(feature = "tls-replication")]
            Self::Remote { .. } => self.source().logical_history_sha256(),
        }
    }
    pub(super) fn release_capture_pins_for_decision(&self) -> Result<()> {
        self.require_current()?;
        match self {
            Self::Local(old) => old.release_capture_pins_for_decision()?,
            #[cfg(feature = "tls-replication")]
            Self::Remote { recipients, .. } => {
                for actual in recipients.values() {
                    actual.release_capture_pin_for_decision()?;
                }
            }
        }
        self.require_current()
    }
}
