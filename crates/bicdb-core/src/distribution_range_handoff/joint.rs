//! Native joint-data handoff. Old/new sets are independent data quorums;
//! metadata correlation and decoded audit projections confer no capability.
use super::*;
use sha2::Digest;
fn invalid(detail: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("native joint data handoff: {}", detail.into()))
}
mod scope;
use scope::{canonical_range, NativeJointScopeData};
mod plan;
pub(crate) use plan::NativeJointHandoffPlan;
#[cfg(feature = "tls-replication")]
pub(crate) use plan::NativeJointReadOnlyPlanIdentity;

mod closed;
pub(crate) use closed::NativeJointOldObservation;

mod old_quorum;
#[cfg(feature = "tls-replication")]
mod remote_old;
#[cfg(feature = "tls-replication")]
pub(crate) use remote_old::{
    NativeRemoteOldOperationV1, NativeRemoteOldReplyV1, NativeRemoteOldRequestV1,
};
#[cfg(feature = "tls-replication")]
mod remote_old_collect;
pub(crate) use old_quorum::{
    NativeJointOldClosedQuorum, NativeJointOldImageProof, NativeJointOldRound,
};
#[cfg(feature = "tls-replication")]
pub(crate) use remote_old_collect::{
    NativeRemoteOldCutCertificate, NativeRemoteOldImageProof, NativeRemoteOldRound,
};

mod decision_owners;
use decision_owners::NativeJointOldDecisionOwners;
mod materialized;
pub(crate) use materialized::{
    NativeJointMaterializedFacts, NativeJointMaterializedQuorums, NativeJointNewObservation,
    NativeJointNewRound,
};
#[cfg(feature = "tls-replication")]
mod remote_new;
#[cfg(feature = "tls-replication")]
pub(crate) use remote_new::{
    NativeRemoteDecisionAdmission, NativeRemoteDecisionBindingV1, NativeRemoteDecisionReceiptV1,
    NativeRemoteDecisionSourceData, NativeRemoteNewFactSetV1, NativeRemoteNewHandoffV1,
    NativeRemoteNewMaterializedQuorum, NativeRemoteNewOperationV1, NativeRemoteNewParticipant,
    NativeRemoteNewPinHandoffPermit, NativeRemoteNewReplyV1, NativeRemoteNewRequestV1,
    NativeRemoteNewRound, NativeRemoteNewSnapshotV1, NativeRemoteNewTargetPinIssuance,
};

#[cfg(feature = "tls-replication")]
pub(crate) use decision::NativeRegisteredDecisionCatalogueRetry;
mod decision;
pub(crate) use decision::{
    NativeJointDecisionData, NativeJointDecisionPublicationAdmission,
    NativeJointDecisionPublicationRound, NativeJointDurableDecision, NativeJointPreparedDecision,
};

mod runtime;
pub(crate) use runtime::NativeJointHandoffRuntime;

mod committed;
pub(crate) use committed::CommittedNativeJointDecisionObservation;

mod bridge;
pub(crate) use bridge::{
    NativeJointActivationAdmission, NativeJointOpenAdmission, NativeJointOpenPhaseQuorum,
    NativeJointOpenPhaseRound, NativeJointPreparationPhaseQuorum, NativeJointPreparationPhaseRound,
    NativeJointPreparedServingObservation, NativeJointPreparedServingQuorum,
    NativeJointPreparedServingRound, NativeJointPreparedServingVoteData,
    NativeJointPublishedServingQuorum, NativeJointReleaseAdmission, NativeJointReleaseContinuation,
    NativeJointReleasePhaseQuorum, NativeJointReleasePhaseRound, NativeJointServingPolicyAdmission,
    NativeJointServingPublicationRound,
};

#[cfg(test)]
mod trace;
#[cfg(test)]
pub(crate) use trace::{NativeJointPhaseTrace, NativeRefreshTraceContext};
