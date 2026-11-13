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

mod closed;
pub(crate) use closed::NativeJointOldObservation;

mod old_quorum;
pub(crate) use old_quorum::{
    NativeJointOldClosedQuorum, NativeJointOldImageProof, NativeJointOldRound,
};

mod materialized;
pub(crate) use materialized::{
    NativeJointMaterializedFacts, NativeJointMaterializedQuorums, NativeJointNewObservation,
    NativeJointNewRound,
};

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
    NativeJointReleasePhaseQuorum, NativeJointReleasePhaseRound,
    NativeJointServingPublicationRound,
};

#[cfg(test)]
mod trace;
#[cfg(test)]
pub(crate) use trace::{NativeJointPhaseTrace, NativeRefreshTraceContext};
