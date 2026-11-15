//! Coordinator-triggered joint handoff steps. A request is identity DATA: it
//! names the committed Pending plan and one bounded operation the RECIPIENT
//! performs itself. No role, quorum, freeze, lease, manifest or certificate is
//! carried in a request or trusted from a reply; the recipient acquires its
//! own fresh Role (witnessed for the authenticated peer) and own metadata.
use super::*;
use crate::distribution::NativeStoreBirthV1;
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::CancellationToken;
use std::time::{Duration, Instant};

const MAX_STEP_REQUEST_BYTES: usize = 16 * 1024;
const MAX_STEP_REPLY_BYTES: usize = 64 * 1024;
/// A reply that returns the recipient's original source manifest DATA is
/// bounded like every other message that carries that manifest.
const MAX_STEP_MANIFEST_REPLY_BYTES: usize = 2 * 1024 * 1024 + MAX_STEP_REPLY_BYTES;

/// Operations are added by later phases (decision, bridge, serve). Each one
/// is an instruction for the recipient's own bounded work, never authority.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum NativeJointHandoffStepOperationV1 {
    /// The Registered target discovers the donor's image source, installs it
    /// and retains its own joint target owner. Resumable by `target_attempt`.
    TargetInstall {
        donor: ClusterNodeId,
        target_attempt: Uuid,
    },
    /// The Registered target, which retains the completed install of
    /// `target_attempt` and its own joint target owner, asks the donor itself
    /// to retire the completed export (the existing target-to-donor export
    /// retirement; the donor observes the target's owner over its own pinned
    /// transport). The reply returns the target's original source manifest as
    /// historical DATA. Repeatable: the donor answers its retained terminal.
    SourceExportRetire {
        donor: ClusterNodeId,
        target_attempt: Uuid,
    },
}
impl NativeJointHandoffStepOperationV1 {
    fn install_identity(&self) -> (&ClusterNodeId, Uuid) {
        match self {
            Self::TargetInstall {
                donor,
                target_attempt,
            }
            | Self::SourceExportRetire {
                donor,
                target_attempt,
            } => (donor, *target_attempt),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJointHandoffStepRequestV1 {
    pub version: u32,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub relocation_id: RelocationId,
    pub plan_id: String,
    pub session_id: Uuid,
    pub nonce: Uuid,
    pub scope_sha256: String,
    pub operation: NativeJointHandoffStepOperationV1,
    pub business: Uuid,
    pub challenge: Uuid,
    pub budget_ms: u64,
}
impl NativeJointHandoffStepRequestV1 {
    pub(crate) fn from_plan(
        plan: &NativeJointHandoffPlan,
        operation: NativeJointHandoffStepOperationV1,
        business: Uuid,
        budget_ms: u64,
    ) -> Result<Self> {
        let p = plan.promise();
        let result = Self {
            version: 1,
            range_id: plan.old_range().id,
            range_epoch: plan.old_range().epoch,
            relocation_id: p.relocation_id,
            plan_id: p.plan_id.clone(),
            session_id: p.session_id,
            nonce: p.nonce,
            scope_sha256: plan.checksum_sha256()?,
            operation,
            business,
            challenge: Uuid::now_v7(),
            budget_ms,
        };
        result.require_plan(plan)?;
        Ok(result)
    }
    pub(crate) fn require_shape(&self) -> Result<()> {
        crate::distribution_range_image::bounded_json(self, MAX_STEP_REQUEST_BYTES)?;
        if self.version != 1
            || self.range_id.get() == 0
            || self.relocation_id.get() == 0
            || self.range_epoch == 0
            || self.session_id.is_nil()
            || self.nonce.is_nil()
            || self.business.is_nil()
            || self.challenge.is_nil()
            || self.plan_id.is_empty()
            || self.plan_id.len() > 1024
            || !(1..=5000).contains(&self.budget_ms)
            || !is_digest(&self.scope_sha256)
        {
            return Err(invalid("invalid bounded joint handoff step request DATA"));
        }
        let (donor, target_attempt) = self.operation.install_identity();
        if donor.as_str().is_empty()
            || donor.as_str().len() > 1024
            || target_attempt.is_nil()
            || target_attempt == self.session_id
        {
            return Err(invalid(
                "invalid bounded joint handoff target install identity",
            ));
        }
        Ok(())
    }
    pub(crate) fn cancellation(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.require_shape()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(self.budget_ms));
        token.check()?;
        Ok(token)
    }
    pub(crate) fn purpose(&self) -> MetadataRecoveryRolePurpose {
        MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id: self.relocation_id,
            plan_id: self.plan_id.clone(),
            session_id: self.session_id,
            nonce: self.nonce,
        }
    }
    /// The recipient compares against the plan built from ITS OWN committed
    /// metadata observation; the request never supplies that plan.
    pub(crate) fn require_plan(&self, plan: &NativeJointHandoffPlan) -> Result<()> {
        self.require_shape()?;
        let p = plan.promise();
        if plan.old_range().id != self.range_id
            || plan.old_range().epoch != self.range_epoch
            || p.relocation_id != self.relocation_id
            || p.plan_id != self.plan_id
            || p.session_id != self.session_id
            || p.nonce != self.nonce
            || plan.checksum_sha256()? != self.scope_sha256
        {
            return Err(invalid(
                "joint handoff step request differs from actual Pending plan",
            ));
        }
        // The donor's own receiver independently decides whether it is the
        // selected image source of its ALL-old certificate.
        if !plan
            .old_members()
            .contains_key(self.operation.install_identity().0)
        {
            return Err(invalid(
                "joint handoff step donor is not an actual OLD member",
            ));
        }
        Ok(())
    }
    /// Identity that must not change across retries of one logical step.
    /// Business, challenge and budget are per-request correlation only.
    pub(crate) fn binding(&self) -> Result<String> {
        self.require_shape()?;
        digest(&(
            "native-joint-handoff-step-binding-v1",
            self.version,
            self.range_id,
            self.range_epoch,
            self.relocation_id,
            &self.plan_id,
            self.session_id,
            self.nonce,
            &self.scope_sha256,
            &self.operation,
        ))
    }
}

/// Progress DATA. `Complete` reports that the recipient retained its own
/// outcome; it is not a decision, activation, Open or Serving grant.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum NativeJointHandoffStepStageV1 {
    Discovering,
    Preparing,
    Proposing,
    Installing,
    History,
    HistoryAudit,
    Rows,
    Verifying,
    FinalImage,
    NativeBase,
    CatalogueFloor,
    JointTarget,
    Complete,
}

/// Retained once by the recipient and returned unchanged to every retry of
/// the same logical step. Historical identity only.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJointHandoffStepOutcomeV1 {
    pub binding_sha256: String,
    pub stage: NativeJointHandoffStepStageV1,
    pub complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_session: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_receipt_sha256: Option<String>,
}
impl NativeJointHandoffStepOutcomeV1 {
    pub(crate) fn require_shape(&self) -> Result<()> {
        let complete = self.stage == NativeJointHandoffStepStageV1::Complete;
        if !is_digest(&self.binding_sha256)
            || self.complete != complete
            || self
                .manifest_sha256
                .as_deref()
                .is_some_and(|x| !is_digest(x))
            || self
                .native_receipt_sha256
                .as_deref()
                .is_some_and(|x| !is_digest(x))
            || self.source_session.is_some_and(|x| x.is_nil())
            || self.manifest_sha256.is_some() != self.source_session.is_some()
            || (complete
                && (self.manifest_sha256.is_none()
                    || self.range_index.is_none()
                    || self.native_receipt_sha256.is_none()))
            || (self.stage == NativeJointHandoffStepStageV1::Discovering
                && self.manifest_sha256.is_some())
        {
            return Err(invalid("joint handoff step outcome DATA is inconsistent"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJointHandoffStepReplyV1 {
    pub request: NativeJointHandoffStepRequestV1,
    pub node_id: ClusterNodeId,
    pub birth: NativeStoreBirthV1,
    pub outcome: NativeJointHandoffStepOutcomeV1,
    pub metadata_term: u64,
    pub metadata_read_index: u64,
    pub metadata_context_id: Uuid,
    /// The recipient's original source manifest, returned by
    /// `SourceExportRetire` only. Historical DATA covered by the checksum:
    /// every member that later receives it compares it with its own facts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_manifest: Option<crate::distribution_range_image::NativeRemoteImageManifestV1>,
    pub checksum_sha256: String,
}
impl NativeJointHandoffStepReplyV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        let bound = if value.source_manifest.is_some() {
            MAX_STEP_MANIFEST_REPLY_BYTES
        } else {
            MAX_STEP_REPLY_BYTES
        };
        Ok(hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("native-joint-handoff-step-reply-v1", value),
                bound,
            )?,
        )))
    }
    pub(crate) fn require_request(&self, request: &NativeJointHandoffStepRequestV1) -> Result<()> {
        self.outcome.require_shape()?;
        let (donor, target_attempt) = request.operation.install_identity();
        match (&request.operation, &self.source_manifest) {
            (NativeJointHandoffStepOperationV1::TargetInstall { .. }, None) => {}
            (NativeJointHandoffStepOperationV1::SourceExportRetire { .. }, Some(manifest)) => {
                manifest.require_shape()?;
                if !self.outcome.complete
                    || self.outcome.manifest_sha256.as_ref() != Some(&manifest.checksum_sha256)
                    || self.outcome.source_session != Some(manifest.source_session)
                    || &manifest.source_id != donor
                    || manifest.target_attempt != target_attempt
                    || manifest.target_id != self.node_id
                    || manifest.capture_range.id != request.range_id
                    || manifest.capture_range.epoch != request.range_epoch
                    || manifest.relocation_id != request.relocation_id
                    || manifest.handoff_session != request.session_id
                    || manifest.nonce != request.nonce
                {
                    return Err(invalid(
                        "joint handoff step reply returns another source manifest",
                    ));
                }
            }
            _ => {
                return Err(invalid(
                    "joint handoff step reply manifest DATA differs from its operation",
                ))
            }
        }
        if self.request != *request
            || self.outcome.binding_sha256 != request.binding()?
            || self.metadata_term == 0
            || self.metadata_read_index == 0
            || self.metadata_context_id.is_nil()
            || self.checksum_sha256 != self.checksum()?
        {
            return Err(invalid(
                "joint handoff step reply changes exact request/current observation",
            ));
        }
        Ok(())
    }
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(hex::encode(sha2::Sha256::digest(
        crate::distribution_range_image::bounded_json(value, MAX_STEP_REQUEST_BYTES)?,
    )))
}
