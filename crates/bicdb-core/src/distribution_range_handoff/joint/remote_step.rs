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
    /// A NEW member of the committed decision prepares its own local joint
    /// serving and retains that preparation custody in its own host. The
    /// decision digest is correlation DATA; the recipient compares it with
    /// the decision it observes committed at its own metadata point.
    BridgePrepare { decision_sha256: String },
    /// Read-only. A NEW member makes its own fresh committed observation at
    /// its own metadata point and reports historical checksums of the state
    /// relevant to `kind`. Nothing is created, changed or granted.
    BridgePhaseObserve {
        decision_sha256: String,
        kind: NativeJointBridgePhaseKindV1,
    },
    /// A prepared NEW member publishes its own native serving bridge. The
    /// member itself collects fresh PREPARATION observations from every other
    /// NEW member over its own pinned transport, before and after its own
    /// durable write; the request carries no observation, quorum or phase.
    BridgePublish { decision_sha256: String },
}
impl NativeJointHandoffStepOperationV1 {
    /// The committed decision a bridge operation is bound to; `None` for the
    /// operations that run against the Pending plan before any decision.
    pub(crate) fn bridge_decision_sha256(&self) -> Option<&str> {
        match self {
            Self::TargetInstall { .. } => None,
            Self::BridgePrepare { decision_sha256 }
            | Self::BridgePublish { decision_sha256 }
            | Self::BridgePhaseObserve {
                decision_sha256, ..
            } => Some(decision_sha256),
        }
    }
    /// Read-only operations never create or change recipient state.
    pub(crate) fn read_only(&self) -> bool {
        matches!(self, Self::BridgePhaseObserve { .. })
    }
}

/// Which retained member state a phase observation reports.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum NativeJointBridgePhaseKindV1 {
    Preparation,
    Open,
    Release,
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
    /// Bridge operations run after the membership is final, when no Pending
    /// plan exists any more. The identity comes from the committed decision
    /// the SENDER itself observed; the recipient compares it with its own.
    pub(crate) fn from_decision(
        data: &NativeJointDecisionData,
        operation: NativeJointHandoffStepOperationV1,
        business: Uuid,
        budget_ms: u64,
    ) -> Result<Self> {
        let p = data.promise();
        let result = Self {
            version: 1,
            range_id: data.old_range().id,
            range_epoch: data.old_range().epoch,
            relocation_id: p.relocation_id,
            plan_id: p.plan_id.clone(),
            session_id: p.session_id,
            nonce: p.nonce,
            scope_sha256: data.plan_sha256()?,
            operation,
            business,
            challenge: Uuid::now_v7(),
            budget_ms,
        };
        result.require_decision(data)?;
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
        match &self.operation {
            NativeJointHandoffStepOperationV1::TargetInstall {
                donor,
                target_attempt,
            } => {
                if donor.as_str().is_empty()
                    || donor.as_str().len() > 1024
                    || target_attempt.is_nil()
                    || *target_attempt == self.session_id
                {
                    return Err(invalid(
                        "invalid bounded joint handoff target install identity",
                    ));
                }
            }
            NativeJointHandoffStepOperationV1::BridgePrepare { decision_sha256 }
            | NativeJointHandoffStepOperationV1::BridgePublish { decision_sha256 }
            | NativeJointHandoffStepOperationV1::BridgePhaseObserve {
                decision_sha256, ..
            } => {
                if !is_digest(decision_sha256) {
                    return Err(invalid(
                        "invalid bounded joint handoff bridge decision identity",
                    ));
                }
            }
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
        match &self.operation {
            NativeJointHandoffStepOperationV1::TargetInstall { donor, .. } => {
                // The donor's own receiver independently decides whether it
                // is the selected image source of its ALL-old certificate.
                if !plan.old_members().contains_key(donor) {
                    return Err(invalid(
                        "joint handoff step donor is not an actual OLD member",
                    ));
                }
            }
            NativeJointHandoffStepOperationV1::BridgePrepare { .. }
            | NativeJointHandoffStepOperationV1::BridgePublish { .. }
            | NativeJointHandoffStepOperationV1::BridgePhaseObserve { .. } => {
                // A Pending plan names no decision. Bridge operations are
                // bound only by `require_decision` to a committed decision.
                return Err(invalid(
                    "joint handoff bridge step is bound to a committed decision, not a Pending plan",
                ));
            }
        }
        Ok(())
    }
    /// The caller compares against the decision IT observed committed at its
    /// own metadata point; the request never supplies that decision.
    pub(crate) fn require_decision(&self, data: &NativeJointDecisionData) -> Result<()> {
        self.require_shape()?;
        let p = data.promise();
        if data.old_range().id != self.range_id
            || data.old_range().epoch != self.range_epoch
            || p.old_epoch != self.range_epoch
            || p.range_id != self.range_id
            || p.relocation_id != self.relocation_id
            || p.plan_id != self.plan_id
            || p.session_id != self.session_id
            || p.nonce != self.nonce
            || data.plan_sha256()? != self.scope_sha256
            || self.operation.bridge_decision_sha256() != Some(data.checksum_sha256())
        {
            return Err(invalid(
                "joint handoff bridge step differs from actual committed decision",
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
    /// A surviving member's closed writer DATA domain is not yet verified.
    BridgeWriterDomain,
    /// The writer domain is closed; the native preparation is not retained.
    BridgePreparation,
    /// The member's native bridge publication is not confirmed: either no
    /// durable write happened yet, or one happened and the member's second
    /// observation window did not confirm an unchanged NEW member set. The
    /// same request identity resumes it under a distinct fresh phase.
    BridgePublication,
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
    /// Present on every bridge outcome and on no install outcome: the
    /// decision the recipient itself observed committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_sha256: Option<String>,
    /// The recipient's own retained native preparation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preparation_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_database_incarnation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_visibility_sequence: Option<u64>,
    /// The recipient's own protected native decision receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_receipt_sha256: Option<String>,
    /// The recipient's actual Store slot open at the retained preparation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_open_id: Option<Uuid>,
    /// Digest of the recipient's exact writer/owner registration object: the
    /// identity an in-process observer compares by pointer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_identity_sha256: Option<String>,
    /// The recipient's own durable native serving bridge receipt. Present
    /// only on a complete `BridgePublish` outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bridge_receipt_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_kind: Option<NativeJointBridgePhaseKindV1>,
    /// What was left of the recipient's own fresh metadata observation when
    /// it replied. The receiver of the reply may only shorten its window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_remaining_ms: Option<u64>,
}
impl NativeJointHandoffStepOutcomeV1 {
    /// An install outcome with no bridge DATA.
    pub(crate) fn install(
        binding_sha256: String,
        stage: NativeJointHandoffStepStageV1,
        manifest: Option<(String, Uuid)>,
        floor: Option<(u64, String)>,
    ) -> Self {
        let (manifest_sha256, source_session) = manifest.unzip();
        let (range_index, native_receipt_sha256) = floor.unzip();
        Self {
            binding_sha256,
            stage,
            complete: stage == NativeJointHandoffStepStageV1::Complete,
            manifest_sha256,
            source_session,
            range_index,
            native_receipt_sha256,
            decision_sha256: None,
            preparation_sha256: None,
            native_database_incarnation: None,
            local_visibility_sequence: None,
            decision_receipt_sha256: None,
            store_open_id: None,
            writer_identity_sha256: None,
            bridge_receipt_sha256: None,
            phase_kind: None,
            window_remaining_ms: None,
        }
    }
    /// A bridge outcome that has not retained a preparation yet.
    pub(crate) fn bridge_progress(
        binding_sha256: String,
        stage: NativeJointHandoffStepStageV1,
        decision_sha256: String,
    ) -> Self {
        let mut result = Self::install(binding_sha256, stage, None, None);
        result.decision_sha256 = Some(decision_sha256);
        result
    }
    fn bridge_fields(&self) -> [bool; 6] {
        [
            self.preparation_sha256.is_some(),
            self.native_database_incarnation.is_some(),
            self.local_visibility_sequence.is_some(),
            self.decision_receipt_sha256.is_some(),
            self.store_open_id.is_some(),
            self.writer_identity_sha256.is_some(),
        ]
    }
    /// The outcome must be the kind the exact requested operation produces.
    pub(crate) fn require_operation(
        &self,
        operation: &NativeJointHandoffStepOperationV1,
    ) -> Result<()> {
        self.require_shape()?;
        let matches = match operation {
            NativeJointHandoffStepOperationV1::TargetInstall { .. } => {
                self.decision_sha256.is_none()
            }
            NativeJointHandoffStepOperationV1::BridgePrepare { decision_sha256 } => {
                self.decision_sha256.as_ref() == Some(decision_sha256)
                    && self.phase_kind.is_none()
                    && self.bridge_receipt_sha256.is_none()
                    && self.stage != NativeJointHandoffStepStageV1::BridgePublication
            }
            NativeJointHandoffStepOperationV1::BridgePublish { decision_sha256 } => {
                self.decision_sha256.as_ref() == Some(decision_sha256)
                    && self.phase_kind.is_none()
                    && self.bridge_receipt_sha256.is_some() == self.complete
                    && matches!(
                        self.stage,
                        NativeJointHandoffStepStageV1::BridgePublication
                            | NativeJointHandoffStepStageV1::Complete
                    )
            }
            NativeJointHandoffStepOperationV1::BridgePhaseObserve {
                decision_sha256,
                kind,
            } => {
                self.decision_sha256.as_ref() == Some(decision_sha256)
                    && self.phase_kind == Some(*kind)
                    && self.bridge_receipt_sha256.is_none()
                    && self.complete
            }
        };
        if !matches {
            return Err(invalid(
                "joint handoff step outcome DATA answers another operation",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_shape(&self) -> Result<()> {
        let complete = self.stage == NativeJointHandoffStepStageV1::Complete;
        if let Some(decision) = &self.decision_sha256 {
            let fields = self.bridge_fields();
            if !is_digest(&self.binding_sha256)
                || !is_digest(decision)
                || self.complete != complete
                || self.manifest_sha256.is_some()
                || self.source_session.is_some()
                || self.range_index.is_some()
                || self.native_receipt_sha256.is_some()
                || !matches!(
                    self.stage,
                    NativeJointHandoffStepStageV1::BridgeWriterDomain
                        | NativeJointHandoffStepStageV1::BridgePreparation
                        | NativeJointHandoffStepStageV1::BridgePublication
                        | NativeJointHandoffStepStageV1::Complete
                )
                || fields.iter().any(|present| *present != complete)
                || (self.bridge_receipt_sha256.is_some() && !complete)
                || self
                    .bridge_receipt_sha256
                    .as_deref()
                    .is_some_and(|x| !is_digest(x))
                || self
                    .writer_identity_sha256
                    .as_deref()
                    .is_some_and(|x| !is_digest(x))
                || self
                    .preparation_sha256
                    .as_deref()
                    .is_some_and(|x| !is_digest(x))
                || self
                    .decision_receipt_sha256
                    .as_deref()
                    .is_some_and(|x| !is_digest(x))
                || self
                    .native_database_incarnation
                    .as_deref()
                    .is_some_and(|x| x.is_empty() || x.len() > 256)
                || self.store_open_id.is_some_and(|x| x.is_nil())
                || self.phase_kind.is_some() != self.window_remaining_ms.is_some()
                || (self.phase_kind.is_some() && !complete)
                || self
                    .window_remaining_ms
                    .is_some_and(|left| !(1..=5000).contains(&left))
            {
                return Err(invalid(
                    "joint handoff bridge step outcome DATA is inconsistent",
                ));
            }
            return Ok(());
        }
        if self.bridge_fields().iter().any(|present| *present)
            || self.bridge_receipt_sha256.is_some()
            || self.phase_kind.is_some()
            || self.window_remaining_ms.is_some()
            || matches!(
                self.stage,
                NativeJointHandoffStepStageV1::BridgeWriterDomain
                    | NativeJointHandoffStepStageV1::BridgePreparation
                    | NativeJointHandoffStepStageV1::BridgePublication
            )
        {
            return Err(invalid("joint handoff step outcome DATA is inconsistent"));
        }
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
    pub checksum_sha256: String,
}
impl NativeJointHandoffStepReplyV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        Ok(hex::encode(sha2::Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("native-joint-handoff-step-reply-v1", value),
                MAX_STEP_REPLY_BYTES,
            )?,
        )))
    }
    pub(crate) fn require_request(&self, request: &NativeJointHandoffStepRequestV1) -> Result<()> {
        self.outcome.require_operation(&request.operation)?;
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
