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
/// The member's own metadata read audit is bounded where it is produced.
pub(crate) const MAX_STEP_WRITER_AUDIT_BYTES: usize = 256 * 1024;
/// A reply that carries that audit is bounded like the audit plus a reply.
const MAX_STEP_AUDIT_REPLY_BYTES: usize = MAX_STEP_WRITER_AUDIT_BYTES + MAX_STEP_REPLY_BYTES;
/// A retained host command failure is reported as bounded text DATA.
pub(crate) const MAX_STEP_HOST_COMMAND_ERROR_BYTES: usize = 1024;

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
    /// A member holding its own confirmed bridge persists its own catalogue
    /// activation. As for `BridgePublish`, the member itself collects fresh
    /// OPEN observations of every other NEW member before and after its own
    /// durable write; the request carries no observation, quorum or phase.
    BridgeOpen { decision_sha256: String },
    /// A surviving original member retires the obsolete OLD custody it still
    /// holds (its own OLD observation and, on the donor, the remote export
    /// certificate) after IT observed every other NEW member durably open in
    /// one bounded window of authenticated RELEASE observations. Absent
    /// custody is success; no source data is cleaned.
    RetireOldCustody { decision_sha256: String },
    /// A durably open member releases its own retained freeze and arms its
    /// own same-open generation. The member itself collects fresh RELEASE
    /// observations of every other NEW member before and after its release.
    BridgeRelease { decision_sha256: String },
    /// The RETIRED source (an original member absent from the final
    /// membership) records its own retirement from the range on its own
    /// committed metadata read, or confirms the record it already holds.
    /// No row is deleted and no fence is released.
    SourceRetire { decision_sha256: String },
    /// A released member publishes its own first ordinary writer DATA policy
    /// for the final membership. The member itself collects, before its own
    /// durable write, one authenticated WRITER AUDIT observation from every
    /// other NEW member (each released and armed, each carrying that
    /// member's own metadata read audit as DATA) and re-observes every other
    /// member afterwards. On the added member the transition belongs to its
    /// host thread: the step records the command and reports it pending.
    BridgeWriterPolicy { decision_sha256: String },
    /// The added member, holding its own confirmed writer policy, moves its
    /// retained first policy to its ordinary DATA socket and publishes its
    /// guarded ingress. Performed by its host thread; the step records the
    /// command and reports it pending until the host retained an outcome.
    TargetServe { decision_sha256: String },
}
impl NativeJointHandoffStepOperationV1 {
    /// The donor and target attempt of an install-family operation, which
    /// runs against the Pending plan; `None` for every bridge operation.
    fn install_identity(&self) -> Option<(&ClusterNodeId, Uuid)> {
        match self {
            Self::TargetInstall {
                donor,
                target_attempt,
            }
            | Self::SourceExportRetire {
                donor,
                target_attempt,
            } => Some((donor, *target_attempt)),
            Self::BridgePrepare { .. }
            | Self::BridgePublish { .. }
            | Self::BridgeOpen { .. }
            | Self::RetireOldCustody { .. }
            | Self::BridgeRelease { .. }
            | Self::SourceRetire { .. }
            | Self::BridgeWriterPolicy { .. }
            | Self::TargetServe { .. }
            | Self::BridgePhaseObserve { .. } => None,
        }
    }
    /// The committed decision a bridge operation is bound to; `None` for the
    /// operations that run against the Pending plan before any decision.
    pub(crate) fn bridge_decision_sha256(&self) -> Option<&str> {
        match self {
            Self::TargetInstall { .. } | Self::SourceExportRetire { .. } => None,
            Self::BridgePrepare { decision_sha256 }
            | Self::BridgePublish { decision_sha256 }
            | Self::BridgeOpen { decision_sha256 }
            | Self::RetireOldCustody { decision_sha256 }
            | Self::BridgeRelease { decision_sha256 }
            | Self::SourceRetire { decision_sha256 }
            | Self::BridgeWriterPolicy { decision_sha256 }
            | Self::TargetServe { decision_sha256 }
            | Self::BridgePhaseObserve {
                decision_sha256, ..
            } => Some(decision_sha256),
        }
    }
    /// Read-only operations never create or change recipient state.
    pub(crate) fn read_only(&self) -> bool {
        match self {
            Self::BridgePhaseObserve { .. } => true,
            Self::TargetInstall { .. }
            | Self::SourceExportRetire { .. }
            | Self::BridgePrepare { .. }
            | Self::BridgePublish { .. }
            | Self::BridgeOpen { .. }
            | Self::RetireOldCustody { .. }
            | Self::BridgeRelease { .. }
            | Self::BridgeWriterPolicy { .. }
            | Self::TargetServe { .. }
            | Self::SourceRetire { .. } => false,
        }
    }
    /// The added member's serving transition, as opposed to a writer policy.
    pub(crate) fn target_serve(&self) -> bool {
        matches!(self, Self::TargetServe { .. })
    }
}

/// Which retained member state a phase observation reports.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum NativeJointBridgePhaseKindV1 {
    Preparation,
    Open,
    Release,
    /// A RELEASE observation that additionally carries, as checksummed DATA
    /// of the reply, the member's own metadata read audit of the observation
    /// behind it, when the member is released and armed.
    WriterAudit,
}
impl NativeJointBridgePhaseKindV1 {
    /// Both kinds report the member's durable activation and release state.
    pub(crate) fn reports_release(self) -> bool {
        matches!(self, Self::Release | Self::WriterAudit)
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
    /// Bridge operations run after the membership is final, when no Pending
    /// plan exists any more. The identity comes from the committed decision
    /// the SENDER itself observed; the recipient compares it with its own.
    pub(crate) fn from_decision(
        data: &dyn NativeJointBridgeDecision,
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
            }
            | NativeJointHandoffStepOperationV1::SourceExportRetire {
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
            | NativeJointHandoffStepOperationV1::BridgeOpen { decision_sha256 }
            | NativeJointHandoffStepOperationV1::RetireOldCustody { decision_sha256 }
            | NativeJointHandoffStepOperationV1::BridgeRelease { decision_sha256 }
            | NativeJointHandoffStepOperationV1::SourceRetire { decision_sha256 }
            | NativeJointHandoffStepOperationV1::BridgeWriterPolicy { decision_sha256 }
            | NativeJointHandoffStepOperationV1::TargetServe { decision_sha256 }
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
            NativeJointHandoffStepOperationV1::TargetInstall { donor, .. }
            | NativeJointHandoffStepOperationV1::SourceExportRetire { donor, .. } => {
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
            | NativeJointHandoffStepOperationV1::BridgeOpen { .. }
            | NativeJointHandoffStepOperationV1::RetireOldCustody { .. }
            | NativeJointHandoffStepOperationV1::BridgeRelease { .. }
            | NativeJointHandoffStepOperationV1::SourceRetire { .. }
            | NativeJointHandoffStepOperationV1::BridgeWriterPolicy { .. }
            | NativeJointHandoffStepOperationV1::TargetServe { .. }
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
    pub(crate) fn require_decision(&self, data: &dyn NativeJointBridgeDecision) -> Result<()> {
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
    /// The member's catalogue activation is not confirmed; same resumption
    /// rule as `BridgePublication`.
    BridgeActivation,
    /// The member's release is not confirmed; same resumption rule as
    /// `BridgePublication`. The freeze may already be dropped and armed.
    BridgeRelease,
    /// The member's writer DATA policy is not confirmed; same resumption
    /// rule as `BridgePublication`. The durable policy may already exist.
    BridgeWriterPolicy,
    /// The step was accepted and recorded for the member's host thread,
    /// which has not retained a successful outcome for it yet.
    BridgeHostCommand,
    Complete,
}

/// What the retired source's own durable retirement record states. Reported
/// DATA: the completing metadata stage compares every field with the
/// committed relocation and takes the member from the authenticated peer.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJointSourceRetirementDataV1 {
    pub cluster_id: crate::distribution::ClusterId,
    pub node_id: ClusterNodeId,
    pub node_incarnation: u64,
    pub relocation_id: RelocationId,
    pub range_id: RangeId,
    pub session_id: Uuid,
    pub nonce: Uuid,
    pub activation_epoch: u64,
    pub decision_sha256: String,
    pub activated_quorum_sha256: String,
    /// Checksum of the source's own durable retirement record.
    pub checksum_sha256: String,
}
impl NativeJointSourceRetirementDataV1 {
    pub(crate) fn require_shape(&self) -> Result<()> {
        if self.node_id.as_str().is_empty()
            || self.node_id.as_str().len() > 1024
            || self.relocation_id.get() == 0
            || self.range_id.get() == 0
            || self.session_id.is_nil()
            || self.nonce.is_nil()
            || self.activation_epoch == 0
            || !is_digest(&self.decision_sha256)
            || !is_digest(&self.activated_quorum_sha256)
            || !is_digest(&self.checksum_sha256)
        {
            return Err(invalid("invalid joint source retirement DATA"));
        }
        Ok(())
    }
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
    /// The post-quorum metadata anchor recorded in that durable bridge.
    /// Present only on an OPEN phase observation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bridge_anchor: Option<(u64, u64, Uuid)>,
    /// The recipient's own durable catalogue activation. Present only on a
    /// complete `BridgeOpen` outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_sha256: Option<String>,
    /// RELEASE observation and complete `BridgeRelease` only: whether the
    /// recipient's own freeze is dropped AND its same-open generation armed,
    /// as its own fresh release observation saw it under its service lock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub released: Option<bool>,
    /// The armed same-open generation; present exactly when released.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub released_generation: Option<Uuid>,
    /// Complete `RetireOldCustody` only: the recipient holds no OLD custody
    /// of this decision any more.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_custody_retired: Option<bool>,
    /// Complete `SourceRetire` only, and the only member DATA of that
    /// outcome: the retired source prepared nothing for the new membership.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_retirement: Option<NativeJointSourceRetirementDataV1>,
    /// Complete `BridgeWriterPolicy` and `TargetServe` only: the recipient's
    /// own writer DATA policy for the final membership is installed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_policy_installed: Option<bool>,
    /// Complete `TargetServe` only: the ranges the recipient recovered as
    /// their leader when it published its guarded ingress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_recovered_ranges: Option<u64>,
    /// Pending host command only: the text of the failure its previous
    /// attempt on the host thread retained. The command is armed again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_command_error: Option<String>,
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
            bridge_anchor: None,
            activation_sha256: None,
            released: None,
            released_generation: None,
            old_custody_retired: None,
            source_retirement: None,
            writer_policy_installed: None,
            target_recovered_ranges: None,
            host_command_error: None,
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
    /// The retired source's complete outcome.
    pub(crate) fn source_retired(
        binding_sha256: String,
        decision_sha256: String,
        retirement: NativeJointSourceRetirementDataV1,
    ) -> Self {
        let mut result = Self::bridge_progress(
            binding_sha256,
            NativeJointHandoffStepStageV1::Complete,
            decision_sha256,
        );
        result.source_retirement = Some(retirement);
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
        let later_none = self.released.is_none() && self.old_custody_retired.is_none();
        let policy_family = matches!(
            operation,
            NativeJointHandoffStepOperationV1::BridgeWriterPolicy { .. }
                | NativeJointHandoffStepOperationV1::TargetServe { .. }
        );
        if (self.writer_policy_installed.is_some()
            || self.target_recovered_ranges.is_some()
            || self.host_command_error.is_some()
            || matches!(
                self.stage,
                NativeJointHandoffStepStageV1::BridgeWriterPolicy
                    | NativeJointHandoffStepStageV1::BridgeHostCommand
            ))
            && !policy_family
        {
            return Err(invalid(
                "joint handoff step outcome DATA answers another operation",
            ));
        }
        if self.source_retirement.is_some()
            != matches!(
                operation,
                NativeJointHandoffStepOperationV1::SourceRetire { .. }
            )
        {
            return Err(invalid(
                "joint handoff step outcome DATA answers another operation",
            ));
        }
        let matches = match operation {
            NativeJointHandoffStepOperationV1::SourceRetire { decision_sha256 } => {
                self.decision_sha256.as_ref() == Some(decision_sha256)
                    && later_none
                    && self.complete
                    && self.phase_kind.is_none()
                    && self
                        .source_retirement
                        .as_ref()
                        .is_some_and(|retirement| &retirement.decision_sha256 == decision_sha256)
            }
            NativeJointHandoffStepOperationV1::TargetInstall { .. }
            | NativeJointHandoffStepOperationV1::SourceExportRetire { .. } => {
                self.decision_sha256.is_none()
            }
            NativeJointHandoffStepOperationV1::BridgePrepare { decision_sha256 } => {
                self.decision_sha256.as_ref() == Some(decision_sha256)
                    && later_none
                    && self.phase_kind.is_none()
                    && self.bridge_receipt_sha256.is_none()
                    && self.activation_sha256.is_none()
                    && !matches!(
                        self.stage,
                        NativeJointHandoffStepStageV1::BridgePublication
                            | NativeJointHandoffStepStageV1::BridgeActivation
                            | NativeJointHandoffStepStageV1::BridgeRelease
                    )
            }
            NativeJointHandoffStepOperationV1::RetireOldCustody { decision_sha256 } => {
                self.decision_sha256.as_ref() == Some(decision_sha256)
                    && self.phase_kind.is_none()
                    && self.bridge_anchor.is_none()
                    && self.released.is_none()
                    && self.complete
                    && self.bridge_receipt_sha256.is_some()
                    && self.activation_sha256.is_some()
                    && self.old_custody_retired == Some(true)
            }
            NativeJointHandoffStepOperationV1::BridgeRelease { decision_sha256 } => {
                self.decision_sha256.as_ref() == Some(decision_sha256)
                    && self.phase_kind.is_none()
                    && self.bridge_anchor.is_none()
                    && self.old_custody_retired.is_none()
                    && self.bridge_receipt_sha256.is_some() == self.complete
                    && self.activation_sha256.is_some() == self.complete
                    && self.released == self.complete.then_some(true)
                    && matches!(
                        self.stage,
                        NativeJointHandoffStepStageV1::BridgeRelease
                            | NativeJointHandoffStepStageV1::Complete
                    )
            }
            NativeJointHandoffStepOperationV1::BridgeWriterPolicy { decision_sha256 }
            | NativeJointHandoffStepOperationV1::TargetServe { decision_sha256 } => {
                let serve = operation.target_serve();
                self.decision_sha256.as_ref() == Some(decision_sha256)
                    && self.phase_kind.is_none()
                    && self.bridge_anchor.is_none()
                    && self.old_custody_retired.is_none()
                    && self.bridge_receipt_sha256.is_some() == self.complete
                    && self.activation_sha256.is_some() == self.complete
                    && self.released == self.complete.then_some(true)
                    && self.writer_policy_installed == self.complete.then_some(true)
                    && self.target_recovered_ranges.is_some() == (self.complete && serve)
                    && (self.host_command_error.is_none()
                        || self.stage == NativeJointHandoffStepStageV1::BridgeHostCommand)
                    && match self.stage {
                        NativeJointHandoffStepStageV1::Complete
                        | NativeJointHandoffStepStageV1::BridgeHostCommand => true,
                        NativeJointHandoffStepStageV1::BridgeWriterPolicy => !serve,
                        _ => false,
                    }
            }
            NativeJointHandoffStepOperationV1::BridgeOpen { decision_sha256 } => {
                self.decision_sha256.as_ref() == Some(decision_sha256)
                    && later_none
                    && self.phase_kind.is_none()
                    && self.bridge_anchor.is_none()
                    && self.bridge_receipt_sha256.is_some() == self.complete
                    && self.activation_sha256.is_some() == self.complete
                    && matches!(
                        self.stage,
                        NativeJointHandoffStepStageV1::BridgeActivation
                            | NativeJointHandoffStepStageV1::Complete
                    )
            }
            NativeJointHandoffStepOperationV1::BridgePublish { decision_sha256 } => {
                self.decision_sha256.as_ref() == Some(decision_sha256)
                    && later_none
                    && self.phase_kind.is_none()
                    && self.bridge_anchor.is_none()
                    && self.activation_sha256.is_none()
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
                    && self.old_custody_retired.is_none()
                    && self.activation_sha256.is_some() == kind.reports_release()
                    && self.released.is_some() == self.activation_sha256.is_some()
                    && self.bridge_receipt_sha256.is_some()
                        != (*kind == NativeJointBridgePhaseKindV1::Preparation)
                    && self.bridge_anchor.is_some() == self.bridge_receipt_sha256.is_some()
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
                        | NativeJointHandoffStepStageV1::BridgeActivation
                        | NativeJointHandoffStepStageV1::BridgeRelease
                        | NativeJointHandoffStepStageV1::BridgeWriterPolicy
                        | NativeJointHandoffStepStageV1::BridgeHostCommand
                        | NativeJointHandoffStepStageV1::Complete
                )
                || (self.writer_policy_installed.is_some()
                    && (!complete || self.writer_policy_installed != Some(true)))
                || (self.target_recovered_ranges.is_some()
                    && self.writer_policy_installed.is_none())
                || self.host_command_error.as_deref().is_some_and(|text| {
                    complete || text.is_empty() || text.len() > MAX_STEP_HOST_COMMAND_ERROR_BYTES
                })
                || fields
                    .iter()
                    .any(|present| *present != (complete && self.source_retirement.is_none()))
                || self.source_retirement.as_ref().is_some_and(|retirement| {
                    !complete
                        || retirement.require_shape().is_err()
                        || self.bridge_receipt_sha256.is_some()
                        || self.old_custody_retired.is_some()
                        || self.phase_kind.is_some()
                })
                || (self.released.is_some() && self.activation_sha256.is_none())
                || self.released_generation.is_some() != (self.released == Some(true))
                || self.released_generation.is_some_and(|x| x.is_nil())
                || (self.old_custody_retired.is_some() && !complete)
                || (self.bridge_receipt_sha256.is_some() && !complete)
                || (self.bridge_anchor.is_some() && self.bridge_receipt_sha256.is_none())
                || (self.activation_sha256.is_some() && self.bridge_receipt_sha256.is_none())
                || self.bridge_anchor.is_some_and(|(term, index, context)| {
                    term == 0 || index == 0 || context.is_nil()
                })
                || self
                    .activation_sha256
                    .as_deref()
                    .is_some_and(|x| !is_digest(x))
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
            || self.bridge_anchor.is_some()
            || self.activation_sha256.is_some()
            || self.released.is_some()
            || self.released_generation.is_some()
            || self.old_custody_retired.is_some()
            || self.source_retirement.is_some()
            || self.writer_policy_installed.is_some()
            || self.target_recovered_ranges.is_some()
            || self.host_command_error.is_some()
            || self.phase_kind.is_some()
            || self.window_remaining_ms.is_some()
            || matches!(
                self.stage,
                NativeJointHandoffStepStageV1::BridgeWriterDomain
                    | NativeJointHandoffStepStageV1::BridgePreparation
                    | NativeJointHandoffStepStageV1::BridgePublication
                    | NativeJointHandoffStepStageV1::BridgeActivation
                    | NativeJointHandoffStepStageV1::BridgeRelease
                    | NativeJointHandoffStepStageV1::BridgeWriterPolicy
                    | NativeJointHandoffStepStageV1::BridgeHostCommand
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
    /// The recipient's original source manifest, returned by
    /// `SourceExportRetire` only. Historical DATA covered by the checksum:
    /// every member that later receives it compares it with its own facts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_manifest: Option<crate::distribution_range_image::NativeRemoteImageManifestV1>,
    /// The recipient's own metadata read audit of the observation behind
    /// this reply, returned by a WRITER AUDIT phase observation of a released
    /// member only. Historical DATA covered by the checksum: the member that
    /// receives it validates it completely and never treats it as authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_audit: Option<serde_json::Value>,
    pub checksum_sha256: String,
}
impl NativeJointHandoffStepReplyV1 {
    pub(crate) fn checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        // Only a reply that carries the larger DATA gets the larger bound.
        let bound = if value.source_manifest.is_some() {
            MAX_STEP_MANIFEST_REPLY_BYTES
        } else if value.writer_audit.is_some() {
            MAX_STEP_AUDIT_REPLY_BYTES
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
        self.outcome.require_operation(&request.operation)?;
        let audited = matches!(
            &request.operation,
            NativeJointHandoffStepOperationV1::BridgePhaseObserve {
                kind: NativeJointBridgePhaseKindV1::WriterAudit,
                ..
            }
        ) && self.outcome.released == Some(true);
        if self.writer_audit.is_some() != audited
            || self.writer_audit.as_ref().is_some_and(|audit| {
                !audit.is_object()
                    || crate::distribution_range_image::bounded_json(
                        audit,
                        MAX_STEP_WRITER_AUDIT_BYTES,
                    )
                    .is_err()
            })
        {
            return Err(invalid(
                "joint handoff step reply writer audit DATA differs from its operation",
            ));
        }
        match (request.operation.install_identity(), &self.source_manifest) {
            (_, None)
                if !matches!(
                    request.operation,
                    NativeJointHandoffStepOperationV1::SourceExportRetire { .. }
                ) => {}
            (Some((donor, target_attempt)), Some(manifest))
                if matches!(
                    request.operation,
                    NativeJointHandoffStepOperationV1::SourceExportRetire { .. }
                ) =>
            {
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
