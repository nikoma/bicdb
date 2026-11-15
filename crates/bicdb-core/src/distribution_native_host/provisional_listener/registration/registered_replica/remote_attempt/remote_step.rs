//! Coordinator-triggered target install. The authenticated peer supplies only
//! identity DATA; THIS original Registered receiver witnesses its own fresh
//! Role for that peer, observes its own metadata, fetches the donor's source
//! itself over its pinned transport and runs the same target-local operations
//! the owning host runs. Progress is retained before any reply is built.
use super::*;
use crate::db::{
    NativeRegisteredRemoteImageOperation as ImageOp,
    NativeRegisteredRemoteImageProgress as Progress,
};
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_range_handoff::{
    NativeJointHandoffPlan, NativeJointHandoffStepOperationV1 as StepOperation,
    NativeJointHandoffStepOutcomeV1, NativeJointHandoffStepReplyV1,
    NativeJointHandoffStepRequestV1, NativeJointHandoffStepStageV1 as Stage,
};
use crate::distribution_range_image::{
    NativeRemoteImageManifestV1, NativeRemoteImageSourceOperationV1 as SourceOp,
    NativeRemoteImageSourceRequestV1, NativeSourceDiscoveryOperationV1 as DiscoveryOp,
    NativeSourceDiscoveryRequestV1, RangeImageCursorV1, RangeImageHistoryCursorV1,
};
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use crate::distribution_transport::AuthenticatedNativeRemoteImageSourceFrame;
use std::time::Duration;

/// A further sub-step starts only with this much of the request left. The
/// first sub-step of a call always runs so every call can make progress.
const MIN_SUBSTEP_BUDGET: Duration = Duration::from_millis(1500);
const HISTORY_STEP_ENTRIES: usize = 256;
const VERIFY_STEP_RECORDS: usize = 4096;

/// Memory-only driver position for one target install attempt. The durable
/// facts and the actual owners stay where the target-local operations keep
/// them; this only remembers which bounded operation comes next.
pub(in super::super) struct RegisteredTargetInstallStep {
    binding_sha256: String,
    /// The committed relocation (id, session, nonce) this attempt was
    /// authorized for by the receiver's own Role and metadata.
    relocation: (crate::RelocationId, Uuid, Uuid),
    donor: ClusterNodeId,
    target_attempt: Uuid,
    discovery_started: bool,
    source: Option<Arc<NativeRemoteImageFact>>,
    stage: Stage,
    history_begun: bool,
    history: RangeImageHistoryCursorV1,
    rows: RangeImageCursorV1,
    verification_begun: bool,
    catalogue_floor: Option<(u64, String)>,
    /// Retained once at completion and returned unchanged to every retry.
    completed: Option<NativeJointHandoffStepOutcomeV1>,
}
impl RegisteredTargetInstallStep {
    fn outcome(&self) -> NativeJointHandoffStepOutcomeV1 {
        if let Some(completed) = &self.completed {
            return completed.clone();
        }
        let manifest = self.source.as_ref().map(|source| source.manifest());
        NativeJointHandoffStepOutcomeV1 {
            binding_sha256: self.binding_sha256.clone(),
            stage: self.stage,
            complete: false,
            manifest_sha256: manifest.map(|m| m.checksum_sha256.clone()),
            source_session: manifest.map(|m| m.source_session),
            range_index: None,
            native_receipt_sha256: None,
        }
    }
}

/// Fresh recipient authority for one bounded sub-step. Never retained across
/// sub-steps and never derived from request DATA.
struct StepAuthority {
    role: RecoveryRoleAdmission,
    own: HostMetadataReadObservation,
    business: Uuid,
    plan: NativeJointHandoffPlan,
}

fn remaining(token: &CancellationToken) -> Result<Duration> {
    token.check()?;
    token
        .deadline()
        .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
        .filter(|left| !left.is_zero())
        .ok_or(BicDbError::QueryTimedOut)
}

impl NativeRegisteredReplicaAdmission {
    /// The SAME initiator rule as every other handoff mutation: the metadata
    /// leader's own witness accepts the peer only as range leader or metadata
    /// leader of the committed Pending plan. Nothing here widens that rule.
    fn acquire_joint_step_authority(
        &self,
        backend: &NativeMetadataControlBackend,
        peer: &MetadataReadParticipant,
        request: &NativeJointHandoffStepRequestV1,
        token: &CancellationToken,
    ) -> Result<StepAuthority> {
        token.check()?;
        request.require_shape()?;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let local = backend.transport.local_metadata_read_participant();
        let purpose = request.purpose();
        let witness = backend.provider.witness_recovery_role(
            peer,
            business,
            request.range_id,
            request.range_epoch,
            request.nonce,
            challenge,
            purpose.clone(),
            token,
        )?;
        let role = RecoveryRoleAdmission::from_witness(
            witness,
            peer,
            local.node_id(),
            business,
            request.range_id,
            request.range_epoch,
            request.nonce,
            challenge,
            &purpose,
        )?;
        let leader = backend
            .provider
            .current_metadata_leader_hint(token)?
            .ok_or_else(|| invalid("joint handoff step current metadata leader unavailable"))?;
        let own = backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            remaining(token)?.min(Duration::from_secs(5)),
            token,
        )?;
        role.require_own_host_metadata(&own, business)?;
        let topology = own.committed_topology(business)?;
        let actor = topology
            .nodes
            .get(peer.node_id())
            .ok_or_else(|| invalid("joint handoff step authenticated actor absent"))?;
        if peer.plaintext_loopback()
            || actor.lifecycle != ClusterNodeLifecycle::Active
            || actor.incarnation != peer.member_incarnation()
            || peer
                .certificate_sha256()
                .is_none_or(|pin| !actor.accepts_tls_certificate_sha256(pin))
        {
            return Err(invalid(
                "joint handoff step actual peer/credential differs from committed member",
            ));
        }
        // The plan comes from this receiver's own Role and committed metadata.
        let plan = NativeJointHandoffPlan::from_actual_role_and_own(&role, &own, business)?;
        request.require_plan(&plan)?;
        let relocation = topology
            .relocations
            .get(&request.relocation_id)
            .ok_or_else(|| invalid("joint handoff step relocation absent from own metadata"))?;
        let birth = plan.enrolled_birth(local.node_id())?;
        if &relocation.target != local.node_id()
            || relocation.range_id != request.range_id
            || birth.birth_id() != self.birth_id
            || birth.birth_sha256() != self.birth_sha256
        {
            return Err(invalid(
                "joint handoff step names another target than this original receiver",
            ));
        }
        token.check()?;
        Ok(StepAuthority {
            role,
            own,
            business,
            plan,
        })
    }

    /// The target fetches the donor frame itself under its own fresh metadata.
    fn fetch_joint_step_frame(
        backend: &NativeMetadataControlBackend,
        manifest: &NativeRemoteImageManifestV1,
        operation: SourceOp,
        token: &CancellationToken,
    ) -> Result<AuthenticatedNativeRemoteImageSourceFrame> {
        let leader = backend
            .provider
            .current_metadata_leader_hint(token)?
            .ok_or_else(|| invalid("joint handoff step frame metadata leader unavailable"))?;
        let business = Uuid::now_v7();
        let own = backend.transport.acquire_host_metadata_observation(
            &leader,
            business,
            remaining(token)?.min(Duration::from_secs(5)),
            token,
        )?;
        let request = NativeRemoteImageSourceRequestV1 {
            source_session: manifest.source_session,
            target_attempt: manifest.target_attempt,
            manifest_sha256: manifest.checksum_sha256.clone(),
            business,
            challenge: Uuid::now_v7(),
            budget_ms: remaining(token)?.as_millis().clamp(1, 4000) as u64,
            operation,
        };
        backend
            .transport
            .request_remote_image_source_frame(manifest, &request, &own, business, token)
    }

    /// One bounded step of the coordinator-triggered target install. Repeated
    /// calls with the same identity continue where the previous one stopped;
    /// a completed attempt returns its retained outcome unchanged.
    pub(crate) fn advance_joint_handoff_step(
        self: &Arc<Self>,
        peer: &MetadataReadParticipant,
        request: &NativeJointHandoffStepRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeJointHandoffStepReplyV1> {
        let token = request.cancellation(parent)?;
        self.require_live()?;
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("joint handoff step original Registered backend ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("joint handoff step original Registered runtime ended"))?;
        self.require_listener(&backend.service, &runtime)?;
        let install = self
            .remote_install
            .upgrade()
            .ok_or_else(|| invalid("joint handoff step original install custody ended"))?;
        let joint_targets = self
            .joint_targets
            .upgrade()
            .ok_or_else(|| invalid("joint handoff step original owner table ended"))?;
        let factory = self
            .factory
            .upgrade()
            .ok_or_else(|| invalid("joint handoff step original target factory ended"))?;
        let StepOperation::TargetInstall {
            donor,
            target_attempt,
        } = &request.operation;
        let binding_sha256 = request.binding()?;
        // Authenticate and verify against OWN committed metadata before any
        // retained state is created, read or disclosed.
        let first = self.acquire_joint_step_authority(&backend, peer, request, &token)?;
        let mut observed = (
            first.own.term(),
            first.own.read_index(),
            first.own.context_id(),
        );
        let local = backend.transport.local_metadata_read_participant();
        if donor == local.node_id() {
            return Err(invalid("joint handoff step donor is this target"));
        }
        let range = request.range_id;
        let mut held = install.hold()?;
        // Read under the custody this step holds until it returns: the host
        // sets it only while holding the same custody.
        if held.data_owner_present() {
            return Err(invalid(
                "joint handoff step cannot replace an existing DATA owner",
            ));
        }
        let RegisteredRemoteInstallTables {
            remote_attempts,
            remote_images,
            remote_steps,
            ..
        } = &mut *held;
        // The plan `first` built from this receiver's own fresh Role and
        // committed metadata names the range's CURRENT committed relocation.
        let current = {
            let promise = first.plan.promise();
            (promise.relocation_id, promise.session_id, promise.nonce)
        };
        let foreign_owner = joint_targets
            .try_lock()
            .ok_or_else(|| invalid("joint handoff step owner table busy"))?
            .contains_key(&range);
        let superseded = match remote_steps.get(&range) {
            Some(retained) if retained.binding_sha256 != binding_sha256 => {
                // Preparation is the first operation that retains anything
                // beyond this entry (the attempt and its transferred freeze,
                // in memory); the proposal after it is the first durable
                // write. Until the attempt exists the entry is only a driver
                // position and a fresh authorized step may replace it: the
                // donor or attempt id was wrong or lost, or the relocation it
                // was started for is no longer the committed one.
                let only_position = retained.stage <= Stage::Preparing
                    && !foreign_owner
                    && !remote_attempts.contains_key(&range)
                    && !remote_images.contains_key(&range);
                if !only_position {
                    if retained.relocation != current {
                        // No product path discards a retained install attempt
                        // or its durable Installing state; never invent one.
                        return Err(invalid(&format!(
                            "joint handoff step retains install custody of stale relocation {} \
                             (session {}), which is no longer this range's committed relocation; \
                             this original receiver cannot discard it",
                            retained.relocation.0.get(),
                            retained.relocation.1
                        )));
                    }
                    return Err(invalid(
                        "joint handoff step retains another exact target install attempt",
                    ));
                }
                true
            }
            _ => false,
        };
        if superseded {
            remote_steps.remove(&range);
        }
        if !remote_steps.contains_key(&range) {
            if foreign_owner || remote_attempts.contains_key(&range) {
                return Err(invalid(
                    "joint handoff step cannot adopt install custody it did not start",
                ));
            }
            remote_steps.insert(
                range,
                RegisteredTargetInstallStep {
                    binding_sha256: binding_sha256.clone(),
                    relocation: current,
                    donor: donor.clone(),
                    target_attempt: *target_attempt,
                    discovery_started: false,
                    source: None,
                    stage: Stage::Discovering,
                    history_begun: false,
                    history: RangeImageHistoryCursorV1::default(),
                    rows: RangeImageCursorV1::default(),
                    verification_begun: false,
                    catalogue_floor: None,
                    completed: None,
                },
            );
        }
        let step = remote_steps.get_mut(&range).unwrap();
        let mut custody = RegisteredRemoteInstallCustody {
            // Verified absent above, under the custody held until return.
            data_owner: false,
            backend: backend.clone(),
            runtime: runtime.clone(),
            admission: self.clone(),
            remote_joint_targets: joint_targets.clone(),
            remote_attempts,
            remote_images,
        };
        let mut authority = Some(first);
        let mut first_substep = true;
        while step.completed.is_none() {
            if !first_substep && remaining(&token)? < MIN_SUBSTEP_BUDGET {
                break;
            }
            first_substep = false;
            self.drive_target_install(
                &backend,
                &mut custody,
                step,
                peer,
                request,
                authority.take(),
                &mut observed,
                &token,
            )?;
        }
        drop(authority);
        let outcome = step.outcome();
        drop(custody);
        drop(held);
        self.require_listener(&backend.service, &runtime)?;
        let mut reply = NativeJointHandoffStepReplyV1 {
            request: request.clone(),
            node_id: local.node_id().clone(),
            birth: factory.actual_birth().clone(),
            outcome,
            metadata_term: observed.0,
            metadata_read_index: observed.1,
            metadata_context_id: observed.2,
            checksum_sha256: String::new(),
        };
        reply.checksum_sha256 = reply.checksum()?;
        reply.require_request(request)?;
        token.check()?;
        Ok(reply)
    }

    /// Exactly one bounded target-local operation. Every operation acquires
    /// its own fresh Role/OWN and its own authenticated donor frame, and is
    /// the same retry-safe operation the owning host invokes in-process.
    #[allow(clippy::too_many_arguments)]
    fn drive_target_install(
        &self,
        backend: &NativeMetadataControlBackend,
        custody: &mut RegisteredRemoteInstallCustody<'_>,
        step: &mut RegisteredTargetInstallStep,
        peer: &MetadataReadParticipant,
        request: &NativeJointHandoffStepRequestV1,
        mut prepared: Option<StepAuthority>,
        observed: &mut (u64, u64, Uuid),
        token: &CancellationToken,
    ) -> Result<()> {
        let mut authority = || -> Result<StepAuthority> {
            let fresh = match prepared.take() {
                Some(fresh)
                    if fresh.role.require_live_request().is_ok()
                        && fresh.own.require_live_request(fresh.business).is_ok() =>
                {
                    fresh
                }
                _ => self.acquire_joint_step_authority(backend, peer, request, token)?,
            };
            *observed = (
                fresh.own.term(),
                fresh.own.read_index(),
                fresh.own.context_id(),
            );
            Ok(fresh)
        };
        let local = backend.transport.local_metadata_read_participant();
        if step.stage == Stage::Discovering {
            let fresh = authority()?;
            let operation = if step.discovery_started {
                DiscoveryOp::Describe
            } else {
                DiscoveryOp::Start
            };
            let discovery = NativeSourceDiscoveryRequestV1::from_plan(
                &fresh.plan,
                step.donor.clone(),
                local.node_id().clone(),
                step.target_attempt,
                operation,
                &fresh.own,
                fresh.business,
                remaining(token)?.as_millis().clamp(1, 4000) as u64,
            )?;
            let actual = backend.transport.request_remote_image_source_discovery(
                &fresh.plan,
                &discovery,
                &fresh.own,
                token,
            )?;
            if !step.discovery_started {
                // The donor retained its capture; Describe then returns the
                // original session's manifest for this exact attempt.
                step.discovery_started = true;
                return Ok(());
            }
            let source = Arc::new(NativeRemoteImageFact::from_actual_discovery(actual)?);
            if source.manifest().target_attempt != step.target_attempt
                || source.manifest().source_id != step.donor
            {
                return Err(invalid(
                    "joint handoff step discovered another donor/attempt",
                ));
            }
            // Retained before any target mutation uses it; every later
            // operation requires this identical original fact.
            step.source = Some(source);
            step.stage = Stage::Preparing;
            return Ok(());
        }
        let source = step
            .source
            .clone()
            .ok_or_else(|| invalid("joint handoff step lost its original discovered source"))?;
        let range = request.range_id;
        let history_frame = |cursor: &RangeImageHistoryCursorV1| SourceOp::History {
            cursor: cursor.clone(),
            max_entries: HISTORY_STEP_ENTRIES,
        };
        match step.stage {
            Stage::Discovering | Stage::Complete => unreachable!("handled by the caller"),
            Stage::Preparing => {
                let transferred = custody.remote_attempts.get(&range).is_some_and(|retained| {
                    retained.lease.is_some() && Arc::ptr_eq(retained.preflight.source(), &source)
                });
                if !transferred {
                    let frame = source.bind_step(Self::fetch_joint_step_frame(
                        backend,
                        source.manifest(),
                        history_frame(&RangeImageHistoryCursorV1::default()),
                        token,
                    )?)?;
                    let fresh = authority()?;
                    custody.prepare_registered_remote_install_attempt(
                        source.clone(),
                        &frame,
                        fresh.role.into_handoff()?,
                        fresh.own,
                        fresh.business,
                        token,
                    )?;
                }
                step.stage = Stage::Proposing;
            }
            Stage::Proposing => {
                let frame = source.bind_step(Self::fetch_joint_step_frame(
                    backend,
                    source.manifest(),
                    history_frame(&RangeImageHistoryCursorV1::default()),
                    token,
                )?)?;
                let fresh = authority()?;
                custody.publish_registered_remote_proposal(
                    &source,
                    &frame,
                    fresh.role.into_handoff()?,
                    fresh.own,
                    fresh.business,
                    token,
                )?;
                step.stage = Stage::Installing;
            }
            Stage::Installing => {
                let frame = source.bind_step(Self::fetch_joint_step_frame(
                    backend,
                    source.manifest(),
                    history_frame(&RangeImageHistoryCursorV1::default()),
                    token,
                )?)?;
                let fresh = authority()?;
                custody.publish_registered_remote_installing(
                    &source,
                    &frame,
                    fresh.role.into_handoff()?,
                    fresh.own,
                    fresh.business,
                    token,
                )?;
                step.stage = Stage::History;
            }
            Stage::History => {
                let frame = source.bind_step(Self::fetch_joint_step_frame(
                    backend,
                    source.manifest(),
                    history_frame(&step.history),
                    token,
                )?)?;
                let fresh = authority()?;
                let Progress::History(progress) = custody.advance_registered_remote_image(
                    &source,
                    &frame,
                    fresh.role.into_handoff()?,
                    fresh.own,
                    fresh.business,
                    token,
                    if step.history_begun {
                        ImageOp::AppendHistory
                    } else {
                        ImageOp::BeginHistory
                    },
                )?
                else {
                    return Err(invalid("joint handoff step expected history progress"));
                };
                step.history_begun = true;
                step.history = progress.cursor;
                if step.history.complete() {
                    step.stage = Stage::HistoryAudit;
                }
            }
            Stage::HistoryAudit => {
                let frame = source.bind_step(Self::fetch_joint_step_frame(
                    backend,
                    source.manifest(),
                    history_frame(&RangeImageHistoryCursorV1::default()),
                    token,
                )?)?;
                let fresh = authority()?;
                let Progress::History(progress) = custody.advance_registered_remote_image(
                    &source,
                    &frame,
                    fresh.role.into_handoff()?,
                    fresh.own,
                    fresh.business,
                    token,
                    ImageOp::AuditHistory {
                        max_entries: HISTORY_STEP_ENTRIES,
                    },
                )?
                else {
                    return Err(invalid("joint handoff step expected history audit"));
                };
                if progress.audited {
                    step.stage = Stage::Rows;
                }
            }
            Stage::Rows => {
                let frame = source.bind_step(Self::fetch_joint_step_frame(
                    backend,
                    source.manifest(),
                    SourceOp::Rows {
                        cursor: step.rows.clone(),
                        options: Default::default(),
                    },
                    token,
                )?)?;
                let fresh = authority()?;
                let Progress::Rows(progress) = custody.advance_registered_remote_image(
                    &source,
                    &frame,
                    fresh.role.into_handoff()?,
                    fresh.own,
                    fresh.business,
                    token,
                    ImageOp::ApplyRows,
                )?
                else {
                    return Err(invalid("joint handoff step expected row progress"));
                };
                step.rows = progress.cursor;
                if progress.complete {
                    step.stage = Stage::Verifying;
                }
            }
            Stage::Verifying | Stage::FinalImage | Stage::NativeBase | Stage::CatalogueFloor => {
                let frame = source.bind_step(Self::fetch_joint_step_frame(
                    backend,
                    source.manifest(),
                    SourceOp::Finish,
                    token,
                )?)?;
                let fresh = authority()?;
                let operation = match step.stage {
                    Stage::Verifying if !step.verification_begun => ImageOp::BeginVerification,
                    Stage::Verifying => ImageOp::VerifyTarget {
                        max_scanned_records: VERIFY_STEP_RECORDS,
                    },
                    Stage::FinalImage => ImageOp::PublishFinalImage,
                    Stage::NativeBase => ImageOp::PublishNativeBase,
                    _ => ImageOp::AdoptCatalogueFloor,
                };
                let progress = custody.advance_registered_remote_image(
                    &source,
                    &frame,
                    fresh.role.into_handoff()?,
                    fresh.own,
                    fresh.business,
                    token,
                    operation,
                )?;
                match (step.stage, progress) {
                    (Stage::Verifying, Progress::Verification(progress)) => {
                        step.verification_begun = true;
                        if progress.scan_complete() {
                            step.stage = Stage::FinalImage;
                        }
                    }
                    (Stage::FinalImage, Progress::FinalImage(_)) => {
                        step.stage = Stage::NativeBase;
                    }
                    (Stage::NativeBase, Progress::NativeBase { .. }) => {
                        step.stage = Stage::CatalogueFloor;
                    }
                    (
                        Stage::CatalogueFloor,
                        Progress::CatalogueFloor {
                            range_index,
                            native_receipt_sha256,
                            ..
                        },
                    ) => {
                        step.catalogue_floor = Some((range_index, native_receipt_sha256));
                        step.stage = Stage::JointTarget;
                    }
                    _ => {
                        return Err(invalid(
                            "joint handoff step target operation returned another progress",
                        ))
                    }
                }
            }
            Stage::JointTarget => {
                let retained = custody
                    .remote_joint_targets
                    .try_lock()
                    .ok_or_else(|| invalid("joint handoff step owner table busy"))?
                    .get(&range)
                    .cloned();
                let owner = if let Some(owner) = retained {
                    // An earlier call transferred the lease and then failed a
                    // postcheck. Resume that original owner; never a second.
                    owner
                        .service()
                        .try_lock()
                        .ok_or_else(|| invalid("joint handoff step owner service busy"))?
                        .retain_registered_joint_store_identity(&owner)?;
                    owner
                } else {
                    let frame = source.bind_step(Self::fetch_joint_step_frame(
                        backend,
                        source.manifest(),
                        SourceOp::Finish,
                        token,
                    )?)?;
                    let fresh = authority()?;
                    custody.take_registered_remote_joint_target(
                        &source,
                        &frame,
                        fresh.role.into_handoff()?,
                        fresh.own,
                        fresh.business,
                        token,
                    )?
                };
                if !Arc::ptr_eq(owner.original_target_arc().original_source(), &source) {
                    return Err(invalid(
                        "joint handoff step retained owner has another original source",
                    ));
                }
                let (range_index, native_receipt_sha256) =
                    step.catalogue_floor.clone().ok_or_else(|| {
                        invalid("joint handoff step lost its adopted catalogue floor")
                    })?;
                // The owner stays in this receiver's own table for the later
                // decision step. Only historical identity leaves the process.
                step.stage = Stage::Complete;
                step.completed = Some(NativeJointHandoffStepOutcomeV1 {
                    binding_sha256: step.binding_sha256.clone(),
                    stage: Stage::Complete,
                    complete: true,
                    manifest_sha256: Some(source.manifest().checksum_sha256.clone()),
                    source_session: Some(source.manifest().source_session),
                    range_index: Some(range_index),
                    native_receipt_sha256: Some(native_receipt_sha256),
                });
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn test_take_joint_handoff_step_reply_loss(&self) -> bool {
        self.remote_install
            .upgrade()
            .is_some_and(|install| install.test_take_step_reply_loss())
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    pub(crate) fn test_drop_next_joint_handoff_step_reply(&self) -> Result<()> {
        self.replica
            .as_ref()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?
            .remote_install
            .test_drop_next_step_reply();
        Ok(())
    }
    /// Hold the install custody from another thread exactly as a receiver
    /// step does for its whole duration, until the returned value is dropped.
    pub(crate) fn test_hold_registered_install_step_in_flight(
        &self,
    ) -> Result<TestRegisteredInstallStepInFlight> {
        let install = self.registered_remote_install()?;
        let (ready, held) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            let custody = install.hold();
            let _ = ready.send(custody.is_ok());
            let _ = wait.recv();
            drop(custody);
        });
        if !held.recv().unwrap_or(false) {
            return Err(invalid("test step custody was not acquired"));
        }
        Ok(TestRegisteredInstallStepInFlight {
            release: Some(release),
            thread: Some(thread),
        })
    }
    /// Memory-only driver position; grants nothing and reads no native state.
    pub(crate) fn test_joint_handoff_step_state(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        let receiver = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?;
        let held = receiver.remote_install.hold()?;
        Ok(match held.remote_steps.get(&range) {
            None => serde_json::Value::Null,
            Some(step) => serde_json::json!({
                "binding_sha256":step.binding_sha256,"donor":step.donor,
                "relocation":step.relocation.0,"session":step.relocation.1,
                "target_attempt":step.target_attempt,"stage":step.stage,
                "discovery_started":step.discovery_started,
                "manifest_sha256":step.source.as_ref().map(|s|s.manifest().checksum_sha256.clone()),
                "completed":step.completed,"memory_only":true,
            }),
        })
    }
}

#[cfg(test)]
pub(crate) struct TestRegisteredInstallStepInFlight {
    release: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
#[cfg(test)]
impl Drop for TestRegisteredInstallStepInFlight {
    fn drop(&mut self) {
        drop(self.release.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
