//! A NEW member persists its own catalogue activation when the other NEW
//! members live in other processes. Same construction rule as the remote
//! bridge publication: the local member is its actual retained preparation
//! and its own actual durable bridge, re-proved under its own service lock;
//! every other member is a frame the pinned transport minted after
//! authenticating that member in THIS member's own exchange, attesting that
//! member's own durable bridge under ITS lock.
use super::super::remote_publication::{
    observation_budget, NativeJointBridgeMemberIdentity, RemotePreparationWindow,
    REMOTE_PHASE_BUDGET,
};
use super::*;
use crate::distribution_range_handoff::NativeJointBridgePhaseKindV1;
use crate::distribution_transport::TcpClusterRelocationTransport;
use std::time::Instant;

/// Backing of a remote catalogue-activation admission. Private fields and no
/// constructor outside this file: it exists only inside
/// `NativeJointPreparationCustody::open_remote_activation`.
pub(super) struct RemoteOpenBacking {
    /// This member's own fresh pre-quorum OPEN observation and the writer
    /// registration it proved current under its own service lock.
    local_phase: CommittedNativeJointDecisionObservation,
    local_writer: crate::db::NativeJointPhaseOwner,
    /// `None` only for a recovery of this member's own existing activation
    /// (`recover_remote_activation`): such an admission can never write.
    window: Option<RemotePreparationWindow>,
    /// Every other NEW member's identity before the write: this admission's
    /// own window, or the deciding attempt's for a recovery.
    identities: BTreeMap<ClusterNodeId, NativeJointBridgeMemberIdentity>,
    /// Set once this member's activation commit point is passed (see
    /// `pass_commit_point`); every frame was live at that point.
    past_commit_point: std::sync::atomic::AtomicBool,
}
/// Refusal of a recovery-only Open that found no activation to recover.
pub(crate) const OPEN_RECOVERY_FOUND_NO_ACTIVATION: &str =
    "a recovery of this member's own catalogue activation found no activation to recover";
impl RemoteOpenBacking {
    fn window(&self) -> Result<&RemotePreparationWindow> {
        self.window
            .as_ref()
            .ok_or_else(|| invalid(OPEN_RECOVERY_FOUND_NO_ACTIVATION))
    }
    /// The decision to recover an existing exact activation: the commit
    /// point with a first window; a recovery-only admission has no frame to
    /// check, as an earlier attempt passed its own commit point.
    pub(super) fn pass_recovery_point(&self) -> Result<()> {
        if self.window.is_some() {
            return self.pass_commit_point();
        }
        self.past_commit_point
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
    pub(super) fn passed_commit_point(&self) -> bool {
        self.window.is_some()
            && self
                .past_commit_point
                .load(std::sync::atomic::Ordering::Acquire)
    }
    /// The commit point of this member's own catalogue activation: every
    /// other NEW member's frame must be live here, immediately before the
    /// activation is written (or, when an earlier attempt's exact activation
    /// is recovered, at the recovery decision). After it the frames gate
    /// nothing: what they attested decided only whether to write, the write
    /// is historical, and the activation stays unconfirmed until a SECOND
    /// window, observed after the write, reports every other member
    /// unchanged. This member's own fresh observations still bound every
    /// later check. Checking the first window again through the local
    /// proofs that follow the write refused a durable activation on every
    /// repetition once those proofs took longer than the frames lasted.
    pub(super) fn pass_commit_point(&self) -> Result<()> {
        self.window()?.require_live()?;
        self.past_commit_point
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
    fn require_window(&self, local: &ClusterNodeId) -> Result<()> {
        let Some(window) = &self.window else {
            return Ok(());
        };
        if &window.observer != local || window.kind != NativeJointBridgePhaseKindV1::Open {
            return Err(invalid(
                "remote open window was observed by another member/phase",
            ));
        }
        Ok(())
    }
    /// Liveness of every counted observation plus the in-process rule that
    /// the local post-quorum observation independently follows EVERY
    /// member's open-phase observation.
    pub(super) fn require_post_quorum(
        &self,
        local: &NativeJointPreparedServingObservation,
        bridge: &crate::db::NativeJointServingBridgeReceipt,
        fresh: &CommittedNativeJointDecisionObservation,
    ) -> Result<()> {
        self.local_phase.require_metadata_current()?;
        local
            .actual
            .require_phase_writer_lifetime(&self.local_writer)?;
        if let Some(custody) = &local.custody {
            custody.require_original_caller()?;
        }
        self.require_window(bridge.local_node_id())?;
        let (term, index, context) = fresh.metadata_anchor();
        let mut anchors = vec![self.local_phase.metadata_anchor()];
        if let Some(window) = &self.window {
            if window.frames.len() + 1 != local.data().new_members().len() {
                return Err(invalid("remote open window omits a NEW member"));
            }
            if !self
                .past_commit_point
                .load(std::sync::atomic::Ordering::Acquire)
            {
                window.require_live()?;
            }
            anchors.extend(window.frames.values().map(|frame| frame.metadata_anchor()));
        }
        for old in anchors {
            if term < old.0 || index < old.1 || context == old.2 {
                return Err(invalid(
                    "remote open local MQ is not independently post-all-member-quorum",
                ));
            }
        }
        Ok(())
    }
    /// The post-quorum anchor recorded in every OTHER member's own bridge.
    pub(super) fn bridge_anchors(&self) -> Result<Vec<(u64, u64, Uuid)>> {
        self.identities
            .values()
            .map(|identity| {
                identity
                    .bridge
                    .as_ref()
                    .map(|(_, anchor)| *anchor)
                    .ok_or_else(|| invalid("remote open frame attests no durable bridge"))
            })
            .collect()
    }
    /// The final foreign boundary before the local catalogue write.
    pub(super) fn require_external_current(
        &self,
        data: &NativeJointDecisionData,
        local: &ClusterNodeId,
    ) -> Result<()> {
        self.require_window(local)?;
        // A recovery-only admission trusts no other member and may only
        // recover an activation that already exists.
        match &self.window {
            Some(window) => window.require_complete(data),
            None => Ok(()),
        }
    }
    pub(super) fn require_local_native_scope(
        &self,
        local: &NativeJointPreparedServingObservation,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        scope: &crate::db::NativeJointBridgeReadScope<'_>,
    ) -> Result<()> {
        let writer =
            local.require_local_open_scope(scope, store, &self.local_phase, bridge.clone())?;
        if !writer.same_actual(&self.local_writer) {
            return Err(invalid(
                "remote open local native scope writer generation changed",
            ));
        }
        Ok(())
    }
    pub(super) fn require_local_with_service(
        &self,
        local: &NativeJointPreparedServingObservation,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
        service: &ClusterDataNodeService,
    ) -> Result<()> {
        let writer =
            local.require_open_phase_current(service, &self.local_phase, bridge.clone())?;
        if !writer.same_actual(&self.local_writer) {
            return Err(invalid(
                "remote open writer registration/generation changed",
            ));
        }
        Ok(())
    }
}

/// What one Open attempt's first window attested, retained once that attempt
/// passed its commit point. DATA for the second window; it grants nothing.
#[derive(Clone)]
pub(crate) struct NativeOpenFirstWindow {
    identities: BTreeMap<ClusterNodeId, NativeJointBridgeMemberIdentity>,
    first_contexts: std::collections::BTreeSet<Uuid>,
    post_quorum: (u64, u64, Uuid),
}
impl NativeOpenFirstWindow {
    /// The activation a second window confirms, for this member's activation
    /// retained after an earlier attempt's recovery or write and its
    /// post-write proofs completed.
    pub(crate) fn opened(
        &self,
        activation: Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
        activation_sha256: String,
    ) -> NativeRemoteBridgeOpened {
        NativeRemoteBridgeOpened {
            activation,
            activation_sha256,
            identities: self.identities.clone(),
            first_contexts: self.first_contexts.clone(),
            post_quorum: self.post_quorum,
        }
    }
}
/// This member's durable catalogue activation together with the identity
/// every other NEW member reported BEFORE the write. Not yet confirmed.
pub(crate) struct NativeRemoteBridgeOpened {
    activation: Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
    activation_sha256: String,
    identities: BTreeMap<ClusterNodeId, NativeJointBridgeMemberIdentity>,
    first_contexts: std::collections::BTreeSet<Uuid>,
    post_quorum: (u64, u64, Uuid),
}
impl NativeRemoteBridgeOpened {
    pub(crate) fn activation(
        &self,
    ) -> &Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt> {
        &self.activation
    }
    pub(crate) fn activation_sha256(&self) -> &str {
        &self.activation_sha256
    }
}

impl NativeJointPreparationCustody {
    fn require_remote_open_actor(
        &self,
        bridge: &crate::db::NativeJointServingBridgeReceipt,
        client: &TcpClusterRelocationTransport,
    ) -> Result<()> {
        let actor = client.local_metadata_read_participant();
        if actor.node_id() != self.local_node_id()
            || bridge.local_node_id() != self.local_node_id()
            || bridge.data() != self.data()
            || self
                .data()
                .new_members()
                .get(actor.node_id())
                .is_none_or(|member| member.incarnation != actor.member_incarnation())
        {
            return Err(invalid(
                "remote catalogue activation changed actual recipient/incarnation/bridge",
            ));
        }
        Ok(())
    }
    /// This member's own fresh pre-quorum OPEN observation: its own
    /// metadata observation and attached MStore point (both before any
    /// engine guard), then, under its own service lock, its own store
    /// uncertainty reconciled, the committed decision observed and the
    /// member proved current under its open preparation custody.
    fn observe_open_local_phase(
        self: &Arc<Self>,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        phase: &crate::CancellationToken,
    ) -> Result<(
        CommittedNativeJointDecisionObservation,
        crate::db::NativeJointPhaseOwner,
    )> {
        let started = Instant::now();
        let data = self.data();
        let business = Uuid::now_v7();
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            observation_budget(phase)?,
            phase,
        )?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.service,
            &metadata,
            business,
            phase,
        )?;
        crate::distribution_native_host::range_move_trace(started, || {
            "remote open local phase: own metadata".into()
        });
        let started = Instant::now();
        let mut service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("remote open phase actual service busy"))?;
        service.reconcile_native_joint_open_phase_with_point(
            &metadata,
            business,
            bridge,
            &self.actual,
            phase,
            point.as_deref(),
        )?;
        crate::distribution_native_host::range_move_trace(started, || {
            "remote open local phase: own store reconciled".into()
        });
        let started = Instant::now();
        let committed = self.actual.observe_fresh_committed_with_point(
            &service,
            metadata,
            business,
            data.old_range().id,
            point,
        )?;
        crate::distribution_native_host::range_move_trace(started, || {
            "remote open local phase: committed decision observed".into()
        });
        let started = Instant::now();
        let writer = self.actual.require_current_under_open_preparation_custody(
            &service,
            &committed,
            bridge.clone(),
            self,
        )?;
        crate::distribution_native_host::range_move_trace(started, || {
            "remote open local phase: member and bridge proved current".into()
        });
        Ok((committed, writer))
    }
    /// This member's own complete catalogue activation over its own durable
    /// `bridge`.
    ///
    /// The member resolves only its OWN store uncertainty, takes its own
    /// fresh pre-quorum OPEN observation under its service lock, collects one
    /// authenticated OPEN observation from EVERY other NEW member over its
    /// own pinned transport, takes a distinct post-quorum observation and
    /// then runs the unchanged local catalogue activation, which recovers an
    /// exact existing activation instead of rewriting it. `retain` receives
    /// the receipt before any later check; with `registered` the original
    /// Registered release owner is retained under the same live admission.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_remote_activation(
        self: &Arc<Self>,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        collector: &crate::distribution_consensus::HostMetadataReadObservation,
        collector_business: Uuid,
        registered: bool,
        parent: &crate::CancellationToken,
        retain: &mut dyn FnMut(
            &Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
        ),
        retain_first: &mut dyn FnMut(NativeOpenFirstWindow),
    ) -> Result<(
        NativeRemoteBridgeOpened,
        Option<crate::distribution_native_host::NativeRegisteredRemoteJointReleaseOwner>,
    )> {
        // A fresh request may not detach from the ORIGINAL session ancestry.
        let request = self.actual.bound_preparation_request(parent);
        request.check()?;
        self.require_original_caller()?;
        self.require_remote_open_actor(bridge, client)?;
        let data = self.data();
        let phase = request.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        let part = std::cell::Cell::new(Instant::now());
        let traced = |what: &str| {
            crate::distribution_native_host::range_move_trace(part.replace(Instant::now()), || {
                format!("remote open part: {what}")
            })
        };
        // (1) Own pre-quorum OPEN observation and (2) one OPEN observation
        // of every other NEW member, taken at the same time, as the remote
        // publication does. Both are pre-quorum observations of this open:
        // the post-quorum one below must follow every one of them
        // (`require_post_quorum`), and no order is required among them, as
        // the in-process phase observes all its members in one round. Taken
        // one after the other, the window started only after this member's
        // own reconcile and observation, which on a slow host took about
        // 0.7 s, and its frames then ran out before the activation was
        // written. The window's exchanges are bound to `collector`, not to
        // the local observation; the phase is bounded by the local
        // observation once both are in.
        let (local, window) = std::thread::scope(|scope| {
            let observe_local =
                || self.observe_open_local_phase(bridge, client, metadata_leader, &phase);
            let local = std::thread::Builder::new()
                .name("joint-open-local-phase".into())
                .spawn_scoped(scope, observe_local);
            let window = RemotePreparationWindow::collect(
                NativeJointBridgePhaseKindV1::Open,
                data,
                client,
                collector,
                collector_business,
                &phase,
            );
            let local = match local {
                Ok(worker) => worker
                    .join()
                    .unwrap_or_else(|_| Err(invalid("the open local phase worker panicked"))),
                Err(_) => observe_local(),
            };
            (local, window)
        });
        let (local_phase, local_writer) = local?;
        let window = window?;
        if local_phase.data() != data {
            return Err(invalid(
                "remote open phase changed protected global decision",
            ));
        }
        let phase = phase.child_with_deadline(local_phase.deadline());
        phase.check()?;
        window.require_live()?;
        traced("window");
        // (3) Distinct post-quorum observation, no guard across the I/O.
        let business = Uuid::now_v7();
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            observation_budget(&phase)?,
            &phase,
        )?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.service,
            &metadata,
            business,
            &phase,
        )?;
        let fresh = {
            let service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("remote open post-quorum service busy"))?;
            self.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                data.old_range().id,
                point,
            )?
        };
        traced("post-quorum observation");
        let identities = window.identities.clone();
        let first_contexts = window.contexts();
        self.activate_remote(
            bridge,
            local_phase,
            local_writer,
            Some(window),
            identities,
            first_contexts,
            fresh,
            &phase,
            registered,
            parent,
            retain,
            retain_first,
        )
    }
    /// The admission and the unchanged local activation of a remote Open:
    /// final foreign boundary, local native scope, recover-or-persist,
    /// activated Open installation, then the receipt retained, and with
    /// `registered` the original release owner retained under the same
    /// admission. With no `window` the admission may only recover.
    #[allow(clippy::too_many_arguments)]
    fn activate_remote(
        self: &Arc<Self>,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
        local_phase: CommittedNativeJointDecisionObservation,
        local_writer: crate::db::NativeJointPhaseOwner,
        window: Option<RemotePreparationWindow>,
        identities: BTreeMap<ClusterNodeId, NativeJointBridgeMemberIdentity>,
        first_contexts: std::collections::BTreeSet<Uuid>,
        fresh: CommittedNativeJointDecisionObservation,
        phase: &crate::CancellationToken,
        registered: bool,
        parent: &crate::CancellationToken,
        retain: &mut dyn FnMut(
            &Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
        ),
        retain_first: &mut dyn FnMut(NativeOpenFirstWindow),
    ) -> Result<(
        NativeRemoteBridgeOpened,
        Option<crate::distribution_native_host::NativeRegisteredRemoteJointReleaseOwner>,
    )> {
        let data = self.data();
        let post_quorum = fresh.metadata_anchor();
        let admission = NativeJointOpenAdmission {
            backing: OpenBacking::Remote(RemoteOpenBacking {
                local_phase,
                local_writer,
                window,
                identities: identities.clone(),
                past_commit_point: std::sync::atomic::AtomicBool::new(false),
            }),
            attempted: std::sync::atomic::AtomicBool::new(false),
            local: Arc::new(NativeJointPreparedServingObservation {
                actual: self.actual.clone(),
                service: self.service.clone(),
                fresh: None,
                custody: Some(self.clone()),
            }),
            receipt: bridge.clone(),
            cancellation: phase.child_with_deadline(fresh.deadline()),
            fresh,
        };
        admission.require_live()?;
        let started = Instant::now();
        let activation = admission.persist_local_catalogue_activation(parent);
        crate::distribution_native_host::range_move_trace(started, || {
            format!(
                "remote open part: local catalogue activation ok={}",
                activation.is_ok()
            )
        });
        // Whatever follows, a first window that passed its commit point
        // decided this member's activation; a later attempt that finds it
        // recovers it without asking anyone again.
        if admission.passed_commit_point() {
            retain_first(NativeOpenFirstWindow {
                identities: identities.clone(),
                first_contexts: first_contexts.clone(),
                post_quorum,
            });
        }
        let activation = Arc::new(activation?);
        retain(&activation);
        if activation.native().data() != data
            || activation.native_bridge_sha256() != bridge.checksum_sha256()
        {
            return Err(invalid(
                "remote catalogue activation changed decision or local native bridge",
            ));
        }
        let activation_sha256 = activation.activation_sha256()?;
        let release_owner = if registered {
            Some(admission.retain_registered_release_owner(activation.clone())?)
        } else {
            None
        };
        Ok((
            NativeRemoteBridgeOpened {
                activation,
                activation_sha256,
                identities,
                first_contexts,
                post_quorum,
            },
            release_owner,
        ))
    }
    /// A later attempt of an Open whose first window already passed its
    /// commit point (`first`): nothing here asks another member again. Under
    /// a full fresh phase the member takes its own pre-quorum and
    /// post-quorum observations and runs the unchanged local activation with
    /// an admission that may only RECOVER the exact existing activation;
    /// with none it refuses (`OPEN_RECOVERY_FOUND_NO_ACTIVATION`) and the
    /// caller starts over. The activation stays unconfirmed until a second
    /// window reports every other member unchanged against `first`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn recover_remote_activation(
        self: &Arc<Self>,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        first: &NativeOpenFirstWindow,
        registered: bool,
        parent: &crate::CancellationToken,
        retain: &mut dyn FnMut(
            &Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
        ),
    ) -> Result<(
        NativeRemoteBridgeOpened,
        Option<crate::distribution_native_host::NativeRegisteredRemoteJointReleaseOwner>,
    )> {
        let request = self.actual.bound_preparation_request(parent);
        request.check()?;
        self.require_original_caller()?;
        self.require_remote_open_actor(bridge, client)?;
        let data = self.data();
        let phase = request.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        let started = Instant::now();
        let (local_phase, local_writer) =
            self.observe_open_local_phase(bridge, client, metadata_leader, &phase)?;
        if local_phase.data() != data {
            return Err(invalid(
                "remote open recovery changed protected global decision",
            ));
        }
        let phase = phase.child_with_deadline(local_phase.deadline());
        phase.check()?;
        let business = Uuid::now_v7();
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            observation_budget(&phase)?,
            &phase,
        )?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.service,
            &metadata,
            business,
            &phase,
        )?;
        let fresh = {
            let service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("remote open recovery service busy"))?;
            self.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                data.old_range().id,
                point,
            )?
        };
        let anchor = fresh.metadata_anchor();
        if anchor.0 < first.post_quorum.0
            || anchor.1 < first.post_quorum.1
            || anchor.2 == first.post_quorum.2
        {
            return Err(invalid(
                "remote open recovery is not after the deciding post-quorum observation",
            ));
        }
        crate::distribution_native_host::range_move_trace(started, || {
            "remote open recovery: own observations".into()
        });
        self.activate_remote(
            bridge,
            local_phase,
            local_writer,
            None,
            first.identities.clone(),
            first.first_contexts.clone(),
            fresh,
            &phase,
            registered,
            parent,
            retain,
            &mut |_| {},
        )
    }
    /// The AFTER half of the remote activation: a second bounded OPEN window
    /// bound to a metadata observation taken after the durable write. Every
    /// other member must report exactly the identity and bridge it reported
    /// before the write; this member's own activation is re-read from its
    /// store. Any difference, refusal or timeout leaves it unconfirmed.
    pub(crate) fn confirm_remote_open_second_window(
        self: &Arc<Self>,
        opened: &NativeRemoteBridgeOpened,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        let request = self.actual.bound_preparation_request(parent);
        request.check()?;
        self.require_original_caller()?;
        self.require_remote_open_actor(bridge, client)?;
        let data = self.data();
        let phase = request.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        #[cfg(test)]
        let fault = super::super::remote_publication::test_take_second_window_fault(
            self.local_node_id(),
            data.checksum_sha256(),
        );
        #[cfg(test)]
        if fault
            == Some(crate::distribution_range_handoff::NativeBridgeSecondWindowFault::LostReply)
        {
            return Err(crate::BicDbError::QueryTimedOut);
        }
        let business = Uuid::now_v7();
        let own = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            observation_budget(&phase)?,
            &phase,
        )?;
        if own.term() < opened.post_quorum.0
            || own.read_index() < opened.post_quorum.1
            || own.context_id() == opened.post_quorum.2
        {
            return Err(invalid(
                "remote open second window is not after the post-quorum observation",
            ));
        }
        let window = RemotePreparationWindow::collect(
            NativeJointBridgePhaseKindV1::Open,
            data,
            client,
            &own,
            business,
            &phase,
        )?;
        #[allow(unused_mut)]
        let mut identities = window.identities.clone();
        #[cfg(test)]
        if fault
            == Some(
                crate::distribution_range_handoff::NativeBridgeSecondWindowFault::ChangedIdentity,
            )
        {
            let first = identities.values_mut().next().unwrap();
            first.writer_identity_sha256 = "f".repeat(64);
        }
        if identities != opened.identities {
            return Err(invalid(
                "remote open second window: a NEW member's identity/bridge changed across the durable write",
            ));
        }
        for frame in window.frames.values() {
            let (term, index, context) = frame.metadata_anchor();
            if term < opened.post_quorum.0
                || index < opened.post_quorum.1
                || context == opened.post_quorum.2
                || opened.first_contexts.contains(&context)
            {
                return Err(invalid(
                    "remote open second window reused or preceded first-window metadata",
                ));
            }
        }
        // This member's own durable activation, re-read from its own store.
        if self.durable_activation_sha256(bridge)?.as_deref() != Some(opened.activation_sha256()) {
            return Err(invalid(
                "remote open second window: own durable activation is absent or changed",
            ));
        }
        window.require_live()?;
        own.require_live_request(business)?;
        phase.check()
    }
    /// The checksum of this member's actual protected catalogue activation
    /// over its own `bridge`, read under its own service lock.
    pub(crate) fn durable_activation_sha256(
        &self,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
    ) -> Result<Option<String>> {
        self.require_original_caller()?;
        if bridge.data() != self.data() || bridge.local_node_id() != self.local_node_id() {
            return Err(invalid(
                "retained bridge receipt belongs to another decision/member",
            ));
        }
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("retained activation service busy"))?;
        service.durable_native_joint_activation_sha256(bridge.clone())
    }
}
