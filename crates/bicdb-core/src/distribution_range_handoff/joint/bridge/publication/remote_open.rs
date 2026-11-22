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
    window: RemotePreparationWindow,
}
impl RemoteOpenBacking {
    fn require_window(&self, local: &ClusterNodeId) -> Result<()> {
        if &self.window.observer != local || self.window.kind != NativeJointBridgePhaseKindV1::Open
        {
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
        if self.window.frames.len() + 1 != local.data().new_members().len() {
            return Err(invalid("remote open window omits a NEW member"));
        }
        self.window.require_live()?;
        let (term, index, context) = fresh.metadata_anchor();
        let mut anchors = vec![self.local_phase.metadata_anchor()];
        anchors.extend(
            self.window
                .frames
                .values()
                .map(|frame| frame.metadata_anchor()),
        );
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
        self.window
            .identities
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
        self.window.require_complete(data)
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
        // (1) Own pre-quorum OPEN observation; metadata I/O before guards.
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
        let (local_phase, local_writer) = {
            let mut service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("remote open phase actual service busy"))?;
            service.reconcile_native_joint_open_phase_with_point(
                &metadata,
                business,
                bridge,
                &self.actual,
                &phase,
                point.as_deref(),
            )?;
            let committed = self.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                data.old_range().id,
                point,
            )?;
            let writer = self.actual.require_current_under_open_preparation_custody(
                &service,
                &committed,
                bridge.clone(),
                self,
            )?;
            (committed, writer)
        };
        if local_phase.data() != data {
            return Err(invalid(
                "remote open phase changed protected global decision",
            ));
        }
        let phase = phase.child_with_deadline(local_phase.deadline());
        phase.check()?;
        // (2) Every other NEW member's own durable bridge, under its lock.
        let window = RemotePreparationWindow::collect(
            NativeJointBridgePhaseKindV1::Open,
            data,
            client,
            collector,
            collector_business,
            &phase,
        )?;
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
        let identities = window.identities.clone();
        let first_contexts = window.contexts();
        let post_quorum = fresh.metadata_anchor();
        let admission = NativeJointOpenAdmission {
            backing: OpenBacking::Remote(RemoteOpenBacking {
                local_phase,
                local_writer,
                window,
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
        // (4) The unchanged local activation: final foreign boundary, local
        // native scope, recover-or-persist, activated Open installation.
        let activation = Arc::new(admission.persist_local_catalogue_activation(parent)?);
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
