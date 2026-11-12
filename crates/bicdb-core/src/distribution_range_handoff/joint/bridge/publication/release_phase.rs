//! Current same-open continuation AFTER every protected native/catalogue
//! activation. Old preparation/MQ objects are not retained or made live.
use super::*;
use crate::distribution_data::{
    ActualServiceNativeJointReleaseEvidence, NativeJointImageReleaseOwner,
    NativeJointReleaseOwnerView,
};
use crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt;
use std::time::{Duration, Instant};

enum ReleaseOwner {
    Survivor(Arc<NativeJointHandoffRuntime>),
    Image(parking_lot::Mutex<NativeJointImageReleaseOwner>),
}
struct ReleaseMember {
    service: Arc<Mutex<ClusterDataNodeService>>,
    native: Arc<crate::db::NativeJointServingBridgeReceipt>,
    activation: Arc<NativeJointCatalogueActivationReceipt>,
    // Retain only the original registration, never the Prepared/Frozen owner.
    writer: Arc<crate::db::NativeWriterHostState>,
    writer_root: std::path::PathBuf,
    owner: ReleaseOwner,
}
/// Aggregated immutable actual facts and original caller provenance ONLY.
/// Construction gives no permission to release. No Prepared/Frozen Arc leaks
/// into this object; the real runtime/image owners retain their own leases.
pub(crate) struct NativeJointReleaseContinuation {
    caller: crate::CancellationToken,
    original_cancel: crate::CancellationToken,
    data: NativeJointDecisionData,
    members: BTreeMap<ClusterNodeId, ReleaseMember>,
}
pub(crate) struct NativeJointReleasePhaseRound {
    continuation: Arc<NativeJointReleaseContinuation>,
    cancellation: crate::CancellationToken,
    members: BTreeMap<ClusterNodeId, ActualServiceNativeJointReleaseEvidence>,
    poisoned: bool,
}
pub(crate) struct NativeJointReleasePhaseQuorum {
    continuation: Arc<NativeJointReleaseContinuation>,
    cancellation: crate::CancellationToken,
    members: BTreeMap<ClusterNodeId, ActualServiceNativeJointReleaseEvidence>,
    claimed: std::sync::atomic::AtomicBool,
}
/// One distinct post-all-member MQ and one release attempt. It never renews
/// the old receipt, supplies a Frozen or authorizes a different/new open.
pub(crate) struct NativeJointReleaseAdmission {
    phase: Arc<NativeJointReleasePhaseQuorum>,
    local: ClusterNodeId,
    post: ActualServiceNativeJointReleaseEvidence,
    cancellation: crate::CancellationToken,
    attempted: std::sync::atomic::AtomicBool,
}
impl NativeJointPublishedServingQuorum {
    pub(crate) fn retain_release_continuation(
        &self,
        activations: BTreeMap<ClusterNodeId, Arc<NativeJointCatalogueActivationReceipt>>,
        survivors: BTreeMap<ClusterNodeId, Arc<NativeJointHandoffRuntime>>,
        image: NativeJointImageReleaseOwner,
    ) -> Result<NativeJointReleaseContinuation> {
        self.prepared.require_original_caller()?;
        if self.receipts.keys().ne(self.data().new_members().keys())
            || activations.keys().ne(self.receipts.keys())
            || self.prepared.observations.keys().ne(self.receipts.keys())
        {
            return Err(invalid(
                "joint continuation needs every original final native and catalogue activation",
            ));
        }
        let additions: Vec<_> = self
            .data()
            .new_members()
            .keys()
            .filter(|n| !self.data().old_members().contains_key(*n))
            .cloned()
            .collect();
        if additions.len() != 1 {
            return Err(invalid(
                "joint release needs the actual single added image member",
            ));
        }
        let target = additions[0].clone();
        if survivors
            .keys()
            .any(|n| n == &target || !self.receipts.contains_key(n))
            || survivors.len() + 1 != self.receipts.len()
        {
            return Err(invalid(
                "joint continuation changed complete actual survivor/image owner set",
            ));
        }
        let mut image = Some(image);
        let mut members = BTreeMap::new();
        for (node, native) in &self.receipts {
            let activation = activations[node].clone();
            let service = self.prepared.observations[node].service.clone();
            let (writer, writer_root) = self.prepared.observations[node]
                .actual
                .original_writer_for_continuation()?;
            writer.require_store(&writer_root, activation.store_open_id())?;

            if activation.native_bridge_sha256() != native.checksum_sha256()
                || activation.native().native_database_incarnation()
                    != native.native_database_incarnation()
                || activation.native().data() != self.data()
                || activation.native().local_node_id() != node
            {
                return Err(invalid(
                    "joint continuation substituted actual activation/member/open",
                ));
            }
            let owner = if node == &target {
                let owner = image
                    .take()
                    .ok_or_else(|| invalid("joint image owner missing"))?;
                owner.require_same_service(&service)?;
                ReleaseOwner::Image(parking_lot::Mutex::new(owner))
            } else {
                let runtime = survivors
                    .get(node)
                    .ok_or_else(|| invalid("joint survivor owner missing"))?
                    .clone();
                runtime.require_same_service(&service)?;
                ReleaseOwner::Survivor(runtime)
            };
            members.insert(
                node.clone(),
                ReleaseMember {
                    service,
                    native: native.clone(),
                    activation,
                    writer,
                    writer_root,
                    owner,
                },
            );
        }
        Ok(NativeJointReleaseContinuation {
            caller: self.prepared.caller.child(),
            original_cancel: self.prepared.cancellation.child(),
            data: self.data().clone(),
            members,
        })
    }
}
impl NativeJointReleaseContinuation {
    fn require_original(&self) -> Result<()> {
        self.caller.check()?;
        for member in self.members.values() {
            member
                .writer
                .require_store(&member.writer_root, member.activation.store_open_id())?;
            member.writer.require_range(self.data.activation_range())?;
        }
        // ONLY old MQ expiry is historical. Explicit old cancellation and the
        // original caller's independent deadline remain effective forever.
        if self.original_cancel.is_cancel_requested() {
            return Err(crate::BicDbError::QueryCanceled);
        }
        Ok(())
    }
    fn with_owner<T>(
        &self,
        node: &ClusterNodeId,
        operation: impl for<'a> FnOnce(
            &Arc<Mutex<ClusterDataNodeService>>,
            NativeJointReleaseOwnerView<'a>,
        ) -> Result<T>,
    ) -> Result<T> {
        self.require_original()?;
        let member = self
            .members
            .get(node)
            .ok_or_else(|| invalid("joint release replaced final member"))?;
        match &member.owner {
            ReleaseOwner::Survivor(runtime) => {
                runtime.with_current_release_owner(&member.native, operation)
            }
            ReleaseOwner::Image(owner) => owner
                .try_lock()
                .ok_or_else(|| invalid("joint image release owner busy"))?
                .with_current_release_owner(&member.native, operation),
        }
    }
    #[cfg(test)]
    pub(crate) fn test_original_image_release(&self) -> Result<()> {
        self.require_original()?;
        for member in self.members.values() {
            if let ReleaseOwner::Image(owner) = &member.owner {
                return owner
                    .try_lock()
                    .ok_or_else(|| invalid("original image owner busy"))?
                    .release(&self.caller);
            }
        }
        Err(invalid("original actual image owner missing"))
    }
    pub(crate) fn begin_fresh_release_phase(
        self: &Arc<Self>,
        budget: Duration,
    ) -> Result<NativeJointReleasePhaseRound> {
        self.require_original()?;
        if budget.is_zero()
            || budget > Duration::from_secs(4)
            || self.members.keys().ne(self.data.new_members().keys())
            || !self
                .members
                .contains_key(&self.data.activation_range().leader)
        {
            return Err(invalid(
                "joint release requires bounded current authority and all final members/leader",
            ));
        }
        Ok(NativeJointReleasePhaseRound {
            continuation: self.clone(),
            cancellation: self.caller.child_with_deadline(Instant::now() + budget),
            members: BTreeMap::new(),
            poisoned: false,
        })
    }
}
fn remaining(token: &crate::CancellationToken) -> Result<Duration> {
    token.check()?;
    token
        .deadline()
        .and_then(|d| d.checked_duration_since(Instant::now()))
        .filter(|d| !d.is_zero())
        .ok_or(crate::BicDbError::QueryTimedOut)
}
impl NativeJointReleasePhaseRound {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn acquire_actual_member(
        &mut self,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        leader: &ClusterNodeId,
    ) -> Result<()> {
        if self.poisoned {
            return Err(invalid("joint release phase is poisoned"));
        }
        self.poisoned = true;
        self.continuation.require_original()?;
        let actor = client.local_metadata_read_participant();
        let node = actor.node_id();
        let member = self
            .continuation
            .members
            .get(node)
            .ok_or_else(|| invalid("joint release is not an original final member"))?;
        if self.members.contains_key(node)
            || self.continuation.data.new_members()[node].incarnation != actor.member_incarnation()
        {
            return Err(invalid("joint release duplicate/changed actual actor"));
        }
        let business = Uuid::now_v7();
        // No owner/service/database/journal lock is held during real MQ I/O.
        let host = client.acquire_host_metadata_observation(
            leader,
            business,
            remaining(&self.cancellation)?,
            &self.cancellation,
        )?;
        let evidence = self.continuation.with_owner(node, |service, owner| {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("joint release service busy"))?;
            let evidence = actual.observe_actual_joint_release_member(
                host,
                business,
                member.native.clone(),
                owner,
            )?;
            evidence.require_original_writer_identity(&member.writer)?;
            Ok(evidence)
        })?;
        self.cancellation = self
            .cancellation
            .child_with_deadline(evidence.committed().deadline());
        self.cancellation.check()?;
        self.members.insert(node.clone(), evidence);
        self.poisoned = false;
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeJointReleasePhaseQuorum> {
        self.cancellation.check()?;
        self.continuation.require_original()?;
        if self.poisoned || self.members.keys().ne(self.continuation.members.keys()) {
            return Err(invalid(
                "joint release phase omitted/replaced final participant",
            ));
        }
        let result = NativeJointReleasePhaseQuorum {
            continuation: self.continuation,
            cancellation: self.cancellation,
            members: self.members,
            claimed: std::sync::atomic::AtomicBool::new(false),
        };
        result.require_live()?;
        // The complete final actual sweep is done immediately before the
        // recipient transition, followed by local validation under its journal.
        Ok(result)
    }
}
impl NativeJointReleasePhaseQuorum {
    fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.continuation.require_original()?;
        for member in self.members.values() {
            member.require_live()?;
        }
        Ok(())
    }
    fn require_member_current(&self, node: &ClusterNodeId) -> Result<()> {
        self.require_live()?;
        self.continuation.with_owner(node, |service, owner| {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("joint release current service busy"))?;
            self.members[node].require_current(&actual, owner)
        })?;
        self.require_live()
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn acquire_local_release(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        leader: &ClusterNodeId,
    ) -> Result<NativeJointReleaseAdmission> {
        self.require_live()?;
        if self.claimed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Err(invalid("joint release phase already selected an attempt"));
        }
        let actor = client.local_metadata_read_participant();
        let node = actor.node_id().clone();
        let member = self
            .continuation
            .members
            .get(&node)
            .ok_or_else(|| invalid("joint release recipient omitted"))?;
        if self.continuation.data.new_members()[&node].incarnation != actor.member_incarnation() {
            return Err(invalid("joint release recipient incarnation changed"));
        }
        let business = Uuid::now_v7();
        let host = client.acquire_host_metadata_observation(
            leader,
            business,
            remaining(&self.cancellation)?,
            &self.cancellation,
        )?;
        let post = self.continuation.with_owner(&node, |service, owner| {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("joint release post-QQ service busy"))?;
            let evidence = actual.observe_actual_joint_release_member(
                host,
                business,
                member.native.clone(),
                owner,
            )?;
            evidence.require_original_writer_identity(&member.writer)?;
            Ok(evidence)
        })?;
        let (term, index, context) = post.committed().metadata_anchor();
        for observed in self.members.values() {
            let old = observed.committed().metadata_anchor();
            if term < old.0 || index < old.1 || context == old.2 {
                return Err(invalid(
                    "joint release needs independently post-all-member MQ",
                ));
            }
        }
        let result = NativeJointReleaseAdmission {
            cancellation: self
                .cancellation
                .child_with_deadline(post.committed().deadline()),
            phase: self.clone(),
            local: node,
            post,
            attempted: std::sync::atomic::AtomicBool::new(false),
        };
        result.require_live()?;
        Ok(result)
    }
}
impl NativeJointReleaseAdmission {
    pub(crate) fn native(&self) -> &crate::db::NativeJointServingBridgeReceipt {
        self.post.native()
    }
    pub(crate) fn activation(
        &self,
    ) -> &crate::distribution_range_consensus::NativeJointCatalogueActivationFact {
        self.post.activation()
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.phase.require_live()?;
        self.post.require_live()
    }
    pub(crate) fn require_recipient(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        native: &crate::db::NativeJointServingBridgeReceipt,
    ) -> Result<()> {
        self.require_live()?;
        let member = &self.phase.continuation.members[&self.local];
        if !Arc::ptr_eq(service, &member.service)
            || native.checksum_sha256() != member.native.checksum_sha256()
            || native.native_database_incarnation() != member.native.native_database_incarnation()
            || native.local_node_id() != &self.local
        {
            return Err(invalid(
                "joint release changed actual recipient service/open/native bridge",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_local_with_scope(
        &self,
        service: &ClusterDataNodeService,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
        owner: NativeJointReleaseOwnerView<'_>,
    ) -> Result<()> {
        self.require_live()?;
        self.phase.members[&self.local].require_current_with_scope(service, scope, owner)?;
        self.post
            .require_current_with_scope(service, scope, owner)?;
        self.require_live()
    }
    pub(crate) fn require_local_with_store_scope(
        &self,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
        owner: NativeJointReleaseOwnerView<'_>,
    ) -> Result<()> {
        self.require_live()?;
        self.phase.members[&self.local].require_current_with_store_scope(store, scope, owner)?;
        self.post
            .require_current_with_store_scope(store, scope, owner)?;
        self.require_live()
    }
    pub(crate) fn require_external_current(&self) -> Result<()> {
        self.require_live()?;
        for node in self.phase.members.keys() {
            if node != &self.local {
                self.phase.require_member_current(node)?;
            }
        }
        self.require_live()
    }
    pub(crate) fn release_actual_local(&self) -> Result<()> {
        self.require_live()?;
        if self
            .attempted
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(invalid("joint release admission already attempted"));
        }
        self.require_external_current()?;
        let member = &self.phase.continuation.members[&self.local];
        match &member.owner {
            ReleaseOwner::Survivor(runtime) => {
                runtime.release_with_fresh_phase(self, member.activation.clone())
            }
            ReleaseOwner::Image(owner) => owner
                .try_lock()
                .ok_or_else(|| invalid("joint release image owner busy"))?
                .release_with_fresh_phase(self),
        }
    }
    #[cfg(test)]
    pub(crate) fn test_release_after_actual_drop_error(&self) -> Result<()> {
        self.require_live()?;
        if self
            .attempted
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(invalid("joint release admission already attempted"));
        }
        self.require_external_current()?;
        let member = &self.phase.continuation.members[&self.local];
        match &member.owner {
            ReleaseOwner::Image(owner) => owner
                .try_lock()
                .ok_or_else(|| invalid("joint release image owner busy"))?
                .test_release_with_fresh_phase_after_drop(self),
            ReleaseOwner::Survivor(_) => Err(invalid("actual drop control requires image target")),
        }
    }
}
