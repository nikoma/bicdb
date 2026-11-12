//! NEW-data preparation quorum after the actual OLD+NEW decision was durably
//! published and committed in metadata. These are real service/native-WAL
//! facts, not generic Ready replies or decoded membership/voter lists.
use super::*;
use crate::distribution_data::{ActualServiceNativeJointPreparedEvidence, ClusterDataNodeService};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) struct NativeJointPreparedServingObservation {
    actual: ActualServiceNativeJointPreparedEvidence,
    service: Arc<Mutex<ClusterDataNodeService>>,
}
impl NativeJointPreparedServingObservation {
    pub(crate) fn from_actual_service(
        actual: ActualServiceNativeJointPreparedEvidence,
        service: Arc<Mutex<ClusterDataNodeService>>,
    ) -> Result<Self> {
        let result = Self { actual, service };
        result.require_current()?;
        Ok(result)
    }
    fn require_current(&self) -> Result<()> {
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("joint bridge service busy"))?;
        self.actual.require_current(&service)
    }
    fn data(&self) -> &NativeJointDecisionData {
        self.actual.committed().data()
    }
    fn local_node_id(&self) -> &ClusterNodeId {
        self.actual.prepared().local_node_id()
    }
    fn release_read_pin(&self) -> Result<()> {
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("joint bridge pin service busy"))?;
        self.actual.release_read_pin(&service)
    }
}

pub(crate) struct NativeJointPreparedServingRound {
    data: NativeJointDecisionData,
    observations: BTreeMap<ClusterNodeId, Arc<NativeJointPreparedServingObservation>>,
    cancellation: crate::CancellationToken,
    poisoned: bool,
}
impl NativeJointPreparedServingRound {
    /// The initial scope comes only from a real prepared local service fact.
    /// There is no constructor accepting an intended range or decoded body.
    pub(crate) fn begin(
        first: NativeJointPreparedServingObservation,
        parent: &crate::CancellationToken,
    ) -> Result<Self> {
        first.require_current()?;
        let cancellation = parent.child_with_deadline(first.actual.committed().deadline());
        cancellation.check()?;
        let mut round = Self {
            data: first.data().clone(),
            observations: BTreeMap::new(),
            cancellation,
            poisoned: false,
        };
        round.record(first)?;
        Ok(round)
    }
    pub(crate) fn record(
        &mut self,
        observation: NativeJointPreparedServingObservation,
    ) -> Result<()> {
        if self.poisoned {
            return Err(invalid("joint bridge preparation round is poisoned"));
        }
        let result = self.record_inner(observation);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn record_inner(&mut self, observation: NativeJointPreparedServingObservation) -> Result<()> {
        self.cancellation.check()?;
        observation.require_current()?;
        let prepared = observation.actual.prepared();
        if observation.data() != &self.data
            || !self
                .data
                .new_members()
                .contains_key(observation.local_node_id())
            || prepared.decision_sha256() != self.data.checksum_sha256()
            || prepared.range_index() != self.data.range_index()
            || prepared.logical_genesis_sha256() != self.data.logical_genesis_sha256()
            || prepared.logical_history_sha256() != self.data.logical_history_sha256()
            || self.observations.contains_key(observation.local_node_id())
        {
            return Err(invalid(
                "joint bridge NEW prepared member/point conflicts or duplicates",
            ));
        }
        // Each local B is native evidence from THAT database. It is neither
        // compared with another replica's B nor substituted for range R.
        self.cancellation = self
            .cancellation
            .child_with_deadline(observation.actual.committed().deadline());
        self.observations
            .insert(observation.local_node_id().clone(), Arc::new(observation));
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<NativeJointPreparedServingQuorum> {
        self.cancellation.check()?;
        if self.poisoned
            || self.observations.len() < self.data.new_members().len() / 2 + 1
            || !self
                .observations
                .contains_key(&self.data.activation_range().leader)
        {
            return Err(invalid(
                "joint bridge needs an actual distinct NEW prepared majority and leader",
            ));
        }
        let quorum = NativeJointPreparedServingQuorum {
            data: self.data,
            observations: self.observations,
            cancellation: self.cancellation,
        };
        quorum.require_current()?;
        Ok(quorum)
    }
}

/// Actual NEW materialized/prepared quorum, still not a serving grant. The
/// original deadlines and owned physical freezes/pins remain retained.
pub(crate) struct NativeJointPreparedServingQuorum {
    data: NativeJointDecisionData,
    observations: BTreeMap<ClusterNodeId, Arc<NativeJointPreparedServingObservation>>,
    cancellation: crate::CancellationToken,
}
impl NativeJointPreparedServingQuorum {
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.data
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        self.cancellation.check()?;
        for observation in self.observations.values() {
            observation.require_current()?;
        }
        Ok(())
    }
    /// Counted activation writers can follow only after these read pins drain.
    /// This does NOT release a freeze, clear a promise or activate a catalogue.
    pub(super) fn release_read_pins_for_activation(&self) -> Result<()> {
        self.require_current()?;
        for observation in self.observations.values() {
            observation.release_read_pin()?;
        }
        self.require_current()
    }
}

/// Audit projection only. Its decoder cannot construct the quorum or the
/// publication admission below. Local visibility remains local to each vote.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeJointPreparedServingVoteData {
    pub(crate) preparation_sha256: String,
    pub(crate) native_database_incarnation: String,
    pub(crate) actual_local_visibility_sequence: u64,
    pub(crate) metadata_context_id: Uuid,
    pub(crate) metadata_term: u64,
    pub(crate) metadata_read_index: u64,
}

/// The only native bridge publisher. Construction performs a real fresh Host
/// MQ AFTER collection and retains the actual quorum and original deadlines.
pub(crate) struct NativeJointActivationAdmission {
    quorum: Arc<NativeJointPreparedServingQuorum>,
    local: Arc<NativeJointPreparedServingObservation>,
    fresh: CommittedNativeJointDecisionObservation,
    cancellation: crate::CancellationToken,
}
impl NativeJointPreparedServingQuorum {
    pub(crate) fn acquire_local_activation(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointActivationAdmission> {
        self.require_current()?;
        let actor = client.local_metadata_read_participant();
        let local = self
            .observations
            .get(actor.node_id())
            .ok_or_else(|| invalid("joint activation requester has no actual NEW preparation"))?
            .clone();
        let expected = &self.data.new_members()[actor.node_id()];
        if actor.member_incarnation() != expected.incarnation {
            return Err(invalid(
                "joint activation actual transport incarnation changed",
            ));
        }
        let cancellation = parent.child_with_deadline(
            self.cancellation
                .deadline()
                .ok_or_else(|| invalid("joint NEW quorum has no original deadline"))?,
        );
        cancellation.check()?;
        let remaining = cancellation
            .deadline()
            .and_then(|deadline| deadline.checked_duration_since(std::time::Instant::now()))
            .ok_or_else(|| invalid("joint activation original budget expired"))?
            .min(std::time::Duration::from_secs(5));
        let business = Uuid::now_v7();
        // No database/service guard is held during the actual metadata I/O.
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            remaining,
            &cancellation,
        )?;
        let fresh = {
            let service = local
                .service
                .try_lock()
                .ok_or_else(|| invalid("joint activation metadata binding service busy"))?;
            service.observe_committed_native_joint_decision(
                metadata,
                business,
                self.data.old_range().id,
            )?
        };
        let cancellation = cancellation.child_with_deadline(fresh.deadline());
        let result = NativeJointActivationAdmission {
            quorum: self.clone(),
            local,
            fresh,
            cancellation,
        };
        result.require_live()?;
        // Keep real freeze ownership. Only the physical read pins are released
        // to admit the counted native metadata writer which follows.
        self.release_read_pins_for_activation()?;
        result.require_live()?;
        Ok(result)
    }
}
impl NativeJointActivationAdmission {
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.quorum.data
    }
    pub(crate) fn committed(&self) -> &CommittedNativeJointDecisionObservation {
        &self.fresh
    }
    pub(crate) fn local_preparation(&self) -> &crate::db::NativeJointServingPreparation {
        self.local.actual.prepared()
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        if self.fresh.data() != &self.quorum.data
            || self.fresh.native_receipt().local_node_id() != self.local.local_node_id()
        {
            return Err(invalid(
                "joint activation changed actual local prepared/committed decision",
            ));
        }
        let (term, index, context) = self.fresh.metadata_anchor();
        for observation in self.quorum.observations.values() {
            let (vote_term, vote_index, vote_context) =
                observation.actual.prepared().metadata_anchor();
            if context == vote_context || term < vote_term || index < vote_index {
                return Err(invalid(
                    "joint activation metadata is not independently post-NEW-quorum",
                ));
            }
        }
        Ok(())
    }
    /// The caller already owns THIS service latch. Every other local-service
    /// check is nonwaiting; never recursively acquire the current latch.
    pub(crate) fn require_external_preparations_current(&self) -> Result<()> {
        self.require_live()?;
        for (node, observation) in &self.quorum.observations {
            if node != self.local.local_node_id() {
                observation.require_current()?;
            }
        }
        Ok(())
    }
    pub(crate) fn votes(&self) -> BTreeMap<ClusterNodeId, NativeJointPreparedServingVoteData> {
        self.quorum
            .observations
            .iter()
            .map(|(node, observation)| {
                let actual = observation.actual.prepared();
                let (term, index, context) = actual.metadata_anchor();
                (
                    node.clone(),
                    NativeJointPreparedServingVoteData {
                        preparation_sha256: actual.checksum_sha256().into(),
                        native_database_incarnation: actual.native_database_incarnation().into(),
                        actual_local_visibility_sequence: actual.actual_local_visibility_sequence(),
                        metadata_context_id: context,
                        metadata_term: term,
                        metadata_read_index: index,
                    },
                )
            })
            .collect()
    }
    pub(crate) fn publish_local_native_bridge(
        &self,
        parent: &crate::CancellationToken,
    ) -> Result<crate::db::NativeJointServingBridgeReceipt> {
        parent.check()?;
        self.require_external_preparations_current()?;
        let service = self
            .local
            .service
            .try_lock()
            .ok_or_else(|| invalid("joint activation local service busy"))?;
        service.publish_local_native_joint_bridge(self, &self.local.actual, parent)
    }
}
