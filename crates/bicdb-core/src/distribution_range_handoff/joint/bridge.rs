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
