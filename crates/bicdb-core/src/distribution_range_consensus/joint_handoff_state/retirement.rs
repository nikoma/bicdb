//! Durable retirement of a retired joint source from its range.
//!
//! The record is a membership tombstone written by the retired source itself
//! after ITS OWN committed metadata read showed the recorded activation
//! quorum of the new set. It grants nothing and removes nothing: the
//! permanent handoff promise, the range-log epoch fence, the retained history
//! and every row stay exactly as the source cut left them, so the copy is as
//! unwritable and unreadable as before. Reclaiming the rows is a separate
//! protocol that must present this record.
use super::*;
use crate::distribution::{NativeJointHandoffStageV1, RelocationId};
use crate::distribution_consensus::HostMetadataReadObservation;

/// Domain of `source_cleanup_sha256`.
const SOURCE_RETIREMENT_DOMAIN: &str = "bicdb-native-joint-source-retirement-v1";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeJointSourceRetirement {
    version: u32,
    cluster_id: ClusterId,
    node_id: ClusterNodeId,
    node_incarnation: u64,
    relocation_id: RelocationId,
    range_id: RangeId,
    session_id: Uuid,
    nonce: Uuid,
    /// The epoch of the final membership the source is absent from. The
    /// source's own range log stays fenced at this epoch.
    activation_epoch: u64,
    decision_sha256: String,
    activated_quorum_sha256: String,
    /// The closed prefix this member retains. Nothing was deleted.
    retained_resolved_through: u64,
    rows_retained: bool,
}
impl NativeJointSourceRetirement {
    pub(super) fn decision_sha256(&self) -> &str {
        &self.decision_sha256
    }
    pub(super) fn validate(&self) -> Result<()> {
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if self.version != 1
            || self.session_id.is_nil()
            || self.nonce.is_nil()
            || self.range_id.get() == 0
            || self.relocation_id.get() == 0
            || self.activation_epoch == 0
            || !self.rows_retained
            || !digest(&self.decision_sha256)
            || !digest(&self.activated_quorum_sha256)
        {
            return Err(range_write_error("damaged joint source retirement"));
        }
        Ok(())
    }
    fn checksum(&self) -> Result<String> {
        self.validate()?;
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            SOURCE_RETIREMENT_DOMAIN,
            self,
        ))?)))
    }
}

/// The actual durable retirement of this open store. It is not a permission:
/// it lets the metadata stage record what the source itself wrote.
pub(crate) struct NativeJointSourceRetirementReceipt {
    store_open_id: Uuid,
    retirement: NativeJointSourceRetirement,
    sha256: String,
}
impl NativeJointSourceRetirementReceipt {
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.sha256
    }
    pub(crate) fn cluster_id(&self) -> &ClusterId {
        &self.retirement.cluster_id
    }
    pub(crate) fn node_id(&self) -> &ClusterNodeId {
        &self.retirement.node_id
    }
    pub(crate) fn node_incarnation(&self) -> u64 {
        self.retirement.node_incarnation
    }
    pub(crate) fn relocation_id(&self) -> RelocationId {
        self.retirement.relocation_id
    }
    pub(crate) fn range_id(&self) -> RangeId {
        self.retirement.range_id
    }
    pub(crate) fn session_id(&self) -> Uuid {
        self.retirement.session_id
    }
    pub(crate) fn nonce(&self) -> Uuid {
        self.retirement.nonce
    }
    pub(crate) fn activation_epoch(&self) -> u64 {
        self.retirement.activation_epoch
    }
    pub(crate) fn decision_sha256(&self) -> &str {
        &self.retirement.decision_sha256
    }
    pub(crate) fn activated_quorum_sha256(&self) -> &str {
        &self.retirement.activated_quorum_sha256
    }
    /// The same open store still holds exactly this record behind its
    /// unchanged permanent promise and epoch fence.
    pub(crate) fn require_current(&self, store: &RangeWriteStore) -> Result<()> {
        let range = self.retirement.range_id;
        if self.store_open_id != store.open_id
            || store.catalog.node_id != self.retirement.node_id
            || store
                .catalog
                .native_joint_handoffs
                .as_ref()
                .and_then(|states| states.get(&range))
                .and_then(|state| state.source_retirement.as_ref())
                != Some(&self.retirement)
            || store
                .catalog
                .handoff_promises
                .get(&range)
                .is_none_or(|promise| {
                    promise.session_id != self.retirement.session_id
                        || promise.nonce != self.retirement.nonce
                        || promise.activation_epoch != self.retirement.activation_epoch
                })
            || store.progress(range).current_epoch != self.retirement.activation_epoch
        {
            return Err(range_write_error(
                "joint source retirement actual open/record/fence changed",
            ));
        }
        Ok(())
    }
}

impl RangeWriteStore {
    /// Record this member's retirement from `range`, or confirm the exact
    /// record it already holds. `own` is this member's own committed metadata
    /// read. Refused unless that read shows this member as the relocation's
    /// source, absent from the final membership, with the activation quorum
    /// of the new set recorded, and unless this store still holds the closed
    /// session of exactly that decision behind its permanent promise.
    pub(crate) fn retire_native_joint_source(
        &mut self,
        own: &HostMetadataReadObservation,
        business: Uuid,
        range: RangeId,
    ) -> Result<NativeJointSourceRetirementReceipt> {
        own.require_live_request(business)?;
        self.require_native_writer_host_base()?;
        if self.recovery_persistence_uncertain {
            return Err(range_write_error(
                "joint source retirement cannot follow an uncertain catalogue write",
            ));
        }
        let topology = own.committed_topology(business)?;
        let local = self.catalog.node_id.clone();
        let promise = self
            .catalog
            .handoff_promises
            .get(&range)
            .cloned()
            .ok_or_else(|| range_write_error("joint source retirement has no permanent promise"))?;
        let plan = topology
            .relocations
            .get(&promise.relocation_id)
            .ok_or_else(|| {
                range_write_error("joint source retirement relocation is not committed")
            })?;
        let record = plan.native_joint_handoff_v1.as_ref().ok_or_else(|| {
            range_write_error("joint source retirement relocation has no committed decision")
        })?;
        let activated = record.activated_quorum_sha256().ok_or_else(|| {
            range_write_error(
                "joint source retirement requires the committed activation quorum of the new set",
            )
        })?;
        let state = self
            .catalog
            .native_joint_handoffs
            .as_ref()
            .and_then(|states| states.get(&range))
            .ok_or_else(|| range_write_error("joint source retirement has no closed session"))?;
        if own.requester_node_id() != &local
            || topology.cluster_id != self.catalog.cluster_id
            || topology
                .nodes
                .get(&local)
                .is_none_or(|node| node.incarnation != own.requester_incarnation())
            || record.stage() < NativeJointHandoffStageV1::AwaitingCleanup
            || plan.range_id != range
            || promise.range_id != range
            || plan.source.as_ref() != Some(&local)
            || plan.target == local
            || plan.promoted_epoch != Some(promise.activation_epoch)
            || plan.handoff_session.as_ref().is_none_or(|session| {
                session.session_id != promise.session_id || session.nonce != promise.nonce
            })
            || record.session_id() != promise.session_id
            || record.nonce() != promise.nonce
            || record.old_configuration_sha256() != promise.old_config_sha256
            || record.new_configuration_sha256() != promise.new_config_sha256
            || state.session_id != promise.session_id
            || state.nonce != promise.nonce
            || state.old_closed_native_sha256.is_none()
            || state.global_decision_sha256.as_deref() != Some(record.decision_sha256())
            || state.serving_activation.is_some()
            || self.catalog.image_install_intents.contains_key(&range)
            || self.progress(range).current_epoch != promise.activation_epoch
        {
            return Err(range_write_error(
                "joint source retirement is not this member's committed, activated and closed handoff",
            ));
        }
        // A Cleaned record no longer describes the current range. Before it,
        // the committed final membership must be the one this member left.
        if record.stage() != NativeJointHandoffStageV1::Cleaned
            && topology.range_by_id(range).is_none_or(|descriptor| {
                descriptor.epoch != promise.activation_epoch
                    || descriptor.replicas.iter().any(|r| r.node_id == local)
            })
        {
            return Err(range_write_error(
                "joint source retirement member is still in the committed range",
            ));
        }
        let retirement = NativeJointSourceRetirement {
            version: 1,
            cluster_id: self.catalog.cluster_id.clone(),
            node_id: local,
            node_incarnation: own.requester_incarnation(),
            relocation_id: promise.relocation_id,
            range_id: range,
            session_id: promise.session_id,
            nonce: promise.nonce,
            activation_epoch: promise.activation_epoch,
            decision_sha256: record.decision_sha256().into(),
            activated_quorum_sha256: activated.into(),
            retained_resolved_through: promise.resolved_through,
            rows_retained: true,
        };
        let sha256 = retirement.checksum()?;
        if record
            .source_cleanup_sha256()
            .is_some_and(|recorded| recorded != sha256)
        {
            return Err(range_write_error(
                "committed joint source retirement is not this member's record",
            ));
        }
        match &state.source_retirement {
            Some(existing) if existing == &retirement => {}
            Some(_) => {
                return Err(range_write_error(
                    "a different joint source retirement must not be overwritten",
                ))
            }
            None => {
                own.require_live_request(business)?;
                let previous = self.catalog.clone();
                self.catalog
                    .native_joint_handoffs
                    .as_mut()
                    .and_then(|states| states.get_mut(&range))
                    .expect("validated joint state exists")
                    .source_retirement = Some(retirement.clone());
                // An ambiguous write leaves no receipt and the previous
                // memory state. A retry writes the identical record again.
                self.persist_or_restore(previous, ())?;
            }
        }
        let receipt = NativeJointSourceRetirementReceipt {
            store_open_id: self.open_id,
            retirement,
            sha256,
        };
        receipt.require_current(self)?;
        Ok(receipt)
    }
}
