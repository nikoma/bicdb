//! Durable, one-voter-per-tick distribution of signed schema stages.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{
    ClusterNodeId, ClusterNodeLifecycle, ClusterTopology, MetadataMemberRole,
    SCHEMA_COMPATIBILITY_NODE_LABEL, SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL,
};
use crate::distribution_consensus::{MetadataConsensusRole, MetadataConsensusStatus};
use crate::distribution_data::{
    ClusterSchemaActivationReceipt, ClusterSchemaFinalizationReceipt, ClusterSchemaStageReceipt,
};
use crate::error::{BicDbError, Result};
use crate::storage;
use crate::SignedClusterSchemaBundle;

pub const CLUSTER_SCHEMA_ROLLOUT_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_CLUSTER_SCHEMA_ROLLOUT_STATE: &str = "cluster-schema-rollout.json";

fn rollout_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(message.into())
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSchemaRolloutLimits {
    pub max_voters: usize,
    pub max_state_bytes: u64,
}

impl Default for ClusterSchemaRolloutLimits {
    fn default() -> Self {
        Self {
            max_voters: 1_024,
            max_state_bytes: 80 * 1024 * 1024,
        }
    }
}

impl ClusterSchemaRolloutLimits {
    pub fn validate(self) -> Result<()> {
        if !(1..=16_384).contains(&self.max_voters)
            || !(64 * 1024 * 1024..=256 * 1024 * 1024).contains(&self.max_state_bytes)
        {
            return Err(rollout_error("cluster schema rollout limits are invalid"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClusterSchemaRolloutPhase {
    Staging,
    ReadyForActivation,
    ActivationFenced,
    Activating,
    Complete,
    Promoted,
    Finalizing,
    Finalized,
}

fn usize_is_zero(value: &usize) -> bool {
    *value == 0
}

fn map_is_empty<K, V>(value: &BTreeMap<K, V>) -> bool {
    value.is_empty()
}

fn bool_is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterSchemaRolloutState {
    pub format_version: u32,
    pub rollout_id: Uuid,
    pub phase: ClusterSchemaRolloutPhase,
    pub cluster_id: crate::ClusterId,
    pub topology_generation: u64,
    pub metadata_leader_id: ClusterNodeId,
    pub base_fingerprint_sha256: String,
    pub signed_bundle: SignedClusterSchemaBundle,
    pub voters: Vec<ClusterNodeId>,
    pub next_voter: usize,
    pub receipts: BTreeMap<ClusterNodeId, ClusterSchemaStageReceipt>,
    #[serde(default, skip_serializing_if = "bool_is_false")]
    pub compatibility_window: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_topology_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_topology_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "usize_is_zero")]
    pub activation_next_voter: usize,
    #[serde(default, skip_serializing_if = "map_is_empty")]
    pub activation_receipts: BTreeMap<ClusterNodeId, ClusterSchemaActivationReceipt>,
    #[serde(default, skip_serializing_if = "usize_is_zero")]
    pub finalization_next_voter: usize,
    #[serde(default, skip_serializing_if = "map_is_empty")]
    pub finalization_receipts: BTreeMap<ClusterNodeId, ClusterSchemaFinalizationReceipt>,
    pub limits: ClusterSchemaRolloutLimits,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub checksum_sha256: String,
}

impl ClusterSchemaRolloutState {
    fn calculate_checksum(&self) -> Result<String> {
        let mut value = self.clone();
        value.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&value)?)))
    }

    fn refresh_checksum(&mut self) -> Result<()> {
        self.checksum_sha256 = self.calculate_checksum()?;
        Ok(())
    }

    pub fn validate(
        &self,
        trusted_keys: &BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaRolloutLimits,
    ) -> Result<()> {
        limits.validate()?;
        self.signed_bundle.verify(trusted_keys)?;
        let voters = self.voters.iter().cloned().collect::<BTreeSet<_>>();
        let receipt_nodes = self.receipts.keys().cloned().collect::<BTreeSet<_>>();
        let activation_receipt_nodes = self
            .activation_receipts
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let finalization_receipt_nodes = self
            .finalization_receipts
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let expected_receipts = self
            .voters
            .iter()
            .take(self.next_voter)
            .cloned()
            .collect::<BTreeSet<_>>();
        let expected_activation_receipts = self
            .voters
            .iter()
            .take(self.activation_next_voter)
            .cloned()
            .collect::<BTreeSet<_>>();
        let expected_finalization_receipts = self
            .voters
            .iter()
            .take(self.finalization_next_voter)
            .cloned()
            .collect::<BTreeSet<_>>();
        let phase_valid = match self.phase {
            ClusterSchemaRolloutPhase::Staging => {
                self.next_voter < self.voters.len()
                    && self.activation_topology_generation.is_none()
                    && self.promotion_topology_generation.is_none()
                    && self.activation_next_voter == 0
                    && self.activation_receipts.is_empty()
                    && self.finalization_next_voter == 0
                    && self.finalization_receipts.is_empty()
            }
            ClusterSchemaRolloutPhase::ReadyForActivation => {
                self.next_voter == self.voters.len()
                    && self.activation_topology_generation.is_none()
                    && self.promotion_topology_generation.is_none()
                    && self.activation_next_voter == 0
                    && self.activation_receipts.is_empty()
                    && self.finalization_next_voter == 0
                    && self.finalization_receipts.is_empty()
            }
            ClusterSchemaRolloutPhase::ActivationFenced => {
                self.next_voter == self.voters.len()
                    && self.activation_topology_generation
                        == self.topology_generation.checked_add(1)
                    && self.promotion_topology_generation.is_none()
                    && self.activation_next_voter == 0
                    && self.activation_receipts.is_empty()
                    && self.finalization_next_voter == 0
                    && self.finalization_receipts.is_empty()
            }
            ClusterSchemaRolloutPhase::Activating => {
                self.next_voter == self.voters.len()
                    && self.activation_topology_generation
                        == self.topology_generation.checked_add(1)
                    && self.promotion_topology_generation.is_none()
                    && self.activation_next_voter < self.voters.len()
                    && self.finalization_next_voter == 0
                    && self.finalization_receipts.is_empty()
            }
            ClusterSchemaRolloutPhase::Complete => {
                self.next_voter == self.voters.len()
                    && self.activation_topology_generation
                        == self.topology_generation.checked_add(1)
                    && self.promotion_topology_generation.is_none()
                    && self.activation_next_voter == self.voters.len()
                    && self.finalization_next_voter == 0
                    && self.finalization_receipts.is_empty()
            }
            ClusterSchemaRolloutPhase::Promoted => {
                self.compatibility_window
                    && self.next_voter == self.voters.len()
                    && self.activation_topology_generation
                        == self.topology_generation.checked_add(1)
                    && self.promotion_topology_generation == self.topology_generation.checked_add(2)
                    && self.activation_next_voter == self.voters.len()
                    && self.finalization_next_voter == 0
                    && self.finalization_receipts.is_empty()
            }
            ClusterSchemaRolloutPhase::Finalizing => {
                self.next_voter == self.voters.len()
                    && self.activation_topology_generation
                        == self.topology_generation.checked_add(1)
                    && self.activation_next_voter == self.voters.len()
                    && ((!self.compatibility_window
                        && self.promotion_topology_generation.is_none())
                        || (self.compatibility_window
                            && self.promotion_topology_generation
                                == self.topology_generation.checked_add(2)))
                    && self.finalization_next_voter > 0
                    && self.finalization_next_voter < self.voters.len()
            }
            ClusterSchemaRolloutPhase::Finalized => {
                self.next_voter == self.voters.len()
                    && self.activation_topology_generation
                        == self.topology_generation.checked_add(1)
                    && self.activation_next_voter == self.voters.len()
                    && ((!self.compatibility_window
                        && self.promotion_topology_generation.is_none())
                        || (self.compatibility_window
                            && self.promotion_topology_generation
                                == self.topology_generation.checked_add(2)))
                    && self.finalization_next_voter == self.voters.len()
            }
        };
        if self.format_version != CLUSTER_SCHEMA_ROLLOUT_FORMAT_VERSION
            || self.rollout_id.is_nil()
            || self.topology_generation == 0
            || self.voters.is_empty()
            || self.voters.len() > limits.max_voters
            || voters.len() != self.voters.len()
            || !voters.contains(&self.metadata_leader_id)
            || self.next_voter > self.voters.len()
            || receipt_nodes != expected_receipts
            || activation_receipt_nodes != expected_activation_receipts
            || finalization_receipt_nodes != expected_finalization_receipts
            || !phase_valid
            || self.base_fingerprint_sha256.len() != 64
            || hex::decode(&self.base_fingerprint_sha256).map_or(true, |bytes| bytes.len() != 32)
            || self.base_fingerprint_sha256 == self.signed_bundle.bundle.fingerprint.sha256
            || self.created_at_ms > self.updated_at_ms
            || self.limits != limits
            || self.checksum_sha256.len() != 64
            || self.calculate_checksum()? != self.checksum_sha256
            || serde_json::to_vec(self)?.len() as u64 > limits.max_state_bytes
        {
            return Err(rollout_error(
                "cluster schema rollout identity, phase, receipts, checksum, or bound is invalid",
            ));
        }
        for (node, receipt) in &self.receipts {
            if !voters.contains(node)
                || receipt.base_fingerprint_sha256 != self.base_fingerprint_sha256
            {
                return Err(rollout_error(
                    "cluster schema rollout receipt has the wrong voter or base fingerprint",
                ));
            }
            receipt.validate_for(&self.signed_bundle)?;
        }
        for (node, receipt) in &self.activation_receipts {
            let stage = self.receipts.get(node).ok_or_else(|| {
                rollout_error(
                    "cluster schema activation receipt has no matching durable stage receipt",
                )
            })?;
            receipt.validate_for(node, self.rollout_id, stage, &self.signed_bundle)?;
            if !receipt.complete() {
                return Err(rollout_error(
                    "cluster schema activation completion set contains a progress receipt",
                ));
            }
        }
        for (node, receipt) in &self.finalization_receipts {
            let activation = self.activation_receipts.get(node).ok_or_else(|| {
                rollout_error(
                    "cluster schema finalization receipt has no matching activation receipt",
                )
            })?;
            receipt.validate_for(node, activation)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClusterSchemaRolloutAdvance {
    Progress(ClusterSchemaRolloutState),
    ReadyForActivation(ClusterSchemaRolloutState),
}

pub trait ClusterSchemaStageTransport {
    fn stage_signed_schema_bundle(
        &mut self,
        node_id: &ClusterNodeId,
        signed_bundle: &SignedClusterSchemaBundle,
    ) -> Result<ClusterSchemaStageReceipt>;
}

pub trait ClusterSchemaActivationTransport {
    fn advance_signed_schema_activation(
        &mut self,
        node_id: &ClusterNodeId,
        rollout_id: Uuid,
        stage_id: Uuid,
        base_fingerprint_sha256: &str,
        target_fingerprint_sha256: &str,
    ) -> Result<ClusterSchemaActivationReceipt>;
}

pub trait ClusterSchemaFinalizationTransport {
    fn finalize_signed_schema_activation(
        &mut self,
        node_id: &ClusterNodeId,
        activation: &ClusterSchemaActivationReceipt,
    ) -> Result<ClusterSchemaFinalizationReceipt>;
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClusterSchemaActivationRolloutAdvance {
    Progress {
        state: ClusterSchemaRolloutState,
        receipt: ClusterSchemaActivationReceipt,
    },
    Complete(ClusterSchemaRolloutState),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClusterSchemaFinalizationRolloutAdvance {
    Progress(ClusterSchemaRolloutState),
    Finalized(ClusterSchemaRolloutState),
}

#[derive(Debug)]
pub struct ClusterSchemaRolloutRun {
    path: PathBuf,
    fsync: bool,
    limits: ClusterSchemaRolloutLimits,
    trusted_keys: BTreeMap<String, [u8; 32]>,
    state: ClusterSchemaRolloutState,
}

impl ClusterSchemaRolloutRun {
    pub fn begin(
        root: impl AsRef<Path>,
        topology: &ClusterTopology,
        consensus: &MetadataConsensusStatus,
        signed_bundle: SignedClusterSchemaBundle,
        trusted_keys: BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaRolloutLimits,
        fsync: bool,
        now_ms: u64,
    ) -> Result<Self> {
        limits.validate()?;
        signed_bundle.verify(&trusted_keys)?;
        let path = root.as_ref().join(DEFAULT_CLUSTER_SCHEMA_ROLLOUT_STATE);
        if path.exists() {
            let existing = Self::open(root, trusted_keys, limits, fsync)?;
            if existing.state.signed_bundle == signed_bundle
                && existing.state.topology_generation == topology.generation
            {
                existing.validate_authority(topology, consensus)?;
                return Ok(existing);
            }
            return Err(rollout_error(
                "another cluster schema rollout is already active",
            ));
        }
        let (voters, base_fingerprint_sha256) =
            validate_rollout_authority(topology, consensus, limits)?;
        if base_fingerprint_sha256 == signed_bundle.bundle.fingerprint.sha256 {
            return Err(rollout_error(
                "cluster schema rollout target is already active",
            ));
        }
        let mut state = ClusterSchemaRolloutState {
            format_version: CLUSTER_SCHEMA_ROLLOUT_FORMAT_VERSION,
            rollout_id: Uuid::now_v7(),
            phase: ClusterSchemaRolloutPhase::Staging,
            cluster_id: topology.cluster_id.clone(),
            topology_generation: topology.generation,
            metadata_leader_id: consensus.node_id.clone(),
            base_fingerprint_sha256,
            signed_bundle,
            voters,
            next_voter: 0,
            receipts: BTreeMap::new(),
            compatibility_window: true,
            activation_topology_generation: None,
            promotion_topology_generation: None,
            activation_next_voter: 0,
            activation_receipts: BTreeMap::new(),
            finalization_next_voter: 0,
            finalization_receipts: BTreeMap::new(),
            limits,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            checksum_sha256: String::new(),
        };
        state.refresh_checksum()?;
        state.validate(&trusted_keys, limits)?;
        let run = Self {
            path,
            fsync,
            limits,
            trusted_keys,
            state,
        };
        run.persist()?;
        Ok(run)
    }

    pub fn open(
        root: impl AsRef<Path>,
        trusted_keys: BTreeMap<String, [u8; 32]>,
        limits: ClusterSchemaRolloutLimits,
        fsync: bool,
    ) -> Result<Self> {
        limits.validate()?;
        let path = root.as_ref().join(DEFAULT_CLUSTER_SCHEMA_ROLLOUT_STATE);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > limits.max_state_bytes
        {
            return Err(rollout_error(
                "cluster schema rollout state is unsafe or outside its byte bound",
            ));
        }
        #[cfg(unix)]
        let file = {
            use std::os::unix::fs::OpenOptionsExt;
            fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)?
        };
        #[cfg(not(unix))]
        let file = std::fs::File::open(&path)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(limits.max_state_bytes.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != metadata.len() || bytes.len() as u64 > limits.max_state_bytes {
            return Err(rollout_error(
                "cluster schema rollout state changed or grew while reading",
            ));
        }
        let state: ClusterSchemaRolloutState = serde_json::from_slice(&bytes)?;
        state.validate(&trusted_keys, limits)?;
        Ok(Self {
            path,
            fsync,
            limits,
            trusted_keys,
            state,
        })
    }

    pub fn state(&self) -> &ClusterSchemaRolloutState {
        &self.state
    }

    pub fn advance<T: ClusterSchemaStageTransport>(
        &mut self,
        topology: &ClusterTopology,
        consensus: &MetadataConsensusStatus,
        transport: &mut T,
        now_ms: u64,
    ) -> Result<ClusterSchemaRolloutAdvance> {
        self.state.validate(&self.trusted_keys, self.limits)?;
        if matches!(
            self.state.phase,
            ClusterSchemaRolloutPhase::ActivationFenced
                | ClusterSchemaRolloutPhase::Activating
                | ClusterSchemaRolloutPhase::Complete
                | ClusterSchemaRolloutPhase::Promoted
                | ClusterSchemaRolloutPhase::Finalizing
                | ClusterSchemaRolloutPhase::Finalized
        ) {
            return Err(rollout_error(
                "cluster schema rollout is already fenced for activation",
            ));
        }
        self.validate_authority(topology, consensus)?;
        if now_ms < self.state.updated_at_ms {
            return Err(rollout_error("cluster schema rollout clock regressed"));
        }
        if self.state.phase == ClusterSchemaRolloutPhase::ReadyForActivation {
            return Ok(ClusterSchemaRolloutAdvance::ReadyForActivation(
                self.state.clone(),
            ));
        }
        let node_id = self.state.voters[self.state.next_voter].clone();
        let receipt = transport.stage_signed_schema_bundle(&node_id, &self.state.signed_bundle)?;
        receipt.validate_for(&self.state.signed_bundle)?;
        if receipt.base_fingerprint_sha256 != self.state.base_fingerprint_sha256 {
            return Err(rollout_error(format!(
                "cluster schema voter {node_id} staged from {}, expected {}",
                receipt.base_fingerprint_sha256, self.state.base_fingerprint_sha256
            )));
        }
        self.state.receipts.insert(node_id, receipt);
        self.state.next_voter += 1;
        if self.state.next_voter == self.state.voters.len() {
            self.state.phase = ClusterSchemaRolloutPhase::ReadyForActivation;
        }
        self.state.updated_at_ms = now_ms;
        self.state.refresh_checksum()?;
        self.state.validate(&self.trusted_keys, self.limits)?;
        self.persist()?;
        if self.state.phase == ClusterSchemaRolloutPhase::ReadyForActivation {
            Ok(ClusterSchemaRolloutAdvance::ReadyForActivation(
                self.state.clone(),
            ))
        } else {
            Ok(ClusterSchemaRolloutAdvance::Progress(self.state.clone()))
        }
    }

    /// Construct the sole permissible target-digest topology mutation. The
    /// returned candidate has no authority until the caller commits it through
    /// metadata consensus. Every voter changes in one topology generation, so
    /// old live digests fail the existing strict schema fence immediately.
    pub fn activation_fence_topology(
        &self,
        topology: &ClusterTopology,
        consensus: &MetadataConsensusStatus,
        now_ms: u64,
    ) -> Result<ClusterTopology> {
        self.state.validate(&self.trusted_keys, self.limits)?;
        if self.state.compatibility_window {
            return Err(rollout_error(
                "new cluster schema rollouts must open an additive compatibility window",
            ));
        }
        if self.state.phase != ClusterSchemaRolloutPhase::ReadyForActivation {
            return Err(rollout_error(
                "cluster schema activation fence requires every durable stage receipt",
            ));
        }
        if now_ms < self.state.updated_at_ms {
            return Err(rollout_error("cluster schema rollout clock regressed"));
        }
        self.validate_authority(topology, consensus)?;
        let mut candidate = topology.clone();
        candidate.fence_schema_activation(
            &self.state.voters.iter().cloned().collect(),
            &self.state.base_fingerprint_sha256,
            &self.state.signed_bundle.bundle.fingerprint.sha256,
            consensus.node_id.clone(),
            now_ms,
        )?;
        if candidate.generation != self.state.topology_generation.saturating_add(1) {
            return Err(rollout_error(
                "cluster schema activation fence did not advance exactly one topology generation",
            ));
        }
        Ok(candidate)
    }

    /// Construct a topology generation that keeps the common base active and
    /// publishes the signed additive target as a pending compatibility digest.
    /// The candidate gains authority only through metadata quorum commit.
    pub fn compatibility_window_topology(
        &self,
        topology: &ClusterTopology,
        consensus: &MetadataConsensusStatus,
        now_ms: u64,
    ) -> Result<ClusterTopology> {
        self.state.validate(&self.trusted_keys, self.limits)?;
        if !self.state.compatibility_window
            || self.state.phase != ClusterSchemaRolloutPhase::ReadyForActivation
        {
            return Err(rollout_error(
                "cluster schema compatibility window requires a new fully staged rollout",
            ));
        }
        if now_ms < self.state.updated_at_ms {
            return Err(rollout_error("cluster schema rollout clock regressed"));
        }
        self.validate_authority(topology, consensus)?;
        let mut candidate = topology.clone();
        candidate.open_schema_compatibility_window(
            &self.state.voters.iter().cloned().collect(),
            &self.state.base_fingerprint_sha256,
            &self.state.signed_bundle.bundle.fingerprint.sha256,
            consensus.node_id.clone(),
            now_ms,
        )?;
        if candidate.generation != self.state.topology_generation.saturating_add(1) {
            return Err(rollout_error(
                "cluster schema compatibility window did not advance exactly one topology generation",
            ));
        }
        Ok(candidate)
    }

    /// Reconcile the durable rollout after metadata consensus commits the
    /// target-digest fence. This is safe after a lost proposal response or a
    /// metadata-leader restart because authority comes from the committed
    /// topology, not from the proposer response.
    pub fn observe_activation_fence(
        &mut self,
        topology: &ClusterTopology,
        consensus: &MetadataConsensusStatus,
        now_ms: u64,
    ) -> Result<ClusterSchemaRolloutState> {
        self.state.validate(&self.trusted_keys, self.limits)?;
        if !matches!(
            self.state.phase,
            ClusterSchemaRolloutPhase::ReadyForActivation
                | ClusterSchemaRolloutPhase::ActivationFenced
                | ClusterSchemaRolloutPhase::Activating
                | ClusterSchemaRolloutPhase::Complete
                | ClusterSchemaRolloutPhase::Promoted
                | ClusterSchemaRolloutPhase::Finalizing
                | ClusterSchemaRolloutPhase::Finalized
        ) {
            return Err(rollout_error(
                "cluster schema activation fence cannot precede complete staging",
            ));
        }
        if now_ms < self.state.updated_at_ms {
            return Err(rollout_error("cluster schema rollout clock regressed"));
        }
        if self.state.compatibility_window
            && matches!(
                self.state.phase,
                ClusterSchemaRolloutPhase::Promoted
                    | ClusterSchemaRolloutPhase::Finalizing
                    | ClusterSchemaRolloutPhase::Finalized
            )
        {
            validate_promoted_authority(topology, consensus, &self.state)?;
        } else {
            validate_fenced_authority(topology, consensus, &self.state)?;
        }
        if self.state.phase == ClusterSchemaRolloutPhase::ReadyForActivation {
            self.state.phase = ClusterSchemaRolloutPhase::ActivationFenced;
            self.state.activation_topology_generation = Some(topology.generation);
            self.state.updated_at_ms = now_ms;
            self.state.refresh_checksum()?;
            self.state.validate(&self.trusted_keys, self.limits)?;
            self.persist()?;
        }
        Ok(self.state.clone())
    }

    /// Build the one eligible promotion after every voter has completed the
    /// signed target. The active base remains authoritative until this second
    /// candidate is quorum committed.
    pub fn promotion_topology(
        &self,
        topology: &ClusterTopology,
        consensus: &MetadataConsensusStatus,
        now_ms: u64,
    ) -> Result<ClusterTopology> {
        self.state.validate(&self.trusted_keys, self.limits)?;
        if !self.state.compatibility_window
            || self.state.phase != ClusterSchemaRolloutPhase::Complete
        {
            return Err(rollout_error(
                "cluster schema promotion requires all-voter compatibility-window completion",
            ));
        }
        if now_ms < self.state.updated_at_ms {
            return Err(rollout_error("cluster schema rollout clock regressed"));
        }
        validate_fenced_authority(topology, consensus, &self.state)?;
        let mut candidate = topology.clone();
        candidate.promote_schema_compatibility_window(
            &self.state.voters.iter().cloned().collect(),
            &self.state.base_fingerprint_sha256,
            &self.state.signed_bundle.bundle.fingerprint.sha256,
            consensus.node_id.clone(),
            now_ms,
        )?;
        if candidate.generation != self.state.topology_generation.saturating_add(2) {
            return Err(rollout_error(
                "cluster schema promotion did not advance exactly one window generation",
            ));
        }
        Ok(candidate)
    }

    /// Reconcile the quorum-committed active-target promotion after a lost
    /// proposal response or coordinator restart.
    pub fn observe_promotion(
        &mut self,
        topology: &ClusterTopology,
        consensus: &MetadataConsensusStatus,
        now_ms: u64,
    ) -> Result<ClusterSchemaRolloutState> {
        self.state.validate(&self.trusted_keys, self.limits)?;
        if !self.state.compatibility_window
            || !matches!(
                self.state.phase,
                ClusterSchemaRolloutPhase::Complete
                    | ClusterSchemaRolloutPhase::Promoted
                    | ClusterSchemaRolloutPhase::Finalizing
                    | ClusterSchemaRolloutPhase::Finalized
            )
        {
            return Err(rollout_error(
                "cluster schema promotion cannot precede compatibility-window completion",
            ));
        }
        if now_ms < self.state.updated_at_ms {
            return Err(rollout_error("cluster schema rollout clock regressed"));
        }
        validate_promoted_authority(topology, consensus, &self.state)?;
        if self.state.phase == ClusterSchemaRolloutPhase::Complete {
            self.state.phase = ClusterSchemaRolloutPhase::Promoted;
            self.state.promotion_topology_generation = Some(topology.generation);
            self.state.updated_at_ms = now_ms;
            self.state.refresh_checksum()?;
            self.state.validate(&self.trusted_keys, self.limits)?;
            self.persist()?;
        }
        Ok(self.state.clone())
    }

    /// Advance one destination at a time through the authenticated local
    /// activation API. Progress responses leave the durable voter cursor in
    /// place; only an exact complete receipt advances it. Lost responses are
    /// therefore safe to retry against the node-local idempotent checkpoint.
    pub fn advance_activation<T: ClusterSchemaActivationTransport>(
        &mut self,
        topology: &ClusterTopology,
        consensus: &MetadataConsensusStatus,
        transport: &mut T,
        now_ms: u64,
    ) -> Result<ClusterSchemaActivationRolloutAdvance> {
        self.state.validate(&self.trusted_keys, self.limits)?;
        if !matches!(
            self.state.phase,
            ClusterSchemaRolloutPhase::ActivationFenced
                | ClusterSchemaRolloutPhase::Activating
                | ClusterSchemaRolloutPhase::Complete
        ) {
            return Err(rollout_error(
                "cluster schema activation cannot advance before the quorum fence",
            ));
        }
        validate_fenced_authority(topology, consensus, &self.state)?;
        if now_ms < self.state.updated_at_ms {
            return Err(rollout_error("cluster schema rollout clock regressed"));
        }
        if self.state.phase == ClusterSchemaRolloutPhase::Complete {
            return Ok(ClusterSchemaActivationRolloutAdvance::Complete(
                self.state.clone(),
            ));
        }
        let node_id = self.state.voters[self.state.activation_next_voter].clone();
        let stage = self.state.receipts.get(&node_id).ok_or_else(|| {
            rollout_error(format!(
                "cluster schema voter {node_id} has no durable stage receipt"
            ))
        })?;
        let receipt = transport.advance_signed_schema_activation(
            &node_id,
            self.state.rollout_id,
            stage.stage_id,
            &stage.base_fingerprint_sha256,
            &self.state.signed_bundle.bundle.fingerprint.sha256,
        )?;
        receipt.validate_for(
            &node_id,
            self.state.rollout_id,
            stage,
            &self.state.signed_bundle,
        )?;
        self.state.phase = ClusterSchemaRolloutPhase::Activating;
        if receipt.complete() {
            self.state
                .activation_receipts
                .insert(node_id, receipt.clone());
            self.state.activation_next_voter += 1;
            if self.state.activation_next_voter == self.state.voters.len() {
                self.state.phase = ClusterSchemaRolloutPhase::Complete;
            }
        }
        self.state.updated_at_ms = now_ms;
        self.state.refresh_checksum()?;
        self.state.validate(&self.trusted_keys, self.limits)?;
        self.persist()?;
        if self.state.phase == ClusterSchemaRolloutPhase::Complete {
            Ok(ClusterSchemaActivationRolloutAdvance::Complete(
                self.state.clone(),
            ))
        } else {
            Ok(ClusterSchemaActivationRolloutAdvance::Progress {
                state: self.state.clone(),
                receipt,
            })
        }
    }

    /// Verify and clean one completed voter at a time. A destination first
    /// persists a compact finalization proof and only then removes its stage
    /// and activation checkpoints. The coordinator checkpoints the returned
    /// node-bound proof before moving to the next voter, so a lost response is
    /// safely retried against the destination proof.
    pub fn advance_finalization<T: ClusterSchemaFinalizationTransport>(
        &mut self,
        topology: &ClusterTopology,
        consensus: &MetadataConsensusStatus,
        transport: &mut T,
        now_ms: u64,
    ) -> Result<ClusterSchemaFinalizationRolloutAdvance> {
        self.state.validate(&self.trusted_keys, self.limits)?;
        let can_finalize = if self.state.compatibility_window {
            matches!(
                self.state.phase,
                ClusterSchemaRolloutPhase::Promoted
                    | ClusterSchemaRolloutPhase::Finalizing
                    | ClusterSchemaRolloutPhase::Finalized
            )
        } else {
            matches!(
                self.state.phase,
                ClusterSchemaRolloutPhase::Complete
                    | ClusterSchemaRolloutPhase::Finalizing
                    | ClusterSchemaRolloutPhase::Finalized
            )
        };
        if !can_finalize {
            if self.state.compatibility_window
                && self.state.phase == ClusterSchemaRolloutPhase::Complete
            {
                return Err(rollout_error(
                    "cluster schema finalization requires quorum-committed target promotion",
                ));
            }
            return Err(rollout_error(
                "cluster schema finalization requires all-voter activation completion",
            ));
        }
        if self.state.compatibility_window {
            validate_promoted_authority(topology, consensus, &self.state)?;
        } else {
            validate_fenced_authority(topology, consensus, &self.state)?;
        }
        if now_ms < self.state.updated_at_ms {
            return Err(rollout_error("cluster schema rollout clock regressed"));
        }
        if self.state.phase == ClusterSchemaRolloutPhase::Finalized {
            return Ok(ClusterSchemaFinalizationRolloutAdvance::Finalized(
                self.state.clone(),
            ));
        }
        let node_id = self.state.voters[self.state.finalization_next_voter].clone();
        let activation = self
            .state
            .activation_receipts
            .get(&node_id)
            .ok_or_else(|| {
                rollout_error(format!(
                    "cluster schema voter {node_id} has no durable activation completion receipt"
                ))
            })?;
        let receipt = transport.finalize_signed_schema_activation(&node_id, activation)?;
        receipt.validate_for(&node_id, activation)?;
        self.state.finalization_receipts.insert(node_id, receipt);
        self.state.finalization_next_voter += 1;
        self.state.phase = if self.state.finalization_next_voter == self.state.voters.len() {
            ClusterSchemaRolloutPhase::Finalized
        } else {
            ClusterSchemaRolloutPhase::Finalizing
        };
        self.state.updated_at_ms = now_ms;
        self.state.refresh_checksum()?;
        self.state.validate(&self.trusted_keys, self.limits)?;
        self.persist()?;
        if self.state.phase == ClusterSchemaRolloutPhase::Finalized {
            Ok(ClusterSchemaFinalizationRolloutAdvance::Finalized(
                self.state.clone(),
            ))
        } else {
            Ok(ClusterSchemaFinalizationRolloutAdvance::Progress(
                self.state.clone(),
            ))
        }
    }

    fn validate_authority(
        &self,
        topology: &ClusterTopology,
        consensus: &MetadataConsensusStatus,
    ) -> Result<()> {
        let (voters, base) = validate_rollout_authority(topology, consensus, self.limits)?;
        if topology.cluster_id != self.state.cluster_id
            || topology.generation != self.state.topology_generation
            || consensus.node_id != self.state.metadata_leader_id
            || voters != self.state.voters
            || base != self.state.base_fingerprint_sha256
        {
            return Err(rollout_error(
                "cluster schema rollout authority, topology, voters, or base fingerprint changed",
            ));
        }
        Ok(())
    }

    fn persist(&self) -> Result<()> {
        let bytes = serde_json::to_vec(&self.state)?;
        if bytes.len() as u64 > self.limits.max_state_bytes {
            return Err(rollout_error(
                "cluster schema rollout state exceeds its durable byte bound",
            ));
        }
        storage::write_atomic(&self.path, &bytes, self.fsync)
    }
}

fn validate_rollout_authority(
    topology: &ClusterTopology,
    consensus: &MetadataConsensusStatus,
    limits: ClusterSchemaRolloutLimits,
) -> Result<(Vec<ClusterNodeId>, String)> {
    topology.validate()?;
    if consensus.cluster_id != topology.cluster_id
        || consensus.topology_generation != topology.generation
        || consensus.role != MetadataConsensusRole::Leader
        || consensus.leader_id.as_ref() != Some(&consensus.node_id)
    {
        return Err(rollout_error(
            "cluster schema rollout requires the current metadata leader and exact committed topology",
        ));
    }
    let mut voters = consensus.voters.clone();
    voters.sort();
    voters.dedup();
    if voters.is_empty()
        || voters.len() > limits.max_voters
        || voters.len() != consensus.voters.len()
    {
        return Err(rollout_error(
            "cluster schema rollout voter set is empty, duplicate, or outside its bound",
        ));
    }
    let topology_voters = topology
        .nodes
        .iter()
        .filter(|(_, node)| node.metadata_role == MetadataMemberRole::Voter)
        .map(|(node_id, _)| node_id.clone())
        .collect::<BTreeSet<_>>();
    if voters.iter().cloned().collect::<BTreeSet<_>>() != topology_voters {
        return Err(rollout_error(
            "committed consensus voters do not match topology metadata voters",
        ));
    }
    let mut base = None::<String>;
    for voter in &voters {
        let node = topology.nodes.get(voter).ok_or_else(|| {
            rollout_error(format!(
                "cluster schema voter {voter} is absent from topology"
            ))
        })?;
        if node.lifecycle != ClusterNodeLifecycle::Active
            || node.metadata_role != MetadataMemberRole::Voter
            || node
                .labels
                .contains_key(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
        {
            return Err(rollout_error(format!(
                "cluster schema voter {voter} is not active or already has a pending target"
            )));
        }
        let fingerprint = node
            .labels
            .get(SCHEMA_COMPATIBILITY_NODE_LABEL)
            .filter(|value| value.len() == 64 && hex::decode(value).is_ok())
            .ok_or_else(|| {
                rollout_error(format!(
                    "cluster schema voter {voter} has no valid advertised fingerprint"
                ))
            })?;
        if base.as_ref().is_some_and(|base| base != fingerprint) {
            return Err(rollout_error(
                "cluster schema voters do not share one base fingerprint",
            ));
        }
        base = Some(fingerprint.clone());
    }
    Ok((voters, base.expect("non-empty voter set")))
}

fn validate_fenced_authority(
    topology: &ClusterTopology,
    consensus: &MetadataConsensusStatus,
    state: &ClusterSchemaRolloutState,
) -> Result<()> {
    topology.validate()?;
    let target = &state.signed_bundle.bundle.fingerprint.sha256;
    let mut voters = consensus.voters.clone();
    voters.sort();
    voters.dedup();
    if topology.cluster_id != state.cluster_id
        || topology.generation != state.topology_generation.saturating_add(1)
        || consensus.cluster_id != state.cluster_id
        || consensus.topology_generation != topology.generation
        || consensus.role != MetadataConsensusRole::Leader
        || consensus.leader_id.as_ref() != Some(&consensus.node_id)
        || voters != state.voters
        || topology.metadata_voters() != state.voters.iter().cloned().collect()
        || topology
            .relocations
            .values()
            .any(|relocation| relocation.is_active())
    {
        return Err(rollout_error(
            "cluster schema activation fence is not the exact committed voter topology",
        ));
    }
    for voter in &state.voters {
        let node = topology.nodes.get(voter).ok_or_else(|| {
            rollout_error(format!("cluster schema activation voter {voter} is absent"))
        })?;
        let labels_valid = if state.compatibility_window {
            node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL) == Some(&state.base_fingerprint_sha256)
                && node.labels.get(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL) == Some(target)
        } else {
            node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL) == Some(target)
                && !node
                    .labels
                    .contains_key(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
        };
        if node.lifecycle != ClusterNodeLifecycle::Active
            || node.metadata_role != MetadataMemberRole::Voter
            || !labels_valid
        {
            return Err(rollout_error(format!(
                "cluster schema activation voter {voter} does not match the committed compatibility authority"
            )));
        }
    }
    if state.phase != ClusterSchemaRolloutPhase::ReadyForActivation
        && state.activation_topology_generation != Some(topology.generation)
    {
        return Err(rollout_error(
            "cluster schema activation durable fence generation does not match consensus",
        ));
    }
    Ok(())
}

fn validate_promoted_authority(
    topology: &ClusterTopology,
    consensus: &MetadataConsensusStatus,
    state: &ClusterSchemaRolloutState,
) -> Result<()> {
    topology.validate()?;
    let target = &state.signed_bundle.bundle.fingerprint.sha256;
    let mut voters = consensus.voters.clone();
    voters.sort();
    voters.dedup();
    if !state.compatibility_window
        || topology.cluster_id != state.cluster_id
        || topology.generation != state.topology_generation.saturating_add(2)
        || consensus.cluster_id != state.cluster_id
        || consensus.topology_generation != topology.generation
        || consensus.role != MetadataConsensusRole::Leader
        || consensus.leader_id.as_ref() != Some(&consensus.node_id)
        || voters != state.voters
        || topology.metadata_voters() != state.voters.iter().cloned().collect()
        || topology
            .relocations
            .values()
            .any(|relocation| relocation.is_active())
    {
        return Err(rollout_error(
            "cluster schema promotion is not the exact committed voter topology",
        ));
    }
    for voter in &state.voters {
        let node = topology.nodes.get(voter).ok_or_else(|| {
            rollout_error(format!("cluster schema promoted voter {voter} is absent"))
        })?;
        if node.lifecycle != ClusterNodeLifecycle::Active
            || node.metadata_role != MetadataMemberRole::Voter
            || node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL) != Some(target)
            || node
                .labels
                .contains_key(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
        {
            return Err(rollout_error(format!(
                "cluster schema voter {voter} is not promoted at the target fingerprint"
            )));
        }
    }
    if state.activation_topology_generation != state.topology_generation.checked_add(1)
        || (state.phase != ClusterSchemaRolloutPhase::Complete
            && state.promotion_topology_generation != Some(topology.generation))
    {
        return Err(rollout_error(
            "cluster schema durable window or promotion generation does not match consensus",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    use crate::distribution::{
        ClusterNode, DistributionConfig, DistributionStore, SCHEMA_COMPATIBILITY_NODE_LABEL,
        SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL,
    };
    use crate::distribution_consensus::{MetadataAppendResponse, MetadataConsensusStore};

    struct FakeTransport {
        receipts: BTreeMap<ClusterNodeId, ClusterSchemaStageReceipt>,
        calls: Vec<ClusterNodeId>,
        lose_once_for: Option<ClusterNodeId>,
    }

    struct FakeActivationTransport {
        calls: BTreeMap<ClusterNodeId, u64>,
        activation_ids: BTreeMap<ClusterNodeId, Uuid>,
        lose_complete_once_for: Option<ClusterNodeId>,
    }

    struct FakeFinalizationTransport {
        calls: BTreeMap<ClusterNodeId, u64>,
        receipts: BTreeMap<ClusterNodeId, ClusterSchemaFinalizationReceipt>,
        lose_once_for: Option<ClusterNodeId>,
    }

    impl ClusterSchemaActivationTransport for FakeActivationTransport {
        fn advance_signed_schema_activation(
            &mut self,
            node_id: &ClusterNodeId,
            rollout_id: Uuid,
            stage_id: Uuid,
            _base_fingerprint_sha256: &str,
            target_fingerprint_sha256: &str,
        ) -> Result<ClusterSchemaActivationReceipt> {
            let calls = self.calls.entry(node_id.clone()).or_default();
            *calls += 1;
            let complete = *calls >= 2;
            let activation_id = *self
                .activation_ids
                .entry(node_id.clone())
                .or_insert_with(Uuid::now_v7);
            let receipt = ClusterSchemaActivationReceipt {
                rollout_id,
                node_id: node_id.clone(),
                stage_id,
                activation_id,
                phase: if complete {
                    crate::ClusterSchemaActivationPhase::Complete
                } else {
                    crate::ClusterSchemaActivationPhase::Collections
                },
                target_fingerprint_sha256: target_fingerprint_sha256.to_string(),
                live_fingerprint_sha256: if complete {
                    target_fingerprint_sha256.to_string()
                } else {
                    "22".repeat(32)
                },
                activation_state_checksum_sha256: hex::encode(Sha256::digest(format!(
                    "{node_id}:{calls}"
                ))),
                inspected_items: 1,
                applied_change: true,
                updated_at_ms: 200 + *calls,
            };
            if complete && self.lose_complete_once_for.as_ref() == Some(node_id) {
                self.lose_complete_once_for = None;
                return Err(rollout_error("simulated lost activation response"));
            }
            Ok(receipt)
        }
    }

    impl ClusterSchemaFinalizationTransport for FakeFinalizationTransport {
        fn finalize_signed_schema_activation(
            &mut self,
            node_id: &ClusterNodeId,
            activation: &ClusterSchemaActivationReceipt,
        ) -> Result<ClusterSchemaFinalizationReceipt> {
            *self.calls.entry(node_id.clone()).or_default() += 1;
            let receipt = self
                .receipts
                .entry(node_id.clone())
                .or_insert_with(|| ClusterSchemaFinalizationReceipt {
                    rollout_id: activation.rollout_id,
                    node_id: node_id.clone(),
                    stage_id: activation.stage_id,
                    activation_id: activation.activation_id,
                    target_fingerprint_sha256: activation.target_fingerprint_sha256.clone(),
                    activation_state_checksum_sha256: activation
                        .activation_state_checksum_sha256
                        .clone(),
                    finalization_checksum_sha256: hex::encode(Sha256::digest(format!(
                        "finalized:{node_id}:{}",
                        activation.activation_id
                    ))),
                    finalized_at_ms: 300,
                })
                .clone();
            if self.lose_once_for.as_ref() == Some(node_id) {
                self.lose_once_for = None;
                return Err(rollout_error("simulated lost finalization response"));
            }
            Ok(receipt)
        }
    }

    impl ClusterSchemaStageTransport for FakeTransport {
        fn stage_signed_schema_bundle(
            &mut self,
            node_id: &ClusterNodeId,
            signed_bundle: &SignedClusterSchemaBundle,
        ) -> Result<ClusterSchemaStageReceipt> {
            self.calls.push(node_id.clone());
            let next = self.receipts.len() as u128 + 1;
            let receipt = self
                .receipts
                .entry(node_id.clone())
                .or_insert_with(|| ClusterSchemaStageReceipt {
                    stage_id: Uuid::from_u128(next),
                    base_fingerprint_sha256: "11".repeat(32),
                    target_fingerprint_sha256: signed_bundle.bundle.fingerprint.sha256.clone(),
                    stage_checksum_sha256: hex::encode(Sha256::digest(node_id.as_str().as_bytes())),
                })
                .clone();
            if self.lose_once_for.as_ref() == Some(node_id) {
                self.lose_once_for = None;
                return Err(rollout_error("simulated lost stage response"));
            }
            Ok(receipt)
        }
    }

    fn topology(root: &Path) -> ClusterTopology {
        let config = DistributionConfig {
            enabled: true,
            cluster_id: crate::ClusterId::new("schema-rollout").unwrap(),
            node_id: ClusterNodeId::new("n1").unwrap(),
            node_address: "127.0.0.1:9001".to_string(),
            replication_factor: 3,
            initial_ranges: 1,
            ..DistributionConfig::default()
        };
        let mut store = DistributionStore::initialize_at(root, config, false, 1).unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        for (id, port) in [("n2", 9002), ("n3", 9003)] {
            store
                .join_node(
                    ClusterNode::new(
                        ClusterNodeId::new(id).unwrap(),
                        format!("127.0.0.1:{port}"),
                        1,
                        1_000_000,
                        2,
                    )
                    .unwrap(),
                    &actor,
                    2,
                )
                .unwrap();
        }
        let mut topology = store.topology().clone();
        for node in topology.nodes.values_mut() {
            node.labels
                .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "11".repeat(32));
        }
        topology.validate().unwrap();
        topology
    }

    fn signed_bundle(root: &Path) -> (SignedClusterSchemaBundle, BTreeMap<String, [u8; 32]>) {
        let mut db =
            crate::BicDb::open_with_config(root, crate::DbConfig::default().with_fsync(false))
                .unwrap();
        db.create_collection("items").unwrap();
        db.create_collection("new_items").unwrap();
        let bundle = db.cluster_schema_bundle().unwrap();
        let key = SigningKey::from_bytes(&[31; 32]);
        let signed = SignedClusterSchemaBundle {
            format_version: crate::SIGNED_CLUSTER_SCHEMA_BUNDLE_FORMAT_VERSION,
            signature_ed25519_hex: hex::encode(
                key.sign(&bundle.signing_message().unwrap()).to_bytes(),
            ),
            signer_key_id: "release-key".to_string(),
            bundle,
        };
        let trusted = BTreeMap::from([("release-key".to_string(), key.verifying_key().to_bytes())]);
        (signed, trusted)
    }

    #[test]
    fn rollout_checkpoints_each_voter_and_recovers_a_lost_response() {
        let temp = tempfile::tempdir().unwrap();
        let topology = topology(&temp.path().join("topology"));
        let consensus = MetadataConsensusStatus {
            cluster_id: topology.cluster_id.clone(),
            node_id: ClusterNodeId::new("n1").unwrap(),
            role: MetadataConsensusRole::Leader,
            current_term: 1,
            voted_for: Some(ClusterNodeId::new("n1").unwrap()),
            leader_id: Some(ClusterNodeId::new("n1").unwrap()),
            commit_index: 1,
            last_log_index: 1,
            last_log_term: 1,
            topology_generation: topology.generation,
            voters: vec![
                ClusterNodeId::new("n1").unwrap(),
                ClusterNodeId::new("n2").unwrap(),
                ClusterNodeId::new("n3").unwrap(),
            ],
            learners: Vec::new(),
        };
        let (signed, trusted) = signed_bundle(&temp.path().join("desired"));
        let limits = ClusterSchemaRolloutLimits::default();
        let run_root = temp.path().join("run");
        std::fs::create_dir_all(&run_root).unwrap();
        let mut run = ClusterSchemaRolloutRun::begin(
            &run_root,
            &topology,
            &consensus,
            signed,
            trusted.clone(),
            limits,
            false,
            100,
        )
        .unwrap();
        let mut transport = FakeTransport {
            receipts: BTreeMap::new(),
            calls: Vec::new(),
            lose_once_for: Some(ClusterNodeId::new("n2").unwrap()),
        };
        assert!(matches!(
            run.advance(&topology, &consensus, &mut transport, 101)
                .unwrap(),
            ClusterSchemaRolloutAdvance::Progress(_)
        ));
        assert!(run
            .advance(&topology, &consensus, &mut transport, 102)
            .unwrap_err()
            .to_string()
            .contains("lost stage response"));
        assert_eq!(run.state().next_voter, 1);
        drop(run);

        let mut reopened =
            ClusterSchemaRolloutRun::open(&run_root, trusted.clone(), limits, false).unwrap();
        assert!(matches!(
            reopened
                .advance(&topology, &consensus, &mut transport, 103)
                .unwrap(),
            ClusterSchemaRolloutAdvance::Progress(_)
        ));
        let ready = reopened
            .advance(&topology, &consensus, &mut transport, 104)
            .unwrap();
        assert!(matches!(
            ready,
            ClusterSchemaRolloutAdvance::ReadyForActivation(_)
        ));
        assert_eq!(reopened.state().receipts.len(), 3);
        assert_eq!(
            transport
                .calls
                .iter()
                .filter(|node| node.as_str() == "n2")
                .count(),
            2
        );

        let mut changed = topology.clone();
        changed.generation += 1;
        assert!(reopened
            .advance(&changed, &consensus, &mut transport, 105)
            .unwrap_err()
            .to_string()
            .contains("exact committed topology"));

        let consensus_root = temp.path().join("consensus");
        let mut leader = MetadataConsensusStore::open(
            &consensus_root,
            topology.cluster_id.clone(),
            ClusterNodeId::new("n1").unwrap(),
            topology.clone(),
            false,
        )
        .unwrap();
        let election = leader.start_election().unwrap();
        leader
            .become_leader(&BTreeSet::from([
                ClusterNodeId::new("n1").unwrap(),
                ClusterNodeId::new("n2").unwrap(),
            ]))
            .unwrap();
        let window = reopened
            .compatibility_window_topology(&topology, &leader.status(), 106)
            .unwrap();
        assert_eq!(window.generation, topology.generation + 1);
        for voter in &consensus.voters {
            assert_eq!(
                window.nodes[voter]
                    .labels
                    .get(SCHEMA_COMPATIBILITY_NODE_LABEL),
                Some(&"11".repeat(32)),
                "the compatibility window must keep the base active"
            );
            assert_eq!(
                window.nodes[voter]
                    .labels
                    .get(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL),
                Some(&reopened.state.signed_bundle.bundle.fingerprint.sha256)
            );
            assert_eq!(
                topology.nodes[voter]
                    .labels
                    .get(SCHEMA_COMPATIBILITY_NODE_LABEL),
                Some(&"11".repeat(32)),
                "the uncommitted candidate cannot mutate the published topology"
            );
        }
        let entry = leader.propose_topology(window.clone()).unwrap();
        assert_eq!(leader.committed_topology(), &topology);
        let response = MetadataAppendResponse {
            cluster_id: topology.cluster_id.clone(),
            term: election.term,
            node_id: ClusterNodeId::new("n2").unwrap(),
            success: true,
            match_index: entry.index,
            conflict_index: entry.index + 1,
        };
        assert!(leader
            .record_append_response(&ClusterNodeId::new("n2").unwrap(), &response)
            .unwrap());
        assert_eq!(leader.committed_topology(), &window);

        let observed = reopened
            .observe_activation_fence(&window, &leader.status(), 107)
            .unwrap();
        assert_eq!(observed.phase, ClusterSchemaRolloutPhase::ActivationFenced);
        assert_eq!(
            observed.activation_topology_generation,
            Some(window.generation)
        );
        drop(reopened);
        let mut recovered =
            ClusterSchemaRolloutRun::open(&run_root, trusted.clone(), limits, false).unwrap();
        assert_eq!(
            recovered.state().phase,
            ClusterSchemaRolloutPhase::ActivationFenced
        );

        let mut activation = FakeActivationTransport {
            calls: BTreeMap::new(),
            activation_ids: BTreeMap::new(),
            lose_complete_once_for: Some(ClusterNodeId::new("n2").unwrap()),
        };
        let mut saw_lost_response = false;
        for now_ms in 108..130 {
            match recovered.advance_activation(&window, &leader.status(), &mut activation, now_ms) {
                Ok(ClusterSchemaActivationRolloutAdvance::Complete(_)) => break,
                Ok(ClusterSchemaActivationRolloutAdvance::Progress { .. }) => {}
                Err(error) if error.to_string().contains("lost activation response") => {
                    saw_lost_response = true;
                    drop(recovered);
                    recovered =
                        ClusterSchemaRolloutRun::open(&run_root, trusted.clone(), limits, false)
                            .unwrap();
                }
                Err(error) => panic!("unexpected activation error: {error}"),
            }
        }
        assert!(saw_lost_response);
        assert_eq!(recovered.state().phase, ClusterSchemaRolloutPhase::Complete);
        assert_eq!(recovered.state().activation_receipts.len(), 3);
        assert_eq!(
            activation.calls[&ClusterNodeId::new("n2").unwrap()],
            3,
            "a lost complete response must retry the same voter"
        );

        let promoted = recovered
            .promotion_topology(&window, &leader.status(), 130)
            .unwrap();
        assert_eq!(promoted.generation, topology.generation + 2);
        for voter in &consensus.voters {
            assert_eq!(
                promoted.nodes[voter]
                    .labels
                    .get(SCHEMA_COMPATIBILITY_NODE_LABEL),
                Some(&recovered.state.signed_bundle.bundle.fingerprint.sha256)
            );
            assert!(!promoted.nodes[voter]
                .labels
                .contains_key(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL));
        }
        assert!(recovered
            .advance_finalization(
                &window,
                &leader.status(),
                &mut FakeFinalizationTransport {
                    calls: BTreeMap::new(),
                    receipts: BTreeMap::new(),
                    lose_once_for: None,
                },
                130,
            )
            .unwrap_err()
            .to_string()
            .contains("requires quorum-committed target promotion"));
        let promotion_entry = leader.propose_topology(promoted.clone()).unwrap();
        assert_eq!(leader.committed_topology(), &window);
        let promotion_response = MetadataAppendResponse {
            cluster_id: topology.cluster_id.clone(),
            term: election.term,
            node_id: ClusterNodeId::new("n2").unwrap(),
            success: true,
            match_index: promotion_entry.index,
            conflict_index: promotion_entry.index + 1,
        };
        assert!(leader
            .record_append_response(&ClusterNodeId::new("n2").unwrap(), &promotion_response,)
            .unwrap());
        assert_eq!(leader.committed_topology(), &promoted);
        let promoted_state = recovered
            .observe_promotion(&promoted, &leader.status(), 131)
            .unwrap();
        assert_eq!(promoted_state.phase, ClusterSchemaRolloutPhase::Promoted);
        assert_eq!(
            promoted_state.promotion_topology_generation,
            Some(promoted.generation)
        );

        let mut finalization = FakeFinalizationTransport {
            calls: BTreeMap::new(),
            receipts: BTreeMap::new(),
            lose_once_for: Some(ClusterNodeId::new("n2").unwrap()),
        };
        let mut saw_lost_finalization = false;
        for now_ms in 132..147 {
            match recovered.advance_finalization(
                &promoted,
                &leader.status(),
                &mut finalization,
                now_ms,
            ) {
                Ok(ClusterSchemaFinalizationRolloutAdvance::Finalized(_)) => break,
                Ok(ClusterSchemaFinalizationRolloutAdvance::Progress(_)) => {}
                Err(error) if error.to_string().contains("lost finalization response") => {
                    saw_lost_finalization = true;
                    drop(recovered);
                    recovered =
                        ClusterSchemaRolloutRun::open(&run_root, trusted.clone(), limits, false)
                            .unwrap();
                }
                Err(error) => panic!("unexpected finalization error: {error}"),
            }
        }
        assert!(saw_lost_finalization);
        assert_eq!(
            recovered.state().phase,
            ClusterSchemaRolloutPhase::Finalized
        );
        assert_eq!(recovered.state().finalization_receipts.len(), 3);
        assert_eq!(
            finalization.calls[&ClusterNodeId::new("n2").unwrap()],
            2,
            "a lost finalization response must retry the same voter"
        );
    }
}
