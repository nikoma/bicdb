//! Durable orchestration for quorum-fenced replica digest runs.
//!
//! Each invocation advances at most one voter by one node-owned bounded step,
//! then atomically checkpoints coordinator progress. Remote state is always
//! fetched before compare-and-advance, so a coordinator crash after the remote
//! write but before the local checkpoint is recovered rather than replayed.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{ClusterId, ClusterNodeId, RangeDescriptor, RangeId, RangeReplicaRole};
use crate::distribution_anti_entropy::{
    compare_range_digest_manifests, RangeDigestLimits, RangeDigestManifest, RangeDigestState,
    RangeDigestTransport,
};
use crate::distribution_range_consensus::RangeBackupFenceQuorum;
use crate::error::{BicDbError, Result};

pub const RANGE_DIGEST_RUN_FORMAT_VERSION: u32 = 1;

fn run_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("range digest run: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRunLimits {
    pub digest: RangeDigestLimits,
    pub max_replicas: usize,
    pub max_steps: u64,
    pub max_run_state_bytes: u64,
}

impl Default for RangeDigestRunLimits {
    fn default() -> Self {
        Self {
            digest: RangeDigestLimits::default(),
            max_replicas: 9,
            max_steps: 10_000_000,
            max_run_state_bytes: 128 * 1024 * 1024,
        }
    }
}

impl RangeDigestRunLimits {
    pub fn validate(&self) -> Result<()> {
        self.digest.validate()?;
        if !(1..=64).contains(&self.max_replicas)
            || !(1..=1_000_000_000).contains(&self.max_steps)
            || !(64 * 1024..=512 * 1024 * 1024).contains(&self.max_run_state_bytes)
        {
            return Err(run_error("replica, step, or state limit is outside bounds"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestReplicaProgress {
    pub node_id: ClusterNodeId,
    pub checkpoint_sha256: Option<String>,
    pub resume_after_key: Option<String>,
    pub scanned_records: u64,
    pub serialized_bytes: u64,
    pub manifest: Option<RangeDigestManifest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRootEvidence {
    pub root_sha256: String,
    pub node_ids: Vec<ClusterNodeId>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RangeDigestRunOutcome {
    Healthy,
    DivergentCertifiedSource,
    DivergentUncertified,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRunReport {
    pub session_id: Uuid,
    pub cluster_id: ClusterId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub resolved_through: u64,
    pub required_quorum: usize,
    pub outcome: RangeDigestRunOutcome,
    pub root_evidence: Vec<RangeDigestRootEvidence>,
    pub certified_source_root_sha256: Option<String>,
    pub divergent_buckets: Vec<u32>,
    pub checksum_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRun {
    pub format_version: u32,
    pub session_id: Uuid,
    pub cluster_id: ClusterId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub start_token: u64,
    pub end_token: Option<u64>,
    pub resolved_through: u64,
    pub required_quorum: usize,
    pub installed_at_ms: u64,
    pub expires_at_ms: u64,
    pub limits: RangeDigestRunLimits,
    pub replicas: Vec<RangeDigestReplicaProgress>,
    pub next_replica: usize,
    pub completed_steps: u64,
    pub completed: bool,
    pub report: Option<RangeDigestRunReport>,
    pub checksum_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangeDigestRunAdvance {
    Progress {
        node_id: ClusterNodeId,
        completed_replicas: usize,
        total_replicas: usize,
    },
    Complete(RangeDigestRunReport),
}

impl RangeDigestRun {
    pub fn create(
        cluster_id: ClusterId,
        range: &RangeDescriptor,
        fence: &RangeBackupFenceQuorum,
        limits: RangeDigestRunLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if fence.plan_id.is_nil()
            || fence.range_id != range.id
            || fence.range_epoch != range.epoch
            || fence.required_quorum != range.voter_count() / 2 + 1
            || fence.observations.len() < fence.required_quorum
            || fence.observations.len() > limits.max_replicas
            || fence.installed_at_ms >= fence.expires_at_ms
        {
            return Err(run_error("range fence identity or quorum is invalid"));
        }
        let voters = range
            .replicas
            .iter()
            .filter(|replica| replica.role == RangeReplicaRole::Voter)
            .map(|replica| replica.node_id.clone())
            .collect::<BTreeSet<_>>();
        let mut selected = BTreeSet::new();
        for observation in &fence.observations {
            if !voters.contains(&observation.node_id)
                || !selected.insert(observation.node_id.clone())
                || observation.range_id != range.id
                || observation.current_epoch != range.epoch
                || observation.last_index != fence.resolved_through
                || observation.resolved_through != fence.resolved_through
            {
                return Err(run_error(
                    "range fence observations are duplicated, divergent, or not current voters",
                ));
            }
        }
        let replicas = selected
            .into_iter()
            .map(|node_id| RangeDigestReplicaProgress {
                node_id,
                checkpoint_sha256: None,
                resume_after_key: None,
                scanned_records: 0,
                serialized_bytes: 0,
                manifest: None,
            })
            .collect();
        let mut run = Self {
            format_version: RANGE_DIGEST_RUN_FORMAT_VERSION,
            session_id: fence.plan_id,
            cluster_id,
            range_id: range.id,
            range_epoch: range.epoch,
            start_token: range.start_token,
            end_token: range.end_token,
            resolved_through: fence.resolved_through,
            required_quorum: fence.required_quorum,
            installed_at_ms: fence.installed_at_ms,
            expires_at_ms: fence.expires_at_ms,
            limits,
            replicas,
            next_replica: 0,
            completed_steps: 0,
            completed: false,
            report: None,
            checksum_sha256: String::new(),
        };
        run.refresh_checksum()?;
        run.validate()?;
        Ok(run)
    }

    pub fn advance<T: RangeDigestTransport>(
        &mut self,
        transport: &T,
        now_ms: u64,
    ) -> Result<RangeDigestRunAdvance> {
        self.validate()?;
        if self.completed {
            return Ok(RangeDigestRunAdvance::Complete(
                self.report
                    .clone()
                    .ok_or_else(|| run_error("completed run has no report"))?,
            ));
        }
        if now_ms < self.installed_at_ms || now_ms >= self.expires_at_ms {
            return Err(run_error("range digest fence is not currently valid"));
        }
        if self.completed_steps >= self.limits.max_steps {
            return Err(run_error("range digest run exhausted its step budget"));
        }
        let replica_index = self.next_incomplete_replica()?;
        let node_id = self.replicas[replica_index].node_id.clone();
        let status = transport.advance_range_digest(
            &node_id,
            self.range_id,
            self.range_epoch,
            self.session_id,
            None,
            &self.limits.digest,
            now_ms,
        )?;
        self.validate_remote_state(&node_id, &status)?;

        let previous = &self.replicas[replica_index];
        let state = match previous.checkpoint_sha256.as_deref() {
            Some(expected) if status.checksum_sha256 == expected && !status.completed => {
                let advanced = transport.advance_range_digest(
                    &node_id,
                    self.range_id,
                    self.range_epoch,
                    self.session_id,
                    Some(expected),
                    &self.limits.digest,
                    now_ms,
                )?;
                self.validate_remote_state(&node_id, &advanced)?;
                if advanced.checksum_sha256 == expected {
                    return Err(run_error(format!(
                        "node {node_id} acknowledged a digest advance without changing state"
                    )));
                }
                advanced
            }
            _ => status,
        };
        self.ensure_monotonic_replica_progress(replica_index, &state)?;
        let manifest = state
            .completed
            .then(|| state.manifest(&self.limits.digest))
            .transpose()?;
        self.replicas[replica_index] = RangeDigestReplicaProgress {
            node_id: node_id.clone(),
            checkpoint_sha256: Some(state.checksum_sha256),
            resume_after_key: state.resume_after_key,
            scanned_records: state.scanned_records,
            serialized_bytes: state.serialized_bytes,
            manifest,
        };
        self.completed_steps = self.completed_steps.saturating_add(1);
        self.next_replica = (replica_index + 1) % self.replicas.len();
        let completed_replicas = self
            .replicas
            .iter()
            .filter(|replica| replica.manifest.is_some())
            .count();
        let result = if completed_replicas == self.replicas.len() {
            let report = self.build_report()?;
            self.completed = true;
            self.report = Some(report.clone());
            RangeDigestRunAdvance::Complete(report)
        } else {
            RangeDigestRunAdvance::Progress {
                node_id,
                completed_replicas,
                total_replicas: self.replicas.len(),
            }
        };
        self.refresh_checksum()?;
        self.validate()?;
        Ok(result)
    }

    pub fn advance_and_checkpoint<T: RangeDigestTransport>(
        &mut self,
        path: impl AsRef<Path>,
        transport: &T,
        now_ms: u64,
        fsync: bool,
    ) -> Result<RangeDigestRunAdvance> {
        let result = self.advance(transport, now_ms)?;
        save_range_digest_run(path, self, fsync)?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        if self.format_version != RANGE_DIGEST_RUN_FORMAT_VERSION
            || self.session_id.is_nil()
            || self.range_epoch == 0
            || self.end_token.is_some_and(|end| end <= self.start_token)
            || self.required_quorum == 0
            || self.required_quorum > self.replicas.len()
            || self.replicas.is_empty()
            || self.replicas.len() > self.limits.max_replicas
            || self.next_replica >= self.replicas.len()
            || self.completed_steps > self.limits.max_steps
            || self.installed_at_ms >= self.expires_at_ms
        {
            return Err(run_error(
                "run identity, quorum, cursor, or bounds are invalid",
            ));
        }
        let mut nodes = BTreeSet::new();
        for replica in &self.replicas {
            if !nodes.insert(replica.node_id.clone()) {
                return Err(run_error("run contains duplicate replica identities"));
            }
            if let Some(checksum) = &replica.checkpoint_sha256 {
                validate_sha256(checksum)?;
            } else if replica.resume_after_key.is_some()
                || replica.scanned_records != 0
                || replica.serialized_bytes != 0
                || replica.manifest.is_some()
            {
                return Err(run_error("uninitialized replica carries digest progress"));
            }
            if replica
                .resume_after_key
                .as_ref()
                .is_some_and(|cursor| cursor.len() > 16 * 1024)
            {
                return Err(run_error("replica resume cursor exceeds its bound"));
            }
            if let Some(manifest) = &replica.manifest {
                manifest.validate(&self.limits.digest)?;
                self.validate_manifest_identity(&replica.node_id, manifest)?;
                if manifest.scanned_records != replica.scanned_records
                    || manifest.serialized_bytes != replica.serialized_bytes
                {
                    return Err(run_error("replica manifest totals disagree with progress"));
                }
            }
        }
        let all_complete = self
            .replicas
            .iter()
            .all(|replica| replica.manifest.is_some());
        if self.completed != all_complete || self.completed != self.report.is_some() {
            return Err(run_error("run completion, manifests, and report disagree"));
        }
        if let Some(report) = &self.report {
            report.validate()?;
            if *report != self.build_report()? {
                return Err(run_error("stored run report does not match manifests"));
            }
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(run_error("run state checksum mismatch"));
        }
        if serde_json::to_vec(self)?.len() as u64 > self.limits.max_run_state_bytes {
            return Err(run_error("run state exceeds its byte bound"));
        }
        Ok(())
    }

    fn next_incomplete_replica(&self) -> Result<usize> {
        (0..self.replicas.len())
            .map(|offset| (self.next_replica + offset) % self.replicas.len())
            .find(|index| self.replicas[*index].manifest.is_none())
            .ok_or_else(|| run_error("incomplete run has no pending replica"))
    }

    fn validate_remote_state(
        &self,
        node_id: &ClusterNodeId,
        state: &RangeDigestState,
    ) -> Result<()> {
        state.validate(&self.limits.digest)?;
        if state.session_id != self.session_id
            || state.cluster_id != self.cluster_id
            || &state.node_id != node_id
            || state.range_id != self.range_id
            || state.range_epoch != self.range_epoch
            || state.start_token != self.start_token
            || state.end_token != self.end_token
            || state.resolved_through != self.resolved_through
        {
            return Err(run_error(format!(
                "node {node_id} returned a digest outside the run fence"
            )));
        }
        Ok(())
    }

    fn ensure_monotonic_replica_progress(
        &self,
        replica_index: usize,
        state: &RangeDigestState,
    ) -> Result<()> {
        let previous = &self.replicas[replica_index];
        if previous.checkpoint_sha256.is_none() {
            return Ok(());
        }
        if state.scanned_records < previous.scanned_records
            || state.serialized_bytes < previous.serialized_bytes
            || (state.scanned_records == previous.scanned_records
                && state.serialized_bytes != previous.serialized_bytes)
        {
            return Err(run_error(format!(
                "node {} regressed its digest totals",
                previous.node_id
            )));
        }
        if state.checksum_sha256 != previous.checkpoint_sha256.as_deref().unwrap_or_default()
            && !state.completed
            && state.resume_after_key == previous.resume_after_key
        {
            return Err(run_error(format!(
                "node {} changed digest state without cursor or completion progress",
                previous.node_id
            )));
        }
        Ok(())
    }

    fn validate_manifest_identity(
        &self,
        node_id: &ClusterNodeId,
        manifest: &RangeDigestManifest,
    ) -> Result<()> {
        if manifest.session_id != self.session_id
            || manifest.cluster_id != self.cluster_id
            || &manifest.node_id != node_id
            || manifest.range_id != self.range_id
            || manifest.range_epoch != self.range_epoch
            || manifest.start_token != self.start_token
            || manifest.end_token != self.end_token
            || manifest.resolved_through != self.resolved_through
        {
            return Err(run_error("replica manifest identity differs from run"));
        }
        Ok(())
    }

    fn build_report(&self) -> Result<RangeDigestRunReport> {
        let manifests = self
            .replicas
            .iter()
            .map(|replica| {
                replica
                    .manifest
                    .as_ref()
                    .ok_or_else(|| run_error("cannot report before every replica completes"))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut roots = BTreeMap::<String, Vec<ClusterNodeId>>::new();
        for manifest in &manifests {
            roots
                .entry(manifest.root_sha256.clone())
                .or_default()
                .push(manifest.node_id.clone());
        }
        let root_evidence = roots
            .into_iter()
            .map(|(root_sha256, mut node_ids)| {
                node_ids.sort();
                RangeDigestRootEvidence {
                    root_sha256,
                    node_ids,
                }
            })
            .collect::<Vec<_>>();
        let certified_source_root_sha256 = root_evidence
            .iter()
            .find(|evidence| evidence.node_ids.len() >= self.required_quorum)
            .map(|evidence| evidence.root_sha256.clone());
        let outcome = if root_evidence.len() == 1 {
            RangeDigestRunOutcome::Healthy
        } else if certified_source_root_sha256.is_some() {
            RangeDigestRunOutcome::DivergentCertifiedSource
        } else {
            RangeDigestRunOutcome::DivergentUncertified
        };
        let baseline = manifests[0];
        let mut divergent_buckets = BTreeSet::new();
        for manifest in manifests.iter().skip(1) {
            divergent_buckets.extend(
                compare_range_digest_manifests(baseline, manifest, &self.limits.digest)?
                    .divergent_buckets,
            );
        }
        RangeDigestRunReport::create(
            self.session_id,
            self.cluster_id.clone(),
            self.range_id,
            self.range_epoch,
            self.resolved_through,
            self.required_quorum,
            outcome,
            root_evidence,
            certified_source_root_sha256,
            divergent_buckets.into_iter().collect(),
        )
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            session_id: Uuid,
            cluster_id: &'a ClusterId,
            range_id: RangeId,
            range_epoch: u64,
            start_token: u64,
            end_token: Option<u64>,
            resolved_through: u64,
            required_quorum: usize,
            installed_at_ms: u64,
            expires_at_ms: u64,
            limits: RangeDigestRunLimits,
            replicas: &'a [RangeDigestReplicaProgress],
            next_replica: usize,
            completed_steps: u64,
            completed: bool,
            report: &'a Option<RangeDigestRunReport>,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            session_id: self.session_id,
            cluster_id: &self.cluster_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            start_token: self.start_token,
            end_token: self.end_token,
            resolved_through: self.resolved_through,
            required_quorum: self.required_quorum,
            installed_at_ms: self.installed_at_ms,
            expires_at_ms: self.expires_at_ms,
            limits: self.limits,
            replicas: &self.replicas,
            next_replica: self.next_replica,
            completed_steps: self.completed_steps,
            completed: self.completed,
            report: &self.report,
        })
    }

    fn refresh_checksum(&mut self) -> Result<()> {
        self.checksum_sha256 = self.calculate_checksum()?;
        Ok(())
    }
}

impl RangeDigestRunReport {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create(
        session_id: Uuid,
        cluster_id: ClusterId,
        range_id: RangeId,
        range_epoch: u64,
        resolved_through: u64,
        required_quorum: usize,
        outcome: RangeDigestRunOutcome,
        root_evidence: Vec<RangeDigestRootEvidence>,
        certified_source_root_sha256: Option<String>,
        divergent_buckets: Vec<u32>,
    ) -> Result<Self> {
        let mut report = Self {
            session_id,
            cluster_id,
            range_id,
            range_epoch,
            resolved_through,
            required_quorum,
            outcome,
            root_evidence,
            certified_source_root_sha256,
            divergent_buckets,
            checksum_sha256: String::new(),
        };
        report.checksum_sha256 = report.calculate_checksum()?;
        report.validate()?;
        Ok(report)
    }

    pub fn validate(&self) -> Result<()> {
        if self.session_id.is_nil()
            || self.range_epoch == 0
            || self.required_quorum == 0
            || self.root_evidence.is_empty()
        {
            return Err(run_error("digest report identity or quorum is invalid"));
        }
        let mut previous_root = None::<&str>;
        let mut nodes = BTreeSet::new();
        for evidence in &self.root_evidence {
            validate_sha256(&evidence.root_sha256)?;
            if evidence.node_ids.is_empty()
                || previous_root.is_some_and(|previous| previous >= evidence.root_sha256.as_str())
            {
                return Err(run_error("digest root evidence is empty or non-canonical"));
            }
            previous_root = Some(&evidence.root_sha256);
            let mut previous_node = None::<&ClusterNodeId>;
            for node in &evidence.node_ids {
                if previous_node.is_some_and(|previous| previous >= node) || !nodes.insert(node) {
                    return Err(run_error("digest report node evidence is duplicated"));
                }
                previous_node = Some(node);
            }
        }
        let certified = self
            .root_evidence
            .iter()
            .filter(|evidence| evidence.node_ids.len() >= self.required_quorum)
            .collect::<Vec<_>>();
        if certified.len() > 1
            || self.certified_source_root_sha256.as_deref()
                != certified
                    .first()
                    .map(|evidence| evidence.root_sha256.as_str())
            || (self.outcome == RangeDigestRunOutcome::Healthy && self.root_evidence.len() != 1)
            || (self.outcome == RangeDigestRunOutcome::DivergentCertifiedSource
                && (self.root_evidence.len() < 2 || certified.len() != 1))
            || (self.outcome == RangeDigestRunOutcome::DivergentUncertified
                && (self.root_evidence.len() < 2 || !certified.is_empty()))
        {
            return Err(run_error(
                "digest report outcome disagrees with root evidence",
            ));
        }
        if !self
            .divergent_buckets
            .windows(2)
            .all(|pair| pair[0] < pair[1])
            || (self.root_evidence.len() == 1 && !self.divergent_buckets.is_empty())
        {
            return Err(run_error("digest report bucket evidence is non-canonical"));
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(run_error("digest report checksum mismatch"));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            session_id: Uuid,
            cluster_id: &'a ClusterId,
            range_id: RangeId,
            range_epoch: u64,
            resolved_through: u64,
            required_quorum: usize,
            outcome: RangeDigestRunOutcome,
            root_evidence: &'a [RangeDigestRootEvidence],
            certified_source_root_sha256: &'a Option<String>,
            divergent_buckets: &'a [u32],
        }
        sha256_json(&Payload {
            session_id: self.session_id,
            cluster_id: &self.cluster_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            resolved_through: self.resolved_through,
            required_quorum: self.required_quorum,
            outcome: self.outcome,
            root_evidence: &self.root_evidence,
            certified_source_root_sha256: &self.certified_source_root_sha256,
            divergent_buckets: &self.divergent_buckets,
        })
    }
}

pub fn save_range_digest_run(
    path: impl AsRef<Path>,
    run: &RangeDigestRun,
    fsync: bool,
) -> Result<()> {
    run.validate()?;
    let bytes = serde_json::to_vec(run)?;
    if bytes.len() as u64 > run.limits.max_run_state_bytes {
        return Err(run_error("range digest run exceeds its write bound"));
    }
    crate::storage::write_atomic(path.as_ref(), &bytes, fsync)
}

pub fn load_range_digest_run(
    path: impl AsRef<Path>,
    limits: &RangeDigestRunLimits,
) -> Result<RangeDigestRun> {
    limits.validate()?;
    let path = path.as_ref();
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > limits.max_run_state_bytes
    {
        return Err(run_error(
            "range digest run file is unsafe or outside its bound",
        ));
    }
    let length = metadata.len();
    let mut bytes = Vec::with_capacity(length as usize);
    File::open(path)?
        .take(limits.max_run_state_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || bytes.len() as u64 > limits.max_run_state_bytes {
        return Err(run_error("range digest run changed or grew while reading"));
    }
    let run: RangeDigestRun = serde_json::from_slice(&bytes)?;
    if run.limits != *limits {
        return Err(run_error(
            "range digest run limits differ from requested limits",
        ));
    }
    run.validate()?;
    Ok(run)
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(run_error("SHA-256 is not canonical lowercase hex"));
    }
    Ok(())
}

fn sha256_json(value: &impl Serialize) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use parking_lot::Mutex;
    use serde_json::json;

    use crate::distribution::{PlacementPolicy, RangeReplica, ReplicaId};
    use crate::distribution_range_consensus::RangeWriteProgress;
    use crate::Record;

    #[derive(Debug)]
    struct TestDigestTransport {
        cluster_id: ClusterId,
        range: RangeDescriptor,
        records: BTreeMap<ClusterNodeId, Vec<Record>>,
        states: Mutex<BTreeMap<ClusterNodeId, RangeDigestState>>,
        fail_after_advance_once: Mutex<Option<ClusterNodeId>>,
    }

    impl RangeDigestTransport for TestDigestTransport {
        fn advance_range_digest(
            &self,
            destination: &ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            session_id: Uuid,
            expected_checksum_sha256: Option<&str>,
            limits: &RangeDigestLimits,
            _now_ms: u64,
        ) -> Result<RangeDigestState> {
            let mut states = self.states.lock();
            let state = states.entry(destination.clone()).or_insert_with(|| {
                RangeDigestState::create(
                    session_id,
                    self.cluster_id.clone(),
                    destination.clone(),
                    range_id,
                    range_epoch,
                    self.range.start_token,
                    self.range.end_token,
                    0,
                    limits,
                )
                .unwrap()
            });
            let Some(expected) = expected_checksum_sha256 else {
                return Ok(state.clone());
            };
            if state.checksum_sha256 != expected {
                return Err(run_error("test transport stale checksum"));
            }
            if state.completed {
                return Ok(state.clone());
            }
            if state.scanned_records == 0 {
                let records = self.records.get(destination).unwrap();
                let bytes = records
                    .iter()
                    .map(|record| serde_json::to_vec(record).unwrap().len())
                    .sum();
                state.apply_batch(
                    None,
                    "records-complete",
                    "items",
                    range_id,
                    range_epoch,
                    records,
                    bytes,
                    limits,
                )?;
                if self.fail_after_advance_once.lock().as_ref() == Some(destination) {
                    *self.fail_after_advance_once.lock() = None;
                    return Err(run_error("simulated lost response after remote checkpoint"));
                }
            } else {
                let cursor = state.resume_after_key.clone();
                state.finish(cursor.as_deref(), limits)?;
            }
            Ok(state.clone())
        }
    }

    fn record(id: usize, value: usize) -> Record {
        Record::new(format!("item-{id:03}")).with_metadata(json!({"value": value}))
    }

    fn fixture(
        selected: usize,
        divergent_last: bool,
    ) -> (RangeDescriptor, RangeBackupFenceQuorum, TestDigestTransport) {
        let cluster_id = ClusterId::new("run-cluster").unwrap();
        let nodes = (0..3)
            .map(|index| ClusterNodeId::new(format!("node-{index}")).unwrap())
            .collect::<Vec<_>>();
        let range = RangeDescriptor {
            id: RangeId::new(8).unwrap(),
            start_token: 0,
            end_token: None,
            epoch: 4,
            replicas: nodes
                .iter()
                .enumerate()
                .map(|(index, node_id)| RangeReplica {
                    id: ReplicaId::new(index as u64 + 1).unwrap(),
                    node_id: node_id.clone(),
                    role: RangeReplicaRole::Voter,
                })
                .collect(),
            leader: nodes[0].clone(),
            approximate_bytes: 0,
            approximate_qps: 0,
            placement: PlacementPolicy::default(),
        };
        let observations = nodes
            .iter()
            .take(selected)
            .map(|node_id| RangeWriteProgress {
                node_id: node_id.clone(),
                range_id: range.id,
                current_epoch: range.epoch,
                last_index: 0,
                resolved_through: 0,
                compacted_through: 0,
            })
            .collect();
        let fence = RangeBackupFenceQuorum {
            plan_id: Uuid::new_v4(),
            range_id: range.id,
            range_epoch: range.epoch,
            resolved_through: 0,
            required_quorum: 2,
            installed_at_ms: 10,
            expires_at_ms: 10_000,
            observations,
        };
        let base = (0..5).map(|id| record(id, id)).collect::<Vec<_>>();
        let mut records = BTreeMap::new();
        for (index, node_id) in nodes.iter().take(selected).enumerate() {
            let mut replica = base.clone();
            if divergent_last && index + 1 == selected {
                replica[2] = record(2, 999);
            }
            records.insert(node_id.clone(), replica);
        }
        let transport = TestDigestTransport {
            cluster_id,
            range: range.clone(),
            records,
            states: Mutex::new(BTreeMap::new()),
            fail_after_advance_once: Mutex::new(Some(nodes[1].clone())),
        };
        (range, fence, transport)
    }

    fn finish_run(
        path: &Path,
        mut run: RangeDigestRun,
        transport: &TestDigestTransport,
    ) -> RangeDigestRunReport {
        save_range_digest_run(path, &run, false).unwrap();
        for _ in 0..50 {
            match run.advance_and_checkpoint(path, transport, 100, false) {
                Ok(RangeDigestRunAdvance::Progress { .. }) => {}
                Ok(RangeDigestRunAdvance::Complete(report)) => return report,
                Err(error) if error.to_string().contains("simulated lost response") => {
                    run = load_range_digest_run(path, &run.limits).unwrap();
                }
                Err(error) => panic!("unexpected run error: {error}"),
            }
        }
        panic!("digest run did not finish")
    }

    #[test]
    fn run_recovers_uncertain_remote_advance_and_certifies_matching_quorum() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("run.json");
        let (range, fence, transport) = fixture(2, false);
        let limits = RangeDigestRunLimits {
            digest: RangeDigestLimits {
                bucket_count: 16,
                max_records_per_batch: 16,
                ..RangeDigestLimits::default()
            },
            ..RangeDigestRunLimits::default()
        };
        let run =
            RangeDigestRun::create(transport.cluster_id.clone(), &range, &fence, limits).unwrap();
        let report = finish_run(&path, run, &transport);
        assert_eq!(report.outcome, RangeDigestRunOutcome::Healthy);
        assert_eq!(report.root_evidence.len(), 1);
        assert_eq!(report.root_evidence[0].node_ids.len(), 2);
        load_range_digest_run(&path, &limits).unwrap();

        let mut bytes = std::fs::read(&path).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(load_range_digest_run(&path, &limits).is_err());
    }

    #[test]
    fn disagreement_without_a_root_quorum_is_explicitly_uncertified() {
        let directory = tempfile::tempdir().unwrap();
        let (range, fence, transport) = fixture(2, true);
        let limits = RangeDigestRunLimits {
            digest: RangeDigestLimits {
                bucket_count: 16,
                max_records_per_batch: 16,
                ..RangeDigestLimits::default()
            },
            ..RangeDigestRunLimits::default()
        };
        let run =
            RangeDigestRun::create(transport.cluster_id.clone(), &range, &fence, limits).unwrap();
        let report = finish_run(&directory.path().join("run.json"), run, &transport);
        assert_eq!(report.outcome, RangeDigestRunOutcome::DivergentUncertified);
        assert!(report.certified_source_root_sha256.is_none());
        assert_eq!(report.divergent_buckets.len(), 1);
    }

    #[test]
    fn three_observations_can_certify_the_two_matching_sources() {
        let directory = tempfile::tempdir().unwrap();
        let (range, fence, transport) = fixture(3, true);
        *transport.fail_after_advance_once.lock() = None;
        let limits = RangeDigestRunLimits {
            digest: RangeDigestLimits {
                bucket_count: 16,
                max_records_per_batch: 16,
                ..RangeDigestLimits::default()
            },
            ..RangeDigestRunLimits::default()
        };
        let run =
            RangeDigestRun::create(transport.cluster_id.clone(), &range, &fence, limits).unwrap();
        let report = finish_run(&directory.path().join("run.json"), run, &transport);
        assert_eq!(
            report.outcome,
            RangeDigestRunOutcome::DivergentCertifiedSource
        );
        let certified = report.certified_source_root_sha256.unwrap();
        assert_eq!(
            report
                .root_evidence
                .iter()
                .find(|evidence| evidence.root_sha256 == certified)
                .unwrap()
                .node_ids
                .len(),
            2
        );
    }
}
