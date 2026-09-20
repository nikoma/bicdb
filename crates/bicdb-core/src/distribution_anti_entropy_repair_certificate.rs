//! Range-level certification for completed anti-entropy repairs.
//!
//! A certificate covers every node in the original fenced digest report. Each
//! divergent voter must supply one freshly verified repair run per divergent
//! bucket. Replacing those buckets in its original manifest must reconstruct
//! the quorum-certified source root exactly before fence release is eligible.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{ClusterId, ClusterNodeId, RangeId};
use crate::distribution_anti_entropy::{
    calculate_range_digest_root, RangeDigestBucket, RangeDigestLimits, RangeDigestManifest,
};
use crate::distribution_anti_entropy_repair_run::{
    RangeDigestRepairRun, RangeDigestRepairRunPhase,
};
use crate::distribution_anti_entropy_run::{RangeDigestRunOutcome, RangeDigestRunReport};
use crate::error::{BicDbError, Result};

pub const RANGE_DIGEST_REPAIR_CERTIFICATE_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_RANGE_DIGEST_REPAIR_CERTIFICATE: &str = "range-repair-certificate.json";

fn certificate_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("range repair certificate: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRepairCertificateLimits {
    pub digest: RangeDigestLimits,
    pub max_replicas: usize,
    pub max_bucket_evidences: usize,
    pub max_state_bytes: u64,
}

impl Default for RangeDigestRepairCertificateLimits {
    fn default() -> Self {
        Self {
            digest: RangeDigestLimits::default(),
            max_replicas: 9,
            max_bucket_evidences: 65_536,
            max_state_bytes: 256 * 1024 * 1024,
        }
    }
}

impl RangeDigestRepairCertificateLimits {
    pub fn validate(&self) -> Result<()> {
        self.digest.validate()?;
        if !(1..=64).contains(&self.max_replicas)
            || !(1..=4_194_304).contains(&self.max_bucket_evidences)
            || !(64 * 1024..=512 * 1024 * 1024).contains(&self.max_state_bytes)
        {
            return Err(certificate_error(
                "replica, bucket-evidence, or state bounds are invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestVerifiedBucketEvidence {
    pub repair_id: Uuid,
    pub digest_session_id: Uuid,
    pub cluster_id: ClusterId,
    pub source_node_id: ClusterNodeId,
    pub destination_node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub resolved_through: u64,
    pub bucket: u32,
    pub source_bucket: RangeDigestBucket,
    pub authority: RangeDigestRunReport,
    pub repair_run_checksum_sha256: String,
    pub checksum_sha256: String,
}

impl RangeDigestVerifiedBucketEvidence {
    pub fn create(run: &RangeDigestRepairRun, run_dir: impl AsRef<Path>) -> Result<Self> {
        run.validate()?;
        if run.phase != RangeDigestRepairRunPhase::Verified {
            return Err(certificate_error(
                "bucket evidence requires a freshly verified repair run",
            ));
        }
        let mut evidence = Self {
            repair_id: run.repair_id,
            digest_session_id: run.digest_session_id,
            cluster_id: run.cluster_id.clone(),
            source_node_id: run.source_node_id.clone(),
            destination_node_id: run.destination_node_id.clone(),
            range_id: run.range_id,
            range_epoch: run.range_epoch,
            resolved_through: run.resolved_through,
            bucket: run.bucket,
            source_bucket: run.verified_source_bucket(run_dir)?,
            authority: run.authority.clone(),
            repair_run_checksum_sha256: run.checksum_sha256.clone(),
            checksum_sha256: String::new(),
        };
        evidence.checksum_sha256 = evidence.calculate_checksum()?;
        evidence.validate(&run.limits.repair.digest)?;
        Ok(evidence)
    }

    pub fn validate(&self, limits: &RangeDigestLimits) -> Result<()> {
        limits.validate()?;
        self.authority.validate()?;
        if self.repair_id.is_nil()
            || self.digest_session_id != self.authority.session_id
            || self.cluster_id != self.authority.cluster_id
            || self.range_id != self.authority.range_id
            || self.range_epoch != self.authority.range_epoch
            || self.resolved_through != self.authority.resolved_through
            || self.authority.outcome != RangeDigestRunOutcome::DivergentCertifiedSource
            || self.source_bucket.bucket != self.bucket
            || self.bucket as usize >= limits.bucket_count
            || !self.authority.divergent_buckets.contains(&self.bucket)
            || !node_has_root(
                &self.authority,
                &self.source_node_id,
                self.authority
                    .certified_source_root_sha256
                    .as_deref()
                    .ok_or_else(|| certificate_error("authority has no certified source root"))?,
            )
            || node_has_root(
                &self.authority,
                &self.destination_node_id,
                self.authority
                    .certified_source_root_sha256
                    .as_deref()
                    .ok_or_else(|| certificate_error("authority has no certified source root"))?,
            )
        {
            return Err(certificate_error(
                "verified bucket identity or authority is invalid",
            ));
        }
        validate_sha256(&self.source_bucket.sha256)?;
        validate_sha256(&self.repair_run_checksum_sha256)?;
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(certificate_error("verified bucket checksum mismatch"));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            repair_id: Uuid,
            digest_session_id: Uuid,
            cluster_id: &'a ClusterId,
            source_node_id: &'a ClusterNodeId,
            destination_node_id: &'a ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            resolved_through: u64,
            bucket: u32,
            source_bucket: &'a RangeDigestBucket,
            authority: &'a RangeDigestRunReport,
            repair_run_checksum_sha256: &'a str,
        }
        sha256_json(&Payload {
            repair_id: self.repair_id,
            digest_session_id: self.digest_session_id,
            cluster_id: &self.cluster_id,
            source_node_id: &self.source_node_id,
            destination_node_id: &self.destination_node_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            resolved_through: self.resolved_through,
            bucket: self.bucket,
            source_bucket: &self.source_bucket,
            authority: &self.authority,
            repair_run_checksum_sha256: &self.repair_run_checksum_sha256,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRepairedReplica {
    pub node_id: ClusterNodeId,
    pub original_manifest: RangeDigestManifest,
    pub verified_buckets: Vec<RangeDigestVerifiedBucketEvidence>,
    pub reconstructed_root_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRepairCertificate {
    pub format_version: u32,
    pub certificate_id: Uuid,
    pub report: RangeDigestRunReport,
    pub certified_source_root_sha256: String,
    pub covered_nodes: Vec<ClusterNodeId>,
    pub source_manifests: Vec<RangeDigestManifest>,
    pub repaired_replicas: Vec<RangeDigestRepairedReplica>,
    pub limits: RangeDigestRepairCertificateLimits,
    pub checksum_sha256: String,
}

impl RangeDigestRepairCertificate {
    pub fn create(
        certificate_id: Uuid,
        report: RangeDigestRunReport,
        original_manifests: Vec<RangeDigestManifest>,
        verified_buckets: Vec<RangeDigestVerifiedBucketEvidence>,
        limits: RangeDigestRepairCertificateLimits,
    ) -> Result<Self> {
        Self::create_internal(
            certificate_id,
            report,
            original_manifests,
            verified_buckets,
            limits,
            true,
        )
    }

    fn create_internal(
        certificate_id: Uuid,
        report: RangeDigestRunReport,
        original_manifests: Vec<RangeDigestManifest>,
        verified_buckets: Vec<RangeDigestVerifiedBucketEvidence>,
        limits: RangeDigestRepairCertificateLimits,
        validate_final: bool,
    ) -> Result<Self> {
        limits.validate()?;
        if original_manifests.len() > limits.max_replicas
            || verified_buckets.len() > limits.max_bucket_evidences
        {
            return Err(certificate_error("certificate input exceeds its bounds"));
        }
        let certified_source_root_sha256 = report
            .certified_source_root_sha256
            .clone()
            .ok_or_else(|| certificate_error("report has no certified source root"))?;
        let covered_nodes = report_nodes(&report)?;
        let mut manifest_by_node = BTreeMap::new();
        let mut interval = None::<(u64, Option<u64>, usize)>;
        for manifest in original_manifests {
            let manifest_interval = (
                manifest.start_token,
                manifest.end_token,
                manifest.bucket_count,
            );
            if interval.is_some_and(|expected| expected != manifest_interval) {
                return Err(certificate_error(
                    "original manifests disagree on token interval or digest fanout",
                ));
            }
            interval = Some(manifest_interval);
            if manifest_by_node
                .insert(manifest.node_id.clone(), manifest)
                .is_some()
            {
                return Err(certificate_error("duplicate original replica manifest"));
            }
        }
        if manifest_by_node.len() != covered_nodes.len()
            || manifest_by_node.keys().ne(covered_nodes.iter())
        {
            return Err(certificate_error(
                "original manifests do not exactly cover report evidence nodes",
            ));
        }
        let mut evidence_by_destination =
            BTreeMap::<ClusterNodeId, BTreeMap<u32, RangeDigestVerifiedBucketEvidence>>::new();
        for evidence in verified_buckets {
            evidence.validate(&limits.digest)?;
            let source_manifest = manifest_by_node
                .get(&evidence.source_node_id)
                .ok_or_else(|| certificate_error("bucket evidence source has no manifest"))?;
            validate_manifest_identity(&report, source_manifest, &limits.digest)?;
            if source_manifest.root_sha256 != certified_source_root_sha256
                || source_manifest.buckets.get(evidence.bucket as usize)
                    != Some(&evidence.source_bucket)
            {
                return Err(certificate_error(
                    "bucket evidence summary does not match its certified source manifest",
                ));
            }
            if evidence.authority != report
                || evidence_by_destination
                    .entry(evidence.destination_node_id.clone())
                    .or_default()
                    .insert(evidence.bucket, evidence)
                    .is_some()
            {
                return Err(certificate_error(
                    "bucket evidence is duplicated or belongs to another report",
                ));
            }
        }

        let divergent_buckets = report
            .divergent_buckets
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut source_manifests = Vec::new();
        let mut repaired_replicas = Vec::new();
        for node_id in &covered_nodes {
            let manifest = manifest_by_node
                .remove(node_id)
                .ok_or_else(|| certificate_error("covered node has no manifest"))?;
            validate_manifest_identity(&report, &manifest, &limits.digest)?;
            let expected_report_root = root_for_node(&report, node_id)?;
            if manifest.root_sha256 != expected_report_root {
                return Err(certificate_error(
                    "original manifest root disagrees with report evidence",
                ));
            }
            if manifest.root_sha256 == certified_source_root_sha256 {
                if evidence_by_destination.contains_key(node_id) {
                    return Err(certificate_error(
                        "certified source node unexpectedly carries repair evidence",
                    ));
                }
                source_manifests.push(manifest);
                continue;
            }
            let evidence_by_bucket = evidence_by_destination
                .remove(node_id)
                .ok_or_else(|| certificate_error("divergent voter has no verified repairs"))?;
            if evidence_by_bucket.len() != divergent_buckets.len()
                || evidence_by_bucket
                    .keys()
                    .copied()
                    .ne(divergent_buckets.iter().copied())
            {
                return Err(certificate_error(
                    "divergent voter is missing verified bucket coverage",
                ));
            }
            let mut buckets = manifest.buckets.clone();
            let mut evidences = Vec::with_capacity(evidence_by_bucket.len());
            for (bucket, evidence) in evidence_by_bucket {
                buckets[bucket as usize] = evidence.source_bucket.clone();
                evidences.push(evidence);
            }
            let reconstructed_root_sha256 = calculate_range_digest_root(
                &manifest.cluster_id,
                manifest.range_id,
                manifest.range_epoch,
                manifest.start_token,
                manifest.end_token,
                manifest.resolved_through,
                &buckets,
            )?;
            if reconstructed_root_sha256 != certified_source_root_sha256 {
                return Err(certificate_error(
                    "verified buckets do not reconstruct the certified source root",
                ));
            }
            repaired_replicas.push(RangeDigestRepairedReplica {
                node_id: node_id.clone(),
                original_manifest: manifest,
                verified_buckets: evidences,
                reconstructed_root_sha256,
            });
        }
        if !manifest_by_node.is_empty() || !evidence_by_destination.is_empty() {
            return Err(certificate_error(
                "certificate carries unbound replica evidence",
            ));
        }
        let mut certificate = Self {
            format_version: RANGE_DIGEST_REPAIR_CERTIFICATE_FORMAT_VERSION,
            certificate_id,
            report,
            certified_source_root_sha256,
            covered_nodes,
            source_manifests,
            repaired_replicas,
            limits,
            checksum_sha256: String::new(),
        };
        certificate.checksum_sha256 = certificate.calculate_checksum()?;
        if validate_final {
            certificate.validate()?;
        }
        Ok(certificate)
    }

    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        self.report.validate()?;
        if self.format_version != RANGE_DIGEST_REPAIR_CERTIFICATE_FORMAT_VERSION
            || self.certificate_id.is_nil()
            || self.report.outcome != RangeDigestRunOutcome::DivergentCertifiedSource
            || self.report.certified_source_root_sha256.as_deref()
                != Some(self.certified_source_root_sha256.as_str())
            || self.covered_nodes != report_nodes(&self.report)?
            || self.covered_nodes.len() > self.limits.max_replicas
        {
            return Err(certificate_error(
                "certificate identity or coverage is invalid",
            ));
        }
        let mut manifests = self.source_manifests.clone();
        manifests.extend(
            self.repaired_replicas
                .iter()
                .map(|replica| replica.original_manifest.clone()),
        );
        let evidence = self
            .repaired_replicas
            .iter()
            .flat_map(|replica| replica.verified_buckets.iter().cloned())
            .collect::<Vec<_>>();
        if evidence.len() > self.limits.max_bucket_evidences {
            return Err(certificate_error("certificate evidence exceeds its bound"));
        }
        // Recreate the certificate from its bound inputs to recheck all root
        // substitutions. Source manifests are retained in `source_manifests`.
        let recreated = Self::create_internal(
            self.certificate_id,
            self.report.clone(),
            manifests,
            evidence,
            self.limits,
            false,
        )?;
        if recreated.covered_nodes != self.covered_nodes
            || recreated.source_manifests != self.source_manifests
            || recreated.repaired_replicas != self.repaired_replicas
            || recreated.certified_source_root_sha256 != self.certified_source_root_sha256
        {
            return Err(certificate_error("certificate contents do not reconstruct"));
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(certificate_error("certificate checksum mismatch"));
        }
        if serde_json::to_vec(self)?.len() as u64 > self.limits.max_state_bytes {
            return Err(certificate_error(
                "certificate exceeds its state byte bound",
            ));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            certificate_id: Uuid,
            report: &'a RangeDigestRunReport,
            certified_source_root_sha256: &'a str,
            covered_nodes: &'a [ClusterNodeId],
            source_manifests: &'a [RangeDigestManifest],
            repaired_replicas: &'a [RangeDigestRepairedReplica],
            limits: RangeDigestRepairCertificateLimits,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            certificate_id: self.certificate_id,
            report: &self.report,
            certified_source_root_sha256: &self.certified_source_root_sha256,
            covered_nodes: &self.covered_nodes,
            source_manifests: &self.source_manifests,
            repaired_replicas: &self.repaired_replicas,
            limits: self.limits,
        })
    }
}

pub fn save_range_digest_repair_certificate(
    path: impl AsRef<Path>,
    certificate: &RangeDigestRepairCertificate,
    fsync: bool,
) -> Result<()> {
    certificate.validate()?;
    let bytes = serde_json::to_vec(certificate)?;
    if bytes.is_empty() || bytes.len() as u64 > certificate.limits.max_state_bytes {
        return Err(certificate_error("certificate exceeds its write bound"));
    }
    crate::storage::write_atomic(path.as_ref(), &bytes, fsync)
}

pub fn load_range_digest_repair_certificate(
    path: impl AsRef<Path>,
    limits: &RangeDigestRepairCertificateLimits,
) -> Result<RangeDigestRepairCertificate> {
    limits.validate()?;
    let path = path.as_ref();
    let file = open_no_follow(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > limits.max_state_bytes {
        return Err(certificate_error(
            "certificate file is unsafe or outside its bound",
        ));
    }
    let length = metadata.len();
    let mut bytes = Vec::with_capacity(
        usize::try_from(length).map_err(|_| certificate_error("certificate is too large"))?,
    );
    file.take(limits.max_state_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || bytes.len() as u64 > limits.max_state_bytes {
        return Err(certificate_error(
            "certificate changed or grew while being read",
        ));
    }
    let certificate: RangeDigestRepairCertificate = serde_json::from_slice(&bytes)?;
    if certificate.limits != *limits {
        return Err(certificate_error(
            "certificate limits changed across restart",
        ));
    }
    certificate.validate()?;
    Ok(certificate)
}

fn open_no_follow(path: &Path) -> Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(Into::into)
    }
    #[cfg(not(unix))]
    {
        if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(certificate_error("certificate must not be a symbolic link"));
        }
        OpenOptions::new().read(true).open(path).map_err(Into::into)
    }
}

fn validate_manifest_identity(
    report: &RangeDigestRunReport,
    manifest: &RangeDigestManifest,
    limits: &RangeDigestLimits,
) -> Result<()> {
    manifest.validate(limits)?;
    if manifest.session_id != report.session_id
        || manifest.cluster_id != report.cluster_id
        || manifest.range_id != report.range_id
        || manifest.range_epoch != report.range_epoch
        || manifest.resolved_through != report.resolved_through
    {
        return Err(certificate_error(
            "manifest identity is outside the report fence",
        ));
    }
    Ok(())
}

fn report_nodes(report: &RangeDigestRunReport) -> Result<Vec<ClusterNodeId>> {
    report.validate()?;
    let mut nodes = report
        .root_evidence
        .iter()
        .flat_map(|evidence| evidence.node_ids.iter().cloned())
        .collect::<Vec<_>>();
    nodes.sort();
    if nodes.windows(2).any(|window| window[0] == window[1]) {
        return Err(certificate_error("report repeats one evidence node"));
    }
    Ok(nodes)
}

fn root_for_node(report: &RangeDigestRunReport, node_id: &ClusterNodeId) -> Result<String> {
    report
        .root_evidence
        .iter()
        .find(|evidence| evidence.node_ids.contains(node_id))
        .map(|evidence| evidence.root_sha256.clone())
        .ok_or_else(|| certificate_error("node has no report root evidence"))
}

fn node_has_root(report: &RangeDigestRunReport, node_id: &ClusterNodeId, root: &str) -> bool {
    report
        .root_evidence
        .iter()
        .any(|evidence| evidence.root_sha256 == root && evidence.node_ids.contains(node_id))
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(certificate_error(
            "SHA-256 value is not canonical lowercase hex",
        ));
    }
    Ok(())
}

fn sha256_json(value: &impl Serialize) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::distribution_anti_entropy::{
        compare_range_digest_manifests, range_digest_bucket_for_record, RangeDigestState,
    };
    use crate::distribution_anti_entropy_run::RangeDigestRootEvidence;
    use crate::record::Record;

    fn manifest(
        node_id: ClusterNodeId,
        value: u64,
        session_id: Uuid,
        cluster_id: &ClusterId,
        range_id: RangeId,
        limits: &RangeDigestLimits,
    ) -> RangeDigestManifest {
        let mut state = RangeDigestState::create(
            session_id,
            cluster_id.clone(),
            node_id,
            range_id,
            4,
            0,
            None,
            17,
            limits,
        )
        .unwrap();
        let record = Record::new("item-1").with_metadata(json!({"value": value}));
        let bytes = serde_json::to_vec(&record).unwrap().len();
        state
            .apply_batch(
                None,
                "items/item-1",
                "items",
                range_id,
                4,
                &[record],
                bytes,
                limits,
            )
            .unwrap();
        state.finish(Some("items/item-1"), limits).unwrap()
    }

    #[test]
    fn certificate_requires_complete_verified_root_reconstruction_and_reopens() {
        let digest = RangeDigestLimits {
            bucket_count: 16,
            max_records_per_batch: 8,
            max_bytes_per_batch: 64 * 1024,
            max_record_bytes: 64 * 1024,
            max_state_bytes: 64 * 1024,
        };
        let limits = RangeDigestRepairCertificateLimits {
            digest,
            max_replicas: 3,
            max_bucket_evidences: 16,
            max_state_bytes: 1024 * 1024,
        };
        let session_id = Uuid::new_v4();
        let cluster_id = ClusterId::new("certificate-cluster").unwrap();
        let range_id = RangeId::new(8).unwrap();
        let source_a = ClusterNodeId::new("node-a").unwrap();
        let source_b = ClusterNodeId::new("node-b").unwrap();
        let destination = ClusterNodeId::new("node-c").unwrap();
        let manifest_a = manifest(
            source_a.clone(),
            1,
            session_id,
            &cluster_id,
            range_id,
            &digest,
        );
        let manifest_b = manifest(
            source_b.clone(),
            1,
            session_id,
            &cluster_id,
            range_id,
            &digest,
        );
        let manifest_c = manifest(
            destination.clone(),
            2,
            session_id,
            &cluster_id,
            range_id,
            &digest,
        );
        let diff = compare_range_digest_manifests(&manifest_a, &manifest_c, &digest).unwrap();
        assert_eq!(diff.divergent_buckets.len(), 1);
        let bucket = range_digest_bucket_for_record("items", "item-1", &digest).unwrap();
        assert_eq!(diff.divergent_buckets, vec![bucket]);
        let report = RangeDigestRunReport::create(
            session_id,
            cluster_id,
            range_id,
            4,
            17,
            2,
            RangeDigestRunOutcome::DivergentCertifiedSource,
            vec![
                RangeDigestRootEvidence {
                    root_sha256: manifest_a.root_sha256.clone(),
                    node_ids: vec![source_a.clone(), source_b],
                },
                RangeDigestRootEvidence {
                    root_sha256: manifest_c.root_sha256.clone(),
                    node_ids: vec![destination.clone()],
                },
            ],
            Some(manifest_a.root_sha256.clone()),
            vec![bucket],
        )
        .unwrap();
        let mut evidence = RangeDigestVerifiedBucketEvidence {
            repair_id: Uuid::new_v4(),
            digest_session_id: session_id,
            cluster_id: report.cluster_id.clone(),
            source_node_id: source_a,
            destination_node_id: destination,
            range_id,
            range_epoch: 4,
            resolved_through: 17,
            bucket,
            source_bucket: manifest_a.buckets[bucket as usize].clone(),
            authority: report.clone(),
            repair_run_checksum_sha256: "a".repeat(64),
            checksum_sha256: String::new(),
        };
        evidence.checksum_sha256 = evidence.calculate_checksum().unwrap();
        evidence.validate(&digest).unwrap();

        assert!(RangeDigestRepairCertificate::create(
            Uuid::new_v4(),
            report.clone(),
            vec![manifest_a.clone(), manifest_b.clone(), manifest_c.clone()],
            Vec::new(),
            limits,
        )
        .is_err());
        let certificate = RangeDigestRepairCertificate::create(
            Uuid::new_v4(),
            report,
            vec![manifest_a, manifest_b, manifest_c],
            vec![evidence],
            limits,
        )
        .unwrap();
        assert_eq!(certificate.repaired_replicas.len(), 1);
        assert_eq!(certificate.source_manifests.len(), 2);

        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join(DEFAULT_RANGE_DIGEST_REPAIR_CERTIFICATE);
        save_range_digest_repair_certificate(&path, &certificate, false).unwrap();
        assert_eq!(
            load_range_digest_repair_certificate(&path, &limits).unwrap(),
            certificate
        );
        let mut damaged = std::fs::read(&path).unwrap();
        let middle = damaged.len() / 2;
        damaged[middle] ^= 1;
        std::fs::write(&path, damaged).unwrap();
        assert!(load_range_digest_repair_certificate(&path, &limits).is_err());
    }
}
