//! Native host bootstrap. A provisional seed owns a real, enforced-writer
//! factory but exposes no ordinary data, writer, readiness or read handles.
//! Authenticated birth gathering and metadata publication are separate steps.
use crate::db::{FreshRangeGenesisAuthority, RangeImageBirthMember, RangeImageBirthSpec};
use crate::distribution::{
    ClusterNodeId, ClusterNodeLifecycle, ClusterTopology, MetadataMemberRole, RangeReplicaRole,
};
use crate::{BicDb, BicDbError, CancellationToken, CollectionMode, DbConfig, Result, StorageMode};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const SEED_MANIFEST: &str = "native-bootstrap-seed.json";
const MAX_PLAN_BYTES: usize = 2 * 1024 * 1024;
fn invalid(detail: &str) -> BicDbError {
    BicDbError::Cluster(format!("native host bootstrap: {detail}"))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeBootstrapCollection {
    pub name: String,
    pub mode: CollectionMode,
}

/// Initial configuration data only. This is never a metadata, membership,
/// storage-birth, serving or read authority, including at generation one.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeClusterBootstrapPlan {
    pub version: u32,
    pub topology: ClusterTopology,
    pub collections: Vec<NativeBootstrapCollection>,
}
impl NativeClusterBootstrapPlan {
    pub(crate) fn validate(&self) -> Result<()> {
        crate::distribution_range_consensus::validate_native_json_size(self, MAX_PLAN_BYTES)?;
        self.topology.validate()?;
        if self.version != 1
            || self.topology.generation != 1
            || self.topology.range_image_bootstrap_id.is_none()
            || self.topology.nodes.len() > 64
            || self.collections.is_empty()
            || self.collections.len() > 1024
            || !self.topology.relocations.is_empty()
            || !self.topology.applied_rebalance_plans.is_empty()
        {
            return Err(invalid("invalid initial native configuration"));
        }
        for member in self.topology.nodes.values() {
            if member.lifecycle != ClusterNodeLifecycle::Active
                || member.metadata_role != MetadataMemberRole::Voter
                || member.native_store_birth_v1.is_some()
                || member.tls_certificate_sha256.is_none()
                || member.pending_tls_certificate_sha256.is_some()
            {
                return Err(invalid(
                    "initial members must be active, pinned, unenrolled voters",
                ));
            }
        }
        for range in self.topology.ranges.values() {
            if range.epoch != 1
                || range.voter_count() != usize::from(self.topology.replication_factor)
                || range
                    .replicas
                    .iter()
                    .any(|r| r.role != RangeReplicaRole::Voter)
                || range.approximate_bytes != 0
                || range.approximate_qps != 0
            {
                return Err(invalid(
                    "initial ranges require fully planned epoch-one voters and no data",
                ));
            }
        }
        let mut names = BTreeSet::new();
        for collection in &self.collections {
            // This initial facade supports bounded record collection plans.
            // Arbitrary SQL/global DDL is a different contract.
            if collection.name.is_empty()
                || collection.name.len() > 128
                || !collection
                    .name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
                || collection.name.starts_with("__bicdb_")
                || !names.insert(collection.name.clone())
            {
                return Err(invalid("invalid or duplicate initial record collection"));
            }
        }
        Ok(())
    }
    pub(crate) fn checksum(&self) -> Result<String> {
        self.validate()?;
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            "bicdb-native-bootstrap-plan-v1",
            self,
        ))?)))
    }
    fn local_spec(&self, node: &ClusterNodeId) -> Result<RangeImageBirthSpec> {
        self.validate()?;
        let local = self
            .topology
            .nodes
            .get(node)
            .ok_or_else(|| invalid("local node absent from initial plan"))?;
        Ok(RangeImageBirthSpec {
            cluster_id: self.topology.cluster_id.clone(),
            bootstrap_id: self.topology.range_image_bootstrap_id.unwrap(),
            node_id: node.clone(),
            member_incarnation: local.incarnation,
            initial_members: self
                .topology
                .nodes
                .iter()
                .map(|(id, member)| {
                    (
                        id.clone(),
                        RangeImageBirthMember {
                            incarnation: member.incarnation,
                            tls_certificate_sha256: member.tls_certificate_sha256.clone(),
                            pending_tls_certificate_sha256: member
                                .pending_tls_certificate_sha256
                                .clone(),
                        },
                    )
                })
                .collect(),
            restore_activation: None,
            initial_ranges: self
                .topology
                .ranges
                .values()
                .filter(|range| {
                    range
                        .replicas
                        .iter()
                        .any(|r| r.node_id == *node && r.role == RangeReplicaRole::Voter)
                })
                .cloned()
                .collect(),
        })
    }
}

/// A refusal/matching record. Reading or decoding it cannot mint a factory.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct NativeSeedManifest {
    version: u32,
    plan_sha256: String,
    node: ClusterNodeId,
    incarnation: u64,
    birth_id: Uuid,
    birth_sha256: String,
    schema_sha256: String,
}

/// Real local state for a never-admitted node. It is intentionally not Clone
/// or serializable and supplies no database/ordinary read/write accessor.
pub(crate) struct NativeProvisionalNode {
    database: Arc<RwLock<BicDb>>,
    factory: FreshRangeGenesisAuthority,
    plan: NativeClusterBootstrapPlan,
    spec: RangeImageBirthSpec,
    root: PathBuf,
    manifest: NativeSeedManifest,
}
impl NativeProvisionalNode {
    pub(crate) fn create(
        root: &Path,
        config: DbConfig,
        plan: NativeClusterBootstrapPlan,
        local: &ClusterNodeId,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        plan.validate()?;
        cancellation.check()?;
        if !config.fsync || config.storage_mode != StorageMode::ServerPaged {
            return Err(invalid("native bootstrap requires fsync server_paged"));
        }
        let spec = plan.local_spec(local)?;
        // The actual factory atomically refuses an existing directory. No
        // generic open, adoption of a legacy root, or cleanup is hidden here.
        let (mut database, _) =
            BicDb::create_new_enrolled_range_image_database(root, config.clone(), spec.clone())?;
        for collection in &plan.collections {
            cancellation.check()?;
            database.create_collection_with_mode(&collection.name, collection.mode.clone())?;
        }
        drop(database);
        // Materialize the persisted schema through the actual strict opener.
        // A freshly created in-memory namespace is not a paged-image proof.
        let (database, factory) = BicDb::open_range_image_database(root, config, &spec)?;
        let database = Arc::new(RwLock::new(database));
        let manifest = Self::reattest(&database, &factory, &plan, &spec, cancellation)?;
        crate::storage::write_atomic(
            &root.join(SEED_MANIFEST),
            &serde_json::to_vec_pretty(&manifest)?,
            true,
        )?;
        cancellation.check()?;
        Ok(Self {
            database,
            factory,
            plan,
            spec,
            root: root.canonicalize()?,
            manifest,
        })
    }

    pub(crate) fn open_unregistered(
        root: &Path,
        config: DbConfig,
        plan: NativeClusterBootstrapPlan,
        local: &ClusterNodeId,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        plan.validate()?;
        cancellation.check()?;
        for path in [
            crate::distribution::DEFAULT_CLUSTER_TOPOLOGY,
            crate::distribution::DEFAULT_DISTRIBUTION_CONFIG,
            crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE,
        ] {
            if root.join(path).exists() {
                return Err(invalid("metadata already exists; use enrolled host recovery, not provisional bootstrap"));
            }
        }
        let original: NativeSeedManifest =
            serde_json::from_slice(&std::fs::read(root.join(SEED_MANIFEST))?)?;
        let spec = plan.local_spec(local)?;
        if original.version != 1
            || original.plan_sha256 != plan.checksum()?
            || original.node != *local
            || original.incarnation != spec.member_incarnation
        {
            return Err(invalid("saved seed belongs to another initial plan/member"));
        }
        let (database, factory) = BicDb::open_range_image_database(root, config, &spec)?;
        // Check the refusal record BEFORE reattesting/writing any genesis.
        let native = database.native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )?;
        if native.writer_enforcement_version() != 1
            || native.birth_id() != original.birth_id
            || native.birth_sha256() != original.birth_sha256
            || database.schema_compatibility_fingerprint()?.sha256 != original.schema_sha256
        {
            return Err(invalid(
                "saved seed does not match the actual reopened birth/schema",
            ));
        }
        let database = Arc::new(RwLock::new(database));
        let actual = Self::reattest(&database, &factory, &plan, &spec, cancellation)?;
        if original != actual {
            return Err(invalid(
                "strictly reopened seed does not match actual birth/schema",
            ));
        }
        Ok(Self {
            database,
            factory,
            plan,
            spec,
            root: root.canonicalize()?,
            manifest: actual,
        })
    }

    fn reattest(
        database: &Arc<RwLock<BicDb>>,
        factory: &FreshRangeGenesisAuthority,
        plan: &NativeClusterBootstrapPlan,
        spec: &RangeImageBirthSpec,
        cancellation: &CancellationToken,
    ) -> Result<NativeSeedManifest> {
        cancellation.check()?;
        {
            let db = database.read();
            let native = db.native_recovery_admission(
                &spec.cluster_id,
                spec.bootstrap_id,
                &spec.node_id,
                spec.member_incarnation,
            )?;
            if native.writer_enforcement_version() != 1 {
                return Err(invalid(
                    "a legacy birth cannot be upgraded into the enforced-writer domain",
                ));
            }
            let actual = db.collections();
            for expected in &plan.collections {
                if !actual
                    .iter()
                    .any(|c| c.name == expected.name && c.mode == expected.mode)
                {
                    return Err(invalid("actual persisted schema differs from initial plan"));
                }
            }
            let actual = db.schema_compatibility_fingerprint()?;
            if plan.topology.nodes.values().any(|node| {
                node.labels
                    .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                    .is_some_and(|expected| expected != &actual.sha256)
            }) {
                return Err(invalid(
                    "actual schema does not match the initial schema commitment",
                ));
            }
        }
        for range in &spec.initial_ranges {
            cancellation.check()?;
            let gate = database.read().range_mutation_gate();
            let frozen = gate.try_freeze(range.id, Uuid::now_v7())?;
            BicDb::initialize_range_image_genesis(
                database,
                factory,
                range,
                &frozen,
                Duration::from_secs(5),
                cancellation,
            )?;
        }
        let db = database.read();
        factory.require_unwritten_enrollment_database(&db, &plan.topology)?;
        let native = db.native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )?;
        cancellation.check()?;
        Ok(NativeSeedManifest {
            version: 1,
            plan_sha256: plan.checksum()?,
            node: spec.node_id.clone(),
            incarnation: spec.member_incarnation,
            birth_id: native.birth_id(),
            birth_sha256: native.birth_sha256().into(),
            schema_sha256: db.schema_compatibility_fingerprint()?.sha256,
        })
    }
}
