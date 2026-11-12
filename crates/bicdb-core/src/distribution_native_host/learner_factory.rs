//! Actual fresh factory for a registered UNBOUND metadata learner. Creation
//! consumes a live authenticated HostMQ observation; the durable manifest is
//! refusal/configuration data, never a replacement for that observation.
use super::*;
use crate::distribution::{ClusterNetworkTransportConfig, NativeStoreBirthV1};
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_native_admission::ActualNativeStoreEnrollment;
use crate::ClusterSchemaBundle;
use std::fs::File;
use std::io::Read;

mod registered_catchup;

const MANIFEST: &str = "native-unassigned-learner.json";
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LearnerManifest {
    version: u32,
    spec: RangeImageBirthSpec,
    birth: NativeStoreBirthV1,
    schema_sha256: String,
    initial_metadata_context: Uuid,
    initial_metadata_term: u64,
    initial_metadata_read_index: u64,
}

/// No ordinary read/write accessor, numeric constructor or metadata voter
/// grant. The next host step must authenticate the actual birth over TLS,
/// commit its binding, then initialize/catch up an actual owned metadata store.
pub(crate) struct NativeUnassignedLearnerFactory {
    pub(super) database: Arc<RwLock<BicDb>>,
    pub(super) factory: FreshRangeGenesisAuthority,
    pub(super) spec: RangeImageBirthSpec,
    manifest: LearnerManifest,
}
impl NativeUnassignedLearnerFactory {
    pub(super) fn actual_birth(&self) -> &NativeStoreBirthV1 {
        &self.manifest.birth
    }

    fn context(
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
    ) -> Result<(ClusterTopology, ClusterNodeId, String)> {
        observed.require_live_request(business)?;
        network.validate()?;
        if network.dev_localhost_plaintext || network.tls.is_none() {
            return Err(invalid("native learner factory requires actual pinned TLS"));
        }
        let topology = observed.committed_topology(business)?;
        let local = observed.requester_node_id();
        let member = topology
            .nodes
            .get(local)
            .ok_or_else(|| invalid("actual HostMQ requester is absent"))?;
        if member.lifecycle != ClusterNodeLifecycle::Active
            || member.metadata_role != MetadataMemberRole::Learner
            || member.incarnation != observed.requester_incarnation()
            || topology.range_image_bootstrap_id.is_none()
            || topology
                .ranges
                .values()
                .any(|range| range.replicas.iter().any(|r| &r.node_id == local))
        {
            return Err(invalid(
                "fresh factory requires the registered unassigned learner requester",
            ));
        }
        let tls = network.tls.as_ref().unwrap();
        if !tls.require_client_cert || tls.dev_localhost_plaintext {
            return Err(invalid("native learner requires mutual TLS"));
        }
        let pin = crate::replication_transport::replication_certificate_sha256(&tls.cert_path)?;
        if !member.accepts_tls_certificate_sha256(&pin) {
            return Err(invalid(
                "learner local certificate differs from actual committed membership",
            ));
        }
        let mut schema = None;
        for range in topology.ranges.values() {
            let required = range
                .required_schema_sha256(&topology.nodes)
                .ok_or_else(|| invalid("native learner needs a committed active range schema"))?;
            if schema
                .as_ref()
                .is_some_and(|value: &String| value != required)
            {
                return Err(invalid(
                    "native learner schema transition is not yet coherent",
                ));
            }
            schema = Some(required.to_owned());
        }
        for node in topology.nodes.values().filter(|node| {
            node.metadata_role == MetadataMemberRole::Voter
                && node.lifecycle == ClusterNodeLifecycle::Active
        }) {
            if !node
                .native_store_birth_v1
                .as_ref()
                .is_some_and(|birth| birth.writer_enforcement_version() == 1)
            {
                return Err(invalid(
                    "native learner admission needs an enrolled native metadata cohort",
                ));
            }
        }
        Ok((
            topology.clone(),
            local.clone(),
            schema.ok_or_else(|| invalid("native learner has no active schema"))?,
        ))
    }

    pub(crate) fn create(
        root: &Path,
        config: DbConfig,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
        bundle: &ClusterSchemaBundle,
        parent: &CancellationToken,
    ) -> Result<Self> {
        let token = parent.child_with_deadline(observed.deadline());
        token.check()?;
        let (topology, local, schema) = Self::context(observed, business, network)?;
        if topology.nodes[&local].native_store_birth_v1.is_some() {
            return Err(invalid("a committed birth cannot be replaced by a blank factory under the same incarnation"));
        }
        bundle.verify()?;
        if bundle.fingerprint.sha256 != schema {
            return Err(invalid(
                "bootstrap schema bundle differs from actual committed metadata",
            ));
        }
        if !bundle.structural_records.is_empty() {
            return Err(BicDbError::Unsupported(
                "native learner SQL structural schema initialization is not implemented".into(),
            ));
        }
        let spec = RangeImageBirthSpec {
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id: topology.range_image_bootstrap_id.unwrap(),
            node_id: local.clone(),
            member_incarnation: topology.nodes[&local].incarnation,
            restore_activation: observed.restore_activation(business)?.cloned(),
            initial_ranges: Vec::new(),
            initial_members: topology
                .nodes
                .iter()
                .map(|(id, node)| {
                    (
                        id.clone(),
                        RangeImageBirthMember {
                            incarnation: node.incarnation,
                            tls_certificate_sha256: node.tls_certificate_sha256.clone(),
                            pending_tls_certificate_sha256: node
                                .pending_tls_certificate_sha256
                                .clone(),
                        },
                    )
                })
                .collect(),
        };
        let (mut db, factory) =
            BicDb::create_new_enrolled_range_image_database(root, config.clone(), spec.clone())?;
        db.initialize_unassigned_native_learner_schema(&factory, &topology, bundle, &token)?;
        drop(factory);
        drop(db);
        let (db, factory) = BicDb::open_range_image_database(root, config, &spec)?;
        let actual = factory.require_unwritten_enrollment_database(&db, &topology)?;
        let birth = NativeStoreBirthV1::from_actual_native(&actual)?;
        if db.schema_compatibility_fingerprint()?.sha256 != schema {
            return Err(invalid(
                "strict reopened learner schema differs from actual source",
            ));
        }
        observed.require_live_request(business)?;
        token.check()?;
        let manifest = LearnerManifest {
            version: 1,
            spec: spec.clone(),
            birth,
            schema_sha256: schema,
            initial_metadata_context: observed.context_id(),
            initial_metadata_term: observed.term(),
            initial_metadata_read_index: observed.read_index(),
        };
        crate::storage::write_atomic(
            &root.join(MANIFEST),
            &serde_json::to_vec_pretty(&manifest)?,
            true,
        )?;
        // The actual observation has its own cancellation ancestry. Checking
        // only the caller-derived token after fsync cannot renew that proof.
        observed.require_live_request(business)?;
        token.check()?;
        Ok(Self {
            database: Arc::new(RwLock::new(db)),
            factory,
            spec,
            manifest,
        })
    }

    /// Strict pre-metadata continuation only. Initialized or assigned members
    /// need the ordinary owned-metadata/recovery loader, never a fresh grant.
    pub(crate) fn reopen_uninitialized(
        root: &Path,
        config: DbConfig,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<Self> {
        Self::reopen_existing(root, config, observed, business, network, parent, true)
    }

    /// Strict cold loader only; protected initialization is inspected later by
    /// the owned metadata constructor. This never asserts a fresh factory.
    pub(crate) fn reopen_enrolled_metadata(
        root: &Path,
        config: DbConfig,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<Self> {
        Self::reopen_existing(root, config, observed, business, network, parent, false)
    }

    fn reopen_existing(
        root: &Path,
        config: DbConfig,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
        uninitialized: bool,
    ) -> Result<Self> {
        let token = parent.child_with_deadline(observed.deadline());
        token.check()?;
        let (topology, local, schema) = Self::context(observed, business, network)?;
        let mut bytes = Vec::new();
        File::open(root.join(MANIFEST))?
            .take((MAX_PLAN_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_PLAN_BYTES {
            return Err(invalid("learner manifest bound exceeded"));
        }
        let manifest: LearnerManifest = serde_json::from_slice(&bytes)?;
        if manifest.version != 1
            || manifest.spec.node_id != local
            || manifest.schema_sha256 != schema
            || manifest.spec.restore_activation.as_ref() != observed.restore_activation(business)?
        {
            return Err(invalid(
                "learner manifest has another actual requested scope",
            ));
        }
        let (db, factory) = BicDb::open_range_image_database(root, config, &manifest.spec)?;
        if uninitialized {
            db.require_native_metadata_uninitialized()?;
        } else if db.current_commit_seq() != 0 {
            // Necessary refusal only: no range/data authority is inferred from
            // zero. A root with business-sequence history is not this bounded
            // unassigned-metadata continuation contract.
            return Err(invalid(
                "cold unassigned learner has business-sequence history; quarantined",
            ));
        }
        let actual = factory.require_unwritten_enrollment_database(&db, &topology)?;
        if NativeStoreBirthV1::from_actual_native(&actual)? != manifest.birth
            || db.schema_compatibility_fingerprint()?.sha256 != manifest.schema_sha256
        {
            return Err(invalid(
                "actual learner birth/schema changed; do not replace or reset it",
            ));
        }
        if topology.nodes[&local]
            .native_store_birth_v1
            .as_ref()
            .is_some_and(|birth| birth != &manifest.birth)
        {
            return Err(invalid(
                "actual committed learner belongs to a different physical birth",
            ));
        }
        if !uninitialized
            && topology.nodes[&local].native_store_birth_v1.as_ref() != Some(&manifest.birth)
        {
            return Err(invalid(
                "cold metadata requires the current committed actual learner birth",
            ));
        }
        if !uninitialized {
            if let Some(record) = db.native_metadata_initialization_record(
                &manifest.spec.cluster_id,
                manifest.spec.bootstrap_id,
                &local,
                manifest.spec.member_incarnation,
            )? {
                record.require_namespaces_current(&db)?;
            }
        }
        observed.require_live_request(business)?;
        token.check()?;
        Ok(Self {
            spec: manifest.spec.clone(),
            database: Arc::new(RwLock::new(db)),
            factory,
            manifest,
        })
    }

    /// Fresh own learner MQ plus actual birth/schema. Valid after metadata
    /// preparation too; unlike enrollment_proposal this never asserts uninitialized.
    pub(super) fn enrolled_metadata_context(
        &self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
    ) -> Result<ClusterTopology> {
        let (topology, local, schema) = Self::context(observed, business, network)?;
        if local != self.spec.node_id
            || schema != self.manifest.schema_sha256
            || self.spec.restore_activation.as_ref() != observed.restore_activation(business)?
            || topology.nodes[&local].native_store_birth_v1.as_ref() != Some(&self.manifest.birth)
        {
            return Err(invalid(
                "metadata transition needs current committed actual learner binding",
            ));
        }
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("actual learner DB busy"))?;
        if db.schema_compatibility_fingerprint()?.sha256 != schema {
            return Err(invalid(
                "actual learner schema changed before metadata transition",
            ));
        }
        let native = self
            .factory
            .require_unwritten_enrollment_database(&db, &topology)?;
        if NativeStoreBirthV1::from_actual_native(&native)? != self.manifest.birth {
            return Err(invalid(
                "actual learner birth changed before metadata transition",
            ));
        }
        observed.require_live_request(business)?;
        Ok(topology)
    }

    /// Post-initialization continuation ONLY, consuming an actual protected
    /// MStore observation. Blank factory/create predicates stay Learner-only.
    pub(super) fn require_owned_metadata_member(
        &self,
        proof: &crate::distribution_consensus::NativeOwnedMetadataMemberObservation,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
    ) -> Result<()> {
        observed.require_live_request(business)?;
        let topology = observed.committed_topology(business)?;
        if proof.topology() != topology
            || !self.spec.initial_ranges.is_empty()
            || observed.requester_node_id() != &self.spec.node_id
            || observed.restore_activation(business)? != self.spec.restore_activation.as_ref()
            || topology
                .ranges
                .values()
                .any(|r| r.replicas.iter().any(|p| p.node_id == self.spec.node_id))
        {
            return Err(invalid(
                "owned metadata-only member scope differs or has data placement",
            ));
        }
        let member = topology
            .nodes
            .get(&self.spec.node_id)
            .ok_or_else(|| invalid("owned member absent"))?;
        let pin = crate::replication_transport::replication_certificate_sha256(
            &network
                .tls
                .as_ref()
                .ok_or_else(|| invalid("owned member TLS absent"))?
                .cert_path,
        )?;
        if network.dev_localhost_plaintext
            || !member.accepts_tls_certificate_sha256(&pin)
            || member.lifecycle != ClusterNodeLifecycle::Active
            || member.incarnation != self.spec.member_incarnation
            || member.native_store_birth_v1.as_ref() != Some(&self.manifest.birth)
        {
            return Err(invalid(
                "owned metadata member actual birth/identity changed",
            ));
        }
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("owned metadata member DB busy"))?;
        proof.require_database(&db)?;
        if db.schema_compatibility_fingerprint()?.sha256 != self.manifest.schema_sha256
            || topology.ranges.values().any(|r| {
                r.required_schema_sha256(&topology.nodes)
                    != Some(self.manifest.schema_sha256.as_str())
            })
        {
            return Err(invalid("owned metadata member actual schema differs"));
        }
        observed.require_live_request(business)
    }

    /// Configuration proposal only. It must cross authenticated birth
    /// observation and metadata consensus before any owned metadata/vote/data
    /// service is exposed. This method does not commit or promote anything.
    pub(super) fn enrollment_proposal(
        &self,
        observed: &HostMetadataReadObservation,
        business: Uuid,
        network: &ClusterNetworkTransportConfig,
    ) -> Result<ClusterTopology> {
        let (mut topology, local, schema) = Self::context(observed, business, network)?;
        if local != self.spec.node_id
            || schema != self.manifest.schema_sha256
            || self.spec.restore_activation.as_ref() != observed.restore_activation(business)?
        {
            return Err(invalid("learner proposal requester/schema/restore changed"));
        }
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("actual learner factory is busy"))?;
        db.require_native_metadata_uninitialized()?;
        let actual = self
            .factory
            .require_unwritten_enrollment_database(&db, &topology)?;
        if NativeStoreBirthV1::from_actual_native(&actual)? != self.manifest.birth
            || db.schema_compatibility_fingerprint()?.sha256 != schema
        {
            return Err(invalid(
                "learner proposal differs from actual owned factory",
            ));
        }
        let previously_enrolled = topology.nodes[&local].native_store_birth_v1.is_some();
        ActualNativeStoreEnrollment::from_actual_factory(&self.factory, &db, &topology)?
            .bind_initial_or_unassigned_learner(&mut topology)?;
        if !previously_enrolled {
            topology.generation = topology
                .generation
                .checked_add(1)
                .ok_or_else(|| invalid("learner enrollment topology generation exhausted"))?;
        }
        observed.require_live_request(business)?;
        Ok(topology)
    }
}
