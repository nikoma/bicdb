//! Strict cold reopen of an INITIAL member whose consensus an earlier process
//! of the same root already exposed. Nothing here creates a factory, a
//! genesis, a metadata identity, a term, a vote or any data authority: the
//! protected birth, the protected preparation record and the persisted owned
//! metadata state are read back and must agree with each other and with the
//! common plan. Everything current (leader, committed configuration, writer
//! policy, leadership of a range, readiness) is obtained afterwards by the
//! ordinary producers of this new process.
use super::*;
use crate::distribution_native_host::initial_preparation::INITIAL_METADATA_DIRECTORY;

impl NativeProvisionalNode {
    /// The same root, plan, member and birth as the process that exposed
    /// consensus. Returns the protected accepted initial configuration, which
    /// is historical identity DATA for the metadata reopen, never current C.
    pub(crate) fn open_exposed(
        root: &Path,
        config: DbConfig,
        plan: NativeClusterBootstrapPlan,
        local: &ClusterNodeId,
        cancellation: &CancellationToken,
    ) -> Result<(Self, ClusterTopology)> {
        plan.validate()?;
        cancellation.check()?;
        if !config.fsync || config.storage_mode != StorageMode::ServerPaged {
            return Err(invalid("native reopen requires fsync server_paged"));
        }
        let bytes = {
            use std::io::Read;
            let mut bytes = Vec::new();
            std::fs::File::open(root.join(SEED_MANIFEST))?
                .take((MAX_PLAN_BYTES + 1) as u64)
                .read_to_end(&mut bytes)?;
            bytes
        };
        if bytes.len() > MAX_PLAN_BYTES {
            return Err(invalid("initial member manifest bound exceeded"));
        }
        let manifest: NativeSeedManifest = serde_json::from_slice(&bytes)?;
        let spec = plan.local_spec(local)?;
        if manifest.version != 1
            || manifest.plan_sha256 != plan.checksum()?
            || manifest.node != *local
            || manifest.incarnation != spec.member_incarnation
        {
            return Err(invalid(
                "saved initial member belongs to another plan/member",
            ));
        }
        let state = root
            .join(INITIAL_METADATA_DIRECTORY)
            .join(crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
        if !state.is_file() {
            return Err(invalid(
                "initial member has no owned metadata state; a root that never exposed consensus is not reopened here",
            ));
        }
        let (database, factory) = BicDb::open_range_image_database(root, config, &spec)?;
        database.check_recovery_state()?;
        let native = database.native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )?;
        if native.writer_enforcement_version() != 1
            || native.metadata_initialization_version() != 1
            || native.abort_protocol_version() != 1
            || native.birth_id() != manifest.birth_id
            || native.birth_sha256() != manifest.birth_sha256
            || database.schema_compatibility_fingerprint()?.sha256 != manifest.schema_sha256
        {
            return Err(invalid(
                "reopened initial member birth/schema differs from its manifest",
            ));
        }
        let record = database
            .read_native_initial_preparation(
                &spec.cluster_id,
                spec.bootstrap_id,
                &spec.node_id,
                spec.member_incarnation,
            )?
            .ok_or_else(|| invalid("initial member has no protected preparation record"))?;
        if record.stage() != NativeInitialPreparationStage::ConsensusExposedHistorical
            || record.plan_sha256() != manifest.plan_sha256
        {
            return Err(invalid(
                "initial member never exposed consensus for this plan; use preparation recovery",
            ));
        }
        let accepted = record.configuration().clone();
        let identity = database
            .native_metadata_initialization_record(
                &spec.cluster_id,
                spec.bootstrap_id,
                &spec.node_id,
                spec.member_incarnation,
            )?
            .ok_or_else(|| invalid("initial member has no protected metadata identity"))?;
        if !identity.registered() || identity.store_id() != record.peer_fact().store_id {
            return Err(invalid(
                "initial member metadata identity is not the registered accepted one",
            ));
        }
        cancellation.check()?;
        Ok((
            Self {
                database: Arc::new(RwLock::new(database)),
                factory,
                plan,
                spec,
                root: root.canonicalize()?,
                manifest,
                prepared_metadata: parking_lot::Mutex::new(None),
            },
            accepted,
        ))
    }
}

impl NativeInitialConsensusLaunch {
    /// True for an owner reopened from an existing root: its ranges are not
    /// at their protected genesis and take the recovery path, never genesis.
    pub(in crate::distribution_native_host) fn reopened(&self) -> bool {
        self.reopened
    }
    /// Reopen the owned metadata state as a Follower with no known leader
    /// (persisted term, vote and log are kept as they are) and open a NEW
    /// metadata-only service over the same root. No listener is started.
    pub(crate) fn reopen(
        seed: Arc<NativeProvisionalNode>,
        accepted: ClusterTopology,
        network: ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        network.validate()?;
        if network.dev_localhost_plaintext {
            return Err(invalid("initial member reopen needs pinned mutual TLS"));
        }
        let metadata = crate::MetadataConsensusStore::reopen_native_owned(
            seed.root.join(INITIAL_METADATA_DIRECTORY),
            Arc::clone(&seed.database),
            seed.spec.node_id.clone(),
            accepted,
            parent,
        )?;
        let committed = metadata.committed_topology().clone();
        seed.require_initial_observer_identity(&committed)?;
        let service = Arc::new(Mutex::new(
            ClusterDataNodeService::open_cold_initial_member_metadata(
                seed.spec.cluster_id.clone(),
                seed.spec.node_id.clone(),
                &seed.root,
                Arc::clone(&seed.database),
                &metadata,
                &seed,
            )?,
        ));
        let metadata = Arc::new(Mutex::new(metadata));
        // Own last committed configuration: routing DATA for this node's
        // transport only. The metadata scheduler replaces it as it catches up.
        let topology = Arc::new(RwLock::new(committed));
        let transport = Arc::new(TcpClusterRelocationTransport::new(
            seed.spec.cluster_id.clone(),
            seed.spec.node_id.clone(),
            Arc::clone(&topology),
            network,
        )?);
        let provider = Arc::new(
            crate::distribution_metadata_read_provider::MetadataReadProvider::new(
                Arc::clone(&metadata),
                transport.clone(),
            )?,
        );
        parent.check()?;
        Ok(Self {
            metadata,
            service,
            topology,
            seed,
            transport,
            provider,
            reopened: true,
        })
    }
}
