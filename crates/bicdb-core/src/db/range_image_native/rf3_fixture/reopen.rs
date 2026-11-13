//! COPY-ONLY recovery diagnostics. Never reopens the retained original or
//! turns persisted JSON into authority: strict native/metadata loaders verify
//! actual birth and checksummed state, and no coordinator is recovered/armed.
use super::*;
use crate::distribution_consensus::MetadataConsensusStore;
use std::path::Path;

impl ActualNativeRf3Fixture {
    /// Copies a stopped, owned test tree before any engine open. Symlinks are
    /// refused so this cannot collect an external credential/cache directory.
    pub(crate) fn copy_stopped_recovery_tree(source: &Path) -> Result<tempfile::TempDir> {
        fn copy(source: &Path, target: &Path) -> Result<()> {
            for item in std::fs::read_dir(source)? {
                let item = item?;
                let kind = item.file_type()?;
                let destination = target.join(item.file_name());
                if kind.is_dir() {
                    std::fs::create_dir(&destination)?;
                    copy(&item.path(), &destination)?;
                } else if kind.is_file() {
                    std::fs::copy(item.path(), &destination)?;
                    std::fs::File::open(&destination)?.sync_all()?;
                } else {
                    return Err(invalid(
                        "copied recovery fixture refuses symlinks/special files",
                    ));
                }
            }
            std::fs::File::open(target)?.sync_all()?;
            // A stopped store is restored the way an operator must restore
            // it: the genuine source authorizes its copy's physical rebind.
            // A raw copy stays refused; see the Native origin root tests.
            if source.join("native-origin-root-v1.json").is_file() {
                crate::authorize_native_cold_restore(source, target)?;
            }
            Ok(())
        }
        let destination = tempfile::tempdir()?;
        copy(source, destination.path())?;
        Ok(destination)
    }

    /// Takes ownership of a newly copied TempDir. Existing native genesis,
    /// pending commands, irreversible catalog tails, UIDs and refusals stay
    /// intact. The returned coordinator has EMPTY recovered epochs and is NOT
    /// installed as commit admission. Fresh recovery must use actual protocol.
    pub(crate) fn reopen_copied_recovery_state(root: tempfile::TempDir) -> Result<Self> {
        let metadata_path = root
            .path()
            .join("recovery-metadata/n0/cluster-metadata-consensus.json");
        let encoded: serde_json::Value = serde_json::from_slice(&std::fs::read(&metadata_path)?)?;
        let bootstrap: ClusterTopology =
            serde_json::from_value(encoded["state"]["committed_topology"].clone())?;
        bootstrap.validate()?;
        let cluster = bootstrap.cluster_id.clone();
        let mut verified_topology = None;
        for name in ["n0", "n1", "n2"] {
            let id = ClusterNodeId::new(name)?;
            let directory = root.path().join("recovery-metadata").join(name);
            if !directory.join("cluster-metadata-consensus.json").is_file() {
                return Err(invalid("copy lacks an actual saved metadata store"));
            }
            let store = MetadataConsensusStore::open(
                &directory,
                cluster.clone(),
                id,
                bootstrap.clone(),
                true,
            )?;
            let actual = store.committed_topology();
            if let Some(expected) = &verified_topology {
                if expected != actual {
                    return Err(invalid("saved metadata stores disagree on topology"));
                }
            } else {
                verified_topology = Some(actual.clone());
            }
        }
        let topology =
            verified_topology.ok_or_else(|| invalid("no verified saved metadata topology"))?;
        if topology.nodes.len() != 3 || topology.range_image_bootstrap_id.is_none() {
            return Err(invalid(
                "copy is not the actual three-member native fixture domain",
            ));
        }
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut databases = BTreeMap::new();
        let mut specs = BTreeMap::new();
        for (id, member) in &topology.nodes {
            let path = root.path().join(id.as_str());
            let preflight: FactoryPreflight =
                serde_json::from_slice(&std::fs::read(path.join(FACTORY_PREFLIGHT))?)?;
            let spec = preflight.spec;
            if spec.cluster_id != cluster
                || spec.node_id != *id
                || spec.member_incarnation != member.incarnation
                || Some(spec.bootstrap_id) != topology.range_image_bootstrap_id
            {
                return Err(invalid(
                    "saved birth preflight differs from verified metadata identity",
                ));
            }
            let (db, _) = BicDb::open_range_image_database(&path, config.clone(), &spec)?;
            let _ =
                db.native_recovery_admission(&cluster, spec.bootstrap_id, id, member.incarnation)?;
            let schema = db.schema_compatibility_fingerprint()?.sha256;
            if member
                .labels
                .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                != Some(&schema)
            {
                return Err(invalid(
                    "copied native schema differs from saved committed metadata",
                ));
            }
            databases.insert(id.clone(), Arc::new(RwLock::new(db)));
            specs.insert(id.clone(), spec);
        }
        let topology = Arc::new(RwLock::new(topology));
        let mut services = BTreeMap::new();
        for (id, db) in &databases {
            let service = ClusterDataNodeService::open(
                cluster.clone(),
                id.clone(),
                db.read().data_path(),
                Arc::clone(db),
                true,
            )?;
            services.insert(id.clone(), Arc::new(Mutex::new(service)));
        }
        let leader = topology
            .read()
            .ranges
            .values()
            .next()
            .ok_or_else(|| invalid("saved fixture has no range"))?
            .leader
            .clone();
        let transport = Arc::new(ActualNativeRf3ServiceTransport {
            caller: leader.clone(),
            topology: Arc::clone(&topology),
            services: services.clone(),
            observations: Mutex::new(Vec::new()),
            unavailable: Mutex::new(BTreeSet::new()),
            native_delivery: std::sync::atomic::AtomicBool::new(false),
            native_drop_once: Mutex::new(None),
            lose_commit_replies_after_durability: std::sync::atomic::AtomicBool::new(false),
        });
        let coordinator = Arc::new(RangeWriteCoordinator::new_schema_fenced(
            cluster,
            leader.clone(),
            Arc::clone(&topology),
            Arc::clone(&services[&leader]),
            Arc::clone(&transport),
        )?);
        // Deliberately no recover_local_leadership, genesis or commit admission.
        Ok(Self {
            root,
            topology,
            databases,
            services,
            specs,
            fresh_factories: BTreeMap::new(),
            transport,
            coordinator,
        })
    }
}
