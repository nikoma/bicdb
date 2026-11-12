//! Native-owned metadata initialization. No DB lock is acquired by vote/append
//! persistence: constructor work finishes BEFORE the store is exposed behind
//! the metadata mutex. The owner retains the actual DB and both root claims.
use super::*;
use crate::db::FreshRangeGenesisAuthority;
use crate::{BicDb, CancellationToken};
use parking_lot::{Mutex, RwLock};
use std::sync::{Arc, OnceLock};

pub(super) const NATIVE_METADATA_CONSENSUS_FORMAT: u32 = 2;
fn owners() -> &'static Mutex<BTreeSet<usize>> {
    static OWNERS: OnceLock<Mutex<BTreeSet<usize>>> = OnceLock::new();
    OWNERS.get_or_init(|| Mutex::new(BTreeSet::new()))
}
pub(super) fn known_born_v2(topology: &ClusterTopology, node: &ClusterNodeId) -> bool {
    topology
        .nodes
        .get(node)
        .and_then(|n| n.native_store_birth_v1.as_ref())
        .is_some_and(|birth| birth.writer_enforcement_version() == 1)
}
#[derive(Debug)]
pub(super) struct NativeMetadataStoreOwner {
    // Strong actual-open retention, never a weak path/JSON-only claim.
    _database: Arc<RwLock<BicDb>>,
    _directory: bicdb_page::DirectoryLock,
    health: Arc<bicdb_page::DurabilityHealth>,
    key: usize,
}
impl NativeMetadataStoreOwner {
    fn acquire(root: &Path, database: Arc<RwLock<BicDb>>, create: bool) -> Result<Self> {
        let key = Arc::as_ptr(&database) as usize;
        if !owners().lock().insert(key) {
            return Err(metadata_error(
                "actual native DB already owns a metadata store",
            ));
        }
        let result = (|| {
            if create {
                fs::create_dir_all(root)?;
            }
            let guard = database
                .try_read()
                .ok_or_else(|| metadata_error("native DB busy before metadata open"))?;
            if root.canonicalize()? == guard.data_path().canonicalize()? {
                return Err(metadata_error(
                    "native metadata needs a separate owned metadata directory",
                ));
            }
            let health = guard.native_metadata_owner_health()?;
            drop(guard);
            let directory = bicdb_page::DirectoryLock::acquire(root).map_err(|e| {
                metadata_error(format!("metadata directory already owned/unavailable: {e}"))
            })?;
            Ok(Self {
                _database: database,
                _directory: directory,
                health,
                key,
            })
        })();
        if result.is_err() {
            owners().lock().remove(&key);
        }
        result
    }
    pub(super) fn require_current(&self) -> Result<()> {
        // No BicDb.{read,write}, file/network wait, or reverse lock acquisition.
        self.health.check().map_err(crate::error::durability_error)
    }
}
impl Drop for NativeMetadataStoreOwner {
    fn drop(&mut self) {
        owners().lock().remove(&self.key);
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeMetadataInitializationCut {
    AfterProtectedPrepare,
    AfterStateFile,
}

impl MetadataConsensusStore {
    pub(crate) fn initialize_native_owned(
        root: impl AsRef<Path>,
        database: Arc<RwLock<BicDb>>,
        factory: &FreshRangeGenesisAuthority,
        topology: ClusterTopology,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        Self::initialize_native_owned_with(
            root.as_ref(),
            database,
            factory,
            topology,
            cancellation,
            |_| Ok(()),
        )
    }
    #[cfg(test)]
    pub(crate) fn test_initialize_native_owned_with(
        root: &Path,
        database: Arc<RwLock<BicDb>>,
        factory: &FreshRangeGenesisAuthority,
        topology: ClusterTopology,
        cancellation: &CancellationToken,
        cut: impl FnMut(NativeMetadataInitializationCut) -> Result<()>,
    ) -> Result<Self> {
        Self::initialize_native_owned_with(root, database, factory, topology, cancellation, cut)
    }
    fn initialize_native_owned_with(
        root: &Path,
        database: Arc<RwLock<BicDb>>,
        factory: &FreshRangeGenesisAuthority,
        topology: ClusterTopology,
        cancellation: &CancellationToken,
        mut cut: impl FnMut(NativeMetadataInitializationCut) -> Result<()>,
    ) -> Result<Self> {
        cancellation.check()?;
        topology.validate()?;
        let node = factory.node_id().clone();
        if !known_born_v2(&topology, &node) {
            return Err(metadata_error(
                "native metadata initialization needs actual enrolled born-v2 configuration",
            ));
        }
        let owner = NativeMetadataStoreOwner::acquire(root, Arc::clone(&database), true)?;
        let path = root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(metadata_error(
                    "metadata state already exists; strict reopen required",
                ))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
        let store_id = Uuid::now_v7();
        let prepared = database
            .try_read()
            .ok_or_else(|| metadata_error("native DB busy before metadata prepare"))?
            .prepare_native_metadata_store(factory, &topology, store_id)?;
        cancellation.check()?;
        cut(NativeMetadataInitializationCut::AfterProtectedPrepare)?;
        let state = MetadataConsensusState {
            format_version: NATIVE_METADATA_CONSENSUS_FORMAT,
            native_store_id: Some(store_id),
            cluster_id: topology.cluster_id.clone(),
            node_id: node,
            current_term: 0,
            voted_for: None,
            role: MetadataConsensusRole::Follower,
            leader_id: None,
            base_index: 0,
            base_term: 0,
            commit_index: 0,
            committed_topology: topology.clone(),
            log: Vec::new(),
            committed_restore_activation: None,
        };
        let mut store = Self {
            root: root.to_path_buf(),
            fsync: true,
            state,
            match_index: BTreeMap::new(),
            last_leader_contact_ms: unix_time_ms(),
            native_owner: Some(owner),
        };
        store.reset_match_indexes();
        store.persist()?;
        cancellation.check()?;
        cut(NativeMetadataInitializationCut::AfterStateFile)?;
        let persisted = decode_state(&fs::read(&path)?)?;
        if persisted.native_store_id != Some(prepared.store_id()) || persisted != store.state {
            return Err(metadata_error("initial metadata state readback changed"));
        }
        database
            .try_read()
            .ok_or_else(|| metadata_error("native DB busy during metadata registration"))?
            .finish_native_metadata_store(&prepared, &topology)?;
        cancellation.check()?;
        Ok(store)
    }

    pub(crate) fn reopen_native_owned(
        root: impl AsRef<Path>,
        database: Arc<RwLock<BicDb>>,
        node: ClusterNodeId,
        expected: ClusterTopology,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        expected.validate()?;
        if !known_born_v2(&expected, &node) {
            return Err(metadata_error(
                "native reopen requires enrolled born-v2 configuration",
            ));
        }
        let root = root.as_ref();
        let owner = NativeMetadataStoreOwner::acquire(root, Arc::clone(&database), false)?;
        // No missing-file fallback. Read protected receipt BEFORE any metadata write.
        let receipt = database
            .try_read()
            .ok_or_else(|| metadata_error("native DB busy before metadata reopen"))?
            .read_native_metadata_store(&expected, &node)?;
        if !receipt.registered() {
            return Err(metadata_error(
                "native metadata initialization incomplete; voting quarantined",
            ));
        }
        let mut state = decode_state(
            &fs::read(root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)).map_err(|e| {
                metadata_error(format!(
                    "protected metadata state missing/unreadable; never reset: {e}"
                ))
            })?,
        )?;
        validate_state(&state)?;
        if state.native_store_id != Some(receipt.store_id())
            || state.node_id != node
            || state.cluster_id != expected.cluster_id
        {
            return Err(metadata_error(
                "metadata state does not match actual protected store identity",
            ));
        }
        crate::distribution::validate_native_store_birth_snapshot(
            &expected,
            &state.committed_topology,
        )?;
        crate::distribution::ensure_range_image_domain(
            expected.range_image_bootstrap_id,
            state.committed_topology.range_image_bootstrap_id,
        )?;
        // Validate actual birth AGAIN against the independently persisted state.
        let actual = database
            .try_read()
            .ok_or_else(|| metadata_error("native DB busy revalidating metadata state"))?
            .read_native_metadata_store(&state.committed_topology, &node)?;
        if actual != receipt {
            return Err(metadata_error("protected metadata receipt changed"));
        }
        cancellation.check()?;
        state.role = MetadataConsensusRole::Follower;
        state.leader_id = None;
        let mut store = Self {
            root: root.to_path_buf(),
            fsync: true,
            state,
            match_index: BTreeMap::new(),
            last_leader_contact_ms: unix_time_ms(),
            native_owner: Some(owner),
        };
        store.reset_match_indexes();
        store.persist()?;
        cancellation.check()?;
        Ok(store)
    }
}

#[cfg(test)]
mod tests;
