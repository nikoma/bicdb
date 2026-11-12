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
    actual_open: std::sync::Weak<()>,
    actual_directory: std::sync::Weak<bicdb_page::DirectoryLock>,
    key: usize,
    actor: Option<NativeMetadataActorBinding>,
}
#[derive(Debug)]
struct NativeMetadataActorBinding {
    node: ClusterNodeId,
    birth: crate::distribution::NativeStoreBirthV1,
    incarnation: u64,
    pins: BTreeSet<String>,
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
            let actual_open = guard.native_metadata_owner_open_lifetime();
            let actual_directory = guard
                .native_service_directory_lifetime()
                .ok_or_else(|| metadata_error("actual native directory lifetime absent"))?;
            drop(guard);
            let directory = bicdb_page::DirectoryLock::acquire(root).map_err(|e| {
                metadata_error(format!("metadata directory already owned/unavailable: {e}"))
            })?;
            Ok(Self {
                _database: database,
                _directory: directory,
                health,
                actual_open,
                actual_directory,
                key,
                actor: None,
            })
        })();
        if result.is_err() {
            owners().lock().remove(&key);
        }
        result
    }
    fn bind_actual_actor(
        &mut self,
        db: &BicDb,
        topology: &ClusterTopology,
        node: &ClusterNodeId,
    ) -> Result<()> {
        self.require_current()?;
        let member = topology
            .nodes
            .get(node)
            .ok_or_else(|| metadata_error("native metadata local actor absent"))?;
        let native = db.native_recovery_admission(
            &topology.cluster_id,
            topology
                .range_image_bootstrap_id
                .ok_or_else(|| metadata_error("native metadata bootstrap absent"))?,
            node,
            member.incarnation,
        )?;
        let birth = crate::distribution::NativeStoreBirthV1::from_actual_native(&native)?;
        if member.native_store_birth_v1.as_ref() != Some(&birth) {
            return Err(metadata_error(
                "metadata actor configuration does not name actual local birth",
            ));
        }
        let pins = member
            .tls_certificate_sha256
            .iter()
            .chain(member.pending_tls_certificate_sha256.iter())
            .cloned()
            .collect();
        self.actor = Some(NativeMetadataActorBinding {
            node: node.clone(),
            birth,
            incarnation: native.local_member_incarnation(),
            pins,
        });
        Ok(())
    }
    fn require_actor(
        &self,
        topology: &ClusterTopology,
        node: &ClusterNodeId,
        voter: bool,
    ) -> Result<()> {
        self.require_current()?;
        let actor = self
            .actor
            .as_ref()
            .ok_or_else(|| metadata_error("actual native metadata actor was not bound"))?;
        let member = topology
            .nodes
            .get(node)
            .ok_or_else(|| metadata_error("actual native metadata actor removed"))?;
        let current_pins = member
            .tls_certificate_sha256
            .iter()
            .chain(member.pending_tls_certificate_sha256.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        let pins_match = if actor.pins.is_empty() {
            current_pins.is_empty()
        } else {
            !actor.pins.is_disjoint(&current_pins)
        };
        if &actor.node != node
            || actor.incarnation != member.incarnation
            || member.native_store_birth_v1.as_ref() != Some(&actor.birth)
            || member.lifecycle != ClusterNodeLifecycle::Active
            || !pins_match
            || (voter && member.metadata_role != MetadataMemberRole::Voter)
        {
            return Err(metadata_error(
                "actual native metadata actor is retired/replaced/not eligible for this receipt",
            ));
        }
        Ok(())
    }
    pub(super) fn require_current(&self) -> Result<()> {
        // No BicDb.{read,write}, file/network wait, or reverse lock acquisition.
        if self.actual_open.upgrade().is_none() || self.actual_directory.upgrade().is_none() {
            return Err(metadata_error("original native DB open lifetime ended"));
        }
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
    pub(super) fn require_native_metadata_actor_current(&self, voter: bool) -> Result<()> {
        self.require_native_metadata_owner_current()?;
        if let Some(owner) = &self.native_owner {
            owner.require_actor(&self.state.committed_topology, &self.state.node_id, voter)?;
        }
        Ok(())
    }
    pub(super) fn demote_native_retired_metadata_actor(&mut self) {
        if self.native_owner.is_some() && self.require_native_metadata_actor_current(true).is_err()
        {
            self.state.role = MetadataConsensusRole::Follower;
            self.state.leader_id = None;
        }
    }
    pub(super) fn require_native_metadata_owner_current(&self) -> Result<()> {
        if let Some(owner) = &self.native_owner {
            owner.require_current()?;
        }
        if self.state.native_store_id.is_some() && self.native_owner.is_none() {
            return Err(metadata_error(
                "protected metadata lacks actual native owner",
            ));
        }
        Ok(())
    }
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
        let mut owner = NativeMetadataStoreOwner::acquire(root, Arc::clone(&database), true)?;
        {
            let actual_database = database
                .try_read()
                .ok_or_else(|| metadata_error("native DB busy binding initial actor"))?;
            owner.bind_actual_actor(&actual_database, &topology, &node)?;
        }
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
        let mut owner = NativeMetadataStoreOwner::acquire(root, Arc::clone(&database), false)?;
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
        {
            let actual_database = database
                .try_read()
                .ok_or_else(|| metadata_error("native DB busy binding reopened actor"))?;
            owner.bind_actual_actor(&actual_database, &state.committed_topology, &node)?;
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

#[cfg(test)]
mod actor_tests;
