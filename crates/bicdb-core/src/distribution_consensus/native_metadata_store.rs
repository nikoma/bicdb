//! Native-owned metadata initialization. No DB lock is acquired by vote/append
//! persistence: constructor work finishes BEFORE the store is exposed behind
//! the metadata mutex. The owner retains the actual DB and both root claims.
use super::*;
use crate::db::FreshRangeGenesisAuthority;
use crate::{BicDb, CancellationToken};
use parking_lot::{Mutex, RwLock};
use std::sync::{Arc, OnceLock};

mod append_ack;
#[cfg(feature = "tls-replication")]
mod learner_enrollment;
pub(crate) use append_ack::NativeMetadataAppendProvenance;
pub use append_ack::NativeMetadataAppendReceiver;

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
    // Captured only from the protected Registered receipt, never the JSON alone.
    registered_store_id: Option<Uuid>,
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
                registered_store_id: None,
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

/// Borrow of an ACTUAL owned, never-voted metadata store. No DTO/Clone path.
pub(crate) struct NativeMetadataPreVoteFact<'a> {
    store: &'a MetadataConsensusStore,
}
impl NativeMetadataPreVoteFact<'_> {
    pub(crate) fn require_current(&self) -> Result<()> {
        self.store.require_native_metadata_actor_current(true)?;
        let s = &self.store.state;
        if s.native_store_id.is_none()
            || s.current_term != 0
            || s.voted_for.is_some()
            || s.role != MetadataConsensusRole::Follower
            || s.leader_id.is_some()
            || s.base_index != 0
            || s.base_term != 0
            || s.commit_index != 0
            || !s.log.is_empty()
            || s.committed_restore_activation.is_some()
        {
            return Err(metadata_error(
                "initial preparation needs actual term-zero never-voted owned metadata state",
            ));
        }
        Ok(())
    }
    pub(crate) fn store_id(&self) -> Uuid {
        self.store.state.native_store_id.unwrap()
    }
    pub(crate) fn topology(&self) -> &ClusterTopology {
        &self.store.state.committed_topology
    }
    pub(crate) fn state_sha256(&self) -> Result<String> {
        self.require_current()?;
        state_checksum(&self.store.state)
    }
}

impl MetadataConsensusStore {
    pub(crate) fn require_listener_database(
        &self,
        actual: &crate::distribution_data::ClusterServiceOpenGuard,
    ) -> Result<()> {
        actual.require_live()?;
        if actual
            .native_destination_birth()
            .is_some_and(|birth| birth.writer_enforcement_version() == 1)
        {
            let owner = self.native_owner.as_ref().ok_or_else(|| {
                metadata_error("enforced metadata listener requires actual same-open owned store")
            })?;
            owner.require_current()?;
            let actor = owner
                .actor
                .as_ref()
                .ok_or_else(|| metadata_error("metadata listener actor is not bound"))?;
            actual.require_owned_metadata_open(
                &owner.actual_open,
                &self.state.cluster_id,
                &self.state.node_id,
                &actor.birth,
            )?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn test_native_metadata_identity(&self) -> Option<Uuid> {
        self.state.native_store_id
    }

    pub(crate) fn registered_native_learner_store_id(&self) -> Result<Uuid> {
        self.require_native_metadata_actor_current(false)?;
        let owner = self
            .native_owner
            .as_ref()
            .ok_or_else(|| metadata_error("actual native metadata owner absent"))?;
        let id = owner
            .registered_store_id
            .ok_or_else(|| metadata_error("Prepared native metadata remains quarantined"))?;
        if self.state.native_store_id != Some(id)
            || self
                .state
                .committed_topology
                .nodes
                .get(&self.state.node_id)
                .map(|n| n.metadata_role)
                != Some(MetadataMemberRole::Learner)
        {
            return Err(metadata_error(
                "metadata attachment requires the registered actual learner identity",
            ));
        }
        Ok(id)
    }

    /// Historical identity inspection only; no owner/read/quorum proof is minted.
    pub(crate) fn inspect_native_store_identity(
        root: impl AsRef<Path>,
        store_id: Uuid,
        database: &BicDb,
        native: &crate::db::NativeRecoveryAdmission,
        original_member: &crate::distribution::ClusterNode,
    ) -> Result<()> {
        native.require_current(database)?;
        let state = decode_state(&fs::read(
            root.as_ref().join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
        )?)?;
        validate_state(&state)?;
        if state.native_store_id != Some(store_id)
            || &state.cluster_id != native.cluster_id()
            || &state.node_id != native.local_node_id()
            || state.committed_topology.range_image_bootstrap_id != Some(native.bootstrap_id())
        {
            return Err(metadata_error(
                "historical preparation metadata identity changed",
            ));
        }
        let actual_birth = crate::distribution::NativeStoreBirthV1::from_actual_native(native)?;
        let member = state
            .committed_topology
            .nodes
            .get(native.local_node_id())
            .ok_or_else(|| metadata_error("historical preparation actor was removed"))?;
        let original_pins = original_member
            .tls_certificate_sha256
            .iter()
            .chain(original_member.pending_tls_certificate_sha256.iter())
            .collect::<BTreeSet<_>>();
        let current_pins = member
            .tls_certificate_sha256
            .iter()
            .chain(member.pending_tls_certificate_sha256.iter())
            .collect::<BTreeSet<_>>();
        if original_member.id != *native.local_node_id()
            || original_member.incarnation != native.local_member_incarnation()
            || member.lifecycle != ClusterNodeLifecycle::Active
            || member.incarnation != native.local_member_incarnation()
            || member.native_store_birth_v1.as_ref() != Some(&actual_birth)
            || original_pins.is_empty()
            || original_pins.is_disjoint(&current_pins)
        {
            return Err(metadata_error(
                "historical preparation no longer names the actual authorized actor",
            ));
        }
        native.require_current(database)?;
        Ok(())
    }
    pub(crate) fn native_initial_pre_vote_fact(&self) -> Result<NativeMetadataPreVoteFact<'_>> {
        let fact = NativeMetadataPreVoteFact { store: self };
        fact.require_current()?;
        Ok(fact)
    }
    pub(super) fn require_native_learner_promotions_ready(
        &self,
        proposed: &ClusterTopology,
    ) -> Result<()> {
        let enforced_cluster = self
            .state
            .committed_topology
            .nodes
            .values()
            .chain(proposed.nodes.values())
            .any(|node| {
                node.native_store_birth_v1
                    .as_ref()
                    .is_some_and(|birth| birth.writer_enforcement_version() == 1)
            });
        if !enforced_cluster {
            return Ok(());
        }
        for (id, next) in &proposed.nodes {
            let Some(previous) = self.state.committed_topology.nodes.get(id) else {
                continue;
            };
            if previous.metadata_role != MetadataMemberRole::Learner
                || next.metadata_role != MetadataMemberRole::Voter
            {
                continue;
            }
            if !known_born_v2(&self.state.committed_topology, id) {
                return Err(metadata_error(
                    "native learner promotion requires committed actual factory enrollment",
                ));
            }
            // This is the leader's actual replication progress, not the
            // installed-index number submitted by a promotion caller. The
            // authenticated transport and owned receiver bind the ACK to the
            // physical native member before record_append_response counts it.
            if self.state.commit_index == 0
                || self.match_index.get(id).copied().unwrap_or(0) < self.state.commit_index
            {
                return Err(metadata_error(
                    "native learner promotion requires actual caught-up metadata acknowledgement",
                ));
            }
        }
        Ok(())
    }

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
        if self.native_owner.is_none()
            && (self.state.native_store_id.is_some()
                || known_born_v2(&self.state.committed_topology, &self.state.node_id))
        {
            return Err(metadata_error(
                "protected/born-v2 metadata requires actual native owned store",
            ));
        }
        Ok(())
    }
    /// Explicit initial-host recovery of a protected NEVER-EXPOSED prepare.
    /// Registered/maybe-voted state uses strict reopen; it is never reset.
    pub(crate) fn prepare_initial_native_owned(
        root: impl AsRef<Path>,
        database: Arc<RwLock<BicDb>>,
        factory: &FreshRangeGenesisAuthority,
        topology: ClusterTopology,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        let node = factory.node_id().clone();
        let record = {
            let db = database
                .try_read()
                .ok_or_else(|| metadata_error("initial DB busy"))?;
            let member = topology
                .nodes
                .get(&node)
                .ok_or_else(|| metadata_error("initial local member absent"))?;
            db.native_metadata_initialization_record(
                &topology.cluster_id,
                topology
                    .range_image_bootstrap_id
                    .ok_or_else(|| metadata_error("initial bootstrap absent"))?,
                &node,
                member.incarnation,
            )?
        };
        let Some(record) = record else {
            return Self::initialize_native_owned(root, database, factory, topology, cancellation);
        };
        if record.registered() {
            return Self::reopen_native_owned(root, database, node, topology, cancellation);
        }
        let plain = hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(&topology, 4 * 1024 * 1024)?,
        ));
        if record.initial_topology_sha256() != plain {
            return Err(metadata_error(
                "protected never-exposed prepare names another initial C",
            ));
        }
        let root = root.as_ref();
        let mut owner = NativeMetadataStoreOwner::acquire(root, Arc::clone(&database), true)?;
        let state = match fs::read(root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)) {
            Ok(bytes) => decode_state(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => MetadataConsensusState {
                format_version: NATIVE_METADATA_CONSENSUS_FORMAT,
                native_store_id: Some(record.store_id()),
                cluster_id: topology.cluster_id.clone(),
                node_id: node.clone(),
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
            },
            Err(e) => return Err(e.into()),
        };
        validate_state(&state)?;
        if state.native_store_id != Some(record.store_id())
            || state.committed_topology != topology
            || state.node_id != node
        {
            return Err(metadata_error(
                "protected initial metadata identity/C changed",
            ));
        }
        {
            let actual_database = database
                .try_read()
                .ok_or_else(|| metadata_error("initial DB busy binding resumed actor"))?;
            owner.bind_actual_actor(&actual_database, &topology, &node)?;
        }
        let mut store = Self {
            root: root.to_path_buf(),
            fsync: true,
            state,
            match_index: BTreeMap::new(),
            last_leader_contact_ms: unix_time_ms(),
            native_owner: Some(owner),
        };
        store.native_initial_pre_vote_fact()?.require_current()?;
        store.reset_match_indexes();
        store.persist()?;
        cancellation.check()?;
        database
            .try_read()
            .ok_or_else(|| metadata_error("initial DB busy sealing original preparation"))?
            .finish_native_metadata_store(&record, &topology)?;
        store.native_owner.as_mut().unwrap().registered_store_id = Some(record.store_id());
        cancellation.check()?;
        Ok(store)
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
        cut: impl FnMut(NativeMetadataInitializationCut) -> Result<()>,
    ) -> Result<Self> {
        let mut retained = None;
        Self::initialize_native_owned_into(
            root,
            database,
            factory,
            topology,
            cancellation,
            &mut retained,
            cut,
        )?;
        retained.ok_or_else(|| metadata_error("actual initialized owner absent"))
    }

    /// Host-only ownership continuation. The caller keeps this slot on EVERY
    /// error. A Prepared owner cannot be exposed or recreated as term zero.
    pub(crate) fn initialize_native_learner_owned_retaining(
        root: &Path,
        database: Arc<RwLock<BicDb>>,
        factory: &FreshRangeGenesisAuthority,
        topology: ClusterTopology,
        cancellation: &CancellationToken,
        retained: &mut Option<Self>,
    ) -> Result<()> {
        if retained.is_some()
            || topology
                .nodes
                .get(factory.node_id())
                .map(|n| n.metadata_role)
                != Some(MetadataMemberRole::Learner)
        {
            return Err(metadata_error(
                "learner host initialization requires an empty owner slot and actual learner",
            ));
        }
        Self::initialize_native_owned_into(
            root,
            database,
            factory,
            topology,
            cancellation,
            retained,
            |_| Ok(()),
        )
    }

    #[cfg(test)]
    pub(crate) fn test_initialize_native_learner_owned_retaining_with(
        root: &Path,
        database: Arc<RwLock<BicDb>>,
        factory: &FreshRangeGenesisAuthority,
        topology: ClusterTopology,
        cancellation: &CancellationToken,
        retained: &mut Option<Self>,
        cut: impl FnMut(NativeMetadataInitializationCut) -> Result<()>,
    ) -> Result<()> {
        if retained.is_some()
            || topology
                .nodes
                .get(factory.node_id())
                .map(|n| n.metadata_role)
                != Some(MetadataMemberRole::Learner)
        {
            return Err(metadata_error(
                "actual learner initialization test scope differs",
            ));
        }
        Self::initialize_native_owned_into(
            root,
            database,
            factory,
            topology,
            cancellation,
            retained,
            cut,
        )
    }

    fn initialize_native_owned_into(
        root: &Path,
        database: Arc<RwLock<BicDb>>,
        factory: &FreshRangeGenesisAuthority,
        topology: ClusterTopology,
        cancellation: &CancellationToken,
        retained: &mut Option<Self>,
        mut cut: impl FnMut(NativeMetadataInitializationCut) -> Result<()>,
    ) -> Result<()> {
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
        *retained = Some(Self {
            root: root.to_path_buf(),
            fsync: true,
            state,
            match_index: BTreeMap::new(),
            last_leader_contact_ms: unix_time_ms(),
            native_owner: Some(owner),
        });
        // The new host retains the real owner as soon as its identity exists,
        // including failures during the first protected native publication.
        let prepared = database
            .try_read()
            .ok_or_else(|| metadata_error("native DB busy before metadata prepare"))?
            .prepare_native_metadata_store(factory, &topology, store_id)?;
        cancellation.check()?;
        cut(NativeMetadataInitializationCut::AfterProtectedPrepare)?;
        let store = retained
            .as_mut()
            .expect("actual initializer retained its owner");
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
        store.native_owner.as_mut().unwrap().registered_store_id = Some(prepared.store_id());
        cancellation.check()?;
        Ok(())
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
        owner.registered_store_id = Some(receipt.store_id());
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

#[cfg(test)]
mod learner_tests;
