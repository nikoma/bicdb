//! Collection storage names and catalog-only renames.
//!
//! Rows are stored under a collection's *storage name* (segment file in
//! embedded mode, key prefix in paged mode), fixed when the collection is
//! created. Renaming a collection therefore changes only the catalog: the
//! logical name moves, the storage name stays. A new collection that would
//! collide with a storage name still in use receives a fresh unique one.
//!
//! Publication order makes a crash at any point recoverable: the collection
//! catalog is written first with `renamed_from` set (open maps index
//! definitions still recorded under the old name through it), then the index
//! catalog, then the marker is cleared. Embedded mode checkpoints first, so no
//! write-ahead-log frame names the old collection when the rename publishes.
use super::*;

const STORAGE_NAME_PREFIX: &str = "__bicdb_st_";

/// Internal collection through which a replication primary ships catalog
/// operations (create, drop, rename collection). Catalog changes are not
/// transactional writes, so each one is followed by a small commit here; a
/// standby replays the operation when it applies that commit, in stream order.
pub(crate) const CATALOG_LOG_COLLECTION: &str = "__bicdb_catalog_log";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum CatalogOp {
    Create {
        name: String,
        mode: CollectionMode,
    },
    Drop {
        name: String,
    },
    Rename {
        from: String,
        to: String,
    },
    /// The primary's complete index catalog; standbys build what they lack
    /// from their own rows.
    IndexCatalog {
        definitions: Vec<IndexDefinition>,
    },
    MeshAuthorization {
        name: String,
        enabled: bool,
    },
}

impl BicDb {
    pub(crate) fn validate_replication_reader_requirements(frame: &CommitFrame) -> Result<()> {
        let require = |name: &str| -> Result<()> {
            let feature = match name {
                "__bicdb_rename_intents" | "__bicdb_rename_images" | "__bicdb_rename_gc" => {
                    Some(format::SQL_RENAME_INTENTS_FEATURE_FLAG)
                }
                "__bicdb_recycle_intents"
                | "__bicdb_recycle_purges"
                | "__bicdb_recycle_creations" => Some(format::SQL_CATALOG_INTENTS_FEATURE_FLAG),
                _ => None,
            };
            if let Some(feature) = feature {
                if !frame
                    .required_reader_features
                    .iter()
                    .any(|present| present == feature)
                {
                    return Err(BicDbError::Replication(format!(
                        "replicated collection `{name}` requires reader capability `{feature}`"
                    )));
                }
            }
            Ok(())
        };
        for write in &frame.writes {
            require(&write.collection)?;
            if let Some(meta) = &write.collection_meta {
                require(&meta.name)?;
            }
            if write.collection == CATALOG_LOG_COLLECTION
                && write.operation != ReplicationOperationType::Delete
            {
                let record: Record = serde_json::from_slice(&write.payload)?;
                let op: CatalogOp = serde_json::from_value(record.metadata)?;
                match op {
                    CatalogOp::Create { name, .. }
                    | CatalogOp::Drop { name }
                    | CatalogOp::MeshAuthorization { name, .. } => require(&name)?,
                    CatalogOp::Rename { from, to } => {
                        require(&from)?;
                        require(&to)?;
                    }
                    CatalogOp::IndexCatalog { definitions } => {
                        for definition in definitions {
                            require(&definition.collection)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// The physical storage name of a logical collection.
    pub(crate) fn storage_name_of(&self, collection: &str) -> String {
        self.storage_aliases
            .read()
            .get(collection)
            .cloned()
            .unwrap_or_else(|| collection.to_string())
    }

    /// `Some(fresh name)` when `name` is already some collection's storage
    /// name; `None` when the new collection can store under its own name.
    pub(super) fn storage_name_for_new(&self, name: &str) -> Option<String> {
        let taken = self
            .collections
            .values()
            .any(|state| state.read().meta.storage() == name);
        taken.then(|| format!("{STORAGE_NAME_PREFIX}{}", Uuid::new_v4().simple()))
    }

    pub(super) fn set_storage_alias(&self, logical: &str, storage: &str) {
        let mut aliases = self.storage_aliases.write();
        if logical == storage {
            aliases.remove(logical);
        } else {
            aliases.insert(logical.to_string(), storage.to_string());
        }
        if let Some(paged) = self.paged_records.as_ref() {
            paged.set_collection_aliases(aliases.clone());
        }
    }

    pub(super) fn remove_storage_alias(&self, logical: &str) {
        let mut aliases = self.storage_aliases.write();
        if aliases.remove(logical).is_some() {
            if let Some(paged) = self.paged_records.as_ref() {
                paged.set_collection_aliases(aliases.clone());
            }
        }
    }

    fn replicates_catalog(&self) -> bool {
        self.config.replication.enabled && self.ha_node.role() == NodeRole::Primary
    }

    /// Additional Native backings touched by catalog operations. A trusted
    /// host must declare these resources before delegating scoped DDL effects;
    /// the names are owned here, alongside the actual catalog-log writer.
    /// This exposes the resource inventory and changes no replication format,
    /// lifecycle operation, mutation grant or durability requirement.
    pub fn catalog_operation_scope_collections(&self) -> &'static [&'static str] {
        if self.replicates_catalog() {
            &[CATALOG_LOG_COLLECTION]
        } else {
            &[]
        }
    }

    /// On a replication primary, ships `op` to standbys (see
    /// [`CATALOG_LOG_COLLECTION`]).
    pub(super) fn log_catalog_op(&mut self, op: CatalogOp) -> Result<()> {
        if !self.replicates_catalog() {
            return Ok(());
        }
        if !self.collections.contains_key(CATALOG_LOG_COLLECTION) {
            let meta = self
                .new_local_collection_metadata(&CollectionMeta::standard(CATALOG_LOG_COLLECTION));
            self.collections.insert(
                CATALOG_LOG_COLLECTION.to_string(),
                RwLock::new(CollectionState::new(meta)),
            );
            self.persist_catalog()?;
        }
        let record = Record::new(Uuid::new_v4().simple().to_string())
            .with_metadata(serde_json::to_value(&op)?);
        let mut tx = self.begin_transaction()?;
        tx.insert(CATALOG_LOG_COLLECTION, record)?;
        tx.commit()
    }

    /// Standby side: replays the catalog operations carried by `frame`.
    pub(crate) fn apply_replicated_catalog_ops(&mut self, frame: &CommitFrame) -> Result<()> {
        for write in &frame.writes {
            if write.collection != CATALOG_LOG_COLLECTION
                || write.operation == ReplicationOperationType::Delete
            {
                continue;
            }
            let record: Record = serde_json::from_slice(&write.payload)?;
            let op: CatalogOp = serde_json::from_value(record.metadata)?;
            match op {
                CatalogOp::Create { name, mode } => {
                    if !self.collections.contains_key(&name) {
                        validate_collection_name(&name)?;
                        let storage_name = self.storage_name_for_new(&name);
                        let mut meta = CollectionMeta::standard(name.clone());
                        meta.mode = mode;
                        meta.storage_name = storage_name.clone();
                        meta = self.new_local_collection_metadata(&meta);
                        if let Some(storage) = storage_name {
                            self.set_storage_alias(&name, &storage);
                        }
                        self.collections
                            .insert(name.clone(), RwLock::new(CollectionState::new(meta)));
                        self.persist_catalog()?;
                    }
                }
                CatalogOp::Drop { name } => {
                    if self.collections.contains_key(&name) {
                        self.drop_collection_unchecked(&name)?;
                    }
                }
                CatalogOp::Rename { from, to } => {
                    if self.collections.contains_key(&from) && !self.collections.contains_key(&to) {
                        self.rename_collection_unchecked(&from, &to)?;
                    }
                }
                CatalogOp::IndexCatalog { definitions } => {
                    self.reconcile_local_indexes(&definitions)?;
                }
                CatalogOp::MeshAuthorization { name, enabled } => {
                    let state = self.collection_state_mut(&name)?;
                    if enabled && state.meta.policy.is_some() {
                        return Err(replication_error(
                            "replicated mesh authorization targets a protected collection",
                        ));
                    }
                    state.meta.mesh_sync_enabled = enabled;
                    self.persist_catalog()?;
                    self.invalidate_sync_vector_cache();
                }
            }
        }
        Ok(())
    }

    /// Creates the catalog log of a replicating primary if it is missing, so
    /// catalog announcements need no exclusive access later.
    pub fn ensure_catalog_log(&mut self) -> Result<()> {
        if !self.replicates_catalog() || self.collections.contains_key(CATALOG_LOG_COLLECTION) {
            return Ok(());
        }
        let meta =
            self.new_local_collection_metadata(&CollectionMeta::standard(CATALOG_LOG_COLLECTION));
        self.collections.insert(
            CATALOG_LOG_COLLECTION.to_string(),
            RwLock::new(CollectionState::new(meta)),
        );
        self.persist_catalog()
    }

    /// Primary: announces the index catalog to standbys when it changed since
    /// the last announcement. Returns whether it announced.
    pub fn announce_index_catalog(&self) -> Result<bool> {
        if !self.replicates_catalog() || !self.collections.contains_key(CATALOG_LOG_COLLECTION) {
            return Ok(false);
        }
        let definitions = self.index_definitions();
        let mut announced = self.announced_index_catalog.lock();
        if announced.as_ref() == Some(&definitions) {
            return Ok(false);
        }
        let op = CatalogOp::IndexCatalog {
            definitions: definitions.clone(),
        };
        let record = Record::new(Uuid::new_v4().simple().to_string())
            .with_metadata(serde_json::to_value(&op)?);
        let mut tx = self.begin_transaction()?;
        tx.insert(CATALOG_LOG_COLLECTION, record)?;
        tx.commit()?;
        *announced = Some(definitions);
        Ok(true)
    }

    /// Standby: makes the local indexes match the primary's catalog, building
    /// each from this node's own rows. A failed build is reported and skipped:
    /// reads stay correct without the index.
    fn reconcile_local_indexes(&mut self, wanted: &[IndexDefinition]) -> Result<()> {
        self.local_index_maintenance = true;
        let result = (|| {
            for existing in self.index_definitions() {
                if !wanted.iter().any(|definition| definition == &existing) {
                    self.drop_index(&existing.name)?;
                }
            }
            for definition in wanted {
                if self.indexes.contains_key(&definition.name) {
                    continue;
                }
                if !self.collections.contains_key(&definition.collection) {
                    return Err(replication_error(format!(
                        "index {} references a missing replicated table",
                        definition.name
                    )));
                }
                self.create_index_online(definition.clone())?;
            }
            Ok(())
        })();
        self.local_index_maintenance = false;
        result
    }

    /// Renames a collection without moving its rows. Its indexes, vector index,
    /// planner statistics, policy and flashback history follow it.
    ///
    /// Refused for collections whose name other durable state refers to and
    /// that would not follow: mesh-synchronized collections, graph projection
    /// sources, memory indexes, and databases that replicate or distribute by
    /// collection name.
    pub fn rename_collection(&mut self, old: &str, new: &str) -> Result<()> {
        self.ensure_writable("rename collection")?;
        self.rename_collection_unchecked(old, new)?;
        self.log_catalog_op(CatalogOp::Rename {
            from: old.to_string(),
            to: new.to_string(),
        })
    }

    fn rename_collection_unchecked(&mut self, old: &str, new: &str) -> Result<()> {
        validate_collection_name(old)?;
        validate_collection_name(new)?;
        if old == new {
            return Ok(());
        }
        if !self.collections.contains_key(old) {
            return Err(BicDbError::CollectionNotFound(old.to_string()));
        }
        if self.collections.contains_key(new) {
            return Err(BicDbError::CollectionAlreadyExists(new.to_string()));
        }
        self.ensure_renamable(old)?;
        let history = self
            .collection_state(old)?
            .meta
            .flashback
            .is_some()
            .then(|| {
                (
                    flashback_history_collection(old),
                    flashback_history_collection(new),
                )
            })
            .filter(|(old_history, _)| self.collections.contains_key(old_history));
        if let Some((_, new_history)) = history.as_ref() {
            if self.collections.contains_key(new_history) {
                return Err(BicDbError::CollectionAlreadyExists(new_history.clone()));
            }
        }

        if self.paged_records.is_none() {
            // Embedded mode replays the write-ahead log by logical name: fold it
            // into segments so no frame names the old collection afterwards.
            let plan = self.checkpoint_begin()?;
            self.checkpoint_write_segments(&plan)?;
            let keep_from = plan.keep_from;
            self.checkpoint_truncate_wal(keep_from)?;
        }
        // The startup snapshot is keyed by logical name.
        crate::snapshot::invalidate(&self.path)?;

        self.rename_collection_entry(old, new)?;
        if let Some((old_history, new_history)) = history.as_ref() {
            self.rename_collection_entry(old_history, new_history)?;
        }
        for name in std::iter::once(new).chain(history.as_ref().map(|(_, h)| h.as_str())) {
            if let Ok(mut state) = self.collection_state_mut(name) {
                state.meta.renamed_from = None;
            }
        }
        self.persist_catalog()?;
        self.flashback_refresh_tracked();
        self.refresh_graph_projections()?;
        self.schema_compatibility.invalidate();
        Ok(())
    }

    fn ensure_renamable(&self, collection: &str) -> Result<()> {
        let refuse = |reason: &str| {
            Err(BicDbError::Unsupported(format!(
                "collection `{collection}` cannot be renamed: {reason}"
            )))
        };
        if self.commit_admission.read().is_some() || self.config.require_commit_admission {
            return refuse("distributed placement addresses collections by name");
        }
        if self.collection_state(collection)?.meta.mesh_sync_enabled {
            return refuse("it is mesh-synchronized");
        }
        if self.graphs.read().values().any(|graph| {
            graph
                .definition
                .node_sources
                .iter()
                .any(|source| source.collection == collection)
                || graph
                    .definition
                    .edge_sources
                    .iter()
                    .any(|source| source.collection == collection)
        }) {
            return refuse("a graph projection reads it");
        }
        if self
            .memory_indexes
            .read()
            .values()
            .any(|index| index.collection == collection)
        {
            return refuse("a memory index reads it");
        }
        Ok(())
    }

    /// Moves one catalog entry, publishing the collection catalog (with
    /// `renamed_from`) before the index catalog.
    fn rename_collection_entry(&mut self, old: &str, new: &str) -> Result<()> {
        let state = self
            .collections
            .remove(old)
            .ok_or_else(|| BicDbError::CollectionNotFound(old.to_string()))?;
        let mut state = state.into_inner();
        let storage = state.meta.storage().to_string();
        state.meta.name = new.to_string();
        state.meta.storage_name = (storage != new).then(|| storage.clone());
        state.meta.renamed_from = Some(old.to_string());
        self.collections.insert(new.to_string(), RwLock::new(state));
        {
            let mut aliases = self.storage_aliases.write();
            aliases.remove(old);
            if storage != new {
                aliases.insert(new.to_string(), storage);
            }
            if let Some(paged) = self.paged_records.as_ref() {
                paged.set_collection_aliases(aliases.clone());
            }
        }
        self.persist_catalog()?;

        let mut indexes_changed = false;
        for index in self.indexes.values_mut() {
            let definition = &mut index.get_mut().definition;
            if definition.collection == old {
                definition.collection = new.to_string();
                indexes_changed = true;
            }
        }
        if indexes_changed {
            self.index_generation
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.persist_index_catalog()?;
        }

        let vector_index = self.hnsw_indexes.write().remove(old);
        if let Some(vector_index) = vector_index {
            self.hnsw_indexes
                .write()
                .insert(new.to_string(), vector_index);
        }
        match fs::rename(self.hnsw_index_path(old), self.hnsw_index_path(new)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        if let Some(stats) = self.planner_stats.tables.remove(old) {
            self.planner_stats.tables.insert(new.to_string(), stats);
            self.persist_planner_stats()?;
        }
        Ok(())
    }
}

const PENDING_CATALOG: &str = "replication-catalog-pending.json";
impl BicDb {
    /// Publish catalog redo before the row WAL can advance the recovered
    /// watermark. A crash between those two durability boundaries must not
    /// make the next sender skip a still-unapplied DROP/RENAME/index catalog.
    pub(super) fn stage_pending_replication_catalog(&self, frame: &CommitFrame) -> Result<()> {
        if self.path.join(PENDING_CATALOG).exists() {
            return Err(replication_error(
                "catalog apply is incomplete; reopen this replica to finish its durable redo",
            ));
        }
        if !frame
            .writes
            .iter()
            .any(|write| write.collection == CATALOG_LOG_COLLECTION)
        {
            return Ok(());
        }
        // The durable redo of a catalog frame on a standby: once the rename
        // happened and its directory sync failed, the next open may or may
        // not redo the frame, so the outcome is unknown and the standby is
        // fenced.
        write_sidecar(
            &self.path.join(PENDING_CATALOG),
            FrameKind::Index,
            &serde_json::to_vec(frame)?,
            self.config.fsync,
            &self.encryption,
            "pending_catalog",
            storage::FailClosed::AfterRename(&self.durability),
        )
    }

    pub(super) fn finish_pending_replication_catalog(&self) -> Result<()> {
        match fs::remove_file(self.path.join(PENDING_CATALOG)) {
            Ok(()) => {
                if self.config.fsync {
                    File::open(&self.path)?.sync_all()?;
                }
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn recover_pending_replication_catalog(&mut self) -> Result<()> {
        let Some(bytes) = read_optional_sidecar(
            &self.path.join(PENDING_CATALOG),
            FrameKind::Index,
            &self.encryption,
        )?
        else {
            return Ok(());
        };
        let frame: CommitFrame = serde_json::from_slice(&bytes)?;
        frame.verify_checksum()?;
        if frame.cluster_id != self.config.replication.cluster_id && self.config.replication.enabled
        {
            return Err(replication_error(
                "pending catalog redo belongs to another cluster",
            ));
        }
        Self::validate_replication_reader_requirements(&frame)?;
        self.install_replicated_reader_features(&frame.required_reader_features)?;
        if frame.commit_seq <= self.current_commit_seq() {
            self.apply_replicated_catalog_ops(&frame)?;
        }
        self.finish_pending_replication_catalog()
    }
}

#[cfg(test)]
mod redo_tests {
    use super::*;
    #[test]
    fn replica_catalog_redo_finishes_a_rename_after_its_row_wal_committed() {
        for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
            let root = tempfile::tempdir().unwrap();
            let replication = |mode| ReplicationConfig {
                enabled: true,
                mode,
                listen_addr: Some("127.0.0.1:0".into()),
                tls: Some(crate::ReplicationTlsConfig {
                    cert_path: PathBuf::new(),
                    key_path: PathBuf::new(),
                    ca_path: PathBuf::new(),
                    require_client_cert: true,
                    dev_localhost_plaintext: true,
                }),
                ..ReplicationConfig::default()
            };
            let primary_config = DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode.clone())
                .with_replication(replication(ReplicationMode::Primary));
            let standby_config = DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode)
                .with_replication(replication(ReplicationMode::Standby));
            let mut primary =
                BicDb::open_with_config(root.path().join("p"), primary_config).unwrap();
            let mut standby =
                BicDb::open_with_config(root.path().join("s"), standby_config.clone()).unwrap();
            primary.create_collection("before").unwrap();
            primary.insert("before", Record::new("retained")).unwrap();
            let mut cursor = ReplicationCursor::new(0);
            standby
                .apply_replication_batch(
                    &primary.replication_frames_after(&mut cursor, 100).unwrap(),
                )
                .unwrap();
            primary.rename_collection("before", "after").unwrap();
            let frames = primary.replication_frames_after(&mut cursor, 100).unwrap();
            let frame = frames.last().unwrap();
            standby.ensure_replication_frame_collections(frame).unwrap();
            standby.stage_pending_replication_catalog(frame).unwrap();
            let writes = replication_writes_to_tx_writes(frame).unwrap();
            standby
                .apply_imported_replication_commit(
                    frame.tx_id,
                    frame.commit_seq,
                    frame.timestamp,
                    &writes,
                    &frame.journal,
                )
                .unwrap();
            // Simulate death at the boundary before the catalog operation.
            assert!(standby.collections.contains_key("before"));
            drop(standby);
            let standby = BicDb::open_with_config(root.path().join("s"), standby_config).unwrap();
            assert!(standby
                .get_unchecked("after", "retained")
                .unwrap()
                .is_some());
            assert!(!standby.collections.contains_key("before"));
            assert!(!standby.path.join(PENDING_CATALOG).exists());
        }
    }
}

// Storage-mode copy recreates physical row ownership while preserving the
// complete logical collection contract, including empty vector dimensions and
// below-SQL mutation grants. Call only after trusted rows have been copied.
impl BicDb {
    pub(crate) fn restore_migrated_collection_contract(
        &mut self,
        source: &CollectionMeta,
    ) -> Result<()> {
        self.ensure_writable("restore migrated collection contract")?;
        if source.flashback.is_some() {
            return Err(BicDbError::FormatCompatibility(
                "flashback collection contract requires history/SCN migration".into(),
            ));
        }
        let target = self.collection_state_mut(&source.name)?;
        let mut copied = source.clone();
        copied.storage_name = target.meta.storage_name.clone();
        copied.local_namespace_id = target.meta.local_namespace_id;
        copied.renamed_from = None;
        target.meta = copied;
        self.persist_catalog()?;
        self.schema_compatibility.invalidate();
        Ok(())
    }
}

impl BicDb {
    /// Validate a host's live catalog-undo owner. Startup recovery still
    /// requires complete quiescence; live undo may retain only this pending
    /// transaction. This does not grant mutation authority or relax rename's
    /// checks on native grants, incarnations, or row-lock ownership.
    pub fn check_catalog_undo_owner(&self, tx: &Transaction) -> Result<()> {
        self.check_recovery_state()?;
        if !std::ptr::eq(tx.db.as_ptr(), self) || !Arc::ptr_eq(&tx.tx_states, &self.tx_states) {
            return Err(BicDbError::Unsupported(
                "catalog undo owner belongs to a different database".into(),
            ));
        }
        if tx.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        let other_pending = self.tx_states.shards.iter().any(|shard| {
            shard
                .lock()
                .iter()
                .any(|(id, state)| *id != tx.id.0 && *state == TxState::Pending)
        });
        let other_locks = self.write_locks.values_any(|owner| *owner != tx.id);
        if other_pending || other_locks {
            return Err(BicDbError::TransactionConflict(
                "cannot undo catalog while another transaction is pending or holds row locks"
                    .into(),
            ));
        }
        self.validate_transaction_collection_incarnations(tx)?;
        self.check_recovery_state()
    }

    /// Rename a collection and retain one transaction's writes/read observations
    /// on that same backing. Catalog mutation requires an exclusive database
    /// borrow and either a declared catalog scope or no other pending native
    /// transaction/retained row lock. The host remains responsible for SQL
    /// catalog visibility and transactional DDL undo. Native grants retain
    /// their immutable relation restrictions.
    pub fn rename_collection_preserving_pending_writes(
        &mut self,
        tx: &mut Transaction,
        old: &str,
        new: &str,
    ) -> Result<()> {
        self.check_recovery_state()?;
        let result = self.rename_collection_preserving_pending_writes_inner(tx, old, new);
        self.check_recovery_state()?;
        result
    }
    fn rename_collection_preserving_pending_writes_inner(
        &mut self,
        tx: &mut Transaction,
        old: &str,
        new: &str,
    ) -> Result<()> {
        // Pointer identity plus an owned per-open registry prevents address reuse
        // from admitting a transaction from another database/health instance.
        if !std::ptr::eq(tx.db.as_ptr(), self) || !Arc::ptr_eq(&tx.tx_states, &self.tx_states) {
            return Err(BicDbError::Unsupported(
                "transaction belongs to a different database instance".into(),
            ));
        }
        if tx.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        if old == new {
            return Ok(());
        }
        // A Transaction retains database access independently of this exclusive
        // borrow. Only the supplied transaction's overlays are rebound below;
        // A declared scope atomically excludes prior and future observations of
        // both names/backing. Without one, keep the conservative global gate:
        // other owners' overlays cannot be rebound by this operation. Locks
        // are checked separately while a completed transaction releases them.
        if !self
            .catalog_admission
            .owner_covers(tx.id, tx.catalog_actor_scope, old, new)
        {
            let other_pending = self.tx_states.shards.iter().any(|shard| {
                shard
                    .lock()
                    .iter()
                    .any(|(id, state)| *id != tx.id.0 && *state == TxState::Pending)
            });
            let other_locks = self.write_locks.values_any(|owner| *owner != tx.id);
            if other_pending || other_locks {
                return Err(BicDbError::TransactionConflict(
                    "cannot rename while another transaction is pending or holds row locks".into(),
                ));
            }
        }
        // Application grants/commit validators are bound to authored relation
        // names. Rebinding an already-authorized write would change that
        // authority after validation. Native catalog migration needs its own
        // explicit contract; refuse before changing either backing or writes.
        if tx.mutation_actor.is_some() && tx.writes.iter().any(|write| write.collection == old) {
            return Err(BicDbError::MutationDenied(
                "cannot rename a collection with pending native application writes".into(),
            ));
        }
        if tx.write_index.contains_key(new) {
            return Err(BicDbError::Unsupported(
                "cannot rename onto a name with pending transaction writes".into(),
            ));
        }
        let moved_locks = tx
            .locked_keys
            .lock()
            .order
            .iter()
            .filter(|key| key.0 == old)
            .map(|key| (key.clone(), (new.to_string(), key.1.clone())))
            .collect::<Vec<_>>();
        for (source, target) in &moved_locks {
            if self.write_locks.get_copied(source) != Some(tx.id)
                || self.write_locks.get_copied(target).is_some()
            {
                return Err(BicDbError::TransactionConflict(
                    "cannot rename inconsistent or occupied transaction row locks".into(),
                ));
            }
        }
        self.validate_transaction_collection_incarnations(tx)?;
        tx.bind_collection_incarnation(old)?;
        self.rename_collection(old, new)?;
        {
            let mut bindings = tx.collection_incarnations.lock();
            bindings.remove(new);
            if let Some(incarnation) = bindings.remove(old) {
                bindings.insert(new.to_string(), incarnation);
            }
        }
        // The logical name is the row-lock identity. Publish the renamed lock
        // before releasing its former identity, and retain acquisition order
        // so existing rollback marks still release the same suffix.
        for (_, target) in &moved_locks {
            self.write_locks.insert(target.clone(), tx.id);
        }
        {
            let mut locked = tx.locked_keys.lock();
            for key in &mut locked.order {
                if key.0 == old {
                    key.0 = new.to_string();
                }
            }
            for (source, target) in &moved_locks {
                locked.held.remove(source);
                locked.held.insert(target.clone());
            }
        }
        for (source, _) in &moved_locks {
            release_owned_write_locks(&self.write_locks, tx.id, std::slice::from_ref(source));
        }
        for write in &mut tx.writes {
            if write.collection == old {
                write.collection = new.to_string();
            }
        }
        tx.rebuild_write_index();
        {
            let mut versions = tx.observed_record_versions.lock();
            versions.remove(new);
            if let Some(rows) = versions.remove(old) {
                versions.insert(new.to_string(), rows);
            }
        }
        {
            let mut images = tx.read_images.lock();
            if let Some(rows) = images.rows.remove(new) {
                images.len -= rows.len();
            }
            if let Some(rows) = images.rows.remove(old) {
                images.rows.insert(new.to_string(), rows);
            }
        }
        if let Some(generation) = tx.serializable_generation_snapshot.remove(old) {
            tx.serializable_generation_snapshot
                .insert(new.to_string(), generation);
        }
        if tx.serializable_reads.remove(old) {
            tx.serializable_reads.insert(new.to_string());
        }
        Ok(())
    }
}
