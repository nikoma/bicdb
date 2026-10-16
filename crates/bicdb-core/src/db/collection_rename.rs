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

    /// On a replication primary, ships `op` to standbys (see
    /// [`CATALOG_LOG_COLLECTION`]).
    pub(super) fn log_catalog_op(&mut self, op: CatalogOp) -> Result<()> {
        if !self.replicates_catalog() {
            return Ok(());
        }
        if !self.collections.contains_key(CATALOG_LOG_COLLECTION) {
            let meta = CollectionMeta::standard(CATALOG_LOG_COLLECTION);
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
                        self.drop_collection(&name)?;
                    }
                }
                CatalogOp::Rename { from, to } => {
                    if self.collections.contains_key(&from) && !self.collections.contains_key(&to) {
                        self.rename_collection_unchecked(&from, &to)?;
                    }
                }
                CatalogOp::IndexCatalog { definitions } => {
                    self.reconcile_local_indexes(&definitions);
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
        let meta = CollectionMeta::standard(CATALOG_LOG_COLLECTION);
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
    fn reconcile_local_indexes(&mut self, wanted: &[IndexDefinition]) {
        self.local_index_maintenance = true;
        let local = self.index_definitions();
        for existing in &local {
            let keep = wanted.iter().any(|definition| definition == existing);
            if !keep {
                if let Err(error) = self.drop_index(&existing.name) {
                    eprintln!(
                        "bicdb replication: could not drop local index {}: {error}",
                        existing.name
                    );
                }
            }
        }
        for definition in wanted {
            if self.indexes.contains_key(&definition.name)
                || !self.collections.contains_key(&definition.collection)
            {
                continue;
            }
            match self.create_index_online(definition.clone()) {
                Ok(_) => eprintln!(
                    "bicdb replication: built local index {} on {}",
                    definition.name, definition.collection
                ),
                Err(error) => eprintln!(
                    "bicdb replication: could not build local index {}: {error}",
                    definition.name
                ),
            }
        }
        self.local_index_maintenance = false;
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
