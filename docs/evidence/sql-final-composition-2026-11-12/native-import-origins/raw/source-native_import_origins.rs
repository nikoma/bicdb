//! Native receiver allocation provenance. Only the opaque actual-source
//! capture enters this producer; portable metadata never enters as authority.
use super::*;
use crate::NativeImportedBackingOrigin;

fn invalid(message: &str) -> BicDbError {
    BicDbError::Replication(format!("Native imported backing: {message}"))
}
fn sha256_shape(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}
fn contract_hash(meta: &CollectionMeta) -> Result<String> {
    Ok(hex::encode(sha2::Sha256::digest(serde_json::to_vec(
        &meta.portable_contract(),
    )?)))
}

impl NativeImportedBackingOrigin {
    fn validate(&self, meta: &CollectionMeta) -> Result<()> {
        if self.version != 1
            || self.source_sequence == 0
            || [
                self.source_root,
                self.source_birth,
                self.source_namespace,
                self.target_root,
                self.target_birth,
                self.target_namespace,
                self.generation,
            ]
            .iter()
            .any(Uuid::is_nil)
            || self.source_root == self.target_root
            || !sha256_shape(&self.source_root_sha256)
            || !sha256_shape(&self.source_contract_sha256)
            || meta.native_backing_origin_root != Some(self.target_root)
            || meta.native_backing_birth_id != Some(self.target_birth)
            || meta.local_namespace_id != Some(self.target_namespace)
            || meta.storage() != self.target_storage
        {
            return Err(invalid("damaged or transplanted actual target allocation"));
        }
        validate_collection_name(&self.source_storage)?;
        validate_collection_name(&self.target_storage)?;
        Ok(())
    }
    fn same_source(&self, root: &NativeOriginObservation, fact: &CollectionMeta) -> Result<bool> {
        Ok(self.source_root == root.root_id()
            && self.source_root_sha256 == root.record_sha256()
            && Some(self.source_birth) == fact.native_backing_birth_id
            && Some(self.source_namespace) == fact.local_namespace_id
            && self.source_storage == fact.storage()
            && self.source_contract_sha256 == contract_hash(fact)?)
    }
}

pub(super) fn pending(meta: &CollectionMeta) -> bool {
    meta.native_imported_backing_origin
        .as_ref()
        .is_some_and(|origin| origin.pending)
}

pub(super) fn validate_catalog(metas: &[CollectionMeta], active: bool) -> Result<()> {
    let mut source_births = BTreeSet::new();
    let mut source_namespaces = BTreeSet::new();
    let mut generations = BTreeSet::new();
    for meta in metas {
        if let Some(origin) = &meta.native_imported_backing_origin {
            if !active {
                return Err(invalid("catalog provenance has no Native format fence"));
            }
            origin.validate(meta)?;
            if !source_births.insert((origin.source_root, origin.source_birth))
                || !source_namespaces.insert((origin.source_root, origin.source_namespace))
                || !generations.insert(origin.generation)
            {
                return Err(invalid("ambiguous imported backing or install generation"));
            }
        }
    }
    Ok(())
}

pub(super) fn validated_decisions(
    facts: BTreeMap<String, CollectionMeta>,
) -> Result<BTreeMap<String, CollectionMeta>> {
    let facts = native_wal_backings::validated(facts)?;
    validate_catalog(&facts.values().cloned().collect::<Vec<_>>(), true)?;
    if facts.values().any(|meta| {
        meta.native_catalog_allocation_pending
            || meta
                .native_imported_backing_origin
                .as_ref()
                .is_none_or(|origin| origin.pending)
    }) {
        return Err(invalid("WAL allocation decision is incomplete"));
    }
    Ok(facts)
}

/// Normalize only a pending marker whose exact actual target and accepted
/// source were committed together. Completed control metadata outlives WAL
/// cuts; a pending marker without that decision remains fenced for retry.
pub(super) fn repair_pending(
    metas: &mut [CollectionMeta],
    decisions: &BTreeMap<(Uuid, Uuid), CollectionMeta>,
) -> Result<bool> {
    let mut changed = false;
    for meta in metas {
        if !pending(meta) {
            continue;
        }
        let origin = meta.native_imported_backing_origin.as_ref().unwrap();
        if let Some(decision) = decisions.get(&(origin.target_birth, origin.generation)) {
            let mut expected = origin.clone();
            expected.pending = false;
            if decision.native_imported_backing_origin.as_ref() != Some(&expected)
                || decision.local_namespace_id != meta.local_namespace_id
                || decision.native_backing_origin_root != meta.native_backing_origin_root
                || decision.storage() != meta.storage()
            {
                return Err(invalid(
                    "pending target differs from its committed allocation decision",
                ));
            }
            meta.native_imported_backing_origin
                .as_mut()
                .unwrap()
                .pending = false;
            changed = true;
        }
    }
    Ok(changed)
}

/// Not serializable or externally constructible. The source observation and
/// facts come from the checksum-bound actual Native exporter, never JSON.
pub(super) struct AcceptedNativeImport {
    root: NativeOriginObservation,
    facts: BTreeMap<String, CollectionMeta>,
    sequence: u64,
    checksum: [u8; 32],
}

impl BicDb {
    /// Trusted host setup before this fresh primary's first allocation. An
    /// existing empty directory is not a Native factory and is not upgraded.
    pub fn initialize_fresh_primary_replication_origins(&mut self) -> Result<bool> {
        if !self.config.replication.enabled
            || self.is_replication_standby()
            || !self.config.fsync
            || !self.collections.is_empty()
            || self.current_commit_seq() != 0
            || (!self.fresh_directory_created
                && self.native_origin.is_none()
                && !native_origin::has_prepared_factory(&self.path)?)
        {
            return Ok(false);
        }
        self.activate_native_origin_root_v1()?;
        self.activate_local_namespace_identity_v1()?;
        self.activate_native_collection_birth_v1()?;
        self.activate_native_wal_backing_capture_v1()?;
        self.activate_native_catalog_allocation_facts_v1()?;
        Ok(true)
    }

    pub(super) fn accept_native_import(
        &self,
        frame: &CommitFrame,
    ) -> Result<Option<AcceptedNativeImport>> {
        let Some(facts) = frame.captured_native_catalog_allocations()? else {
            return Ok(None);
        };
        if facts.is_empty() {
            return Ok(None);
        }
        let root = frame
            .captured_native_origin_root()?
            .ok_or_else(|| invalid("actual source root is missing"))?;
        if !self.config.fsync {
            return Err(invalid("actual target allocation requires fsync"));
        }
        for (name, fact) in facts {
            validate_collection_meta_namespaces(fact)?;
            if fact.native_backing_origin_root != Some(root.root_id()) {
                return Err(invalid("actual source birth belongs to another root"));
            }
            let mut create = false;
            for write in &frame.writes {
                if write.collection == *name {
                    return Err(invalid("creation frame also writes its unpublished child"));
                }
                if write.collection == collection_rename::CATALOG_LOG_COLLECTION
                    && write.operation != ReplicationOperationType::Delete
                {
                    let record: Record = serde_json::from_slice(&write.payload)?;
                    if let collection_rename::CatalogOp::Create {
                        name: created,
                        mode,
                    } = serde_json::from_value(record.metadata)?
                    {
                        create |= created == *name && mode == fact.mode;
                    }
                }
            }
            if !create {
                return Err(invalid(
                    "source allocation has no corresponding catalog CREATE",
                ));
            }
            for existing in self.collections() {
                if existing.name != *name
                    && existing
                        .native_imported_backing_origin
                        .as_ref()
                        .is_some_and(|origin| {
                            origin.source_root == root.root_id()
                                && (Some(origin.source_birth) == fact.native_backing_birth_id
                                    || Some(origin.source_namespace) == fact.local_namespace_id)
                        })
                {
                    return Err(invalid(
                        "source allocation already belongs to another actual target",
                    ));
                }
            }
            if let Some(state) = self.collections.get(name) {
                let state = state.read();
                let origin = state
                    .meta
                    .native_imported_backing_origin
                    .as_ref()
                    .ok_or_else(|| {
                        invalid("existing unowned target cannot acquire imported provenance")
                    })?;
                origin.validate(&state.meta)?;
                if !origin.same_source(root, fact)?
                    || (origin.pending
                        && (origin.source_sequence != frame.commit_seq
                            || origin.source_frame_checksum != frame.checksum))
                {
                    return Err(invalid(
                        "existing target belongs to another accepted allocation",
                    ));
                }
            }
        }
        if self.native_origin.is_none()
            && (!self.collections.is_empty()
                || self.current_commit_seq() != 0
                || (!self.fresh_directory_created
                    && !native_origin::has_prepared_factory(&self.path)?))
        {
            return Err(invalid("actual fresh target factory is required"));
        }
        if !self.collections.is_empty()
            && (!self.local_namespace_identity_active
                || !self.native_collection_birth_active
                || !self.native_wal_backing_capture_active)
        {
            return Err(invalid(
                "existing target lacks its Native allocation fences",
            ));
        }
        Ok(Some(AcceptedNativeImport {
            root: root.clone(),
            facts: facts.clone(),
            sequence: frame.commit_seq,
            checksum: frame.checksum,
        }))
    }

    pub(super) fn prepare_native_import_allocations(
        &mut self,
        accepted: Option<&AcceptedNativeImport>,
    ) -> Result<BTreeMap<String, CollectionMeta>> {
        let Some(accepted) = accepted else {
            return Ok(BTreeMap::new());
        };
        self.check_catalog_recovery_quiescent()?;
        let setup = (|| -> Result<()> {
            self.install_actual_native_origin_root_v1()?;
            format::enable_local_namespace_identity(&self.path)?;
            self.local_namespace_identity_active = true;
            format::enable_native_collection_birth(&self.path)?;
            self.native_collection_birth_active = true;
            format::enable_native_wal_backing_capture(&self.path)?;
            self.native_wal_backing_capture_active = true;
            format::enable_native_catalog_allocation_facts(&self.path)?;
            self.native_catalog_allocation_facts_active = true;
            format::enable_native_imported_backing_origins(&self.path)?;
            self.native_imported_backing_origins_active = true;
            Ok(())
        })();
        if let Err(error) = setup {
            self.fence_durability("native_import_origin.format_fence", &error);
            return Err(error);
        }
        let mut decisions = BTreeMap::new();
        for (name, fact) in &accepted.facts {
            if !self.collections.contains_key(name) {
                let mut meta = self.new_local_collection_metadata(fact);
                meta.storage_name = self.storage_name_for_new(name);
                let origin = NativeImportedBackingOrigin {
                    version: 1,
                    source_root: accepted.root.root_id(),
                    source_root_sha256: accepted.root.record_sha256().into(),
                    source_birth: fact
                        .native_backing_birth_id
                        .ok_or_else(|| invalid("missing source birth"))?,
                    source_namespace: fact
                        .local_namespace_id
                        .ok_or_else(|| invalid("missing source namespace"))?,
                    source_storage: fact.storage().into(),
                    source_contract_sha256: contract_hash(fact)?,
                    source_sequence: accepted.sequence,
                    source_frame_checksum: accepted.checksum,
                    target_root: meta
                        .native_backing_origin_root
                        .ok_or_else(|| invalid("missing target root"))?,
                    target_birth: meta
                        .native_backing_birth_id
                        .ok_or_else(|| invalid("missing target birth"))?,
                    target_namespace: meta
                        .local_namespace_id
                        .ok_or_else(|| invalid("missing target namespace"))?,
                    target_storage: meta.storage().into(),
                    generation: Uuid::new_v4(),
                    pending: true,
                };
                meta.native_imported_backing_origin = Some(origin);
                if let Some(storage) = &meta.storage_name {
                    self.set_storage_alias(name, storage);
                }
                let mut state = CollectionState::new(meta);
                state.paged_lazy = self.paged_records.is_some();
                self.collections.insert(name.clone(), RwLock::new(state));
                self.persist_catalog()?;
                self.schema_compatibility.invalidate();
            }
            let state = self.collection_state(name)?;
            if pending(&state.meta) {
                let mut meta = state.meta.clone();
                meta.native_imported_backing_origin
                    .as_mut()
                    .unwrap()
                    .pending = false;
                decisions.insert(name.clone(), meta);
            }
        }
        validated_decisions(decisions)
    }

    pub(super) fn validate_native_import_decisions(
        &self,
        decisions: &BTreeMap<String, CollectionMeta>,
    ) -> Result<()> {
        if decisions.is_empty() {
            return Ok(());
        }
        if !self.config.fsync || !self.native_imported_backing_origins_active {
            return Err(invalid("allocation decision is unfenced"));
        }
        validated_decisions(decisions.clone())?;
        for (name, decision) in decisions {
            let state = self.collection_state(name)?;
            let mut actual = state.meta.clone();
            if !pending(&actual) {
                return Err(invalid("allocation decision has no actual pending target"));
            }
            actual
                .native_imported_backing_origin
                .as_mut()
                .unwrap()
                .pending = false;
            if actual != *decision {
                return Err(invalid("allocation decision replaced its actual target"));
            }
        }
        Ok(())
    }

    pub(super) fn finish_native_import_allocations(
        &mut self,
        decisions: &BTreeMap<String, CollectionMeta>,
    ) -> Result<()> {
        if decisions.is_empty() {
            return Ok(());
        }
        self.validate_native_import_decisions(decisions)?;
        for name in decisions.keys() {
            self.collection_state_mut(name)?
                .meta
                .native_imported_backing_origin
                .as_mut()
                .unwrap()
                .pending = false;
        }
        self.persist_catalog()
    }
}

#[cfg(all(test, unix))]
mod tests;
