//! Atomic logical-definition / physical-generation publication.
use super::*;

pub(super) fn load_btree_bindings(
    path: &Path,
    encryption: &EncryptionRuntime,
) -> Result<BTreeMap<String, String>> {
    let Some(bytes) = read_optional_sidecar(path, FrameKind::Index, encryption)? else {
        return Ok(BTreeMap::new());
    };
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let catalog: IndexCatalog = serde_json::from_slice(&bytes)?;
    let mut physical_names = FxHashSet::default();
    for (name, physical) in &catalog.btree_bindings {
        let definition = catalog
            .indexes
            .iter()
            .find(|definition| &definition.name == name);
        let valid_physical = physical
            .rsplit_once(".__btree.")
            .is_some_and(|(prefix, id)| {
                !prefix.is_empty() && Uuid::parse_str(id).is_ok_and(|id| !id.is_nil())
            });
        if !definition.is_some_and(|definition| definition.kind == IndexKind::BTree)
            || !valid_physical
            || !physical_names.insert(physical.clone())
        {
            return Err(BicDbError::Index(
                "invalid authoritative B-tree generation binding".into(),
            ));
        }
    }
    Ok(catalog.btree_bindings)
}

impl BicDb {
    /// Refuse older readers before publishing Oracle-v1 profile metadata, even
    /// when the database has no expression indexes yet.
    pub fn enable_oracle_simple_case_v1(&mut self) -> Result<()> {
        self.ensure_writable("enable Oracle simple-case v1")?;
        self.ensure_no_pending_transactions()?;
        format::enable_oracle_simple_case_v1(&self.path)?;
        self.check_recovery_state()
    }

    /// Replace an existing legacy case-expression index with an independently
    /// verified, owned Oracle-v1 shadow. The definition and physical binding
    /// share one durable catalog publication. Retired physical entries remain
    /// intact for readers; this operation does not reclaim generations.
    pub fn publish_oracle_case_v1_index_replacement(
        &mut self,
        expected_old: &IndexDefinition,
        shadow: &PagedBTreeBuildState,
    ) -> Result<()> {
        self.ensure_writable("publish Oracle simple-case v1 index")?;
        self.ensure_no_pending_transactions()?;
        shadow.validate()?;
        let durable = self.load_paged_btree_build(&shadow.definition.name, shadow.limits)?;
        if durable != *shadow || shadow.phase != PagedBTreeBuildPhase::Complete {
            return Err(BicDbError::Index(
                "Oracle replacement shadow is not the owned completed build".into(),
            ));
        }
        let replacement = oracle_case_v1_definition(expected_old.clone());
        if replacement == *expected_old || replacement.kind != IndexKind::BTree {
            return Err(BicDbError::Index(
                "Oracle replacement requires a legacy case-expression B-tree".into(),
            ));
        }
        let mut renamed_shadow = shadow.definition.clone();
        renamed_shadow.name.clone_from(&expected_old.name);
        if replacement != renamed_shadow || shadow.definition.name == expected_old.name {
            return Err(BicDbError::Index(
                "Oracle replacement changes more than case semantics".into(),
            ));
        }
        let current = self
            .indexes
            .get(&expected_old.name)
            .map(|state| state.read().definition.clone());
        // Idempotent retry after a crash following the catalog rename.
        if current.as_ref() == Some(&replacement)
            && self.btree_bindings.lock().get(&expected_old.name) == Some(&shadow.physical_name)
            && !self.indexes.contains_key(&shadow.definition.name)
        {
            return self.check_recovery_state();
        }
        if current.as_ref() != Some(expected_old)
            || !self
                .indexes
                .get(&shadow.definition.name)
                .is_some_and(|state| {
                    let state = state.read();
                    state.definition == shadow.definition
                        && state.paged_read_through
                        && !state.full_text_build_incomplete
                })
            || self
                .fts_generations
                .lock()
                .indexes
                .get(&shadow.definition.name)
                != Some(&shadow.physical_name)
            || self.paged_records.is_none()
        {
            return Err(BicDbError::Index(
                "Oracle replacement source, shadow, or database changed".into(),
            ));
        }
        self.enable_index_format_capabilities(&replacement)?;
        let old_bindings = self.btree_bindings.lock().clone();
        let old = self
            .indexes
            .remove(&expected_old.name)
            .expect("validated source");
        let mut new = self
            .indexes
            .remove(&shadow.definition.name)
            .expect("validated shadow");
        new.get_mut().definition = replacement;
        self.indexes.insert(expected_old.name.clone(), new);
        {
            let mut bindings = self.btree_bindings.lock();
            bindings.remove(&shadow.definition.name);
            bindings.insert(expected_old.name.clone(), shadow.physical_name.clone());
        }
        if let Err(error) = self.persist_index_catalog() {
            let mut new = self
                .indexes
                .remove(&expected_old.name)
                .expect("replacement");
            new.get_mut().definition = shadow.definition.clone();
            self.indexes.insert(shadow.definition.name.clone(), new);
            self.indexes.insert(expected_old.name.clone(), old);
            *self.btree_bindings.lock() = old_bindings;
            return Err(error);
        }
        // Only the catalog is authoritative. No fallible derived-file write
        // may intervene before the in-process alias follows its publication.
        let mut generations = self.fts_generations.lock();
        generations.indexes.remove(&shadow.definition.name);
        generations
            .indexes
            .insert(expected_old.name.clone(), shadow.physical_name.clone());
        let paged = self.paged_records.as_ref().expect("validated paged store");
        paged.remove_index_alias(&shadow.definition.name);
        paged.set_index_alias(&expected_old.name, &shadow.physical_name);
        self.index_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.schema_compatibility.invalidate();
        self.check_recovery_state()
    }

    /// Embedded counterpart: the current verified shadow store is moved as a
    /// whole, and reopen reconstructs its keys from the new atomic definition.
    pub fn publish_embedded_oracle_case_v1_index_replacement(
        &mut self,
        expected_old: &IndexDefinition,
        shadow: &IndexDefinition,
    ) -> Result<()> {
        self.ensure_writable("publish embedded Oracle simple-case v1 index")?;
        self.ensure_no_pending_transactions()?;
        if self.paged_records.is_some() {
            return Err(BicDbError::Index(
                "embedded index publication requires embedded storage".into(),
            ));
        }
        let replacement = oracle_case_v1_definition(expected_old.clone());
        let mut renamed_shadow = shadow.clone();
        renamed_shadow.name.clone_from(&expected_old.name);
        if replacement == *expected_old
            || replacement.kind != IndexKind::BTree
            || replacement != renamed_shadow
            || shadow.name == expected_old.name
        {
            return Err(BicDbError::Index(
                "Oracle replacement changes more than case semantics".into(),
            ));
        }
        let current = self
            .indexes
            .get(&expected_old.name)
            .map(|state| state.read().definition.clone());
        if current.as_ref() == Some(&replacement) && !self.indexes.contains_key(&shadow.name) {
            return self.check_recovery_state();
        }
        if current.as_ref() != Some(expected_old)
            || self
                .indexes
                .get(&shadow.name)
                .map(|state| state.read().definition.clone())
                != Some(shadow.clone())
            || !self.verify_index(&shadow.name)?.valid
        {
            return Err(BicDbError::Index(
                "Oracle replacement source or owned shadow changed".into(),
            ));
        }
        self.enable_index_format_capabilities(&replacement)?;
        let old = self
            .indexes
            .remove(&expected_old.name)
            .expect("validated source");
        let mut new = self.indexes.remove(&shadow.name).expect("validated shadow");
        new.get_mut().definition = replacement;
        self.indexes.insert(expected_old.name.clone(), new);
        if let Err(error) = self.persist_index_catalog() {
            let mut new = self
                .indexes
                .remove(&expected_old.name)
                .expect("replacement");
            new.get_mut().definition = shadow.clone();
            self.indexes.insert(shadow.name.clone(), new);
            self.indexes.insert(expected_old.name.clone(), old);
            return Err(error);
        }
        self.index_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.schema_compatibility.invalidate();
        self.check_recovery_state()
    }
}

fn oracle_case_v1_definition(mut definition: IndexDefinition) -> IndexDefinition {
    fn field(value: &mut IndexField) {
        match value {
            IndexField::Upper(inner) => {
                field(inner);
                *value = IndexField::OracleUpperV1(inner.clone());
            }
            IndexField::Lower(inner) => {
                field(inner);
                *value = IndexField::OracleLowerV1(inner.clone());
            }
            IndexField::OracleUpperV1(inner)
            | IndexField::OracleLowerV1(inner)
            | IndexField::Trim(inner)
            | IndexField::TimestampDay(inner)
            | IndexField::Coalesce { field: inner, .. } => field(inner),
            IndexField::FirstNonNull(fields) => fields.iter_mut().for_each(field),
            _ => {}
        }
    }
    fn predicate(value: &mut IndexPredicate) {
        match value {
            IndexPredicate::Field { field: value } => field(value),
            IndexPredicate::Literal { .. } => {}
            IndexPredicate::Unary { value, .. } => predicate(value),
            IndexPredicate::Binary { left, right, .. } => {
                predicate(left);
                predicate(right);
            }
        }
    }
    definition.fields.iter_mut().for_each(field);
    if let Some(value) = &mut definition.predicate {
        predicate(value);
    }
    if let Some(exclusion) = &mut definition.exclusion {
        for element in &mut exclusion.elements {
            match element {
                IndexExclusionElement::Equal { field: value } => field(value),
                IndexExclusionElement::Overlaps { start, end, .. } => {
                    field(start);
                    field(end);
                }
            }
        }
    }
    definition
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(db: &mut BicDb, definition: IndexDefinition) -> PagedBTreeBuildState {
        let limits = PagedBTreeBuildLimits::default();
        let name = definition.name.clone();
        db.begin_paged_btree_build(definition, limits, 100).unwrap();
        let governor =
            ResourceGovernor::new(crate::ResourceGovernorConfig::default(), 100).unwrap();
        let demand = ResourceDemand {
            memory_bytes: limits.max_batch_bytes as u64,
            io_bytes: limits.max_batch_bytes as u64,
            cpu_slots: 1,
            io_charge_bytes: 1,
        };
        for now in 101..1000 {
            if let PagedBTreeBuildAdvance::Complete(state) = db
                .advance_paged_btree_build_governed(&name, limits, &governor, demand, now)
                .unwrap()
            {
                return state;
            }
        }
        panic!("bounded build did not complete");
    }

    #[test]
    fn replacement_retains_old_reader_generation_and_refuses_bound_generation_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("rows").unwrap();
        db.insert(
            "rows",
            Record::new("one").with_metadata(serde_json::json!({"value":"é"})),
        )
        .unwrap();
        let old = IndexDefinition {
            name: "legacy".into(),
            collection: "rows".into(),
            fields: vec![IndexField::Upper(Box::new(IndexField::MetadataPath(vec![
                "value".into(),
            ])))],
            unique: true,
            nulls_not_distinct: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        };
        let original = build(&mut db, old.clone());
        let mut transformed = oracle_case_v1_definition(old.clone());
        transformed.name = "owned-shadow".into();
        let shadow = build(&mut db, transformed);
        let paged = Arc::clone(db.paged_records.as_ref().unwrap());
        let old_reader = paged.latest_snapshot();
        let key = encode_index_key(&[IndexValue::String("é".into())]);
        let old_entries = || {
            paged
                .scan_index_exact(&old_reader, &original.physical_name, &key)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
        };
        let before = old_entries();
        assert_eq!(before.len(), 1);
        db.publish_oracle_case_v1_index_replacement(&old, &shadow)
            .unwrap();
        assert_eq!(old_entries(), before);
        assert!(db
            .remove_physical_index_entries(&paged, &shadow.physical_name)
            .is_err());
        assert_eq!(
            db.lookup_index("legacy", &[IndexValue::String("É".into())])
                .unwrap(),
            vec!["one"]
        );
        db.flush().unwrap();
        drop(old_reader);
        drop(paged);
        drop(db);
        let db = BicDb::open_with_config(root.path(), config).unwrap();
        let paged = db.paged_records.as_ref().unwrap();
        let entries = paged
            .scan_index_exact(&paged.latest_snapshot(), &original.physical_name, &key)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(entries, before);
    }
}
