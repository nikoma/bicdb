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
    fn validate_oracle_replacement_names(&self, old: &str, shadow: &str) -> Result<()> {
        if old.eq_ignore_ascii_case(shadow)
            || self
                .indexes
                .keys()
                .filter(|name| name.eq_ignore_ascii_case(old))
                .count()
                != 1
            || self
                .indexes
                .keys()
                .filter(|name| name.eq_ignore_ascii_case(shadow))
                .count()
                > 1
        {
            return Err(BicDbError::Index(
                "ambiguous Oracle replacement source or shadow name".into(),
            ));
        }
        Ok(())
    }

    /// Refuse older readers before publishing Oracle-v1 profile metadata, even
    /// when the database has no expression indexes yet.
    pub fn enable_oracle_simple_case_v1(&mut self) -> Result<()> {
        self.ensure_writable("enable Oracle simple-case v1")?;
        self.ensure_no_pending_transactions()?;
        self.install_replicated_reader_features(&[
            format::ORACLE_SIMPLE_CASE_V1_FEATURE_FLAG.into()
        ])?;
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
        self.validate_oracle_replacement_names(&expected_old.name, &shadow.definition.name)?;
        shadow.validate()?;
        let durable = self.load_paged_btree_build(&shadow.definition.name, shadow.limits)?;
        if durable != *shadow || shadow.phase != PagedBTreeBuildPhase::Complete {
            return Err(BicDbError::Index(
                "Oracle replacement shadow is not the owned completed build".into(),
            ));
        }
        let mut replacement = shadow.definition.clone();
        replacement.name.clone_from(&expected_old.name);
        if !oracle_case_v1_replacement_matches(expected_old, &replacement) {
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
        self.validate_oracle_replacement_names(&expected_old.name, &shadow.name)?;
        if self.paged_records.is_some() {
            return Err(BicDbError::Index(
                "embedded index publication requires embedded storage".into(),
            ));
        }
        let mut replacement = shadow.clone();
        replacement.name.clone_from(&expected_old.name);
        if !oracle_case_v1_replacement_matches(expected_old, &replacement) {
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

/// Normalize only corresponding typed case-expression nodes. Exact definition
/// equality then protects every unrelated field, literal, predicate operator,
/// uniqueness policy and collection identity. Existing v1 nodes stay intact.
fn oracle_case_v1_replacement_matches(old: &IndexDefinition, new: &IndexDefinition) -> bool {
    fn field(old: &IndexField, new: &mut IndexField) {
        match (old, &mut *new) {
            (
                IndexField::Upper(_),
                IndexField::OracleUpperV1(inner) | IndexField::OracleUpperNationalV1(inner),
            ) => {
                *new = IndexField::Upper(inner.clone());
            }
            (
                IndexField::Lower(_),
                IndexField::OracleLowerV1(inner) | IndexField::OracleLowerNationalV1(inner),
            ) => {
                *new = IndexField::Lower(inner.clone());
            }
            _ => {}
        }
        match (old, new) {
            (IndexField::Upper(a), IndexField::Upper(b))
            | (IndexField::Lower(a), IndexField::Lower(b))
            | (IndexField::OracleUpperV1(a), IndexField::OracleUpperV1(b))
            | (IndexField::OracleLowerV1(a), IndexField::OracleLowerV1(b))
            | (IndexField::OracleUpperNationalV1(a), IndexField::OracleUpperNationalV1(b))
            | (IndexField::OracleLowerNationalV1(a), IndexField::OracleLowerNationalV1(b))
            | (IndexField::Trim(a), IndexField::Trim(b))
            | (IndexField::TimestampDay(a), IndexField::TimestampDay(b))
            | (IndexField::Coalesce { field: a, .. }, IndexField::Coalesce { field: b, .. }) => {
                field(a, b)
            }
            (IndexField::FirstNonNull(a), IndexField::FirstNonNull(b)) => {
                for (a, b) in a.iter().zip(b.iter_mut()) {
                    field(a, b);
                }
            }
            _ => {}
        }
    }
    fn predicate(old: &IndexPredicate, new: &mut IndexPredicate) {
        match (old, new) {
            (IndexPredicate::Field { field: a }, IndexPredicate::Field { field: b }) => field(a, b),
            (IndexPredicate::Unary { value: a, .. }, IndexPredicate::Unary { value: b, .. }) => {
                predicate(a, b)
            }
            (
                IndexPredicate::Binary {
                    left: a, right: b, ..
                },
                IndexPredicate::Binary {
                    left: c, right: d, ..
                },
            ) => {
                predicate(a, c);
                predicate(b, d);
            }
            _ => {}
        }
    }
    if new.kind != IndexKind::BTree || new == old || !new.uses_oracle_simple_case_v1() {
        return false;
    }
    let mut normalized = new.clone();
    for (a, b) in old.fields.iter().zip(normalized.fields.iter_mut()) {
        field(a, b);
    }
    if let (Some(a), Some(b)) = (&old.predicate, &mut normalized.predicate) {
        predicate(a, b);
    }
    if let (Some(a), Some(b)) = (&old.exclusion, &mut normalized.exclusion) {
        for (a, b) in a.elements.iter().zip(b.elements.iter_mut()) {
            match (a, b) {
                (
                    IndexExclusionElement::Equal { field: a },
                    IndexExclusionElement::Equal { field: b },
                ) => field(a, b),
                (
                    IndexExclusionElement::Overlaps {
                        start: a, end: b, ..
                    },
                    IndexExclusionElement::Overlaps {
                        start: c, end: d, ..
                    },
                ) => {
                    field(a, c);
                    field(b, d);
                }
                _ => {}
            }
        }
    }
    normalized == *old
}

#[cfg(test)]
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
            | IndexField::OracleUpperNationalV1(inner)
            | IndexField::OracleLowerNationalV1(inner)
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
    fn replacement_validation_preserves_case_family_literals_and_unrelated_policy() {
        let old = IndexDefinition {
            name: "legacy".into(),
            collection: "rows".into(),
            fields: vec![IndexField::Upper(Box::new(IndexField::MetadataPath(vec![
                "value".into(),
            ])))],
            unique: true,
            nulls_not_distinct: false,
            kind: IndexKind::BTree,
            predicate: Some(IndexPredicate::Binary {
                operator: IndexPredicateBinaryOperator::Equal,
                left: Box::new(IndexPredicate::Field {
                    field: IndexField::Lower(Box::new(IndexField::MetadataPath(vec![
                        "other".into()
                    ]))),
                }),
                right: Box::new(IndexPredicate::Literal {
                    value: serde_json::json!("literal"),
                }),
                value_type: IndexPredicateValueType::String,
            }),
            exclusion: None,
        };
        let replacement = oracle_case_v1_definition(old.clone());
        assert!(oracle_case_v1_replacement_matches(&old, &replacement));
        let mut changed = replacement.clone();
        changed.unique = false;
        assert!(!oracle_case_v1_replacement_matches(&old, &changed));
        let mut changed = replacement.clone();
        changed.fields[0] =
            IndexField::OracleLowerV1(Box::new(IndexField::MetadataPath(vec!["value".into()])));
        assert!(!oracle_case_v1_replacement_matches(&old, &changed));
        let mut changed = replacement.clone();
        if let Some(IndexPredicate::Binary { right, .. }) = &mut changed.predicate {
            *right = Box::new(IndexPredicate::Literal {
                value: serde_json::json!("changed"),
            });
        }
        assert!(!oracle_case_v1_replacement_matches(&old, &changed));
        assert!(!oracle_case_v1_replacement_matches(&old, &old));
        let mut national = replacement.clone();
        national.fields[0] =
            IndexField::OracleUpperNationalV1(Box::new(IndexField::MetadataPath(vec![
                "value".into()
            ])));
        assert!(oracle_case_v1_replacement_matches(&old, &national));
        let mut opposite = national.clone();
        opposite.fields[0] =
            IndexField::OracleLowerNationalV1(Box::new(IndexField::MetadataPath(vec![
                "value".into()
            ])));
        assert!(!oracle_case_v1_replacement_matches(&old, &opposite));
        // A preexisting v1 expression cannot silently change its family.
        assert!(!oracle_case_v1_replacement_matches(&replacement, &national));
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
