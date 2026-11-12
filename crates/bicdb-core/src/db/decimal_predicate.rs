//! Persisted Decimal predicate comparison semantics belong to the generation,
//! not merely the unchanged predicate JSON. Admission precedes page recovery
//! and every public handle, including application-specific recovery openers.
use super::*;

pub(super) const COMPARISON_VERSION: u32 = 1;

pub(super) fn affected(definition: &IndexDefinition) -> bool {
    fn decimal_comparison(predicate: &IndexPredicate) -> bool {
        match predicate {
            IndexPredicate::Binary {
                value_type,
                left,
                right,
                ..
            } => {
                *value_type == IndexPredicateValueType::Decimal
                    || decimal_comparison(left)
                    || decimal_comparison(right)
            }
            IndexPredicate::Unary { value, .. } => decimal_comparison(value),
            IndexPredicate::Field { .. } | IndexPredicate::Literal { .. } => false,
        }
    }
    definition
        .predicate
        .as_ref()
        .is_some_and(decimal_comparison)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct UpgradeIndex {
    original: IndexDefinition,
    shadow: IndexDefinition,
    #[serde(default)]
    published_generation: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct UpgradePlan {
    version: u32,
    run_id: Uuid,
    source_commit_seq: u64,
    original_catalog: Vec<IndexDefinition>,
    original_bindings: BTreeMap<String, String>,
    replacements: Vec<UpgradeIndex>,
}

pub(super) struct Admission {
    pub current: bool,
    pub plan: Option<UpgradePlan>,
}

pub(super) fn check_open_admission(
    root: &Path,
    encryption: &EncryptionRuntime,
    mode: &StorageMode,
    config: &DbConfig,
) -> Result<Admission> {
    // Primary-record recovery/export does not consult secondary predicates.
    // Its handle is mutation-fenced by ensure_writable and must remain usable
    // even when unrelated index metadata cannot be decoded. Normal indexed
    // admission still validates and repairs the complete persisted catalog.
    if !config.load_secondary_indexes {
        return Ok(Admission {
            current: false,
            plan: None,
        });
    }
    let Some(bytes) = read_optional_sidecar(
        &root.join(DEFAULT_INDEX_CATALOG),
        FrameKind::Index,
        encryption,
    )?
    else {
        return Ok(Admission {
            current: true,
            plan: None,
        });
    };
    if bytes.is_empty() {
        return Ok(Admission {
            current: true,
            plan: None,
        });
    }
    let catalog: IndexCatalog = serde_json::from_slice(&bytes)?;
    match catalog.decimal_predicate_semantics_version {
        None => {
            if catalog.indexes.iter().any(affected) || catalog.decimal_predicate_upgrade.is_some() {
                if !config.load_secondary_indexes || !config.fsync {
                    return Err(BicDbError::FormatCompatibility(
                        "legacy Decimal predicate transition requires secondary indexes and fsync enabled".into(),
                    ));
                }
                // The existing Native contract only permits predicates on
                // B-trees, and excludes predicates from exclusion indexes.
                // Check that contract before recovery as well: invalid catalog
                // definitions must never become new supported index kinds.
                for definition in catalog.indexes.iter().filter(|d| affected(d)) {
                    validate_index_definition(definition)?;
                }
                if catalog.decimal_predicate_upgrade.is_some()
                    && !format::read_or_legacy_metadata(root)?
                        .feature_flags
                        .contains(format::DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG)
                {
                    return Err(BicDbError::FormatCompatibility(
                        "unfenced Decimal predicate upgrade plan".into(),
                    ));
                }
                return Ok(Admission {
                    current: false,
                    plan: catalog.decimal_predicate_upgrade,
                });
            }
        }
        Some(COMPARISON_VERSION) => {
            if catalog.decimal_predicate_upgrade.is_some() {
                return Err(BicDbError::FormatCompatibility(
                    "completed Decimal certificate retains an upgrade plan".into(),
                ));
            }
            if !format::read_or_legacy_metadata(root)?
                .feature_flags
                .contains(format::DECIMAL_PREDICATE_COMPARISON_V1_FEATURE_FLAG)
            {
                return Err(BicDbError::FormatCompatibility(
                    "unfenced Decimal predicate semantic certificate".into(),
                ));
            }
            if *mode == StorageMode::ServerPaged {
                for definition in catalog.indexes.iter().filter(|definition| {
                    affected(definition) && definition.kind == IndexKind::BTree
                }) {
                    if !catalog.btree_bindings.contains_key(&definition.name) {
                        return Err(BicDbError::FormatCompatibility(
                            "Decimal predicate semantic certificate lacks an authoritative physical generation binding".into(),
                        ));
                    }
                }
            }
        }
        Some(_) => {
            return Err(BicDbError::FormatCompatibility(
                "unsupported Decimal predicate comparison semantic version".into(),
            ));
        }
    }
    Ok(Admission {
        current: true,
        plan: None,
    })
}

fn invalid(detail: &str) -> BicDbError {
    BicDbError::FormatCompatibility(format!("Decimal predicate transition: {detail}"))
}

impl BicDb {
    // This is private startup maintenance. No public handle exists, no SQL
    // owner is bypassed, and shadow definitions never enter the replicated
    // logical catalog. The existing transaction fence remains in force.
    pub(super) fn finish_decimal_predicate_upgrade(&mut self) -> Result<()> {
        if !self.config.load_secondary_indexes {
            return Ok(());
        }
        if self.decimal_predicates_current {
            return Ok(());
        }
        self.ensure_writable("upgrade Decimal predicate generations")?;
        self.ensure_no_pending_transactions()?;
        format::enable_decimal_predicate_comparison_v1(&self.path)?;
        self.local_index_maintenance = true;
        let result = self.finish_decimal_predicate_upgrade_inner();
        self.local_index_maintenance = false;
        result
    }

    fn finish_decimal_predicate_upgrade_inner(&mut self) -> Result<()> {
        if self.decimal_predicate_upgrade.is_none() {
            let mut original_catalog = self.index_definitions();
            original_catalog.sort_by(|a, b| a.name.cmp(&b.name));
            let mut replacements = Vec::new();
            for original in original_catalog.iter().filter(|d| affected(d)) {
                let mut shadow = original.clone();
                shadow.name = format!("__decimal_predicate_{}", Uuid::new_v4());
                if self.indexes.contains_key(&shadow.name)
                    || self.paged_btree_build_path(&shadow.name).exists()
                {
                    return Err(invalid("fresh owned shadow identity already exists"));
                }
                replacements.push(UpgradeIndex {
                    original: original.clone(),
                    shadow,
                    published_generation: None,
                });
            }
            self.decimal_predicate_upgrade = Some(UpgradePlan {
                version: COMPARISON_VERSION,
                run_id: Uuid::new_v4(),
                source_commit_seq: self.last_commit_seq(),
                original_catalog,
                original_bindings: self.btree_bindings.lock().clone(),
                replacements,
            });
            // Before the first shadow writes any data, its ownership, exact
            // definitions and source horizon are in the atomic native catalog.
            self.persist_index_catalog()?;
            test_process_cut("after_plan");
        }
        self.validate_decimal_upgrade_plan()?;
        let count = self
            .decimal_predicate_upgrade
            .as_ref()
            .unwrap()
            .replacements
            .len();
        for position in 0..count {
            self.validate_decimal_upgrade_plan()?;
            let entry = self
                .decimal_predicate_upgrade
                .as_ref()
                .unwrap()
                .replacements[position]
                .clone();
            if entry.published_generation.is_some() {
                continue;
            }
            if self.paged_records.is_none() {
                // Embedded indexes have no durable key generation: open has
                // already rebuilt them from authoritative records. Verify every
                // affected definition before certifying the reconstructed set.
                if !self.verify_index(&entry.original.name)?.valid {
                    return Err(invalid("embedded reconstructed index did not verify"));
                }
                continue;
            }
            let limits = self.paged_btree_build_limits();
            if self.indexes.contains_key(&entry.shadow.name) {
                self.reconcile_published_paged_btree_build(&entry.shadow)?;
            } else {
                self.create_index(entry.shadow.clone())?;
            }
            let completed = self.load_paged_btree_build(&entry.shadow.name, limits)?;
            test_process_cut("after_build");
            self.publish_decimal_upgrade_replacement(position, &completed)?;
            test_process_cut("after_replacement");
        }
        self.validate_decimal_upgrade_plan()?;
        for entry in &self
            .decimal_predicate_upgrade
            .as_ref()
            .unwrap()
            .replacements
        {
            if !self.verify_index(&entry.original.name)?.valid {
                return Err(invalid("replacement generation did not verify"));
            }
            if self.paged_records.is_some() && entry.published_generation.is_none() {
                return Err(invalid("not every durable replacement was published"));
            }
        }
        // Flush all recovered source and completed page generations before
        // publishing the semantic certificate. A failed checkpoint keeps the
        // durable plan and admission fence for the next recovery attempt.
        self.checkpoint_for_resume()?;
        test_process_cut("before_certificate");
        let old_plan = self.decimal_predicate_upgrade.take();
        self.decimal_predicates_current = true;
        if let Err(error) = self.persist_index_catalog() {
            self.decimal_predicates_current = false;
            self.decimal_predicate_upgrade = old_plan;
            return Err(error);
        }
        test_process_cut("after_certificate");
        Ok(())
    }

    fn validate_decimal_upgrade_plan(&self) -> Result<()> {
        let plan = self
            .decimal_predicate_upgrade
            .as_ref()
            .ok_or_else(|| invalid("missing owned upgrade plan"))?;
        if plan.version != COMPARISON_VERSION
            || plan.run_id.is_nil()
            || plan.source_commit_seq != self.last_commit_seq()
        {
            return Err(invalid("upgrade version, owner or source horizon changed"));
        }
        let originals: BTreeMap<_, _> = plan
            .original_catalog
            .iter()
            .map(|d| (d.name.clone(), d.clone()))
            .collect();
        if originals.len() != plan.original_catalog.len() {
            return Err(invalid("duplicate original catalog identities"));
        }
        let expected: BTreeSet<_> = plan
            .original_catalog
            .iter()
            .filter(|d| affected(d))
            .map(|d| d.name.clone())
            .collect();
        let actual: BTreeSet<_> = plan
            .replacements
            .iter()
            .map(|e| e.original.name.clone())
            .collect();
        if expected != actual || actual.len() != plan.replacements.len() {
            return Err(invalid(
                "upgrade plan does not cover the affected catalog exactly",
            ));
        }
        let mut catalog = originals.clone();
        let mut bindings = plan.original_bindings.clone();
        let mut shadows = BTreeSet::new();
        for entry in &plan.replacements {
            let mut normalized = entry.shadow.clone();
            normalized.name.clone_from(&entry.original.name);
            if originals.get(&entry.original.name) != Some(&entry.original)
                || normalized != entry.original
                || originals.contains_key(&entry.shadow.name)
                || !shadows.insert(entry.shadow.name.clone())
            {
                return Err(invalid(
                    "owned shadow changes a definition or aliases another identity",
                ));
            }
            if let Some(physical) = &entry.published_generation {
                let completed = self
                    .load_paged_btree_build(&entry.shadow.name, self.paged_btree_build_limits())?;
                if completed.definition != entry.shadow
                    || completed.phase != PagedBTreeBuildPhase::Complete
                    || &completed.physical_name != physical
                    || completed.starting_commit_seq != plan.source_commit_seq
                {
                    return Err(invalid(
                        "published replacement lacks its exact owned completed build",
                    ));
                }
                bindings.insert(entry.original.name.clone(), physical.clone());
            } else if self.indexes.contains_key(&entry.shadow.name) {
                let state = self
                    .load_paged_btree_build(&entry.shadow.name, self.paged_btree_build_limits())?;
                if state.definition != entry.shadow
                    || !matches!(
                        state.phase,
                        PagedBTreeBuildPhase::Publishing | PagedBTreeBuildPhase::Complete
                    )
                    || state.starting_commit_seq != plan.source_commit_seq
                    || self.btree_bindings.lock().get(&entry.shadow.name)
                        != Some(&state.physical_name)
                {
                    return Err(invalid(
                        "visible shadow lacks its exact owned native publication",
                    ));
                }
                catalog.insert(entry.shadow.name.clone(), entry.shadow.clone());
                bindings.insert(entry.shadow.name.clone(), state.physical_name);
            }
        }
        let current: BTreeMap<_, _> = self
            .index_definitions()
            .into_iter()
            .map(|d| (d.name.clone(), d))
            .collect();
        if current != catalog || *self.btree_bindings.lock() != bindings {
            return Err(invalid(
                "catalog or authoritative generation bindings diverged from the owned plan",
            ));
        }
        Ok(())
    }

    fn publish_decimal_upgrade_replacement(
        &mut self,
        position: usize,
        completed: &PagedBTreeBuildState,
    ) -> Result<()> {
        self.ensure_no_pending_transactions()?;
        self.validate_decimal_upgrade_plan()?;
        completed.validate()?;
        let plan = self.decimal_predicate_upgrade.as_ref().unwrap();
        let entry = plan.replacements[position].clone();
        let durable = self.load_paged_btree_build(&entry.shadow.name, completed.limits)?;
        if durable != *completed
            || completed.phase != PagedBTreeBuildPhase::Complete
            || completed.definition != entry.shadow
            || completed.starting_commit_seq != plan.source_commit_seq
            || self.fts_generations.lock().indexes.get(&entry.shadow.name)
                != Some(&completed.physical_name)
            || !self.indexes.get(&entry.shadow.name).is_some_and(|state| {
                let state = state.read();
                state.definition == entry.shadow
                    && state.paged_read_through
                    && !state.full_text_build_incomplete
            })
        {
            return Err(invalid(
                "replacement is not the exact completed owned shadow",
            ));
        }
        let old_bindings = self.btree_bindings.lock().clone();
        let old = self
            .indexes
            .remove(&entry.original.name)
            .ok_or_else(|| invalid("missing original definition"))?;
        let mut new = self
            .indexes
            .remove(&entry.shadow.name)
            .ok_or_else(|| invalid("missing owned shadow"))?;
        new.get_mut().definition = entry.original.clone();
        self.indexes.insert(entry.original.name.clone(), new);
        {
            let mut bindings = self.btree_bindings.lock();
            bindings.remove(&entry.shadow.name);
            bindings.insert(entry.original.name.clone(), completed.physical_name.clone());
        }
        self.decimal_predicate_upgrade
            .as_mut()
            .unwrap()
            .replacements[position]
            .published_generation = Some(completed.physical_name.clone());
        if let Err(error) = self.persist_index_catalog() {
            let mut new = self.indexes.remove(&entry.original.name).unwrap();
            new.get_mut().definition = entry.shadow.clone();
            self.indexes.insert(entry.shadow.name.clone(), new);
            self.indexes.insert(entry.original.name.clone(), old);
            *self.btree_bindings.lock() = old_bindings;
            self.decimal_predicate_upgrade
                .as_mut()
                .unwrap()
                .replacements[position]
                .published_generation = None;
            return Err(error);
        }
        // No fallible derived-file writes between the atomic catalog and the
        // in-process aliases. Old generations remain intact throughout.
        let mut generations = self.fts_generations.lock();
        generations.indexes.remove(&entry.shadow.name);
        generations
            .indexes
            .insert(entry.original.name.clone(), completed.physical_name.clone());
        let paged = self.paged_records.as_ref().unwrap();
        paged.remove_index_alias(&entry.shadow.name);
        paged.set_index_alias(&entry.original.name, &completed.physical_name);
        self.index_generation.fetch_add(1, AtomicOrdering::Relaxed);
        self.schema_compatibility.invalidate();
        self.check_recovery_state()
    }
}

// The cut seam exists only in the Core unit-test binary. Production builds
// cannot activate it through an environment variable.
#[inline]
pub(super) fn test_process_cut(point: &str) {
    #[cfg(test)]
    if std::env::var("BICDB_TEST_DECIMAL_UPGRADE_CUT").as_deref() == Ok(point) {
        std::process::abort();
    }
    let _ = point;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sha2::{Digest, Sha256};

    fn config() -> DbConfig {
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
    }

    fn copy_actual_old_fixture(root: &Path) {
        copy_actual_old_fixture_named(root, "decimal-predicate-legacy-main482");
    }

    fn copy_actual_old_fixture_named(root: &Path, name: &str) {
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name);
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(source.join("FIXTURE.json")).unwrap()).unwrap();
        for (name, identity) in manifest["files"].as_object().unwrap() {
            let bytes = fs::read(source.join(name)).unwrap();
            assert_eq!(bytes.len() as u64, identity["bytes"].as_u64().unwrap());
            assert_eq!(
                hex::encode(Sha256::digest(&bytes)),
                identity["sha256"].as_str().unwrap()
            );
            let target = root.join(name);
            fs::create_dir_all(target.parent().unwrap()).unwrap();
            fs::write(target, bytes).unwrap();
        }
    }

    #[test]
    #[ignore = "subprocess entry point; invoked only by the parent process-cut controls"]
    fn child_upgrade() {
        let root =
            std::env::var_os("BICDB_TEST_DECIMAL_UPGRADE_PATH").expect("owned child store path");
        let mut db = BicDb::open_with_config(Path::new(&root), config()).unwrap();
        db.close().unwrap();
        panic!("process did not reach the required cut");
    }

    fn cut(root: &Path, point: &str) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "db::decimal_predicate::tests::child_upgrade",
                "--nocapture",
            ])
            .current_dir(root)
            .env("BICDB_TEST_DECIMAL_UPGRADE_PATH", root)
            .env("BICDB_TEST_DECIMAL_UPGRADE_CUT", point)
            .output()
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                output.status.signal(),
                Some(libc::SIGABRT),
                "{point}: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        #[cfg(not(unix))]
        assert!(!output.status.success());
    }

    fn verify_ack_uniqueness_and_reopens(root: &Path) {
        for reopen in 0..3 {
            let mut db = BicDb::open_with_config(root, config()).unwrap();
            assert_eq!(
                db.lookup_index(
                    "native_decimal_partial",
                    &[IndexValue::String("same".into())]
                )
                .unwrap(),
                vec!["acknowledged".to_string()],
                "reopen={reopen}"
            );
            assert_eq!(
                db.get("precision_probe", "acknowledged")
                    .unwrap()
                    .unwrap()
                    .metadata,
                json!({"amount":"0.5000000000000000000001","business_key":"same"})
            );
            assert_eq!(db.collection_record_count("precision_probe").unwrap(), 1);
            assert!(db
                .insert(
                    "precision_probe",
                    Record::new("duplicate")
                        .with_metadata(json!({"amount":"0.75","business_key":"same"}))
                )
                .is_err());
            assert!(db.get("precision_probe", "duplicate").unwrap().is_none());
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|report| report.valid));
            db.close().unwrap();
        }
    }

    #[test]
    fn owned_decimal_transition_survives_eight_process_publication_cuts() {
        for point in [
            "after_plan",
            "before_build_checkpoint",
            "during_build",
            "after_shadow_catalog",
            "after_build",
            "after_replacement",
            "before_certificate",
            "after_certificate",
        ] {
            let root = tempfile::tempdir().unwrap();
            copy_actual_old_fixture(root.path());
            cut(root.path(), point);
            verify_ack_uniqueness_and_reopens(root.path());
            let catalog: serde_json::Value =
                serde_json::from_slice(&fs::read(root.path().join(DEFAULT_INDEX_CATALOG)).unwrap())
                    .unwrap();
            assert_eq!(
                catalog["decimal_predicate_semantics_version"],
                json!(COMPARISON_VERSION)
            );
            assert!(catalog.get("decimal_predicate_upgrade").is_none());
            assert_eq!(catalog["indexes"].as_array().unwrap().len(), 1);
        }
    }

    #[test]
    fn changed_owner_definition_and_horizon_cannot_adopt_an_upgrade_plan() {
        for corruption in ["version", "horizon", "shadow_alias", "omitted_definition"] {
            let root = tempfile::tempdir().unwrap();
            copy_actual_old_fixture(root.path());
            cut(root.path(), "after_plan");
            let path = root.path().join(DEFAULT_INDEX_CATALOG);
            let mut catalog: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            let plan = &mut catalog["decimal_predicate_upgrade"];
            match corruption {
                "version" => plan["version"] = json!(COMPARISON_VERSION + 1),
                "horizon" => {
                    plan["source_commit_seq"] =
                        json!(plan["source_commit_seq"].as_u64().unwrap() + 1)
                }
                "shadow_alias" => {
                    plan["replacements"][0]["shadow"]["name"] =
                        plan["replacements"][0]["original"]["name"].clone()
                }
                "omitted_definition" => plan["replacements"] = json!([]),
                _ => unreachable!(),
            }
            fs::write(path, serde_json::to_vec_pretty(&catalog).unwrap()).unwrap();
            let error = BicDb::open_with_config(root.path(), config()).unwrap_err();
            assert!(
                error.to_string().contains("Decimal predicate transition"),
                "{corruption}: {error}"
            );
        }
    }
    #[test]
    fn multiple_generations_resume_after_each_atomic_replacement() {
        let root = tempfile::tempdir().unwrap();
        copy_actual_old_fixture_named(root.path(), "decimal-predicate-legacy-main482-multiple");
        cut(root.path(), "after_replacement");
        cut(root.path(), "after_replacement");
        for _ in 0..3 {
            let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
            assert_eq!(db.collection_record_count("precision_probe").unwrap(), 2);
            assert_eq!(db.index_definitions().len(), 2);
            for index in ["native_decimal_partial", "nested_decimal_partial"] {
                assert_eq!(
                    db.lookup_index(index, &[IndexValue::String("same".into())])
                        .unwrap(),
                    vec!["acknowledged".to_string()]
                );
                assert_eq!(
                    db.lookup_index(index, &[IndexValue::String("other".into())])
                        .unwrap(),
                    vec!["other_acknowledged".to_string()]
                );
            }
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|report| report.valid));
            db.close().unwrap();
        }
    }
}
