use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, IndexPredicate,
    IndexPredicateBinaryOperator, IndexPredicateValueType, IndexValue, Record, StorageMode,
};
use serde_json::json;

fn path(name: &str) -> IndexField {
    IndexField::MetadataPath(vec![name.into()])
}
fn row(id: &str, value: &str) -> Record {
    Record::new(id).with_metadata(json!({"value":value}))
}
fn index(name: &str, field: IndexField, unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        collection: "rows".into(),
        fields: vec![field],
        unique,
        nulls_not_distinct: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    }
}
fn key(value: &str) -> [IndexValue; 1] {
    [IndexValue::String(value.into())]
}
fn mode_config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(mode)
}

#[test]
fn unicode_and_legacy_keys_stay_distinct_through_backfill_mutation_rollback_and_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = mode_config(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("rows").unwrap();
        db.insert("rows", row("one", "é")).unwrap();
        db.create_index(index(
            "unicode",
            IndexField::OracleUpperV1(Box::new(path("value"))),
            false,
        ))
        .unwrap();
        db.create_index(index(
            "legacy",
            IndexField::Upper(Box::new(path("value"))),
            false,
        ))
        .unwrap();
        assert_eq!(db.lookup_index("unicode", &key("É")).unwrap(), vec!["one"]);
        assert_eq!(db.lookup_index("legacy", &key("é")).unwrap(), vec!["one"]);
        assert!(db.lookup_index("legacy", &key("É")).unwrap().is_empty());
        db.insert("rows", row("one", "ǆ")).unwrap();
        assert!(db.lookup_index("unicode", &key("É")).unwrap().is_empty());
        assert_eq!(db.lookup_index("unicode", &key("Ǆ")).unwrap(), vec!["one"]);
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("rows", row("one", "ø")).unwrap();
        tx.rollback().unwrap();
        assert_eq!(db.lookup_index("unicode", &key("Ǆ")).unwrap(), vec!["one"]);
        assert!(db.verify_index("unicode").unwrap().valid);
        db.flush().unwrap();
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        assert_eq!(db.lookup_index("unicode", &key("Ǆ")).unwrap(), vec!["one"]);
        assert_eq!(db.lookup_index("legacy", &key("ǆ")).unwrap(), vec!["one"]);
        let format: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("format.json")).unwrap())
                .unwrap();
        assert!(format["feature_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "oracle_simple_case_v1"));
        if db.is_server_paged() {
            capture_old_reader_fixture(root.path());
        }
        db.drop_index("unicode").unwrap();
        db.flush().unwrap();
        drop(db);
        let format: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("format.json")).unwrap())
                .unwrap();
        assert!(format["feature_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "oracle_simple_case_v1"));
    }
}

#[test]
fn unicode_unique_keys_refuse_real_case_collisions_while_legacy_ascii_keys_do_not() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), mode_config(mode)).unwrap();
        db.create_collection("rows").unwrap();
        db.insert("rows", row("a", "é")).unwrap();
        db.insert("rows", row("b", "É")).unwrap();
        db.create_index(index(
            "legacy",
            IndexField::Upper(Box::new(path("value"))),
            true,
        ))
        .unwrap();
        assert!(db
            .create_index(index(
                "unicode",
                IndexField::OracleUpperV1(Box::new(path("value"))),
                true
            ))
            .is_err());
        assert!(!db.index_definitions().iter().any(|d| d.name == "unicode"));
        assert_eq!(db.lookup_index("legacy", &key("é")).unwrap(), vec!["a"]);
        assert_eq!(db.lookup_index("legacy", &key("É")).unwrap(), vec!["b"]);
    }
}

#[test]
fn partial_predicate_case_mapping_alone_requires_the_sticky_reader_fence() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = mode_config(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("rows").unwrap();
        db.insert("rows", row("one", "é")).unwrap();
        let mut definition = index("partial", IndexField::Id, false);
        definition.predicate = Some(IndexPredicate::Binary {
            operator: IndexPredicateBinaryOperator::Equal,
            left: Box::new(IndexPredicate::Field {
                field: IndexField::OracleUpperV1(Box::new(path("value"))),
            }),
            right: Box::new(IndexPredicate::Literal { value: json!("É") }),
            value_type: IndexPredicateValueType::String,
        });
        db.create_index(definition).unwrap();
        assert_eq!(
            db.lookup_index("partial", &key("one")).unwrap(),
            vec!["one"]
        );
        db.insert("rows", row("one", "ø")).unwrap();
        assert!(db.lookup_index("partial", &key("one")).unwrap().is_empty());
        db.flush().unwrap();
        drop(db);
        let db = BicDb::open_with_config(root.path(), config).unwrap();
        assert!(db.lookup_index("partial", &key("one")).unwrap().is_empty());
        let format: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("format.json")).unwrap())
                .unwrap();
        assert!(format["feature_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "oracle_simple_case_v1"));
    }
}

// Optional external-binary qualification artifact. Never enabled in ordinary CI.
// Keep raw generated identity material outside repository evidence.
fn capture_old_reader_fixture(source: &std::path::Path) {
    let Some(destination) = std::env::var_os("BICDB_ORACLE_CASE_OLD_READER_FIXTURE") else {
        return;
    };
    let destination = std::path::PathBuf::from(destination);
    assert_eq!(
        destination.parent(),
        Some(std::path::Path::new(
            "/root/bicdb-durability-s3/oracle-case-oldreader-proof"
        ))
    );
    assert!(
        !destination.exists(),
        "refuse to replace an existing proof artifact"
    );
    fn copy(source: &std::path::Path, destination: &std::path::Path) {
        std::fs::create_dir(destination).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let target = destination.join(entry.file_name());
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            assert!(!metadata.file_type().is_symlink());
            if metadata.is_dir() {
                copy(&path, &target);
            } else {
                std::fs::copy(&path, &target).unwrap();
            }
        }
    }
    copy(source, &destination);
}

fn completed_shadow(
    db: &mut BicDb,
    definition: IndexDefinition,
) -> bicdb_core::PagedBTreeBuildState {
    use bicdb_core::{
        PagedBTreeBuildAdvance, PagedBTreeBuildLimits, ResourceDemand, ResourceGovernor,
        ResourceGovernorConfig,
    };
    let limits = PagedBTreeBuildLimits::default();
    let name = definition.name.clone();
    db.begin_paged_btree_build(definition, limits, 100).unwrap();
    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
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
    panic!("bounded shadow build did not complete");
}

#[test]
fn atomic_case_binding_survives_legacy_alias_staleness_writes_reopen_rename_and_drop() {
    let root = tempfile::tempdir().unwrap();
    let config = mode_config(StorageMode::ServerPaged);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    db.create_collection("rows").unwrap();
    db.insert("rows", row("one", "é")).unwrap();
    let old = index("legacy", IndexField::Upper(Box::new(path("value"))), true);
    db.create_index(old.clone()).unwrap();
    let shadow = completed_shadow(
        &mut db,
        index(
            "owned-shadow",
            IndexField::OracleUpperV1(Box::new(path("value"))),
            true,
        ),
    );
    // Complete shadows are ordinary load-bearing indexes: intervening data and
    // ownership-intent commits must not invalidate a correctly maintained build.
    db.insert("rows", row("two", "ø")).unwrap();
    let before = std::fs::read(root.path().join("indexes.json")).unwrap();
    let mut wrong_old = old.clone();
    wrong_old.unique = false;
    assert!(db
        .publish_oracle_case_v1_index_replacement(&wrong_old, &shadow)
        .is_err());
    assert_eq!(
        std::fs::read(root.path().join("indexes.json")).unwrap(),
        before
    );
    db.publish_oracle_case_v1_index_replacement(&old, &shadow)
        .unwrap();
    db.publish_oracle_case_v1_index_replacement(&old, &shadow)
        .unwrap();
    assert_eq!(db.lookup_index("legacy", &key("É")).unwrap(), vec!["one"]);
    assert_eq!(db.lookup_index("legacy", &key("Ø")).unwrap(), vec!["two"]);
    assert!(db
        .discard_paged_btree_build(&shadow.definition.name, shadow.limits)
        .is_err());
    assert!(db.insert("rows", row("duplicate", "É")).is_err());
    db.flush().unwrap();
    drop(db);
    // The old derived file intentionally still aliases `legacy` to its old
    // generation. The atomic catalog, not this stale cache, controls reopen.
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    db.publish_oracle_case_v1_index_replacement(&old, &shadow)
        .unwrap();
    assert_eq!(db.lookup_index("legacy", &key("É")).unwrap(), vec!["one"]);
    db.insert("rows", row("one", "ǆ")).unwrap();
    assert_eq!(db.lookup_index("legacy", &key("Ǆ")).unwrap(), vec!["one"]);
    db.rename_index("legacy", "renamed").unwrap();
    db.flush().unwrap();
    drop(db);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    assert_eq!(db.lookup_index("renamed", &key("Ǆ")).unwrap(), vec!["one"]);
    assert!(db.verify_index("renamed").unwrap().valid);
    db.drop_index("renamed").unwrap();
    db.flush().unwrap();
    drop(db);
    let db = BicDb::open_with_config(root.path(), config).unwrap();
    assert!(!db
        .index_definitions()
        .iter()
        .any(|d| d.name == "legacy" || d.name == "renamed" || d.name == "owned-shadow"));
}

#[test]
fn explicit_oracle_capability_does_not_require_an_expression_index() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), mode_config(mode)).unwrap();
        db.enable_oracle_simple_case_v1().unwrap();
        let format: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("format.json")).unwrap())
                .unwrap();
        assert!(format["feature_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "oracle_simple_case_v1"));
    }
}

enum CaseShadow {
    Paged(bicdb_core::PagedBTreeBuildState),
    Embedded(IndexDefinition),
}
fn publish_case_shadow(
    db: &mut BicDb,
    old: &IndexDefinition,
    shadow: &CaseShadow,
) -> bicdb_core::Result<()> {
    match shadow {
        CaseShadow::Paged(state) => db.publish_oracle_case_v1_index_replacement(old, state),
        CaseShadow::Embedded(definition) => {
            db.publish_embedded_oracle_case_v1_index_replacement(old, definition)
        }
    }
}

#[test]
fn atomic_case_binding_failure_steps_leave_whole_definition_and_generation_pairs() {
    use bicdb_page::test_faults::arm_io_failure;
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for step in ["write", "sync", "rename", "dir_sync"] {
            let root = tempfile::tempdir().unwrap();
            let config = mode_config(mode.clone()).with_fsync(true);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            db.create_collection("rows").unwrap();
            db.insert("rows", row("one", "é")).unwrap();
            let old = index("legacy", IndexField::Upper(Box::new(path("value"))), true);
            db.create_index(old.clone()).unwrap();
            let definition = index(
                "owned-shadow",
                IndexField::OracleUpperV1(Box::new(path("value"))),
                true,
            );
            let shadow = if mode == StorageMode::ServerPaged {
                CaseShadow::Paged(completed_shadow(&mut db, definition))
            } else {
                db.create_index(definition.clone()).unwrap();
                CaseShadow::Embedded(definition)
            };
            let fault = arm_io_failure(&format!("index_catalog.{step}"), root.path(), 1, false);
            assert!(
                publish_case_shadow(&mut db, &old, &shadow).is_err(),
                "{step}"
            );
            assert_eq!(fault.fired(), 1, "{step}");
            assert!(
                db.get("rows", "one").is_err(),
                "uncertain publication fences the handle: {step}"
            );
            drop(fault);
            drop(db);
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            let published = step == "dir_sync";
            assert_eq!(
                db.lookup_index("legacy", &key(if published { "É" } else { "é" }))
                    .unwrap(),
                vec!["one"],
                "{step}"
            );
            assert!(
                db.lookup_index("legacy", &key(if published { "é" } else { "É" }))
                    .unwrap()
                    .is_empty(),
                "{step}"
            );
            publish_case_shadow(&mut db, &old, &shadow).unwrap();
            assert_eq!(
                db.lookup_index("legacy", &key("É")).unwrap(),
                vec!["one"],
                "{step}"
            );
        }
    }
}

#[test]
#[ignore = "subprocess helper invoked by atomic_case_binding_publication_process_crashes"]
fn atomic_case_binding_crash_child() {
    let Ok(root) = std::env::var("BICDB_CASE_BINDING_TEST_ROOT") else {
        return;
    };
    let mode = if std::env::var("BICDB_CASE_BINDING_MODE").unwrap() == "paged" {
        StorageMode::ServerPaged
    } else {
        StorageMode::EmbeddedMemory
    };
    let mut db =
        BicDb::open_with_config(&root, mode_config(mode.clone()).with_fsync(true)).unwrap();
    let old = index("legacy", IndexField::Upper(Box::new(path("value"))), true);
    let shadow = if mode == StorageMode::ServerPaged {
        CaseShadow::Paged(
            db.load_paged_btree_build("owned-shadow", bicdb_core::PagedBTreeBuildLimits::default())
                .unwrap(),
        )
    } else {
        CaseShadow::Embedded(
            db.index_definitions()
                .into_iter()
                .find(|definition| definition.name == "owned-shadow")
                .unwrap(),
        )
    };
    println!("publishing-owned-shadow");
    use std::io::Write;
    std::io::stdout().flush().unwrap();
    publish_case_shadow(&mut db, &old, &shadow).unwrap();
    panic!("configured publication crash did not fire");
}

#[test]
fn atomic_case_binding_publication_process_crashes() {
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for point in [
            "after_write",
            "after_sync",
            "after_rename",
            "after_dir_sync",
        ] {
            let root = tempfile::tempdir().unwrap();
            let config = mode_config(mode.clone()).with_fsync(true);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            db.create_collection("rows").unwrap();
            db.insert("rows", row("one", "é")).unwrap();
            let old = index("legacy", IndexField::Upper(Box::new(path("value"))), true);
            db.create_index(old.clone()).unwrap();
            let definition = index(
                "owned-shadow",
                IndexField::OracleUpperV1(Box::new(path("value"))),
                true,
            );
            let shadow = if mode == StorageMode::ServerPaged {
                CaseShadow::Paged(completed_shadow(&mut db, definition))
            } else {
                db.create_index(definition.clone()).unwrap();
                CaseShadow::Embedded(definition)
            };
            db.flush().unwrap();
            drop(db);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "atomic_case_binding_crash_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("BICDB_CASE_BINDING_TEST_ROOT", root.path())
                .env(
                    "BICDB_CASE_BINDING_MODE",
                    if mode == StorageMode::ServerPaged {
                        "paged"
                    } else {
                        "embedded"
                    },
                )
                .env("BICDB_TEST_CRASH_AT", format!("index_catalog.{point}"))
                .output()
                .unwrap();
            #[cfg(unix)]
            assert_eq!(
                output.status.signal(),
                Some(6),
                "{point}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("publishing-owned-shadow"),
                "crash must follow startup: {point}"
            );
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            let published = matches!(point, "after_rename" | "after_dir_sync");
            assert_eq!(
                db.lookup_index("legacy", &key(if published { "É" } else { "é" }))
                    .unwrap(),
                vec!["one"],
                "{point}"
            );
            assert!(
                db.lookup_index("legacy", &key(if published { "é" } else { "É" }))
                    .unwrap()
                    .is_empty(),
                "{point}"
            );
            publish_case_shadow(&mut db, &old, &shadow).unwrap();
            assert!(db.verify_index("legacy").unwrap().valid, "{point}");
        }
    }
}

#[test]
fn embedded_atomic_case_binding_preserves_uniqueness_and_reopens() {
    let root = tempfile::tempdir().unwrap();
    let config = mode_config(StorageMode::EmbeddedMemory);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    db.create_collection("rows").unwrap();
    db.insert("rows", row("one", "é")).unwrap();
    let old = index("legacy", IndexField::Upper(Box::new(path("value"))), true);
    let shadow = index(
        "owned-shadow",
        IndexField::OracleUpperV1(Box::new(path("value"))),
        true,
    );
    db.create_index(old.clone()).unwrap();
    db.create_index(shadow.clone()).unwrap();
    db.insert("rows", row("two", "ø")).unwrap();
    db.publish_embedded_oracle_case_v1_index_replacement(&old, &shadow)
        .unwrap();
    db.publish_embedded_oracle_case_v1_index_replacement(&old, &shadow)
        .unwrap();
    assert_eq!(db.lookup_index("legacy", &key("É")).unwrap(), vec!["one"]);
    assert_eq!(db.lookup_index("legacy", &key("Ø")).unwrap(), vec!["two"]);
    assert!(db.insert("rows", row("duplicate", "É")).is_err());
    db.flush().unwrap();
    drop(db);
    let mut db = BicDb::open_with_config(root.path(), config).unwrap();
    db.publish_embedded_oracle_case_v1_index_replacement(&old, &shadow)
        .unwrap();
    assert!(db.verify_index("legacy").unwrap().valid);
    assert_eq!(db.lookup_index("legacy", &key("É")).unwrap(), vec!["one"]);
    db.insert("rows", row("one", "ǆ")).unwrap();
    assert_eq!(db.lookup_index("legacy", &key("Ǆ")).unwrap(), vec!["one"]);
}

#[test]
fn owned_shadow_collision_refuses_publication_without_changing_legacy_constraint() {
    use bicdb_core::{
        PagedBTreeBuildAdvance, PagedBTreeBuildLimits, ResourceDemand, ResourceGovernor,
        ResourceGovernorConfig,
    };
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), mode_config(StorageMode::ServerPaged)).unwrap();
    db.create_collection("rows").unwrap();
    db.insert("rows", row("one", "é")).unwrap();
    db.insert("rows", row("two", "É")).unwrap();
    let old = index("legacy", IndexField::Upper(Box::new(path("value"))), true);
    db.create_index(old.clone()).unwrap();
    let limits = PagedBTreeBuildLimits::default();
    db.begin_paged_btree_build(
        index(
            "owned-shadow",
            IndexField::OracleUpperV1(Box::new(path("value"))),
            true,
        ),
        limits,
        100,
    )
    .unwrap();
    let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
    let demand = ResourceDemand {
        memory_bytes: limits.max_batch_bytes as u64,
        io_bytes: limits.max_batch_bytes as u64,
        cpu_slots: 1,
        io_charge_bytes: 1,
    };
    let mut refused = false;
    for now in 101..1000 {
        match db.advance_paged_btree_build_governed("owned-shadow", limits, &governor, demand, now)
        {
            Err(error) => {
                assert!(error.to_string().contains("duplicate"), "{error}");
                refused = true;
                break;
            }
            Ok(PagedBTreeBuildAdvance::Progress(_)) => {}
            Ok(PagedBTreeBuildAdvance::Complete(_)) => {
                panic!("colliding shadow must never publish")
            }
        }
    }
    assert!(refused);
    let state = db.load_paged_btree_build("owned-shadow", limits).unwrap();
    assert!(db
        .publish_oracle_case_v1_index_replacement(&old, &state)
        .is_err());
    assert_eq!(db.index_definitions(), vec![old]);
    assert_eq!(db.lookup_index("legacy", &key("é")).unwrap(), vec!["one"]);
    assert_eq!(db.lookup_index("legacy", &key("É")).unwrap(), vec!["two"]);
    assert!(db
        .discard_paged_btree_build("owned-shadow", limits)
        .unwrap());
}

#[test]
fn oracle_publication_refuses_pending_writes_and_invalid_build_ownership() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), mode_config(mode.clone())).unwrap();
        db.create_collection("rows").unwrap();
        let old = index("legacy", IndexField::Upper(Box::new(path("value"))), true);
        db.create_index(old.clone()).unwrap();
        let definition = index(
            "owned-shadow",
            IndexField::OracleUpperV1(Box::new(path("value"))),
            true,
        );
        let shadow = if mode == StorageMode::ServerPaged {
            CaseShadow::Paged(completed_shadow(&mut db, definition))
        } else {
            db.create_index(definition.clone()).unwrap();
            CaseShadow::Embedded(definition)
        };
        let before = std::fs::read(root.path().join("indexes.json")).unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("rows", row("pending", "é")).unwrap();
        assert!(publish_case_shadow(&mut db, &old, &shadow).is_err());
        assert!(db.enable_oracle_simple_case_v1().is_err());
        assert_eq!(
            std::fs::read(root.path().join("indexes.json")).unwrap(),
            before
        );
        tx.rollback().unwrap();
        if let CaseShadow::Paged(state) = &shadow {
            let mut unrelated = state.clone();
            unrelated.run_id = uuid::Uuid::now_v7();
            assert!(db
                .publish_oracle_case_v1_index_replacement(&old, &unrelated)
                .is_err());
            assert_eq!(
                std::fs::read(root.path().join("indexes.json")).unwrap(),
                before
            );
        }
        match &shadow {
            CaseShadow::Paged(state) => {
                let mut ambiguous = state.clone();
                ambiguous.definition.name = "LEGACY".into();
                assert!(db
                    .publish_oracle_case_v1_index_replacement(&old, &ambiguous)
                    .unwrap_err()
                    .to_string()
                    .contains("ambiguous"));
            }
            CaseShadow::Embedded(definition) => {
                let mut ambiguous = definition.clone();
                ambiguous.name = "LEGACY".into();
                assert!(db
                    .publish_embedded_oracle_case_v1_index_replacement(&old, &ambiguous)
                    .unwrap_err()
                    .to_string()
                    .contains("ambiguous"));
            }
        }
        assert_eq!(
            std::fs::read(root.path().join("indexes.json")).unwrap(),
            before
        );
        publish_case_shadow(&mut db, &old, &shadow).unwrap();
    }
}
