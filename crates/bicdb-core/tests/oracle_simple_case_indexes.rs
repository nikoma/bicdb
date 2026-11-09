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
