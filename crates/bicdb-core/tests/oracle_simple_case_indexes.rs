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

#[test]
fn national_case_keys_preserve_the_distinct_character_family_through_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for (upper, original, folded) in [(true, "ɡ", "Ɡ"), (false, "Ⱥ", "ⱥ")] {
            let root = tempfile::tempdir().unwrap();
            let config = mode_config(mode.clone());
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            db.create_collection("rows").unwrap();
            db.insert("rows", row("a", original)).unwrap();
            db.insert("rows", row("b", folded)).unwrap();
            let character = if upper {
                IndexField::OracleUpperV1(Box::new(path("value")))
            } else {
                IndexField::OracleLowerV1(Box::new(path("value")))
            };
            let national = if upper {
                IndexField::OracleUpperNationalV1(Box::new(path("value")))
            } else {
                IndexField::OracleLowerNationalV1(Box::new(path("value")))
            };
            db.create_index(index("character", character, true))
                .unwrap();
            assert!(db
                .create_index(index("national_unique", national.clone(), true))
                .is_err());
            assert!(!db
                .index_definitions()
                .iter()
                .any(|d| d.name == "national_unique"));
            db.create_index(index("national", national, false)).unwrap();
            assert_eq!(
                db.lookup_index("character", &key(original)).unwrap(),
                vec!["a"]
            );
            let mut actual = db.lookup_index("national", &key(folded)).unwrap();
            actual.sort();
            assert_eq!(actual, vec!["a", "b"]);
            db.insert("rows", row("b", "unrelated")).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("rows", row("a", "rolled back")).unwrap();
            tx.rollback().unwrap();
            assert_eq!(
                db.lookup_index("national", &key(folded)).unwrap(),
                vec!["a"]
            );
            db.flush().unwrap();
            drop(db);
            let db = BicDb::open_with_config(root.path(), config).unwrap();
            assert_eq!(
                db.lookup_index("national", &key(folded)).unwrap(),
                vec!["a"]
            );
            assert_eq!(
                db.lookup_index("character", &key(original)).unwrap(),
                vec!["a"]
            );
            assert!(db.verify_index("national").unwrap().valid);
        }
    }
}

#[test]
fn national_partial_predicate_alone_enables_the_reader_fence() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = mode_config(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("rows").unwrap();
        db.insert("rows", row("one", "ɡ")).unwrap();
        let mut definition = index("national_partial", IndexField::Id, false);
        definition.predicate = Some(IndexPredicate::Binary {
            operator: IndexPredicateBinaryOperator::Equal,
            left: Box::new(IndexPredicate::Field {
                field: IndexField::OracleUpperNationalV1(Box::new(path("value"))),
            }),
            right: Box::new(IndexPredicate::Literal {
                value: json!("Ɡ")
            }),
            value_type: IndexPredicateValueType::String,
        });
        db.create_index(definition).unwrap();
        assert_eq!(
            db.lookup_index("national_partial", &key("one")).unwrap(),
            vec!["one"]
        );
        db.flush().unwrap();
        drop(db);
        let db = BicDb::open_with_config(root.path(), config).unwrap();
        assert_eq!(
            db.lookup_index("national_partial", &key("one")).unwrap(),
            vec!["one"]
        );
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
