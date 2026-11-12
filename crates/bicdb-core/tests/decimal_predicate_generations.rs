use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, IndexPredicate,
    IndexPredicateBinaryOperator, IndexPredicateValueType, IndexValue, Record, StorageMode,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

fn definition() -> IndexDefinition {
    IndexDefinition {
        name: "decimal_unique".into(),
        collection: "probe".into(),
        fields: vec![IndexField::MetadataPath(vec!["business_key".into()])],
        unique: true,
        nulls_not_distinct: false,
        kind: IndexKind::BTree,
        predicate: Some(IndexPredicate::Binary {
            operator: IndexPredicateBinaryOperator::Greater,
            left: Box::new(IndexPredicate::Field {
                field: IndexField::MetadataPath(vec!["amount".into()]),
            }),
            right: Box::new(IndexPredicate::Literal {
                value: json!("0.5"),
            }),
            value_type: IndexPredicateValueType::Decimal,
        }),
        exclusion: None,
    }
}

fn acknowledged() -> Record {
    Record::new("acknowledged")
        .with_metadata(json!({"business_key":"same","amount":"0.5000000000000000000001"}))
}

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(mode)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}

fn bytes(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn visit(root: &Path, at: &Path, result: &mut BTreeMap<String, Vec<u8>>) {
        for entry in std::fs::read_dir(at).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().to_str().unwrap().into(),
                    std::fs::read(&path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}

fn actual_legacy_fixture(target: &Path) {
    actual_legacy_fixture_named(target, "decimal-predicate-legacy-main482");
}

fn actual_legacy_fixture_named(target: &Path, name: &str) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(source.join("FIXTURE.json")).unwrap()).unwrap();
    for (name, identity) in manifest["files"].as_object().unwrap() {
        let value = std::fs::read(source.join(name)).unwrap();
        assert_eq!(value.len() as u64, identity["bytes"].as_u64().unwrap());
        assert_eq!(
            hex::encode(Sha256::digest(&value)),
            identity["sha256"].as_str().unwrap()
        );
        let path = target.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, value).unwrap();
    }
}

#[test]
fn legacy_admission_cannot_be_bypassed_by_disabling_secondary_index_loading() {
    let root = tempfile::tempdir().unwrap();
    actual_legacy_fixture(root.path());
    let before = bytes(root.path());
    let error = BicDb::open_with_config(
        root.path(),
        config(StorageMode::ServerPaged).with_secondary_indexes(false),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("legacy Decimal predicate"),
        "{error}"
    );
    assert_eq!(
        bytes(root.path()),
        before,
        "preflight must preserve the old bytes"
    );
}

#[test]
fn actual_legacy_generation_is_rebuilt_before_reads_and_writes_then_reopens_twice() {
    let root = tempfile::tempdir().unwrap();
    actual_legacy_fixture(root.path());
    let config = config(StorageMode::ServerPaged);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    for reopen in 0..3 {
        assert_eq!(
            db.lookup_index(
                "native_decimal_partial",
                &[IndexValue::String("same".into())]
            )
            .unwrap(),
            vec!["acknowledged".to_string()],
            "reopen={reopen}: actual old generation must be repaired before admission"
        );
        assert_eq!(
            db.get("precision_probe", "acknowledged")
                .unwrap()
                .unwrap()
                .metadata,
            acknowledged().metadata
        );
        assert!(db
            .insert(
                "precision_probe",
                Record::new("duplicate")
                    .with_metadata(json!({"business_key":"same","amount":"0.75"}))
            )
            .is_err());
        assert!(db.get("precision_probe", "duplicate").unwrap().is_none());
        assert!(db
            .verify_indexes_consistently()
            .unwrap()
            .iter()
            .all(|report| report.valid));
        db.close().unwrap();
        if reopen == 2 {
            break;
        }
        db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    }
}

#[test]
fn fresh_generation_keeps_acknowledged_keys_and_uniqueness_through_two_reopens() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = config(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("probe").unwrap();
        db.insert("probe", acknowledged()).unwrap();
        db.create_index(definition()).unwrap();
        for reopen in 0..3 {
            assert_eq!(
                db.lookup_index("decimal_unique", &[IndexValue::String("same".into())])
                    .unwrap(),
                vec!["acknowledged".to_string()],
                "{mode} reopen={reopen}"
            );
            assert!(db
                .insert(
                    "probe",
                    Record::new("duplicate")
                        .with_metadata(json!({"business_key":"same","amount":"0.75"})),
                )
                .is_err());
            assert!(db.get("probe", "duplicate").unwrap().is_none());
            assert_eq!(
                db.get("probe", "acknowledged").unwrap().unwrap().metadata,
                acknowledged().metadata
            );
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|report| report.valid));
            db.close().unwrap();
            if reopen == 2 {
                break;
            }
            db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        }
        let catalog: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("indexes.json")).unwrap())
                .unwrap();
        assert_eq!(catalog["decimal_predicate_semantics_version"], 1);
        if mode == StorageMode::ServerPaged {
            assert!(catalog["btree_bindings"]["decimal_unique"].is_string());
        }
    }
}

#[test]
fn empty_generation_keeps_its_physical_binding_and_an_unrelated_native_ack() {
    let root = tempfile::tempdir().unwrap();
    let config = config(StorageMode::ServerPaged);
    let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
    db.create_collection("probe").unwrap();
    db.create_collection("unrelated").unwrap();
    let mut other = db.begin_transaction().unwrap();
    other
        .insert("unrelated", Record::new("separate-ack"))
        .unwrap();
    db.create_empty_btree_index(definition()).unwrap();
    other.commit().unwrap();
    db.insert("probe", acknowledged()).unwrap();
    db.close().unwrap();
    let db = BicDb::open_with_config(root.path(), config).unwrap();
    assert!(db.get("unrelated", "separate-ack").unwrap().is_some());
    assert_eq!(
        db.lookup_index("decimal_unique", &[IndexValue::String("same".into())])
            .unwrap(),
        vec!["acknowledged".to_string()]
    );
    assert!(db
        .verify_indexes_consistently()
        .unwrap()
        .iter()
        .all(|report| report.valid));
    db.close().unwrap();
}

#[test]
fn semantic_version_cannot_claim_authority_without_its_reader_fence_or_binding() {
    for kind in ["unfenced", "unbound", "unknown"] {
        let root = tempfile::tempdir().unwrap();
        let config = config(StorageMode::ServerPaged);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        db.create_collection("probe").unwrap();
        db.insert("probe", acknowledged()).unwrap();
        db.create_index(definition()).unwrap();
        db.close().unwrap();
        // Explicit corruption controls, not a fabricated old-format fixture.
        let path = root.path().join(if kind == "unfenced" {
            "format.json"
        } else {
            "indexes.json"
        });
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        match kind {
            "unfenced" => value["feature_flags"]
                .as_array_mut()
                .unwrap()
                .retain(|flag| flag.as_str() != Some("decimal_predicate_comparison_v1")),
            "unbound" => {
                value["btree_bindings"].as_object_mut().unwrap().clear();
            }
            "unknown" => value["decimal_predicate_semantics_version"] = json!(2),
            _ => unreachable!(),
        }
        std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        let error = BicDb::open_with_config(root.path(), config).unwrap_err();
        assert!(
            error.to_string().contains(match kind {
                "unfenced" => "unfenced Decimal",
                "unbound" => "authoritative physical",
                _ => "unsupported Decimal",
            }),
            "{kind}: {error}"
        );
    }
}

#[test]
fn decimal_predicates_keep_the_existing_native_index_kind_contract() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    db.create_collection("probe").unwrap();
    for kind in [
        IndexKind::FullText,
        IndexKind::Jsonb,
        IndexKind::Array,
        IndexKind::Spatial,
    ] {
        let mut definition = definition();
        definition.name = format!("invalid_{kind:?}");
        definition.kind = kind;
        definition.unique = false;
        let error = db.create_index(definition).unwrap_err();
        assert!(
            error.to_string().contains("must use B-tree kind"),
            "{error}"
        );
    }
    let mut definition = definition();
    definition.unique = false;
    definition.exclusion = Some(bicdb_core::IndexExclusion {
        elements: vec![bicdb_core::IndexExclusionElement::Equal {
            field: definition.fields[0].clone(),
        }],
    });
    let error = db.create_index(definition).unwrap_err();
    assert!(
        error.to_string().contains("non-unique, non-partial B-tree"),
        "{error}"
    );
    assert!(db.index_definitions().is_empty());
}

#[test]
fn actual_old_embedded_and_multiple_nested_generations_upgrade_before_admission() {
    for (fixture, mode, expected_rows) in [
        (
            "decimal-predicate-legacy-main482-embedded",
            StorageMode::EmbeddedMemory,
            1,
        ),
        (
            "decimal-predicate-legacy-main482-multiple",
            StorageMode::ServerPaged,
            2,
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        actual_legacy_fixture_named(root.path(), fixture);
        for reopen in 0..3 {
            let mut db = BicDb::open_with_config(root.path(), config(mode.clone())).unwrap();
            assert_eq!(
                db.collection_record_count("precision_probe").unwrap(),
                expected_rows,
                "{fixture} reopen={reopen}"
            );
            assert_eq!(
                db.lookup_index(
                    "native_decimal_partial",
                    &[IndexValue::String("same".into())]
                )
                .unwrap(),
                vec!["acknowledged".to_string()]
            );
            assert_eq!(
                db.get("precision_probe", "acknowledged")
                    .unwrap()
                    .unwrap()
                    .metadata,
                acknowledged().metadata
            );
            if expected_rows == 2 {
                assert_eq!(
                    db.lookup_index(
                        "nested_decimal_partial",
                        &[IndexValue::String("same".into())]
                    )
                    .unwrap(),
                    vec!["acknowledged".to_string()]
                );
                assert!(db
                    .get("precision_probe", "other_acknowledged")
                    .unwrap()
                    .is_some());
            }
            assert!(db
                .insert(
                    "precision_probe",
                    Record::new("duplicate")
                        .with_metadata(json!({"business_key":"same","amount":"0.75"}))
                )
                .is_err());
            assert!(db
                .verify_indexes_consistently()
                .unwrap()
                .iter()
                .all(|report| report.valid));
            db.close().unwrap();
        }
    }
}

#[test]
fn actual_old_keys_that_become_duplicates_refuse_admission_without_losing_either_ack() {
    let root = tempfile::tempdir().unwrap();
    actual_legacy_fixture_named(root.path(), "decimal-predicate-legacy-main482-duplicate");
    for _ in 0..3 {
        let error =
            BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap_err();
        assert!(
            error.to_string().contains("duplicate") || error.to_string().contains("unique"),
            "{error}"
        );
        // Independent Native row inspection after the refused Core opener.
        // This creates no public Core handle and never bypasses its gate.
        let _root_guard = bicdb_page::DirectoryLock::acquire(root.path()).unwrap();
        let cfg = config(StorageMode::ServerPaged);
        let paged = bicdb_core::PagedRecords::open(
            root.path().join("paged"),
            bicdb_core::PagedRecordsOptions {
                page_size: cfg.paged_page_size,
                buffer_pool_bytes: 8 * 1024 * 1024,
                fsync: true,
                ..Default::default()
            },
        )
        .unwrap();
        let snapshot = paged.latest_snapshot();
        let rows = paged
            .scan(&snapshot, "precision_probe")
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 2);
        for (id, amount) in [
            ("acknowledged", "0.5000000000000000000001"),
            ("second_acknowledged", "0.5000000000000000000002"),
        ] {
            let row = rows.iter().find(|row| row.id == id).unwrap();
            assert_eq!(row.metadata, json!({"business_key":"same","amount":amount}));
        }
    }
}

#[test]
fn every_upgrade_catalog_replacement_step_fails_closed_and_recovers_the_ack() {
    use bicdb_page::test_faults::arm_io_failure;
    let baseline = tempfile::tempdir().unwrap();
    actual_legacy_fixture(baseline.path());
    let observation = arm_io_failure("index_catalog.write", baseline.path(), u64::MAX, false);
    let mut db =
        BicDb::open_with_config(baseline.path(), config(StorageMode::ServerPaged)).unwrap();
    let publications = observation.seen();
    assert!(
        publications >= 4,
        "must include plan, shadow, replacement and certificate"
    );
    assert_eq!(observation.fired(), 0);
    drop(observation);
    db.close().unwrap();
    for step in ["write", "sync", "rename", "dir_sync"] {
        for nth in 1..=publications {
            let root = tempfile::tempdir().unwrap();
            actual_legacy_fixture(root.path());
            let fault = arm_io_failure(&format!("index_catalog.{step}"), root.path(), nth, false);
            let error =
                BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap_err();
            assert_eq!(fault.fired(), 1, "{step} publication{nth}: {error}");
            assert!(
                root.path()
                    .join(bicdb_core::DURABILITY_FAILURE_MARKER)
                    .exists(),
                "{step} publication{nth}: missing durability fence"
            );
            drop(fault);
            for reopen in 0..3 {
                let mut db =
                    BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
                assert!(
                    db.durability_state().is_healthy(),
                    "{step} publication{nth} reopen{reopen}"
                );
                assert_eq!(
                    db.lookup_index(
                        "native_decimal_partial",
                        &[IndexValue::String("same".into())]
                    )
                    .unwrap(),
                    vec!["acknowledged".to_string()]
                );
                assert_eq!(
                    db.get("precision_probe", "acknowledged")
                        .unwrap()
                        .unwrap()
                        .metadata,
                    acknowledged().metadata
                );
                assert!(db
                    .insert(
                        "precision_probe",
                        Record::new("duplicate")
                            .with_metadata(json!({"business_key":"same","amount":"0.75"}))
                    )
                    .is_err());
                assert!(db
                    .verify_indexes_consistently()
                    .unwrap()
                    .iter()
                    .all(|report| report.valid));
                db.close().unwrap();
            }
        }
    }
}
