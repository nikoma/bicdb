//! Offline `embedded_memory` -> `server_paged` migration: the escape hatch
//! for a database whose working set outgrew RAM under the resident engine.

use bicdb_core::storage_mode_migration::{migrate_storage_mode, DEFAULT_MIGRATION_BATCH};
use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;

/// Read the mode straight out of `format.json` (the durable record).
fn recorded_mode(root: &std::path::Path) -> String {
    let text = std::fs::read_to_string(root.join("format.json")).unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    value["storage_mode"].as_str().unwrap().to_string()
}

fn seed_source(path: &std::path::Path, rows: usize) {
    let mut db = BicDb::open_with_config(
        path,
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::EmbeddedMemory),
    )
    .unwrap();
    db.create_collection("landings").unwrap();
    db.create_collection("audit").unwrap();
    db.bulk_load_insert(
        "landings",
        (0..rows).map(|index| {
            Record::new(format!("id-{index:06}")).with_metadata(json!({
                "source_message_id": format!("src-{index:06}"),
                "normalized_domain": format!("d{}.example", index % 97),
                "payload": {"body": "x".repeat(200), "n": index},
            }))
        }),
    )
    .unwrap();
    db.insert("audit", Record::new("a1").with_metadata(json!({"k": "v"})))
        .unwrap();

    // A unique index (the conflict target shape) and a non-unique one.
    db.create_index(IndexDefinition {
        nulls_not_distinct: false,
        name: "landings_source_message_id".to_string(),
        collection: "landings".to_string(),
        fields: vec![IndexField::MetadataPath(vec![
            "source_message_id".to_string()
        ])],
        unique: true,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db.create_index(IndexDefinition {
        nulls_not_distinct: false,
        name: "landings_domain".to_string(),
        collection: "landings".to_string(),
        fields: vec![IndexField::MetadataPath(vec![
            "normalized_domain".to_string()
        ])],
        unique: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db.close().unwrap();
}

#[test]
fn embedded_memory_migrates_to_server_paged_with_data_and_indexes() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let target = target_dir.path().join("migrated");
    let rows = 2_500usize;
    seed_source(source_dir.path(), rows);

    let report = migrate_storage_mode(
        source_dir.path(),
        &target,
        StorageMode::ServerPaged,
        DEFAULT_MIGRATION_BATCH,
    )
    .expect("migration must succeed");

    assert_eq!(report.source_mode, "embedded_memory");
    assert_eq!(report.target_mode, "server_paged");
    assert_eq!(report.collections, 2);
    assert_eq!(report.records, rows as u64 + 1);
    assert_eq!(report.verified_records, report.records);
    assert_eq!(report.indexes, 2);

    // The source is untouched and still embedded_memory.
    assert_eq!(recorded_mode(source_dir.path()), "embedded_memory");

    // The target must be OPENED as server_paged — the ADR-004 fence refuses
    // a default (embedded) open, which is exactly the deployment step an
    // operator has to make after migrating.
    assert!(
        BicDb::open(&target).is_err(),
        "the fence must refuse opening a paged store as embedded"
    );
    let migrated = BicDb::open_with_config(
        &target,
        DbConfig::default().with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    assert_eq!(recorded_mode(&target), "server_paged");
    assert_eq!(migrated.scan_collection("landings").unwrap().len(), rows);
    assert_eq!(migrated.scan_collection("audit").unwrap().len(), 1);

    // Spot-check payload fidelity at both ends and the middle.
    for index in [0usize, rows / 2, rows - 1] {
        let record = migrated
            .get("landings", &format!("id-{index:06}"))
            .unwrap()
            .expect("row must survive migration");
        assert_eq!(
            record.metadata["source_message_id"],
            json!(format!("src-{index:06}"))
        );
        assert_eq!(record.metadata["payload"]["n"], json!(index));
        assert_eq!(
            record.metadata["payload"]["body"].as_str().unwrap().len(),
            200
        );
    }

    // Index definitions came across...
    let names: Vec<String> = migrated
        .index_definitions()
        .into_iter()
        .map(|definition| definition.name)
        .collect();
    assert!(names.contains(&"landings_source_message_id".to_string()));
    assert!(names.contains(&"landings_domain".to_string()));
    drop(migrated);

    // ...and the unique index actually enforces on the migrated store.
    let mut migrated = BicDb::open_with_config(
        &target,
        DbConfig::default().with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let duplicate = Record::new("id-new").with_metadata(json!({
        "source_message_id": "src-000000",
        "normalized_domain": "d0.example",
    }));
    assert!(
        migrated.insert("landings", duplicate).is_err(),
        "the migrated unique index must still reject a duplicate"
    );
}

#[test]
fn migration_refuses_unsafe_targets() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    seed_source(source_dir.path(), 10);

    // Same directory.
    assert!(migrate_storage_mode(
        source_dir.path(),
        source_dir.path(),
        StorageMode::ServerPaged,
        DEFAULT_MIGRATION_BATCH
    )
    .is_err());

    // Target already holds a database.
    let occupied = target_dir.path().join("occupied");
    BicDb::open_with_config(&occupied, DbConfig::default().with_fsync(false))
        .unwrap()
        .close()
        .unwrap();
    assert!(migrate_storage_mode(
        source_dir.path(),
        &occupied,
        StorageMode::ServerPaged,
        DEFAULT_MIGRATION_BATCH
    )
    .is_err());

    // Migrating to the mode it already has is a no-op error, not a silent copy.
    let same = target_dir.path().join("same");
    assert!(migrate_storage_mode(
        source_dir.path(),
        &same,
        StorageMode::EmbeddedMemory,
        DEFAULT_MIGRATION_BATCH
    )
    .is_err());
}

#[test]
fn round_trip_preserves_renamed_rows_and_collection_security_contracts() {
    use bicdb_core::{CollectionPolicy, MutationPolicy};
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    seed_source(&source, 17);
    let mut db = BicDb::open(&source).unwrap();
    db.rename_collection("landings", "renamed").unwrap();
    db.set_mutation_policy(
        "audit",
        MutationPolicy {
            append_only: true,
            grants_required: true,
            ..Default::default()
        },
    )
    .unwrap();
    db.set_collection_policy("audit", CollectionPolicy::tenant_field("tenant"))
        .unwrap();
    db.set_collection_mesh_sync_enabled("renamed", true)
        .unwrap();
    let expected = db.verification_manifest().unwrap().tables;
    db.close().unwrap();
    let paged = root.path().join("paged");
    let first = migrate_storage_mode(&source, &paged, StorageMode::ServerPaged, 3).unwrap();
    assert_eq!(first.records, 18);
    assert_eq!(first.verified_records, 18);
    let returned = root.path().join("returned");
    let second = migrate_storage_mode(&paged, &returned, StorageMode::EmbeddedMemory, 2).unwrap();
    assert_eq!(second.source_mode, "server_paged");
    assert_eq!(second.verified_records, 18);
    let db = BicDb::open(&returned).unwrap();
    assert_eq!(expected, db.verification_manifest().unwrap().tables);
    let audit = db
        .collections()
        .into_iter()
        .find(|meta| meta.name == "audit")
        .unwrap();
    assert!(audit.mutation_policy.unwrap().append_only);
    assert_eq!(audit.policy.unwrap().tenant_field, "tenant");
    let renamed = db
        .collections()
        .into_iter()
        .find(|meta| meta.name == "renamed")
        .unwrap();
    assert!(renamed.mesh_sync_enabled);
    assert_eq!(db.index_definitions().len(), 2);
}

#[test]
fn migration_refuses_live_source_and_does_not_claim_flashback_history_copy() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    seed_source(&source, 2);
    let mut db = BicDb::open(&source).unwrap();
    let target = root.path().join("target");
    assert!(migrate_storage_mode(&source, &target, StorageMode::ServerPaged, 1).is_err());
    assert!(!target.exists());
    db.enable_flashback("audit", 3600).unwrap();
    db.close().unwrap();
    let error = migrate_storage_mode(&source, &target, StorageMode::ServerPaged, 1).unwrap_err();
    assert!(error.to_string().contains("history/SCN"));
    assert!(!target.exists());
}

#[test]
fn empty_vector_dimension_survives_copy() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let mut db = BicDb::open(&source).unwrap();
    db.create_collection("vectors").unwrap();
    db.insert(
        "vectors",
        Record::new("first").with_vector(vec![1.0, 2.0, 3.0]),
    )
    .unwrap();
    db.delete("vectors", "first").unwrap();
    assert_eq!(db.collections()[0].vector_dim, Some(3));
    db.close().unwrap();
    let target = root.path().join("target");
    let report = migrate_storage_mode(&source, &target, StorageMode::ServerPaged, 1).unwrap();
    assert_eq!(report.verified_records, 0);
    let db = BicDb::open_with_config(
        &target,
        DbConfig::default().with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    assert_eq!(db.collections()[0].vector_dim, Some(3));
}
