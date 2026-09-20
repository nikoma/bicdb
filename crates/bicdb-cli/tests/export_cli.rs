use std::{fs, process::Command};

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;

#[test]
fn export_streams_restartable_primary_key_ordered_json_lines() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("events").unwrap();
    for id in ["c", "a", "b"] {
        db.insert(
            "events",
            Record::new(id).with_metadata(json!({"value": id})),
        )
        .unwrap();
    }
    db.flush().unwrap();
    db.close().unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "export",
            root.path().to_str().unwrap(),
            "--collection",
            "events",
            "--after-id",
            "a",
            "--batch-rows",
            "1",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], "b");
    assert_eq!(rows[1]["id"], "c");

    let encoded_output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "export",
            root.path().to_str().unwrap(),
            "--collection",
            "events",
            "--after-id-hex",
            &hex::encode("a"),
            "--batch-rows",
            "1",
        ])
        .output()
        .unwrap();
    assert!(encoded_output.status.success());
    let encoded_rows = String::from_utf8(encoded_output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(encoded_rows, rows);
}

#[test]
fn export_opens_server_paged_database_in_its_recorded_storage_mode() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(false),
    )
    .unwrap();
    db.create_collection("events").unwrap();
    db.insert(
        "events",
        Record::new("paged").with_metadata(json!({"value": "retained"})),
    )
    .unwrap();
    db.flush().unwrap();
    db.close().unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "export",
            root.path().to_str().unwrap(),
            "--collection",
            "events",
            "--batch-rows",
            "1",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let row: serde_json::Value = serde_json::from_slice(output.stdout.trim_ascii()).unwrap();
    assert_eq!(row["id"], "paged");
    assert_eq!(row["metadata"]["value"], "retained");
}

#[test]
fn locality_export_preserves_primary_key_order_and_resume_cursor() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(false),
    )
    .unwrap();
    db.create_collection("events").unwrap();
    for id in ["d", "a", "c", "b"] {
        db.insert(
            "events",
            Record::new(id).with_metadata(json!({"value": id})),
        )
        .unwrap();
    }
    db.flush().unwrap();
    db.close().unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "export",
            root.path().to_str().unwrap(),
            "--collection",
            "events",
            "--after-id",
            "a",
            "--batch-rows",
            "2",
            "--locality",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let ids = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, ["b", "c", "d"]);
}

#[test]
fn export_does_not_load_unrelated_secondary_indexes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(false),
    )
    .unwrap();
    db.create_collection("events").unwrap();
    db.insert(
        "events",
        Record::new("recoverable").with_metadata(json!({"value": "retained"})),
    )
    .unwrap();
    db.create_index(IndexDefinition {
        name: "events_value".to_string(),
        collection: "events".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["value".to_string()])],
        kind: IndexKind::BTree,
        unique: false,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db.flush().unwrap();
    db.close().unwrap();

    // A bounded primary-record export is also a recovery primitive. It must
    // not decode or reconstruct unrelated secondary-index or CDC state before
    // it can emit collection rows.
    fs::write(root.path().join("indexes.json"), b"not valid json").unwrap();
    fs::write(root.path().join("sync.log"), b"not a valid sync log").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "export",
            root.path().to_str().unwrap(),
            "--collection",
            "events",
            "--batch-rows",
            "1",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let row: serde_json::Value = serde_json::from_slice(output.stdout.trim_ascii()).unwrap();
    assert_eq!(row["id"], "recoverable");
}
