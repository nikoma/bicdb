//! Catalog-only collection renames: rows stay under their storage name, and
//! everything addressed by the logical name follows the rename. Every scenario
//! runs against both storage engines and survives a reopen.

use bicdb_core::{
    flashback_history_collection, BicDb, DbConfig, FlashbackPoint, IndexDefinition, IndexField,
    IndexKind, IndexValue, Record, StorageMode,
};
use serde_json::json;

fn configs() -> Vec<(&'static str, DbConfig)> {
    vec![
        ("embedded", DbConfig::default().with_fsync(false)),
        (
            "paged",
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        ),
    ]
}

fn put(db: &BicDb, collection: &str, id: &str, color: &str) {
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        collection,
        Record::new(id).with_metadata(json!({ "color": color })),
    )
    .unwrap();
    tx.commit().unwrap();
}

fn ids(db: &BicDb, collection: &str) -> Vec<String> {
    let mut ids = db
        .scan_collection(collection)
        .unwrap()
        .into_iter()
        .map(|record| record.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn color_index(collection: &str) -> IndexDefinition {
    IndexDefinition {
        name: format!("{collection}_color"),
        collection: collection.to_string(),
        fields: vec![IndexField::MetadataPath(vec!["color".to_string()])],
        unique: false,
        kind: IndexKind::default(),
        predicate: None,
        exclusion: None,
    }
}

fn by_color(db: &BicDb, index: &str, color: &str) -> Vec<String> {
    let mut ids = db
        .lookup_index(index, &[IndexValue::String(color.to_string())])
        .unwrap();
    ids.sort();
    ids
}

#[test]
fn rename_moves_rows_indexes_and_survives_reopen() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
            db.create_collection("orders").unwrap();
            db.create_index(color_index("orders")).unwrap();
            put(&db, "orders", "a", "red");
            put(&db, "orders", "b", "blue");
            // Unflushed writes must survive the rename and a crash after it.
            db.rename_collection("orders", "archive").unwrap();
            assert!(db.scan_collection("orders").is_err(), "{mode}");
            assert_eq!(ids(&db, "archive"), vec!["a", "b"], "{mode}");
            assert_eq!(by_color(&db, "orders_color", "red"), vec!["a"], "{mode}");
            put(&db, "archive", "c", "red");
            // A new collection may take the old name; it shares nothing.
            db.create_collection("orders").unwrap();
            put(&db, "orders", "z", "green");
            assert_eq!(ids(&db, "orders"), vec!["z"], "{mode}");
            assert_eq!(ids(&db, "archive"), vec!["a", "b", "c"], "{mode}");
        }
        let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
        assert_eq!(ids(&db, "archive"), vec!["a", "b", "c"], "{mode} reopened");
        assert_eq!(ids(&db, "orders"), vec!["z"], "{mode} reopened");
        assert_eq!(
            by_color(&db, "orders_color", "red"),
            vec!["a", "c"],
            "{mode} reopened"
        );
        // Dropping one leaves the other untouched, and names can swap back.
        assert!(db.drop_collection("orders").unwrap(), "{mode}");
        db.rename_collection("archive", "orders").unwrap();
        assert_eq!(ids(&db, "orders"), vec!["a", "b", "c"], "{mode}");
        drop(db);
        let db = BicDb::open_with_config(dir.path(), config).unwrap();
        assert_eq!(
            ids(&db, "orders"),
            vec!["a", "b", "c"],
            "{mode} reopened twice"
        );
    }
}

#[test]
fn flashback_history_follows_a_rename() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
        db.create_collection("ledger").unwrap();
        put(&db, "ledger", "a", "red");
        db.enable_flashback("ledger", 0).unwrap();
        let before = db.current_scn().unwrap();
        put(&db, "ledger", "a", "blue");
        db.rename_collection("ledger", "ledger_2026").unwrap();
        assert!(
            db.scan_collection(&flashback_history_collection("ledger"))
                .is_err(),
            "{mode}"
        );
        let past = db
            .flashback_rows("ledger_2026", FlashbackPoint::Scn(before))
            .unwrap();
        assert_eq!(past[0].metadata["color"], "red", "{mode}");
        // Still tracked under the new name.
        put(&db, "ledger_2026", "a", "green");
        drop(db);
        let db = BicDb::open_with_config(dir.path(), config).unwrap();
        let versions = db.flashback_versions("ledger_2026", None, None).unwrap();
        assert_eq!(versions.len(), 3, "{mode}: baseline + two updates");
    }
}

#[test]
fn rename_refuses_collisions() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
        db.create_collection("a").unwrap();
        db.create_collection("b").unwrap();
        assert!(db.rename_collection("a", "b").is_err(), "{mode}");
        assert!(db.rename_collection("missing", "c").is_err(), "{mode}");
        db.rename_collection("a", "a").unwrap();
    }
}
