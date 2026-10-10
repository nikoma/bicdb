//! Database branches: an exact copy of a live database, and a diff against it.
use bicdb_core::{diff_databases, BicDb, DbConfig, Record, RecordChangeKind, StorageMode};
use serde_json::json;

fn open(path: &std::path::Path, storage: StorageMode) -> BicDb {
    BicDb::open_with_config(
        path,
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(storage),
    )
    .unwrap()
}

fn put(db: &BicDb, collection: &str, id: &str, value: i64) {
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        collection,
        Record::new(id).with_metadata(json!({ "value": value })),
    )
    .unwrap();
    tx.commit().unwrap();
}

#[test]
fn a_branch_is_an_exact_independent_copy_and_diffs_against_its_source() {
    for storage in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut main = open(&root.path().join("main"), storage.clone());
        main.create_collection("items").unwrap();
        main.create_collection("dropped_later").unwrap();
        // More records than one diff batch.
        let mut tx = main.begin_transaction().unwrap();
        for index in 0..3000 {
            tx.insert(
                "items",
                Record::new(format!("k{index:05}")).with_metadata(json!({ "value": index })),
            )
            .unwrap();
        }
        tx.commit().unwrap();
        put(&main, "dropped_later", "x", 1);

        let branch_path = root.path().join("feature");
        let commit_seq = main.create_branch(&branch_path).unwrap();
        assert_eq!(commit_seq, main.last_applied_commit_seq(), "{storage:?}");
        assert!(main.create_branch(&branch_path).is_err(), "{storage:?}");

        // Both evolve independently.
        put(&main, "items", "main-only", 1);
        let mut branch = open(&branch_path, storage.clone());
        assert_eq!(
            branch.scan_collection("items").unwrap().len(),
            3000,
            "{storage:?}"
        );
        put(&branch, "items", "k00007", -7);
        put(&branch, "items", "zz-new", 1);
        branch.delete("items", "k00010").unwrap();
        branch.create_collection("branch_only").unwrap();
        put(&branch, "branch_only", "b", 1);
        branch.drop_collection("dropped_later").unwrap();
        assert!(
            main.get("items", "zz-new").unwrap().is_none(),
            "{storage:?}"
        );

        let diff = diff_databases(&branch, &main, 100).unwrap();
        let items = diff
            .collections
            .iter()
            .find(|collection| collection.collection == "items")
            .unwrap();
        assert_eq!(
            (items.added, items.removed, items.changed),
            (1, 2, 1),
            "{storage:?}"
        );
        let only_branch = diff
            .collections
            .iter()
            .find(|collection| collection.collection == "branch_only")
            .unwrap();
        assert!(
            only_branch.only_in_compared && only_branch.added == 1,
            "{storage:?}"
        );
        let dropped = diff
            .collections
            .iter()
            .find(|collection| collection.collection == "dropped_later")
            .unwrap();
        assert!(dropped.only_in_base && dropped.removed == 1, "{storage:?}");
        let changes = diff
            .changes
            .iter()
            .filter(|change| change.collection == "items")
            .map(|change| (change.id.as_str(), change.change))
            .collect::<Vec<_>>();
        assert_eq!(
            changes,
            vec![
                ("k00007", RecordChangeKind::Changed),
                ("k00010", RecordChangeKind::Removed),
                ("main-only", RecordChangeKind::Removed),
                ("zz-new", RecordChangeKind::Added),
            ],
            "{storage:?}"
        );
        // Identical databases have no differences.
        assert!(diff_databases(&main, &main, 10)
            .unwrap()
            .collections
            .is_empty());
    }
}
