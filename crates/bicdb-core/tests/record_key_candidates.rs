use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;
use std::collections::BTreeSet;
fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(mode)
}
fn row(id: &str) -> Record {
    Record::new(id).with_metadata(json!({"payload":"x".repeat(32768)}))
}
#[test]
fn key_candidates_resolve_visibility_pending_writes_and_cold_snapshots() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open_with_config(dir.path(), config(mode.clone())).unwrap();
            db.create_collection("rows").unwrap();
            for id in ["a", "b", "c"] {
                db.insert("rows", row(id)).unwrap();
            }
            db.delete("rows", "b").unwrap();
        }
        let mut db = BicDb::open_with_config(dir.path(), config(mode.clone())).unwrap();
        let visible = db
            .scan_collection_record_key_candidates_with_prefix_unchecked("rows", "")
            .unwrap()
            .into_iter()
            .filter(|id| db.get("rows", id).unwrap().is_some())
            .collect::<BTreeSet<_>>();
        assert_eq!(visible, BTreeSet::from(["a".to_string(), "c".to_string()]));
        let mut pending = db.begin_transaction().unwrap();
        pending.delete("rows", "a").unwrap();
        pending.insert("rows", row("d")).unwrap();
        let visible = pending
            .scan_collection_record_key_candidates_with_prefix_unchecked("rows", "")
            .unwrap()
            .into_iter()
            .filter(|id| pending.get("rows", id).unwrap().is_some())
            .collect::<BTreeSet<_>>();
        assert_eq!(visible, BTreeSet::from(["c".to_string(), "d".to_string()]));
        pending.rollback().unwrap();
        let mut reader = db.begin_transaction().unwrap();
        reader.set_snapshot_isolation(true, 0).unwrap();
        db.delete("rows", "a").unwrap();
        db.insert("rows", row("e")).unwrap();
        let visible = reader
            .scan_collection_record_key_candidates_with_prefix_unchecked("rows", "")
            .unwrap()
            .into_iter()
            .filter(|id| reader.get("rows", id).unwrap().is_some())
            .collect::<BTreeSet<_>>();
        assert_eq!(visible, BTreeSet::from(["a".to_string(), "c".to_string()]));
        reader.rollback().unwrap();
        let prefix = db
            .scan_collection_record_key_candidates_with_prefix_unchecked("rows", "e")
            .unwrap();
        assert_eq!(prefix, vec!["e"]);
        drop(db);
        let db = BicDb::open_with_config(dir.path(), config(mode)).unwrap();
        let visible = db
            .scan_collection_record_key_candidates_with_prefix_unchecked("rows", "")
            .unwrap()
            .into_iter()
            .filter(|id| db.get("rows", id).unwrap().is_some())
            .collect::<BTreeSet<_>>();
        assert_eq!(visible, BTreeSet::from(["c".to_string(), "e".to_string()]));
    }
}
