//! A commit that has returned is visible to the next transaction of every
//! other session, even while commits with lower sequences are still applying.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;
use std::sync::{mpsc, Arc};

#[test]
fn acknowledged_commits_are_visible_to_new_transactions_elsewhere() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_storage_mode(mode.clone())
                .with_fsync(false),
        )
        .unwrap();
        db.create_collection("rows").unwrap();
        let db = Arc::new(db);
        let (sent, acknowledged) = mpsc::channel::<String>();
        let writers = (0..8)
            .map(|writer| {
                let db = db.clone();
                let sent = sent.clone();
                std::thread::spawn(move || {
                    for n in 0..300 {
                        let id = format!("w{writer}-{n}");
                        let mut tx = db.begin_transaction().unwrap();
                        tx.insert("rows", Record::new(&id).with_metadata(json!({"n": n})))
                            .unwrap();
                        tx.commit().unwrap();
                        sent.send(id).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        drop(sent);
        let checker = {
            let db = db.clone();
            std::thread::spawn(move || {
                let mut missing = Vec::new();
                for id in acknowledged {
                    let tx = db.begin_transaction().unwrap();
                    if tx.get("rows", &id).unwrap().is_none() {
                        missing.push(id);
                    }
                }
                missing
            })
        };
        for writer in writers {
            writer.join().unwrap();
        }
        let missing = checker.join().unwrap();
        assert!(
            missing.is_empty(),
            "{mode:?}: {} acknowledged rows invisible, e.g. {:?}",
            missing.len(),
            &missing[..missing.len().min(5)]
        );
    }
}
