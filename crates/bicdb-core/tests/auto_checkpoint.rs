//! Embedded databases fold their transaction log into segments by themselves:
//! a store that rewrites the same large records keeps a bounded log and opens
//! with the latest state, without the application calling compact.

use bicdb_core::{BicDb, DbConfig, Record};
use serde_json::json;

const THRESHOLD: u64 = 256 * 1024;

fn log_bytes(path: &std::path::Path) -> u64 {
    std::fs::metadata(path.join("transactions.log"))
        .map(|meta| meta.len())
        .unwrap_or(0)
}

fn snapshot(generation: usize) -> Record {
    // ~40 KB per write: a whole-mailbox snapshot saved on every change.
    Record::new("mailbox").with_metadata(json!({
        "generation": generation,
        "body": "x".repeat(40 * 1024),
    }))
}

#[test]
fn repeated_large_writes_keep_the_log_bounded_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default().with_auto_checkpoint_wal_bytes(THRESHOLD),
        )
        .unwrap();
        db.create_collection("state").unwrap();
        db.insert("state", Record::new("gone").with_metadata(json!({"v": 1})))
            .unwrap();
        for generation in 0..200 {
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("state", snapshot(generation)).unwrap();
            if generation == 100 {
                tx.delete("state", "gone").unwrap();
            }
            tx.commit().unwrap();
            // At most one write past the threshold before it is folded away.
            assert!(
                log_bytes(dir.path()) < THRESHOLD + 64 * 1024,
                "log {} bytes after write {generation}",
                log_bytes(dir.path())
            );
        }
    }
    let db = BicDb::open(dir.path()).unwrap();
    let latest = db
        .get("state", "mailbox")
        .unwrap()
        .expect("latest snapshot");
    assert_eq!(latest.metadata["generation"], 199);
    assert!(
        db.get("state", "gone").unwrap().is_none(),
        "the delete survives"
    );
}

#[test]
fn a_large_log_left_by_an_older_version_is_folded_at_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default().with_auto_checkpoint_wal_bytes(0),
        )
        .unwrap();
        db.create_collection("state").unwrap();
        for generation in 0..50 {
            db.insert("state", snapshot(generation)).unwrap();
        }
    }
    assert!(
        log_bytes(dir.path()) > 1024 * 1024,
        "disabled: the log grows"
    );
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default().with_auto_checkpoint_wal_bytes(THRESHOLD),
    )
    .unwrap();
    assert!(log_bytes(dir.path()) < THRESHOLD, "folded at open");
    assert_eq!(
        db.get("state", "mailbox").unwrap().unwrap().metadata["generation"],
        49
    );
}

#[test]
fn concurrent_writers_lose_nothing_while_checkpoints_run() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default().with_auto_checkpoint_wal_bytes(THRESHOLD),
        )
        .unwrap();
        db.create_collection("state").unwrap();
        let db_ref: &BicDb = &db;
        std::thread::scope(|scope| {
            for writer in 0..4 {
                scope.spawn(move || {
                    for n in 0..60 {
                        let mut tx = db_ref.begin_transaction().unwrap();
                        tx.insert(
                            "state",
                            Record::new(format!("w{writer}-{n}")).with_metadata(json!({
                                "writer": writer,
                                "n": n,
                                "body": "y".repeat(8 * 1024),
                            })),
                        )
                        .unwrap();
                        tx.commit().unwrap();
                    }
                });
            }
        });
        assert!(log_bytes(dir.path()) < THRESHOLD + 256 * 1024);
        assert_eq!(db.scan_collection("state").unwrap().len(), 240);
    }
    let db = BicDb::open(dir.path()).unwrap();
    assert_eq!(db.scan_collection("state").unwrap().len(), 240);
    for writer in 0..4 {
        for n in 0..60 {
            let record = db.get("state", &format!("w{writer}-{n}")).unwrap();
            assert_eq!(record.expect("committed row").metadata["n"], n);
        }
    }
}
