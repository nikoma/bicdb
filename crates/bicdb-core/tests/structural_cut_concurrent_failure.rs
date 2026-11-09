#![cfg(feature = "experimental-structural-cuts")]

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::{Arc, Barrier, Mutex};

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;

fn config() -> DbConfig {
    DbConfig::default()
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(256 * 1024)
        .with_fsync(true)
        .with_sync_outbox(false)
}

fn rows(db: &BicDb) -> BTreeMap<String, serde_json::Value> {
    db.scan_collection("items")
        .unwrap()
        .into_iter()
        .map(|row| (row.id, row.metadata))
        .collect()
}

/// A page-image failure races four already-prepared logical commits, each
/// larger than the pool. The ledger lives outside the database, fsynced per
/// successful result. Recovery must retain every ack and each uncertain batch
/// must be entirely present or absent, including after a later durable write.
#[test]
fn concurrent_large_commits_stay_atomic_after_an_image_failure() {
    for fail_after in [5, 50, 150] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("db");
        let ledger_path = root.path().join("acknowledged-writers");
        let ack = Arc::new(Mutex::new(std::fs::File::create(&ledger_path).unwrap()));
        let mut db = BicDb::open_with_config(&path, config()).unwrap();
        db.create_collection("items").unwrap();
        let mut seed = db.begin_transaction().unwrap();
        for i in 0..300 {
            seed.insert(
                "items",
                Record::new(format!("acked-{i:05}"))
                    .with_metadata(json!({"seed": i, "blob": "a".repeat(1024)})),
            )
            .unwrap();
        }
        seed.commit().unwrap();
        let acknowledged_before = rows(&db);
        let db = Arc::new(db);
        let barrier = Arc::new(Barrier::new(4));
        let fault =
            bicdb_page::test_faults::arm_io_failure("page_log.write", &path, fail_after, false);
        let results = std::thread::scope(|scope| {
            let workers = (0..4)
                .map(|writer| {
                    let db = db.clone();
                    let barrier = barrier.clone();
                    let ack = ack.clone();
                    scope.spawn(move || {
                        let mut tx = db.begin_transaction().unwrap();
                        for i in 0..500 {
                            tx.insert(
                                "items",
                                Record::new(format!("writer-{writer}-{i:05}")).with_metadata(
                                    json!({"writer": writer, "i": i, "blob": "b".repeat(1024)}),
                                ),
                            )
                            .unwrap();
                        }
                        barrier.wait();
                        let result = tx.commit();
                        if result.is_ok() {
                            let mut file = ack.lock().unwrap();
                            writeln!(file, "{writer}").unwrap();
                            file.sync_all().unwrap();
                        }
                        result.is_ok()
                    })
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(
            fault.fired() > 0,
            "fault {fail_after} did not reach page images"
        );
        assert!(
            results.iter().any(|ok| !ok),
            "image failure acknowledged all commits"
        );
        drop(fault);
        drop(db);
        drop(ack);
        let acknowledged = std::fs::read_to_string(&ledger_path)
            .unwrap()
            .lines()
            .map(|line| line.parse::<usize>().unwrap())
            .collect::<BTreeSet<_>>();
        let mut db = BicDb::open_with_config(&path, config()).unwrap();
        let recovered = rows(&db);
        for (id, value) in &acknowledged_before {
            assert_eq!(
                recovered.get(id),
                Some(value),
                "fault {fail_after}: lost {id}"
            );
        }
        for writer in 0..4 {
            let count = recovered
                .keys()
                .filter(|id| id.starts_with(&format!("writer-{writer}-")))
                .count();
            assert!(
                count == 0 || count == 500,
                "fault {fail_after}: partial writer {writer}: {count}"
            );
            if acknowledged.contains(&writer) {
                assert_eq!(
                    count, 500,
                    "fault {fail_after}: lost acknowledged writer {writer}"
                );
            }
            if count == 500 {
                for i in 0..500 {
                    assert_eq!(
                        recovered[&format!("writer-{writer}-{i:05}")],
                        json!({"writer": writer, "i": i, "blob": "b".repeat(1024)})
                    );
                }
            }
        }
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", Record::new("after-recovery")).unwrap();
        tx.commit().unwrap();
        let expected = rows(&db);
        db.close().unwrap();
        for _ in 0..2 {
            let mut db = BicDb::open_with_config(&path, config()).unwrap();
            assert_eq!(
                rows(&db),
                expected,
                "fault {fail_after}: repeated recovery differs"
            );
            db.close().unwrap();
        }
    }
}
