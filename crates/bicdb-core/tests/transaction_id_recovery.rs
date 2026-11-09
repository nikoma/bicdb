//! Transaction identities returned for acknowledged writes survive WAL cuts.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};

#[test]
fn acknowledged_transaction_ids_do_not_restart_after_checkpoint_and_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_storage_mode(mode.clone())
            .with_fsync(true)
            .with_sync_outbox(false)
            .with_auto_checkpoint_wal_bytes(0);
        let mut maximum = 0;
        for reopen in 0..3 {
            let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
            db.create_collection("rows").unwrap();
            for index in 0..40 {
                let mut tx = db.begin_transaction().unwrap();
                let id = tx.id().0;
                assert!(
                    id > maximum,
                    "{mode:?}, reopen={reopen}: acknowledged id {id} reused below {maximum}"
                );
                tx.insert("rows", Record::new(format!("{reopen}-{index}")))
                    .unwrap();
                tx.commit().unwrap();
                maximum = id;
            }
            db.checkpoint_for_resume().unwrap();
        }
    }
}

#[test]
fn legacy_floor_markers_remain_readable_and_native_floor_is_respected() {
    for (marker, native_floor) in [("7\n", 0), ("7 5\n", 0), ("7 5 1200\n", 1200)] {
        let dir = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(true)
            .with_sync_outbox(false);
        drop(BicDb::open_with_config(dir.path(), config.clone()).unwrap());
        std::fs::write(dir.path().join("commit-seq.floor"), marker).unwrap();
        let db = BicDb::open_with_config(dir.path(), config).unwrap();
        let tx = db.begin_transaction().unwrap();
        assert!(tx.id().0 > native_floor);
        tx.rollback().unwrap();
    }
}

#[test]
fn invalid_or_exhausted_native_floor_refuses_open() {
    for marker in [
        "7 5 invalid\n",
        "7 5 18446744073709551615\n",
        "7 5 9 trailing\n",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(true)
            .with_sync_outbox(false);
        drop(BicDb::open_with_config(dir.path(), config.clone()).unwrap());
        std::fs::write(dir.path().join("commit-seq.floor"), marker).unwrap();
        assert!(matches!(
            BicDb::open_with_config(dir.path(), config),
            Err(bicdb_core::BicDbError::FormatCompatibility(_))
        ));
    }
}

#[test]
#[cfg(unix)]
fn a_crash_on_either_side_of_floor_publication_preserves_acknowledged_ids() {
    use std::os::unix::process::ExitStatusExt;
    for point in [
        "transaction_id_floor.before_publish",
        "transaction_id_floor.after_publish",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db_dir = dir.path().join("db");
        let ledger = dir.path().join("acks");
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "transaction_id_floor_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("BICDB_TXID_DB", &db_dir)
            .env("BICDB_TXID_LEDGER", &ledger)
            .env("BICDB_TEST_CRASH_AT", point)
            .output()
            .unwrap();
        assert_eq!(
            child.status.signal(),
            Some(libc::SIGABRT),
            "expected a real abort at {point}, not an assertion failure: {}",
            String::from_utf8_lossy(&child.stderr)
        );
        let ids: Vec<u64> = std::fs::read_to_string(&ledger)
            .unwrap()
            .lines()
            .map(|id| id.parse().unwrap())
            .collect();
        assert_eq!(
            ids.len(),
            40,
            "all independent acknowledgments must precede crash"
        );
        let maximum = *ids.iter().max().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(
                &db_dir,
                DbConfig::default()
                    .with_storage_mode(StorageMode::ServerPaged)
                    .with_fsync(true)
                    .with_sync_outbox(false),
            )
            .unwrap();
            for id in &ids {
                assert!(
                    db.get("rows", &id.to_string()).unwrap().is_some(),
                    "lost acknowledged tx {id}"
                );
            }
            let tx = db.begin_transaction().unwrap();
            assert!(tx.id().0 > maximum, "reissued acknowledged transaction id");
            tx.rollback().unwrap();
        }
    }
}

#[test]
fn exhausted_allocator_cannot_return_the_same_id_twice() {
    let dir = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fsync(true)
        .with_sync_outbox(false);
    drop(BicDb::open_with_config(dir.path(), config.clone()).unwrap());
    std::fs::write(
        dir.path().join("commit-seq.floor"),
        format!("7 5 {}\n", u64::MAX - 2),
    )
    .unwrap();
    let db = BicDb::open_with_config(dir.path(), config).unwrap();
    let tx = db.begin_transaction().unwrap();
    assert_eq!(tx.id().0, u64::MAX - 1);
    tx.rollback().unwrap();
    assert!(matches!(
        db.begin_transaction(),
        Err(bicdb_core::BicDbError::FormatCompatibility(_))
    ));
}

#[test]
#[ignore = "subprocess helper invoked by the crash-boundary test"]
fn transaction_id_floor_crash_child() {
    use std::io::Write;
    let Some(path) = std::env::var_os("BICDB_TXID_DB") else {
        return;
    };
    let ledger_path = std::env::var_os("BICDB_TXID_LEDGER").unwrap();
    let mut db = BicDb::open_with_config(
        path,
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_fsync(true)
            .with_sync_outbox(false)
            .with_auto_checkpoint_wal_bytes(0),
    )
    .unwrap();
    db.create_collection("rows").unwrap();
    let mut ledger = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&ledger_path)
        .unwrap();
    std::fs::File::open(std::path::Path::new(&ledger_path).parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
    for _ in 0..40 {
        let mut tx = db.begin_transaction().unwrap();
        let id = tx.id().0;
        tx.insert("rows", Record::new(id.to_string())).unwrap();
        tx.commit().unwrap();
        writeln!(ledger, "{id}").unwrap();
        ledger.sync_all().unwrap();
    }
    db.checkpoint_for_resume().unwrap();
    panic!("expected process abort before returning from checkpoint");
}
