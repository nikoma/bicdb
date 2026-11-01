//! After a durable decision, a failed apply is an uncertain commit: fence the
//! live handle and recover every collection, never a committed table prefix.
use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, OnlineBackupLimits, Record,
    StorageMode,
};
use serde_json::json;
use std::time::{Duration, Instant};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default().with_fsync(true).with_storage_mode(mode)
}

const COLLECTIONS: [&str; 3] = ["a_accounts", "b_ledger", "c_totals"];

#[test]
fn missing_native_store_is_refused_without_creating_an_empty_replacement() {
    for checkpoint in [false, true] {
        let root = tempfile::tempdir().unwrap();
        {
            let mut db =
                BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
            db.create_collection("kept").unwrap();
            db.insert("kept", Record::new("acknowledged")).unwrap();
            if checkpoint {
                db.checkpoint_for_resume().unwrap();
            }
        }
        std::fs::remove_dir_all(root.path().join("paged")).unwrap();
        let error =
            BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap_err();
        assert!(
            error.to_string().contains("native page store is missing"),
            "{error}"
        );
        assert!(!root.path().join("paged/store.pages").exists());
    }
}

#[test]
fn failed_materialization_child() {
    let Some(root) = std::env::var_os("BICDB_MATERIALIZATION_CHILD") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mode: StorageMode =
        serde_json::from_slice(&std::fs::read(root.join("mode.json")).unwrap()).unwrap();
    let db = BicDb::open_with_config(root.join("db"), config(mode)).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.delete("a_accounts", "kept").unwrap();
    for collection in COLLECTIONS {
        tx.insert(
            collection,
            Record::new("after").with_metadata(json!({"v": 2})),
        )
        .unwrap();
    }
    let failpoint = std::env::var("BICDB_MATERIALIZATION_FAILPOINT").unwrap();
    std::env::set_var(
        &failpoint,
        if failpoint.ends_with("COLLECTION_APPLY") {
            "a_accounts"
        } else {
            "1"
        },
    );
    let error = tx.commit().unwrap_err();
    std::env::remove_var(&failpoint);
    assert!(error.to_string().contains("injected"), "{error}");
    assert!(db.check_recovery_state().is_err());
    assert!(db.get("a_accounts", "after").is_err());
    assert!(db.begin_transaction().is_err());
    assert!(db.stats().is_err());
    assert!(db.checkpoint_for_resume().is_err());
    assert!(db
        .capture_online_backup(OnlineBackupLimits::default())
        .is_err());
    std::fs::write(root.join("fenced"), b"durable decision retained").unwrap();
    loop {
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn crash_after_failed_apply_recovers_the_entire_durable_transaction() {
    for (mode, failpoint) in [
        (StorageMode::EmbeddedMemory, "BICDB_TEST_FAIL_COMMIT_APPLY"),
        (StorageMode::ServerPaged, "BICDB_TEST_FAIL_COMMIT_APPLY"),
        (
            StorageMode::ServerPaged,
            "BICDB_TEST_FAIL_PAGED_COLLECTION_APPLY",
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("mode.json"),
            serde_json::to_vec(&mode).unwrap(),
        )
        .unwrap();
        {
            let mut db =
                BicDb::open_with_config(root.path().join("db"), config(mode.clone())).unwrap();
            for collection in COLLECTIONS {
                db.create_collection(collection).unwrap();
                db.insert(
                    collection,
                    Record::new("kept").with_metadata(json!({"v": 1})),
                )
                .unwrap();
                db.create_index(IndexDefinition {
                    name: format!("{collection}_v"),
                    collection: collection.into(),
                    fields: vec![IndexField::MetadataPath(vec!["v".into()])],
                    kind: IndexKind::BTree,
                    unique: false,
                    predicate: None,
                    exclusion: None,
                })
                .unwrap();
            }
            db.checkpoint_for_resume().unwrap();
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "failed_materialization_child", "--nocapture"])
            .env("BICDB_MATERIALIZATION_CHILD", root.path())
            .env("BICDB_MATERIALIZATION_FAILPOINT", failpoint)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !root.path().join("fenced").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before fencing: {mode:?}/{failpoint}"
            );
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not reach the post-decision crash point");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap(); // No destructor/close can complete the failed apply.
        let db = BicDb::open_with_config(root.path().join("db"), config(mode)).unwrap();
        db.check_recovery_state().unwrap();
        assert!(db.get("a_accounts", "kept").unwrap().is_none());
        for collection in COLLECTIONS {
            assert_eq!(
                db.get(collection, "after").unwrap().unwrap().metadata["v"],
                2
            );
            if collection != "a_accounts" {
                assert_eq!(
                    db.get(collection, "kept").unwrap().unwrap().metadata["v"],
                    1
                );
            }
        }
        assert!(db
            .verify_indexes_consistently()
            .unwrap()
            .iter()
            .all(|i| i.valid));
        let mut next = db.begin_transaction().unwrap();
        next.insert("b_ledger", Record::new("healthy-after-recovery"))
            .unwrap();
        next.commit().unwrap();
        assert!(db
            .get("b_ledger", "healthy-after-recovery")
            .unwrap()
            .is_some());
    }
}
