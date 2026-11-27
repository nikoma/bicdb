//! Two-phase commit in the core: PREPARE TRANSACTION keeps a transaction's
//! writes invisible and its locks and unique claims held until COMMIT or
//! ROLLBACK PREPARED, across reopen, checkpoint and a killed process.
//! See docs/postgresql-two-phase-commit.md.

use std::path::Path;
use std::process::Command;

use bicdb_core::{
    BicDb, BicDbError, DbConfig, IndexDefinition, IndexField, IndexKind,
    PreparedTransactionRequester, Record, StorageMode,
};
use serde_json::json;

const MODES: [StorageMode; 2] = [StorageMode::EmbeddedMemory, StorageMode::ServerPaged];

fn config(mode: &StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(mode.clone())
}

fn open(path: &Path, mode: &StorageMode) -> BicDb {
    let mut db = BicDb::open_with_config(path, config(mode)).unwrap();
    db.set_max_prepared_transactions(4).unwrap();
    db
}

fn fresh(path: &Path, mode: &StorageMode) -> BicDb {
    let mut db = open(path, mode);
    db.create_collection("accounts").unwrap();
    let mut seed = db.begin_transaction().unwrap();
    seed.insert("accounts", row("a", 100)).unwrap();
    seed.insert("accounts", row("b", 200)).unwrap();
    seed.commit().unwrap();
    db
}

fn row(id: &str, balance: i64) -> Record {
    Record::new(id).with_metadata(json!({ "balance": balance, "email": format!("{id}@x") }))
}

fn owner() -> PreparedTransactionRequester<'static> {
    PreparedTransactionRequester {
        user: "alice",
        superuser: false,
        database: "bicdb",
    }
}

fn balance(db: &BicDb, id: &str) -> Option<i64> {
    let tx = db.begin_transaction().unwrap();
    let record = tx.get("accounts", id).unwrap();
    record.map(|record| record.metadata["balance"].as_i64().unwrap())
}

fn sqlstate(error: &BicDbError) -> &'static str {
    match error {
        BicDbError::PreparedTransaction { sqlstate, .. } => sqlstate,
        BicDbError::TransactionConflict(_) => "40001",
        other => panic!("unexpected error {other:?}"),
    }
}

/// Prepare an update of `a` and an insert of `c` as `gid`.
fn prepare(db: &BicDb, gid: &str) {
    let mut tx = db.begin_transaction().unwrap();
    tx.update("accounts", row("a", 50)).unwrap();
    tx.insert("accounts", row("c", 7)).unwrap();
    db.prepare_transaction(&mut tx, gid, "alice", "bicdb")
        .unwrap();
    drop(tx);
}

fn other_writer_is_blocked(db: &BicDb, id: &str) {
    let mut other = db.begin_transaction().unwrap();
    let blocked = other
        .update("accounts", row(id, 1))
        .and_then(|()| other.commit());
    assert!(
        matches!(blocked, Err(BicDbError::TransactionConflict(_))),
        "{id} must stay locked by the prepared transaction: {blocked:?}"
    );
}

#[test]
fn disabled_by_default_with_postgres_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config(&StorageMode::EmbeddedMemory)).unwrap();
    db.create_collection("accounts").unwrap();
    assert_eq!(db.max_prepared_transactions(), 0);
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("accounts", row("a", 1)).unwrap();
    let error = db
        .prepare_transaction(&mut tx, "g", "alice", "bicdb")
        .unwrap_err();
    assert_eq!(sqlstate(&error), "55000");
    assert_eq!(error.to_string(), "prepared transactions are disabled");
}

#[test]
fn prepared_writes_stay_invisible_and_locked_until_commit_prepared() {
    for mode in MODES {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh(dir.path(), &mode);
        prepare(&db, "g1");
        assert_eq!(balance(&db, "a"), Some(100));
        assert_eq!(balance(&db, "c"), None);
        other_writer_is_blocked(&db, "a");
        other_writer_is_blocked(&db, "c");
        let listed = db.prepared_transactions().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].gid, "g1");
        assert_eq!(listed[0].owner, "alice");
        db.commit_prepared("g1", owner()).unwrap();
        assert_eq!(balance(&db, "a"), Some(50));
        assert_eq!(balance(&db, "c"), Some(7));
        assert!(db.prepared_transactions().unwrap().is_empty());
        // Locks are gone.
        let mut after = db.begin_transaction().unwrap();
        after.update("accounts", row("a", 51)).unwrap();
        after.commit().unwrap();
        assert_eq!(
            sqlstate(&db.commit_prepared("g1", owner()).unwrap_err()),
            "42704"
        );
    }
}

#[test]
fn rollback_prepared_discards_and_releases() {
    for mode in MODES {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh(dir.path(), &mode);
        prepare(&db, "g1");
        db.rollback_prepared("g1", owner()).unwrap();
        assert_eq!(balance(&db, "a"), Some(100));
        assert_eq!(balance(&db, "c"), None);
        let mut after = db.begin_transaction().unwrap();
        after.update("accounts", row("a", 1)).unwrap();
        after.insert("accounts", row("c", 1)).unwrap();
        after.commit().unwrap();
        assert!(db.prepared_transactions().unwrap().is_empty());
    }
}

#[test]
fn gid_ownership_and_limits_follow_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path(), &StorageMode::EmbeddedMemory);
    prepare(&db, "g1");
    let mut duplicate = db.begin_transaction().unwrap();
    duplicate.update("accounts", row("b", 1)).unwrap();
    let error = db
        .prepare_transaction(&mut duplicate, "g1", "alice", "bicdb")
        .unwrap_err();
    assert_eq!(sqlstate(&error), "42710");
    assert_eq!(
        error.to_string(),
        "transaction identifier \"g1\" is already in use"
    );
    duplicate.rollback().unwrap();

    let long = "x".repeat(200);
    let mut too_long = db.begin_transaction().unwrap();
    let error = db
        .prepare_transaction(&mut too_long, &long, "alice", "bicdb")
        .unwrap_err();
    assert_eq!(sqlstate(&error), "22023");
    too_long.rollback().unwrap();

    let stranger = PreparedTransactionRequester {
        user: "mallory",
        superuser: false,
        database: "bicdb",
    };
    assert_eq!(
        sqlstate(&db.commit_prepared("g1", stranger).unwrap_err()),
        "42501"
    );
    let elsewhere = PreparedTransactionRequester {
        user: "alice",
        superuser: false,
        database: "other",
    };
    assert_eq!(
        sqlstate(&db.rollback_prepared("g1", elsewhere).unwrap_err()),
        "0A000"
    );
    assert_eq!(
        sqlstate(&db.commit_prepared("nope", owner()).unwrap_err()),
        "42704"
    );
    let superuser = PreparedTransactionRequester {
        user: "root",
        superuser: true,
        database: "bicdb",
    };
    db.commit_prepared("g1", superuser).unwrap();
    assert_eq!(balance(&db, "a"), Some(50));
}

#[test]
fn the_limit_is_max_prepared_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = fresh(dir.path(), &StorageMode::EmbeddedMemory);
    db.set_max_prepared_transactions(1).unwrap();
    prepare(&db, "g1");
    let mut second = db.begin_transaction().unwrap();
    second.update("accounts", row("b", 1)).unwrap();
    let error = db
        .prepare_transaction(&mut second, "g2", "alice", "bicdb")
        .unwrap_err();
    assert_eq!(sqlstate(&error), "53200");
    second.rollback().unwrap();
    // The failed PREPARE released nothing it did not take: b is writable.
    let mut third = db.begin_transaction().unwrap();
    third.update("accounts", row("b", 2)).unwrap();
    third.commit().unwrap();
}

#[test]
fn a_prepared_unique_key_excludes_other_writers() {
    for mode in MODES {
        let dir = tempfile::tempdir().unwrap();
        let mut db = fresh(dir.path(), &mode);
        db.create_index(IndexDefinition {
            nulls_not_distinct: false,
            name: "accounts-email".to_string(),
            collection: "accounts".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["email".to_string()])],
            kind: IndexKind::BTree,
            unique: true,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert(
            "accounts",
            Record::new("d").with_metadata(json!({ "balance": 1, "email": "shared@x" })),
        )
        .unwrap();
        db.prepare_transaction(&mut tx, "g1", "alice", "bicdb")
            .unwrap();
        drop(tx);
        let mut rival = db.begin_transaction().unwrap();
        let attempt = rival
            .insert(
                "accounts",
                Record::new("e").with_metadata(json!({ "balance": 2, "email": "shared@x" })),
            )
            .and_then(|()| rival.commit());
        assert!(attempt.is_err(), "rival must not claim the prepared key");
        db.commit_prepared("g1", owner()).unwrap();
        assert_eq!(balance(&db, "d"), Some(1));
        assert_eq!(balance(&db, "e"), None);
    }
}

#[test]
fn prepared_state_survives_reopen_and_checkpoint() {
    for mode in MODES {
        for checkpoint in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            {
                let db = fresh(dir.path(), &mode);
                prepare(&db, "keep");
                prepare_b(&db, "drop");
                if checkpoint {
                    match &mode {
                        StorageMode::EmbeddedMemory => {
                            let mut db = db;
                            let plan = db.checkpoint_begin().unwrap();
                            db.checkpoint_write_segments(&plan).unwrap();
                            db.checkpoint_truncate_wal(plan.keep_from()).unwrap();
                            db.close().unwrap();
                        }
                        _ => {
                            db.checkpoint_for_resume().unwrap();
                            db.close().unwrap();
                        }
                    }
                } else {
                    db.close().unwrap();
                }
            }
            let db = open(dir.path(), &mode);
            let gids = db
                .prepared_transactions()
                .unwrap()
                .into_iter()
                .map(|prepared| prepared.gid)
                .collect::<Vec<_>>();
            assert_eq!(gids, ["drop", "keep"], "{mode:?} checkpoint={checkpoint}");
            assert_eq!(balance(&db, "a"), Some(100));
            assert_eq!(balance(&db, "c"), None);
            other_writer_is_blocked(&db, "a");
            other_writer_is_blocked(&db, "b");
            db.rollback_prepared("drop", owner()).unwrap();
            db.commit_prepared("keep", owner()).unwrap();
            assert_eq!(balance(&db, "a"), Some(50));
            assert_eq!(balance(&db, "b"), Some(200));
            assert_eq!(balance(&db, "c"), Some(7));
            db.close().unwrap();
            // The resolution is durable and nothing comes back.
            let db = open(dir.path(), &mode);
            assert!(db.prepared_transactions().unwrap().is_empty());
            assert_eq!(balance(&db, "a"), Some(50));
            assert_eq!(balance(&db, "c"), Some(7));
            let mut after = db.begin_transaction().unwrap();
            after.update("accounts", row("a", 9)).unwrap();
            after.update("accounts", row("b", 9)).unwrap();
            after.commit().unwrap();
        }
    }
}

fn prepare_b(db: &BicDb, gid: &str) {
    let mut tx = db.begin_transaction().unwrap();
    tx.update("accounts", row("b", 0)).unwrap();
    db.prepare_transaction(&mut tx, gid, "alice", "bicdb")
        .unwrap();
}

/// Child half of the kill test: prepare, optionally commit it, then abort
/// the process without closing the store.
#[test]
fn kill_child() {
    let Ok(dir) = std::env::var("BICDB_2PC_CHILD_DIR") else {
        return;
    };
    let mode = match std::env::var("BICDB_2PC_CHILD_MODE").as_deref() {
        Ok("paged") => StorageMode::ServerPaged,
        _ => StorageMode::EmbeddedMemory,
    };
    let mut db = BicDb::open_with_config(&dir, config(&mode).with_fsync(true)).unwrap();
    db.set_max_prepared_transactions(4).unwrap();
    db.create_collection("accounts").unwrap();
    let mut seed = db.begin_transaction().unwrap();
    seed.insert("accounts", row("a", 100)).unwrap();
    seed.insert("accounts", row("b", 200)).unwrap();
    seed.commit().unwrap();
    prepare(&db, "pending");
    prepare_b(&db, "resolved");
    db.commit_prepared("resolved", owner()).unwrap();
    std::process::abort();
}

#[test]
fn a_killed_process_keeps_prepared_and_resolved_state() {
    for (name, mode) in [
        ("memory", StorageMode::EmbeddedMemory),
        ("paged", StorageMode::ServerPaged),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "kill_child", "--nocapture", "--test-threads=1"])
            .env("BICDB_2PC_CHILD_DIR", dir.path())
            .env("BICDB_2PC_CHILD_MODE", name)
            .status()
            .unwrap();
        assert!(!status.success(), "the child aborts");
        let db = open(dir.path(), &mode);
        let prepared = db.prepared_transactions().unwrap();
        assert_eq!(prepared.len(), 1, "{name}");
        assert_eq!(prepared[0].gid, "pending");
        assert_eq!(balance(&db, "a"), Some(100));
        assert_eq!(
            balance(&db, "b"),
            Some(0),
            "{name}: COMMIT PREPARED is durable"
        );
        assert_eq!(balance(&db, "c"), None);
        other_writer_is_blocked(&db, "a");
        db.commit_prepared("pending", owner()).unwrap();
        assert_eq!(balance(&db, "a"), Some(50));
        assert_eq!(balance(&db, "c"), Some(7));
    }
}

#[test]
fn a_backup_restores_prepared_transactions_and_lists_no_internal_collection() {
    use bicdb_core::{create_backup, restore_backup, BackupCreateOptions, BackupRestoreOptions};
    for mode in MODES {
        let source = tempfile::tempdir().unwrap();
        let restored = tempfile::tempdir().unwrap();
        let archive = tempfile::tempdir().unwrap();
        let archive = archive.path().join("prepared.bicbackup");
        {
            let db = fresh(source.path(), &mode);
            prepare(&db, "kept");
            db.close().unwrap();
        }
        create_backup(
            source.path(),
            &archive,
            BackupCreateOptions {
                passphrase: "prepared-backup-passphrase".to_string(),
                base_backup: None,
            },
        )
        .unwrap();
        restore_backup(
            &archive,
            restored.path(),
            BackupRestoreOptions {
                passphrase: "prepared-backup-passphrase".to_string(),
                force: true,
            },
        )
        .unwrap();
        let db = open(restored.path(), &mode);
        let gids = db
            .prepared_transactions()
            .unwrap()
            .into_iter()
            .map(|prepared| prepared.gid)
            .collect::<Vec<_>>();
        assert_eq!(gids, ["kept"], "{mode:?}");
        assert_eq!(balance(&db, "a"), Some(100));
        other_writer_is_blocked(&db, "a");
        db.commit_prepared("kept", owner()).unwrap();
        assert_eq!(balance(&db, "a"), Some(50));
        assert_eq!(balance(&db, "c"), Some(7));
    }
}

#[test]
fn the_prepare_record_collection_cannot_be_mesh_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = fresh(dir.path(), &StorageMode::EmbeddedMemory);
    let error = db
        .set_collection_mesh_sync_enabled(bicdb_core::PREPARED_XACTS_COLLECTION, true)
        .unwrap_err();
    assert!(matches!(error, BicDbError::Authorization(_)), "{error:?}");
}

#[test]
fn an_unrestorable_prepare_record_never_keeps_the_store_from_opening() {
    for mode in MODES {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = fresh(dir.path(), &mode);
            prepare(&db, "good");
            // A record that cannot be parsed, and one whose table is gone.
            let mut tx = db.begin_transaction().unwrap();
            tx.insert(
                bicdb_core::PREPARED_XACTS_COLLECTION,
                Record::new("garbled").with_metadata(json!({ "format": 1, "gid": 7 })),
            )
            .unwrap();
            tx.commit().unwrap();
            db.close().unwrap();
        }
        let db = open(dir.path(), &mode);
        // The good one is restored with its locks.
        other_writer_is_blocked(&db, "a");
        let superuser = PreparedTransactionRequester {
            user: "root",
            superuser: true,
            database: "bicdb",
        };
        // The unreadable one is visible as unrestorable, never committable.
        let error = db.commit_prepared("garbled", superuser).unwrap_err();
        assert_eq!(sqlstate(&error), "55000", "{mode:?}");
        assert_eq!(
            sqlstate(&db.rollback_prepared("garbled", owner()).unwrap_err()),
            "42501"
        );
        db.rollback_prepared("garbled", superuser).unwrap();
        db.commit_prepared("good", owner()).unwrap();
        assert_eq!(balance(&db, "a"), Some(50));
        db.close().unwrap();
        let db = open(dir.path(), &mode);
        assert!(db.prepared_transactions().unwrap().is_empty());
    }
}
