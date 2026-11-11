use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use bicdb_core::{BicDb, BicDbError, MutationActor, Record, TransactionIsolation};
use serde_json::json;

fn actor(trace: &str) -> MutationActor {
    MutationActor {
        actor_id: "carrier-transaction-test".to_string(),
        roles: BTreeSet::new(),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: "carrier-transaction-test".to_string(),
        originating_resource: None,
        originating_action: Some("test".to_string()),
        trace_id: trace.to_string(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000,
    }
}

fn account(id: &str, balance: i64) -> Record {
    Record::new(id).with_metadata(json!({"balance": balance}))
}

#[test]
fn statement_conflict_checks_are_opt_in_and_keep_commit_validation() {
    for immediate in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            directory.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        db.create_collection("accounts").unwrap();
        db.insert("accounts", account("a", 100)).unwrap();
        let mut reader = db.begin_transaction().unwrap();
        reader.set_snapshot_isolation(true, 0).unwrap();
        if immediate {
            reader.set_statement_write_conflict_check(true).unwrap();
        }
        assert_eq!(
            reader.get("accounts", "a").unwrap().unwrap().metadata["balance"],
            100
        );
        let mut writer = db.begin_transaction().unwrap();
        writer.update("accounts", account("a", 75)).unwrap();
        writer.commit().unwrap();
        // READ COMMITTED repair APIs must not widen a fixed snapshot.
        assert!(reader
            .read_committed_update_record("accounts", "a")
            .unwrap()
            .is_none());
        assert!(reader
            .read_committed_update_stored("accounts", "a")
            .unwrap()
            .is_none());
        assert!(reader
            .read_committed_delete_record("accounts", "a")
            .unwrap()
            .is_none());
        if immediate {
            assert!(matches!(
                reader.update("accounts", account("a", 50)),
                Err(BicDbError::TransactionConflict(_))
            ));
            assert_eq!(reader.write_len(), 0);
            reader.rollback().unwrap();
        } else {
            reader.update("accounts", account("a", 50)).unwrap();
            assert!(matches!(
                reader.commit(),
                Err(BicDbError::TransactionConflict(_))
            ));
        }
        assert_eq!(
            db.get("accounts", "a").unwrap().unwrap().metadata["balance"],
            75
        );
    }
}

#[test]
fn statement_conflict_checks_do_not_buffer_a_partial_batch_or_stale_delete() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    db.create_collection("accounts").unwrap();
    db.insert("accounts", account("a", 100)).unwrap();
    db.insert("accounts", account("b", 100)).unwrap();
    let mut reader = db.begin_transaction().unwrap();
    reader.set_snapshot_isolation(true, 0).unwrap();
    reader.set_statement_write_conflict_check(true).unwrap();
    assert!(reader.get("accounts", "a").unwrap().is_some());
    let mut writer = db.begin_transaction().unwrap();
    writer.delete("accounts", "a").unwrap();
    writer.commit().unwrap();
    assert!(matches!(
        reader.write_upserts_with_statement_snapshots(
            "accounts",
            [(account("b", 50), 0), (account("a", 50), 0)]
        ),
        Err(BicDbError::TransactionConflict(_))
    ));
    assert_eq!(reader.write_len(), 0);
    assert!(matches!(
        reader.delete("accounts", "a"),
        Err(BicDbError::TransactionConflict(_))
    ));
    assert_eq!(reader.write_len(), 0);
    reader.rollback().unwrap();
    assert!(db.get("accounts", "a").unwrap().is_none());
    assert_eq!(
        db.get("accounts", "b").unwrap().unwrap().metadata["balance"],
        100
    );
}

#[test]
fn repeatable_read_keeps_its_fixed_mvcc_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("accounts").unwrap();
    db.insert("accounts", account("a", 100)).unwrap();

    let repeatable = db
        .begin_application_transaction_with_isolation(
            actor("repeatable"),
            TransactionIsolation::RepeatableRead,
        )
        .unwrap();
    assert_eq!(
        repeatable
            .get_authorized("accounts", "a")
            .unwrap()
            .unwrap()
            .metadata["balance"],
        100
    );

    let mut writer = db.begin_transaction().unwrap();
    writer.update("accounts", account("a", 75)).unwrap();
    writer.commit().unwrap();

    assert_eq!(
        repeatable
            .get_authorized("accounts", "a")
            .unwrap()
            .unwrap()
            .metadata["balance"],
        100
    );
    repeatable.commit().unwrap();
    assert_eq!(
        db.get("accounts", "a").unwrap().unwrap().metadata["balance"],
        75
    );
}

#[test]
fn read_committed_refreshes_at_each_host_statement_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("accounts").unwrap();
    db.insert("accounts", account("a", 100)).unwrap();

    let mut read_committed = db
        .begin_application_transaction_with_isolation(
            actor("read-committed"),
            TransactionIsolation::ReadCommitted,
        )
        .unwrap();
    assert_eq!(
        read_committed
            .get_authorized("accounts", "a")
            .unwrap()
            .unwrap()
            .metadata["balance"],
        100
    );
    let mut writer = db.begin_transaction().unwrap();
    writer.update("accounts", account("a", 75)).unwrap();
    writer.commit().unwrap();

    read_committed.refresh_read_committed_snapshot().unwrap();
    assert_eq!(
        read_committed
            .get_authorized("accounts", "a")
            .unwrap()
            .unwrap()
            .metadata["balance"],
        75
    );
    read_committed.commit().unwrap();
}

#[test]
fn serializable_relation_reads_reject_write_skew() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("accounts").unwrap();
    db.insert("accounts", account("a", 100)).unwrap();
    db.insert("accounts", account("b", 100)).unwrap();

    let mut first = db
        .begin_application_transaction_with_isolation(
            actor("serializable-first"),
            TransactionIsolation::Serializable,
        )
        .unwrap();
    let mut second = db
        .begin_application_transaction_with_isolation(
            actor("serializable-second"),
            TransactionIsolation::Serializable,
        )
        .unwrap();
    first.record_serializable_read("accounts").unwrap();
    second.record_serializable_read("accounts").unwrap();
    assert_eq!(
        first.scan_collection_authorized("accounts").unwrap().len(),
        2
    );
    assert_eq!(
        second.scan_collection_authorized("accounts").unwrap().len(),
        2
    );

    first.update("accounts", account("a", 0)).unwrap();
    second.update("accounts", account("b", 0)).unwrap();
    first.commit().unwrap();
    let error = second
        .commit()
        .expect_err("the stale serializable writer must abort");
    assert!(matches!(error, BicDbError::TransactionConflict(_)));
    assert_eq!(
        db.get("accounts", "a").unwrap().unwrap().metadata["balance"],
        0
    );
    assert_eq!(
        db.get("accounts", "b").unwrap().unwrap().metadata["balance"],
        100
    );
}

#[test]
fn postgres_serializable_rejects_write_skew_and_allows_disjoint_writes() {
    use bicdb_core::{CancellationToken, DbConfig, StorageMode};

    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for skew in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(
                directory.path(),
                DbConfig::default().with_storage_mode(mode.clone()),
            )
            .unwrap();
            db.create_collection("accounts").unwrap();
            db.insert("accounts", account("a", 100)).unwrap();
            db.insert("accounts", account("b", 100)).unwrap();
            let mut first = db.begin_transaction().unwrap();
            let mut second = db.begin_transaction().unwrap();
            let cancellation = CancellationToken::uncancelable();
            for tx in [&mut first, &mut second] {
                tx.acquire_postgres_serializable_snapshot(0, false, false, &cancellation)
                    .unwrap();
            }
            let first_read = if skew { "b" } else { "a" };
            let second_read = if skew { "a" } else { "b" };
            assert!(first.get("accounts", first_read).unwrap().is_some());
            assert!(second.get("accounts", second_read).unwrap().is_some());
            first.update("accounts", account("a", 0)).unwrap();
            second.update("accounts", account("b", 0)).unwrap();
            first.commit().unwrap();
            let outcome = second.commit();
            if skew {
                assert!(matches!(outcome, Err(BicDbError::TransactionConflict(_))));
            } else {
                outcome.unwrap();
            }
            assert_eq!(
                db.get("accounts", "b").unwrap().unwrap().metadata["balance"],
                if skew { 100 } else { 0 }
            );
        }
    }
}

#[test]
fn postgres_deferrable_snapshot_releases_admission_and_keeps_its_visibility() {
    use bicdb_core::{BlockingWaitAdmission, CancellationToken, Result};
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::Duration;

    #[derive(Debug)]
    struct WaitObserver(mpsc::SyncSender<()>);

    impl BlockingWaitAdmission for WaitObserver {
        fn suspend(&self) -> bool {
            let _ = self.0.try_send(());
            true
        }

        fn resume(&self, _cancellation: &CancellationToken) -> Result<()> {
            Ok(())
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    db.create_collection("accounts").unwrap();
    db.insert("accounts", account("a", 100)).unwrap();
    db.insert("accounts", account("b", 100)).unwrap();
    let mut writer = db.begin_transaction().unwrap();
    writer
        .acquire_postgres_serializable_snapshot(0, false, false, &CancellationToken::uncancelable())
        .unwrap();
    writer.update("accounts", account("b", 50)).unwrap();

    thread::scope(|scope| {
        let (waiting, observed_wait) = mpsc::sync_channel(1);
        let (peer_committed, observed_commit) = mpsc::sync_channel(1);
        let db = &db;
        let reader = scope.spawn(move || {
            let token = CancellationToken::uncancelable()
                .with_wait_admission(Arc::new(WaitObserver(waiting)))
                .child_with_deadline(std::time::Instant::now() + Duration::from_secs(5));
            let mut tx = db.begin_transaction().unwrap();
            tx.acquire_postgres_serializable_snapshot(0, true, true, &token)
                .unwrap();
            observed_commit
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            assert_eq!(
                tx.get("accounts", "a").unwrap().unwrap().metadata["balance"],
                100
            );
            assert_eq!(
                tx.get("accounts", "b").unwrap().unwrap().metadata["balance"],
                100
            );
            tx.commit().unwrap();
        });
        observed_wait.recv_timeout(Duration::from_secs(5)).unwrap();
        writer.commit().unwrap();
        let mut later = db.begin_transaction().unwrap();
        later.update("accounts", account("a", 50)).unwrap();
        later.commit().unwrap();
        peer_committed.send(()).unwrap();
        reader.join().unwrap();
    });
}
