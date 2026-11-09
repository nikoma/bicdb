//! Physical unique-key announcements must include native/signed and stored
//! writers, and must not hold the index read latch across a blocking wait.
use bicdb_core::{
    BicDb, CancellationToken, DbConfig, IndexDefinition, IndexField, IndexKind, IndexValue, Record,
    StorageMode, StoredRecord,
};
use serde_json::json;
use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};
fn setup(mode: StorageMode, nulls_not_distinct: bool) -> (tempfile::TempDir, Arc<BicDb>) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode),
    )
    .unwrap();
    db.create_collection("items").unwrap();
    db.create_index(IndexDefinition {
        name: "items_unique".into(),
        collection: "items".into(),
        fields: vec![IndexField::MetadataPath(vec!["k".into()])],
        unique: true,
        nulls_not_distinct,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    (dir, Arc::new(db))
}
fn spawn<T: Send + 'static>(
    db: &Arc<BicDb>,
    run: impl FnOnce() -> T + Send + 'static,
) -> thread::JoinHandle<T> {
    let database = db.clone();
    thread::spawn(move || {
        let result = run();
        drop(database);
        result
    })
}
#[test]
fn parsed_and_stored_native_writers_announce_before_commit_or_rollback() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for stored in [false, true] {
            for commit in [false, true] {
                let (_dir, db) = setup(mode.clone(), false);
                let mut owner = db.begin_transaction().unwrap();
                let proposed = Record::new("winner").with_metadata(json!({"k":"same","v":20}));
                if stored {
                    owner
                        .write_stored_inserts_with_statement_snapshots(
                            "items",
                            [(Arc::new(StoredRecord::from_record(&proposed).unwrap()), 0)],
                        )
                        .unwrap();
                } else {
                    owner.insert("items", proposed).unwrap();
                }
                let waiter = db.begin_transaction().unwrap();
                let (done, received) = mpsc::channel();
                let worker = spawn(&db, move || {
                    let candidate =
                        Record::new("candidate").with_metadata(json!({"k":"same","v":10}));
                    done.send(waiter.reserve_unique_index_record_cancellable(
                        "items_unique",
                        &candidate,
                        &CancellationToken::uncancelable(),
                    ))
                    .unwrap();
                });
                assert!(
                    received.recv_timeout(Duration::from_millis(30)).is_err(),
                    "native pending key was not announced"
                );
                let (committed, receipt) = mpsc::channel();
                let owner_worker = spawn(&db, move || {
                    committed
                        .send(if commit {
                            owner.commit()
                        } else {
                            owner.rollback()
                        })
                        .unwrap();
                });
                // A waiter retaining an index read latch would stop this commit from
                // acquiring the write latch needed to apply its index entry.
                receipt
                    .recv_timeout(Duration::from_secs(5))
                    .expect("index latch blocked its owner")
                    .unwrap();
                received
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap();
                worker.join().unwrap();
                owner_worker.join().unwrap();
                assert_eq!(
                    db.lookup_index_exact("items_unique", &[IndexValue::from("same")])
                        .unwrap(),
                    if commit {
                        vec!["winner".to_string()]
                    } else {
                        vec![]
                    }
                );
            }
        }
    }
}
#[test]
fn null_distinct_keys_are_unreserved_and_nulls_not_distinct_are_announced() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for not_distinct in [false, true] {
            let (_dir, db) = setup(mode.clone(), not_distinct);
            let mut owner = db.begin_transaction().unwrap();
            owner
                .insert(
                    "items",
                    Record::new("winner").with_metadata(json!({"k":null})),
                )
                .unwrap();
            let waiter = db.begin_transaction().unwrap();
            let (done, received) = mpsc::channel();
            let worker = spawn(&db, move || {
                done.send(waiter.reserve_unique_index_record_cancellable(
                    "items_unique",
                    &Record::new("candidate").with_metadata(json!({"k":null})),
                    &CancellationToken::uncancelable(),
                ))
                .unwrap();
            });
            if not_distinct {
                assert!(received.recv_timeout(Duration::from_millis(30)).is_err());
                owner.rollback().unwrap();
                received
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap();
            } else {
                received
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap();
                owner.rollback().unwrap();
            }
            worker.join().unwrap();
        }
    }
}
#[test]
fn savepoint_rollback_releases_native_unique_announcements() {
    let (_dir, db) = setup(StorageMode::ServerPaged, false);
    let mut owner = db.begin_transaction().unwrap();
    let mark = owner.rollback_mark();
    owner
        .insert(
            "items",
            Record::new("winner").with_metadata(json!({"k":"same"})),
        )
        .unwrap();
    let waiter = db.begin_transaction().unwrap();
    let (done, received) = mpsc::channel();
    let worker = spawn(&db, move || {
        done.send(waiter.reserve_unique_index_record_cancellable(
            "items_unique",
            &Record::new("candidate").with_metadata(json!({"k":"same"})),
            &CancellationToken::uncancelable(),
        ))
        .unwrap();
    });
    assert!(received.recv_timeout(Duration::from_millis(30)).is_err());
    owner.rollback_to_mark(mark).unwrap();
    received
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
}
