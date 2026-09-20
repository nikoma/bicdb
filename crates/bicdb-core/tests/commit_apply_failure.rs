//! A commit whose apply fails must neither wedge the database nor come back
//! from the dead.
//!
//! The hazard: `commit_transaction` enqueues the WAL frames BEFORE applying.
//! By the time the apply runs, those frames may already be durable — any
//! concurrent committer's durable write drains the contiguous queue, including
//! ours. So a failed apply used to leave two time bombs:
//!
//! - the contiguous `applied_watermark` never marked the failed seq, so it
//!   stalled forever and every LATER commit stayed invisible to new snapshots
//!   for the rest of the session (the wedge);
//! - recovery replayed the durable Commit frames, applying a transaction whose
//!   client was told it failed (the resurrection).
//!
//! The fix revokes the commit: an `Abort` frame at a fresh seq (recovery's
//! state fold lets a later Abort override an earlier Commit), both seqs marked
//! in the watermark, and the revoke made durable before the error returns.
//!
//! `BICDB_TEST_FAIL_COMMIT_APPLY` is the failpoint; env vars are process-global
//! so everything here runs in ONE test, sequentially.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;
use tempfile::TempDir;

fn insert(db: &BicDb, id: &str) -> bicdb_core::Result<()> {
    let mut tx = db.begin_transaction()?;
    tx.insert("t", Record::new(id).with_metadata(json!({ "id": id })))?;
    tx.commit()
}

#[test]
fn a_failed_apply_is_revoked_not_wedged_and_not_resurrected() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = TempDir::new().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode.clone());
        {
            let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
            db.create_collection("t").unwrap();

            insert(&db, "before").expect("baseline commit");

            std::env::set_var("BICDB_TEST_FAIL_COMMIT_APPLY", "1");
            let failed = insert(&db, "failed");
            std::env::remove_var("BICDB_TEST_FAIL_COMMIT_APPLY");
            assert!(
                failed.is_err(),
                "[{mode}] the injected failure must surface"
            );

            // THE WEDGE: before the fix, the failed seq blocked the contiguous
            // watermark, so this commit would succeed but never become visible.
            insert(&db, "after").expect("commit after the failure");
            assert!(
                db.get("t", "after").unwrap().is_some(),
                "[{mode}] a commit after the failure is invisible: the \
                 watermark is wedged"
            );
            // The wedge specifically stalls SNAPSHOT visibility (plain gets
            // read resident state directly), so a transactional read is the
            // assertion that detects it — and it must run on a FRESH thread:
            // this thread just committed "after", so its per-thread commit
            // floor would lift the snapshot past the wedge and mask it.
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    let mut tx = db.begin_transaction().unwrap();
                    assert!(
                        tx.get("t", "after").unwrap().is_some(),
                        "[{mode}] a fresh snapshot cannot see the post-failure \
                         commit: the applied watermark is wedged"
                    );
                    tx.rollback().unwrap();
                });
            });
            assert!(
                db.get("t", "before").unwrap().is_some(),
                "[{mode}] the baseline row disappeared"
            );
            assert!(
                db.get("t", "failed").unwrap().is_none(),
                "[{mode}] the failed transaction's row is visible live"
            );
            // Crash-shaped exit: no close, so recovery replays the log and
            // must honor the revoke.
            drop(db);
        }

        let db = BicDb::open_with_config(dir.path(), config).unwrap();
        assert!(
            db.get("t", "before").unwrap().is_some(),
            "[{mode}] baseline row lost across reopen"
        );
        assert!(
            db.get("t", "after").unwrap().is_some(),
            "[{mode}] post-failure row lost across reopen"
        );
        // THE RESURRECTION: before the fix, recovery replayed the failed
        // transaction's durable Commit frames.
        assert!(
            db.get("t", "failed").unwrap().is_none(),
            "[{mode}] recovery resurrected a transaction whose client saw an error"
        );
        assert_eq!(db.scan_collection("t").unwrap().len(), 2, "[{mode}]");
    }
}
