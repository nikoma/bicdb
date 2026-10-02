//! Flashback (time travel): durable, SCN-ordered row history.
//!
//! Every scenario runs against both storage engines, because history is
//! stored as ordinary collection rows and must survive each engine's own
//! commit, recovery and checkpoint paths.

use bicdb_core::{
    flashback_history_collection, scn_to_unix_micros, BicDb, BicDbError, DbConfig,
    FlashbackOperation, FlashbackPoint, Record, StorageMode,
};
use serde_json::json;

fn configs() -> Vec<(&'static str, DbConfig)> {
    vec![
        ("embedded", DbConfig::default().with_fsync(false)),
        (
            "paged",
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        ),
    ]
}

fn row(id: &str, balance: i64) -> Record {
    Record::new(id).with_metadata(json!({ "balance": balance }))
}

fn upsert(db: &BicDb, id: &str, balance: i64) {
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("accounts", row(id, balance)).unwrap();
    tx.commit().unwrap();
}

fn delete(db: &BicDb, id: &str) {
    let mut tx = db.begin_transaction().unwrap();
    tx.delete("accounts", id).unwrap();
    tx.commit().unwrap();
}

fn balances(records: &[Record]) -> Vec<(String, i64)> {
    records
        .iter()
        .map(|record| {
            (
                record.id.clone(),
                record.metadata["balance"].as_i64().unwrap(),
            )
        })
        .collect()
}

fn as_of(db: &BicDb, scn: u64) -> Vec<(String, i64)> {
    balances(
        &db.flashback_rows("accounts", FlashbackPoint::Scn(scn))
            .unwrap(),
    )
}

fn pairs(items: &[(&str, i64)]) -> Vec<(String, i64)> {
    items.iter().map(|(id, v)| (id.to_string(), *v)).collect()
}

#[test]
fn as_of_returns_every_committed_state_and_survives_restart() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        let (s0, s1, s2, s3);
        {
            let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
            db.create_collection("accounts").unwrap();
            upsert(&db, "a", 100);
            upsert(&db, "b", 200);

            let enabled = db.enable_flashback("accounts", 0).unwrap();
            assert!(enabled.baseline_complete, "{mode}");
            s0 = db.current_scn().unwrap();
            assert!(s0 >= enabled.since_scn, "{mode}");

            upsert(&db, "a", 150);
            s1 = db.current_scn().unwrap();
            delete(&db, "b");
            s2 = db.current_scn().unwrap();
            upsert(&db, "c", 300);
            upsert(&db, "a", 175);
            s3 = db.current_scn().unwrap();
            assert!(s0 < s1 && s1 < s2 && s2 < s3, "{mode}: SCNs must increase");

            assert_eq!(as_of(&db, s0), pairs(&[("a", 100), ("b", 200)]), "{mode}");
            assert_eq!(as_of(&db, s1), pairs(&[("a", 150), ("b", 200)]), "{mode}");
            assert_eq!(as_of(&db, s2), pairs(&[("a", 150)]), "{mode}");
            assert_eq!(as_of(&db, s3), pairs(&[("a", 175), ("c", 300)]), "{mode}");
            // Untracked collections keep no history.
            db.create_collection("scratch").unwrap();
            assert!(db.flashback_config("scratch").unwrap().is_none(), "{mode}");
        }
        // History is ordinary durable data: it must survive a reopen.
        let db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
        assert_eq!(
            as_of(&db, s0),
            pairs(&[("a", 100), ("b", 200)]),
            "{mode} reopened"
        );
        assert_eq!(as_of(&db, s2), pairs(&[("a", 150)]), "{mode} reopened");
        assert_eq!(
            as_of(&db, s3),
            pairs(&[("a", 175), ("c", 300)]),
            "{mode} reopened"
        );
        // The SCN clock keeps increasing across restarts.
        upsert(&db, "a", 999);
        let s4 = db.current_scn().unwrap();
        assert!(s4 > s3, "{mode}: SCN went backwards after reopen");
        assert_eq!(as_of(&db, s3), pairs(&[("a", 175), ("c", 300)]), "{mode}");
        assert_eq!(as_of(&db, s4), pairs(&[("a", 999), ("c", 300)]), "{mode}");
    }
}

#[test]
fn timestamps_map_to_scns() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
        db.create_collection("accounts").unwrap();
        upsert(&db, "a", 1);
        db.enable_flashback("accounts", 0).unwrap();
        let before = db.current_scn().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        upsert(&db, "a", 2);
        let micros = scn_to_unix_micros(before);
        let rows = db
            .flashback_rows("accounts", FlashbackPoint::TimestampMicros(micros))
            .unwrap();
        assert_eq!(balances(&rows), pairs(&[("a", 1)]), "{mode}");
        let now = db
            .flashback_rows(
                "accounts",
                FlashbackPoint::TimestampMicros(scn_to_unix_micros(db.current_scn().unwrap())),
            )
            .unwrap();
        assert_eq!(balances(&now), pairs(&[("a", 2)]), "{mode}");
    }
}

#[test]
fn versions_between_lists_each_version_with_its_scn_range() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
        db.create_collection("accounts").unwrap();
        upsert(&db, "a", 1);
        db.enable_flashback("accounts", 0).unwrap();
        upsert(&db, "a", 2);
        upsert(&db, "b", 10);
        delete(&db, "a");

        let versions = db.flashback_versions("accounts", None, None).unwrap();
        let summary = versions
            .iter()
            .map(|version| {
                (
                    version.record.id.clone(),
                    version.record.metadata["balance"].as_i64().unwrap(),
                    version.operation,
                    version.start_scn.is_some(),
                    version.end_scn.is_some(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            summary,
            vec![
                (
                    "a".to_string(),
                    1,
                    FlashbackOperation::Baseline,
                    false,
                    true
                ),
                ("a".to_string(), 2, FlashbackOperation::Update, true, true),
                ("a".to_string(), 2, FlashbackOperation::Delete, true, false),
                ("b".to_string(), 10, FlashbackOperation::Insert, true, false),
            ],
            "{mode}"
        );
        // Consecutive versions of a row chain: one ends where the next starts.
        assert_eq!(versions[0].end_scn, versions[1].start_scn, "{mode}");
        assert_eq!(versions[1].end_scn, versions[2].start_scn, "{mode}");

        // A window after the update and before the delete sees only the
        // updated version of `a` (start before the window -> start unknown).
        let window = db.current_scn().unwrap();
        let after = db
            .flashback_versions(
                "accounts",
                Some(FlashbackPoint::Scn(window)),
                Some(FlashbackPoint::Scn(window)),
            )
            .unwrap();
        assert_eq!(after.len(), 2, "{mode}: delete version of a and b's insert");
    }
}

#[test]
fn one_history_row_per_row_per_transaction_and_none_for_rollback() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
        db.create_collection("accounts").unwrap();
        db.enable_flashback("accounts", 0).unwrap();
        let history = flashback_history_collection("accounts");

        let mut tx = db.begin_transaction().unwrap();
        tx.insert("accounts", row("a", 1)).unwrap();
        tx.insert("accounts", row("a", 2)).unwrap();
        tx.insert("accounts", row("a", 3)).unwrap();
        tx.commit().unwrap();
        assert_eq!(db.scan_collection(&history).unwrap().len(), 1, "{mode}");

        let mut tx = db.begin_transaction().unwrap();
        tx.insert("accounts", row("a", 4)).unwrap();
        tx.rollback().unwrap();
        assert_eq!(db.scan_collection(&history).unwrap().len(), 1, "{mode}");

        // Deleting a row that does not exist records nothing.
        let mut tx = db.begin_transaction().unwrap();
        tx.delete("accounts", "missing").unwrap();
        tx.commit().unwrap();
        assert_eq!(db.scan_collection(&history).unwrap().len(), 1, "{mode}");
    }
}

#[test]
fn history_boundaries_are_enforced() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
        db.create_collection("accounts").unwrap();
        upsert(&db, "a", 1);
        let before_enable = db.current_scn().unwrap();
        db.create_collection("plain").unwrap();
        assert!(matches!(
            db.flashback_rows("plain", FlashbackPoint::Scn(before_enable)),
            Err(BicDbError::Flashback(_))
        ));
        db.enable_flashback("accounts", 0).unwrap();
        let too_old = db.flashback_rows("accounts", FlashbackPoint::Scn(before_enable));
        assert!(
            matches!(&too_old, Err(BicDbError::Flashback(message)) if message.contains("snapshot too old")),
            "{mode}: {too_old:?}"
        );
        let future = db.current_scn().unwrap() + 10_000_000;
        assert!(
            matches!(
                db.flashback_rows("accounts", FlashbackPoint::Scn(future)),
                Err(BicDbError::Flashback(_))
            ),
            "{mode}: future SCNs are rejected"
        );
    }
}

#[test]
fn purge_keeps_the_state_at_the_horizon_and_disable_drops_history() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
        db.create_collection("accounts").unwrap();
        upsert(&db, "a", 1);
        upsert(&db, "b", 1);
        db.enable_flashback("accounts", 3600).unwrap();
        upsert(&db, "a", 2);
        delete(&db, "b");
        upsert(&db, "a", 3);
        let horizon = db.current_scn().unwrap();
        upsert(&db, "a", 4);
        let latest = db.current_scn().unwrap();

        let removed = db
            .purge_flashback("accounts", Some(FlashbackPoint::Scn(horizon)))
            .unwrap();
        // a: baseline + two updates before the horizon -> keep only the last;
        // b: baseline + delete before the horizon -> nothing left to answer.
        assert_eq!(removed, 4, "{mode}");
        assert_eq!(as_of(&db, horizon), pairs(&[("a", 3)]), "{mode}");
        assert_eq!(as_of(&db, latest), pairs(&[("a", 4)]), "{mode}");
        assert!(
            db.flashback_rows("accounts", FlashbackPoint::Scn(horizon - 1))
                .is_err(),
            "{mode}: purged range is no longer queryable"
        );
        // Retention-based purge with an hour of retention removes nothing new.
        assert_eq!(db.purge_expired_flashback().unwrap(), 0, "{mode}");

        assert!(db.disable_flashback("accounts").unwrap(), "{mode}");
        assert!(
            db.scan_collection(&flashback_history_collection("accounts"))
                .is_err(),
            "{mode}: history collection is dropped with flashback"
        );
        // Untracked again: writes no longer need the history collection.
        upsert(&db, "a", 5);
    }
}

#[test]
fn dropping_a_tracked_collection_drops_its_history() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
        db.create_collection("accounts").unwrap();
        db.enable_flashback("accounts", 0).unwrap();
        upsert(&db, "a", 1);
        assert!(db.drop_collection("accounts").unwrap(), "{mode}");
        assert!(
            db.scan_collection(&flashback_history_collection("accounts"))
                .is_err(),
            "{mode}"
        );
        db.create_collection("accounts").unwrap();
        upsert(&db, "a", 1);
        assert!(db.flashback_config("accounts").unwrap().is_none(), "{mode}");
    }
}

#[test]
fn history_survives_compaction_and_reopen() {
    for (mode, config) in configs() {
        let dir = tempfile::tempdir().unwrap();
        let (s0, s1);
        {
            let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
            db.create_collection("accounts").unwrap();
            upsert(&db, "a", 1);
            db.enable_flashback("accounts", 0).unwrap();
            s0 = db.current_scn().unwrap();
            upsert(&db, "a", 2);
            delete(&db, "a");
            upsert(&db, "z", 7);
            s1 = db.current_scn().unwrap();
            // Materializes everything into segments and truncates the log, so
            // the reopen below cannot lean on WAL replay.
            db.compact().unwrap();
        }
        let db = BicDb::open_with_config(dir.path(), config).unwrap();
        assert_eq!(as_of(&db, s0), pairs(&[("a", 1)]), "{mode}");
        assert_eq!(as_of(&db, s1), pairs(&[("z", 7)]), "{mode}");
        let versions = db.flashback_versions("accounts", None, None).unwrap();
        assert_eq!(
            versions.len(),
            4,
            "{mode}: baseline, update, delete, insert"
        );
        upsert(&db, "z", 8);
        assert!(db.current_scn().unwrap() > s1, "{mode}");
    }
}
