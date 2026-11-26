//! The index-only `count(*)` counts B-tree entries instead of rows. It may
//! only answer when the index holds exactly one entry per row visible to the
//! statement; these cases compare it with the row path (`k + 0 = v`, which no
//! index answers) and check that it declines where it cannot know.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::plan_diagnostics::{with_plan_diagnostics, DiagnosticsOptions};
use bicdb_sql::SqlSession;

/// (count, whether the index-only route answered).
fn count(session: &mut SqlSession<'_>, predicate: &str) -> (i64, bool) {
    let sql = format!("SELECT count(*) FROM v WHERE {predicate}");
    let (result, decisions) =
        with_plan_diagnostics(DiagnosticsOptions::default(), || session.execute(&sql));
    let rows = result.unwrap_or_else(|error| panic!("{sql}: {error}")).rows;
    let index_only = decisions
        .iter()
        .any(|decision| decision.candidates[decision.chosen].signature.kind == "IndexOnlyCount");
    (rows[0][0].to_cell().parse().unwrap(), index_only)
}

/// The index-only count and the row path's count of `k = value`.
fn both(session: &mut SqlSession<'_>, value: i64) -> ((i64, bool), i64) {
    let indexed = count(session, &format!("k = {value}"));
    let (scanned, _) = count(session, &format!("k + 0 = {value}"));
    (indexed, scanned)
}

const SETUP: &str = "CREATE TABLE v (id int4 PRIMARY KEY, k int4, payload text);
     INSERT INTO v SELECT g, g % 10, 'p' || g FROM generate_series(1, 1000) g;
     CREATE INDEX v_k ON v (k);";

#[test]
fn counts_follow_updates_deletes_and_concurrent_commits() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db).execute(SETUP).unwrap();
    let db = db;
    let mut reader = SqlSession::new_shared(&db);
    let mut writer = SqlSession::new_shared(&db);
    let ((n, index_only), scanned) = both(&mut reader, 3);
    assert!(index_only, "the route must answer the plain case");
    assert_eq!((n, n), (100, scanned));

    // (3) An UPDATE that keeps the key leaves one entry per row.
    writer
        .execute("UPDATE v SET payload = 'q' WHERE k = 3")
        .unwrap();
    let ((n, _), scanned) = both(&mut reader, 3);
    assert_eq!((n, scanned), (100, 100));

    // (2) An UPDATE that moves the key: the old entry goes, the new one counts.
    writer
        .execute("UPDATE v SET k = 4 WHERE id IN (3, 13, 23)")
        .unwrap();
    assert_eq!(both(&mut reader, 3).1, 97);
    assert_eq!(both(&mut reader, 3).0 .0, 97);
    assert_eq!(both(&mut reader, 4).0 .0, both(&mut reader, 4).1);

    // (1) A delete committed by another session between two read-committed
    // statements of an open transaction is seen by the next statement.
    reader.execute("BEGIN").unwrap();
    let ((before, _), _) = both(&mut reader, 5);
    writer.execute("DELETE FROM v WHERE id IN (5, 15)").unwrap();
    let ((after, index_only), scanned) = both(&mut reader, 5);
    reader.execute("COMMIT").unwrap();
    assert_eq!((before, after, scanned), (100, 98, 98));
    assert!(index_only);

    // A repeatable-read snapshot older than the index's committed state:
    // the route declines and the row path keeps the snapshot's count.
    reader
        .execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .unwrap();
    let ((first, _), _) = both(&mut reader, 6);
    writer.execute("DELETE FROM v WHERE id = 6").unwrap();
    let ((n, index_only), scanned) = both(&mut reader, 6);
    assert!(
        !index_only,
        "a repeatable-read snapshot must not be answered from the index"
    );
    assert_eq!((first, n, scanned), (100, 100, 100));

    // (4) Versions the repeatable-read snapshot keeps alive (dead to new
    // snapshots, not yet swept) are not counted by a fresh statement.
    writer.execute("UPDATE v SET k = 7 WHERE id = 16").unwrap();
    writer.execute("DELETE FROM v WHERE id = 26").unwrap();
    let ((n, index_only), scanned) = both(&mut writer, 6);
    assert!(index_only);
    assert_eq!((n, scanned), (97, 97));
    let ((n, _), scanned) = both(&mut writer, 7);
    assert_eq!(n, scanned);
    reader.execute("COMMIT").unwrap();
    let ((n, _), scanned) = both(&mut reader, 6);
    assert_eq!((n, scanned), (97, 97));

    // The writer's own uncommitted changes: the route declines.
    writer.execute("BEGIN").unwrap();
    writer.execute("DELETE FROM v WHERE k = 8").unwrap();
    let ((n, index_only), scanned) = both(&mut writer, 8);
    writer.execute("ROLLBACK").unwrap();
    assert!(!index_only);
    assert_eq!((n, scanned), (0, 0));
    let ((n, _), scanned) = both(&mut reader, 8);
    assert_eq!((n, scanned), (100, 100));
}

#[test]
fn counts_read_persisted_entries_after_a_paged_reopen() {
    // (5) A lazily paged collection after a reopen keeps no resident rows;
    // the count must come from the persisted index entries.
    let root = tempfile::tempdir().unwrap();
    let config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
    };
    {
        let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute(SETUP).unwrap();
        session
            .execute("UPDATE v SET k = 4 WHERE id IN (3, 13); DELETE FROM v WHERE id = 23")
            .unwrap();
        db.close().unwrap();
    }
    let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let ((n, index_only), scanned) = both(&mut session, 3);
    assert_eq!((n, scanned), (97, 97), "index-only {index_only}");
    assert!(
        index_only,
        "the paged reopen must exercise the index-only route"
    );
    let ((n, _), scanned) = both(&mut session, 4);
    assert_eq!((n, scanned), (102, 102));
    session
        .execute("DELETE FROM v WHERE id = 33; UPDATE v SET k = 3 WHERE id = 14")
        .unwrap();
    let ((n, _), scanned) = both(&mut session, 3);
    assert_eq!((n, scanned), (97, 97));
}
