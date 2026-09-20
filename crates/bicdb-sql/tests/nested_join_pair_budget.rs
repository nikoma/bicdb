//! Regression: a nested-loop (cartesian / non-equi) join must refuse to
//! enumerate an unbounded number of row pairs. Cross joins fully materialize
//! and the inner loop is O(left x right), so an unbounded product is a memory
//! AND cpu exhaustion vector reachable by any authenticated role.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

fn db() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    (directory, db)
}

#[test]
fn cartesian_bomb_is_refused_not_materialized() {
    let (_d, mut db) = db();
    let mut s = SqlSession::new_unprivileged(&mut db, "mallory");
    // 200^5 = 3.2e11 pairs — the exact shape that OOM-killed the host.
    let err = s
        .execute(
            "SELECT count(*) FROM generate_series(1,200) a, generate_series(1,200) b, \
             generate_series(1,200) c, generate_series(1,200) d, generate_series(1,200) e",
        )
        .expect_err("the cartesian bomb must be refused");
    let msg = format!("{err}");
    assert!(
        msg.contains("nested-loop limit") && msg.contains("row pairs"),
        "expected the pair-budget refusal, got: {msg}"
    );
}

#[test]
fn small_cross_join_still_works() {
    let (_d, mut db) = db();
    let mut s = SqlSession::new_unprivileged(&mut db, "mallory");
    // 100 x 100 = 10_000 pairs, well under the default 100M ceiling.
    let r = s
        .execute("SELECT count(*) FROM generate_series(1,100) a, generate_series(1,100) b")
        .unwrap();
    assert_eq!(format!("{:?}", r.rows[0][0]), "Int(10000)");
}
