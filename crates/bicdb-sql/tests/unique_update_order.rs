//! PG16 controls: immediate uniqueness observes earlier row replacements,
//! while Oracle's statement-end policy remains covered independently.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

#[test]
fn native_unique_updates_vacate_only_preceding_rows_and_roll_back_errors() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for pending in [false, true] {
            for (values, assignment, succeeds) in [
                ([0, 1, 2, 3, 4], "seq=seq-1 WHERE seq>2", true),
                ([4, 3, 2, 1, 0], "seq=seq-1 WHERE seq>2", false),
                ([0, 1, 2, 3, 4], "seq=4-seq", false),
                ([0, 1, 2, 3, 4], "seq=0", false),
            ] {
                let root = tempfile::tempdir().unwrap();
                let mut db = BicDb::open_with_config(
                    root.path(),
                    DbConfig::default()
                        .with_fsync(false)
                        .with_storage_mode(mode.clone()),
                )
                .unwrap();
                let mut sql = SqlSession::new(&mut db);
                sql.execute(
                    "CREATE TABLE entries(owner numeric(10,0),seq numeric(10,0),UNIQUE(owner,seq))",
                )
                .unwrap();
                if pending {
                    sql.execute("BEGIN").unwrap();
                }
                for v in values {
                    sql.execute(&format!("INSERT INTO entries VALUES(7,{v})"))
                        .unwrap();
                }
                sql.execute("INSERT INTO entries VALUES(8,3)").unwrap();
                sql.execute("DELETE FROM entries WHERE owner=7 AND seq=2")
                    .unwrap();
                if !pending {
                    sql.execute("BEGIN").unwrap();
                }
                sql.execute("SAVEPOINT before_shift").unwrap();
                let clause = if assignment.contains("WHERE") {
                    assignment.replace("WHERE seq", "WHERE owner=7 AND seq")
                } else {
                    format!("{assignment} WHERE owner=7")
                };
                let result = sql.execute(&format!("UPDATE entries SET {clause}"));
                if succeeds {
                    result.unwrap();
                } else {
                    assert_eq!(
                        result.unwrap_err().sqlstate(),
                        "23505",
                        "{mode:?}, pending={pending}, {assignment}"
                    );
                    sql.execute("ROLLBACK TO before_shift").unwrap();
                }
                let expected = if succeeds { [0, 1, 2, 3] } else { [0, 1, 3, 4] };
                assert_eq!(
                    sql.execute("SELECT seq FROM entries WHERE owner=7 ORDER BY seq")
                        .unwrap()
                        .rows
                        .into_iter()
                        .map(|r| r[0].to_cell())
                        .collect::<Vec<_>>(),
                    expected
                        .into_iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    sql.execute("SELECT seq FROM entries WHERE owner=8")
                        .unwrap()
                        .rows[0][0]
                        .to_cell(),
                    "3"
                );
                sql.execute("ROLLBACK").unwrap();
            }
        }
    }
}
