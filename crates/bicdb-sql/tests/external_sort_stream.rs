//! Full-table ORDER BY through the external merge sort: results must equal
//! the materializing engine's (transaction-forced oracle) under a spill
//! budget tiny enough that every query merges many runs.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

const ROWS: usize = 5_000;
static SPILL_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn external_sort_matches_the_materializing_oracle_across_shapes() {
    let _env_guard = SPILL_ENV_LOCK.lock().unwrap();
    let spill_dir = tempfile::tempdir().unwrap();
    std::env::set_var("BICDB_SPILL_DIR", spill_dir.path());

    let dir = tempfile::tempdir().unwrap();
    let config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_query_work_memory_bytes(64 * 1024)
            .with_query_temp_space_bytes(64 * 1024 * 1024)
            .with_query_merge_fan_in(4)
    };
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE s (id TEXT PRIMARY KEY, n INT, t TEXT, f DOUBLE PRECISION)")
            .unwrap();
        for chunk in (0..ROWS).collect::<Vec<_>>().chunks(500) {
            let values = chunk
                .iter()
                .map(|index| {
                    if index % 11 == 0 {
                        format!("('k{index:05}', NULL, NULL, NULL)")
                    } else {
                        format!(
                            "('k{index:05}', {}, 'w{:04}', {}.25)",
                            (index * 37) % 1000,
                            (index * 13) % 3000,
                            (index * 7) % 500,
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.execute(&format!("INSERT INTO s VALUES {values}"))
                .unwrap();
        }
    }
    db.close().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);

    let queries = [
        "SELECT id FROM s ORDER BY n",
        "SELECT id FROM s ORDER BY n DESC",
        "SELECT id FROM s ORDER BY n DESC NULLS LAST",
        "SELECT id FROM s ORDER BY n ASC NULLS FIRST",
        "SELECT id, n FROM s ORDER BY t, n DESC",
        "SELECT id FROM s WHERE n % 3 = 0 ORDER BY n DESC, id",
        "SELECT id FROM s ORDER BY n LIMIT 17 OFFSET 5",
        "SELECT id, t FROM s ORDER BY f, id DESC",
    ];
    let mut streamed = Vec::new();
    for query in queries {
        let result = sql
            .execute(query)
            .unwrap_or_else(|error| panic!("`{query}` failed: {error}"));
        streamed.push(result.rows);
    }

    // Oracle pass: a transaction forces every streaming path to decline.
    sql.execute("BEGIN").unwrap();
    for (query, streamed_rows) in queries.iter().zip(&streamed) {
        let oracle = sql
            .execute(query)
            .unwrap_or_else(|error| panic!("oracle `{query}` failed: {error}"));
        assert_eq!(
            &oracle.rows, streamed_rows,
            "external sort diverged from the materializing sort for `{query}`"
        );
    }
    sql.execute("COMMIT").unwrap();

    // The spill directory must be clean again: run files are removed.
    let leftovers: Vec<_> = std::fs::read_dir(spill_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name())
        .collect();
    assert!(leftovers.is_empty(), "leftover spill files: {leftovers:?}");
}

#[test]
fn external_sort_resource_limits_fail_closed_without_leaking_files() {
    let _env_guard = SPILL_ENV_LOCK.lock().unwrap();
    let spill_dir = tempfile::tempdir().unwrap();
    std::env::set_var("BICDB_SPILL_DIR", spill_dir.path());

    let quota_dir = tempfile::tempdir().unwrap();
    let quota_config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_query_work_memory_bytes(64 * 1024)
            .with_query_temp_space_bytes(64 * 1024)
            .with_query_merge_fan_in(2)
    };
    let mut quota_db = BicDb::open_with_config(quota_dir.path(), quota_config()).unwrap();
    let mut quota_sql = SqlSession::new(&mut quota_db);
    quota_sql
        .execute("CREATE TABLE quota_rows (id TEXT PRIMARY KEY, n INT, body TEXT)")
        .unwrap();
    let body = "q".repeat(4 * 1024);
    for index in 0..100 {
        quota_sql
            .execute(&format!(
                "INSERT INTO quota_rows VALUES ('q{index:03}', {index}, '{body}')"
            ))
            .unwrap();
    }
    drop(quota_sql);
    quota_db.close().unwrap();
    let mut quota_db = BicDb::open_with_config(quota_dir.path(), quota_config()).unwrap();
    let mut quota_sql = SqlSession::new(&mut quota_db);
    let quota_error = match quota_sql.execute("SELECT id, body FROM quota_rows ORDER BY n DESC") {
        Err(error) => error,
        Ok(result) => panic!(
            "temporary quota was bypassed and returned {} rows",
            result.rows.len()
        ),
    };
    assert_eq!(quota_error.sqlstate(), "53100");
    for query in [
        "SELECT DISTINCT body FROM quota_rows",
        "SELECT body, COUNT(*) FROM quota_rows GROUP BY body",
    ] {
        let error = match quota_sql.execute(query) {
            Err(error) => error,
            Ok(result) => panic!(
                "temporary quota was bypassed for `{query}` and returned {} rows",
                result.rows.len()
            ),
        };
        assert_eq!(error.sqlstate(), "53100", "wrong SQLSTATE for `{query}`");
    }
    drop(quota_sql);
    drop(quota_db);

    let row_dir = tempfile::tempdir().unwrap();
    let row_config = || {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_query_work_memory_bytes(64 * 1024)
            .with_query_temp_space_bytes(1024 * 1024)
            .with_query_merge_fan_in(2)
    };
    let mut row_db = BicDb::open_with_config(row_dir.path(), row_config()).unwrap();
    let mut row_sql = SqlSession::new(&mut row_db);
    row_sql
        .execute("CREATE TABLE wide_rows (id TEXT PRIMARY KEY, n INT, body TEXT)")
        .unwrap();
    let oversized = "x".repeat(128 * 1024);
    row_sql
        .execute(&format!(
            "INSERT INTO wide_rows VALUES ('wide', 1, '{oversized}')"
        ))
        .unwrap();
    drop(row_sql);
    row_db.close().unwrap();
    let mut row_db = BicDb::open_with_config(row_dir.path(), row_config()).unwrap();
    let mut row_sql = SqlSession::new(&mut row_db);
    let row_error = match row_sql.execute("SELECT id, body FROM wide_rows ORDER BY n") {
        Err(error) => error,
        Ok(result) => panic!(
            "work-memory limit was bypassed and returned {} rows",
            result.rows.len()
        ),
    };
    assert_eq!(row_error.sqlstate(), "53200");
    drop(row_sql);
    drop(row_db);

    let leftovers: Vec<_> = std::fs::read_dir(spill_dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name())
        .collect();
    assert!(leftovers.is_empty(), "leftover spill files: {leftovers:?}");
}
