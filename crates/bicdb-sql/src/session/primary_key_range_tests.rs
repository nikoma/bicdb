//! Ranges and MIN/MAX over a single NUMERIC/NUMBER primary key on a reopened
//! server-paged table. The key lives only in the record id (its `_pkey`
//! index is unreadable), and after a reopen the table is lazy: nothing is
//! resident. Those shapes must read the matching ids from one seek of the id
//! key space, not enumerate every id and fetch every row (~850 ms for a
//! 20-row BETWEEN, ~1.4 s for MIN on a 21k-row wide table).
use super::*;

fn run(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> Result<SqlResult> {
    if profile == "postgres" {
        return session.execute(sql);
    }
    let sql = crate::oracle::lower_session_sql(session.db_ref(), sql, profile, None, "bicdb")?;
    session.execute(&sql)
}

struct Profiled {
    rows: Vec<Vec<String>>,
    full_scans: usize,
    prefix_scans: usize,
    materialized: usize,
    walks: u64,
}

fn profiled(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> Profiled {
    let walks_before = bicdb_core::paged_live_count_walks_on_this_thread();
    crate::SQL_PROFILE_STACK
        .with(|stack| stack.borrow_mut().push(crate::SqlProfileStats::default()));
    let result = run(session, profile, sql);
    let stats = crate::SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    let rows = result
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|value| value.to_cell()).collect())
        .collect();
    Profiled {
        rows,
        full_scans: stats.full_scan_count,
        prefix_scans: stats.record_id_prefix_scans,
        materialized: stats.rows_materialized,
        walks: bicdb_core::paged_live_count_walks_on_this_thread() - walks_before,
    }
}

fn assert_bounded(profile: &str, sql: &str, result: &Profiled) {
    assert_eq!(result.full_scans, 0, "{profile}: `{sql}` scanned the table");
    assert_eq!(
        result.prefix_scans, 0,
        "{profile}: `{sql}` enumerated every record id"
    );
    assert_eq!(result.walks, 0, "{profile}: `{sql}` walked every row chain");
    assert!(
        result.materialized <= 64,
        "{profile}: `{sql}` materialized {} rows",
        result.materialized
    );
}

#[test]
fn key_ranges_and_extremes_on_reopened_paged_tables_read_only_the_range() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let root = tempfile::tempdir().unwrap();
        let config = bicdb_core::DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(bicdb_core::StorageMode::ServerPaged);
        let key = if profile == "postgres" {
            "NUMERIC"
        } else {
            "NUMBER"
        };
        {
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
            let mut session = SqlSession::new(&mut db);
            run(
                &mut session,
                profile,
                &format!(
                    "CREATE TABLE wide_rows (wide_rows_id {key} PRIMARY KEY, label VARCHAR(400))"
                ),
            )
            .unwrap();
            let padding = "x".repeat(300);
            let ids = (100..2100).collect::<Vec<i64>>();
            for chunk in ids.chunks(200) {
                let values = chunk
                    .iter()
                    .map(|id| format!("({id}, 'L{id}{padding}')"))
                    .collect::<Vec<_>>()
                    .join(",");
                run(
                    &mut session,
                    profile,
                    &format!("INSERT INTO wide_rows VALUES {values}"),
                )
                .unwrap();
            }
            // Stale keys at both ends and inside a range: the id key space
            // still holds them, so every candidate must resolve visibility.
            run(
                &mut session,
                profile,
                "DELETE FROM wide_rows WHERE wide_rows_id IN (100, 105, 2099)",
            )
            .unwrap();
            drop(session);
            db.close().unwrap();
        }
        // Reopen: the collection is lazy, the state of every long-lived
        // server-paged database.
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
        let mut session = SqlSession::new(&mut db);
        for (sql, expected) in [
            (
                "SELECT COUNT(*) FROM wide_rows WHERE wide_rows_id BETWEEN 100 AND 119",
                vec![vec!["18".to_string()]],
            ),
            (
                "SELECT COUNT(*) FROM wide_rows WHERE wide_rows_id > 100 AND wide_rows_id < 120",
                vec![vec!["18".to_string()]],
            ),
            (
                "SELECT COUNT(*) FROM wide_rows WHERE wide_rows_id >= 101 AND wide_rows_id <= 101.5",
                vec![vec!["1".to_string()]],
            ),
            (
                "SELECT wide_rows_id FROM wide_rows WHERE wide_rows_id >= 2095 ORDER BY wide_rows_id",
                (2095..2099).map(|id| vec![id.to_string()]).collect(),
            ),
            (
                "SELECT wide_rows_id FROM wide_rows WHERE wide_rows_id < 103 ORDER BY wide_rows_id",
                vec![vec!["101".to_string()], vec!["102".to_string()]],
            ),
            (
                "SELECT COUNT(*) FROM wide_rows WHERE wide_rows_id > 5000",
                vec![vec!["0".to_string()]],
            ),
            ("SELECT MIN(wide_rows_id) FROM wide_rows", vec![vec!["101".to_string()]]),
            ("SELECT MAX(wide_rows_id) FROM wide_rows", vec![vec!["2098".to_string()]]),
        ] {
            let result = profiled(&mut session, profile, sql);
            assert_eq!(result.rows, expected, "{profile}: {sql}");
            assert_bounded(profile, sql, &result);
        }
        // Read-your-writes: pending keys outside the committed range and a
        // pending delete of the committed extreme.
        session.execute("BEGIN").unwrap();
        run(
            &mut session,
            profile,
            "INSERT INTO wide_rows VALUES (50, 'pending')",
        )
        .unwrap();
        run(
            &mut session,
            profile,
            "DELETE FROM wide_rows WHERE wide_rows_id = 2098",
        )
        .unwrap();
        for (sql, expected) in [
            ("SELECT MIN(wide_rows_id) FROM wide_rows", "50"),
            ("SELECT MAX(wide_rows_id) FROM wide_rows", "2097"),
            (
                "SELECT COUNT(*) FROM wide_rows WHERE wide_rows_id BETWEEN 40 AND 101",
                "2",
            ),
        ] {
            let result = profiled(&mut session, profile, sql);
            assert_eq!(
                result.rows,
                vec![vec![expected.to_string()]],
                "{profile} in transaction: {sql}"
            );
        }
        session.execute("ROLLBACK").unwrap();
        let result = profiled(
            &mut session,
            profile,
            "SELECT MIN(wide_rows_id) FROM wide_rows",
        );
        assert_eq!(result.rows, vec![vec!["101".to_string()]], "{profile}");
    }
}
