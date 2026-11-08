//! SQL-level resolution of a commit reported with SQLSTATE 08007: the error
//! DETAIL carries the identity, and `bicdb_commit_outcome` on the reopened
//! database answers definitively. See docs/durability-failure-handling.md.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_page::test_faults::arm_io_failure;
use bicdb_sql::{SqlEngine, SqlErrorField, SqlSession, SqlValue};

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn identity(detail: &str) -> (String, u64) {
    let mut incarnation = None;
    let mut sequence = None;
    for part in detail.split_whitespace() {
        if let Some(value) = part.strip_prefix("incarnation=") {
            incarnation = Some(value.to_string());
        }
        if let Some(value) = part.strip_prefix("commit_seq=") {
            sequence = value.parse().ok();
        }
    }
    (incarnation.unwrap(), sequence.unwrap())
}

#[test]
fn an_08007_commit_resolves_through_sql_after_reopen() {
    for (faults, kept) in [
        (vec!["transaction_log.write"], false),
        (vec!["transaction_log.sync", "transaction_log.repair"], true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (incarnation, sequence);
        {
            let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
            let mut session = SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE t (id int4 PRIMARY KEY); INSERT INTO t VALUES (1)")
                .unwrap();
            let _faults: Vec<_> = faults
                .iter()
                .map(|site| arm_io_failure(site, dir.path(), 1, false))
                .collect();
            let error = session.execute("INSERT INTO t VALUES (2)").unwrap_err();
            assert_eq!(error.sqlstate(), "08007", "{faults:?}: {error}");
            let fields = error.fields();
            let detail = fields
                .iter()
                .find_map(|field| match field {
                    SqlErrorField::Detail(detail) => Some(detail.clone()),
                    _ => None,
                })
                .expect("08007 carries its identity in DETAIL");
            assert!(fields.iter().any(|field| matches!(field,
                SqlErrorField::Hint(hint) if hint.contains("bicdb_commit_outcome"))));
            (incarnation, sequence) = identity(&detail);
            // Later statements are refused with 58030.
            let refused = session.execute("INSERT INTO t VALUES (3)").unwrap_err();
            assert_eq!(refused.sqlstate(), "58030", "{refused}");
        }
        let db = BicDb::open_with_config(dir.path(), config()).unwrap();
        let engine = SqlEngine::new(&db);
        let answer = engine
            .execute(&format!(
                "SELECT bicdb_commit_outcome('{incarnation}', {sequence})"
            ))
            .unwrap();
        let expected = if kept { "committed" } else { "not_committed" };
        assert_eq!(
            answer.rows,
            vec![vec![SqlValue::String(expected.into())]],
            "{faults:?}"
        );
        let rows = engine.execute("SELECT id FROM t WHERE id = 2").unwrap();
        assert_eq!(rows.rows.len(), usize::from(kept), "{faults:?}");
    }
}
