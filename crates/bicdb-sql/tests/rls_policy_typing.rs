//! Row policies compare values with the column's type, exactly as a WHERE
//! clause on the table does: a `char(n)` policy key ignores trailing
//! padding, numeric keys compare by value. Expected counts were observed on
//! PostgreSQL 16. Each table has an indexed twin (the policy key can drive
//! the index) and both storage modes are covered.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

const MODES: [StorageMode; 2] = [StorageMode::EmbeddedMemory, StorageMode::ServerPaged];

fn open(mode: StorageMode) -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode),
    )
    .unwrap();
    (dir, db)
}

fn exec(sql: &mut SqlSession<'_>, statement: &str) {
    sql.execute(statement)
        .unwrap_or_else(|error| panic!("`{statement}` failed: {error}"));
}

fn count(sql: &mut SqlSession<'_>, statement: &str) -> i64 {
    let result = sql
        .execute(statement)
        .unwrap_or_else(|error| panic!("`{statement}` failed: {error}"));
    match &result.rows[0][0] {
        SqlValue::Int(value) => *value,
        other => panic!("`{statement}`: {other:?}"),
    }
}

/// `<name>_s` (no index) and `<name>_i` (index on the policy column).
fn twins(sql: &mut SqlSession<'_>, name: &str, column_type: &str, values: &str, policy: &str) {
    for variant in ["s", "i"] {
        let table = format!("{name}_{variant}");
        for statement in [
            format!("CREATE TABLE {table} (id INT PRIMARY KEY, org_id {column_type}, number INT)"),
            format!("INSERT INTO {table} VALUES {values}"),
            format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"),
            format!("CREATE POLICY p ON {table} USING ({policy})"),
            format!("GRANT SELECT ON {table} TO reader"),
        ] {
            exec(sql, &statement);
        }
        if variant == "i" {
            exec(
                sql,
                &format!("CREATE INDEX {table}_ix ON {table} (org_id, number)"),
            );
        }
    }
}

fn assert_counts(sql: &mut SqlSession<'_>, mode: &StorageMode, name: &str, cases: &[(&str, i64)]) {
    for (key, expected) in cases {
        exec(sql, &format!("SET app.org = '{key}'"));
        for variant in ["s", "i"] {
            for query in [
                format!("SELECT count(*) FROM {name}_{variant}"),
                format!("SELECT count(*) FROM {name}_{variant} WHERE number = 1"),
            ] {
                assert_eq!(
                    count(sql, &query),
                    *expected,
                    "{mode:?} key={key:?} `{query}`"
                );
            }
        }
    }
}

#[test]
fn character_policy_keys_ignore_trailing_padding() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        exec(&mut sql, "CREATE ROLE reader LOGIN");
        let rows = "(1,'ab',1),(2,'ab  ',1),(3,'x',1),(4,NULL,1)";
        // char(4) = text: the padded value compares as its trimmed text.
        twins(
            &mut sql,
            "bp",
            "char(4)",
            rows,
            "org_id = (SELECT current_setting('app.org'))",
        );
        twins(
            &mut sql,
            "bd",
            "char(4)",
            rows,
            "org_id = current_setting('app.org')",
        );
        // char(4) = char: both sides padded.
        twins(
            &mut sql,
            "bc",
            "char(4)",
            rows,
            "org_id = (SELECT current_setting('app.org')::char(4))",
        );
        exec(&mut sql, "SET ROLE reader");
        for name in ["bp", "bd"] {
            assert_counts(
                &mut sql,
                &mode,
                name,
                &[("ab", 2), ("ab ", 0), ("x", 1), ("zz", 0)],
            );
        }
        assert_counts(&mut sql, &mode, "bc", &[("ab", 2), ("ab ", 2), ("x", 1)]);
        exec(&mut sql, "RESET ROLE");
    }
}

#[test]
fn numeric_policy_keys_compare_by_value() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        exec(&mut sql, "CREATE ROLE reader LOGIN");
        twins(
            &mut sql,
            "nm",
            "numeric",
            "(1,1,1),(2,1.0,1),(3,1.50,1),(4,2,1),(5,1.5,1)",
            "org_id = (SELECT current_setting('app.org')::numeric)",
        );
        exec(&mut sql, "SET ROLE reader");
        assert_counts(
            &mut sql,
            &mode,
            "nm",
            &[("1", 2), ("1.000", 2), ("1.5", 2), ("3", 0)],
        );
        exec(&mut sql, "RESET ROLE");
    }
}
