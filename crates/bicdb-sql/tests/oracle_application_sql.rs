//! Application SQL shapes under the Oracle profile: schema-local collection
//! functions, character patterns over joined columns, Oracle DROP options,
//! statement-level unique checks and driver metadata settings.
use bicdb_core::BicDb;
use bicdb_sql::oracle::{ensure_database_profile, lower_session_sql, ORACLE_PREVIEW_PROFILE};
use bicdb_sql::{SqlSession, SqlValue};

fn database(profile: &str) -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    ensure_database_profile(&mut db, profile).unwrap();
    (root, db)
}

fn app_database() -> (tempfile::TempDir, BicDb) {
    let (root, mut db) = database(ORACLE_PREVIEW_PROFILE);
    SqlSession::new(&mut db)
        .execute("CREATE SCHEMA \"APP\"")
        .unwrap();
    (root, db)
}

fn run(db: &mut BicDb, sql: &str) -> bicdb_sql::Result<Vec<Vec<SqlValue>>> {
    let lowered = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, Some("\"APP\""), "app")?;
    SqlSession::new(db)
        .execute(&lowered)
        .map(|result| result.rows)
}

/// Statements as the database owner in the default schema.
fn run_owner(db: &mut BicDb, sql: &str) -> bicdb_sql::Result<Vec<Vec<SqlValue>>> {
    let lowered = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
    SqlSession::new(db)
        .execute(&lowered)
        .map(|result| result.rows)
}

fn texts(rows: Vec<Vec<SqlValue>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    SqlValue::Null => "<null>".to_string(),
                    other => other.to_cell(),
                })
                .collect()
        })
        .collect()
}

#[test]
fn schema_local_collection_functions_feed_multiset_operators() {
    let (_root, mut db) = app_database();
    for source in [
        "CREATE TYPE TABLE_OF_VARCHAR2 AS TABLE OF VARCHAR2(4000)",
        "CREATE OR REPLACE FUNCTION toTableOfVarchar2(p_list IN VARCHAR2)
      RETURN TABLE_OF_VARCHAR2 DETERMINISTIC
    AS
      l_tab TABLE_OF_VARCHAR2 := TABLE_OF_VARCHAR2();
    BEGIN
      FOR i IN
      ( select trim('\"' from REGEXP_SUBSTR(p_list, '(\".*?\"|.*?)(,|$)', 1, level, NULL, 1)) split
      from dual
      connect by level<=length(regexp_replace(p_list,'\".*?\"|[^,]*'))+1)
      LOOP
       l_tab.EXTEND;
       l_tab(l_tab.COUNT) := i.split;
     END LOOP;
     RETURN l_tab;
   END toTableOfVarchar2;",
        "CREATE or REPLACE FUNCTION isIntersectCSV( p_csv1 IN VARCHAR2 , p_csv2 IN VARCHAR2)
RETURN CHAR AS
BEGIN
   IF toTableOfVarchar2(p_csv1) MULTISET INTERSECT toTableOfVarchar2(p_csv2) IS NOT EMPTY
   THEN
       RETURN 'Y';
   ELSE
       RETURN 'N';
   END IF;
END;",
        "CREATE TABLE accounts (id NUMBER(10) PRIMARY KEY, scopes VARCHAR2(200))",
        "INSERT INTO accounts VALUES (1, 'Document,EMail')",
        "INSERT INTO accounts VALUES (2, 'Document')",
    ] {
        run(&mut db, source).unwrap_or_else(|error| panic!("{source}: {error}"));
    }
    assert_eq!(
        texts(
            run(
                &mut db,
                "SELECT id FROM accounts WHERE isIntersectCSV(scopes, 'EMail') = 'Y' ORDER BY id"
            )
            .unwrap()
        ),
        vec![vec!["1"]]
    );
    assert_eq!(
        texts(run(&mut db, "SELECT isIntersectCSV('a,b', 'c') FROM DUAL").unwrap()),
        vec![vec!["N"]]
    );
    // As drivers send it: quoted, schema-resolved names.
    assert_eq!(
        texts(
            run(
                &mut db,
                "SELECT \"ID\" FROM \"APP\".\"ACCOUNTS\" AS \"ACCOUNTS\" \
                 WHERE \"ISINTERSECTCSV\"(\"SCOPES\", 'EMail') = 'Y' ORDER BY \"ID\""
            )
            .unwrap()
        ),
        vec![vec!["1"]]
    );
}

/// PostgreSQL-only PL/pgSQL forms (`=` assignment, RAISE WARNING) and
/// data-modifying functions in a query's select list keep the Oracle
/// profile's behaviour: a function that modifies data cannot be called from
/// a query over a table (Oracle raises ORA-14551), and `x = 1;` is not an
/// assignment.
#[test]
fn postgres_dialect_forms_do_not_apply_to_the_oracle_profile() {
    let (_root, mut db) = app_database();
    for source in [
        "CREATE TABLE audit_rows (id NUMBER)",
        "CREATE TABLE audit_sources (id NUMBER)",
        "INSERT INTO audit_sources VALUES (1)",
        "CREATE OR REPLACE FUNCTION note_row(p_id IN NUMBER) RETURN NUMBER AS
         BEGIN
           INSERT INTO audit_rows VALUES (p_id);
           RETURN p_id;
         END;",
    ] {
        run(&mut db, source).unwrap();
    }
    assert!(run(&mut db, "SELECT note_row(id) FROM audit_sources").is_err());
    assert_eq!(
        texts(run(&mut db, "SELECT COUNT(*) FROM audit_rows").unwrap()),
        vec![vec!["0".to_string()]]
    );
    assert!(run(
        &mut db,
        "CREATE OR REPLACE FUNCTION equals_form RETURN NUMBER AS
           n NUMBER;
         BEGIN
           n = 1;
           RETURN n;
         END;"
    )
    .and_then(|_| run(&mut db, "SELECT equals_form FROM dual"))
    .is_err());
}

#[test]
fn character_patterns_over_joined_column_functions() {
    let (_root, mut db) = app_database();
    for source in [
        "CREATE TABLE t_table (t_id NUMBER(10) PRIMARY KEY, name VARCHAR2(40))",
        "CREATE TABLE t_column (c_id NUMBER(10) PRIMARY KEY, t_id NUMBER(10), ColumnName VARCHAR2(30), IsKey CHAR(1))",
        "INSERT INTO t_table VALUES (1, 'Orders')",
        "INSERT INTO t_column VALUES (10, 1, 'DocumentNo', 'N')",
        "INSERT INTO t_column VALUES (11, 1, 'Name', 'N')",
        "INSERT INTO t_column VALUES (12, 1, 'Orders_ID', 'Y')",
    ] {
        run(&mut db, source).unwrap_or_else(|error| panic!("{source}: {error}"));
    }
    assert_eq!(
        texts(
            run(
                &mut db,
                "SELECT c.ColumnName FROM t_column c INNER JOIN t_table t ON (c.t_id=t.t_id) \
                 WHERE t.t_id=1 AND (c.IsKey='Y' OR Upper(c.ColumnName) IN ('NAME') \
                 OR Upper(c.ColumnName) Like '%DOCUMENT%') ORDER BY c.c_id"
            )
            .unwrap()
        ),
        vec![vec!["DocumentNo"], vec!["Name"], vec!["Orders_ID"]]
    );
    // Character functions of columns describe as text, so later passes (bind
    // typing, protocol Describe) resolve them too.
    assert_eq!(
        bicdb_sql::infer_query_result_types(
            &db,
            "SELECT upper(\"C\".\"COLUMNNAME\"), lower(\"C\".\"COLUMNNAME\") FROM \"APP\".\"T_COLUMN\" \"C\""
        )
        .unwrap(),
        Some(vec![Some("text".to_string()), Some("text".to_string())])
    );
    assert_eq!(
        texts(
            run(
                &mut db,
                "SELECT COUNT(*) FROM (SELECT c.c_id FROM t_column c LEFT OUTER JOIN t_table t \
                 ON (c.t_id=t.t_id) WHERE Upper(c.ColumnName) Like UPPER('%name%')) a"
            )
            .unwrap()
        ),
        vec![vec!["1"]]
    );
}

#[test]
fn drop_table_cascade_constraints_removes_referencing_foreign_keys() {
    let (_root, mut db) = database(ORACLE_PREVIEW_PROFILE);
    for source in [
        "CREATE TABLE parent_t (id NUMBER(10) PRIMARY KEY)",
        "CREATE TABLE child_t (id NUMBER(10) PRIMARY KEY, parent_id NUMBER(10), \
         CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent_t (id))",
        "INSERT INTO parent_t VALUES (1)",
        "INSERT INTO child_t VALUES (1, 1)",
    ] {
        run_owner(&mut db, source).unwrap_or_else(|error| panic!("{source}: {error}"));
    }
    assert!(run_owner(&mut db, "DROP TABLE parent_t").is_err());
    run_owner(&mut db, "DROP TABLE parent_t CASCADE CONSTRAINTS").unwrap();
    // The referencing table and its rows stay; only the constraint is gone.
    run_owner(&mut db, "INSERT INTO child_t VALUES (2, 99)").unwrap();
    assert_eq!(
        texts(run_owner(&mut db, "SELECT COUNT(*) FROM child_t").unwrap()),
        vec![vec!["2"]]
    );
    run_owner(&mut db, "CREATE TABLE gone_t (id NUMBER(10))").unwrap();
    run_owner(&mut db, "DROP TABLE gone_t CASCADE CONSTRAINTS").unwrap();
    assert!(run_owner(&mut db, "SELECT COUNT(*) FROM gone_t").is_err());
}

/// RAW values in Oracle routines keep the profile's conversion rules: the
/// PostgreSQL bytea typing of routine variables does not apply, and RAW
/// concatenation still requires an explicit conversion.
#[test]
fn raw_routine_parameters_keep_oracle_profile_conversions() {
    let (_root, mut db) = app_database();
    let created = run(
        &mut db,
        "CREATE OR REPLACE FUNCTION raw_pair(p IN RAW) RETURN VARCHAR2 AS
         BEGIN
           RETURN p || p;
         END;",
    );
    let error = created
        .and_then(|_| run(&mut db, "SELECT raw_pair(HEXTORAW('0102')) FROM dual"))
        .unwrap_err();
    assert!(
        error.to_string().contains("Oracle concatenation requires"),
        "{error}"
    );
}

#[test]
fn unique_constraints_are_checked_when_the_statement_ends() {
    let (_root, mut db) = app_database();
    for source in [
        "CREATE TABLE entries (id NUMBER(10) PRIMARY KEY, owner_id NUMBER(10), seq NUMBER(10))",
        "CREATE UNIQUE INDEX entries_unique ON entries (owner_id, seq)",
        // Inserted out of sequence order, so rows are visited 3, 2.
        "INSERT INTO entries VALUES (1, 7, 3)",
        "INSERT INTO entries VALUES (2, 7, 2)",
        "INSERT INTO entries VALUES (3, 7, 1)",
        "DELETE FROM entries WHERE id = 3",
    ] {
        run(&mut db, source).unwrap_or_else(|error| panic!("{source}: {error}"));
    }
    run(
        &mut db,
        "UPDATE entries SET seq = seq - 1 WHERE owner_id = 7 AND seq > 1",
    )
    .unwrap();
    assert_eq!(
        texts(run(&mut db, "SELECT id, seq FROM entries ORDER BY seq").unwrap()),
        vec![vec!["2", "1"], vec!["1", "2"]]
    );
    // A real duplicate at the end of the statement still fails, atomically.
    let error = run(&mut db, "UPDATE entries SET seq = 5 WHERE owner_id = 7").unwrap_err();
    assert!(error.to_string().contains("ORA-00001"), "{error}");
    assert_eq!(
        texts(run(&mut db, "SELECT id, seq FROM entries ORDER BY seq").unwrap()),
        vec![vec!["2", "1"], vec!["1", "2"]]
    );
}

#[test]
fn driver_metadata_presets_are_published() {
    for profile in ["postgres", ORACLE_PREVIEW_PROFILE] {
        let (_root, mut db) = database(profile);
        let rows = SqlSession::new(&mut db)
            .execute("SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'")
            .unwrap()
            .rows;
        assert_eq!(texts(rows), vec![vec!["32"]], "{profile}");
    }
}

#[test]
fn postgres_drop_table_respects_referencing_foreign_keys() {
    let (_root, mut db) = database("postgres");
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE parent_t (id INTEGER PRIMARY KEY)",
        "CREATE TABLE child_t (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent_t (id))",
        "CREATE TABLE self_t (id INTEGER PRIMARY KEY, up INTEGER REFERENCES self_t (id))",
        "INSERT INTO parent_t VALUES (1)",
        "INSERT INTO child_t VALUES (1, 1)",
    ] {
        session.execute(sql).unwrap();
    }
    let error = session.execute("DROP TABLE parent_t").unwrap_err();
    assert_eq!(error.sqlstate(), "2BP01", "{error}");
    // Dropping the referencing table in the same statement is allowed, and a
    // self-reference never blocks.
    session.execute("DROP TABLE self_t").unwrap();
    session
        .execute("CREATE TABLE other_t (id INTEGER PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE other_child (id INTEGER REFERENCES other_t (id))")
        .unwrap();
    session.execute("DROP TABLE other_t, other_child").unwrap();
    session.execute("DROP TABLE parent_t CASCADE").unwrap();
    session
        .execute("INSERT INTO child_t VALUES (2, 99)")
        .unwrap();
    assert_eq!(
        texts(
            session
                .execute("SELECT COUNT(*) FROM child_t")
                .unwrap()
                .rows
        ),
        vec![vec!["2"]]
    );
}

#[test]
fn stored_functions_apply_to_aggregate_results_across_joins() {
    let (_root, mut db) = app_database();
    for source in [
        "CREATE OR REPLACE FUNCTION daysBetween
(
    p_date1     DATE,
    p_date2     DATE
)
 	RETURN NUMBER
AS
BEGIN
    RETURN (TRUNC(p_date1) - TRUNC(p_date2));
END;",
        "CREATE TABLE runs (run_id NUMBER(10) PRIMARY KEY, run_date DATE)",
        "CREATE TABLE run_entries (entry_id NUMBER(10) PRIMARY KEY, run_id NUMBER(10))",
        "CREATE TABLE run_lines (line_id NUMBER(10) PRIMARY KEY, entry_id NUMBER(10), invoice_id NUMBER(10))",
        "INSERT INTO runs VALUES (1, TO_DATE('2024-01-01','YYYY-MM-DD'))",
        "INSERT INTO runs VALUES (2, TO_DATE('2024-01-11','YYYY-MM-DD'))",
        "INSERT INTO run_entries VALUES (10, 1)",
        "INSERT INTO run_lines VALUES (100, 10, 7)",
    ] {
        run(&mut db, source).unwrap_or_else(|error| panic!("{source}: {error}"));
    }
    let sql = "SELECT COUNT(*), COALESCE(DAYSBETWEEN(MAX(dr2.run_date), MAX(dr.run_date)),0) \
               FROM runs dr2, runs dr \
               INNER JOIN run_entries dre ON (dr.run_id=dre.run_id) \
               INNER JOIN run_lines drl ON (dre.entry_id=drl.entry_id) \
               WHERE dr2.run_id=2 AND drl.invoice_id=7";
    assert_eq!(texts(run(&mut db, sql).unwrap()), vec![vec!["1", "10"]]);
    let none = sql.replace("invoice_id=7", "invoice_id=8");
    assert_eq!(texts(run(&mut db, &none).unwrap()), vec![vec!["0", "0"]]);
}

/// An unqualified foreign-key target names a table in the application
/// schema, like every other relation reference. It was left unqualified, so
/// table-level and ALTER-added keys failed the REFERENCES gate against a
/// nonexistent public relation ("permission denied for table parent_t") and
/// column-level keys could not find the target to expand its primary key.
#[test]
fn unqualified_foreign_key_targets_resolve_in_the_application_schema() {
    let (_root, mut db) = app_database();
    run(
        &mut db,
        "CREATE TABLE parent_t (id NUMBER(10) PRIMARY KEY, uu VARCHAR2(36) UNIQUE)",
    )
    .unwrap();
    run(&mut db, "INSERT INTO parent_t (id, uu) VALUES (1, 'a')").unwrap();
    run(
        &mut db,
        "CREATE TABLE child_t (id NUMBER(10) PRIMARY KEY, parent_id NUMBER(10), \
         CONSTRAINT fk FOREIGN KEY (parent_id) REFERENCES parent_t (id))",
    )
    .unwrap();
    run(
        &mut db,
        "CREATE TABLE column_child (id NUMBER(10) PRIMARY KEY, parent_id NUMBER(10) REFERENCES parent_t)",
    )
    .unwrap();
    run(
        &mut db,
        "CREATE TABLE altered_child (id NUMBER(10) PRIMARY KEY, parent_id NUMBER(10))",
    )
    .unwrap();
    run(
        &mut db,
        "ALTER TABLE \"APP\".\"ALTERED_CHILD\" ADD CONSTRAINT \"ALTERED_FK\" FOREIGN KEY (\"PARENT_ID\") \
         REFERENCES \"PARENT_T\"(\"ID\") ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED",
    )
    .unwrap();
    for table in ["child_t", "column_child"] {
        run(
            &mut db,
            &format!("INSERT INTO {table} (id, parent_id) VALUES (1, 1)"),
        )
        .unwrap();
        assert!(
            run(
                &mut db,
                &format!("INSERT INTO {table} (id, parent_id) VALUES (2, 99)")
            )
            .is_err(),
            "{table} must enforce its key against APP.PARENT_T"
        );
    }
    // The ALTER-added key acts on APP.PARENT_T: deleting the parent row
    // applies its ON DELETE SET NULL.
    run(&mut db, "INSERT INTO parent_t (id, uu) VALUES (2, 'b')").unwrap();
    run(
        &mut db,
        "INSERT INTO altered_child (id, parent_id) VALUES (1, 2)",
    )
    .unwrap();
    run(&mut db, "DELETE FROM parent_t WHERE id = 2").unwrap();
    assert_eq!(
        texts(run(&mut db, "SELECT parent_id FROM altered_child WHERE id = 1").unwrap()),
        vec![vec!["<null>".to_string()]]
    );
}

/// A missing relation in the application schema is reported as missing. With
/// a WHERE clause the privilege gate re-parsed the already schema-encoded
/// physical name as SQL and failed with the reserved `__bicdb_s_` prefix
/// error instead (seen for unsupported dictionary views such as
/// USER_TAB_COLUMNS).
#[test]
fn missing_application_relations_are_not_reported_as_reserved_names() {
    let (_root, mut db) = app_database();
    for (relation, sql) in [
        (
            "user_tab_columns",
            "SELECT column_name FROM user_tab_columns WHERE table_name = 'AD_REF_TABLE'",
        ),
        ("no_such_table", "SELECT x FROM no_such_table WHERE y = 'A'"),
    ] {
        let error = run(&mut db, sql).unwrap_err().to_string();
        assert!(!error.contains("__bicdb_s_"), "{sql}: {error}");
        assert!(
            error.to_ascii_lowercase().contains(relation),
            "{sql}: {error}"
        );
    }
}

/// `TOP` is not row-limit syntax in this profile either: a column named
/// `top` is an ordinary identifier.
#[test]
fn column_named_top_is_an_ordinary_identifier() {
    let (_root, mut db) = database(ORACLE_PREVIEW_PROFILE);
    run_owner(
        &mut db,
        "CREATE TABLE ranking (id NUMBER PRIMARY KEY, top NUMBER)",
    )
    .unwrap();
    run_owner(&mut db, "INSERT INTO ranking VALUES (1, 5)").unwrap();
    run_owner(&mut db, "INSERT INTO ranking VALUES (2, 7)").unwrap();
    assert_eq!(
        texts(run_owner(&mut db, "SELECT top FROM ranking ORDER BY id").unwrap()),
        vec![vec!["5".to_string()], vec!["7".to_string()]]
    );
    assert_eq!(
        texts(run_owner(&mut db, "SELECT DISTINCT top FROM ranking WHERE id = 2").unwrap()),
        vec![vec!["7".to_string()]]
    );
    assert!(run_owner(&mut db, "SELECT TOP 1 id FROM ranking").is_err());
}

/// The DROP SCHEMA statement of the PostgreSQL profile is not part of this
/// profile's DDL; it stays refused.
#[test]
fn drop_schema_stays_unsupported() {
    let (_root, mut db) = app_database();
    let error = SqlSession::new(&mut db)
        .execute("DROP SCHEMA \"APP\"")
        .unwrap_err();
    assert!(format!("{error}").contains("not supported"), "{error}");
}

/// Standalone Oracle functions are not overloaded: CREATE OR REPLACE with a
/// different signature replaces the function, as before.
#[test]
fn oracle_standalone_functions_are_replaced_not_overloaded() {
    let (_root, mut db) = app_database();
    run(
        &mut db,
        "CREATE OR REPLACE FUNCTION pick(p_a IN NUMBER) RETURN NUMBER AS
         BEGIN
           RETURN p_a;
         END;",
    )
    .unwrap();
    run(
        &mut db,
        "CREATE OR REPLACE FUNCTION pick(p_a IN NUMBER, p_b IN NUMBER) RETURN NUMBER AS
         BEGIN
           RETURN p_a + p_b;
         END;",
    )
    .unwrap();
    assert_eq!(
        texts(run(&mut db, "SELECT pick(1, 2) FROM dual").unwrap()),
        vec![vec!["3".to_string()]]
    );
    assert!(run(&mut db, "SELECT pick(1) FROM dual").is_err());
}
