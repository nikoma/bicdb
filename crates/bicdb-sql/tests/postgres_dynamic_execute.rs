use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn dynamic_command_literals_preserve_quoted_text_and_unicode() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(r#"CREATE FUNCTION quoted_dynamic() RETURNS TEXT LANGUAGE plpgsql AS $$ DECLARE answer TEXT; BEGIN EXECUTE 'SELECT ''quoted''''value:'' || $1::text' INTO answer USING '水'; RETURN answer; END $$"#).unwrap();
    assert_eq!(
        sql.execute("SELECT quoted_dynamic()").unwrap().rows,
        vec![vec![SqlValue::String("quoted'value:水".into())]]
    );
}

fn seeded() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db).execute("CREATE TABLE measurements(id INT PRIMARY KEY,tenant TEXT,value TEXT); INSERT INTO measurements VALUES(1,'a','first'),(2,'a','second'),(3,'b','other')").unwrap();
    (root, db)
}

#[test]
fn execute_into_using_keeps_values_bound_and_does_not_capture_column_names() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION read_dynamic(p_id INT) RETURNS TEXT LANGUAGE plpgsql AS $$ DECLARE value TEXT := 'wrong variable'; answer TEXT; BEGIN EXECUTE format('SELECT value FROM %I WHERE id=$1', 'measurements') INTO answer USING p_id; RETURN answer; END $$").unwrap();
    assert_eq!(
        sql.execute("SELECT read_dynamic(1)").unwrap().rows,
        vec![vec![SqlValue::String("first".into())]]
    );
}

#[test]
fn dynamic_update_reports_row_count_without_changing_found() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION update_dynamic(p_tenant TEXT,p_value TEXT) RETURNS BIGINT LANGUAGE plpgsql AS $$ DECLARE n BIGINT; BEGIN PERFORM 1 WHERE false; EXECUTE 'UPDATE measurements SET value=$1 WHERE tenant=$2' USING p_value,p_tenant; GET DIAGNOSTICS n = ROW_COUNT; IF FOUND THEN RAISE EXCEPTION 'EXECUTE changed FOUND'; END IF; RETURN n; END $$").unwrap();
    let value = "quote ' ; DELETE FROM measurements; --";
    assert_eq!(
        sql.execute(&format!(
            "SELECT update_dynamic('a','{}')",
            value.replace('\'', "''")
        ))
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(2)]]
    );
    assert_eq!(
        sql.execute("SELECT count(*) FROM measurements")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3)]]
    );
    assert_eq!(
        sql.execute("SELECT value FROM measurements WHERE id=3")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("other".into())]]
    );
}

#[test]
fn static_dml_row_count_diagnostics_are_per_call() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION counted_update(p_tenant TEXT) RETURNS BIGINT LANGUAGE plpgsql AS $$ DECLARE n BIGINT; BEGIN UPDATE measurements SET value='changed' WHERE tenant=p_tenant; GET CURRENT DIAGNOSTICS n = ROW_COUNT; RETURN n; END $$").unwrap();
    assert_eq!(
        sql.execute("SELECT counted_update('a'),counted_update('missing')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2), SqlValue::Int(0)]]
    );
}

#[test]
fn dynamic_into_strict_and_empty_results_follow_postgres_cardinality() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION dynamic_strict(p_id INT) RETURNS TEXT LANGUAGE plpgsql AS $$ DECLARE answer TEXT; BEGIN EXECUTE 'SELECT value FROM measurements WHERE id=$1' INTO STRICT answer USING p_id; RETURN answer; END $$").unwrap();
    assert_eq!(
        sql.execute("SELECT dynamic_strict(1)").unwrap().rows,
        vec![vec![SqlValue::String("first".into())]]
    );
    assert_eq!(
        sql.execute("SELECT dynamic_strict(999)")
            .unwrap_err()
            .sqlstate(),
        "P0002"
    );
    sql.execute("CREATE FUNCTION dynamic_empty() RETURNS TEXT LANGUAGE plpgsql AS $$ DECLARE answer TEXT := 'old'; BEGIN EXECUTE 'SELECT value FROM measurements WHERE id=$1' INTO answer USING 999; RETURN answer; END $$").unwrap();
    assert_eq!(
        sql.execute("SELECT dynamic_empty()").unwrap().rows,
        vec![vec![SqlValue::Null]]
    );
}

#[test]
fn dynamic_binds_are_restored_before_later_static_sql() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION dynamic_scope(p_id INT) RETURNS TEXT LANGUAGE plpgsql AS $$ DECLARE answer TEXT; original TEXT; BEGIN EXECUTE 'SELECT value FROM measurements WHERE id=$1' INTO answer USING 2; SELECT value INTO original FROM measurements WHERE id=p_id; RETURN answer || ':' || original; END $$").unwrap();
    assert_eq!(
        sql.execute("SELECT dynamic_scope(1)").unwrap().rows,
        vec![vec![SqlValue::String("second:first".into())]]
    );
}

#[test]
fn dynamic_execution_and_static_writes_rollback_together_on_error() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION fail_dynamic() RETURNS BIGINT LANGUAGE plpgsql AS $$ DECLARE n BIGINT; BEGIN EXECUTE 'UPDATE measurements SET value=$1 WHERE tenant=$2' USING 'bad','a'; GET DIAGNOSTICS n=ROW_COUNT; RAISE EXCEPTION 'rollback fixture' USING ERRCODE='23514'; RETURN n; END $$").unwrap();
    assert_eq!(
        sql.execute("SELECT fail_dynamic()").unwrap_err().sqlstate(),
        "23514"
    );
    assert_eq!(
        sql.execute("SELECT value FROM measurements WHERE id=1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("first".into())]]
    );
}

#[test]
fn successful_dynamic_function_cannot_commit_before_later_statement_error() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION write_dynamic() RETURNS BIGINT LANGUAGE plpgsql AS $$ BEGIN EXECUTE 'UPDATE measurements SET value=$1 WHERE id=$2' USING 'bad',1; RETURN 1; END $$").unwrap();
    sql.execute("CREATE FUNCTION fail_after_dynamic() RETURNS BIGINT LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'later expression failed' USING ERRCODE='23514'; RETURN 1; END $$").unwrap();
    assert_eq!(
        sql.execute("SELECT write_dynamic(),fail_after_dynamic()")
            .unwrap_err()
            .sqlstate(),
        "23514"
    );
    assert_eq!(
        sql.execute("SELECT value FROM measurements WHERE id=1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("first".into())]]
    );
}

#[test]
fn failed_dynamic_statement_preserves_earlier_explicit_transaction_work() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION fail_dynamic() RETURNS BIGINT LANGUAGE plpgsql AS $$ BEGIN EXECUTE 'UPDATE measurements SET value=$1 WHERE id=$2' USING 'bad',1; RAISE EXCEPTION 'rollback statement only' USING ERRCODE='23514'; RETURN 1; END $$").unwrap();
    sql.execute("BEGIN; UPDATE measurements SET value='prior' WHERE id=3")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT fail_dynamic()").unwrap_err().sqlstate(),
        "23514"
    );
    sql.execute("COMMIT").unwrap();
    assert_eq!(
        sql.execute("SELECT value FROM measurements ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("first".into())],
            vec![SqlValue::String("second".into())],
            vec![SqlValue::String("prior".into())]
        ]
    );
}

#[test]
fn nested_readonly_row_functions_keep_the_callers_pending_snapshot() {
    let (_root, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION nested_value(p_id INT) RETURNS TEXT LANGUAGE plpgsql AS $$ DECLARE answer TEXT; BEGIN SELECT value INTO answer FROM measurements WHERE id=p_id; RETURN answer; END $$; CREATE FUNCTION outer_value(p_id INT) RETURNS TEXT LANGUAGE plpgsql AS $$ BEGIN RETURN nested_value(p_id); END $$").unwrap();
    sql.execute("BEGIN; UPDATE measurements SET value='pending' WHERE id=1")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT outer_value(id) FROM measurements WHERE id=1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("pending".into())]]
    );
    sql.execute("ROLLBACK").unwrap();
}
