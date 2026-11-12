//! Actual PostgreSQL16 parent constraint triggers and function dependencies.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn lineage(sql: &mut SqlSession<'_>) -> Vec<Vec<SqlValue>> {
    sql.execute("SELECT c.relname,t.tgname,t.tgparentid<>0 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE t.tgname IN('guard','c','child_c') ORDER BY c.relname,t.tgname").unwrap().rows
}
fn entry(table: &str, name: &str, child: bool) -> Vec<SqlValue> {
    vec![
        SqlValue::String(table.into()),
        SqlValue::String(name.into()),
        SqlValue::Bool(child),
    ]
}
fn full_tree() -> Vec<Vec<SqlValue>> {
    vec![
        entry("high", "c", true),
        entry("high", "guard", true),
        entry("low", "c", true),
        entry("low", "child_c", false),
        entry("low", "guard", true),
        entry("p", "c", false),
        entry("p", "guard", false),
    ]
}
fn setup(sql: &mut SqlSession<'_>) {
    sql.execute("CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k)")
        .unwrap();
    sql.execute("CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)")
        .unwrap();
    sql.execute("CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)")
        .unwrap();
    sql.execute("INSERT INTO p VALUES(1,'original-ack')")
        .unwrap();
    sql.execute("CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.payload='bad' THEN RAISE EXCEPTION 'trigger guard rejected bad payload'; END IF; RETURN NEW;END$$").unwrap();
}
fn tree(sql: &mut SqlSession<'_>) {
    sql.execute("CREATE CONSTRAINT TRIGGER c AFTER INSERT ON p DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION f()").unwrap();
    sql.execute("CREATE CONSTRAINT TRIGGER child_c AFTER INSERT ON low DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION f()").unwrap();
    sql.execute("CREATE TRIGGER guard BEFORE INSERT ON p FOR EACH ROW EXECUTE FUNCTION f()")
        .unwrap();
    assert_eq!(lineage(sql), full_tree());
}
fn stores(mut action: impl FnMut(&mut SqlSession<'_>)) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        let expected = {
            let mut sql = SqlSession::new(&mut db);
            setup(&mut sql);
            action(&mut sql);
            lineage(&mut sql)
        };
        db.close().unwrap();
        for _ in 0..2 {
            let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(lineage(&mut sql), expected);
            assert_eq!(
                sql.execute("SELECT k,payload FROM p ORDER BY k")
                    .unwrap()
                    .rows,
                vec![vec![
                    SqlValue::Int(1),
                    SqlValue::String("original-ack".into())
                ]]
            );
            drop(sql);
            db.close().unwrap();
        }
    }
}
#[test]
fn partition_parent_constraint_trigger_clones_and_rejects_at_commit_atomically() {
    stores(|sql| {
        sql.execute("CREATE CONSTRAINT TRIGGER c AFTER INSERT ON p DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION f()").unwrap();
        assert_eq!(
            lineage(sql),
            vec![
                entry("high", "c", true),
                entry("low", "c", true),
                entry("p", "c", false)
            ]
        );
        sql.execute("BEGIN").unwrap();
        sql.execute("INSERT INTO p VALUES(2,'good'),(12,'bad')")
            .unwrap();
        assert_eq!(
            sql.execute("SELECT k FROM p ORDER BY k").unwrap().rows,
            vec![
                vec![SqlValue::Int(1)],
                vec![SqlValue::Int(2)],
                vec![SqlValue::Int(12)]
            ]
        );
        assert_eq!(sql.execute("COMMIT").unwrap_err().sqlstate(), "P0001");
        sql.execute("ROLLBACK").unwrap();
    });
}
#[test]
fn drop_function_restrict_preserves_the_complete_trigger_tree_and_enforcement() {
    stores(|sql| {
        tree(sql);
        assert_eq!(
            sql.execute("DROP FUNCTION f() RESTRICT")
                .unwrap_err()
                .sqlstate(),
            "2BP01"
        );
        assert_eq!(lineage(sql), full_tree());
        assert_eq!(
            sql.execute("INSERT INTO p VALUES(2,'bad')")
                .unwrap_err()
                .sqlstate(),
            "P0001"
        );
    });
}
#[test]
fn drop_function_cascade_removes_every_trigger_and_rollback_restores_the_tree() {
    stores(|sql| {
        tree(sql);
        sql.execute("BEGIN").unwrap();
        sql.execute("DROP FUNCTION f() CASCADE").unwrap();
        assert!(lineage(sql).is_empty());
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(lineage(sql), full_tree());
        assert_eq!(
            sql.execute("INSERT INTO p VALUES(2,'bad')")
                .unwrap_err()
                .sqlstate(),
            "P0001"
        );
    });
}
#[test]
fn committed_function_cascade_reopens_without_orphan_triggers() {
    stores(|sql| {
        tree(sql);
        sql.execute("DROP FUNCTION f() CASCADE").unwrap();
        assert!(lineage(sql).is_empty());
        assert_eq!(
            sql.execute("DROP FUNCTION f()").unwrap_err().sqlstate(),
            "42883"
        );
    });
}
#[test]
fn multi_function_drop_preflights_missing_function_before_mutating_guard_dependencies() {
    stores(|sql| {
        tree(sql);
        assert_eq!(
            sql.execute("DROP FUNCTION f(),missing_function()")
                .unwrap_err()
                .sqlstate(),
            "42883"
        );
        assert_eq!(lineage(sql), full_tree());
        assert_eq!(
            sql.execute("INSERT INTO p VALUES(2,'bad')")
                .unwrap_err()
                .sqlstate(),
            "P0001"
        );
    });
}
