use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn setup(db: &mut BicDb) {
    let mut sql = SqlSession::new(db);
    sql.execute("CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k)")
        .unwrap();
    sql.execute("CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)")
        .unwrap();
    sql.execute("CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)")
        .unwrap();
    sql.execute("INSERT INTO p VALUES(1,'original-ack')")
        .unwrap();
    sql.execute("CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW;END$$")
        .unwrap();
}
fn lineage(sql: &mut SqlSession<'_>) -> Vec<Vec<SqlValue>> {
    sql.execute("SELECT c.relname,t.tgname,t.tgparentid<>0 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE t.tgname='guard' ORDER BY c.relname").unwrap().rows
}
fn entry(table: &str, child: bool) -> Vec<SqlValue> {
    vec![
        SqlValue::String(table.into()),
        SqlValue::String("guard".into()),
        SqlValue::Bool(child),
    ]
}
fn stores(mut run: impl FnMut(&mut BicDb)) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        setup(&mut db);
        run(&mut db);
        db.close().unwrap();
        for _ in 0..2 {
            let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT k,payload FROM p")
                    .unwrap()
                    .rows,
                vec![vec![
                    SqlValue::Int(1),
                    SqlValue::String("original-ack".into())
                ]]
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn row_trigger_clones_existing_and_later_created_or_attached_children() {
    stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TRIGGER guard BEFORE INSERT ON p FOR EACH ROW EXECUTE FUNCTION f()")
            .unwrap();
        assert_eq!(
            lineage(&mut sql),
            vec![entry("high", true), entry("low", true), entry("p", false)]
        );
        sql.execute("CREATE TABLE later PARTITION OF p FOR VALUES FROM(20) TO(30)")
            .unwrap();
        sql.execute("CREATE TABLE attached(k INT PRIMARY KEY,payload TEXT)")
            .unwrap();
        sql.execute("ALTER TABLE p ATTACH PARTITION attached FOR VALUES FROM(30) TO(40)")
            .unwrap();
        assert_eq!(
            lineage(&mut sql),
            vec![
                entry("attached", true),
                entry("high", true),
                entry("later", true),
                entry("low", true),
                entry("p", false)
            ]
        );
    });
}

#[test]
fn trigger_clone_dependency_mode_and_drop_rollback_keep_the_complete_tree() {
    stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TRIGGER guard BEFORE INSERT ON p FOR EACH ROW EXECUTE FUNCTION f()")
            .unwrap();
        assert_eq!(
            sql.execute("DROP TRIGGER guard ON low")
                .unwrap_err()
                .sqlstate(),
            "2BP01"
        );
        assert_eq!(sql.execute("CREATE OR REPLACE TRIGGER guard BEFORE INSERT ON low FOR EACH ROW EXECUTE FUNCTION f()").unwrap_err().sqlstate(),"42710");
        sql.execute("ALTER TABLE p DISABLE TRIGGER guard").unwrap();
        assert_eq!(
            sql.execute("SELECT tgenabled FROM pg_trigger WHERE tgname='guard' ORDER BY tgrelid")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("D".into())]; 3]
        );
        sql.execute("BEGIN").unwrap();
        sql.execute("DROP TRIGGER guard ON p").unwrap();
        assert!(lineage(&mut sql).is_empty());
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(
            lineage(&mut sql),
            vec![entry("high", true), entry("low", true), entry("p", false)]
        );
        assert_eq!(
            sql.execute("SELECT tgenabled FROM pg_trigger WHERE tgname='guard' ORDER BY tgrelid")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("D".into())]; 3]
        );
    });
}

#[test]
fn failed_clone_name_conflict_preflights_all_children_and_replace_is_transactional() {
    stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TRIGGER guard BEFORE INSERT ON low FOR EACH ROW EXECUTE FUNCTION f()")
            .unwrap();
        assert_eq!(
            sql.execute(
                "CREATE TRIGGER guard BEFORE INSERT ON p FOR EACH ROW EXECUTE FUNCTION f()"
            )
            .unwrap_err()
            .sqlstate(),
            "42710"
        );
        assert_eq!(lineage(&mut sql), vec![entry("low", false)]);
        sql.execute("BEGIN").unwrap();
        sql.execute(
            "CREATE OR REPLACE TRIGGER guard BEFORE INSERT ON p FOR EACH ROW EXECUTE FUNCTION f()",
        )
        .unwrap();
        assert_eq!(
            lineage(&mut sql),
            vec![entry("high", true), entry("low", true), entry("p", false)]
        );
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(lineage(&mut sql), vec![entry("low", false)]);
    });
}

#[test]
fn attach_and_detach_publish_schema_and_clones_together_and_keep_local_triggers() {
    stores(|db| {
        let mut sql = SqlSession::new(db);
        sql.execute("CREATE TRIGGER guard BEFORE INSERT ON p FOR EACH ROW EXECUTE FUNCTION f()")
            .unwrap();
        sql.execute("CREATE TABLE attached(k INT PRIMARY KEY,payload TEXT)")
            .unwrap();
        sql.execute("CREATE TRIGGER local_guard BEFORE INSERT ON attached FOR EACH ROW EXECUTE FUNCTION f()").unwrap();
        sql.execute("BEGIN").unwrap();
        sql.execute("ALTER TABLE p ATTACH PARTITION attached FOR VALUES FROM(20) TO(30)")
            .unwrap();
        assert!(lineage(&mut sql).contains(&entry("attached", true)));
        sql.execute("ROLLBACK").unwrap();
        assert!(!lineage(&mut sql)
            .iter()
            .any(|row| row[0] == SqlValue::String("attached".into())));
        sql.execute("ALTER TABLE p ATTACH PARTITION attached FOR VALUES FROM(20) TO(30)")
            .unwrap();
        sql.execute("BEGIN").unwrap();
        sql.execute("ALTER TABLE p DETACH PARTITION attached")
            .unwrap();
        assert_eq!(
            sql.execute(
                "SELECT tgname FROM pg_trigger WHERE tgrelid='attached'::regclass ORDER BY tgname"
            )
            .unwrap()
            .rows,
            vec![vec![SqlValue::String("local_guard".into())]]
        );
        sql.execute("ROLLBACK").unwrap();
        assert!(lineage(&mut sql).contains(&entry("attached", true)));
    });
}
