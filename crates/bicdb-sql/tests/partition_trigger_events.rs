//! Event order and physical trigger context captured from real PostgreSQL16.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::Value;

fn setup(sql: &mut SqlSession<'_>) {
    sql.execute(
        "CREATE TABLE audit(seq BIGSERIAL,op TEXT,phase TEXT,trig TEXT,rel TEXT,oldk INT,newk INT)",
    )
    .unwrap();
    sql.execute("CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k)")
        .unwrap();
    sql.execute("CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)")
        .unwrap();
    sql.execute("CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)")
        .unwrap();
    sql.execute("CREATE FUNCTION audit_fn() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO audit(op,phase,trig,rel,oldk,newk) VALUES(TG_OP,TG_WHEN,TG_NAME,TG_TABLE_NAME,OLD.k,NEW.k); IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END$$").unwrap();
    sql.execute("CREATE TRIGGER parent_b BEFORE INSERT OR UPDATE OR DELETE ON p FOR EACH ROW EXECUTE FUNCTION audit_fn()").unwrap();
    sql.execute("CREATE TRIGGER parent_a AFTER INSERT OR UPDATE OR DELETE ON p FOR EACH ROW EXECUTE FUNCTION audit_fn()").unwrap();
}
fn expected(label: &str) -> Vec<Vec<SqlValue>> {
    let fixture: Value = serde_json::from_str(include_str!(
        "fixtures/postgres/partition_trigger_events_pg16.json"
    ))
    .unwrap();
    fixture[label]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| {
            ["op", "phase", "trigger", "relation", "old", "new"]
                .into_iter()
                .map(|field| match &event[field] {
                    Value::Null => SqlValue::Null,
                    Value::String(value) => SqlValue::String(value.clone()),
                    Value::Number(value) => SqlValue::Int(value.as_i64().unwrap()),
                    _ => panic!("unexpected fixture value"),
                })
                .collect()
        })
        .collect()
}
fn events(sql: &mut SqlSession<'_>) -> Vec<Vec<SqlValue>> {
    sql.execute("SELECT op,phase,trig,rel,oldk,newk FROM audit ORDER BY seq")
        .unwrap()
        .rows
}

#[test]
fn parent_and_child_trigger_events_match_native_pg_insert_move_delete_and_upsert() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            setup(&mut sql);
            sql.execute("INSERT INTO p VALUES(1,'original-ack')")
                .unwrap();
            sql.execute("INSERT INTO low VALUES(2,'direct-ack')")
                .unwrap();
            sql.execute("UPDATE p SET k=11 WHERE k=1").unwrap();
            sql.execute("DELETE FROM p WHERE k=2").unwrap();
            sql.execute("INSERT INTO p VALUES(11,'updated-ack') ON CONFLICT(k) DO UPDATE SET payload=excluded.payload").unwrap();
            assert_eq!(events(&mut sql), expected("events"));
        }
        db.close().unwrap();
        for _ in 0..2 {
            let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(events(&mut sql), expected("events"));
            assert_eq!(
                sql.execute("SELECT k,payload FROM high").unwrap().rows,
                vec![vec![
                    SqlValue::Int(11),
                    SqlValue::String("updated-ack".into())
                ]]
            );
            drop(sql);
            db.close().unwrap();
        }
    }
}

#[test]
fn multirow_partition_insert_fires_all_before_events_then_all_after_events() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            setup(&mut sql);
            sql.execute("INSERT INTO p VALUES(3,'low-multi-ack'),(13,'high-multi-ack')")
                .unwrap();
            assert_eq!(events(&mut sql), expected("multirow_events"));
        }
        db.close().unwrap();
        for _ in 0..2 {
            let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            assert_eq!(
                events(&mut SqlSession::new(&mut db)),
                expected("multirow_events")
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn mixed_partition_upsert_interleaves_before_arbitration_and_queues_after_events_in_row_order() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            setup(&mut sql);
            sql.execute("INSERT INTO p VALUES(3,'low-ack'),(12,'high-ack')")
                .unwrap();
            sql.execute("DELETE FROM audit").unwrap();
            sql.execute("INSERT INTO p VALUES(3,'changed-low'),(14,'new-high'),(12,'changed-high') ON CONFLICT(k) DO UPDATE SET payload=excluded.payload").unwrap();
            assert_eq!(events(&mut sql), expected("mixed_upsert_events"));
        }
        db.close().unwrap();
        for _ in 0..2 {
            let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            assert_eq!(
                events(&mut SqlSession::new(&mut db)),
                expected("mixed_upsert_events")
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn before_insert_trigger_cannot_reroute_the_row_and_all_audit_effects_roll_back() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            setup(&mut sql);
            sql.execute("INSERT INTO p VALUES(1,'original-ack')")
                .unwrap();
            let before = events(&mut sql);
            sql.execute("CREATE FUNCTION move_key() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.k=12;RETURN NEW;END$$").unwrap();
            sql.execute("CREATE TRIGGER z_move BEFORE INSERT ON low FOR EACH ROW EXECUTE FUNCTION move_key()").unwrap();
            assert_eq!(
                sql.execute("INSERT INTO p VALUES(2,'must-refuse')")
                    .unwrap_err()
                    .sqlstate(),
                "23514"
            );
            assert_eq!(events(&mut sql), before);
            assert_eq!(
                sql.execute("SELECT k,payload FROM p").unwrap().rows,
                vec![vec![
                    SqlValue::Int(1),
                    SqlValue::String("original-ack".into())
                ]]
            );
        }
        db.close().unwrap();
    }
}
