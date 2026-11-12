//! Mandatory positives derived from actual PostgreSQL16 trigger-side reads.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::Value;

fn setup(sql: &mut SqlSession<'_>) {
    sql.execute("CREATE TABLE audit(seq BIGSERIAL,op TEXT,phase TEXT,rel TEXT,oldk INT,newk INT,low_n INT,high_n INT,parent_n INT)").unwrap();
    sql.execute("CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k)")
        .unwrap();
    sql.execute("CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)")
        .unwrap();
    sql.execute("CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)")
        .unwrap();
    sql.execute("CREATE FUNCTION audit_fn() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO audit(op,phase,rel,oldk,newk,low_n,high_n,parent_n) VALUES(TG_OP,TG_WHEN,TG_TABLE_NAME,OLD.k,NEW.k,(SELECT COUNT(*) FROM low),(SELECT COUNT(*) FROM high),(SELECT COUNT(*) FROM p)); IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END$$").unwrap();
    sql.execute(
        "CREATE FUNCTION veto() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$",
    )
    .unwrap();
    sql.execute("CREATE TRIGGER parent_b BEFORE INSERT OR UPDATE OR DELETE ON p FOR EACH ROW EXECUTE FUNCTION audit_fn()").unwrap();
    sql.execute("CREATE TRIGGER parent_a AFTER INSERT OR UPDATE OR DELETE ON p FOR EACH ROW EXECUTE FUNCTION audit_fn()").unwrap();
}
fn expected(label: &str, fields: &[&str]) -> Vec<Vec<SqlValue>> {
    let fixture: Value = serde_json::from_str(include_str!(
        "fixtures/postgres/partition_trigger_visibility_pg16.json"
    ))
    .unwrap();
    fixture[label]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            fields
                .iter()
                .map(|field| match &row[*field] {
                    Value::Null => SqlValue::Null,
                    Value::String(value) => SqlValue::String(value.clone()),
                    Value::Number(value) => SqlValue::Int(value.as_i64().unwrap()),
                    _ => panic!("unexpected native fixture cell"),
                })
                .collect()
        })
        .collect()
}
fn verify(sql: &mut SqlSession<'_>, label: &str) {
    assert_eq!(
        sql.execute("SELECT k,payload FROM p ORDER BY k")
            .unwrap()
            .rows,
        expected(&format!("{label}_rows"), &["k", "payload"]),
        "surviving acknowledged rows: {label}"
    );
    // UPDATE/DELETE SQL does not guarantee candidate key order. Compare the
    // visibility progression without imposing an incidental heap key order.
    assert_eq!(
        sql.execute("SELECT op,phase,rel,low_n,high_n,parent_n FROM audit ORDER BY seq")
            .unwrap()
            .rows,
        expected(
            &format!("{label}_events"),
            &["op", "phase", "relation", "low_n", "high_n", "parent_n"]
        ),
        "trigger-side visibility: {label}"
    );
}
fn run(label: &str, seed: &str, trigger: Option<&str>, mutation: &str) {
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
            if !seed.is_empty() {
                sql.execute(seed).unwrap();
            }
            sql.execute("DELETE FROM audit").unwrap();
            if let Some(trigger) = trigger {
                sql.execute(trigger).unwrap();
            }
            let mut returned = sql.execute(mutation).unwrap().rows;
            returned.sort_by_key(|row| match row.first() {
                Some(SqlValue::Int(k)) => *k,
                _ => panic!("integer key expected"),
            });
            assert_eq!(
                returned,
                expected(&format!("{label}_returned"), &["k", "payload"]),
                "affected/returned rows: {label}"
            );
            verify(&mut sql, label);
        }
        db.close().unwrap();
        for _ in 0..2 {
            let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            verify(&mut SqlSession::new(&mut db), label);
            db.close().unwrap();
        }
    }
}
macro_rules! case {
    ($name:ident,$label:literal,$seed:literal,$trigger:expr,$mutation:literal) => {
        #[test]
        fn $name() {
            run($label, $seed, $trigger, $mutation);
        }
    };
}
case!(
    insert_before_triggers_see_earlier_input_rows,
    "insert_multi",
    "",
    None,
    "INSERT INTO p VALUES(1,'first-ack'),(11,'second-ack') RETURNING k,payload"
);
case!(
    destination_before_insert_observes_source_already_deleted,
    "move",
    "INSERT INTO p VALUES(1,'original-ack')",
    None,
    "UPDATE p SET k=11 WHERE k=1 RETURNING k,payload"
);
case!(
    source_before_update_veto_preserves_ack,
    "veto_source_update",
    "INSERT INTO p VALUES(1,'original-ack')",
    Some("CREATE TRIGGER z_veto BEFORE UPDATE ON low FOR EACH ROW EXECUTE FUNCTION veto()"),
    "UPDATE p SET k=11 WHERE k=1 RETURNING k,payload"
);
case!(
    source_before_delete_veto_preserves_ack,
    "veto_source_delete",
    "INSERT INTO p VALUES(1,'original-ack')",
    Some("CREATE TRIGGER z_veto BEFORE DELETE ON low FOR EACH ROW EXECUTE FUNCTION veto()"),
    "UPDATE p SET k=11 WHERE k=1 RETURNING k,payload"
);
case!(
    destination_before_insert_veto_commits_only_source_delete,
    "veto_destination_insert",
    "INSERT INTO p VALUES(1,'original-ack')",
    Some("CREATE TRIGGER z_veto BEFORE INSERT ON high FOR EACH ROW EXECUTE FUNCTION veto()"),
    "UPDATE p SET k=11 WHERE k=1 RETURNING k,payload"
);
case!(
    update_before_triggers_see_prior_moved_rows,
    "update_multi",
    "INSERT INTO p VALUES(1,'first-ack'),(2,'second-ack')",
    None,
    "UPDATE p SET k=k+10 RETURNING k,payload"
);
case!(
    delete_before_triggers_see_prior_deletions,
    "delete_multi",
    "INSERT INTO p VALUES(1,'first-ack'),(2,'second-ack')",
    None,
    "DELETE FROM p RETURNING k,payload"
);
