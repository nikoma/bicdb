//! Native PostgreSQL command tags and transactional rewrite-rule behavior.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn rule_rows_keep_the_original_mutation_tag_and_action_types() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/postgres/rule_command_tags_pg16.json"
    ))
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut s = SqlSession::new(&mut db);
    for sql in fixture["setup"].as_array().unwrap() {
        s.execute(sql.as_str().unwrap()).unwrap();
    }
    for check in fixture["checks"].as_array().unwrap() {
        let sql = check["sql"].as_str().unwrap();
        let result = s.execute(sql).unwrap();
        assert_eq!(
            result.command_complete_tag(),
            check["status"].as_str().unwrap(),
            "{sql}"
        );
        assert_eq!(
            result.columns.len(),
            check["columns"].as_array().unwrap().len(),
            "{sql}"
        );
        assert_eq!(
            result.rows.len(),
            check["rows"].as_array().unwrap().len(),
            "{sql}"
        );
        if sql.contains("scalar_request") {
            assert_eq!(
                result.rows,
                vec![vec![SqlValue::Int(8)], vec![SqlValue::Int(10)]]
            );
        }
        if sql.contains("sum(v)") {
            assert_eq!(result.rows, vec![vec![SqlValue::Int(19)]]);
        }
        if sql.starts_with("INSERT INTO void_request") {
            assert_eq!(result.column_types, vec![Some("void".to_string())]);
        }
    }
}

#[test]
fn rule_creation_replacement_and_drop_follow_transaction_and_savepoint_rollback() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap();
        let mut s = SqlSession::new(&mut db);
        s.execute("CREATE SCHEMA rule_path; SET search_path=rule_path,public; CREATE TABLE request(v integer); CREATE TABLE effects(v integer)").unwrap();
        s.execute("BEGIN; CREATE RULE r AS ON INSERT TO request DO INSTEAD INSERT INTO rule_path.effects VALUES(new.v); ROLLBACK").unwrap();
        assert!(s
            .execute("SELECT rulename FROM pg_rules WHERE schemaname='rule_path'")
            .unwrap()
            .rows
            .is_empty());
        s.execute("CREATE RULE r AS ON INSERT TO request DO INSTEAD INSERT INTO rule_path.effects VALUES(new.v)").unwrap();
        s.execute("BEGIN; SAVEPOINT before_rule; CREATE OR REPLACE RULE r AS ON INSERT TO request DO INSTEAD NOTHING; ROLLBACK TO before_rule; COMMIT").unwrap();
        s.execute("INSERT INTO request VALUES(7)").unwrap();
        s.execute("BEGIN; DROP RULE r ON request; ROLLBACK")
            .unwrap();
        s.execute("INSERT INTO request VALUES(8)").unwrap();
        assert_eq!(
            s.execute("SELECT v FROM effects ORDER BY v").unwrap().rows,
            vec![vec![SqlValue::Int(7)], vec![SqlValue::Int(8)]]
        );
        assert_eq!(
            s.execute("SELECT count(*) FROM request").unwrap().rows,
            vec![vec![SqlValue::Int(0)]]
        );
    }
}

#[test]
fn a_failed_rule_action_rolls_back_every_input_row() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE rule_request(v integer); CREATE TABLE rule_effect(v integer); CREATE FUNCTION rule_effect(n integer) RETURNS void LANGUAGE plpgsql AS $$ BEGIN INSERT INTO rule_effect VALUES(n); IF n=2 THEN RAISE EXCEPTION 'second input fails'; END IF; END $$; CREATE RULE r AS ON INSERT TO rule_request DO INSTEAD SELECT rule_effect(new.v)").unwrap();
    assert_eq!(
        s.execute("INSERT INTO rule_request VALUES(1),(2)")
            .unwrap_err()
            .sqlstate(),
        "P0001"
    );
    assert_eq!(
        s.execute("SELECT count(*) FROM rule_effect").unwrap().rows,
        vec![vec![SqlValue::Int(0)]]
    );
}
