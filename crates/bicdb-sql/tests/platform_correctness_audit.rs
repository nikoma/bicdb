//! Wrong-result and integrity regressions found by the platform-wide audit.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn jsonb_indexes_preserve_scan_results_and_predicate_sensitive_writes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode);
        let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
        let queries = [
            "SELECT id FROM docs WHERE v = '{\"a\":1,\"b\":2}'::jsonb ORDER BY id",
            "SELECT COUNT(*) FROM docs WHERE v = '{\"b\":2,\"a\":1}'::jsonb",
            "SELECT id FROM docs WHERE v = 'null'::jsonb ORDER BY id",
            "SELECT id FROM docs WHERE v IS NULL ORDER BY id",
            "SELECT id FROM docs WHERE v = '1.0'::jsonb ORDER BY id",
            "SELECT id FROM docs WHERE v = '[{\"a\":1,\"b\":2}]'::jsonb ORDER BY id",
            "SELECT id FROM docs ORDER BY v NULLS FIRST, id LIMIT 6",
            "SELECT a.id,b.id FROM docs a JOIN docs b ON a.v=b.v ORDER BY a.id,b.id",
        ];
        let expected = {
            let mut s = SqlSession::new(&mut db);
            s.execute("CREATE TABLE docs(id INT PRIMARY KEY,v JSONB,n INT)")
                .unwrap();
            s.execute("INSERT INTO docs VALUES (1,'{\"a\":1,\"b\":2}',0),(2,'{\"b\":2,\"a\":1}',0),(3,'null',0),(4,NULL,0),(5,'1',0),(6,'1.0',0),(7,'[{\"b\":2,\"a\":1}]',0),(8,'[{\"a\":1,\"b\":2}]',0)").unwrap();
            let baseline: Vec<_> = queries.iter().map(|q| s.execute(q).unwrap().rows).collect();
            assert_eq!(
                baseline[0],
                vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
            );
            assert_eq!(baseline[2], vec![vec![SqlValue::Int(3)]]);
            s.execute("CREATE INDEX docs_v ON docs(v)").unwrap();
            s.execute("CREATE INDEX docs_nv ON docs(n,v)").unwrap();
            baseline
        };
        for reopen in [false, true] {
            if reopen {
                drop(db);
                db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
            }
            let mut s = SqlSession::new(&mut db);
            for (query, rows) in queries.iter().zip(&expected) {
                assert_eq!(
                    &s.execute(query).unwrap().rows,
                    rows,
                    "{query}, reopen={reopen}; values={:?}",
                    (
                        s.execute("SELECT id,v FROM docs ORDER BY id").unwrap(),
                        s.execute("EXPLAIN SELECT id FROM docs ORDER BY v NULLS FIRST,id LIMIT 6")
                            .unwrap()
                    )
                );
            }
        }
        let mut s = SqlSession::new(&mut db);
        s.execute("UPDATE docs SET n=9 WHERE v='{\"a\":1,\"b\":2}'::jsonb")
            .unwrap();
        assert_eq!(
            s.execute("SELECT id FROM docs WHERE n=9 ORDER BY id")
                .unwrap()
                .rows,
            expected[0]
        );
        s.execute("DELETE FROM docs WHERE v='[{\"a\":1,\"b\":2}]'::jsonb")
            .unwrap();
        assert_eq!(
            s.execute("SELECT COUNT(*) FROM docs").unwrap().rows,
            vec![vec![SqlValue::Int(6)]]
        );
    }
}

#[test]
fn canonical_json_unique_keys_cannot_race_and_reservations_follow_savepoints() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE docs(id INT PRIMARY KEY,v JSONB UNIQUE)")
            .unwrap();
        let mut a = SqlSession::new_shared(&db);
        let mut b = SqlSession::new_shared(&db);
        a.execute("BEGIN").unwrap();
        b.execute("BEGIN").unwrap();
        a.execute("INSERT INTO docs VALUES(1,'{\"a\":1,\"b\":2}')")
            .unwrap();
        assert!(
            b.execute("INSERT INTO docs VALUES(2,'{\"b\":2,\"a\":1}')")
                .is_err(),
            "equivalent keys must contend before both can commit"
        );
        b.execute("ROLLBACK").unwrap();
        a.execute("COMMIT").unwrap();
        a.execute("BEGIN").unwrap();
        a.execute("SAVEPOINT s").unwrap();
        a.execute("INSERT INTO docs VALUES(3,'[1.0]')").unwrap();
        a.execute("ROLLBACK TO SAVEPOINT s").unwrap();
        b.execute("BEGIN").unwrap();
        b.execute("INSERT INTO docs VALUES(4,'[1]')").unwrap();
        b.execute("COMMIT").unwrap();
        a.execute("ROLLBACK").unwrap();
        assert_eq!(
            b.execute("SELECT COUNT(*) FROM docs").unwrap().rows,
            vec![vec![SqlValue::Int(2)]]
        );
    }
}

#[test]
fn json_unique_checks_and_on_conflict_see_new_commits() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE docs(id INT PRIMARY KEY,v JSONB UNIQUE)")
        .unwrap();
    let mut old = SqlSession::new_shared(&db);
    let mut writer = SqlSession::new_shared(&db);
    old.execute("BEGIN ISOLATION LEVEL READ COMMITTED").unwrap();
    old.execute("SELECT * FROM docs").unwrap();
    writer.execute("BEGIN").unwrap();
    writer
        .execute("INSERT INTO docs VALUES(1,'{\"a\":1,\"b\":2}')")
        .unwrap();
    writer.execute("COMMIT").unwrap();
    assert!(old
        .execute("INSERT INTO docs VALUES(2,'{\"b\":2,\"a\":1}')")
        .is_err());
    old.execute("ROLLBACK").unwrap();
    old.execute("BEGIN ISOLATION LEVEL READ COMMITTED").unwrap();
    old.execute("SELECT * FROM docs").unwrap();
    writer.execute("BEGIN").unwrap();
    writer.execute("INSERT INTO docs VALUES(3,'[1]')").unwrap();
    writer.execute("COMMIT").unwrap();
    old.execute("INSERT INTO docs VALUES(4,'[1.0]') ON CONFLICT(v) DO NOTHING")
        .unwrap();
    old.execute("COMMIT").unwrap();
    assert_eq!(
        writer
            .execute("SELECT id FROM docs ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(3)]]
    );
}

#[test]
fn copy_validates_trigger_postimages_and_rolls_back_all_effects_on_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE parents(id INT PRIMARY KEY); INSERT INTO parents VALUES(1); CREATE TABLE copied(id INT PRIMARY KEY,v INT NOT NULL CHECK(v>0),p INT REFERENCES parents(id)); CREATE TABLE side_effect(id INT PRIMARY KEY)").unwrap();
    s.execute("CREATE FUNCTION rewrite_copy() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO side_effect VALUES(NEW.id); IF NEW.id=2 THEN NEW.v := NULL; END IF; IF NEW.id=3 THEN NEW.v := -1; END IF; IF NEW.id=4 THEN NEW.p := 99; END IF; RETURN NEW; END $$").unwrap();
    s.execute("CREATE TRIGGER rewrite BEFORE INSERT ON copied FOR EACH ROW EXECUTE FUNCTION rewrite_copy()").unwrap();
    let columns = vec!["id".into(), "v".into(), "p".into()];
    for (id, state) in [(2, "23502"), (3, "23514"), (4, "23503")] {
        let error = s
            .copy_insert_rows(
                "copied",
                &columns,
                vec![
                    vec![Some("1".into()), Some("1".into()), Some("1".into())],
                    vec![Some(id.to_string()), Some("1".into()), Some("1".into())],
                ],
            )
            .expect_err("a trigger cannot make a constraint-violating row valid for COPY");
        assert_eq!(error.sqlstate(), state, "{error}");
        for table in ["copied", "side_effect"] {
            assert!(
                s.execute(&format!("SELECT * FROM {table}"))
                    .unwrap()
                    .rows
                    .is_empty(),
                "failed COPY leaked effects into {table}"
            );
        }
    }
    s.copy_insert_rows(
        "copied",
        &columns,
        vec![vec![Some("1".into()), Some("1".into()), Some("1".into())]],
    )
    .unwrap();
    assert_eq!(
        s.execute("SELECT id FROM copied").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn before_trigger_field_assignments_feed_generated_columns_and_returning() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE generated_rows(id INT PRIMARY KEY,n INT,g INT GENERATED ALWAYS AS(n*2) STORED)").unwrap();
    s.execute("CREATE FUNCTION bump_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.n := NEW.n+1; RETURN NEW; END $$").unwrap();
    s.execute("CREATE TRIGGER bump BEFORE INSERT OR UPDATE ON generated_rows FOR EACH ROW EXECUTE FUNCTION bump_row()").unwrap();
    for (sql,n) in [("INSERT INTO generated_rows(id,n) VALUES(1,2) RETURNING n,g",3),("UPDATE generated_rows SET n=4 WHERE id=1 RETURNING n,g",5),("INSERT INTO generated_rows(id,n) VALUES(1,6) ON CONFLICT(id) DO UPDATE SET n=excluded.n RETURNING n,g",8)] {
        assert_eq!(s.execute(sql).unwrap().rows,vec![vec![SqlValue::Int(n),SqlValue::Int(n*2)]],"{sql}");
    }
    s.copy_insert_rows(
        "generated_rows",
        &["id".into(), "n".into()],
        vec![vec![Some("2".into()), Some("9".into())]],
    )
    .unwrap();
    assert_eq!(
        s.execute("SELECT n,g FROM generated_rows WHERE id=2")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(10), SqlValue::Int(20)]]
    );
}

#[test]
fn foreign_key_checks_cannot_race_parent_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db).execute("CREATE TABLE parents(id INT PRIMARY KEY); CREATE TABLE children(id INT PRIMARY KEY,p INT REFERENCES parents(id)); INSERT INTO parents VALUES(1)").unwrap();
    let mut child = SqlSession::new_shared(&db);
    let mut parent = SqlSession::new_shared(&db);
    child.execute("BEGIN").unwrap();
    parent.execute("BEGIN").unwrap();
    child.execute("INSERT INTO children VALUES(1,1)").unwrap();
    assert!(
        parent.execute("DELETE FROM parents WHERE id=1").is_err(),
        "a pending reference must protect its parent"
    );
    parent.execute("ROLLBACK").unwrap();
    child.execute("COMMIT").unwrap();
    assert!(parent.execute("DELETE FROM parents WHERE id=1").is_err());
}

#[test]
fn nullable_composite_references_are_exempt_from_parent_actions() {
    for action in ["CASCADE", "RESTRICT", "SET NULL", "SET DEFAULT"] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut s = SqlSession::new(&mut db);
        s.execute("CREATE TABLE p(id INT PRIMARY KEY,a INT,b INT,UNIQUE(a,b))")
            .unwrap();
        s.execute(&format!("CREATE TABLE c(id INT PRIMARY KEY,a INT,b INT,FOREIGN KEY(a,b) REFERENCES p(a,b) ON DELETE {action} ON UPDATE {action})")).unwrap();
        s.execute("INSERT INTO p VALUES(1,1,NULL); INSERT INTO c VALUES(1,1,NULL),(2,NULL,2)")
            .unwrap();
        s.execute("UPDATE p SET a=3 WHERE id=1").unwrap();
        s.execute("DELETE FROM p WHERE id=1").unwrap();
        assert_eq!(
            s.execute("SELECT id,a,b FROM c ORDER BY id").unwrap().rows,
            vec![
                vec![SqlValue::Int(1), SqlValue::Int(1), SqlValue::Null],
                vec![SqlValue::Int(2), SqlValue::Null, SqlValue::Int(2)]
            ],
            "{action}"
        );
    }
}

#[test]
fn cascading_deletes_reach_descendants_and_fire_their_guards_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE root_rows(id INT PRIMARY KEY); CREATE TABLE middle_rows(id INT PRIMARY KEY,p INT REFERENCES root_rows(id) ON DELETE CASCADE); CREATE TABLE leaf_rows(id INT PRIMARY KEY,p INT REFERENCES middle_rows(id) ON DELETE CASCADE); CREATE TABLE audit_rows(id INT PRIMARY KEY); INSERT INTO root_rows VALUES(1); INSERT INTO middle_rows VALUES(2,1); INSERT INTO leaf_rows VALUES(3,2)").unwrap();
    s.execute("CREATE FUNCTION log_leaf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit_rows VALUES(OLD.id); RETURN OLD; END $$").unwrap();
    s.execute("CREATE TRIGGER log_delete AFTER DELETE ON leaf_rows FOR EACH ROW EXECUTE FUNCTION log_leaf()").unwrap();
    s.execute("BEGIN; DELETE FROM root_rows WHERE id=1")
        .unwrap();
    for table in ["root_rows", "middle_rows", "leaf_rows"] {
        assert!(
            s.execute(&format!("SELECT * FROM {table}"))
                .unwrap()
                .rows
                .is_empty(),
            "{table}"
        );
    }
    s.execute("ROLLBACK").unwrap();
    assert!(s
        .execute("SELECT * FROM audit_rows")
        .unwrap()
        .rows
        .is_empty());
    s.execute("DELETE FROM root_rows WHERE id=1").unwrap();
    for table in ["root_rows", "middle_rows", "leaf_rows"] {
        assert!(
            s.execute(&format!("SELECT * FROM {table}"))
                .unwrap()
                .rows
                .is_empty(),
            "{table}"
        );
    }
    assert_eq!(
        s.execute("SELECT id FROM audit_rows").unwrap().rows,
        vec![vec![SqlValue::Int(3)]]
    );
}

#[test]
fn a_late_referential_action_error_rolls_back_earlier_cascades() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE p(id INT PRIMARY KEY); CREATE TABLE a_cascade(id INT PRIMARY KEY,p INT REFERENCES p(id) ON DELETE CASCADE); CREATE TABLE z_default(id INT PRIMARY KEY,p INT DEFAULT 99 REFERENCES p(id) ON DELETE SET DEFAULT); INSERT INTO p VALUES(1); INSERT INTO a_cascade VALUES(2,1); INSERT INTO z_default VALUES(3,1)").unwrap();
    assert!(s.execute("DELETE FROM p WHERE id=1").is_err());
    for table in ["p", "a_cascade", "z_default"] {
        assert_eq!(
            s.execute(&format!("SELECT COUNT(*) FROM {table}"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]],
            "failed delete lost rows in {table}"
        );
    }
}

#[test]
fn self_referential_cascades_terminate_and_see_pending_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE tree(id INT PRIMARY KEY,p INT REFERENCES tree(id) ON DELETE CASCADE); INSERT INTO tree VALUES(1,NULL); INSERT INTO tree VALUES(2,1)").unwrap();
    s.execute("BEGIN; INSERT INTO tree VALUES(3,2); DELETE FROM tree WHERE id=1")
        .unwrap();
    assert!(s.execute("SELECT * FROM tree").unwrap().rows.is_empty());
    s.execute("ROLLBACK").unwrap();
    s.execute("UPDATE tree SET p=2 WHERE id=1").unwrap();
    s.execute("DELETE FROM tree WHERE id=1").unwrap();
    assert!(s.execute("SELECT * FROM tree").unwrap().rows.is_empty());
}

#[test]
fn referential_updates_propagate_to_descendants_and_trigger_failures_rollback_the_root() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE codes(id INT PRIMARY KEY,code INT UNIQUE); CREATE TABLE links(id INT PRIMARY KEY,code INT UNIQUE REFERENCES codes(code) ON UPDATE CASCADE ON DELETE SET NULL); CREATE TABLE descendants(id INT PRIMARY KEY,code INT REFERENCES links(code) ON UPDATE CASCADE); INSERT INTO codes VALUES(1,10); INSERT INTO links VALUES(2,10); INSERT INTO descendants VALUES(3,10)").unwrap();
    s.execute("CREATE FUNCTION guard_descendant() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.code=30 THEN RAISE EXCEPTION 'blocked descendant'; END IF; RETURN NEW; END $$").unwrap();
    s.execute("CREATE TRIGGER guard BEFORE UPDATE ON descendants FOR EACH ROW EXECUTE FUNCTION guard_descendant()").unwrap();
    s.execute("UPDATE codes SET code=20 WHERE id=1").unwrap();
    for table in ["codes", "links", "descendants"] {
        assert_eq!(
            s.execute(&format!("SELECT code FROM {table}"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(20)]],
            "{table}"
        );
    }
    assert!(s
        .execute("UPDATE codes SET code=30 WHERE id=1")
        .unwrap_err()
        .to_string()
        .contains("blocked descendant"));
    for table in ["codes", "links", "descendants"] {
        assert_eq!(
            s.execute(&format!("SELECT code FROM {table}"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(20)]],
            "rollback {table}"
        );
    }
    s.execute("DELETE FROM codes WHERE id=1").unwrap();
    for table in ["links", "descendants"] {
        assert_eq!(
            s.execute(&format!("SELECT code FROM {table}"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Null]],
            "set null {table}"
        );
    }
}

#[test]
fn scalar_subquery_aggregate_metadata_uses_its_input_type_even_when_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE scalar_types(id INT PRIMARY KEY,n SMALLINT)")
        .unwrap();
    s.execute("INSERT INTO scalar_types VALUES(1,2),(2,NULL)")
        .unwrap();
    let correlated = s.execute("SELECT id,(SELECT n FROM scalar_types b WHERE b.id=a.id) AS n FROM scalar_types a ORDER BY id").unwrap();
    assert_eq!(
        correlated.column_types,
        vec![Some("int4".into()), Some("int2".into())]
    );
    s.execute("DELETE FROM scalar_types").unwrap();
    for sql in [
        "SELECT (SELECT MAX(x.n) FROM scalar_types x) AS m",
        "SELECT (SELECT MIN(x.n) FROM scalar_types x WHERE id=9) AS m",
    ] {
        let result = s.execute(sql).unwrap();
        assert_eq!(result.column_types, vec![Some("int2".into())], "{sql}");
        assert_eq!(result.rows, vec![vec![SqlValue::Null]]);
    }
}
