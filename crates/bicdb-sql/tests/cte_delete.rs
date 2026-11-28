//! Top-level WITH DELETE shares the native DELETE transaction/CTE path.
//! Results independently checked against PostgreSQL16.13.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlErrorField, SqlSession, SqlValue};
#[test]
fn top_level_with_delete_uses_ctes_for_using_returning_and_subqueries() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE compat_cte_delete(id int primary key,v text)")
        .unwrap();
    session
        .execute("INSERT INTO compat_cte_delete VALUES(1,'one'),(2,'two'),(3,'three')")
        .unwrap();
    let rows=session.execute("WITH doomed AS (SELECT 2 AS id) DELETE FROM compat_cte_delete t USING doomed d WHERE t.id=d.id RETURNING t.id,t.v").unwrap().rows;
    assert_eq!(
        rows.iter()
            .map(|r| r.iter().map(|v| v.to_cell()).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec!["2", "two"]]
    );
    session.execute("WITH doomed AS (SELECT 1 AS id) DELETE FROM compat_cte_delete WHERE id IN (SELECT id FROM doomed)").unwrap();
    let rows = session
        .execute("SELECT * FROM compat_cte_delete ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(
        rows.iter()
            .map(|r| r.iter().map(|v| v.to_cell()).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec!["3", "three"]]
    );
}

#[test]
fn top_level_delete_rolls_back_cte_mutations_on_constraint_failure() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE parent(id int PRIMARY KEY)",
        "CREATE TABLE child(id int REFERENCES parent(id))",
        "INSERT INTO parent VALUES(1)",
        "INSERT INTO child VALUES(1)",
    ] {
        session.execute(sql).unwrap();
    }
    let error=session.execute("WITH added AS (INSERT INTO parent VALUES(2) RETURNING id) DELETE FROM parent WHERE id=1").unwrap_err();
    assert_eq!(error.sqlstate(), "23503");
    assert_eq!(
        session
            .execute("SELECT id FROM parent ORDER BY id")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "1"
    );
    assert_eq!(
        session.execute("SELECT count(*) FROM parent").unwrap().rows[0][0].to_cell(),
        "1"
    );
    session.execute("BEGIN").unwrap();
    session
        .execute(
            "WITH doomed AS (SELECT 1 AS id) DELETE FROM child WHERE id IN(SELECT id FROM doomed)",
        )
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session.execute("SELECT count(*) FROM child").unwrap().rows[0][0].to_cell(),
        "1"
    );
}

#[test]
fn postgres_dml_target_alias_hides_the_original_relation_name() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE alias_target(id int PRIMARY KEY); INSERT INTO alias_target VALUES (1), (2)")
        .unwrap();

    for statement in [
        "DELETE FROM alias_target AS t WHERE alias_target.id = 1",
        "DELETE FROM public.alias_target AS t WHERE public.alias_target.id = 1",
        "DELETE FROM alias_target AS t WHERE EXISTS (SELECT alias_target.id)",
        "DELETE FROM alias_target AS t WHERE t.id = 1 RETURNING alias_target.*",
        "WITH search AS (SELECT 1 AS id) DELETE FROM alias_target AS t USING search WHERE alias_target.id = search.id",
        "UPDATE alias_target AS t SET id = alias_target.id + 10",
    ] {
        let error = session.execute(statement).unwrap_err();
        assert_eq!(error.sqlstate(), "42P01", "{statement}: {error}");
        assert_eq!(
            error.postgres_message(),
            "invalid reference to FROM-clause entry for table \"alias_target\"",
            "{statement}",
        );
        assert!(error.fields().contains(&SqlErrorField::Hint(
            "Perhaps you meant to reference the table alias \"t\".".to_owned(),
        )));
    }
    assert_eq!(
        session
            .execute("SELECT id FROM alias_target ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]],
    );
    assert_eq!(
        session
            .execute("DELETE FROM alias_target AS t WHERE t.id = (SELECT alias_target.id FROM alias_target WHERE id = 2) RETURNING t.id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]],
    );
}

#[test]
fn postgres_aliased_dml_target_can_join_its_original_name() {
    for partitioned in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        let partition_clause = if partitioned {
            " PARTITION BY RANGE (id)"
        } else {
            ""
        };
        session
            .execute(&format!(
                "CREATE TABLE target_source(id int PRIMARY KEY, label text){partition_clause}"
            ))
            .unwrap();
        if partitioned {
            session.execute("CREATE TABLE target_source_rows PARTITION OF target_source FOR VALUES FROM (0) TO (10)").unwrap();
        }
        session
            .execute("INSERT INTO target_source VALUES (1, 'one'), (2, 'two')")
            .unwrap();

        let changed = session
            .execute(
                "UPDATE target_source t SET label = target_source.label FROM target_source
             WHERE t.id = target_source.id + 1 RETURNING t.id, target_source.id, t.label",
            )
            .unwrap();
        assert_eq!(
            changed.rows,
            vec![vec![
                SqlValue::Int(2),
                SqlValue::Int(1),
                SqlValue::String("one".into())
            ]]
        );

        let deleted = session
            .execute(
                "DELETE FROM target_source t USING target_source
             WHERE t.id = target_source.id + 1 RETURNING t.id, target_source.id, t.label",
            )
            .unwrap();
        assert_eq!(deleted.rows, changed.rows);
        assert_eq!(deleted.command_tag.as_deref(), Some("DELETE 1"));
        let empty = session.execute(
            "DELETE FROM target_source t USING target_source WHERE false RETURNING target_source.id",
        ).unwrap();
        assert_eq!(empty.columns, vec!["id"]);
        assert!(empty.rows.is_empty());
        assert_eq!(empty.command_tag.as_deref(), Some("DELETE 0"));
        assert_eq!(
            session
                .execute("SELECT id, label FROM target_source")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1), SqlValue::String("one".into())]]
        );
    }
}
