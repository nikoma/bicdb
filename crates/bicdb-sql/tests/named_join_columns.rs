use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn postgres_named_joins_expose_one_common_column_and_keep_qualified_columns() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE using_l (value text, k int); CREATE TABLE using_r (k bigint, description text)")
        .unwrap();
    session
        .execute("INSERT INTO using_l VALUES ('left', 1), ('pair', 2); INSERT INTO using_r VALUES (2, 'pair'), (3, 'right')")
        .unwrap();

    let result = session
        .execute("SELECT * FROM using_l l FULL JOIN using_r r USING (k) ORDER BY k")
        .unwrap();
    assert_eq!(result.columns, ["k", "value", "description"]);
    assert_eq!(result.column_types[0].as_deref(), Some("int8"));
    assert_eq!(
        result.rows,
        [
            vec![
                SqlValue::Int(1),
                SqlValue::String("left".into()),
                SqlValue::Null
            ],
            vec![
                SqlValue::Int(2),
                SqlValue::String("pair".into()),
                SqlValue::String("pair".into())
            ],
            vec![
                SqlValue::Int(3),
                SqlValue::Null,
                SqlValue::String("right".into())
            ],
        ],
    );
    let result = session
        .execute("SELECT k, l.k, r.k FROM using_l l RIGHT JOIN using_r r USING(k) WHERE k = 3")
        .unwrap();
    assert_eq!(
        result.rows,
        [vec![SqlValue::Int(3), SqlValue::Null, SqlValue::Int(3)]],
    );

    let result = session
        .execute("SELECT * FROM using_l NATURAL JOIN using_r")
        .unwrap();
    assert_eq!(result.columns, ["k", "value", "description"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], SqlValue::Int(2));
    let result = session
        .execute("SELECT l.*, r.* FROM using_l l JOIN using_r r USING(k)")
        .unwrap();
    assert_eq!(result.columns, ["value", "k", "k", "description"]);
    assert_eq!(
        session
            .execute("SELECT * FROM using_l JOIN using_r USING (none)")
            .unwrap_err()
            .sqlstate(),
        "42703",
    );
}

#[test]
fn postgres_chained_named_joins_bind_the_previous_join_output() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE first_input (a text, k int);
             CREATE TABLE second_input (k int, b text);
             CREATE TABLE third_input (k int, c text);
             INSERT INTO first_input VALUES ('a', 1);
             INSERT INTO second_input VALUES (2, 'b');
             INSERT INTO third_input VALUES (2, 'c')",
        )
        .unwrap();
    let result = session
        .execute(
            "SELECT * FROM first_input a FULL JOIN second_input b USING (k)
             JOIN third_input c USING (k) WHERE k > 0 ORDER BY k",
        )
        .unwrap();
    assert_eq!(result.columns, ["k", "a", "b", "c"]);
    assert_eq!(
        result.rows,
        [vec![
            SqlValue::Int(2),
            SqlValue::Null,
            SqlValue::String("b".into()),
            SqlValue::String("c".into()),
        ]],
    );
    let result = session
        .execute(
            "SELECT k, c.c FROM first_input a FULL JOIN second_input b USING (k)
             JOIN (SELECT k AS key, c FROM third_input) c ON c.key = k",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        [vec![SqlValue::Int(2), SqlValue::String("c".into())]],
    );
}
#[test]
fn postgres_from_column_aliases_rename_a_prefix_and_validate_the_source_width() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE alias_data (id integer PRIMARY KEY, body text); INSERT INTO alias_data VALUES (1, 'one'), (2, 'two')").unwrap();
    let result = session
        .execute("SELECT item.* FROM alias_data AS item(renamed) WHERE renamed=2")
        .unwrap();
    assert_eq!(result.columns, ["renamed", "body"]);
    assert_eq!(
        result.rows,
        [[SqlValue::Int(2), SqlValue::String("two".into())]]
    );
    assert_eq!(result.column_types[0].as_deref(), Some("int4"));
    assert_eq!(result.column_metadata[0].attribute_number, 1);
    assert_ne!(result.column_metadata[0].table_oid, 0);
    let result = session.execute("WITH data(new_id) AS (SELECT id, body FROM alias_data) SELECT nested.* FROM (SELECT new_id, body FROM data) nested(k) WHERE k=1").unwrap();
    assert_eq!(result.columns, ["k", "body"]);
    assert_eq!(
        result.rows,
        [[SqlValue::Int(1), SqlValue::String("one".into())]]
    );
    let result = session.execute("SELECT r.renamed FROM alias_data l JOIN alias_data r(renamed) ON l.id=r.renamed WHERE l.id=1").unwrap();
    assert_eq!(result.rows, [[SqlValue::Int(1)]]);
    let error = session
        .execute("SELECT count(*) FROM alias_data item(a,b,c)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42P10");
    assert!(error
        .to_string()
        .contains("table \"item\" has 2 columns available but 3 columns specified"));
}
