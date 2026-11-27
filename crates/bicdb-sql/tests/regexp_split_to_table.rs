use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.to_string())
}

#[test]
fn regexp_split_to_table_matches_postgres_alias_filter_and_null_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let roles = session
        .execute(
            "SELECT role_name
             FROM regexp_split_to_table('patient,billing,support', ',') role_name
             WHERE role_name IN ('patient', 'support')
             ORDER BY role_name",
        )
        .unwrap();
    assert_eq!(
        roles.rows,
        vec![vec![text("patient")], vec![text("support")]]
    );

    let words = session
        .execute(
            "SELECT q.term
             FROM pg_catalog.regexp_split_to_table('one  TWO three', '[[:space:]]+', 'i')
                  WITH ORDINALITY AS q(term, ord)
             WHERE q.ord >= 2
             ORDER BY q.ord",
        )
        .unwrap();
    assert_eq!(words.rows, vec![vec![text("TWO")], vec![text("three")]]);

    let null_rows = session
        .execute("SELECT value FROM regexp_split_to_table(NULL::text, ',') AS value")
        .unwrap();
    assert!(null_rows.rows.is_empty());
    assert_eq!(null_rows.columns, vec!["value".to_string()]);
}

#[test]
fn regexp_split_to_table_ignores_postgres_zero_length_boundary_matches() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute("SELECT value FROM regexp_split_to_table('abc', '') AS value")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![text("a")], vec![text("b")], vec![text("c")]]
    );
}

#[test]
fn regexp_functions_expand_in_the_select_list_with_null_padding() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT pg_catalog.regexp_matches('a1 b2', '([a-z])([0-9])', 'g') AS m,
                    regexp_split_to_table('only', ',') AS s",
        )
        .unwrap();
    assert_eq!(result.columns, ["m", "s"]);
    assert_eq!(
        result.column_types,
        [Some("text[]".to_string()), Some("text".to_string())]
    );
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1], text("only"));
    assert_eq!(result.rows[1][1], SqlValue::Null);

    let result = session
        .execute(
            "SELECT src, regexp_matches(src, '([a-z])') AS m
               FROM (VALUES ('1'), ('a'), (NULL)) AS v(src)",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], text("a"));
}
