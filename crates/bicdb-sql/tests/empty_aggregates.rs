use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn empty_ungrouped_aggregates_return_one_postgresql_shaped_row() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE empty_aggregate_rows (
                id UUID PRIMARY KEY,
                amount INT,
                enabled BOOLEAN,
                payload JSONB
            )",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT COUNT(*), COUNT(amount), SUM(amount), AVG(amount),
                    MIN(id), MAX(id), BOOL_AND(enabled), BOOL_OR(enabled),
                    ARRAY_AGG(id), JSON_AGG(payload), JSONB_AGG(payload)
             FROM empty_aggregate_rows
             WHERE id = '00000000-0000-0000-0000-000000000999'",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![
            SqlValue::Int(0),
            SqlValue::Int(0),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ]
    );
    assert_eq!(
        result.column_types,
        vec![
            Some("int8".to_string()),
            Some("int8".to_string()),
            Some("int8".to_string()),
            Some("numeric".to_string()),
            Some("uuid".to_string()),
            Some("uuid".to_string()),
            Some("bool".to_string()),
            Some("bool".to_string()),
            Some("uuid[]".to_string()),
            Some("json".to_string()),
            Some("jsonb".to_string()),
        ]
    );
}

#[test]
fn empty_join_aggregates_are_ungrouped_while_grouped_aggregates_stay_empty() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE aggregate_parents (id UUID PRIMARY KEY);
             CREATE TABLE aggregate_children (
                id UUID PRIMARY KEY,
                parent_id UUID NOT NULL REFERENCES aggregate_parents(id),
                amount INT
             )",
        )
        .unwrap();

    let ungrouped = session
        .execute(
            "SELECT COUNT(*), SUM(children.amount), MIN(parents.id)
             FROM aggregate_parents parents
             JOIN aggregate_children children ON children.parent_id = parents.id",
        )
        .unwrap();
    assert_eq!(
        ungrouped.rows,
        vec![vec![SqlValue::Int(0), SqlValue::Null, SqlValue::Null]]
    );
    assert_eq!(
        ungrouped.column_types,
        vec![
            Some("int8".to_string()),
            Some("int8".to_string()),
            Some("uuid".to_string()),
        ]
    );

    let grouped = session
        .execute(
            "SELECT parents.id, COUNT(*)
             FROM aggregate_parents parents
             JOIN aggregate_children children ON children.parent_id = parents.id
             GROUP BY parents.id",
        )
        .unwrap();
    assert!(grouped.rows.is_empty());
}
