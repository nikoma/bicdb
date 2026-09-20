use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn cells(rows: &[Vec<SqlValue>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(SqlValue::to_cell).collect())
        .collect()
}

#[test]
fn numeric_primary_and_unique_keys_use_postgres_equality_across_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE typed_keys (
                    id NUMERIC PRIMARY KEY,
                    external_id NUMERIC UNIQUE
                )",
            )
            .unwrap();
        session
            .execute("INSERT INTO typed_keys VALUES (1.0, 9007199254740993.010)")
            .unwrap();

        let primary_duplicate = session
            .execute("INSERT INTO typed_keys VALUES (1.00, 2)")
            .unwrap_err();
        assert_eq!(primary_duplicate.sqlstate(), "23505");
        let unique_duplicate = session
            .execute("INSERT INTO typed_keys VALUES (2, 9007199254740993.0100)")
            .unwrap_err();
        assert_eq!(unique_duplicate.sqlstate(), "23505");
    }

    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        cells(
            &session
                .execute("SELECT external_id FROM typed_keys WHERE id = 1.000")
                .unwrap()
                .rows
        ),
        vec![vec!["9007199254740993.010".to_string()]]
    );
    let duplicate = session
        .execute("INSERT INTO typed_keys VALUES (3, 9007199254740993.01)")
        .unwrap_err();
    assert_eq!(duplicate.sqlstate(), "23505");
}

#[test]
fn foreign_keys_use_parent_types_for_validation_and_cascade() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE typed_parent (id TEXT PRIMARY KEY, code NUMERIC UNIQUE)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE typed_child (
                id TEXT PRIMARY KEY,
                parent_id NUMERIC REFERENCES typed_parent(code)
                    ON UPDATE CASCADE ON DELETE RESTRICT
            )",
        )
        .unwrap();
    session
        .execute("INSERT INTO typed_parent VALUES ('parent', 1.0)")
        .unwrap();
    session
        .execute("INSERT INTO typed_child VALUES ('child', 1.000)")
        .unwrap();
    session
        .execute("UPDATE typed_parent SET code = 2.00 WHERE id = 'parent'")
        .unwrap();
    assert_eq!(
        cells(
            &session
                .execute("SELECT parent_id FROM typed_child WHERE id = 'child'")
                .unwrap()
                .rows
        ),
        vec![vec!["2.00".to_string()]]
    );
    assert_eq!(
        session
            .execute("DELETE FROM typed_parent WHERE id = 'parent'")
            .unwrap_err()
            .sqlstate(),
        "23503"
    );
}

#[test]
fn relational_operators_share_typed_numeric_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE typed_left (id TEXT PRIMARY KEY, amount NUMERIC)")
        .unwrap();
    session
        .execute("CREATE TABLE typed_right (id TEXT PRIMARY KEY, amount NUMERIC)")
        .unwrap();
    session
        .execute(
            "INSERT INTO typed_left VALUES
                ('a', 1.0), ('b', 1.00), ('c', 10),
                ('d', 2), ('e', 9007199254740993.01)",
        )
        .unwrap();
    session
        .execute("INSERT INTO typed_right VALUES ('match', 1.000), ('large', 9007199254740993.010)")
        .unwrap();

    let ordered = session
        .execute("SELECT amount FROM typed_left ORDER BY amount")
        .unwrap();
    assert_eq!(
        cells(&ordered.rows),
        vec![
            vec!["1.0".to_string()],
            vec!["1.00".to_string()],
            vec!["2".to_string()],
            vec!["10".to_string()],
            vec!["9007199254740993.01".to_string()],
        ]
    );

    let distinct = session
        .execute("SELECT DISTINCT amount FROM typed_left ORDER BY amount")
        .unwrap();
    assert_eq!(
        cells(&distinct.rows),
        vec![
            vec!["1.0".to_string()],
            vec!["2".to_string()],
            vec!["10".to_string()],
            vec!["9007199254740993.01".to_string()],
        ]
    );

    let grouped = session
        .execute("SELECT amount, COUNT(*) FROM typed_left GROUP BY amount ORDER BY amount")
        .unwrap();
    assert_eq!(
        cells(&grouped.rows),
        vec![
            vec!["1.0".to_string(), "2".to_string()],
            vec!["2".to_string(), "1".to_string()],
            vec!["10".to_string(), "1".to_string()],
            vec!["9007199254740993.01".to_string(), "1".to_string()],
        ]
    );

    let joined = session
        .execute(
            "SELECT l.id, r.id
             FROM typed_left l JOIN typed_right r ON l.amount = r.amount
             ORDER BY l.id",
        )
        .unwrap();
    assert_eq!(
        cells(&joined.rows),
        vec![
            vec!["a".to_string(), "match".to_string()],
            vec!["b".to_string(), "match".to_string()],
            vec!["e".to_string(), "large".to_string()],
        ]
    );

    let union = session
        .execute("SELECT 1.0::numeric AS n UNION SELECT 1.00::numeric")
        .unwrap();
    assert_eq!(union.rows.len(), 1);
    let intersect = session
        .execute("SELECT 1.0::numeric AS n INTERSECT SELECT 1.00::numeric")
        .unwrap();
    assert_eq!(intersect.rows.len(), 1);
    let except = session
        .execute("SELECT 1.0::numeric AS n EXCEPT SELECT 1.00::numeric")
        .unwrap();
    assert!(except.rows.is_empty());
    let ordered_union = session
        .execute(
            "SELECT 10::numeric AS n
             UNION ALL SELECT 2::numeric
             UNION ALL SELECT 9007199254740993.01::numeric
             ORDER BY n",
        )
        .unwrap();
    assert_eq!(
        cells(&ordered_union.rows),
        vec![
            vec!["2".to_string()],
            vec!["10".to_string()],
            vec!["9007199254740993.01".to_string()],
        ]
    );
}

#[test]
fn exclusion_equality_uses_the_declared_type() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE typed_bookings (
                id TEXT PRIMARY KEY,
                account NUMERIC NOT NULL,
                occupied INT4RANGE NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "ALTER TABLE typed_bookings
             ADD CONSTRAINT typed_bookings_no_overlap
             EXCLUDE USING gist (account WITH =, occupied WITH &&)",
        )
        .unwrap();
    session
        .execute("INSERT INTO typed_bookings VALUES ('first', 7.0, '[1,5)'::int4range)")
        .unwrap();
    let conflict = session
        .execute("INSERT INTO typed_bookings VALUES ('second', 7.00, '[4,8)'::int4range)")
        .unwrap_err();
    assert_eq!(conflict.sqlstate(), "23P01");
    session
        .execute("INSERT INTO typed_bookings VALUES ('other', 8.00, '[4,8)'::int4range)")
        .unwrap();
}

#[test]
fn indexed_range_ordering_matches_postgresql_bound_and_empty_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ordered_ranges (id TEXT PRIMARY KEY, span INT4RANGE NOT NULL)")
        .unwrap();
    session
        .execute("CREATE INDEX ordered_ranges_span_idx ON ordered_ranges (span)")
        .unwrap();
    session
        .execute(
            "INSERT INTO ordered_ranges VALUES
             ('closed', '[1,3]'), ('short', '[1,2)'), ('empty', 'empty'),
             ('unbounded', '(,)'), ('long', '[1,3)')",
        )
        .unwrap();

    assert_eq!(
        cells(
            &session
                .execute("SELECT id FROM ordered_ranges ORDER BY span")
                .unwrap()
                .rows,
        ),
        vec![
            vec!["empty".to_string()],
            vec!["unbounded".to_string()],
            vec!["short".to_string()],
            vec!["long".to_string()],
            vec!["closed".to_string()],
        ]
    );
    assert_eq!(
        cells(
            &session
                .execute(
                    "SELECT id FROM ordered_ranges
                     WHERE span < '[1,3)'::int4range ORDER BY span",
                )
                .unwrap()
                .rows,
        ),
        vec![
            vec!["empty".to_string()],
            vec!["unbounded".to_string()],
            vec!["short".to_string()],
        ]
    );
}
