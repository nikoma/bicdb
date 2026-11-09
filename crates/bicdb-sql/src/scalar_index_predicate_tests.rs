//! An uncorrelated scalar bound should keep a selective secondary-index probe.
use crate::*;

fn check_scalar_index_bound(bound: &str, expected: Vec<String>) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default()
            .with_storage_mode(bicdb_core::StorageMode::ServerPaged)
            .with_fsync(false),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE sections(id NUMERIC(10) PRIMARY KEY, scope_id NUMERIC(10), active CHAR(1));
        CREATE TABLE fields(id NUMERIC(10) PRIMARY KEY, section_id NUMERIC(10), active CHAR(1), hidden VARCHAR(1));
        CREATE UNIQUE INDEX fields_section ON fields(section_id,id) NULLS NOT DISTINCT;
        CREATE TABLE seen(id NUMERIC(10), scope_id NUMERIC(10));
        CREATE TABLE visible(id NUMERIC(10), scope_id NUMERIC(10));
        INSERT INTO sections VALUES(3,7,'Y'),(9,7,'Y'),(1,8,'Y')").unwrap();
    let values = (1..=5000)
        .map(|id| format!("({id},{},'Y',NULL)", (id - 1) % 100 + 1))
        .collect::<Vec<_>>()
        .join(",");
    session
        .execute(&format!("INSERT INTO fields VALUES {values}"))
        .unwrap();
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = session.execute(
        &"SELECT id FROM fields WHERE active='Y'
        AND section_id=__BOUND__
        AND hidden IS NULL
        AND id NOT IN(SELECT id FROM seen WHERE scope_id=11 AND id IS NOT NULL)
        AND (id IN(SELECT id FROM visible WHERE scope_id=11)
             OR (SELECT count(*) FROM visible WHERE scope_id=11)=0)
        ORDER BY coalesce(active,'Y') DESC,id"
            .replace("__BOUND__", bound),
    );
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    let result = result.unwrap();
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r[0].to_cell())
            .collect::<Vec<_>>(),
        expected
    );
    assert!(
        stats.rows_materialized <= 100,
        "{stats:?}; selective index should fetch 50 of 5000 outer rows"
    );
}

#[test]
fn scalar_index_bound_with_residual_membership_does_not_materialize_the_table() {
    check_scalar_index_bound(
        "(SELECT min(id) FROM sections WHERE scope_id=7 AND active='Y')",
        (0..50).map(|i| (3 + i * 100).to_string()).collect(),
    );
}

#[test]
fn null_scalar_aggregate_index_bound_does_not_materialize_the_table() {
    check_scalar_index_bound(
        "(SELECT min(id) FROM sections WHERE scope_id=404 AND active='Y')",
        Vec::new(),
    );
}

#[test]
fn null_scalar_projection_index_bound_does_not_materialize_the_table() {
    check_scalar_index_bound("(SELECT CAST(NULL AS NUMERIC))", Vec::new());
}

#[test]
fn correlated_scalar_null_preserves_later_bindings_and_outer_join_rows() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default()
                .with_storage_mode(mode)
                .with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE outer_choices(id INT PRIMARY KEY,wanted INT);
            CREATE TABLE target_matches(id INT PRIMARY KEY,lookup_key INT);
            CREATE INDEX target_lookup ON target_matches(lookup_key);
            INSERT INTO outer_choices VALUES(1,NULL),(2,2),(3,3);
            INSERT INTO target_matches VALUES(10,2),(11,3)",
            )
            .unwrap();
        let expected = vec![
            vec![SqlValue::Int(1), SqlValue::Null],
            vec![SqlValue::Int(2), SqlValue::Int(10)],
            vec![SqlValue::Int(3), SqlValue::Int(11)],
        ];
        for query in [
            "SELECT o.id,t.id FROM outer_choices o LEFT JOIN target_matches t ON t.lookup_key=(SELECT o.wanted) ORDER BY o.id,t.id",
            "SELECT o.id,t.id FROM outer_choices o LEFT JOIN LATERAL (SELECT r.id FROM target_matches r WHERE r.lookup_key=(SELECT o.wanted))t ON TRUE ORDER BY o.id,t.id",
        ] {
            assert_eq!(session.execute(query).unwrap().rows,expected,"{query}");
        }
    }
}
