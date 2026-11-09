//! An uncorrelated scalar bound should keep a selective secondary-index probe.
use crate::*;

#[test]
fn scalar_index_bound_with_residual_membership_does_not_materialize_the_table() {
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
        CREATE INDEX fields_section ON fields(section_id);
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
        "SELECT id FROM fields WHERE active='Y'
        AND section_id=(SELECT min(id) FROM sections WHERE scope_id=7 AND active='Y')
        AND hidden IS NULL
        AND id NOT IN(SELECT id FROM seen WHERE scope_id=11 AND id IS NOT NULL)
        AND (id IN(SELECT id FROM visible WHERE scope_id=11)
             OR (SELECT count(*) FROM visible WHERE scope_id=11)=0)
        ORDER BY coalesce(active,'Y') DESC,id",
    );
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    let result = result.unwrap();
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r[0].to_cell())
            .collect::<Vec<_>>(),
        (0..50)
            .map(|i| (3 + i * 100).to_string())
            .collect::<Vec<_>>()
    );
    assert!(
        stats.rows_materialized <= 100,
        "{stats:?}; selective index should fetch 50 of 5000 outer rows"
    );
}
