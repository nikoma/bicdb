//! Catalog joins must fit their referenced columns within the query budget.
use bicdb_core::{query_budget::QueryMemoryScope, BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn narrow_catalog_join_does_not_retain_unreferenced_metadata() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    for table in 0..16 {
        let fields = (0..24).map(|i| format!("probe_{i} integer")).collect::<Vec<_>>().join(",");
        session.execute(&format!("CREATE TABLE catalog_projection_{table}({fields})")).unwrap();
    }
    let sql = "SELECT a.attnum,t.typname FROM pg_attribute a JOIN pg_type t ON a.atttypid=t.oid ORDER BY a.attnum,t.typname";
    let expected = session.execute(sql).unwrap();
    assert!(expected.rows.len() >= 384);
    {
        let memory = QueryMemoryScope::new(1024 * 1024);
        assert_eq!(session.execute(sql).unwrap().rows, expected.rows);
        assert!(memory.peak_bytes() <= 1024 * 1024);
    }
    // Wildcards and whole-row values still expose all catalog columns.
    let star = session.execute("SELECT a.* FROM pg_attribute a WHERE a.attname='probe_23'").unwrap();
    assert_eq!(star.rows.len(), 16);
    assert!(star.columns.len() > 10);
    assert!(star.rows.iter().all(|r| r.len() == star.columns.len()));
    let whole = session.execute("SELECT a::text FROM pg_attribute a WHERE a.attname='probe_23'").unwrap();
    assert_eq!(whole.rows.len(), 16);
    assert!(whole.rows.iter().all(|r| matches!(&r[0], SqlValue::String(s) if s.contains("probe_23"))));
}
