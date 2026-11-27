use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn qualified_arrays_keep_their_type_and_postgres_element_comparison() {
    for storage_mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default().with_storage_mode(storage_mode),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE arrays (id int PRIMARY KEY, a text[])",
            r#"INSERT INTO arrays VALUES (1, '{x,"y z",NULL}'), (2, '{x,"y z",z}'), (3, '{x}')"#,
            "CREATE TABLE array_bounds (a int[])",
            "INSERT INTO array_bounds VALUES ('[2:3]={1,NULL}'), ('{1,NULL}')",
        ] {
            session.execute(sql).unwrap();
        }
        for (sql, expected) in [
            (
                r#"SELECT count(*) FROM arrays t WHERE t.a = '{x,"y z",NULL}'"#,
                1,
            ),
            (
                r#"SELECT count(*) FROM arrays t WHERE t.a::text = '{x,"y z",NULL}'"#,
                1,
            ),
            (
                r#"SELECT count(*) FROM arrays t WHERE t.a::varchar = '{x,"y z",NULL}'"#,
                1,
            ),
            (
                r#"SELECT count(*) FROM arrays t WHERE t.a IS NOT DISTINCT FROM '{x,"y z",NULL}'"#,
                1,
            ),
            (
                r#"SELECT count(*) FROM arrays t WHERE t.a > '{x,"y z",z}'"#,
                1,
            ),
            (
                r#"SELECT count(*) FROM arrays t WHERE t.a IN ('{x,"y z",NULL}', '{x}')"#,
                2,
            ),
            (
                r#"WITH c AS (SELECT a FROM arrays) SELECT count(*) FROM c WHERE c.a = '{x,"y z",NULL}'"#,
                1,
            ),
            (
                "SELECT count(*) FROM array_bounds b WHERE b.a = '[2:3]={1,NULL}'",
                1,
            ),
        ] {
            assert_eq!(
                session.execute(sql).unwrap().rows,
                vec![vec![SqlValue::Int(expected)]],
                "{storage_mode:?}: {sql}"
            );
        }
        assert_eq!(
            session
                .execute(r#"SELECT '{1,2}' = '{1, 2}'"#)
                .unwrap()
                .rows,
            vec![vec![SqlValue::Bool(false)]]
        );
    }
}
