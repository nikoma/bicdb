use bicdb_core::{query_budget::QueryMemoryScope, BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn selective_virtual_base_filter_limits_catalog_join_work_with_a_left_join() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    let columns = (1..16)
        .map(|i| format!("c{i} TEXT"))
        .collect::<Vec<_>>()
        .join(",");
    for i in 0..96 {
        session
            .execute(&format!(
                "CREATE TABLE catalog_budget_{i}(id INT PRIMARY KEY,{columns})"
            ))
            .unwrap();
    }
    let oid = session
        .execute("SELECT oid FROM pg_catalog.pg_class WHERE relname='catalog_budget_0'")
        .unwrap()
        .rows[0][0]
        .to_cell()
        .parse::<i64>()
        .unwrap();
    // A small final result must not build the catalog-wide join first. This is
    // the general metadata shape used by clients resolving result-column origins.
    let query = format!(
        "SELECT c.oid,a.attnum,a.attname,c.relname,n.nspname, \
         a.attnotnull OR (t.typtype='d' AND t.typnotnull), \
         a.attidentity!='' OR pg_catalog.pg_get_expr(d.adbin,d.adrelid) LIKE '%nextval(%' \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid \
         JOIN pg_catalog.pg_attribute a ON c.oid=a.attrelid \
         JOIN pg_catalog.pg_type t ON a.atttypid=t.oid \
         LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum \
         JOIN (SELECT {oid} AS oid,1 AS attnum UNION ALL SELECT {oid},2 \
               UNION ALL SELECT {oid},3 UNION ALL SELECT {oid},4) vals \
           ON c.oid=vals.oid AND a.attnum=vals.attnum \
         WHERE c.oid IN ({oid}) ORDER BY a.attnum"
    );
    let limit = 2 * 1024 * 1024;
    let memory = QueryMemoryScope::new(limit);
    let result = session.execute(&query).unwrap();
    assert_eq!(result.rows.len(), 4);
    for (index, row) in result.rows.iter().enumerate() {
        assert_eq!(row[0], SqlValue::Int(oid));
        assert_eq!(row[1], SqlValue::Int(index as i64 + 1));
        assert_eq!(
            row[2].to_cell(),
            if index == 0 {
                "id".into()
            } else {
                format!("c{index}")
            }
        );
        assert_eq!(row[3].to_cell(), "catalog_budget_0");
        assert_eq!(row[4].to_cell(), "public");
        assert_eq!(row[5], SqlValue::Bool(index == 0));
        assert_eq!(row[6], SqlValue::Null);
    }
    assert!(memory.peak_bytes() > 0, "join work must still be charged");
    assert!(memory.peak_bytes() <= limit);
}

#[test]
fn base_where_filter_does_not_create_right_or_full_null_extended_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE outer_filter_l(id INT,v TEXT); INSERT INTO outer_filter_l VALUES(1,'present'),(3,'orphan')").unwrap();
    for join in ["RIGHT JOIN", "FULL JOIN"] {
        let query = format!("SELECT l.id,r.id FROM outer_filter_l l {join} (SELECT 1 AS id UNION ALL SELECT 2) r ON l.id=r.id WHERE l.v IS NULL ORDER BY r.id");
        assert_eq!(
            session.execute(&query).unwrap().rows,
            vec![vec![SqlValue::Null, SqlValue::Int(2)]],
            "{query}"
        );
    }
}

#[test]
fn virtual_base_where_filter_preserves_right_and_full_matches() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE virtual_outer_probe(id INT)")
        .unwrap();
    let oid = session
        .execute("SELECT oid FROM pg_catalog.pg_class WHERE relname='virtual_outer_probe'")
        .unwrap()
        .rows[0][0]
        .to_cell();
    for join in ["RIGHT JOIN", "FULL JOIN"] {
        let query=format!("SELECT c.oid,r.oid FROM pg_catalog.pg_class c {join} (SELECT {oid} AS oid) r ON c.oid=r.oid WHERE c.oid IS NULL");
        assert!(session.execute(&query).unwrap().rows.is_empty(), "{query}");
    }
}

#[test]
fn nullable_right_where_filter_stays_after_left_join_matching() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE left_filter_l(id INT); CREATE TABLE left_filter_r(id INT,v TEXT); INSERT INTO left_filter_l VALUES(1),(2); INSERT INTO left_filter_r VALUES(1,'present')").unwrap();
    assert_eq!(session.execute("SELECT l.id FROM left_filter_l l LEFT JOIN left_filter_r r ON l.id=r.id WHERE r.v IS NULL").unwrap().rows,vec![vec![SqlValue::Int(2)]]);
}

#[test]
fn quoted_case_distinct_alias_does_not_push_nullable_predicate_into_the_base() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE alias_left_probe(id INT); CREATE TABLE alias_right_probe(id INT)")
        .unwrap();
    let left = session
        .execute("SELECT oid FROM pg_catalog.pg_class WHERE relname='alias_left_probe'")
        .unwrap()
        .rows[0][0]
        .to_cell();
    let right = session
        .execute("SELECT oid FROM pg_catalog.pg_class WHERE relname='alias_right_probe'")
        .unwrap()
        .rows[0][0]
        .to_cell();
    let query = format!(
        r#"SELECT "c".oid FROM pg_catalog.pg_class "c" LEFT JOIN (SELECT {right} AS oid) "C" ON "c".oid="C".oid WHERE "c".oid IN ({left},{right}) AND "C".oid IS NULL"#
    );
    assert_eq!(
        session.execute(&query).unwrap().rows,
        vec![vec![SqlValue::Int(left.parse().unwrap())]]
    );
}
