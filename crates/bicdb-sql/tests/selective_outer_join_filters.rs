use bicdb_core::{query_budget::QueryMemoryScope, BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn mixed_join_base_filter_folds_unquoted_identifiers_before_schema_lookup() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_storage_mode(mode)
                .with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE mixed_drivers(Driver_ID INT PRIMARY KEY); INSERT INTO mixed_drivers SELECT g FROM generate_series(0,255) g; CREATE TABLE mixed_columns(Column_ID INT PRIMARY KEY,Driver_ID INT,Rule_ID INT,Element_ID INT,IsActive TEXT,Payload TEXT); CREATE INDEX mixed_columns_driver ON mixed_columns(Driver_ID); INSERT INTO mixed_columns SELECT g,g%256,CASE WHEN g%2=0 THEN 0 ELSE NULL END,g%16,'Y',repeat('x',4096) FROM generate_series(0,999) g; CREATE TABLE mixed_rules(Rule_ID INT PRIMARY KEY,Rule_Name TEXT); INSERT INTO mixed_rules VALUES(0,'present'); CREATE TABLE mixed_elements(Element_ID INT PRIMARY KEY); INSERT INTO mixed_elements SELECT g FROM generate_series(0,15) g").unwrap();
        let query = "SELECT c.Column_ID,c.Payload,vr.Rule_Name FROM mixed_drivers t INNER JOIN mixed_columns c ON(t.Driver_ID=c.Driver_ID) LEFT JOIN mixed_rules vr ON(c.Rule_ID=vr.Rule_ID) INNER JOIN mixed_elements e ON(c.Element_ID=e.Element_ID) WHERE t.Driver_ID=1 AND c.IsActive='Y' ORDER BY c.Column_ID";
        for query in [
            query.to_string(),
            query
                .replace("t.", "T.")
                .replace("mixed_drivers t ", "mixed_drivers T "),
        ] {
            let limit = 2 * 1024 * 1024;
            let memory = QueryMemoryScope::new(limit);
            let rows = session.execute(&query).unwrap().rows;
            assert_eq!(rows.len(), 4);
            for (index, row) in rows.iter().enumerate() {
                assert_eq!(
                    row,
                    &vec![
                        SqlValue::Int(1 + 256 * index as i64),
                        SqlValue::String("x".repeat(4096)),
                        SqlValue::Null
                    ]
                );
            }
            assert!(memory.peak_bytes() > 0 && memory.peak_bytes() <= limit);
        }
        session.execute("CREATE TABLE mixed_case_guard(id INT PRIMARY KEY,\"ID\" INT); INSERT INTO mixed_case_guard VALUES(1,2),(2,1)").unwrap();
        assert_eq!(session.execute("SELECT l.id FROM mixed_case_guard l LEFT JOIN mixed_rules r ON l.id=r.Rule_ID WHERE l.\"ID\"=1").unwrap().rows,vec![vec![SqlValue::Int(2)]]);
        assert_eq!(session.execute("SELECT l.\"ID\" FROM mixed_case_guard l LEFT JOIN mixed_rules r ON l.id=r.Rule_ID WHERE l.ID=1").unwrap().rows,vec![vec![SqlValue::Int(2)]]);
        for query in [
            "SELECT l.id FROM mixed_case_guard l LEFT JOIN mixed_rules r ON l.id=r.Rule_ID WHERE l.\"ID\">1",
            "SELECT l.id FROM mixed_case_guard l LEFT JOIN mixed_rules r ON l.id=r.Rule_ID WHERE l.\"ID\" IN(2)",
            "SELECT l.\"ID\" FROM mixed_case_guard l LEFT JOIN mixed_rules r ON l.id=r.Rule_ID WHERE l.ID>1",
            "SELECT l.\"ID\" FROM mixed_case_guard l LEFT JOIN mixed_rules r ON l.id=r.Rule_ID WHERE l.ID IN(2)",
        ] {
            assert_eq!(session.execute(query).unwrap().rows, vec![vec![SqlValue::Int(1)]], "{query}");
        }
    }
}

#[test]
fn catalog_type_lookup_binds_unique_unqualified_columns_before_joining() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_storage_mode(mode)
                .with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE SCHEMA catalog_binding").unwrap();
        let columns = (0..16)
            .map(|i| format!("c{i} NUMERIC(10)"))
            .collect::<Vec<_>>()
            .join(",");
        for i in 0..16 {
            session
                .execute(&format!(
                    "CREATE TABLE catalog_binding.lookup_{i}({columns})"
                ))
                .unwrap();
        }
        for table in ["lower('LOOKUP_0')", "lower(CAST('LOOKUP_0' AS name))"] {
            let query = format!("SELECT pg_type.typname,format_type(pg_type.oid,pg_attribute.atttypmod) FROM pg_class,pg_attribute,pg_type,pg_namespace WHERE relname={table} AND relkind IN('r','p') AND pg_class.oid=pg_attribute.attrelid AND attname=lower('C0') AND atttypid=pg_type.oid AND pg_class.relnamespace=pg_namespace.oid AND pg_namespace.nspname='catalog_binding'");
            let limit = 2 * 1024 * 1024;
            let memory = QueryMemoryScope::new(limit);
            assert_eq!(
                session.execute(&query).unwrap().rows,
                vec![vec![
                    SqlValue::String("numeric".into()),
                    SqlValue::String("numeric(10,0)".into())
                ]]
            );
            assert!(memory.peak_bytes() <= limit);
        }
    }
}

#[test]
fn indexed_inner_join_filters_wide_right_rows_before_retaining_them() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_storage_mode(mode)
                .with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE selective_drivers(id INTEGER PRIMARY KEY); INSERT INTO selective_drivers VALUES(0),(1),(2),(3); CREATE TABLE selective_payloads(id INTEGER PRIMARY KEY,driver_id INTEGER,selected TEXT,payload TEXT); CREATE INDEX selective_payload_driver ON selective_payloads(driver_id)").unwrap();
        session.execute("INSERT INTO selective_payloads SELECT g,g%4,CASE WHEN g<4 THEN 'yes' ELSE NULL END,repeat('x',4096) FROM generate_series(0,999) g").unwrap();
        for query in [
            "SELECT w.id,w.payload FROM selective_drivers d JOIN selective_payloads w ON w.driver_id=d.id WHERE w.selected IS NOT NULL ORDER BY w.id",
            "SELECT SELECTIVE_PAYLOADS.id,SELECTIVE_PAYLOADS.payload FROM selective_drivers JOIN SELECTIVE_PAYLOADS ON SELECTIVE_PAYLOADS.driver_id=selective_drivers.id WHERE SELECTIVE_PAYLOADS.selected IS NOT NULL ORDER BY SELECTIVE_PAYLOADS.id",
            "SELECT \"w\".id,\"w\".payload FROM selective_drivers d JOIN selective_payloads \"w\" ON \"w\".driver_id=d.id WHERE \"w\".selected IS NOT NULL ORDER BY \"w\".id",
            "SELECT \"selective_payloads\".\"id\",\"selective_payloads\".\"payload\" FROM selective_drivers JOIN \"selective_payloads\" ON \"selective_payloads\".\"driver_id\"=selective_drivers.id WHERE \"selective_payloads\".\"selected\" IS NOT NULL ORDER BY \"selective_payloads\".\"id\"",
            "SELECT w.id,w.payload FROM selective_drivers d JOIN \"selective_payloads\" w ON w.driver_id=d.id WHERE w.selected IS NOT NULL ORDER BY w.id",
        ] {
            let limit = 2 * 1024 * 1024;
            let memory = QueryMemoryScope::new(limit);
            let result = session.execute(query).unwrap();
            assert_eq!(result.rows.len(), 4);
            for (id, row) in result.rows.iter().enumerate() {
                assert_eq!(row[0], SqlValue::Int(id as i64));
                assert_eq!(row[1].to_cell(), "x".repeat(4096));
            }
            assert!(memory.peak_bytes() > 0, "join work must still be charged");
            assert!(memory.peak_bytes() <= limit);
        }
    }
}

#[test]
fn indexed_right_filters_preserve_outer_rows_quoted_bindings_and_function_calls() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE filter_guard_l(id INTEGER PRIMARY KEY,v TEXT); CREATE TABLE filter_guard_r(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO filter_guard_l VALUES(1,NULL),(2,'left'),(3,'orphan'); INSERT INTO filter_guard_r VALUES(1,'right'),(2,'right'); CREATE SEQUENCE filter_guard_calls; CREATE FUNCTION filter_guard_tick() RETURNS BIGINT LANGUAGE plpgsql VOLATILE AS $$ BEGIN RETURN nextval('filter_guard_calls'); END $$").unwrap();
    assert_eq!(
        session.execute("SELECT l.id FROM filter_guard_l l LEFT JOIN filter_guard_r r ON l.id=r.id WHERE r.v IS NULL ORDER BY l.id").unwrap().rows,
        vec![vec![SqlValue::Int(3)]]
    );
    assert_eq!(
        session.execute(r#"SELECT "w".id FROM filter_guard_l "w" JOIN filter_guard_r "W" ON "w".id="W".id WHERE "w".v IS NULL ORDER BY "w".id"#).unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session.execute("SELECT l.id FROM filter_guard_l l JOIN filter_guard_r r ON l.id=r.id WHERE r.v='right' AND filter_guard_tick()>0 ORDER BY l.id").unwrap().rows,
        vec![vec![SqlValue::Int(1)],vec![SqlValue::Int(2)]]
    );
    assert_eq!(
        session
            .execute("SELECT nextval('filter_guard_calls')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3)]]
    );
}

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
    drop(memory);
    let uppercase = query
        .replace("c.oid", "C.OID")
        .replace("c.relname", "C.RELNAME");
    for query in [
        uppercase.clone(),
        uppercase.replace("pg_catalog.pg_class c ", "pg_catalog.pg_class C "),
    ] {
        let memory = QueryMemoryScope::new(limit);
        assert_eq!(session.execute(&query).unwrap().rows, result.rows);
        assert!(memory.peak_bytes() > 0 && memory.peak_bytes() <= limit);
    }
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
