use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn catalog_default_expressions_preserve_literal_values_and_array_bounds() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE default_samples(id INT PRIMARY KEY, label VARCHAR DEFAULT 'it''s ready', n INT DEFAULT 7, enabled BOOLEAN DEFAULT true, data JSONB DEFAULT '{}'::jsonb, tags TEXT[] DEFAULT '{}', bounded INT[] DEFAULT '[0:1]={8,9}'::int[])").unwrap();
    let rows = session.execute("SELECT a.attname,pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid='default_samples'::regclass ORDER BY a.attnum").unwrap().rows;
    assert_eq!(rows.len(), 6);
    assert_eq!(
        rows[0][1],
        SqlValue::String("'it''s ready'::varchar".into())
    );
    assert_eq!(rows[1][1], SqlValue::String("7".into()));
    assert_eq!(rows[2][1], SqlValue::String("true".into()));
    assert_eq!(rows[3][1], SqlValue::String("'{}'::jsonb".into()));
    assert_eq!(rows[4][1], SqlValue::String("'{}'::text[]".into()));
    assert_eq!(rows[5][1], SqlValue::String("'[0:1]={8,9}'::int4[]".into()));
    // Reusing each catalog expression must preserve the original value.
    let expressions = rows
        .iter()
        .map(|r| r[1].to_cell())
        .collect::<Vec<_>>()
        .join(",");
    let projected = session
        .execute(&format!("SELECT {expressions}"))
        .unwrap()
        .rows;
    session
        .execute("INSERT INTO default_samples(id) VALUES(1)")
        .unwrap();
    let stored = session
        .execute("SELECT label,n,enabled,data,tags,bounded FROM default_samples")
        .unwrap()
        .rows;
    assert_eq!(projected, stored);
}

#[test]
fn index_definition_column_overload_returns_expressions_instead_of_whole_ddl() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE index_samples(id INT PRIMARY KEY,a INT,b TEXT); CREATE INDEX sample_parts ON index_samples(a DESC,lower(b))").unwrap();
    let rows=session.execute("SELECT pg_get_indexdef('sample_parts'::regclass,1,true),pg_get_indexdef('sample_parts'::regclass,2,true),pg_get_indexdef('sample_parts'::regclass,99,true),pg_get_indexdef('sample_parts'::regclass,-1,true),pg_get_indexdef('sample_parts'::regclass,1,NULL)").unwrap().rows;
    assert_eq!(
        rows,
        vec![vec![
            SqlValue::String("a".into()),
            SqlValue::String("lower(b)".into()),
            SqlValue::String("".into()),
            SqlValue::String("".into()),
            SqlValue::Null
        ]]
    );
}

#[test]
fn catalog_introspection_uses_all_predicates_and_preserves_alias_independence() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE catalog_samples(id BIGSERIAL PRIMARY KEY,role VARCHAR DEFAULT 'member',n INT DEFAULT 4)").unwrap();
    let prefix = "SELECT a.attname, format_type(a.atttypid,a.atttypmod),pg_get_expr(d.adbin,d.adrelid),a.attnotnull,a.atttypid,a.atttypmod,c.collname,col_description(a.attrelid,a.attnum) AS comment,attidentity AS identity,attgenerated AS attgenerated FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid=d.adrelid AND a.attnum=d.adnum LEFT JOIN pg_type t ON a.atttypid=t.oid LEFT JOIN pg_collation c ON a.attcollation=c.oid AND a.attcollation<>t.typcollation WHERE a.attrelid='catalog_samples'::regclass AND a.attnum>0 AND NOT a.attisdropped";
    let rows = session
        .execute(&format!("{prefix} ORDER BY a.attnum"))
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1][2], SqlValue::String("'member'::varchar".into()));
    assert_eq!(rows[2][2], SqlValue::String("4".into()));
    assert!(session
        .execute(&format!("{prefix} AND false ORDER BY a.attnum"))
        .unwrap()
        .rows
        .is_empty());
    let renamed = prefix
        .replace("a.", "attribute_row.")
        .replace("pg_attribute a ", "pg_attribute attribute_row ");
    assert_eq!(
        session
            .execute(&format!("{renamed} ORDER BY attribute_row.attnum"))
            .unwrap()
            .rows,
        rows
    );
    let prefix="SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = 'catalog_samples'::regclass AND i.indisprimary";
    assert_eq!(
        session
            .execute(&format!(
                "{prefix} ORDER BY array_position(i.indkey,a.attnum)"
            ))
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("id".into())]]
    );
    assert!(session
        .execute(&format!(
            "{prefix} AND false ORDER BY array_position(i.indkey,a.attnum)"
        ))
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn vector_quantifiers_use_resolved_types_and_do_not_accept_plain_text() {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE vector_samples(id INT PRIMARY KEY,v INT2VECTOR,o OIDVECTOR,txt TEXT); INSERT INTO vector_samples VALUES(1,'1 2','3 4','1 2')").unwrap();
    assert_eq!(
        session
            .execute("SELECT 2=ANY(v),3<ALL(o) FROM vector_samples")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(false)]]
    );
    assert_eq!(
        session
            .execute("SELECT 2=ANY('1 2'::int2vector)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    assert!(session
        .execute("SELECT 2=ANY(txt) FROM vector_samples")
        .is_err());
    assert!(session.execute("SELECT 2=ANY('1 2')").is_err());
}

fn catalog_session_db() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    (root, db)
}

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.to_string())
}

/// information_schema.columns reports the SQL-standard type names and the
/// size, precision and scale metadata PostgreSQL 16 reports for the same
/// declarations.
#[test]
fn information_schema_columns_reports_type_precision_metadata() {
    let (_root, mut db) = catalog_session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE DOMAIN amount AS NUMERIC(7,3)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE typed_columns (
                n10 NUMERIC(10), n102 NUMERIC(10,2), n NUMERIC, v60 VARCHAR(60),
                c1 CHAR(1), ts TIMESTAMP, tstz TIMESTAMP WITH TIME ZONE, b BYTEA,
                t TEXT, i2 SMALLINT, i4 INTEGER, i8 BIGINT, ts3 TIMESTAMP(3),
                d DATE, f8 DOUBLE PRECISION, f4 REAL, tm TIME, tmtz TIME WITH TIME ZONE,
                iv INTERVAL, ivds INTERVAL DAY TO SECOND(3), dom amount)",
        )
        .unwrap();
    let rows = session
        .execute(
            "SELECT column_name, data_type, character_maximum_length, character_octet_length,
                    numeric_precision, numeric_precision_radix, numeric_scale,
                    datetime_precision, interval_type, udt_name
             FROM information_schema.columns
             WHERE table_name = 'typed_columns' ORDER BY ordinal_position",
        )
        .unwrap()
        .rows;
    let null = SqlValue::Null;
    let int = SqlValue::Int;
    #[rustfmt::skip]
    let expected = vec![
        vec![text("n10"), text("numeric"), null.clone(), null.clone(), int(10), int(10), int(0), null.clone(), null.clone(), text("numeric")],
        vec![text("n102"), text("numeric"), null.clone(), null.clone(), int(10), int(10), int(2), null.clone(), null.clone(), text("numeric")],
        vec![text("n"), text("numeric"), null.clone(), null.clone(), null.clone(), int(10), null.clone(), null.clone(), null.clone(), text("numeric")],
        vec![text("v60"), text("character varying"), int(60), int(240), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), text("varchar")],
        vec![text("c1"), text("character"), int(1), int(4), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), text("bpchar")],
        vec![text("ts"), text("timestamp without time zone"), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), int(6), null.clone(), text("timestamp")],
        vec![text("tstz"), text("timestamp with time zone"), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), int(6), null.clone(), text("timestamptz")],
        vec![text("b"), text("bytea"), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), text("bytea")],
        vec![text("t"), text("text"), null.clone(), int(1_073_741_824), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), text("text")],
        vec![text("i2"), text("smallint"), null.clone(), null.clone(), int(16), int(2), int(0), null.clone(), null.clone(), text("int2")],
        vec![text("i4"), text("integer"), null.clone(), null.clone(), int(32), int(2), int(0), null.clone(), null.clone(), text("int4")],
        vec![text("i8"), text("bigint"), null.clone(), null.clone(), int(64), int(2), int(0), null.clone(), null.clone(), text("int8")],
        vec![text("ts3"), text("timestamp without time zone"), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), int(3), null.clone(), text("timestamp")],
        vec![text("d"), text("date"), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), int(0), null.clone(), text("date")],
        vec![text("f8"), text("double precision"), null.clone(), null.clone(), int(53), int(2), null.clone(), null.clone(), null.clone(), text("float8")],
        vec![text("f4"), text("real"), null.clone(), null.clone(), int(24), int(2), null.clone(), null.clone(), null.clone(), text("float4")],
        vec![text("tm"), text("time without time zone"), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), int(6), null.clone(), text("time")],
        vec![text("tmtz"), text("time with time zone"), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), int(6), null.clone(), text("timetz")],
        vec![text("iv"), text("interval"), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), int(6), null.clone(), text("interval")],
        vec![text("ivds"), text("interval"), null.clone(), null.clone(), null.clone(), null.clone(), null.clone(), int(3), text("DAY TO SECOND(3)"), text("interval")],
        vec![text("dom"), text("numeric"), null.clone(), null.clone(), int(7), int(10), int(3), null.clone(), null.clone(), text("numeric")],
    ];
    for (row, expected) in rows.iter().zip(&expected) {
        assert_eq!(row, expected);
    }
    assert_eq!(rows.len(), expected.len());
}
