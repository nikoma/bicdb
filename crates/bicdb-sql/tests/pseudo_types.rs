use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn postgres_pseudo_types_are_cataloged_with_stable_oids() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let rows = session
        .execute(
            "SELECT oid, typname
             FROM pg_type
             WHERE typtype = 'p'
             ORDER BY oid",
        )
        .unwrap()
        .rows;
    let expected = [
        (32, "pg_ddl_command"),
        (269, "table_am_handler"),
        (325, "index_am_handler"),
        (705, "unknown"),
        (2249, "record"),
        (2275, "cstring"),
        (2276, "any"),
        (2277, "anyarray"),
        (2278, "void"),
        (2279, "trigger"),
        (2280, "language_handler"),
        (2281, "internal"),
        (2283, "anyelement"),
        (2287, "_record"),
        (2776, "anynonarray"),
        (3115, "fdw_handler"),
        (3310, "tsm_handler"),
        (3500, "anyenum"),
        (3831, "anyrange"),
        (3838, "event_trigger"),
        (4537, "anymultirange"),
        (4538, "anycompatiblemultirange"),
        (5077, "anycompatible"),
        (5078, "anycompatiblearray"),
        (5079, "anycompatiblenonarray"),
        (5080, "anycompatiblerange"),
    ]
    .into_iter()
    .map(|(oid, name)| vec![SqlValue::Int(oid), SqlValue::String(name.into())])
    .collect::<Vec<_>>();
    assert_eq!(rows, expected);

    assert_eq!(
        session
            .execute(
                "SELECT 'record'::regtype::oid,
                        'event_trigger'::regtype::oid,
                        'anycompatiblemultirange'::regtype::oid,
                        'record[]'::regtype::oid",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(2249),
            SqlValue::Int(3838),
            SqlValue::Int(4538),
            SqlValue::Int(2287),
        ]]
    );
}

#[test]
fn supported_pseudo_types_are_preserved_in_routine_signatures() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            r#"CREATE FUNCTION pseudo_identity(value anyelement)
                 RETURNS anyelement
                 LANGUAGE plpgsql
                 AS 'BEGIN RETURN value; END';
               CREATE FUNCTION pseudo_compatible(value anycompatiblearray)
                 RETURNS anycompatiblearray
                 LANGUAGE plpgsql
                 AS 'BEGIN RETURN value; END';
               CREATE FUNCTION pseudo_record()
                 RETURNS record
                 LANGUAGE plpgsql
                 AS 'BEGIN RETURN NULL; END';
               CREATE FUNCTION pseudo_event()
                 RETURNS event_trigger
                 LANGUAGE plpgsql
                 AS 'BEGIN RETURN; END'"#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT proname,
                        pg_get_function_arguments(oid),
                        pg_get_function_result(oid)
                 FROM pg_proc
                 WHERE proname LIKE 'pseudo_%'
                 ORDER BY proname",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("pseudo_compatible".into()),
                SqlValue::String("value anycompatiblearray".into()),
                SqlValue::String("anycompatiblearray".into()),
            ],
            vec![
                SqlValue::String("pseudo_event".into()),
                SqlValue::String(String::new()),
                SqlValue::String("event_trigger".into()),
            ],
            vec![
                SqlValue::String("pseudo_identity".into()),
                SqlValue::String("value anyelement".into()),
                SqlValue::String("anyelement".into()),
            ],
            vec![
                SqlValue::String("pseudo_record".into()),
                SqlValue::String(String::new()),
                SqlValue::String("record".into()),
            ],
        ]
    );
}

#[test]
fn pseudo_types_are_rejected_as_columns_with_invalid_table_definition() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    for (index, pg_type) in [
        "pg_ddl_command",
        "table_am_handler",
        "index_am_handler",
        "unknown",
        "record",
        "cstring",
        "\"any\"",
        "anyarray",
        "void",
        "trigger",
        "language_handler",
        "internal",
        "anyelement",
        "anynonarray",
        "fdw_handler",
        "tsm_handler",
        "anyenum",
        "anyrange",
        "event_trigger",
        "anymultirange",
        "anycompatiblemultirange",
        "anycompatible",
        "anycompatiblearray",
        "anycompatiblenonarray",
        "anycompatiblerange",
        "record[]",
        "cstring[]",
    ]
    .into_iter()
    .enumerate()
    {
        let sql = format!("CREATE TABLE invalid_pseudo_{index} (value {pg_type})");
        let error = session.execute(&sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42P16", "{sql}: {error}");
    }
}

#[test]
fn invalid_pseudo_type_routine_signatures_match_postgres_sqlstates() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    for (sql, sqlstate) in [
        (
            "CREATE FUNCTION bad_poly() RETURNS anyelement LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'",
            "42P13",
        ),
        (
            "CREATE FUNCTION bad_internal() RETURNS internal LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'",
            "42P13",
        ),
        (
            "CREATE FUNCTION bad_void_arg(value void) RETURNS int4 LANGUAGE sql AS 'SELECT 1'",
            "42P13",
        ),
        (
            "CREATE FUNCTION bad_cstring_arg(value cstring) RETURNS int4 LANGUAGE plpgsql AS 'BEGIN RETURN 1; END'",
            "0A000",
        ),
        (
            "CREATE FUNCTION bad_handler() RETURNS fdw_handler LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'",
            "0A000",
        ),
        (
            "CREATE FUNCTION bad_sql_trigger() RETURNS trigger LANGUAGE sql AS 'SELECT NULL'",
            "42P13",
        ),
        (
            "CREATE FUNCTION bad_trigger_arg(value int4) RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'",
            "42P13",
        ),
        (
            "CREATE FUNCTION bad_event_arg(value int4) RETURNS event_trigger LANGUAGE plpgsql AS 'BEGIN RETURN; END'",
            "42P13",
        ),
        (
            "CREATE PROCEDURE bad_proc(value cstring) LANGUAGE plpgsql AS 'BEGIN NULL; END'",
            "0A000",
        ),
        (
            "CREATE FUNCTION bad_table_trigger() RETURNS TABLE(value trigger) LANGUAGE sql AS 'SELECT NULL'",
            "42P13",
        ),
        (
            "CREATE FUNCTION bad_table_cstring() RETURNS TABLE(value cstring) LANGUAGE plpgsql AS 'BEGIN RETURN QUERY SELECT NULL; END'",
            "0A000",
        ),
        (
            "CREATE FUNCTION bad_out_trigger(OUT value trigger) LANGUAGE sql AS 'SELECT NULL'",
            "42P13",
        ),
        (
            "CREATE FUNCTION bad_out_cstring(OUT value cstring) LANGUAGE plpgsql AS 'BEGIN value := NULL; END'",
            "0A000",
        ),
        (
            "CREATE FUNCTION bad_inout_trigger(INOUT value trigger) LANGUAGE plpgsql AS 'BEGIN RETURN; END'",
            "0A000",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
    }
}
