use bicdb_core::BicDb;
use bicdb_sql::{render_oid_alias_array_value, render_oid_alias_value, SqlSession, SqlValue};

#[test]
fn catalog_reference_aliases_resolve_store_and_render_as_oids() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE oid_alias_values (
                id text PRIMARY KEY,
                procedure regproc,
                procedure_signature regprocedure,
                relation regclass,
                type_name regtype,
                namespace regnamespace,
                collation_name regcollation,
                role_name regrole,
                configuration regconfig,
                dictionary regdictionary,
                operator_signature regoperator
            );
            INSERT INTO oid_alias_values VALUES (
                'one',
                'now'::regproc,
                'now()'::regprocedure,
                'pg_catalog.pg_type'::regclass,
                'integer'::regtype,
                'pg_catalog'::regnamespace,
                '\"C\"'::regcollation,
                'bicdb'::regrole,
                'english'::regconfig,
                'english_stem'::regdictionary,
                '+(integer,integer)'::regoperator
            )",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT procedure, procedure_signature, relation, type_name, namespace,
                    collation_name, role_name, configuration, dictionary,
                    operator_signature
             FROM oid_alias_values WHERE id = 'one'",
        )
        .unwrap();
    assert!(result.rows[0]
        .iter()
        .all(|value| matches!(value, SqlValue::Int(_))));

    let values = result.rows[0].clone();
    drop(session);
    for (index, (pg_type, expected)) in [
        ("regproc", "now"),
        ("regprocedure", "now()"),
        ("regclass", "pg_type"),
        ("regtype", "integer"),
        ("regnamespace", "pg_catalog"),
        ("regcollation", "\"C\""),
        ("regrole", "bicdb"),
        ("regconfig", "english"),
        ("regdictionary", "english_stem"),
        ("regoperator", "+(int4,int4)"),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            render_oid_alias_value(&db, pg_type, &values[index]).unwrap(),
            expected
        );
    }
}

#[test]
fn catalog_reference_aliases_fail_closed_for_missing_objects() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    for (sql, state) in [
        ("SELECT 'missing_relation'::regclass", "42P01"),
        ("SELECT 'missing_type'::regtype", "42704"),
        ("SELECT 'missing_schema'::regnamespace", "3F000"),
        ("SELECT 'missing_role'::regrole", "42704"),
        ("SELECT 'missing_function'::regproc", "42883"),
        ("SELECT 'missing(integer)'::regprocedure", "42883"),
        ("SELECT 'missing(integer,integer)'::regoperator", "42883"),
        ("SELECT 'missing'::regcollation", "42704"),
        ("SELECT 'missing'::regconfig", "42704"),
        ("SELECT 'missing'::regdictionary", "42704"),
        ("SELECT 'C'::regcollation", "42704"),
        ("SELECT 'uuidv7'::regproc", "42883"),
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), state, "{sql}");
    }
}

#[test]
fn signature_aliases_disambiguate_supported_overloads() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let rows = session
        .execute(
            "SELECT 'uuidv7()'::regprocedure,
                    'uuidv7(interval)'::regprocedure,
                    '+(integer,integer)'::regoperator",
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    assert!(rows[0]
        .iter()
        .all(|value| matches!(value, SqlValue::Int(_))));
    drop(session);
    assert_eq!(
        render_oid_alias_value(&db, "regprocedure", &rows[0][0]).unwrap(),
        "uuidv7()"
    );
    assert_eq!(
        render_oid_alias_value(&db, "regprocedure", &rows[0][1]).unwrap(),
        "uuidv7(interval)"
    );
    assert_eq!(
        render_oid_alias_value(&db, "regoperator", &rows[0][2]).unwrap(),
        "+(int4,int4)"
    );
}

#[test]
fn catalog_reference_aliases_honor_qualification() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE SCHEMA alias_scope;
             CREATE TABLE alias_scope.qualified_target (id int4 PRIMARY KEY)",
        )
        .unwrap();
    let values = session
        .execute(
            "SELECT 'alias_scope.qualified_target'::regclass,
                    'alias_scope.qualified_target'::regtype,
                    'pg_catalog.now()'::regprocedure,
                    'pg_catalog.?(jsonb,text)'::regoperator",
        )
        .unwrap()
        .rows[0]
        .clone();
    drop(session);
    for (index, (pg_type, expected)) in [
        ("regclass", "alias_scope.qualified_target"),
        ("regtype", "alias_scope.qualified_target"),
        ("regprocedure", "now()"),
        ("regoperator", "?(jsonb,text)"),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            render_oid_alias_value(&db, pg_type, &values[index]).unwrap(),
            expected
        );
    }

    let mut session = SqlSession::new(&mut db);
    for sql in [
        "SELECT 'public.qualified_target'::regclass",
        "SELECT 'missing.qualified_target'::regclass",
        "SELECT 'missing.now()'::regprocedure",
        "SELECT 'public.?(jsonb,text)'::regoperator",
    ] {
        assert!(session.execute(sql).is_err(), "{sql}");
    }
}

#[test]
fn catalog_reference_arrays_store_oids_and_render_names() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE alias_array_target (id int4 PRIMARY KEY);
             CREATE TABLE alias_array_values (id int4 PRIMARY KEY, targets regclass[]);
             INSERT INTO alias_array_values VALUES (
                 1,
                 '{alias_array_target,pg_catalog.pg_type,NULL}'::regclass[]
             )",
        )
        .unwrap();
    let value = session
        .execute("SELECT targets FROM alias_array_values WHERE id = 1")
        .unwrap()
        .rows[0][0]
        .clone();
    let SqlValue::Json(values) = &value else {
        panic!("regclass[] should use array storage");
    };
    let values = values.as_array().unwrap();
    assert!(values[0].is_number());
    assert!(values[1].is_number());
    assert!(values[2].is_null());
    drop(session);
    assert_eq!(
        render_oid_alias_array_value(&db, "regclass", &value).unwrap(),
        SqlValue::Json(serde_json::json!(["alias_array_target", "pg_type", null]))
    );
}

#[test]
fn regclass_identity_survives_catalog_growth_rename_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let original_oid;
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE stable_target (id int4 PRIMARY KEY);
                 CREATE TABLE stable_reference (id int4 PRIMARY KEY, target regclass NOT NULL);
                 INSERT INTO stable_reference VALUES (1, 'stable_target');",
            )
            .unwrap();
        original_oid = match session
            .execute("SELECT target FROM stable_reference WHERE id = 1")
            .unwrap()
            .rows[0][0]
        {
            SqlValue::Int(oid) => oid,
            ref value => panic!("expected numeric regclass storage, got {value:?}"),
        };
        session
            .execute(
                "CREATE TABLE a_table_that_sorts_first (id int4 PRIMARY KEY);
                 ALTER TABLE stable_target RENAME TO renamed_target",
            )
            .unwrap();
        assert_eq!(
            session
                .execute("SELECT target FROM stable_reference WHERE id = 1")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(original_oid)]]
        );
        drop(session);
        assert_eq!(
            render_oid_alias_value(&db, "regclass", &SqlValue::Int(original_oid)).unwrap(),
            "renamed_target"
        );
    }

    let db = BicDb::open(root.path()).unwrap();
    assert_eq!(
        render_oid_alias_value(&db, "regclass", &SqlValue::Int(original_oid)).unwrap(),
        "renamed_target"
    );
}

#[test]
fn regclass_view_identity_survives_catalog_growth_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let view_oid;
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE view_source (id int4 PRIMARY KEY);
                 CREATE VIEW stable_alias_view AS SELECT id FROM view_source;
                 CREATE TABLE view_alias_reference (
                     id int4 PRIMARY KEY,
                     target regclass NOT NULL
                 );
                 INSERT INTO view_alias_reference VALUES (1, 'stable_alias_view')",
            )
            .unwrap();
        view_oid = match session
            .execute("SELECT target FROM view_alias_reference WHERE id = 1")
            .unwrap()
            .rows[0][0]
        {
            SqlValue::Int(oid) => oid,
            ref value => panic!("expected numeric regclass storage, got {value:?}"),
        };
        session
            .execute("CREATE TABLE view_catalog_growth (id int4 PRIMARY KEY)")
            .unwrap();
        drop(session);
        assert_eq!(
            render_oid_alias_value(&db, "regclass", &SqlValue::Int(view_oid)).unwrap(),
            "stable_alias_view"
        );
    }

    let db = BicDb::open(root.path()).unwrap();
    assert_eq!(
        render_oid_alias_value(&db, "regclass", &SqlValue::Int(view_oid)).unwrap(),
        "stable_alias_view"
    );
}

#[test]
fn regclass_defaults_track_renames_and_enforce_drop_dependencies() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE default_target (id int4 PRIMARY KEY)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE default_reference (
                 id int4 PRIMARY KEY,
                 target regclass DEFAULT 'default_target'::regclass,
                 targets regclass[] DEFAULT '{default_target}'::regclass[]
             )",
        )
        .unwrap();
    session
        .execute("INSERT INTO default_reference (id) VALUES (1)")
        .unwrap();
    session
        .execute("ALTER TABLE default_target RENAME TO renamed_default_target")
        .unwrap();
    session
        .execute("INSERT INTO default_reference (id) VALUES (2)")
        .unwrap();

    let rows = session
        .execute("SELECT target FROM default_reference ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], rows[1]);
    assert!(matches!(rows[0][0], SqlValue::Int(_)));
    let arrays = session
        .execute("SELECT targets FROM default_reference ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(arrays[0], arrays[1]);
    assert_eq!(
        session
            .execute("DROP TABLE renamed_default_target")
            .unwrap_err()
            .sqlstate(),
        "2BP01"
    );

    session
        .execute("DROP TABLE renamed_default_target CASCADE")
        .unwrap();
    session
        .execute("INSERT INTO default_reference (id) VALUES (3)")
        .unwrap();
    let after_drop = session
        .execute("SELECT target, targets FROM default_reference WHERE id = 3")
        .unwrap()
        .rows;
    assert_eq!(after_drop[0][0], SqlValue::Null);
    assert_eq!(after_drop[0][1], arrays[0][0]);
}
