use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn pg_dump_zero_range_function_sentinels_restore_as_none() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TYPE public.restored_range AS RANGE (
                subtype = integer,
                multirange_type_name = restored_multirange,
                canonical = 0,
                subtype_diff = 0
            )",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT r.rngcanonical, r.rngsubdiff
                 FROM pg_range r
                 JOIN pg_type t ON t.oid = r.rngtypid
                 WHERE t.typname = 'restored_range'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0), SqlValue::Int(0)]]
    );
}

#[test]
fn create_range_builds_paired_multirange_catalogs_and_values() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TYPE price_range AS RANGE (SUBTYPE = numeric)")
        .unwrap();

    let rows = session
        .execute(
            "SELECT typname, typtype, typcategory, typarray, typinput, typoutput
             FROM pg_type
             WHERE typname IN ('price_range', 'price_multirange')
             ORDER BY typname",
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], SqlValue::String("price_multirange".to_string()));
    assert_eq!(rows[0][1], SqlValue::String("m".to_string()));
    assert_eq!(rows[0][2], SqlValue::String("R".to_string()));
    assert_ne!(rows[0][3], SqlValue::Int(0));
    assert_eq!(rows[0][4], SqlValue::String("multirange_in".to_string()));
    assert_eq!(rows[1][0], SqlValue::String("price_range".to_string()));
    assert_eq!(rows[1][1], SqlValue::String("r".to_string()));
    assert_eq!(rows[1][4], SqlValue::String("range_in".to_string()));
    assert_eq!(
        session
            .execute("SELECT 'price_range'::regtype::text")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("price_range".to_string())]],
    );

    let range_catalog = session
        .execute(
            "SELECT r.rngtypid, r.rngsubtype, r.rngmultitypid, r.rngcollation,
                    r.rngsubopc, r.rngcanonical, r.rngsubdiff
             FROM pg_range r
             WHERE r.rngtypid = (
                 SELECT oid FROM pg_type WHERE typname = 'price_range'
             )",
        )
        .unwrap()
        .rows;
    assert_eq!(range_catalog.len(), 1);
    assert_eq!(range_catalog[0][1], SqlValue::Int(1700));
    assert_eq!(range_catalog[0][3], SqlValue::Int(0));
    assert_eq!(range_catalog[0][4], SqlValue::Int(3125));
    assert_eq!(range_catalog[0][5], SqlValue::Int(0));
    assert_eq!(range_catalog[0][6], SqlValue::Int(0));
    assert_eq!(
        session.execute("SELECT 0::regproc::text").unwrap().rows,
        vec![vec![SqlValue::String("-".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT proname, pronargs, prosrc
                 FROM pg_proc
                 WHERE proname IN ('price_range', 'price_multirange')
                 ORDER BY proname, pronargs, prosrc",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("price_multirange".to_string()),
                SqlValue::Int(0),
                SqlValue::String("multirange_constructor0".to_string()),
            ],
            vec![
                SqlValue::String("price_multirange".to_string()),
                SqlValue::Int(1),
                SqlValue::String("multirange_constructor1".to_string()),
            ],
            vec![
                SqlValue::String("price_multirange".to_string()),
                SqlValue::Int(1),
                SqlValue::String("multirange_constructor2".to_string()),
            ],
            vec![
                SqlValue::String("price_range".to_string()),
                SqlValue::Int(2),
                SqlValue::String("range_constructor2".to_string()),
            ],
            vec![
                SqlValue::String("price_range".to_string()),
                SqlValue::Int(3),
                SqlValue::String("range_constructor3".to_string()),
            ],
        ],
    );

    session
        .execute(
            "CREATE TABLE price_windows (
                id text PRIMARY KEY,
                window price_range,
                windows price_multirange,
                history price_range[]
             )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO price_windows VALUES
             ('one', '[1.20,3.40)', '{[1.20,2.00),[2.00,3.40)}',
              '{\"[1.20,3.40)\",empty}')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT window, windows FROM price_windows")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("[1.20,3.40)".to_string()),
            SqlValue::String("{[1.20,3.40)}".to_string()),
        ]],
    );
    session
        .execute("CREATE INDEX price_windows_window_idx ON price_windows (window)")
        .unwrap();
    session
        .execute(
            "INSERT INTO price_windows VALUES
             ('earlier', '[0,1)', '{}', '{}'),
             ('later', '[10,20)', '{}', '{}')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM price_windows ORDER BY window")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("earlier".to_string())],
            vec![SqlValue::String("one".to_string())],
            vec![SqlValue::String("later".to_string())],
        ],
    );
    assert_eq!(
        session
            .execute("SELECT history FROM price_windows WHERE id = 'one'")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "[\"[1.20,3.40)\",\"empty\"]",
    );
}

#[test]
fn custom_discrete_range_without_canonical_function_preserves_bounds() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TYPE exact_int_span AS RANGE (SUBTYPE = int4)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT '[1,3]'::exact_int_span")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("[1,3]".to_string())]],
    );
    assert_eq!(
        session.execute("SELECT '[1,3]'::int4range").unwrap().rows,
        vec![vec![SqlValue::String("[1,4)".to_string())]],
    );
    assert_eq!(
        session
            .execute("SELECT exact_int_span(1, 3, '[]')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("[1,3]".to_string())]],
    );
    assert_eq!(
        session
            .execute("SELECT exact_int_span_multirange()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("{}".to_string())]],
    );
    assert_eq!(
        session
            .execute(
                "SELECT exact_int_span_multirange(
                    exact_int_span(1, 3, '[]')
                 )",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("{[1,3]}".to_string())]],
    );
    assert_eq!(
        session
            .execute(
                "SELECT lower('[1,3]'::exact_int_span),
                        upper('[1,3]'::exact_int_span),
                        lower_inc('[1,3]'::exact_int_span),
                        upper_inc('[1,3]'::exact_int_span),
                        isempty('empty'::exact_int_span),
                        range_merge('{[1,2],[5,8]}'::exact_int_span_multirange)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(1),
            SqlValue::Int(3),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::String("[1,8]".to_string()),
        ]],
    );
    assert_eq!(
        session
            .execute(
                "SELECT '[1,4]'::exact_int_span && '[3,6]'::exact_int_span,
                        '[1,4]'::exact_int_span @> 3,
                        3 <@ '[1,4]'::exact_int_span,
                        '[1,2]'::exact_int_span -|- '(2,3]'::exact_int_span,
                        '[1,3]'::exact_int_span + '[3,5]'::exact_int_span,
                        '[1,3]'::exact_int_span * '[3,5]'::exact_int_span,
                        '[1,5]'::exact_int_span - '[1,3)'::exact_int_span,
                        '{[1,2]}'::exact_int_span_multirange
                            + '{[5,6]}'::exact_int_span_multirange",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::String("[1,5]".to_string()),
            SqlValue::String("[3,3]".to_string()),
            SqlValue::String("[3,5]".to_string()),
            SqlValue::String("{[1,2],[5,6]}".to_string()),
        ]],
    );
}

#[test]
fn range_options_namespaces_and_collisions_fail_explicitly() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let duplicate = session
        .execute("CREATE TYPE duplicate_span AS RANGE (SUBTYPE = int4, SUBTYPE = int8)")
        .unwrap_err();
    assert_eq!(duplicate.sqlstate(), "42601");

    session
        .execute("CREATE TYPE reserved_set AS ENUM ('one')")
        .unwrap();
    let collision = session
        .execute(
            "CREATE TYPE collided_span AS RANGE (
                SUBTYPE = int4,
                MULTIRANGE_TYPE_NAME = reserved_set
             )",
        )
        .unwrap_err();
    assert_eq!(collision.sqlstate(), "42710");
    assert!(session
        .execute("SELECT oid FROM pg_type WHERE typname = 'collided_span'")
        .unwrap()
        .rows
        .is_empty());

    let missing_schema = session
        .execute(
            "CREATE TYPE missing_namespace_span AS RANGE (
                SUBTYPE = date,
                MULTIRANGE_TYPE_NAME = absent_schema.missing_namespace_set
             )",
        )
        .unwrap_err();
    assert_eq!(missing_schema.sqlstate(), "3F000");

    let unsupported_collation = session
        .execute(
            "CREATE TYPE collated_span AS RANGE (
                SUBTYPE = int4,
                COLLATION = default
             )",
        )
        .unwrap_err();
    assert_eq!(unsupported_collation.sqlstate(), "42804");
}

#[test]
fn registered_discrete_canonical_hook_applies_and_is_cataloged() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session.execute("CREATE TYPE canonical_span").unwrap();
    session
        .execute(
            "CREATE FUNCTION canonical_span_canonical(canonical_span)
             RETURNS canonical_span
             AS 'int4range_canonical' LANGUAGE internal IMMUTABLE STRICT",
        )
        .unwrap();
    session
        .execute(
            "CREATE TYPE canonical_span AS RANGE (
                SUBTYPE = int4,
                CANONICAL = canonical_span_canonical
             )",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT '[1,3]'::canonical_span")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("[1,4)".to_string())]],
    );
    let canonical_oid = session
        .execute(
            "SELECT rngcanonical FROM pg_range
             WHERE rngtypid = (
                 SELECT oid FROM pg_type WHERE typname = 'canonical_span'
             )",
        )
        .unwrap()
        .rows[0][0]
        .clone();
    assert_ne!(canonical_oid, SqlValue::Int(0));
}

#[test]
fn range_shell_finalization_and_transaction_rollback_preserve_catalog_integrity() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session.execute("CREATE TYPE measured_span").unwrap();
    let shell_oid = session
        .execute("SELECT oid FROM pg_type WHERE typname = 'measured_span'")
        .unwrap()
        .rows[0][0]
        .clone();
    session
        .execute("CREATE TYPE measured_span AS RANGE (SUBTYPE = numeric)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT oid FROM pg_type WHERE typname = 'measured_span'")
            .unwrap()
            .rows[0][0],
        shell_oid,
    );

    session.execute("BEGIN").unwrap();
    session
        .execute(
            "CREATE TYPE transient_range AS RANGE (
                SUBTYPE = date,
                MULTIRANGE_TYPE_NAME = transient_set
             )",
        )
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert!(session
        .execute(
            "SELECT typname FROM pg_type
             WHERE typname IN ('transient_range', 'transient_set')",
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn dropping_range_removes_internal_multirange_but_multirange_is_not_independent() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TYPE owned_span AS RANGE (SUBTYPE = int8)")
        .unwrap();
    let error = session
        .execute("DROP TYPE owned_span_multirange")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "2BP01");
    assert_eq!(
        session
            .execute(
                "SELECT typname FROM pg_type
                 WHERE typname IN ('owned_span', 'owned_span_multirange')
                 ORDER BY typname",
            )
            .unwrap()
            .rows
            .len(),
        2,
    );

    session.execute("DROP TYPE owned_span").unwrap();
    assert!(session
        .execute(
            "SELECT typname FROM pg_type
             WHERE typname IN ('owned_span', 'owned_span_multirange')",
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn range_oids_catalogs_and_values_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let (range_oid, multirange_oid) = {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TYPE durable_span AS RANGE (SUBTYPE = date)")
            .unwrap();
        session
            .execute("CREATE TABLE durable_ranges (id text PRIMARY KEY, span durable_span)")
            .unwrap();
        session
            .execute("INSERT INTO durable_ranges VALUES ('one', '[2026-01-01,2026-02-01]')")
            .unwrap();
        let rows = session
            .execute(
                "SELECT typname, oid FROM pg_type
                 WHERE typname IN ('durable_span', 'durable_span_multirange')
                 ORDER BY typname",
            )
            .unwrap()
            .rows;
        let oid = |name: &str| {
            rows.iter()
                .find(|row| row[0] == SqlValue::String(name.to_string()))
                .and_then(|row| match row[1] {
                    SqlValue::Int(oid) => Some(oid),
                    _ => None,
                })
                .unwrap()
        };
        (oid("durable_span"), oid("durable_span_multirange"))
    };

    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute(
                "SELECT typname, oid FROM pg_type
                 WHERE typname IN ('durable_span', 'durable_span_multirange')
                 ORDER BY typname",
            )
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row[1].clone())
            .collect::<Vec<_>>(),
        vec![SqlValue::Int(range_oid), SqlValue::Int(multirange_oid)],
    );
    assert_eq!(
        session
            .execute("SELECT span FROM durable_ranges")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "[2026-01-01,2026-02-01]".to_string(),
        )]],
    );
}
