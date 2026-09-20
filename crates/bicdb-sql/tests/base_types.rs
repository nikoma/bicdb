use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn create_text_base_type(session: &mut SqlSession<'_>, name: &str) {
    session.execute(&format!("CREATE TYPE {name}")).unwrap();
    session
        .execute(&format!(
            "CREATE FUNCTION {name}_in(cstring) RETURNS {name}
             AS 'textin' LANGUAGE internal IMMUTABLE STRICT"
        ))
        .unwrap();
    session
        .execute(&format!(
            "CREATE FUNCTION {name}_out({name}) RETURNS cstring
             AS 'textout' LANGUAGE internal IMMUTABLE STRICT"
        ))
        .unwrap();
    session
        .execute(&format!(
            "CREATE FUNCTION {name}_recv(internal) RETURNS {name}
             AS 'textrecv' LANGUAGE internal IMMUTABLE STRICT"
        ))
        .unwrap();
    session
        .execute(&format!(
            "CREATE FUNCTION {name}_send({name}) RETURNS bytea
             AS 'textsend' LANGUAGE internal IMMUTABLE STRICT"
        ))
        .unwrap();
    session
        .execute(&format!(
            "CREATE TYPE {name} (
                INPUT = {name}_in,
                OUTPUT = {name}_out,
                RECEIVE = {name}_recv,
                SEND = {name}_send,
                INTERNALLENGTH = VARIABLE,
                ALIGNMENT = int4,
                STORAGE = extended,
                CATEGORY = 'S',
                DEFAULT = 'seed',
                DELIMITER = ';',
                COLLATABLE = true
            )"
        ))
        .unwrap();
}

#[test]
fn shell_finalizes_to_durable_registered_base_type() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let (type_oid, array_oid);
    {
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TYPE exact_text").unwrap();
        let shell = session
            .execute(
                "SELECT oid, typarray, typtype, typisdefined, typinput, typoutput
                 FROM pg_type WHERE typname = 'exact_text'",
            )
            .unwrap()
            .rows;
        assert_eq!(shell.len(), 1);
        type_oid = shell[0][0].clone();
        assert_eq!(shell[0][1], SqlValue::Int(0));
        assert_eq!(shell[0][2], SqlValue::String("p".to_string()));
        assert_eq!(shell[0][3], SqlValue::Bool(false));
        assert_eq!(shell[0][4], SqlValue::String("shell_in".to_string()));
        assert_eq!(shell[0][5], SqlValue::String("shell_out".to_string()));
        assert_eq!(
            session
                .execute("CREATE TABLE shell_is_not_storage (value exact_text)")
                .unwrap_err()
                .sqlstate(),
            "42704"
        );

        for sql in [
            "CREATE FUNCTION exact_text_in(cstring) RETURNS exact_text AS $$textin$$ LANGUAGE internal IMMUTABLE STRICT",
            "CREATE FUNCTION exact_text_out(exact_text) RETURNS cstring AS $$textout$$ LANGUAGE internal IMMUTABLE STRICT",
            "CREATE FUNCTION exact_text_recv(internal) RETURNS exact_text AS $$textrecv$$ LANGUAGE internal IMMUTABLE STRICT",
            "CREATE FUNCTION exact_text_send(exact_text) RETURNS bytea AS $$textsend$$ LANGUAGE internal IMMUTABLE STRICT",
        ] {
            session.execute(sql).unwrap();
        }
        session
            .execute(
                "CREATE TYPE exact_text (
                    INPUT=exact_text_in, OUTPUT=exact_text_out,
                    RECEIVE=exact_text_recv, SEND=exact_text_send,
                    INTERNALLENGTH=variable, ALIGNMENT=int4, STORAGE=extended,
                    CATEGORY='S', DEFAULT='seed', DELIMITER=';', COLLATABLE=true
                 )",
            )
            .unwrap();
        let defined = session
            .execute(
                "SELECT oid, typarray, typtype, typisdefined, typcategory,
                        typinput, typoutput, typreceive, typsend, typdefault,
                        typdelim, typcollation
                 FROM pg_type WHERE typname = 'exact_text'",
            )
            .unwrap()
            .rows;
        assert_eq!(defined[0][0], type_oid);
        array_oid = defined[0][1].clone();
        assert_ne!(array_oid, SqlValue::Int(0));
        assert_eq!(defined[0][2], SqlValue::String("b".to_string()));
        assert_eq!(defined[0][3], SqlValue::Bool(true));
        assert_eq!(defined[0][4], SqlValue::String("S".to_string()));
        for value in &defined[0][5..=8] {
            assert!(matches!(value, SqlValue::Int(oid) if *oid > 0));
        }
        assert_eq!(defined[0][9], SqlValue::String("seed".to_string()));
        assert_eq!(defined[0][10], SqlValue::String(";".to_string()));
        assert_eq!(defined[0][11], SqlValue::Int(100));
        assert_eq!(
            session
                .execute("ALTER FUNCTION exact_text_in(cstring) OWNER TO bicdb")
                .unwrap()
                .command_tag,
            Some("ALTER FUNCTION".to_string())
        );
        assert_eq!(
            session
                .execute(
                    "SELECT typinput::oid = (SELECT oid FROM pg_proc WHERE proname = 'exact_text_in'),
                            typoutput::oid = (SELECT oid FROM pg_proc WHERE proname = 'exact_text_out'),
                            typreceive::oid = (SELECT oid FROM pg_proc WHERE proname = 'exact_text_recv'),
                            typsend::oid = (SELECT oid FROM pg_proc WHERE proname = 'exact_text_send')
                     FROM pg_type WHERE typname = 'exact_text'",
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Bool(true); 4]]
        );
        assert_eq!(
            session
                .execute(
                    "SELECT typinput::text, typoutput::text, typreceive::text, typsend::text
                     FROM pg_type WHERE typname = 'exact_text'",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("exact_text_in".to_string()),
                SqlValue::String("exact_text_out".to_string()),
                SqlValue::String("exact_text_recv".to_string()),
                SqlValue::String("exact_text_send".to_string()),
            ]]
        );
        assert_eq!(
            session
                .execute(
                    "SELECT proname, prorettype, proargtypes, prosrc, prolang
                     FROM pg_proc
                     WHERE proname IN ('exact_text_in', 'exact_text_out')
                     ORDER BY proname",
                )
                .unwrap()
                .rows,
            vec![
                vec![
                    SqlValue::String("exact_text_in".to_string()),
                    type_oid.clone(),
                    SqlValue::String("2275".to_string()),
                    SqlValue::String("textin".to_string()),
                    SqlValue::Int(12),
                ],
                vec![
                    SqlValue::String("exact_text_out".to_string()),
                    SqlValue::Int(2275),
                    SqlValue::String(match &type_oid {
                        SqlValue::Int(oid) => oid.to_string(),
                        _ => unreachable!(),
                    }),
                    SqlValue::String("textout".to_string()),
                    SqlValue::Int(12),
                ],
            ]
        );

        session
            .execute(
                "CREATE TABLE base_rows (
                    id int PRIMARY KEY,
                    value exact_text,
                    values exact_text[]
                 )",
            )
            .unwrap();
        session
            .execute("INSERT INTO base_rows (id, values) VALUES (1, '{alpha;beta}')")
            .unwrap();
        session
            .execute("INSERT INTO base_rows VALUES (2, 'zeta', '{gamma;delta}')")
            .unwrap();
        assert_eq!(
            session
                .execute("CREATE INDEX base_rows_value_idx ON base_rows (value)")
                .unwrap_err()
                .sqlstate(),
            "42704"
        );
        assert_eq!(
            session
                .execute("SELECT value, array_length(values, 1) FROM base_rows ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![SqlValue::String("seed".to_string()), SqlValue::Int(2),],
                vec![SqlValue::String("zeta".to_string()), SqlValue::Int(2),],
            ]
        );
        assert_eq!(
            session
                .execute("SELECT values::text FROM base_rows ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![SqlValue::String("{alpha;beta}".to_string())],
                vec![SqlValue::String("{gamma;delta}".to_string())],
            ]
        );
    }
    db.close().unwrap();

    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute("SELECT oid, typarray FROM pg_type WHERE typname = 'exact_text'",)
            .unwrap()
            .rows,
        vec![vec![type_oid, array_oid]]
    );
    assert_eq!(
        session
            .execute("SELECT value, values::text FROM base_rows WHERE id = 2")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("zeta".to_string()),
            SqlValue::String("{gamma;delta}".to_string()),
        ]]
    );
}

#[test]
fn base_types_reject_unsafe_or_mismatched_codecs() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session.execute("CREATE TYPE rejected_base").unwrap();
    for sql in [
        "CREATE FUNCTION arbitrary_in(cstring) RETURNS rejected_base AS 'arbitrary_native_symbol' LANGUAGE internal",
        "CREATE FUNCTION module_in(cstring) RETURNS rejected_base AS '/tmp/evil.so', 'entry' LANGUAGE C",
    ] {
        assert!(session.execute(sql).is_err(), "{sql}");
    }
    session
        .execute("CREATE FUNCTION rejected_in(cstring) RETURNS rejected_base AS 'textin' LANGUAGE internal")
        .unwrap();
    session
        .execute("CREATE FUNCTION rejected_out(rejected_base) RETURNS cstring AS 'uuid_out' LANGUAGE internal")
        .unwrap();
    assert!(session
        .execute("CREATE TYPE rejected_base (INPUT=rejected_in, OUTPUT=rejected_out, INTERNALLENGTH=variable, ALIGNMENT=int4, STORAGE=extended)")
        .is_err());

    create_text_base_type(&mut session, "safe_base");
    assert_eq!(
        session
            .execute(
                "CREATE OR REPLACE FUNCTION safe_base_in(cstring) RETURNS safe_base
                 AS 'uuid_in' LANGUAGE internal",
            )
            .unwrap_err()
            .sqlstate(),
        "2BP01"
    );
    assert_eq!(
        session
            .execute("DROP FUNCTION safe_base_in(cstring)")
            .unwrap_err()
            .sqlstate(),
        "2BP01"
    );
    session
        .execute("CREATE TABLE safe_base_rows (id int PRIMARY KEY, value safe_base)")
        .unwrap();
    session
        .execute("INSERT INTO safe_base_rows VALUES (1, 'stored')")
        .unwrap();
    assert_eq!(
        session
            .execute("CREATE TABLE invalid_base_unique (value safe_base UNIQUE)")
            .unwrap_err()
            .sqlstate(),
        "42704"
    );
    assert_eq!(
        session
            .execute(
                "SELECT value FROM safe_base_rows
                 WHERE value = 'stored'::safe_base",
            )
            .unwrap_err()
            .sqlstate(),
        "42883"
    );
    assert_eq!(
        session
            .execute("SELECT value FROM safe_base_rows ORDER BY value")
            .unwrap_err()
            .sqlstate(),
        "42883"
    );
    assert_eq!(
        session
            .execute("CREATE INDEX safe_base_value_idx ON safe_base_rows (value)")
            .unwrap_err()
            .sqlstate(),
        "42704"
    );
    assert!(session
        .execute("CREATE TYPE safe_base")
        .unwrap_err()
        .to_string()
        .contains("already exists"));

    assert_eq!(
        session
            .execute("DROP TYPE safe_base")
            .unwrap_err()
            .sqlstate(),
        "2BP01"
    );
    session.execute("DROP TYPE safe_base CASCADE").unwrap();
    assert!(session
        .execute("SELECT typname FROM pg_type WHERE typname IN ('safe_base', '_safe_base')",)
        .unwrap()
        .rows
        .is_empty());
    assert!(session
        .execute(
            "SELECT proname FROM pg_proc
             WHERE proname IN ('safe_base_in', 'safe_base_out', 'safe_base_recv', 'safe_base_send')",
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn base_type_finalization_rolls_back_to_the_original_shell() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TYPE rollback_base").unwrap();
    let shell_oid = session
        .execute("SELECT oid FROM pg_type WHERE typname = 'rollback_base'")
        .unwrap()
        .rows[0][0]
        .clone();
    session
        .execute(
            "CREATE FUNCTION rollback_base_in(cstring) RETURNS rollback_base
             AS 'textin' LANGUAGE internal IMMUTABLE STRICT",
        )
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION rollback_base_out(rollback_base) RETURNS cstring
             AS 'textout' LANGUAGE internal IMMUTABLE STRICT",
        )
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute(
            "CREATE TYPE rollback_base (
                INPUT=rollback_base_in, OUTPUT=rollback_base_out,
                INTERNALLENGTH=variable, ALIGNMENT=int4, STORAGE=extended
             )",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT oid, typisdefined FROM pg_type
                 WHERE typname = 'rollback_base'",
            )
            .unwrap()
            .rows,
        vec![vec![shell_oid.clone(), SqlValue::Bool(true)]]
    );
    session.execute("ROLLBACK").unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT oid, typarray, typtype, typisdefined FROM pg_type
                 WHERE typname = 'rollback_base'",
            )
            .unwrap()
            .rows,
        vec![vec![
            shell_oid,
            SqlValue::Int(0),
            SqlValue::String("p".to_string()),
            SqlValue::Bool(false),
        ]]
    );
}

#[test]
fn base_type_like_copies_only_the_physical_representation() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TYPE exact_int",
        "CREATE FUNCTION exact_int_in(cstring) RETURNS exact_int AS 'int4in' LANGUAGE internal IMMUTABLE STRICT",
        "CREATE FUNCTION exact_int_out(exact_int) RETURNS cstring AS 'int4out' LANGUAGE internal IMMUTABLE STRICT",
        "CREATE TYPE exact_int (INPUT=exact_int_in, OUTPUT=exact_int_out, LIKE=int4)",
        "CREATE TABLE exact_int_rows (id int PRIMARY KEY, value exact_int)",
        "INSERT INTO exact_int_rows VALUES (1, '42')",
    ] {
        session.execute(sql).unwrap();
    }
    assert_eq!(
        session
            .execute(
                "SELECT typlen, typbyval, typalign, typstorage, typcategory,
                        typispreferred, typdelim, typcollation
                 FROM pg_type WHERE typname = 'exact_int'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(4),
            SqlValue::Bool(true),
            SqlValue::String("i".to_string()),
            SqlValue::String("p".to_string()),
            SqlValue::String("U".to_string()),
            SqlValue::Bool(false),
            SqlValue::String(",".to_string()),
            SqlValue::Int(0),
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT value FROM exact_int_rows WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(42)]]
    );
    assert_eq!(
        session
            .execute("DROP FUNCTION exact_int_in(cstring)")
            .unwrap_err()
            .sqlstate(),
        "2BP01"
    );
    session
        .execute("DROP FUNCTION exact_int_in(cstring) CASCADE")
        .unwrap();
    assert!(session
        .execute("SELECT typname FROM pg_type WHERE typname = 'exact_int'")
        .unwrap()
        .rows
        .is_empty());
}
