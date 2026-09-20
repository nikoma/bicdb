use bicdb_core::{BicDb, Record};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

const CREATE_TABLE: &str = r#"
    CREATE TABLE binary_values (
        id TEXT PRIMARY KEY,
        payload BYTEA,
        fixed_bits BIT(5),
        flexible_bits VARBIT
    )
"#;

#[test]
fn bytea_and_bits_use_typed_binary_storage_and_read_legacy_records() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session.execute(CREATE_TABLE).unwrap();
        session
            .execute(
                "INSERT INTO binary_values VALUES
                 ('typed', '\\x00ff10'::bytea, '10101'::bit(5), '001001'::varbit)",
            )
            .unwrap();
    }

    let stored = db.get("binary_values", "typed").unwrap().unwrap();
    let bytes = &stored.metadata["payload"]["$bicdb_typed"];
    assert_eq!(bytes["version"], 1);
    assert_eq!(bytes["pg_type"], "bytea");
    assert_eq!(bytes["value"]["type"], "bytes");
    assert_eq!(bytes["value"]["value"], json!([0, 255, 16]));
    assert!(!stored.metadata["payload"].is_string());

    let fixed = &stored.metadata["fixed_bits"]["$bicdb_typed"];
    assert_eq!(fixed["pg_type"], "bit");
    assert_eq!(fixed["value"]["type"], "bit_string");
    assert_eq!(fixed["value"]["value"]["bytes"], json!([168]));
    assert_eq!(fixed["value"]["value"]["bit_len"], 5);
    let flexible = &stored.metadata["flexible_bits"]["$bicdb_typed"];
    assert_eq!(flexible["value"]["value"]["bytes"], json!([36]));
    assert_eq!(flexible["value"]["value"]["bit_len"], 6);

    // Older JSON-array byte records and textual bit records remain readable.
    db.insert(
        "binary_values",
        Record::new("legacy").with_metadata(json!({
            "payload": [222, 173, 190, 239],
            "fixed_bits": "11111",
            "flexible_bits": "0"
        })),
    )
    .unwrap();

    let rows = SqlSession::new(&mut db)
        .execute(
            "SELECT id, payload, fixed_bits, flexible_bits
             FROM binary_values ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![
            vec![
                SqlValue::String("legacy".to_string()),
                SqlValue::String("\\xdeadbeef".to_string()),
                SqlValue::String("11111".to_string()),
                SqlValue::String("0".to_string()),
            ],
            vec![
                SqlValue::String("typed".to_string()),
                SqlValue::String("\\x00ff10".to_string()),
                SqlValue::String("10101".to_string()),
                SqlValue::String("001001".to_string()),
            ],
        ]
    );

    assert_eq!(
        SqlSession::new(&mut db)
            .execute(
                "SELECT length(payload), bit_length(payload), encode(payload, 'hex')
                 FROM binary_values WHERE id = 'typed'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(3),
            SqlValue::Int(24),
            SqlValue::String("00ff10".to_string()),
        ]]
    );

    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE INDEX binary_values_payload_idx ON binary_values (payload)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM binary_values WHERE payload = '\\x00ff10'::bytea")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("typed".to_string())]]
    );

    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT payload, fixed_bits FROM binary_values WHERE id = 'typed'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("\\x00ff10".to_string()),
            SqlValue::String("10101".to_string()),
        ]]
    );
}

#[test]
fn binary_updates_replace_payloads_and_invalid_input_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute(CREATE_TABLE).unwrap();
    session
        .execute("INSERT INTO binary_values VALUES ('entry', '\\x01', '00000', '1')")
        .unwrap();
    session
        .execute(
            "UPDATE binary_values
             SET payload = '\\xabcdef', fixed_bits = '11001', flexible_bits = '000000001'
             WHERE id = 'entry'",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT payload, fixed_bits, flexible_bits FROM binary_values")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("\\xabcdef".to_string()),
            SqlValue::String("11001".to_string()),
            SqlValue::String("000000001".to_string()),
        ]]
    );
    assert!(session
        .execute("UPDATE binary_values SET payload = '\\x0' WHERE id = 'entry'")
        .is_err());
    assert!(session
        .execute("UPDATE binary_values SET flexible_bits = '102' WHERE id = 'entry'")
        .is_err());
}

#[test]
fn bytea_operators_and_functions_match_postgresql_bytes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let rows = session
        .execute(
            "SELECT
                encode('\\x00ff415c'::bytea, 'hex'),
                octet_length('\\x00ff415c'::bytea),
                length('\\x00ff415c'::bytea),
                bit_length('\\x00ff415c'::bytea),
                bit_count('\\xf00f'::bytea),
                get_byte('\\x00ff415c'::bytea, 1),
                get_bit('\\x01'::bytea, 0),
                get_bit('\\x01'::bytea, 7),
                set_byte('\\x00ff'::bytea, 0, 171),
                set_byte('\\x00ff'::bytea, 0, -1),
                set_bit('\\x00'::bytea, 0, 1),
                set_bit('\\x00'::bytea, 7, 1)",
        )
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![vec![
            SqlValue::String("00ff415c".to_string()),
            SqlValue::Int(4),
            SqlValue::Int(4),
            SqlValue::Int(32),
            SqlValue::Int(8),
            SqlValue::Int(255),
            SqlValue::Int(1),
            SqlValue::Int(0),
            SqlValue::String("\\xabff".to_string()),
            SqlValue::String("\\xffff".to_string()),
            SqlValue::String("\\x01".to_string()),
            SqlValue::String("\\x80".to_string()),
        ]]
    );

    session
        .execute("CREATE TABLE bytea_function_rows (id TEXT PRIMARY KEY, bytes BYTEA)")
        .unwrap();
    session
        .execute("INSERT INTO bytea_function_rows VALUES ('stored', '\\x0001020304')")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT substring(bytes FROM 2 FOR 3),
                        position('\\x0203'::bytea IN bytes),
                        overlay(bytes PLACING '\\xaabb'::bytea FROM 2 FOR 2)
                 FROM bytea_function_rows WHERE id = 'stored'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("\\x010203".to_string()),
            SqlValue::Int(3),
            SqlValue::String("\\x00aabb0304".to_string()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT
                    '\\x00ff'::bytea || '\\x415c'::bytea,
                    substring('\\x0011223344'::bytea FROM 2 FOR 3),
                    reverse('\\x00ff415c'::bytea),
                    '\\x00ff'::bytea < '\\x0100'::bytea,
                    '\\x00ff'::bytea < '\\x00ff00'::bytea"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("\\x00ff415c".to_string()),
            SqlValue::String("\\x112233".to_string()),
            SqlValue::String("\\x5c41ff00".to_string()),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );

    assert_eq!(
        session
            .execute(
                r#"SELECT
                    md5('\x00ff'::bytea),
                    sha256('abc'::bytea),
                    crc32('abc'::bytea),
                    crc32c('abc'::bytea),
                    encode(decode('AP9BXA==', 'base64'), 'hex'),
                    encode(decode('\000\\''A\177\377\200', 'escape'), 'hex')"#,
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("d07d34efac6328007ad67c7e0a985e00".to_string()),
            SqlValue::String(
                "\\xba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_string(),
            ),
            SqlValue::Int(891_568_578),
            SqlValue::Int(910_901_175),
            SqlValue::String("00ff415c".to_string()),
            SqlValue::String("005c27417fff80".to_string()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                r#"SELECT
                    substring('\x00010203'::bytea FROM 0 FOR 2),
                    position('\x0203'::bytea IN '\x0001020304'::bytea),
                    overlay('\x0001020304'::bytea PLACING '\xaabb'::bytea FROM 2 FOR 2),
                    btrim('\x0001000002'::bytea, '\x0002'::bytea),
                    ltrim('\x0001000002'::bytea, '\x0002'::bytea),
                    rtrim('\x0001000002'::bytea, '\x0002'::bytea),
                    convert_from('\x636166c3a9'::bytea, 'UTF8'),
                    encode(convert_to('café', 'UTF8'), 'hex'),
                    encode(convert('\x636166e9'::bytea, 'LATIN1', 'UTF8'), 'hex')"#,
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("\\x00".to_string()),
            SqlValue::Int(3),
            SqlValue::String("\\x00aabb0304".to_string()),
            SqlValue::String("\\x01".to_string()),
            SqlValue::String("\\x01000002".to_string()),
            SqlValue::String("\\x0001".to_string()),
            SqlValue::String("café".to_string()),
            SqlValue::String("636166c3a9".to_string()),
            SqlValue::String("636166c3a9".to_string()),
        ]]
    );

    let base64 = session
        .execute(&format!(
            "SELECT encode('{}'::bytea, 'base64')",
            "a".repeat(60)
        ))
        .unwrap();
    assert_eq!(
        base64.rows,
        vec![vec![SqlValue::String(
            "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFh\nYWFh"
                .to_string()
        )]]
    );
    assert_eq!(
        session
            .execute("SELECT encode(decode(E'AAEC\\nAwQ=', 'base64'), 'hex')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("0001020304".to_string())]]
    );
}

#[test]
fn bytea_function_errors_use_postgresql_sqlstates() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    for (sql, sqlstate) in [
        ("SELECT decode('0', 'hex')", "22023"),
        ("SELECT decode('zz', 'hex')", "22023"),
        ("SELECT '\\x0'::bytea", "22023"),
        ("SELECT '\\400'::bytea", "22P02"),
        ("SELECT decode('\\400', 'escape')", "22P02"),
        ("SELECT get_byte('\\x00'::bytea, 1)", "2202E"),
        ("SELECT get_bit('\\x00'::bytea, -1)", "2202E"),
        ("SELECT set_bit('\\x00'::bytea, 0, 2)", "22023"),
        ("SELECT encode('\\x00'::bytea, 'unknown')", "22023"),
        ("SELECT substring('\\x00'::bytea FROM 1 FOR -1)", "22011"),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
    }
}

#[test]
fn bytea_output_guc_switches_text_rendering_without_changing_storage() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute(CREATE_TABLE).unwrap();
    session
        .execute(
            "INSERT INTO binary_values (id, payload) VALUES
             ('output', '\\x005c27417fff80'::bytea)",
        )
        .unwrap();

    assert_eq!(
        session.execute("SHOW bytea_output").unwrap().rows,
        vec![vec![SqlValue::String("hex".to_string())]]
    );
    session.execute("SET bytea_output = escape").unwrap();
    assert_eq!(
        session
            .execute("SELECT payload FROM binary_values WHERE id = 'output'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "\\000\\\\'A\\177\\377\\200".to_string()
        )]]
    );
    session.execute("RESET bytea_output").unwrap();
    assert_eq!(
        session
            .execute("SELECT payload FROM binary_values WHERE id = 'output'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("\\x005c27417fff80".to_string())]]
    );
}

#[test]
fn fixed_bit_typmods_literals_operators_casts_functions_and_indexes_match_postgresql() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE fixed_bits (id TEXT PRIMARY KEY, flags BIT(5) UNIQUE)")
        .unwrap();
    session
        .execute("CREATE INDEX fixed_bits_flags_idx ON fixed_bits (flags)")
        .unwrap();
    session
        .execute("INSERT INTO fixed_bits VALUES ('a', B'10101'), ('b', X'3'::bit(5))")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id, flags FROM fixed_bits ORDER BY flags")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("b".to_string()),
                SqlValue::String("00110".to_string())
            ],
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("10101".to_string())
            ],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM fixed_bits WHERE flags = B'10101'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("a".to_string())]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT X'A'::bit(5), '101'::bit(5), '101010'::bit(5),
                        5::bit(5), (-1)::bit(5), B'10101'::int4,
                        B'10101' & B'11000', B'10101' | B'11000',
                        B'10101' # B'11000', ~B'10101',
                        B'10101' << 2, B'10101' >> 2, B'101' || B'01'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("10100".to_string()),
            SqlValue::String("10100".to_string()),
            SqlValue::String("10101".to_string()),
            SqlValue::String("00101".to_string()),
            SqlValue::String("11111".to_string()),
            SqlValue::Int(21),
            SqlValue::String("10000".to_string()),
            SqlValue::String("11101".to_string()),
            SqlValue::String("01101".to_string()),
            SqlValue::String("01010".to_string()),
            SqlValue::String("10100".to_string()),
            SqlValue::String("00101".to_string()),
            SqlValue::String("10101".to_string()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT bit_count(B'10101'), length(B'10101'),
                        bit_length(B'10101'), octet_length(B'10101'),
                        get_bit(B'10101', 0), set_bit(B'10101', 1, 1)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(3),
            SqlValue::Int(5),
            SqlValue::Int(5),
            SqlValue::Int(1),
            SqlValue::Int(1),
            SqlValue::String("11101".to_string()),
        ]]
    );

    for (sql, sqlstate) in [
        ("INSERT INTO fixed_bits VALUES ('short', B'101')", "22026"),
        ("INSERT INTO fixed_bits VALUES ('long', B'101010')", "22026"),
        ("SELECT B'101' & B'10'", "22026"),
        ("SELECT get_bit(B'1', 1)", "2202E"),
        ("SELECT set_bit(B'1', 0, 2)", "22023"),
        ("SELECT B'10101'::int2", "42846"),
        ("CREATE TABLE invalid_fixed_bits (flags BIT(0))", "22023"),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
    }
}

#[test]
fn variable_bit_typmods_casts_result_types_and_indexes_match_postgresql() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE variable_bits (id TEXT PRIMARY KEY, flags VARBIT(9) UNIQUE)")
        .unwrap();
    session
        .execute("CREATE INDEX variable_bits_flags_idx ON variable_bits (flags)")
        .unwrap();
    session
        .execute(
            "INSERT INTO variable_bits VALUES
             ('empty', B''), ('zero', B'0'), ('two_zero', B'00'),
             ('zero_zero_one', B'001'), ('zero_one', B'01'),
             ('one', B'1'), ('one_long', B'100000000'), ('one_one', B'11')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id, flags FROM variable_bits ORDER BY flags")
            .unwrap()
            .rows,
        [
            ("empty", ""),
            ("zero", "0"),
            ("two_zero", "00"),
            ("zero_zero_one", "001"),
            ("zero_one", "01"),
            ("one", "1"),
            ("one_long", "100000000"),
            ("one_one", "11"),
        ]
        .into_iter()
        .map(|(id, flags)| {
            vec![
                SqlValue::String(id.to_string()),
                SqlValue::String(flags.to_string()),
            ]
        })
        .collect::<Vec<_>>()
    );
    assert_eq!(
        session
            .execute("SELECT id FROM variable_bits WHERE flags = B'100000000'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("one_long".to_string())]]
    );

    let casts = session
        .execute(
            "SELECT B'1'::varbit(5), B'101010'::varbit(5), B''::varbit(5),
                    B'101'::varbit::bit(5), B'101'::bit(5)::varbit(2)",
        )
        .unwrap();
    assert_eq!(
        casts.rows,
        vec![vec![
            SqlValue::String("1".to_string()),
            SqlValue::String("10101".to_string()),
            SqlValue::String(String::new()),
            SqlValue::String("10100".to_string()),
            SqlValue::String("10".to_string()),
        ]]
    );
    assert_eq!(
        casts.column_types,
        vec![
            Some("varbit".to_string()),
            Some("varbit".to_string()),
            Some("varbit".to_string()),
            Some("bit".to_string()),
            Some("varbit".to_string()),
        ]
    );

    let operators = session
        .execute(
            "SELECT ~(B'1'::varbit), B'1'::varbit << 1,
                    B'1'::varbit & B'0'::varbit, B'1'::varbit || B'0'::varbit,
                    set_bit(B'1'::varbit, 0, 0),
                    substring(B'101'::varbit FROM 1 FOR 2),
                    overlay(B'101'::varbit PLACING B'1' FROM 2 FOR 1)",
        )
        .unwrap();
    assert_eq!(
        operators.rows,
        vec![vec![
            SqlValue::String("0".to_string()),
            SqlValue::String("0".to_string()),
            SqlValue::String("0".to_string()),
            SqlValue::String("10".to_string()),
            SqlValue::String("0".to_string()),
            SqlValue::String("10".to_string()),
            SqlValue::String("111".to_string()),
        ]]
    );
    assert_eq!(
        operators.column_types,
        vec![
            Some("bit".to_string()),
            Some("bit".to_string()),
            Some("bit".to_string()),
            Some("varbit".to_string()),
            Some("bit".to_string()),
            Some("bit".to_string()),
            Some("bit".to_string()),
        ]
    );

    for (sql, sqlstate) in [
        (
            "INSERT INTO variable_bits VALUES ('long', B'1010101010')",
            "22001",
        ),
        (
            "UPDATE variable_bits SET flags = B'1010101010' WHERE id = 'empty'",
            "22001",
        ),
        ("SELECT B'1'::varbit & B'01'::varbit", "22026"),
        ("SELECT 5::varbit(5)", "42846"),
        ("SELECT B'1'::varbit::int4", "42846"),
        ("SELECT 1::int2::bit(5)", "42846"),
        ("SELECT get_bit(B''::varbit, 0)", "2202E"),
        (
            "CREATE TABLE invalid_variable_bits (flags VARBIT(0))",
            "22023",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
    }
}
