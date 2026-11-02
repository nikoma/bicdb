use super::*;

fn execute(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
    let lowered = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
    SqlSession::new(db).execute(&lowered)
}

fn preview(path: &std::path::Path) -> BicDb {
    let mut db =
        BicDb::open_with_config(path, bicdb_core::DbConfig::default().with_fsync(false)).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    db
}

#[test]
fn oracle_chr_decodes_complete_al32utf8_characters_in_sql_and_routines() {
    let root = tempfile::tempdir().unwrap();
    let mut db = preview(root.path());
    execute(&mut db, "CREATE FUNCTION character_value(n NUMBER) RETURN VARCHAR2 IS BEGIN RETURN TO_CHAR(CHR(n)); END;").unwrap();
    assert_eq!(
        load_routine(&db, RoutineKind::Function, "character_value")
            .unwrap()
            .unwrap()
            .language,
        PREVIEW_ROUTINE_LANGUAGE
    );
    for (code, expected) in [
        (65_u32, "A"),
        (49835, "«"),
        (49851, "»"),
        (50089, "é"),
        (14844588, "€"),
        (14989485, "中"),
        (4036991104, "😀"),
    ] {
        for (sql, physical_type) in [
            (format!("SELECT CHR({code}) FROM DUAL"), "text"),
            (
                format!("SELECT character_value({code}) FROM DUAL"),
                "varchar",
            ),
        ] {
            let result = execute(&mut db, &sql).unwrap();
            assert_eq!(result.rows[0][0].to_cell(), expected, "{sql}");
            assert_eq!(result.column_types, vec![Some(physical_type.into())]);
        }
    }
}

#[test]
fn default_to_char_resolves_nested_sql_and_procedural_character_results() {
    let root = tempfile::tempdir().unwrap();
    let mut db = preview(root.path());
    execute(
        &mut db,
        "CREATE TABLE character_rows(k NUMBER,v VARCHAR2(10))",
    )
    .unwrap();
    execute(
        &mut db,
        "INSERT INTO character_rows VALUES(1,'prefix'),(2,NULL)",
    )
    .unwrap();
    let sql = "SELECT TO_CHAR(COALESCE(v,TO_CHAR(CHR(49835)))) FROM character_rows ORDER BY k";
    assert_eq!(
        execute(&mut db, sql).unwrap().rows,
        vec![
            vec![SqlValue::String("prefix".into())],
            vec![SqlValue::String("«".into())]
        ]
    );
    execute(&mut db,"CREATE FUNCTION nested_character(n NUMBER) RETURN VARCHAR2 IS result VARCHAR2(30); BEGIN SELECT COALESCE(TO_CHAR(''),TO_CHAR(CHR(n))) INTO result FROM DUAL; RETURN '['||result||']'; END;").unwrap();
    assert_eq!(
        execute(&mut db, "SELECT nested_character(49835) FROM DUAL")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "[«]"
    );
    for value in ["NULL", "''"] {
        assert_eq!(
            execute(&mut db, &format!("SELECT TO_CHAR({value}) FROM DUAL"))
                .unwrap()
                .rows[0][0],
            SqlValue::Null
        );
    }
}

#[test]
fn oracle_chr_number_conversion_and_unsupported_encodings_are_explicit() {
    let root = tempfile::tempdir().unwrap();
    let mut db = preview(root.path());
    for value in ["65.4", "65.5", "65.9", "' 65.9 '", "6.59e1"] {
        assert_eq!(
            execute(&mut db, &format!("SELECT CHR({value}) FROM DUAL"))
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "A"
        );
    }
    for value in ["NULL", "''"] {
        assert_eq!(
            execute(&mut db, &format!("SELECT CHR({value}) FROM DUAL"))
                .unwrap()
                .rows[0][0],
            SqlValue::Null
        );
    }
    for value in ["-1", "-0.1", "0.1", "1e-50", "4294967296"] {
        let error = execute(&mut db, &format!("SELECT CHR({value}) FROM DUAL")).unwrap_err();
        assert_eq!(error.sqlstate(), "22003", "{value}: {error}");
    }
    for value in ["0", "128", "233", "16706"] {
        let error = execute(&mut db, &format!("SELECT CHR({value}) FROM DUAL")).unwrap_err();
        assert_eq!(error.sqlstate(), "0A000", "{value}: {error}");
    }
    assert_eq!(
        execute(&mut db, "SELECT CHR('invalid') FROM DUAL")
            .unwrap_err()
            .sqlstate(),
        "22P02"
    );
}

#[test]
fn older_routine_character_contract_and_native_postgres_chr_are_independent() {
    let root = tempfile::tempdir().unwrap();
    let mut db = preview(root.path());
    SqlSession::new(&mut db).execute("CREATE FUNCTION previous_character() RETURNS TEXT LANGUAGE bicdb_oracle_preview_v9 AS $$BEGIN RETURN chr(49835); END$$").unwrap();
    let previous = execute(&mut db, "SELECT previous_character() FROM DUAL").unwrap();
    assert_eq!(
        previous.rows[0][0].to_cell(),
        char::from_u32(49835).unwrap().to_string()
    );
    let native_root = tempfile::tempdir().unwrap();
    let mut native = BicDb::open(native_root.path()).unwrap();
    let result = SqlSession::new(&mut native)
        .execute("SELECT chr(49835),pg_catalog.chr(49851)")
        .unwrap();
    assert_eq!(
        result.rows[0],
        vec![
            SqlValue::String(char::from_u32(49835).unwrap().to_string()),
            SqlValue::String(char::from_u32(49851).unwrap().to_string())
        ]
    );
    assert_eq!(result.column_types, vec![Some("text".into()); 2]);
    assert_eq!(
        SqlSession::new(&mut native)
            .execute("SELECT chr(CAST(NULL AS INTEGER))")
            .unwrap()
            .rows[0][0],
        SqlValue::Null
    );
    for code in [55296, 1114112] {
        assert_eq!(
            SqlSession::new(&mut native)
                .execute(&format!("SELECT chr({code})"))
                .unwrap_err()
                .sqlstate(),
            "54000"
        );
    }
    assert!(SqlSession::new(&mut native)
        .execute("SELECT to_char(chr(65))")
        .is_err());
}

#[test]
fn coalesce_skips_unselected_character_errors_in_every_evaluator() {
    let root = tempfile::tempdir().unwrap();
    let mut db = preview(root.path());
    assert_eq!(
        execute(&mut db, "SELECT COALESCE(CHR(65),CHR(-1)) FROM DUAL")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "A"
    );
    // The specialized virtual-catalog evaluator supports literal text; keep
    // the selected operand within that contract while guarding the unused call.
    let statement = parse_statements("SELECT coalesce('A',chr(-1))")
        .unwrap()
        .remove(0);
    let Statement::Query(query) = statement else {
        panic!("query")
    };
    let SetExpr::Select(select) = *query.body else {
        panic!("select")
    };
    let SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
        panic!("expression")
    };
    assert_eq!(eval_constant_expr(expr).unwrap().to_cell(), "A");
    assert_eq!(
        SqlSession::new(&mut db)
            .eval_session_expr(expr)
            .unwrap()
            .to_cell(),
        "A"
    );
    execute(&mut db,"CREATE FUNCTION short_character(p_label VARCHAR2) RETURN VARCHAR2 IS BEGIN RETURN COALESCE(p_label,CHR(-1)); END;").unwrap();
    assert_eq!(
        execute(&mut db, "SELECT short_character('prefix') FROM DUAL")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "prefix"
    );
    assert_eq!(
        execute(&mut db, "SELECT short_character(NULL) FROM DUAL")
            .unwrap_err()
            .sqlstate(),
        "22003"
    );
    assert_eq!(
        SqlEngine::new(&db)
            .eval_select_constant_expr(expr)
            .unwrap()
            .to_cell(),
        "A"
    );
    assert_eq!(eval_value(&Record::new("r"), expr).unwrap().to_cell(), "A");
    assert_eq!(
        eval_virtual_value(&BTreeMap::new(), expr)
            .unwrap()
            .to_cell(),
        "A"
    );
    let native = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(native.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut db)
            .execute("SELECT coalesce(chr(65),chr(-1))")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "A"
    );
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE lazy_values(k INTEGER PRIMARY KEY,label TEXT DEFAULT coalesce(chr(65),chr(-1)))").unwrap();
    session
        .execute("INSERT INTO lazy_values(k) VALUES(1)")
        .unwrap();
    session
        .execute("UPDATE lazy_values SET label=coalesce(label,chr(-1))")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT label FROM lazy_values")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "A"
    );
}
