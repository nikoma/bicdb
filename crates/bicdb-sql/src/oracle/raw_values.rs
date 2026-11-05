//! Oracle hexadecimal character-to-RAW conversion, distinct from BYTEA input.
use super::routine_arithmetic::Kind;
use super::*;

pub(super) fn lower_call(
    expr: &mut Expr,
    kind: impl Fn(&Expr) -> Kind,
    defer_unknown: bool,
    sql_context: bool,
) -> Result<()> {
    let Expr::Function(function) = expr else {
        return Ok(());
    };
    if !matches!(function.name.0.as_slice(), [part] if part.as_ident().is_some_and(|id| if id.quote_style.is_some() { id.value == "HEXTORAW" } else { id.value.eq_ignore_ascii_case("hextoraw") }))
    {
        return Ok(());
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(unsupported("HEXTORAW requires one scalar argument"));
    };
    if arguments.args.len() != 1
        || arguments.duplicate_treatment.is_some()
        || !arguments.clauses.is_empty()
        || !matches!(
            &arguments.args[0],
            FunctionArg::Unnamed(FunctionArgExpr::Expr(_))
        )
        || function.over.is_some()
        || function.filter.is_some()
        || !function.within_group.is_empty()
        || function.null_treatment.is_some()
        || !matches!(function.parameters, FunctionArguments::None)
        || function.uses_odbc_syntax
    {
        return Err(unsupported("HEXTORAW requires one scalar argument"));
    }
    let argument = function_args(function).remove(0);
    let source = match kind(&argument) {
        Kind::Unknown if defer_unknown => return Ok(()),
        Kind::Text | Kind::NationalText | Kind::FixedNationalText | Kind::Null => None,
        Kind::Number => Some("number"),
        Kind::Raw => Some("raw"),
        Kind::Date => Some("date"),
        _ => {
            return Err(unsupported(
                "HEXTORAW operand type requires a qualified character conversion",
            ))
        }
    };
    let value = render_sql(&argument);
    let value = source.map_or_else(
        || value.clone(),
        |source| {
            format!(
                "bicdb_oracle_default_to_char({value},'{source}','{}')",
                if sql_context { "sql" } else { "plsql" }
            )
        },
    );
    *expr = expression(&format!("bicdb_oracle_hextoraw({value})"))?;
    Ok(())
}

pub(crate) fn evaluate(args: &[SqlValue]) -> Result<SqlValue> {
    let [value] = args else {
        return Err(SqlError::InvalidSql(
            "HEXTORAW requires one argument".into(),
        ));
    };
    let text = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::String(text) => text,
        _ => {
            return Err(unsupported(
                "HEXTORAW requires a character conversion before evaluation",
            ))
        }
    };
    if text.is_empty() {
        return Ok(SqlValue::Null);
    }
    if !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(errors::database_error(-1465, "22018", "invalid hex number"));
    }
    let mut output = String::with_capacity(text.len() + 3);
    output.push_str("\\x");
    if text.len() % 2 != 0 {
        output.push('0');
    }
    output.push_str(&text.to_ascii_lowercase());
    Ok(SqlValue::String(output))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
        let sql = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        SqlSession::new(db).execute(&sql)
    }
    #[test]
    fn oracle_hex_conversion_preserves_bytes_padding_nulls_and_errors() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for (input, expected) in [
            ("'00FF'", Some("\\x00ff")),
            ("'aBcD'", Some("\\xabcd")),
            ("'A'", Some("\\x0a")),
            ("'ABC'", Some("\\x0abc")),
            ("''", None),
            ("NULL", None),
            ("N'00FF'", Some("\\x00ff")),
            ("1", Some("\\x01")),
            ("123", Some("\\x0123")),
            ("1.0", Some("\\x01")),
            ("HEXTORAW('AB')", Some("\\xab")),
        ] {
            let sql = format!("SELECT HEXTORAW({input}) FROM DUAL");
            assert_eq!(
                run(&mut db, &sql).unwrap().rows[0][0],
                expected
                    .map(|v| SqlValue::String(v.into()))
                    .unwrap_or(SqlValue::Null),
                "{sql}"
            );
        }
        for input in [
            "'G0'",
            "'00 FF'",
            "' 00FF '",
            "2.5",
            "-1",
            "'\\x00ff'",
            "'0x00ff'",
            "'1.0'",
            "CAST('AB' AS CHAR(4))",
        ] {
            let error = run(&mut db, &format!("SELECT HEXTORAW({input}) FROM DUAL")).unwrap_err();
            assert_eq!(error.sqlstate(), "22018", "{input}: {error}");
            assert!(error.to_string().contains("ORA-01465"), "{error}");
        }
        let sql = lower_session_sql(
            &db,
            "SELECT HEXTORAW('00FF') AS value FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )
        .unwrap();
        assert_eq!(
            infer_query_result_types(&db, &sql).unwrap().unwrap(),
            vec![Some("bytea".into())]
        );
        run(&mut db, "CREATE TABLE raw_value(payload BLOB)").unwrap();
        run(&mut db,"CREATE PROCEDURE save_raw_value IS BEGIN INSERT INTO raw_value VALUES(HEXTORAW('ABC')); END;").unwrap();
        run(&mut db, "CALL save_raw_value()").unwrap();
        assert_eq!(
            run(&mut db, "SELECT payload FROM raw_value").unwrap().rows[0][0],
            SqlValue::String("\\x0abc".into())
        );
    }
    #[test]
    fn bind_types_reach_the_typed_hex_conversion_pass() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let source = lower_session_sql(
            &db,
            "SELECT HEXTORAW($1) FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )
        .unwrap();
        for (oid, conversion) in [
            (25, None),
            (1043, None),
            (1700, Some("number")),
            (17, Some("raw")),
        ] {
            let typed = lower_typed_native_sql(&db, &source, ORACLE_PREVIEW_PROFILE, &[oid])
                .unwrap_or_else(|error| panic!("OID {oid}: {error}"));
            assert!(typed.contains("bicdb_oracle_hextoraw"), "{oid}: {typed}");
            if let Some(kind) = conversion {
                assert!(typed.contains(&format!("'{kind}'")), "{oid}: {typed}");
            }
            assert_eq!(
                infer_query_result_types(&db, &typed).unwrap().unwrap(),
                vec![Some("bytea".into())]
            );
        }
    }

    #[test]
    fn persisted_raw_contract_preserves_previous_language_identity() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            preview_routine_language_version(PREVIEW_ROUTINE_LANGUAGE),
            Some(28)
        );
        assert_eq!(
            preview_routine_language_version(PREVIEW_ROUTINE_LANGUAGE_V27),
            Some(27)
        );
        let definition = lower_session_sql(
            &db,
            "CREATE FUNCTION previous_hex RETURN BLOB IS BEGIN RETURN HEXTORAW('AB'); END;",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )
        .unwrap()
        .replace(PREVIEW_ROUTINE_LANGUAGE, PREVIEW_ROUTINE_LANGUAGE_V27);
        SqlSession::new(&mut db).execute(&definition).unwrap();
        assert!(run(&mut db, "SELECT previous_hex() FROM DUAL").is_err());
        let rows=SqlSession::new(&mut db).execute("SELECT lanname,oid FROM pg_catalog.pg_language WHERE lanname IN ('bicdb_oracle_preview_v27','bicdb_oracle_preview_v28') ORDER BY oid").unwrap().rows;
        assert_eq!(
            rows,
            vec![
                vec![
                    SqlValue::String(PREVIEW_ROUTINE_LANGUAGE_V27.into()),
                    SqlValue::Int(999_426)
                ],
                vec![
                    SqlValue::String(PREVIEW_ROUTINE_LANGUAGE.into()),
                    SqlValue::Int(999_427)
                ]
            ]
        );
    }

    #[test]
    fn postgres_bytea_input_and_authored_hextoraw_are_independent() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        assert!(session.execute("SELECT hextoraw('A')").is_err());
        assert!(session.execute("SELECT '\\xA'::bytea").is_err());
        session.execute("CREATE FUNCTION hextoraw(v text) RETURNS text LANGUAGE sql AS $$SELECT 'authored'$$").unwrap();
        assert_eq!(
            session.execute("SELECT hextoraw('A')").unwrap().rows[0][0],
            SqlValue::String("authored".into())
        );
    }
}
