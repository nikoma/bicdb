//! Type-directed Oracle DECODE. A simple CASE evaluates the selector once and
//! visits searches/results lazily. Opaque, non-NULL keys supply DECODE's NULL
//! equality and nonpadded character comparison without changing native CASE.
use super::routine_arithmetic::Kind;
use super::*;

fn kind_name(kind: Kind) -> Result<&'static str> {
    match kind {
        Kind::Null | Kind::Text => Ok("text"),
        Kind::Number => Ok("number"),
        Kind::Date => Ok("date"),
        Kind::Timestamp => Ok("timestamp"),
        _ => Err(unsupported(
            "Oracle DECODE requires resolved character, NUMBER, DATE or TIMESTAMP operands",
        )),
    }
}

pub(super) fn is_decode(function: &Function) -> bool {
    // Qualified application functions and pg_catalog.decode retain their own
    // meaning. Quoted uppercase names are how the Oracle frontend represents
    // unquoted source identifiers.
    function.name.0.len() == 1
        && object_name_lowercase(&function.name).is_ok_and(|name| name == "decode")
}

fn physical_dates(expr: &mut Expr) -> Result<()> {
    match visit_expressions_mut(expr, |expr| {
        let sql = match expr {
            Expr::Cast {
                expr,
                data_type: DataType::Date,
                ..
            } => Some(format!("bicdb_oracle_date_coerce({expr})")),
            Expr::TypedString(value) if value.data_type == DataType::Date => {
                Some(format!("bicdb_oracle_date_coerce({})", value.value))
            }
            _ => None,
        };
        if let Some(sql) = sql {
            match expression(&sql) {
                Ok(replacement) => *expr = replacement,
                Err(error) => return ControlFlow::Break(error),
            }
        }
        ControlFlow::Continue(())
    }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

pub(super) fn lower_call(
    expr: &mut Expr,
    kind: impl Fn(&Expr) -> Kind,
    sql_context: bool,
    defer_unknown: bool,
) -> Result<()> {
    let Expr::Function(function) = expr else {
        return Ok(());
    };
    if !is_decode(function) {
        return Ok(());
    }
    if !sql_context {
        return Err(unsupported(
            "Oracle DECODE may be used inside a SQL statement only",
        ));
    }
    let mut args = function_args(function);
    if args.len() < 3 {
        return Err(SqlError::InvalidSql(
            "Oracle DECODE requires at least three components".into(),
        ));
    }
    // function_args deliberately omits unsupported argument notations. Do not
    // reinterpret DISTINCT, ordered aggregates or named arguments as DECODE.
    let FunctionArguments::List(list) = &function.args else {
        return Err(unsupported("Oracle DECODE requires scalar arguments"));
    };
    if args.len() != list.args.len()
        || list.duplicate_treatment.is_some()
        || !list.clauses.is_empty()
        || function.over.is_some()
        || function.filter.is_some()
    {
        return Err(unsupported("Oracle DECODE requires scalar arguments"));
    }
    let kinds = args.iter().map(&kind).collect::<Vec<_>>();
    if defer_unknown && kinds.contains(&Kind::Unknown) {
        return Ok(());
    }
    let comparison = kind_name(kinds[1])?;
    let result = kind_name(kinds[2])?;
    let incompatible = |source: Kind, target: &str| {
        matches!(source, Kind::Date | Kind::Timestamp) && target == "number"
            || source == Kind::Number && matches!(target, "date" | "timestamp")
    };
    for (index, source) in kinds.iter().copied().enumerate() {
        kind_name(source)?;
        let is_result =
            index >= 2 && (index % 2 == 0 || index == args.len() - 1 && args.len() % 2 == 0);
        if incompatible(source, if is_result { result } else { comparison }) {
            return Err(SqlError::data_exception(
                "42804",
                "inconsistent Oracle DECODE operand types",
                None,
            ));
        }
    }
    for arg in &mut args {
        physical_dates(arg)?;
    }
    let key = |index: usize| -> Result<String> {
        Ok(format!(
            "bicdb_oracle_decode_key({},'{}','{comparison}')",
            date_trunc::exact_numeric_operand_sql(&args[index]),
            kind_name(kinds[index])?
        ))
    };
    let value = |index: usize| -> Result<String> {
        Ok(format!(
            "bicdb_oracle_decode_{result}({},'{}')",
            date_trunc::exact_numeric_operand_sql(&args[index]),
            kind_name(kinds[index])?
        ))
    };
    let mut sql = format!("CASE {}", key(0)?);
    let end = if args.len() % 2 == 0 {
        args.len() - 1
    } else {
        args.len()
    };
    for search in (1..end).step_by(2) {
        sql.push_str(&format!(
            " WHEN {} THEN {}",
            key(search)?,
            value(search + 1)?
        ));
    }
    let default = if end < args.len() {
        value(end)?
    } else {
        format!("bicdb_oracle_decode_{result}(NULL,'text')")
    };
    sql.push_str(&format!(" ELSE {default} END"));
    *expr = expression(&sql)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execute(session: &mut SqlSession<'_>, source: &str) -> Result<SqlResult> {
        let lowered = lower_session_sql(
            session.db_ref(),
            source,
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )?;
        session.execute(&lowered)
    }

    #[test]
    fn decode_reference_values_and_types() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        // Independently measured Oracle 23.26 results. NULL is equal only for
        // this construct; CHAR padding is retained rather than bpchar-cast away.
        for (input, expected, kind) in [
            ("DECODE(NULL,NULL,'yes','no')", "yes", "text"),
            ("DECODE('',NULL,'yes','no')", "yes", "text"),
            ("DECODE('001',1,'yes','no')", "yes", "text"),
            ("DECODE(1,'001','yes','no')", "no", "text"),
            (
                "DECODE(CAST('x' AS CHAR(3)),CAST('x' AS CHAR(1)),'yes','no')",
                "no",
                "text",
            ),
            (
                "DECODE(CAST('x' AS CHAR(3)),'x  ','yes','no')",
                "yes",
                "text",
            ),
            ("DECODE(1,1,CAST('x' AS CHAR(3)),'no')", "x  ", "text"),
            ("DECODE(2,1,NULL,2,0.5)", ".5", "text"),
            ("DECODE(2,1,CAST(NULL AS NUMBER),2,3)", "3", "numeric"),
            ("DECODE(1,1,7,CAST('bad' AS NUMBER))", "7", "numeric"),
            ("DECODE(1,1,7,1/0,9,0)", "7", "numeric"),
            ("DECODE(1,1,7,2,1/0,0)", "7", "numeric"),
            ("DECODE(1,1,7,2,'bad',0)", "7", "numeric"),
            (
                "DECODE(TIMESTAMP '2024-02-29 00:00:00.123456',DATE '2024-02-29',17,0)",
                "17",
                "numeric",
            ),
            (
                "DECODE(1,1,DATE '2024-02-29',NULL)",
                "2024-02-29 00:00:00",
                "timestamp",
            ),
            ("DECODE('29-FEB-24',DATE '2024-02-29',1,0)", "1", "numeric"),
            ("DECODE(2,1,NULL,2,DATE '2024-02-29')", "29-FEB-24", "text"),
            (
                "DECODE(2,1,NULL,2,TIMESTAMP '2024-02-29 12:34:56.123456')",
                "29-FEB-24 12.34.56.123456000 PM",
                "text",
            ),
            (
                "DECODE(DECODE(1,1,DATE '2024-02-29'),DATE '2024-02-29',17,0)",
                "17",
                "numeric",
            ),
        ] {
            let source = format!("SELECT {input} FROM DUAL");
            let result =
                execute(&mut session, &source).unwrap_or_else(|error| panic!("{source}: {error}"));
            assert_eq!(result.rows[0][0].to_cell(), expected, "{source}");
            assert_eq!(result.column_types, vec![Some(kind.into())], "{source}");
        }
        let result = execute(&mut session, "SELECT DECODE(9,1,7) FROM DUAL").unwrap();
        assert_eq!(result.rows, vec![vec![SqlValue::Null]]);
        assert_eq!(result.column_types, vec![Some("numeric".into())]);
    }

    #[test]
    fn decode_errors_are_lazy_but_type_incompatibilities_are_static() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for (input, state) in [
            ("DECODE(2,1,7,2,'bad',0)", "22P02"),
            ("DECODE(2,1,7,1/0,9,0)", "22012"),
            ("DECODE(2,1,7,2,1/0,0)", "22012"),
            ("DECODE('bad',1,1,0)", "22P02"),
            ("DECODE('1,234',1234,1,0)", "22P02"),
            ("DECODE('2024-02-29',DATE '2024-02-29',1,0)", "22007"),
            ("DECODE(1,1,7,2,DATE '2024-02-29',0)", "42804"),
        ] {
            let error = execute(&mut session, &format!("SELECT {input} FROM DUAL")).unwrap_err();
            assert_eq!(error.sqlstate(), state, "{input}: {error}");
        }
    }

    #[test]
    fn decode_stored_sql_is_versioned_and_direct_procedural_use_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        execute(&mut session, "CREATE FUNCTION choose_text(p_value NUMBER) RETURN VARCHAR2 IS result VARCHAR2(20); BEGIN SELECT DECODE(p_value,1,'one',NULL,'nil','other') INTO result FROM DUAL; RETURN result; END;").unwrap();
        for (input, expected) in [("1", "one"), ("NULL", "nil"), ("2", "other")] {
            assert_eq!(
                execute(
                    &mut session,
                    &format!("SELECT choose_text({input}) FROM DUAL")
                )
                .unwrap()
                .rows[0][0]
                    .to_cell(),
                expected
            );
        }
        execute(
            &mut session,
            "CREATE FUNCTION direct_decode RETURN NUMBER IS BEGIN RETURN DECODE(1,1,7,0); END;",
        )
        .unwrap();
        let error = execute(&mut session, "SELECT direct_decode() FROM DUAL").unwrap_err();
        assert!(
            error.to_string().contains("inside a SQL statement only"),
            "{error}"
        );
        let old = format!("CREATE FUNCTION old_decode() RETURNS NUMERIC LANGUAGE {PREVIEW_ROUTINE_LANGUAGE_V8} AS $$DECLARE v NUMERIC; BEGIN SELECT DECODE(1,1,7,0) INTO v; RETURN v; END$$");
        session.execute(&old).unwrap();
        assert!(session.execute("SELECT old_decode()").is_err());
    }

    #[test]
    fn decode_bind_types_are_resolved_after_parse_oids() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let sql = lower_session_sql(
            &db,
            "SELECT DECODE($1,$2,'yes','no') FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
            None,
            "bicdb",
        )
        .unwrap();
        assert!(!sql.contains("bicdb_oracle_decode_key"));
        let numeric =
            lower_typed_native_sql(&db, &sql, ORACLE_PREVIEW_PROFILE, &[25, 1700]).unwrap();
        assert!(numeric.contains("'number'"), "{numeric}");
        let text = lower_typed_native_sql(&db, &sql, ORACLE_PREVIEW_PROFILE, &[1700, 25]).unwrap();
        assert!(!text.contains("'number', 'number'"), "{text}");
    }

    #[test]
    fn decode_uses_query_and_update_column_types_before_shadowed_formals() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            "CREATE TABLE decode_shadow(p_value VARCHAR2(20),label VARCHAR2(20))",
            "INSERT INTO decode_shadow VALUES('001','before')",
            "CREATE FUNCTION decode_shadow_update(p_value NUMBER) RETURN VARCHAR2 IS v VARCHAR2(20); BEGIN UPDATE decode_shadow SET label=DECODE(1,p_value,'hit','miss'); SELECT label INTO v FROM decode_shadow; RETURN v; END;",
        ] {
            execute(&mut session, source).unwrap();
        }
        assert_eq!(
            execute(
                &mut session,
                "SELECT DECODE(1,p_value,'hit','miss') FROM decode_shadow"
            )
            .unwrap()
            .rows[0][0]
                .to_cell(),
            "miss"
        );
        assert_eq!(
            execute(&mut session, "SELECT decode_shadow_update(1) FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "miss"
        );
        execute(
            &mut session,
            "UPDATE decode_shadow SET label=DECODE(1,p_value,'hit','miss')",
        )
        .unwrap();
        assert_eq!(
            execute(&mut session, "SELECT label FROM decode_shadow")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "miss"
        );
    }

    #[test]
    fn compiler_upgrade_preserves_every_previous_language_catalog_identity() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for version in 1..=10 {
            let name = format!("bicdb_oracle_preview_v{version}");
            let oid = 999399 + version;
            assert_eq!(pg_language_oid(&name), oid);
            session.execute(&format!("CREATE FUNCTION reader_{version}() RETURNS NUMERIC LANGUAGE {name} AS $$BEGIN RETURN 7; END$$")).unwrap();
            let result = session.execute(&format!("SELECT l.lanname,l.oid FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang WHERE p.proname='reader_{version}'")).unwrap();
            assert_eq!(result.rows.len(), 1, "version {version}");
            assert_eq!(result.rows[0][0].to_cell(), name);
            assert_eq!(result.rows[0][1].to_cell(), oid.to_string());
        }
    }
}
