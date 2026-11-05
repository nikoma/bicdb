//! Eager three-argument selection with the existing typed NVL conversions.
use super::routine_arithmetic::Kind;
use super::*;

fn builtin_name(value: &str, quoted: bool) -> bool {
    if quoted {
        value == "NVL2"
    } else {
        value.eq_ignore_ascii_case("nvl2")
    }
}

pub(super) fn is_call(function: &sqlparser::ast::Function) -> bool {
    matches!(function.name.0.as_slice(), [part] if part.as_ident().is_some_and(|name|
        builtin_name(&name.value, name.quote_style.is_some())))
}

pub(super) fn is_word(token: &Token) -> bool {
    matches!(token, Token::Word(word) if builtin_name(&word.value, word.quote_style.is_some()))
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
    if !is_call(function) {
        return Ok(());
    }
    let FunctionArguments::List(list) = &function.args else {
        return Err(unsupported("NVL2 requires three scalar arguments"));
    };
    if list.args.len() != 3
        || !list
            .args
            .iter()
            .all(|arg| matches!(arg, FunctionArg::Unnamed(FunctionArgExpr::Expr(_))))
        || list.duplicate_treatment.is_some()
        || !list.clauses.is_empty()
        || function.over.is_some()
        || function.filter.is_some()
        || !function.within_group.is_empty()
        || function.null_treatment.is_some()
        || !matches!(&function.parameters, FunctionArguments::None)
        || function.uses_odbc_syntax
    {
        return Err(SqlError::InvalidSql(
            "NVL2 requires three scalar arguments".into(),
        ));
    }
    let mut condition = function_args(function).remove(0);
    let mut branches = function.clone();
    branches.name = ObjectName::from(vec![Ident::new("nvl")]);
    let FunctionArguments::List(arguments) = &mut branches.args else {
        unreachable!()
    };
    arguments.args.remove(0);
    let mut branches = Expr::Function(branches);
    // Result types and conversions depend on the second and third arguments,
    // independently of the type/value of the null-tested first argument.
    super::nvl::lower_call(&mut branches, kind, sql_context, defer_unknown)?;
    let Expr::Function(mut lowered) = branches else {
        unreachable!()
    };
    let name = object_name_lowercase(&lowered.name)?;
    let Some(target) = name.strip_prefix("bicdb_oracle_nvl_") else {
        // Parse must not freeze unresolved result-argument bind types. Bind
        // inference will revisit the original three-argument expression.
        return Ok(());
    };
    lowered.name = ObjectName::from(vec![Ident::new(format!("bicdb_oracle_nvl2_{target}"))]);
    super::decode::physical_dates(&mut condition)?;
    let FunctionArguments::List(arguments) = &mut lowered.args else {
        unreachable!()
    };
    arguments
        .args
        .insert(0, FunctionArg::Unnamed(FunctionArgExpr::Expr(condition)));
    *expr = Expr::Function(lowered);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
        let lowered = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        SqlSession::new(db).execute(&lowered)
    }

    fn preview(path: &std::path::Path) -> BicDb {
        let mut db =
            BicDb::open_with_config(path, bicdb_core::DbConfig::default().with_fsync(false))
                .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        db
    }

    #[test]
    fn nvl2_is_eager_and_uses_result_argument_types() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = preview(dir.path());
        for (sql, value, pg_type) in [
            ("SELECT NVL2(0,7,8) FROM DUAL", SqlValue::Int(7), "numeric"),
            (
                "SELECT NVL2(NULL,NULL,8) FROM DUAL",
                SqlValue::Int(8),
                "numeric",
            ),
            ("SELECT NVL2(1,NULL,8) FROM DUAL", SqlValue::Null, "numeric"),
            (
                "SELECT NVL2(NULL,'',42) FROM DUAL",
                SqlValue::String("42".into()),
                "text",
            ),
            (
                "SELECT NVL2(NULL,'x',42) FROM DUAL",
                SqlValue::String("42".into()),
                "text",
            ),
        ] {
            let result = run(&mut db, sql).unwrap();
            assert!(
                values_not_distinct(&result.rows[0][0], &value),
                "{sql}: {:?}",
                result.rows
            );
            assert_eq!(result.column_types[0].as_deref(), Some(pg_type), "{sql}");
        }
        for sql in [
            "SELECT NVL2(1,7,1/0) FROM DUAL",
            "SELECT NVL2(NULL,1/0,7) FROM DUAL",
        ] {
            assert_eq!(run(&mut db, sql).unwrap_err().sqlstate(), "22012");
        }
        assert_eq!(
            run(&mut db, "SELECT NVL2(1,7,'bad') FROM DUAL")
                .unwrap_err()
                .sqlstate(),
            "22P02"
        );
        assert!(run(&mut db, "SELECT NVL2(1,2) FROM DUAL").is_err());
        assert_eq!(
            run(&mut db, r#"SELECT "NVL2"(NULL,'',42) FROM DUAL"#)
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("42".into())]]
        );
        for sql in [
            "NVL2(1,2,3) WITHIN GROUP (ORDER BY 1)",
            "NVL2(1,2,3) FILTER (WHERE TRUE)",
            "NVL2(1,2,3) OVER ()",
        ] {
            let mut expr = expression(sql).unwrap();
            assert!(
                lower_call(&mut expr, |_| Kind::Number, true, false).is_err(),
                "{sql}"
            );
        }
    }

    #[test]
    fn nvl2_keeps_row_scopes_and_native_case_independent() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = preview(dir.path());
        run(&mut db,"CREATE TABLE selection_values(id INTEGER PRIMARY KEY,flag INTEGER,amount NUMBER,label VARCHAR2(12))").unwrap();
        run(
            &mut db,
            "INSERT INTO selection_values VALUES(1,1,7,'bad'),(2,NULL,NULL,'8')",
        )
        .unwrap();
        let result=run(&mut db,"WITH local_rows AS (SELECT * FROM selection_values) SELECT NVL2(flag,amount,8) FROM local_rows ORDER BY id").unwrap();
        assert_eq!(
            result
                .rows
                .iter()
                .map(|r| r[0].to_cell())
                .collect::<Vec<_>>(),
            vec!["7", "8"]
        );
        assert_eq!(
            run(
                &mut db,
                "SELECT NVL2(flag,7,label) FROM selection_values WHERE id=1"
            )
            .unwrap_err()
            .sqlstate(),
            "22P02"
        );
        SqlSession::new(&mut db).execute(r#"CREATE FUNCTION "nvl2"(a INTEGER,b INTEGER,c INTEGER) RETURNS INTEGER LANGUAGE sql AS $$SELECT 99$$"#).unwrap();
        assert_eq!(
            run(&mut db, r#"SELECT "nvl2"(1,2,3) FROM DUAL"#)
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "99"
        );
        assert_eq!(
            run(&mut db, r#"SELECT "NVL2"(1,2,3) FROM DUAL"#)
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "2"
        );
        let native = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(native.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE FUNCTION nvl2(a INTEGER,b INTEGER,c INTEGER) RETURNS INTEGER LANGUAGE sql AS $$SELECT a+b+c$$").unwrap();
        assert_eq!(
            session.execute("SELECT nvl2(2,3,4)").unwrap().rows[0][0].to_cell(),
            "9"
        );
        assert_eq!(
            session
                .execute("SELECT CASE WHEN 1 IS NOT NULL THEN 7 ELSE 1/0 END")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "7"
        );
    }

    #[test]
    fn nvl2_routine_capability_is_versioned_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = preview(dir.path());
            run(
                &mut db,
                "CREATE FUNCTION typed_selection RETURN NUMBER IS BEGIN RETURN NVL2(1,2,3); END;",
            )
            .unwrap();
            let old=lower_session_sql(&db,"CREATE FUNCTION previous_selection RETURN NUMBER IS BEGIN RETURN NVL2(1,2,3); END;",ORACLE_PREVIEW_PROFILE,None,"bicdb").unwrap().replace(PREVIEW_ROUTINE_LANGUAGE,PREVIEW_ROUTINE_LANGUAGE_V26);
            SqlSession::new(&mut db).execute(&old).unwrap();
            assert_eq!(
                run(&mut db, "SELECT typed_selection() FROM DUAL")
                    .unwrap()
                    .rows[0][0]
                    .to_cell(),
                "2"
            );
            assert!(run(&mut db, "SELECT previous_selection() FROM DUAL").is_err());
        }
        let mut db = preview(dir.path());
        assert_eq!(
            preview_routine_language_version(PREVIEW_ROUTINE_LANGUAGE_V26),
            Some(26)
        );
        assert_eq!(
            preview_routine_language_version(PREVIEW_ROUTINE_LANGUAGE),
            Some(29)
        );
        let languages = SqlSession::new(&mut db)
            .execute(
                r#"SELECT "lanname","oid" FROM "pg_catalog"."pg_language"
               WHERE "lanname" IN ('bicdb_oracle_preview_v26','bicdb_oracle_preview_v29')
               ORDER BY "oid""#,
            )
            .unwrap();
        assert_eq!(
            languages.rows,
            vec![
                vec![
                    SqlValue::String(PREVIEW_ROUTINE_LANGUAGE_V26.into()),
                    SqlValue::Int(PREVIEW_ROUTINE_LANGUAGE_V26_OID)
                ],
                vec![
                    SqlValue::String(PREVIEW_ROUTINE_LANGUAGE.into()),
                    SqlValue::Int(PREVIEW_ROUTINE_LANGUAGE_OID)
                ],
            ]
        );

        assert_eq!(
            run(&mut db, "SELECT typed_selection() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "2"
        );
        assert!(run(&mut db, "SELECT previous_selection() FROM DUAL").is_err());
    }
}
