//! Versioned Oracle application errors and their PostgreSQL transport metadata.
use super::*;
mod diagnostics;
pub(crate) use diagnostics::{
    contains_diagnostics, current_diagnostic, prepare_diagnostics, DiagnosticScope, Diagnostics,
};

pub(crate) fn truncate_bytes(value: &str, limit: usize) -> &str {
    let mut end = value.len().min(limit);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Builtin {
    RaiseApplicationError,
}

fn error(
    code: i32,
    state: &'static str,
    text: impl Into<String>,
    previous: Option<Arc<SqlError>>,
) -> SqlError {
    // Oracle preserves leading text, tabs and CR, but removes trailing SP/LF.
    // Do this before adding the mandatory space after the error-number colon.
    let text = text.into();
    let text = text.trim_end_matches([' ', '\n']);
    SqlError::OracleError {
        code,
        sqlstate: state,
        message: truncate_bytes(&format!("ORA-{:05}: {}", code.unsigned_abs(), text), 2045)
            .to_string(),
        previous,
        message_valid: Arc::new(std::sync::atomic::AtomicBool::new(true)),
    }
}

pub(crate) fn transport_detail(error: &SqlError) -> String {
    let mut stack = Vec::new();
    let mut current = Some(error);
    while let Some(error) = current {
        match error {
            SqlError::OracleError {
                code,
                message,
                previous,
                ..
            } => {
                stack.push(serde_json::json!({"code":code,"message":message}));
                current = previous.as_deref();
            }
            SqlError::Rethrown(inner) => current = Some(inner),
            SqlError::OracleContext { inner, .. } => current = Some(inner),
            other => {
                stack.push(
                    serde_json::json!({"sqlstate":other.sqlstate(),"message":other.to_string()}),
                );
                break;
            }
        }
    }
    serde_json::json!({"oracle_error_stack":stack}).to_string()
}

pub(crate) fn raise_application_error(
    values: &[SqlValue],
    previous: Option<Arc<SqlError>>,
) -> Result<SqlError> {
    let [code, message, keep] = values else {
        return Err(unsupported(
            "RAISE_APPLICATION_ERROR requires normalized num, msg and keeperrorstack inputs",
        ));
    };
    let keep = match keep {
        SqlValue::Bool(value) => *value,
        SqlValue::Null => true,
        _ => {
            return Err(unsupported(
                "RAISE_APPLICATION_ERROR keeperrorstack requires a BOOLEAN",
            ))
        }
    };
    let code = if matches!(code, SqlValue::Null) {
        0
    } else {
        if matches!(
            code,
            SqlValue::Bool(_)
                | SqlValue::Composite(_)
                | SqlValue::Geometry(_)
                | SqlValue::TsQuery(_)
        ) {
            return Err(unsupported(
                "RAISE_APPLICATION_ERROR num requires NUMBER or numeric character input",
            ));
        }
        let number = cast_value_to_pg_type(code.clone(), "numeric").map_err(|_| {
            error(
                -6502,
                "22P02",
                "PL/SQL: value or conversion error: character to number conversion error",
                None,
            )
        })?;
        let integer = cast_value_to_pg_type(number, "int4")
            .map_err(|_| error(-1426, "22003", "numeric overflow", None))?;
        sql_value_i64(&integer)
            .and_then(|v| i32::try_from(v).ok())
            .ok_or_else(|| error(-1426, "22003", "numeric overflow", None))?
    };
    if !(-20999..=-20000).contains(&code) {
        return Ok(error(
            -21000,
            "22023",
            format!("error number argument to raise_application_error of {code} is out of range"),
            keep.then_some(previous).flatten(),
        ));
    }
    let message = match message {
        SqlValue::Null => String::new(),
        SqlValue::String(value) => value.clone(),
        value
            if matches!(value, SqlValue::Int(_) | SqlValue::Float(_))
                || matches!(value, SqlValue::Json(json) if json.is_number()) =>
        {
            eval_pg_scalar_extra(
                "bicdb_oracle_default_to_char",
                &[
                    message.clone(),
                    SqlValue::String("number".into()),
                    SqlValue::String("plsql".into()),
                ],
            )?
            .expect("known builtin")
            .to_cell()
        }
        _ => {
            return Err(unsupported(
                "RAISE_APPLICATION_ERROR msg requires a character or NUMBER value",
            ))
        }
    };
    Ok(error(
        code,
        "P0001",
        message,
        keep.then_some(previous).flatten(),
    ))
}

pub(crate) fn error_token(error: &SqlError) -> Option<Arc<std::sync::atomic::AtomicBool>> {
    match error {
        SqlError::OracleError { message_valid, .. } => Some(message_valid.clone()),
        SqlError::OracleContext { valid, .. } => Some(valid.clone()),
        SqlError::Rethrown(inner) => error_token(inner),
        _ => None,
    }
}

pub(crate) fn is_cleared(error: &SqlError) -> bool {
    error_token(error).is_some_and(|t| !t.load(std::sync::atomic::Ordering::Relaxed))
}

fn remember(session: &mut SqlSession<'_>, token: &Arc<std::sync::atomic::AtomicBool>) {
    session
        .oracle_error_tokens
        .retain(|w| w.strong_count() != 0);
    if !session
        .oracle_error_tokens
        .iter()
        .any(|w| w.as_ptr() == Arc::as_ptr(token))
    {
        session.oracle_error_tokens.push(Arc::downgrade(token));
    }
}

pub(crate) fn capture(session: &mut SqlSession<'_>, error: SqlError) -> SqlError {
    let error = if error_token(&error).is_none() {
        SqlError::OracleContext {
            inner: Arc::new(error),
            valid: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    } else {
        error
    };
    remember(session, &error_token(&error).expect("Oracle error context"));
    error
}

pub(crate) fn invoke(session: &mut SqlSession<'_>, values: &[SqlValue]) -> SqlError {
    if session.oracle_exception.is_none() {
        session.oracle_error_tokens.clear();
    }
    let previous = session.oracle_exception.clone().filter(|e| !is_cleared(e));
    let result = raise_application_error(values, previous);
    // Conversion to the declared inputs happens before the procedure runs.
    // A successfully bound call may replace saved errors even for a bad range.
    if result.is_ok() && matches!(values.get(2), Some(SqlValue::Bool(false))) {
        for token in session
            .oracle_error_tokens
            .drain(..)
            .filter_map(|w| w.upgrade())
        {
            token.store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }
    capture(
        session,
        match result {
            Ok(error) | Err(error) => error,
        },
    )
}

pub(crate) fn reraise(session: &mut SqlSession<'_>, original: Arc<SqlError>) -> SqlError {
    if is_cleared(&original) {
        capture(
            session,
            error(
                -21002,
                "P0001",
                "attempted to reraise a cleared error",
                None,
            ),
        )
    } else {
        SqlError::Rethrown(original)
    }
}

fn arguments(function: &Function) -> Result<Vec<RoutineExpr>> {
    let params = parse_routine_params(&[
        "num numeric".into(),
        "msg text".into(),
        "keeperrorstack boolean DEFAULT false".into(),
    ])?;
    let actuals = procedure_actual_arguments(function, &params, false)?;
    actuals
        .into_iter()
        .zip(params)
        .map(|(actual, param)| {
            actual
                .cloned()
                .or(param.default_expr)
                .map(RoutineExpr::unbound)
                .ok_or_else(|| unsupported("missing application error argument"))
        })
        .collect()
}

fn is_application_error(function: &Function) -> bool {
    let names = function
        .name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|id| {
            if id.quote_style.is_some() {
                id.value.clone()
            } else {
                id.value.to_uppercase()
            }
        })
        .collect::<Vec<_>>();
    match names
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["RAISE_APPLICATION_ERROR"] => true,
        ["DBMS_STANDARD", "RAISE_APPLICATION_ERROR"]
        | ["SYS", "DBMS_STANDARD", "RAISE_APPLICATION_ERROR"] => true,
        _ => false,
    }
}

pub(crate) fn lower(
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) -> Result<()> {
    for statement in statements {
        match statement {
            RoutineStmt::Sql(Statement::Call(function)) if is_application_error(function) => {
                *statement = RoutineStmt::OracleBuiltin {
                    builtin: Builtin::RaiseApplicationError,
                    arguments: arguments(function)?,
                };
            }
            RoutineStmt::Block { body, handlers, .. } => lower(body, handlers)?,
            RoutineStmt::If {
                then_body,
                else_body,
                ..
            } => {
                lower(then_body, &mut [])?;
                lower(else_body, &mut [])?;
            }
            RoutineStmt::Case {
                arms, else_body, ..
            } => {
                for body in arms {
                    lower(body, &mut [])?;
                }
                if let Some(body) = else_body {
                    lower(body, &mut [])?;
                }
            }
            RoutineStmt::ForLoop { body, .. }
            | RoutineStmt::ForeachLoop { body, .. }
            | RoutineStmt::QueryForLoop { body, .. }
            | RoutineStmt::CursorForLoop { body, .. }
            | RoutineStmt::Loop { body }
            | RoutineStmt::WhileLoop { body, .. } => lower(body, &mut [])?,
            _ => {}
        }
    }
    for handler in handlers {
        lower(&mut handler.statements, &mut [])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(session: &mut SqlSession<'_>, sql: &str) -> Result<SqlResult> {
        let sql = lower_session_sql(session.db_ref(), sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        session.execute(&sql)
    }
    #[test]
    fn application_error_trims_only_trailing_spaces_and_linefeeds() {
        for (input, expected) in [
            ("tail \n ", "tail"),
            ("  head  ", "  head"),
            (" ", ""),
            ("\thello\t", "\thello\t"),
            ("x\r\n", "x\r"),
            ("x\n\t", "x\n\t"),
            ("\nx", "\nx"),
            ("x\0", "x\0"),
        ] {
            let err = error(-20100, "P0001", input, None);
            assert_eq!(err.to_string(), format!("ORA-20100: {expected}"));
            let _scope = DiagnosticScope::new(Some(Arc::new(err)));
            assert_eq!(
                current_diagnostic("bicdb_oracle_current_sqlerrm")
                    .unwrap()
                    .to_cell(),
                format!("ORA-20100: {expected}"),
                "{input:?}"
            );
        }
    }

    #[test]
    fn application_error_codes_round_validate_and_preserve_stack_metadata() {
        for (input, expected) in [
            ("-20000", -20000),
            ("-20999", -20999),
            ("-20000.5", -20001),
            ("-19999.5", -20000),
            ("-19999", -21000),
            ("0", -21000),
        ] {
            let err = raise_application_error(
                &[
                    SqlValue::String(input.into()),
                    SqlValue::String("message".into()),
                    SqlValue::Bool(false),
                ],
                None,
            )
            .unwrap();
            let SqlError::OracleError { code, .. } = err else {
                panic!()
            };
            assert_eq!(code, expected, "{input}");
        }
        let inner = Arc::new(error(-20101, "P0001", "inner", None));
        for keep in [SqlValue::Bool(true), SqlValue::Null] {
            let err = raise_application_error(
                &[
                    SqlValue::Int(-20102),
                    SqlValue::String("outer".into()),
                    keep,
                ],
                Some(inner.clone()),
            )
            .unwrap();
            let detail = transport_detail(&err);
            let detail: JsonValue = serde_json::from_str(&detail).unwrap();
            assert_eq!(detail["oracle_error_stack"].as_array().unwrap().len(), 2);
        }
    }

    #[test]
    fn application_error_stored_calls_handlers_named_arguments_and_rethrow_execute() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE PROCEDURE err_leaf(p_code NUMBER,p_msg VARCHAR2) IS BEGIN RAISE_APPLICATION_ERROR(p_code,p_msg); END;",
            "CREATE FUNCTION err_handled RETURN NUMBER IS BEGIN err_leaf(-20100,'handled');RETURN 0;EXCEPTION WHEN OTHERS THEN RETURN 42;END;",
            "CREATE PROCEDURE err_stack IS BEGIN err_leaf(-20101,'inner');EXCEPTION WHEN OTHERS THEN RAISE_APPLICATION_ERROR(msg=>'outer',num=>-20102,keeperrorstack=>TRUE);END;",
            "CREATE PROCEDURE err_rethrow IS BEGIN err_leaf(-20103,'again');EXCEPTION WHEN OTHERS THEN RAISE;END;",
            "CREATE PROCEDURE err_number_message IS v NUMBER(10,3):=12.500;BEGIN RAISE_APPLICATION_ERROR(-20107,v);END;",
            "CREATE PROCEDURE err_date_message IS v DATE:=DATE '2024-02-29';BEGIN RAISE_APPLICATION_ERROR(-20108,v);END;",
        ] {run(&mut session,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        let error = run(&mut session, "CALL err_leaf(-20100,'original')").unwrap_err();
        assert_eq!(error.sqlstate(), "P0001");
        assert_eq!(error.to_string(), "ORA-20100: original");
        assert_eq!(
            run(&mut session, "SELECT err_handled() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "42"
        );
        let error = run(&mut session, "CALL err_stack()").unwrap_err();
        assert!(transport_detail(&error).contains("inner"));
        assert_eq!(error.to_string(), "ORA-20102: outer");
        let error = run(&mut session, "CALL err_rethrow()").unwrap_err();
        assert_eq!(error.sqlstate(), "P0001");
        assert_eq!(error.to_string(), "ORA-20103: again");
        assert_eq!(
            run(&mut session, "CALL err_number_message()")
                .unwrap_err()
                .to_string(),
            "ORA-20107: 12.5"
        );
        assert_eq!(
            run(&mut session, "CALL err_date_message()")
                .unwrap_err()
                .to_string(),
            "ORA-20108: 29-FEB-24"
        );
        assert!(session.oracle_exception.is_none());
        assert_eq!(
            run(&mut session, "SELECT 1 FROM DUAL").unwrap().rows[0][0].to_cell(),
            "1"
        );
    }

    #[test]
    fn application_error_native_rethrow_retains_state_message_and_detail() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE FUNCTION native_again() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'native original' USING ERRCODE='22012',DETAIL='native detail';EXCEPTION WHEN OTHERS THEN RAISE;END$$").unwrap();
        let error = session.execute("SELECT native_again()").unwrap_err();
        assert_eq!(error.sqlstate(), "22012");
        assert_eq!(error.to_string(), "native original");
        assert_eq!(
            error.fields(),
            vec![SqlErrorField::Detail("native detail".into())]
        );
        session.execute("CREATE FUNCTION native_outside() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RAISE;END$$").unwrap();
        assert_eq!(
            session
                .execute("SELECT native_outside()")
                .unwrap_err()
                .sqlstate(),
            "0Z002"
        );
    }

    #[test]
    fn application_error_diagnostics_follow_nested_handlers_calls_and_variable_scopes() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE FUNCTION err_status RETURN VARCHAR2 IS BEGIN RETURN TO_CHAR(SQLCODE)||'|'||SQLERRM;END;",
            "CREATE FUNCTION err_inherit RETURN VARCHAR2 IS BEGIN RAISE_APPLICATION_ERROR(-20100,'handled');EXCEPTION WHEN OTHERS THEN RETURN err_status();END;",
            "CREATE FUNCTION err_restore RETURN VARCHAR2 IS text_value VARCHAR2(100);BEGIN BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN BEGIN RAISE_APPLICATION_ERROR(-20101,'inner');EXCEPTION WHEN OTHERS THEN text_value:=TO_CHAR(SQLCODE)||'|'||SQLERRM;END;RETURN text_value||'/'||TO_CHAR(SQLCODE)||'|'||SQLERRM;END;END;",
            "CREATE FUNCTION err_shadow RETURN NUMBER IS SQLCODE NUMBER:=42;BEGIN RETURN SQLCODE;END;",
            "CREATE FUNCTION err_shadow_lower RETURN VARCHAR2 IS \"sqlcode\" NUMBER:=99;BEGIN RETURN TO_CHAR(SQLCODE);END;",
            "CREATE FUNCTION err_collision RETURN VARCHAR2 IS \"__bicdb_oracle_sqlcode_0\" NUMBER:=99;BEGIN RETURN TO_CHAR(SQLCODE)||'|'||SQLERRM;END;",
            "CREATE FUNCTION err_init RETURN VARCHAR2 IS v NUMBER:=SQLCODE;BEGIN RETURN TO_CHAR(v)||'|'||SQLERRM;END;",
            "CREATE FUNCTION err_inherit_init RETURN VARCHAR2 IS BEGIN RAISE_APPLICATION_ERROR(-20105,'init');EXCEPTION WHEN OTHERS THEN RETURN err_init();END;",
            "CREATE FUNCTION err_clear_child RETURN VARCHAR2 IS BEGIN RAISE_APPLICATION_ERROR(-20101,'inner');EXCEPTION WHEN OTHERS THEN RETURN 'done';END;",
            "CREATE FUNCTION err_dynamic RETURN VARCHAR2 IS BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN RETURN SQLERRM||'/'||err_clear_child()||'/'||SQLERRM;END;",
            "CREATE FUNCTION err_default_code(p NUMBER DEFAULT SQLCODE) RETURN NUMBER IS BEGIN RETURN p;END;",
            "CREATE FUNCTION err_default_call RETURN VARCHAR2 IS BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN RETURN TO_CHAR(err_default_code());END;",
            "CREATE FUNCTION err_long RETURN VARCHAR2 IS BEGIN RAISE_APPLICATION_ERROR(-20106,RPAD('x',3000,'x'));EXCEPTION WHEN OTHERS THEN RETURN SQLERRM;END;",
        ] {run(&mut session,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        for (sql, expected) in [
            (
                "SELECT err_status() FROM DUAL",
                "0|ORA-0000: normal, successful completion",
            ),
            (
                "SELECT err_inherit() FROM DUAL",
                "-20100|ORA-20100: handled",
            ),
            (
                "SELECT err_status() FROM DUAL",
                "0|ORA-0000: normal, successful completion",
            ),
            (
                "SELECT err_restore() FROM DUAL",
                "-20101|ORA-20101: inner/-20100|ORA-20100: ",
            ),
            ("SELECT err_shadow() FROM DUAL", "42"),
            ("SELECT err_shadow_lower() FROM DUAL", "0"),
            (
                "SELECT err_collision() FROM DUAL",
                "0|ORA-0000: normal, successful completion",
            ),
            (
                "SELECT err_inherit_init() FROM DUAL",
                "-20105|ORA-20105: init",
            ),
            (
                "SELECT err_dynamic() FROM DUAL",
                "ORA-20100: outer/done/ORA-20100: ",
            ),
            ("SELECT err_default_call() FROM DUAL", "-20100"),
        ] {
            assert_eq!(
                run(&mut session, sql)
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    .rows[0][0]
                    .to_cell(),
                expected,
                "{sql}"
            );
        }
        let text = run(&mut session, "SELECT err_long() FROM DUAL")
            .unwrap()
            .rows[0][0]
            .to_cell();
        assert_eq!(text.len(), 512);
        assert!(text.starts_with("ORA-20106: "));
        assert!(session.oracle_exception.is_none());
    }

    #[test]
    fn application_error_standard_names_precede_schema_routines_and_preserve_native_calls() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE PROCEDURE RAISE_APPLICATION_ERROR(num NUMBER,msg VARCHAR2) IS BEGIN NULL;END;",
            "CREATE FUNCTION SQLCODE RETURN NUMBER IS BEGIN RETURN 43;END;",
            "CREATE FUNCTION name_app RETURN NUMBER IS BEGIN RAISE_APPLICATION_ERROR(3,'local');RETURN 42;EXCEPTION WHEN OTHERS THEN RETURN SQLCODE;END;",
            "CREATE FUNCTION name_code RETURN NUMBER IS BEGIN RETURN SQLCODE();END;",
        ] {run(&mut session,sql).unwrap();}
        assert_eq!(
            run(&mut session, "SELECT name_app() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "-21000"
        );
        assert_eq!(
            run(&mut session, "SELECT name_code() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "0"
        );
        assert_eq!(
            session
                .execute(r#"SELECT public."SQLCODE"()"#)
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "43"
        );
    }

    #[test]
    fn application_error_cleared_contexts_reject_rethrow_and_drop_cleared_causes() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for statement in [
            "RAISE_APPLICATION_ERROR(-20100,'outer');",
            "v:=1/0;",
            "SELECT 1 INTO v FROM DUAL WHERE 1=0;",
        ] {
            let sql=format!("CREATE OR REPLACE PROCEDURE cleared_reraise IS v NUMBER;BEGIN BEGIN {statement}EXCEPTION WHEN OTHERS THEN BEGIN RAISE_APPLICATION_ERROR(-20101,'inner');EXCEPTION WHEN OTHERS THEN NULL;END;RAISE;END;END;");
            run(&mut session, &sql).unwrap();
            let error = run(&mut session, "CALL cleared_reraise()").unwrap_err();
            assert_eq!(
                error.to_string(),
                "ORA-21002: attempted to reraise a cleared error"
            );
        }
        run(&mut session,"CREATE PROCEDURE cleared_keep IS BEGIN BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN BEGIN RAISE_APPLICATION_ERROR(-20101,'inner');EXCEPTION WHEN OTHERS THEN NULL;END;RAISE_APPLICATION_ERROR(-20102,'new',TRUE);END;END;").unwrap();
        let error = run(&mut session, "CALL cleared_keep()").unwrap_err();
        let detail: JsonValue = serde_json::from_str(&transport_detail(&error)).unwrap();
        assert_eq!(detail["oracle_error_stack"].as_array().unwrap().len(), 1);
        assert_eq!(detail["oracle_error_stack"][0]["code"], -20102);
    }

    #[test]
    fn application_error_sql_and_pl_calls_have_distinct_diagnostic_entries() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE FUNCTION boundary_status RETURN VARCHAR2 IS BEGIN RETURN TO_CHAR(SQLCODE)||'|'||SQLERRM;END;",
            "CREATE FUNCTION boundary_clear RETURN VARCHAR2 IS BEGIN RAISE_APPLICATION_ERROR(-20101,'inner');EXCEPTION WHEN OTHERS THEN RETURN 'done';END;",
            "CREATE FUNCTION boundary_query RETURN VARCHAR2 IS v VARCHAR2(200);BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN SELECT boundary_status() INTO v FROM DUAL;RETURN v||'/'||TO_CHAR(SQLCODE)||'|'||SQLERRM;END;",
            "CREATE FUNCTION boundary_query_clear RETURN VARCHAR2 IS v VARCHAR2(200);BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN SELECT boundary_clear() INTO v FROM DUAL;RETURN v||'/'||TO_CHAR(SQLCODE)||'|'||SQLERRM;END;",
            "CREATE PROCEDURE boundary_receive(p VARCHAR2,r OUT VARCHAR2) IS BEGIN r:=p;END;",
            "CREATE FUNCTION boundary_call_arg RETURN VARCHAR2 IS v VARCHAR2(200);BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN boundary_receive(SQLERRM,v);RETURN v||'/'||TO_CHAR(SQLCODE)||'|'||SQLERRM;END;",
        ] {run(&mut session,sql).unwrap();}
        for (sql, expected) in [
            (
                "SELECT boundary_query() FROM DUAL",
                "0|ORA-0000: normal, successful completion/-20100|ORA-20100: outer",
            ),
            (
                "SELECT boundary_query_clear() FROM DUAL",
                "done/-20100|ORA-20100: outer",
            ),
            (
                "SELECT boundary_call_arg() FROM DUAL",
                "ORA-20100: outer/-20100|ORA-20100: outer",
            ),
        ] {
            assert_eq!(
                run(&mut session, sql)
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    .rows[0][0]
                    .to_cell(),
                expected,
                "{sql}"
            );
        }
    }
}
