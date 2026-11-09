//! Parse PostgreSQL-style procedure headers without treating their body as SQL.
//!
//! sqlparser's procedure AST describes BEGIN/END bodies and cannot represent
//! PostgreSQL's dollar-quoted CREATE OR REPLACE PROCEDURE. A function-shaped
//! header is used only for parsing/qualification; persistence and CALL retain
//! the procedure kind and its original body.
use super::*;

pub(crate) fn header(sql: &str) -> Result<Option<sqlparser::ast::CreateFunction>> {
    let trimmed = sql.trim_start();
    if !trimmed
        .get(..6)
        .is_some_and(|s| s.eq_ignore_ascii_case("create"))
    {
        return Ok(None);
    }
    let mut source = tokens(sql)?;
    let position = if source.get(1).is_some_and(|t| word(t, "PROCEDURE")) {
        1
    } else if source.get(1).is_some_and(|t| word(t, "OR"))
        && source.get(2).is_some_and(|t| word(t, "REPLACE"))
        && source.get(3).is_some_and(|t| word(t, "PROCEDURE"))
    {
        3
    } else {
        return Ok(None);
    };
    let mut end = position + 2;
    if !matches!(source.get(position + 1), Some(Token::Word(_))) {
        return Err(SqlError::InvalidSql(
            "procedure requires an identifier".into(),
        ));
    }
    while source.get(end) == Some(&Token::Period) {
        if !matches!(source.get(end + 1), Some(Token::Word(_))) {
            return Err(SqlError::InvalidSql("invalid procedure name".into()));
        }
        end += 2;
    }
    if source.get(end) != Some(&Token::LParen) {
        source.splice(end..end, [Token::LParen, Token::RParen]);
    }
    let mut depth = 0;
    let close = (end..source.len())
        .find(|&index| {
            match source[index] {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            depth == 0
        })
        .ok_or_else(|| SqlError::InvalidSql("unclosed procedure arguments".into()))?;
    // The raw procedure interpreter has always accepted modes after a name as
    // well as PostgreSQL's mode-first form. Normalize only the parsing proxy;
    // keep the stored declaration and its original parameter modes unchanged.
    let mut arguments = Vec::new();
    for (index, group) in split_top_level(&source[end + 1..close]).iter().enumerate() {
        if index > 0 {
            arguments.push(Token::Comma);
        }
        if group
            .get(1)
            .is_some_and(|token| ["IN", "OUT", "INOUT"].iter().any(|mode| word(token, mode)))
        {
            if word(&group[1], "IN") && group.get(2).is_some_and(|token| word(token, "OUT")) {
                arguments.extend(tokens("INOUT")?);
                arguments.push(group[0].clone());
                arguments.extend_from_slice(&group[3..]);
            } else {
                arguments.extend([group[1].clone(), group[0].clone()]);
                arguments.extend_from_slice(&group[2..]);
            }
        } else {
            arguments.extend_from_slice(group);
        }
    }
    let old_len = close - end - 1;
    let close = close + arguments.len() - old_len;
    source.splice(end + 1..end + 1 + old_len, arguments);
    source[position] = tokens("FUNCTION")?.remove(0);
    source.splice(close + 1..close + 1, tokens("RETURNS void")?);
    let mut statements = parse_statements(&join(&source))?;
    if statements.len() != 1 {
        return Err(SqlError::InvalidSql(
            "expected one procedure definition".into(),
        ));
    }
    let Statement::CreateFunction(function) = statements.remove(0) else {
        return Err(SqlError::InvalidSql("invalid procedure header".into()));
    };
    Ok(Some(function))
}

pub(super) fn render(mut function: sqlparser::ast::CreateFunction) -> Result<String> {
    function.return_type = None;
    let mut source = tokens(&render_sql(&Statement::CreateFunction(function)))?;
    let index = source
        .iter()
        .take(4)
        .position(|t| word(t, "FUNCTION"))
        .ok_or_else(|| SqlError::InvalidSql("invalid procedure header".into()))?;
    source[index] = tokens("PROCEDURE")?.remove(0);
    Ok(join(&source))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(session: &mut SqlSession<'_>, sql: &str, schema: &str) -> Result<SqlResult> {
        let lowered = lower_session_sql(
            session.db_ref(),
            sql,
            ORACLE_PREVIEW_PROFILE,
            Some(schema),
            "bicdb",
        )?;
        session.execute(&lowered)
    }

    #[test]
    fn procedures_execute_replace_bind_declared_dates_and_survive_reopen() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let path = "\"proc_app\"";
        {
            let mut session = SqlSession::new(&mut db);
            session.execute("CREATE SCHEMA proc_app; CREATE TABLE public.ledger(k INTEGER,v INTEGER); INSERT INTO public.ledger VALUES(1,-999)").unwrap();
            for sql in [
                "CREATE TABLE ledger(k NUMBER PRIMARY KEY,v NUMBER,d DATE)",
                "INSERT INTO ledger VALUES(1,0,NULL)",
                "CREATE OR REPLACE PROCEDURE bump(p_delta IN NUMBER,p_day IN DATE) IS BEGIN UPDATE ledger SET v=v+NVL(p_delta,0),d=p_day WHERE k=1; END bump;",
                "CALL bump(2,TIMESTAMP '2024-02-29 12:34:56.999999')",
            ] { run(&mut session,sql,path).unwrap(); }
            let result = run(&mut session, "SELECT v,d FROM ledger", path).unwrap();
            assert_eq!(result.rows[0][0].to_cell(), "2");
            assert_eq!(result.rows[0][1].to_cell(), "2024-02-29 12:34:56");
            run(&mut session,"CREATE OR REPLACE PROCEDURE bump(p_delta NUMBER,p_day DATE) IS BEGIN UPDATE ledger SET v=v+2*p_delta,d=p_day WHERE k=1; END;",path).unwrap();
            run(&mut session,r#"CREATE PROCEDURE parent(p_delta NUMBER) IS BEGIN IF p_delta>=0 THEN "proc_app".bump(p_delta,DATE '2024-03-01'); ELSE "proc_app".bump(0,DATE '2024-03-01'); END IF; END;"#,path).unwrap();
            run(&mut session, "CALL parent(3)", path).unwrap();
            assert_eq!(
                session.execute("SELECT v FROM public.ledger").unwrap().rows[0][0].to_cell(),
                "-999"
            );
        }
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        run(&mut session, "CALL parent(1)", path).unwrap();
        assert_eq!(
            run(&mut session, "SELECT v FROM ledger", path)
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "10"
        );
    }

    #[test]
    fn procedure_outputs_quoted_headers_defaults_and_parameterless_definitions() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE SCHEMA \"Quoted.Space\"").unwrap();
        run(&mut session,"CREATE PROCEDURE \"Output Value\"(p_value IN NUMBER,p_result OUT NUMBER,p_total IN OUT NUMBER) IS BEGIN p_result:=2*p_value;p_total:=NVL(p_total,0)+p_result; END;","\"Quoted.Space\"").unwrap();
        let output = run(
            &mut session,
            "CALL \"Output Value\"(3,NULL,4)",
            "\"Quoted.Space\"",
        )
        .unwrap();
        assert_eq!(
            output.rows[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["6", "10"]
        );
        assert_eq!(
            output.column_types,
            vec![Some("numeric".into()), Some("numeric".into())]
        );
        run(&mut session,"CREATE FUNCTION output_caller RETURN NUMBER IS v NUMBER; total NUMBER:=4; BEGIN \"Output Value\"(3,v,total); RETURN v+100*total; END;","\"Quoted.Space\"").unwrap();
        assert_eq!(
            run(
                &mut session,
                "SELECT output_caller() FROM DUAL",
                "\"Quoted.Space\""
            )
            .unwrap()
            .rows[0][0]
                .to_cell(),
            "1006"
        );
        run(
            &mut session,
            "CREATE TABLE events(v NUMBER)",
            "\"Quoted.Space\"",
        )
        .unwrap();
        run(
            &mut session,
            "CREATE PROCEDURE no_arguments IS BEGIN INSERT INTO events VALUES(7); END;",
            "\"Quoted.Space\"",
        )
        .unwrap();
        run(&mut session,"CREATE PROCEDURE optional_value(p_value NUMBER DEFAULT CAST(4 AS NUMBER)) IS BEGIN INSERT INTO events VALUES(p_value); END;","\"Quoted.Space\"").unwrap();
        run(&mut session, "CALL no_arguments()", "\"Quoted.Space\"").unwrap();
        run(&mut session, "CALL optional_value()", "\"Quoted.Space\"").unwrap();
        let result = run(
            &mut session,
            "SELECT v FROM events ORDER BY v",
            "\"Quoted.Space\"",
        )
        .unwrap();
        assert_eq!(
            result
                .rows
                .iter()
                .map(|r| r[0].to_cell())
                .collect::<Vec<_>>(),
            vec!["4", "7"]
        );
    }

    #[test]
    fn procedure_header_boundary_and_previous_language_fences_are_preserved() {
        assert!(header("SELECT 'CREATE PROCEDURE'").unwrap().is_none());
        assert!(
            header("CREATE OR REPLACE FUNCTION f() RETURNS void LANGUAGE sql AS $$SELECT 1$$")
                .unwrap()
                .is_none()
        );
        assert!(header(
            "CREATE PROCEDURE p() LANGUAGE plpgsql AS $$BEGIN RETURN; END$$; DROP TABLE other"
        )
        .is_err());
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        assert_eq!(session.execute("CREATE PROCEDURE old_proc() LANGUAGE bicdb_oracle_preview_v11 AS $$BEGIN RETURN; END$$").unwrap_err().sqlstate(),"0A000");
        session.execute("CREATE FUNCTION old_fn() RETURNS INTEGER LANGUAGE bicdb_oracle_preview_v11 AS $$BEGIN RETURN coalesce(1,1/0); END$$").unwrap();
        assert_eq!(
            run(&mut session, "SELECT old_fn() FROM DUAL", "public")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "1"
        );
    }

    #[test]
    fn native_procedure_output_slots_and_call_copyback_remain_postgres_semantics() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE SCHEMA native_proc").unwrap();
        session.execute("CREATE PROCEDURE native_proc.legacy(amount IN INTEGER,total INOUT INTEGER) AS $$BEGIN total:=total+amount;END$$ LANGUAGE 'plpgsql'").unwrap();
        assert_eq!(
            session
                .execute("CALL native_proc.legacy(2,4)")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "6"
        );
        session.execute("CREATE OR REPLACE PROCEDURE native_proc.p(IN amount NUMERIC,OUT doubled NUMERIC,INOUT total NUMERIC) LANGUAGE plpgsql AS $$BEGIN doubled:=amount*2; total:=total+doubled; END$$").unwrap();
        let row = session.execute("CALL native_proc.p(3,1/0,4)").unwrap();
        assert_eq!(
            row.rows[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["6", "10"]
        );
        assert_eq!(
            row.column_types,
            vec![Some("numeric".into()), Some("numeric".into())]
        );
        session.execute("CREATE FUNCTION native_proc.caller() RETURNS NUMERIC LANGUAGE plpgsql AS $$DECLARE doubled NUMERIC;total NUMERIC:=4;BEGIN CALL native_proc.p(total=>total,amount=>3,doubled=>doubled); RETURN doubled+100*total;END$$").unwrap();
        assert_eq!(
            session.execute("SELECT native_proc.caller()").unwrap().rows[0][0].to_cell(),
            "1006"
        );
        assert_eq!(
            session
                .execute("CALL public.p(3,NULL,4)")
                .unwrap_err()
                .sqlstate(),
            "42883"
        );
        assert_eq!(
            session
                .execute("CALL native_proc.p(3,NULL,4,5)")
                .unwrap_err()
                .sqlstate(),
            "42883"
        );
    }
}
