//! Procedural CASE dispatch reuses scalar CASE's lazy arm selection. Arm bodies
//! remain statements, including loop control, queries and function returns.
use super::*;

pub(super) fn parse(statement: &str) -> Result<RoutineStmt> {
    let events = routine_boundaries(statement)?;
    let mut depth = 0usize;
    let mut markers = Vec::new();
    let mut close = None;
    for (start, end, kind) in events {
        match kind {
            RoutineBoundary::Case | RoutineBoundary::If | RoutineBoundary::Loop => depth += 1,
            RoutineBoundary::EndCase | RoutineBoundary::EndIf | RoutineBoundary::EndLoop => {
                if depth == 1 {
                    if kind != RoutineBoundary::EndCase {
                        return Err(SqlError::InvalidSql("CASE requires END CASE".into()));
                    }
                    close = Some((start, end));
                    break;
                }
                depth = depth.checked_sub(1).ok_or_else(|| {
                    SqlError::InvalidSql("unmatched procedural CASE boundary".into())
                })?;
            }
            _ if depth == 1 => markers.push((start, end, kind)),
            _ => {}
        }
    }
    let (body_end, end) =
        close.ok_or_else(|| SqlError::InvalidSql("CASE requires END CASE".into()))?;
    if !statement[end..].trim().is_empty() {
        return Err(SqlError::UnsupportedPlpgsqlFeature(
            "labeled END CASE is not implemented".into(),
        ));
    }
    let Some(&(first, _, RoutineBoundary::When)) = markers.first() else {
        return Err(SqlError::InvalidSql(
            "CASE requires at least one WHEN branch".into(),
        ));
    };
    // A scalar CASE computes an integer dispatch index once. This evaluates a
    // simple-case selector once and never evaluates conditions after a match.
    let selector = statement["CASE".len()..first].trim();
    let mut expression = format!("CASE {selector}");
    let mut arms = Vec::new();
    let mut else_body = None;
    let mut index = 0;
    while index < markers.len() {
        let (_, begin, kind) = markers[index];
        match kind {
            RoutineBoundary::When => {
                let Some(&(then, body, RoutineBoundary::Then)) = markers.get(index + 1) else {
                    return Err(SqlError::InvalidSql("CASE WHEN requires THEN".into()));
                };
                let condition = statement[begin..then].trim();
                if condition.is_empty() {
                    return Err(SqlError::InvalidSql(
                        "CASE WHEN requires an expression".into(),
                    ));
                }
                let end = markers.get(index + 2).map_or(body_end, |marker| marker.0);
                expression.push_str(&format!(" WHEN {condition} THEN {}", arms.len()));
                arms.push(parse_routine_statements(&statement[body..end])?);
                index += 2;
            }
            RoutineBoundary::Else if index + 1 == markers.len() => {
                else_body = Some(parse_routine_statements(&statement[begin..body_end])?);
                index += 1;
            }
            _ => {
                return Err(SqlError::InvalidSql(
                    "invalid procedural CASE branch".into(),
                ))
            }
        }
    }
    expression.push_str(" ELSE -1 END");
    Ok(RoutineStmt::Case {
        expression: RoutineExpr::unbound(parse_routine_expr(&expression)?),
        arms,
        else_body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_procedural_case_is_lazy_nested_and_distinct_from_sql_case() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE FUNCTION case_search(p_value INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE result INTEGER:=5; BEGIN CASE WHEN p_value<0 THEN result:=10; result:=result+1; WHEN p_value=0 THEN result:=20; ELSE result:=30; END CASE; RETURN result; END$$",
            "CREATE FUNCTION case_lazy(p_value INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN CASE WHEN p_value=1 THEN RETURN 10; WHEN 1/p_value>0 THEN RETURN 20; ELSE RETURN 30; END CASE; END$$",
            "CREATE FUNCTION case_missing(p_value INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN CASE WHEN p_value=1 THEN RETURN 10; END CASE; RETURN 20; END$$",
            "CREATE FUNCTION case_handled(p_value INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN CASE WHEN p_value=1 THEN RETURN 10; END CASE; RETURN 20; EXCEPTION WHEN case_not_found THEN RETURN 99; END$$",
            "CREATE FUNCTION case_nested(p_value INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE result INTEGER:=0; BEGIN IF p_value IS NOT NULL THEN FOR n IN 1..3 LOOP CASE WHEN n=2 THEN CASE WHEN p_value>0 THEN result:=result+10; ELSE result:=result+20; END CASE; ELSE IF p_value=0 THEN result:=result+100; ELSE result:=result+1; END IF; END CASE; END LOOP; ELSE result:=30; END IF; RETURN result; END$$",
            "CREATE FUNCTION case_expression(p_value INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE result INTEGER:=0; BEGIN CASE WHEN (CASE WHEN p_value<0 THEN 1 ELSE 2 END)=1 THEN result:=CASE WHEN p_value=-1 THEN 10 ELSE 20 END; WHEN p_value=0 THEN SELECT CASE WHEN p_value=0 THEN 30 ELSE 40 END INTO result; ELSE result:=50; END CASE; RETURN result; END$$",
            "CREATE FUNCTION case_simple(p_value INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN CASE p_value WHEN 1 THEN RETURN 10; WHEN 2 THEN RETURN 20; WHEN NULL THEN RETURN 30; ELSE RETURN 40; END CASE; END$$",
            "CREATE SEQUENCE case_seq START WITH 1",
            "CREATE FUNCTION case_once() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN CASE nextval('case_seq') WHEN 0 THEN RETURN 0; WHEN 1 THEN RETURN 10; ELSE RETURN -1; END CASE; END$$",
            "CREATE FUNCTION case_handler_body(p_value INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN CASE WHEN FALSE THEN RETURN 0; END CASE; RETURN 0; EXCEPTION WHEN case_not_found THEN CASE WHEN p_value=1 THEN RETURN 41; ELSE RETURN 42; END CASE; WHEN OTHERS THEN RETURN -1; END$$",
            "CREATE FUNCTION case_control() RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE result INTEGER:=0; BEGIN FOR n IN 1..5 LOOP CASE WHEN n=2 THEN CONTINUE; WHEN n=4 THEN EXIT; ELSE result:=result+n; END CASE; END LOOP; RETURN result; END$$",
        ] { session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}")); }
        for (name, value, expected) in [
            ("search", "-1", 11),
            ("search", "0", 20),
            ("search", "NULL", 30),
            ("lazy", "1", 10),
            // PostgreSQL integer division: 1/2 is zero. Oracle NUMBER's
            // independently tested fractional division selects the other arm.
            ("lazy", "2", 30),
            ("lazy", "NULL", 30),
            ("missing", "1", 10),
            ("handled", "0", 99),
            ("handled", "NULL", 99),
            ("nested", "1", 12),
            ("nested", "-1", 22),
            ("nested", "0", 220),
            ("nested", "NULL", 30),
            ("expression", "-1", 10),
            ("expression", "-2", 20),
            ("expression", "0", 30),
            ("expression", "NULL", 50),
            ("simple", "1", 10),
            ("simple", "2", 20),
            ("simple", "NULL", 40),
            ("handler_body", "1", 41),
            ("handler_body", "2", 42),
            ("once", "", 10),
            ("control", "", 4),
        ] {
            let sql = format!("SELECT case_{name}({value})");
            assert_eq!(
                session.execute(&sql).unwrap().rows[0][0],
                SqlValue::Int(expected),
                "{sql}"
            );
        }
        assert_eq!(
            session.execute("SELECT currval('case_seq')").unwrap().rows[0][0],
            SqlValue::Int(1)
        );
        for sql in ["SELECT case_missing(0)", "SELECT case_missing(NULL)"] {
            assert_eq!(
                session.execute(sql).unwrap_err().sqlstate(),
                "20000",
                "{sql}"
            );
        }
        assert_eq!(
            session
                .execute("SELECT case_lazy(0)")
                .unwrap_err()
                .sqlstate(),
            "22012"
        );
        assert_eq!(
            session
                .execute("SELECT CASE WHEN FALSE THEN 1 END")
                .unwrap()
                .rows[0][0],
            SqlValue::Null
        );
        // Native definitions currently compile on invocation. The control-flow
        // validator must still reject CONTINUE hidden inside a CASE arm.
        session.execute("CREATE FUNCTION bad_case() RETURNS INT LANGUAGE plpgsql AS $$BEGIN CASE WHEN TRUE THEN CONTINUE; END CASE; RETURN 1; END$$").unwrap();
        assert_eq!(
            session.execute("SELECT bad_case()").unwrap_err().sqlstate(),
            "42601"
        );
        assert_eq!(
            session.execute("SELECT case_search(0)").unwrap().rows[0][0],
            SqlValue::Int(20)
        );
    }

    #[test]
    fn oracle_case_bodies_keep_definition_namespace_and_survive_reopen() {
        let root = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open(root.path()).unwrap();
            oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
            for source in [
                "CREATE SCHEMA case_owner",
                "CREATE TABLE case_owner.case_values(k NUMBER, v NUMBER)",
                "INSERT INTO case_owner.case_values VALUES(1,12),(2,23)",
                "CREATE TABLE case_values(k NUMBER, v NUMBER)",
                "INSERT INTO case_values VALUES(1,-999),(2,-999)",
                "CREATE FUNCTION case_owner.case_total(p_mode NUMBER) RETURN NUMBER IS total NUMBER:=0; amount NUMBER; CURSOR c IS SELECT k FROM case_values ORDER BY k; BEGIN FOR r IN c LOOP CASE WHEN p_mode=1 THEN SELECT v INTO amount FROM case_values WHERE k=r.k; total:=total+amount; WHEN p_mode=2 THEN CASE r.k WHEN 1 THEN total:=total+10; ELSE total:=total+20; END CASE; ELSE NULL; END CASE; END LOOP; RETURN total; END;",
                "CREATE FUNCTION case_owner.case_catch(p_mode NUMBER) RETURN NUMBER IS BEGIN CASE WHEN p_mode=1 THEN RETURN 10; END CASE; RETURN 20; EXCEPTION WHEN CASE_NOT_FOUND THEN RETURN 99; END;",
            ] {
                let sql = oracle::lower_profile_sql(&db, source, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
                SqlSession::new(&mut db).execute(&sql).unwrap_or_else(|error| panic!("{source}: {error}"));
            }
        }
        let mut db = BicDb::open(root.path()).unwrap();
        for (call, expected) in [
            ("case_total(1)", "35"),
            ("case_total(2)", "30"),
            ("case_total(NULL)", "0"),
            ("case_catch(1)", "10"),
            ("case_catch(0)", "99"),
        ] {
            let sql = oracle::lower_profile_sql(
                &db,
                &format!("SELECT case_owner.{call} FROM DUAL"),
                oracle::ORACLE_PREVIEW_PROFILE,
            )
            .unwrap();
            assert_eq!(
                SqlSession::new(&mut db).execute(&sql).unwrap().rows[0][0].to_cell(),
                expected,
                "{call}"
            );
        }
    }
}
