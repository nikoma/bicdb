//! Resolve table-column anchors in the definition's namespace, before erasing
//! Oracle type syntax. This does not infer logical DATE from a physical timestamp.
use super::*;

fn identifier(token: &Token) -> Option<String> {
    let Token::Word(word) = token else {
        return None;
    };
    Some(if word.quote_style.is_some() {
        word.value.clone()
    } else {
        word.value.to_uppercase()
    })
}

fn name_parts(source: &[Token]) -> Option<(Vec<String>, usize)> {
    let mut parts = vec![identifier(source.first()?)?];
    let mut used = 1;
    while source.get(used) == Some(&Token::Period) {
        parts.push(identifier(source.get(used + 1)?)?);
        used += 2;
    }
    Some((parts, used))
}

pub(super) fn definition_namespace(
    source: &[Token],
    fallback: Option<&str>,
) -> Result<Option<String>> {
    let Some(index) = source
        .iter()
        .take(4)
        .position(|token| word(token, "FUNCTION") || word(token, "PROCEDURE"))
    else {
        return Ok(fallback.map(str::to_string));
    };
    let (parts, _) = name_parts(&source[index + 1..])
        .ok_or_else(|| unsupported("routine requires an identifier"))?;
    match parts.as_slice() {
        [_] => Ok(fallback.map(str::to_string)),
        [schema, _] => Ok(Some(schema.clone())),
        _ => Err(unsupported(
            "routine name must have one or two identifier parts",
        )),
    }
}

pub(super) fn lower(
    db: &BicDb,
    source: &[Token],
    namespace: Option<&str>,
    preserve_modifiers: bool,
    preserve_date: bool,
) -> Result<Option<(String, usize)>> {
    // The canonicalizer visits every token. Avoid allocating identifier parts
    // for ordinary SQL expressions that contain no type anchor.
    if !matches!(source.first(), Some(Token::Word(_))) {
        return Ok(None);
    }
    let mut end = 1;
    while source.get(end) == Some(&Token::Period) {
        if !matches!(source.get(end + 1), Some(Token::Word(_))) {
            return Ok(None);
        }
        end += 2;
    }
    if source.get(end) != Some(&Token::Mod)
        || !source.get(end + 1).is_some_and(|token| word(token, "TYPE"))
    {
        return Ok(None);
    }
    let Some((mut parts, used)) = name_parts(source) else {
        return Ok(None);
    };
    if source.get(used) != Some(&Token::Mod)
        || !source
            .get(used + 1)
            .is_some_and(|token| word(token, "TYPE"))
    {
        return Ok(None);
    }
    if !matches!(parts.len(), 2 | 3) {
        return Err(unsupported("%TYPE requires [schema.]table.column"));
    }
    let column_name = parts.pop().expect("anchor column");
    if parts.len() == 1 {
        if let Some(schema) = namespace {
            parts.insert(0, schema.to_string());
        }
    }
    let physical = relation_name_from_parts(&parts)?;
    let schema = load_schema(db, &physical)?
        .ok_or_else(|| unsupported(format!("unknown %TYPE table {}", parts.join("."))))?;
    let column = schema
        .columns
        .iter()
        .find(|column| column.name == column_name)
        .ok_or_else(|| unsupported(format!("unknown %TYPE column {column_name}")))?;
    if preserve_date && matches!(column.pg_type.as_str(), "timestamp" | "timestamptz") {
        return Err(unsupported(
            "temporal %TYPE requires source logical type provenance",
        ));
    }
    let data_type = if preserve_modifiers {
        column.formatted_pg_type()
    } else {
        column.pg_type.clone()
    };
    Ok(Some((data_type, used + 2)))
}

/// Encode Oracle's constrained numeric return in the stored body as an ordinary
/// CAST. Existing readers already enforce that operation; character return
/// lengths and argument modifiers retain their unconstrained signature rules.
pub(super) fn constrain_numeric_returns(header: &[Token], body: &mut Vec<Token>) -> Result<()> {
    if !header.iter().any(|token| word(token, "FUNCTION")) {
        return Ok(());
    }
    let Some(returns) = header.iter().position(|token| word(token, "RETURNS")) else {
        return Ok(());
    };
    let declaration = &header[returns + 1..];
    // Column descriptors render constrained NUMBER as numeric(p,s). Leave
    // unrelated return-type syntax to the existing routine parser.
    if !declaration
        .first()
        .is_some_and(|token| word(token, "NUMERIC"))
        || declaration.get(1) != Some(&Token::LParen)
    {
        return Ok(());
    }
    let schema = raw_routine_type_schema(&join(declaration))?;
    if schema.pg_type != "numeric"
        || !matches!(schema.type_modifier, Some(PgTypeModifier::Numeric { .. }))
    {
        return Ok(());
    }
    let data_type = schema.formatted();
    let mut lowered = Vec::with_capacity(body.len());
    let mut index = 0;
    while index < body.len() {
        lowered.push(body[index].clone());
        index += 1;
        if !word(lowered.last().expect("return token"), "RETURN") {
            continue;
        }
        let end = (index..body.len())
            .find(|position| body[*position] == Token::SemiColon)
            .ok_or_else(|| {
                unsupported("numeric function RETURN requires a terminating semicolon")
            })?;
        if end == index || word(&body[index], "QUERY") {
            return Err(unsupported(
                "numeric function RETURN requires a scalar expression",
            ));
        }
        let value = expression(&join(&body[index..end]))?;
        let value = date_trunc::exact_numeric_operand_sql(&value);
        lowered.extend(tokens(&format!("CAST({value} AS {data_type})"))?);
        index = end;
    }
    *body = lowered;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execute(db: &mut BicDb, sql: &str) -> SqlResult {
        let lowered = lower_profile_sql(db, sql, ORACLE_PREVIEW_PROFILE).unwrap();
        SqlSession::new(db)
            .execute(&lowered)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
    }

    fn database(path: &std::path::Path) -> BicDb {
        let mut db = BicDb::open(path).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for sql in [
            "CREATE SCHEMA red",
            "CREATE SCHEMA blue",
            "CREATE TABLE anchor_rows(value NUMBER(8,3) DEFAULT 1 NOT NULL)",
            "CREATE TABLE red.anchor_rows(value VARCHAR2(3) DEFAULT 'x' NOT NULL)",
            "CREATE TABLE blue.anchor_rows(value NUMBER(5,2) DEFAULT 1 NOT NULL)",
        ] {
            execute(&mut db, sql);
        }
        db
    }

    #[test]
    fn preview_anchors_resolve_definition_owner_and_retain_local_constraints() {
        let root = tempfile::tempdir().unwrap();
        let mut db = database(root.path());
        for sql in [
            "CREATE FUNCTION red.text_anchor(p_value VARCHAR2) RETURN VARCHAR2 IS result anchor_rows.value%TYPE; BEGIN result := p_value; RETURN result; END;",
            "CREATE FUNCTION red.numeric_anchor(p_value NUMBER) RETURN NUMBER IS result blue.anchor_rows.value%TYPE; BEGIN result := p_value; RETURN result; END;",
            "CREATE FUNCTION red.null_anchor RETURN VARCHAR2 IS result anchor_rows.value%TYPE; BEGIN RETURN result; END;",
            "CREATE FUNCTION red.numeric_return(p_value NUMBER) RETURN blue.anchor_rows.value%TYPE IS BEGIN IF p_value IS NULL THEN RETURN NULL; END IF; RETURN p_value; END;",
            "CREATE FUNCTION red.numeric_argument(p_value blue.anchor_rows.value%TYPE) RETURN NUMBER IS BEGIN RETURN p_value; END;",
            "CREATE FUNCTION red.text_return(p_value VARCHAR2) RETURN anchor_rows.value%TYPE IS BEGIN RETURN p_value; END;",
            "CREATE FUNCTION red.positive_return(p_value NUMBER) RETURN blue.anchor_rows.value%TYPE IS BEGIN RETURN CASE WHEN p_value<0 THEN -p_value ELSE p_value END; END;",
        ] { execute(&mut db, sql); }
        for (sql, expected) in [
            ("SELECT red.text_anchor('abc') FROM DUAL", "abc"),
            ("SELECT red.numeric_anchor(12.345) FROM DUAL", "12.35"),
            ("SELECT red.numeric_anchor(-12.345) FROM DUAL", "-12.35"),
            ("SELECT red.numeric_return(12.345) FROM DUAL", "12.35"),
            (
                "SELECT red.numeric_argument(1000.123) FROM DUAL",
                "1000.123",
            ),
            ("SELECT red.text_return('abcdef') FROM DUAL", "abcdef"),
            ("SELECT red.positive_return(-12.345) FROM DUAL", "12.35"),
        ] {
            assert_eq!(
                execute(&mut db, sql).rows[0][0].to_cell(),
                expected,
                "{sql}"
            );
        }
        assert_eq!(
            execute(&mut db, "SELECT red.null_anchor() FROM DUAL").rows[0][0],
            SqlValue::Null
        );
        for sql in [
            "SELECT red.text_anchor('abcd') FROM DUAL",
            "SELECT red.numeric_anchor(1000) FROM DUAL",
            "SELECT red.numeric_return(1000) FROM DUAL",
        ] {
            let lowered = lower_profile_sql(&db, sql, ORACLE_PREVIEW_PROFILE).unwrap();
            assert!(SqlSession::new(&mut db).execute(&lowered).is_err(), "{sql}");
        }
    }

    #[test]
    fn preview_anchors_use_session_namespace_and_preserve_quoted_parts() {
        let root = tempfile::tempdir().unwrap();
        let mut db = database(root.path());
        for sql in [
            "CREATE SCHEMA \"MiX\"",
            "CREATE TABLE \"MiX\".\"Type.Source\"(\"Mixed.Col\" NUMBER(3,1))",
            "CREATE FUNCTION \"MiX\".\"quoted_anchor\"(p_value NUMBER) RETURN NUMBER IS result \"Type.Source\".\"Mixed.Col\"%TYPE; BEGIN result:=p_value; RETURN result; END;",
            "CREATE FUNCTION red.cross_quoted(p_value NUMBER) RETURN NUMBER IS result \"MiX\".\"Type.Source\".\"Mixed.Col\"%TYPE; BEGIN result:=p_value; RETURN result; END;",
        ] {execute(&mut db,sql);}
        for sql in [
            "CREATE FUNCTION selected_anchor(p_value NUMBER) RETURN NUMBER IS result anchor_rows.value%TYPE; BEGIN result:=p_value; RETURN result; END;",
            "CREATE FUNCTION blue.owned_anchor(p_value NUMBER) RETURN NUMBER IS result anchor_rows.value%TYPE; BEGIN result:=p_value; RETURN result; END;",
        ] {
            let path=if sql.contains("blue.owned_anchor") {"\"RED\""} else {"\"BLUE\""};
            let lowered=lower_session_sql(&db,sql,ORACLE_PREVIEW_PROFILE,Some(path),"red").unwrap();
            SqlSession::new(&mut db).execute(&lowered).unwrap();
        }
        for (sql, expected) in [
            ("SELECT \"MiX\".\"quoted_anchor\"(12.35) FROM DUAL", "12.4"),
            ("SELECT red.cross_quoted(12.35) FROM DUAL", "12.4"),
            ("SELECT blue.selected_anchor(13.555) FROM DUAL", "13.56"),
            ("SELECT blue.owned_anchor(13.555) FROM DUAL", "13.56"),
        ] {
            assert_eq!(
                execute(&mut db, sql).rows[0][0].to_cell(),
                expected,
                "{sql}"
            );
        }
    }

    #[test]
    fn preview_anchors_do_not_fall_back_or_guess_logical_temporal_types() {
        let root = tempfile::tempdir().unwrap();
        let mut db = database(root.path());
        execute(&mut db, "CREATE SCHEMA empty_owner");
        execute(&mut db, "CREATE TABLE red.time_rows(value DATE)");
        for (sql,reason) in [
            ("CREATE FUNCTION empty_owner.missing_anchor RETURN NUMBER IS result anchor_rows.value%TYPE; BEGIN RETURN result; END;","unknown %TYPE table EMPTY_OWNER.ANCHOR_ROWS"),
            ("CREATE FUNCTION red.missing_column RETURN NUMBER IS result anchor_rows.absent%TYPE; BEGIN RETURN result; END;","unknown %TYPE column ABSENT"),
            ("CREATE FUNCTION red.unknown_temporal RETURN DATE IS result time_rows.value%TYPE; BEGIN RETURN result; END;","source logical type provenance"),
        ] {
            let error=lower_profile_sql(&db,sql,ORACLE_PREVIEW_PROFILE).unwrap_err();
            assert_eq!(error.sqlstate(),"0A000");assert!(error.to_string().contains(reason),"{error}");
        }
    }

    #[test]
    fn postgres_procedural_unary_fallback_uses_live_values_and_declared_types() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE FUNCTION fallback_numeric(p_value NUMERIC,p_negative BOOLEAN) RETURNS TEXT LANGUAGE plpgsql AS $$BEGIN RETURN CAST(CASE WHEN p_negative THEN -p_value ELSE +p_value END AS VARCHAR(80)); END$$",
            "CREATE FUNCTION fallback_integer(p_value INTEGER) RETURNS BIGINT LANGUAGE plpgsql AS $$BEGIN RETURN CASE WHEN TRUE THEN -p_value ELSE 0 END; END$$",
            "CREATE FUNCTION fallback_skip(p_value NUMERIC) RETURNS NUMERIC LANGUAGE plpgsql AS $$BEGIN RETURN CASE WHEN TRUE THEN p_value ELSE -CAST(1 AS INT)/0 END; END$$",
        ] {session.execute(sql).unwrap();}
        for (sql, expected) in [
            (
                "SELECT fallback_numeric(CAST('123456789012345678901234.56789' AS NUMERIC),TRUE)",
                "-123456789012345678901234.56789",
            ),
            (
                "SELECT fallback_numeric(CAST('123456789012345678901234.56789' AS NUMERIC),FALSE)",
                "123456789012345678901234.56789",
            ),
            (
                "SELECT fallback_numeric(CAST('NaN' AS NUMERIC),TRUE)",
                "NaN",
            ),
            (
                "SELECT fallback_numeric(CAST('Infinity' AS NUMERIC),TRUE)",
                "-Infinity",
            ),
            ("SELECT fallback_integer(12)", "-12"),
            ("SELECT fallback_skip(12.345)", "12.345"),
        ] {
            assert_eq!(
                session
                    .execute(sql)
                    .unwrap_or_else(|error| panic!("{sql}: {error}"))
                    .rows[0][0]
                    .to_cell(),
                expected,
                "{sql}"
            );
        }
        assert_eq!(
            session
                .execute("SELECT fallback_numeric(NULL,TRUE)")
                .unwrap()
                .rows[0][0],
            SqlValue::Null
        );
        assert_eq!(
            session
                .execute("SELECT fallback_integer(CAST('-2147483648' AS INTEGER))")
                .unwrap_err()
                .sqlstate(),
            "22003"
        );
    }

    #[test]
    fn postgres_declared_types_and_namespace_rules_remain_native() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE SCHEMA mixed",
            "CREATE TABLE mixed.anchor_rows(value NUMERIC(5,2))",
            "CREATE FUNCTION mixed.native_local(p_value NUMERIC) RETURNS NUMERIC LANGUAGE plpgsql AS $$DECLARE result NUMERIC(5,2); BEGIN result:=p_value; RETURN result; END$$",
            "CREATE FUNCTION mixed.native_text(p_value TEXT) RETURNS TEXT LANGUAGE plpgsql AS $$DECLARE result VARCHAR(3); BEGIN result:=p_value; RETURN result; END$$",
        ] {session.execute(sql).unwrap();}
        assert_eq!(
            session
                .execute("SELECT mixed.native_local(12.345)")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "12.35"
        );
        assert_eq!(
            session
                .execute("SELECT mixed.native_text('abc')")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "abc"
        );
        assert!(session.execute("SELECT mixed.native_text('abcd')").is_err());
        assert!(session
            .execute("SELECT value FROM \"MIXED\".anchor_rows")
            .is_err());
    }
}
