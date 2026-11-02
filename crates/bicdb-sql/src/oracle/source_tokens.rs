//! Preserve Oracle source operators and sequence names before canonicalization.
use super::*;
use sqlparser::tokenizer::TokenWithSpan;

/// The generic tokenizer can split `1..3` into decimal tokens `1.` and `.3`.
/// Restore only adjacent source dots; quoted literals, comments and separated
/// decimal tokens must retain their original meaning.
pub(super) fn ranges(source: Vec<TokenWithSpan>) -> Vec<Token> {
    let mut result: Vec<TokenWithSpan> = Vec::new();
    let mut split_endpoint = false;
    for mut current in source {
        if matches!(current.token, Token::Whitespace(_)) {
            continue;
        }
        let adjacent = result.last().is_some_and(|previous| {
            previous.span.end == current.span.start
                && matches!(&previous.token, Token::Period | Token::Number(_, _))
                && matches!(&current.token, Token::Period | Token::Number(_, _))
        });
        if adjacent && split_endpoint {
            if let (Some(previous), Token::Number(fraction, _)) =
                (result.last_mut(), &current.token)
            {
                if let Token::Number(integer, _) = &mut previous.token {
                    if !integer.contains('.') && fraction.starts_with('.') {
                        integer.push_str(fraction);
                        previous.span.end = current.span.end;
                        split_endpoint = false;
                        continue;
                    }
                }
            }
        }
        split_endpoint = false;
        if adjacent {
            let left_dot = match &result.last().unwrap().token {
                Token::Period => true,
                Token::Number(value, _) => value.ends_with('.'),
                _ => false,
            };
            let right_dot = match &current.token {
                Token::Period => true,
                Token::Number(value, _) => value.starts_with('.'),
                _ => false,
            };
            if left_dot && right_dot {
                let mut previous = result.pop().unwrap();
                if let Token::Number(value, _) = &mut previous.token {
                    value.pop();
                    if !value.is_empty() {
                        result.push(previous.clone());
                    }
                }
                previous.token = Token::CustomBinaryOperator("..".into());
                result.push(previous);
                if let Token::Number(value, _) = &mut current.token {
                    value.remove(0);
                    if !value.is_empty() {
                        result.push(current);
                        split_endpoint = true;
                    }
                }
                continue;
            }
        }
        result.push(current);
    }
    result.into_iter().map(|token| token.token).collect()
}

pub(super) fn sequence(
    source: &[Token],
    namespace: Option<&str>,
) -> Result<Option<(String, usize)>> {
    if !matches!(source.first(), Some(Token::Word(_))) || source.get(1) != Some(&Token::Period) {
        return Ok(None);
    }
    let mut index = 0;
    let mut parts = Vec::new();
    while let Some(Token::Word(value)) = source.get(index) {
        parts.push(value);
        index += 1;
        if source.get(index) != Some(&Token::Period) {
            break;
        }
        index += 1;
    }
    let Some(last) = parts.last() else {
        return Ok(None);
    };
    if parts.len() < 2
        || last.quote_style.is_some()
        || !(last.value.eq_ignore_ascii_case("NEXTVAL")
            || last.value.eq_ignore_ascii_case("CURRVAL"))
    {
        return Ok(None);
    }
    let function = last.value.to_ascii_lowercase();
    parts.pop();
    if parts.len() > 2 {
        return Err(unsupported("sequence requires [schema.]name"));
    }
    let mut names = parts
        .into_iter()
        .map(|part| {
            if part.quote_style.is_some() {
                part.value.clone()
            } else {
                part.value.to_uppercase()
            }
        })
        .collect::<Vec<_>>();
    if names.len() == 1 {
        if let Some(namespace) = namespace {
            names.insert(0, namespace.to_string());
        }
    }
    let name = names
        .into_iter()
        .map(|name| Ident::with_quote('"', name).to_string())
        .collect::<Vec<_>>()
        .join(".");
    Ok(Some((
        format!("{function}('{}')", name.replace('\'', "''")),
        index,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjacent_range_dots_survive_lowering_without_rewriting_decimal_tokens_or_literals() {
        for (source, expected) in [
            ("1..3", "1 .. 3"),
            ("first..last", "first .. last"),
            ("1..last", "1 .. last"),
            ("first..3", "first .. 3"),
            ("1.5..3.5", "1.5 .. 3.5"),
        ] {
            let lowered = join(&tokens(source).unwrap());
            assert_eq!(lowered, expected, "{source}");
            assert_eq!(join(&tokens(&lowered).unwrap()), lowered);
        }
        assert_eq!(join(&tokens("1. .3").unwrap()), "1. .3");
        assert_eq!(join(&tokens("'1..3' /* 2..4 */").unwrap()), "'1..3'");
        assert_eq!(join(&tokens("\"first..last\"").unwrap()), "\"first..last\"");
    }

    #[test]
    fn preview_numeric_loops_and_quoted_sequence_selectors_keep_definition_ownership() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for source in [
            r#"CREATE SCHEMA "Seq.Owner""#,
            r#"CREATE SEQUENCE "Seq.Owner"."Case.Seq" START WITH 1"#,
            r#"CREATE SEQUENCE "Case.Seq" START WITH 900"#,
            r#"CREATE FUNCTION "Seq.Owner".seq_case RETURN NUMBER IS BEGIN CASE "Seq.Owner"."Case.Seq".NEXTVAL WHEN 0 THEN RETURN 0; WHEN 1 THEN RETURN 10; ELSE RETURN -1; END CASE; END;"#,
            r#"CREATE FUNCTION "Seq.Owner".seq_current RETURN NUMBER IS BEGIN RETURN "Case.Seq".CURRVAL; END;"#,
            "CREATE FUNCTION numeric_loop(p_high NUMBER) RETURN NUMBER IS total NUMBER:=0; BEGIN FOR n IN 1..p_high LOOP total:=total+n; END LOOP; RETURN total; END;",
        ] {
            let sql=lower_profile_sql(&db,source,ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap_or_else(|error| panic!("{source}: {error}"));
        }
        let mut session = SqlSession::new(&mut db);
        for (sql, expected) in [
            (r#"SELECT "Seq.Owner".seq_case() FROM DUAL"#, "10"),
            (r#"SELECT "Seq.Owner".seq_current() FROM DUAL"#, "1"),
            ("SELECT numeric_loop(3) FROM DUAL", "6"),
            ("SELECT numeric_loop(0) FROM DUAL", "0"),
        ] {
            let sql = lower_profile_sql(session.db_ref(), sql, ORACLE_PREVIEW_PROFILE).unwrap();
            assert_eq!(
                session
                    .execute(&sql)
                    .unwrap_or_else(|error| panic!("{sql}: {error}"))
                    .rows[0][0]
                    .to_cell(),
                expected
            );
        }
        // The public shadow is independent of the function's owner sequence.
        let sql = lower_profile_sql(
            session.db_ref(),
            r#"SELECT "Case.Seq".NEXTVAL FROM DUAL"#,
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        assert_eq!(session.execute(&sql).unwrap().rows[0][0].to_cell(), "900");
    }
}
