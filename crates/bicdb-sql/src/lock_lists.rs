//! Repair sqlparser's single-item locking target grammar before AST parsing.
use crate::*;

fn word(token: Option<&Token>, value: &str) -> bool {
    matches!(token,Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(value))
}

/// Multiple OF targets have the same meaning as repeated locking clauses,
/// including the NOWAIT/SKIP LOCKED policy on every target.
pub(crate) fn rewrite(sql: &str) -> Result<Option<String>> {
    if !sql.to_ascii_lowercase().contains("for") || !sql.contains(',') {
        return Ok(None);
    }
    let source = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .map_err(|e| SqlError::InvalidSql(e.to_string()))?
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    let mut output = Vec::new();
    let mut index = 0;
    let mut changed = false;
    while index < source.len() {
        if word(source.get(index), "FOR")
            && (word(source.get(index + 1), "UPDATE") || word(source.get(index + 1), "SHARE"))
            && word(source.get(index + 2), "OF")
        {
            let start = index;
            let mut end = index + 3;
            let mut names = Vec::new();
            loop {
                let name = end;
                if !matches!(source.get(end), Some(Token::Word(_))) {
                    return Err(SqlError::InvalidSql(
                        "locking target expected after OF or comma".into(),
                    ));
                }
                end += 1;
                while source.get(end) == Some(&Token::Period)
                    && matches!(source.get(end + 1), Some(Token::Word(_)))
                {
                    end += 2;
                }
                names.push(&source[name..end]);
                if source.get(end) != Some(&Token::Comma) {
                    break;
                }
                end += 1;
            }
            if names.len() > 1 {
                let modifier_start = end;
                if word(source.get(end), "NOWAIT") {
                    end += 1;
                } else if word(source.get(end), "SKIP") && word(source.get(end + 1), "LOCKED") {
                    end += 2;
                }
                for name in names {
                    output.extend_from_slice(&source[start..start + 3]);
                    output.extend_from_slice(name);
                    output.extend_from_slice(&source[modifier_start..end]);
                }
                index = end;
                changed = true;
                continue;
            }
        }
        output.push(source[index].clone());
        index += 1;
    }
    Ok(changed.then(|| {
        output
            .iter()
            .map(crate::eval::rewrite_helpers::token_sql)
            .collect::<Vec<_>>()
            .join(" ")
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn list_modifiers_and_literal_boundaries_survive_parsing() {
        for suffix in ["", " NOWAIT", " SKIP LOCKED"] {
            let sql = format!("SELECT a.k,b.k FROM a,b FOR UPDATE OF a,b{suffix}");
            let rewritten = rewrite(&sql).unwrap().unwrap();
            let statements = Parser::parse_sql(&PostgreSqlDialect {}, &rewritten).unwrap();
            let Statement::Query(query) = &statements[0] else {
                panic!()
            };
            assert_eq!(query.locks.len(), 2);
            assert_eq!(query.locks[0].nonblock, query.locks[1].nonblock);
            assert_eq!(query.locks[0].of.as_ref().unwrap().to_string(), "a");
            assert_eq!(query.locks[1].of.as_ref().unwrap().to_string(), "b");
        }
        assert!(rewrite("SELECT 'FOR UPDATE OF a,b' AS label")
            .unwrap()
            .is_none());
        assert!(rewrite("SELECT a FROM t FOR UPDATE OF t")
            .unwrap()
            .is_none());
    }

    #[test]
    fn grammar_native_lock_lists_keep_quoted_names_and_reject_missing_targets() {
        let statements =
            parse_statements("SELECT * FROM a,b FOR SHARE OF a,\"b\" SKIP LOCKED").unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!()
        };
        assert_eq!(query.locks.len(), 2);
        assert_eq!(query.locks[1].of.as_ref().unwrap().to_string(), "\"b\"");
        assert!(parse_statements("SELECT * FROM a FOR UPDATE OF a,").is_err());
        assert!(parse_statements("SELECT * FROM a FOR UPDATE OF a,,a").is_err());
        assert!(rewrite("SELECT $$FOR UPDATE OF a,b$$").unwrap().is_none());
        assert!(rewrite("SELECT \"FOR UPDATE OF a,b\" FROM a")
            .unwrap()
            .is_none());
    }
}
