//! Lower PostgreSQL TABLE query bodies without changing TABLE used as a label
//! or in DDL. Keep identifier tokens and their source positions intact.
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

fn keyword(token: &Token) -> Keyword {
    match token {
        Token::Word(word) if word.quote_style.is_none() => word.keyword,
        _ => Keyword::NoKeyword,
    }
}

// AS starts a query only in these creation headers. In particular, neither a
// SELECT column label nor Oracle CREATE TYPE ... AS TABLE OF is a query body.
fn creation_query_as(tokens: &[TokenWithSpan], start: usize, end: usize) -> bool {
    let mut index = start;
    if tokens.get(index).map(|t| keyword(&t.token)) != Some(Keyword::CREATE) {
        return false;
    }
    index += 1;
    if tokens.get(index).map(|t| keyword(&t.token)) == Some(Keyword::OR) {
        if tokens.get(index + 1).map(|t| keyword(&t.token)) != Some(Keyword::REPLACE) {
            return false;
        }
        index += 2;
    }
    while tokens.get(index).is_some_and(|token| {
        matches!(
            keyword(&token.token),
            Keyword::TEMP
                | Keyword::TEMPORARY
                | Keyword::UNLOGGED
                | Keyword::MATERIALIZED
                | Keyword::GLOBAL
                | Keyword::LOCAL
        )
    }) {
        index += 1;
    }
    if !tokens
        .get(index)
        .is_some_and(|token| matches!(keyword(&token.token), Keyword::TABLE | Keyword::VIEW))
    {
        return false;
    }
    let mut depth = 0usize;
    for token in &tokens[index + 1..end] {
        match token.token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            _ if depth == 0 && keyword(&token.token) == Keyword::AS => return false,
            _ => {}
        }
    }
    depth == 0
}

pub(crate) fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = PostgreSqlDialect {};
    let original = Tokenizer::new(&dialect, sql).tokenize_with_location()?;
    if !original
        .iter()
        .any(|token| keyword(&token.token) == Keyword::TABLE)
    {
        return Parser::new(&dialect)
            .with_tokens_with_locations(original)
            .parse_statements();
    }
    let tokens = original
        .iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_)))
        .cloned()
        .collect::<Vec<_>>();
    let mut replacements = std::collections::BTreeSet::new();
    let mut start = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        let previous = index.checked_sub(1).map(|i| &tokens[i].token);
        let set_body = previous.is_some_and(|token| {
            matches!(
                keyword(token),
                Keyword::UNION | Keyword::EXCEPT | Keyword::INTERSECT
            )
        }) || (previous
            .is_some_and(|token| matches!(keyword(token), Keyword::ALL | Keyword::DISTINCT))
            && index >= 2
            && matches!(
                keyword(&tokens[index - 2].token),
                Keyword::UNION | Keyword::EXCEPT | Keyword::INTERSECT
            ));
        let query_start = index == start
            || matches!(previous, Some(Token::LParen))
            || set_body
            || (previous.is_some_and(|token| keyword(token) == Keyword::AS)
                && creation_query_as(&tokens, start, index - 1));
        if keyword(&token.token) == Keyword::TABLE && query_start {
            replacements.insert(index);
        }
        if token.token == Token::SemiColon {
            start = index + 1;
        }
    }
    let mut lowered = Vec::with_capacity(original.len() + 2 * replacements.len());
    let mut index = 0;
    for token in original {
        if matches!(token.token, Token::Whitespace(_)) {
            lowered.push(token);
            continue;
        }
        if replacements.contains(&index) {
            for replacement in [
                Token::make_keyword("SELECT"),
                Token::Mul,
                Token::make_keyword("FROM"),
            ] {
                lowered.push(TokenWithSpan {
                    token: replacement,
                    span: token.span,
                });
            }
        } else {
            lowered.push(token);
        }
        index += 1;
    }
    Parser::new(&dialect)
        .with_tokens_with_locations(lowered)
        .parse_statements()
}

/// sqlparser's remaining TABLE AST stores bare strings, losing quoted-name
/// semantics, and can consume a trailing clause of an unqualified TABLE body.
/// Reject forms we did not lower rather than execute a different query.
/// Called only after the ordinary AST depth/node guard.
pub(crate) fn reject_unlowered(statements: &[Statement]) -> crate::Result<()> {
    use sqlparser::ast::{Query, SetExpr, Visit, Visitor};
    use std::ops::ControlFlow;
    struct Guard;
    impl Visitor for Guard {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            let mut bodies = vec![query.body.as_ref()];
            while let Some(body) = bodies.pop() {
                match body {
                    SetExpr::Table(_) => return ControlFlow::Break(()),
                    SetExpr::SetOperation { left, right, .. } => {
                        bodies.push(left);
                        bodies.push(right);
                    }
                    _ => {}
                }
            }
            ControlFlow::Continue(())
        }
    }
    if statements
        .iter()
        .any(|statement| statement.visit(&mut Guard).is_break())
    {
        return Err(crate::SqlError::Unsupported(
            "TABLE query form is not supported in this position".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_bodies_keep_names_and_clauses() {
        for (source, expected) in [
            (
                "TABLE source ORDER BY id LIMIT 1",
                "SELECT * FROM source ORDER BY id LIMIT 1",
            ),
            ("TABLE public.source", "SELECT * FROM public.source"),
            (
                "CREATE VIEW v AS TABLE source LIMIT 1",
                "CREATE VIEW v AS SELECT * FROM source LIMIT 1",
            ),
            (
                "CREATE MATERIALIZED VIEW v AS TABLE source",
                "CREATE MATERIALIZED VIEW v AS SELECT * FROM source",
            ),
            (
                "CREATE TABLE copy AS TABLE source",
                "CREATE TABLE copy AS SELECT * FROM source",
            ),
            (
                "WITH q AS (TABLE source) SELECT * FROM q",
                "WITH q AS (SELECT * FROM source) SELECT * FROM q",
            ),
            (
                "TABLE a UNION ALL TABLE b",
                "SELECT * FROM a UNION ALL SELECT * FROM b",
            ),
            (
                r#"TABLE "Mixed.Schema"."SourceCase""Tail""#,
                r#"SELECT * FROM "Mixed.Schema"."SourceCase""Tail""#,
            ),
        ] {
            assert_eq!(
                parse(source).unwrap(),
                Parser::parse_sql(&PostgreSqlDialect {}, expected).unwrap(),
                "{source}"
            );
        }
    }

    #[test]
    fn ddl_and_column_labels_are_untouched() {
        for source in [
            "CREATE TABLE source(id INT)",
            "SELECT 1 AS TABLE",
            "CREATE VIEW v AS SELECT 1 AS TABLE",
            "SELECT 'TABLE source' AS label",
            r#"SELECT "TABLE" FROM source"#,
        ] {
            assert_eq!(
                parse(source).unwrap(),
                Parser::parse_sql(&PostgreSqlDialect {}, source).unwrap(),
                "{source}"
            );
        }
        // Unsupported grammar must not become a different CREATE TYPE.
        assert!(parse("CREATE TYPE collection AS TABLE OF INT").is_err());
    }
}
