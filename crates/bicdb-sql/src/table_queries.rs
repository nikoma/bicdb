//! Lower PostgreSQL TABLE query bodies without changing TABLE used as a label
//! or in DDL. Keep identifier tokens and their source positions intact.
use sqlparser::ast::{
    visit_expressions_mut, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, Statement,
};
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
        return parse_tokens(&dialect, original);
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
    parse_tokens(&dialect, lowered)
}

fn parse_tokens(
    dialect: &PostgreSqlDialect,
    mut tokens: Vec<TokenWithSpan>,
) -> Result<Vec<Statement>, ParserError> {
    let document = lower_xml_document_predicates(&mut tokens);
    let mut statements = Parser::new(dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()?;
    if document {
        crate::reject_deep_expressions(&statements)
            .map_err(|error| ParserError::ParserError(error.to_string()))?;
        let _ = visit_expressions_mut(&mut statements, |expr| {
            let Expr::IsNormalized {
                expr: input,
                form: None,
                negated,
            } = expr
            else {
                return std::ops::ControlFlow::<()>::Continue(());
            };
            let name = if *negated {
                "bicdb_xml_is_not_document"
            } else {
                "bicdb_xml_is_document"
            };
            let input = std::mem::replace(
                input.as_mut(),
                Expr::Value(sqlparser::ast::Value::Null.into()),
            );
            *expr = Expr::Function(Function {
                name: ObjectName::from(vec![Ident::new(name)]),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(input))],
                    clauses: Vec::new(),
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: Vec::new(),
            });
            std::ops::ControlFlow::Continue(())
        });
    }
    Ok(statements)
}

fn lower_xml_document_predicates(tokens: &mut Vec<TokenWithSpan>) -> bool {
    if !tokens.iter().any(|token| {
        matches!(&token.token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("document"))
    }) {
        return false;
    }
    let significant = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token.token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let mut documents = Vec::new();
    let mut authored_normalized = Vec::new();
    for (position, &index) in significant.iter().enumerate() {
        if keyword(&tokens[index].token) != Keyword::IS {
            continue;
        }
        let mut following = significant.iter().skip(position + 1);
        let Some(&first) = following.next() else {
            continue;
        };
        let target = if keyword(&tokens[first].token) == Keyword::NOT {
            let Some(&after_not) = following.next() else {
                continue;
            };
            after_not
        } else {
            first
        };
        match &tokens[target].token {
            Token::Word(word)
                if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("document") =>
            {
                documents.push(target);
            }
            token if keyword(token) == Keyword::NORMALIZED => authored_normalized.push(target),
            _ => {}
        }
    }
    if documents.is_empty() {
        return false;
    }
    // sqlparser's existing IS NORMALIZED has the same unary postfix
    // grammar as IS DOCUMENT. Keep explicit NFC on authored normalization
    // predicates so only the XML predicates use the temporary AST form.
    for index in documents {
        tokens[index].token = Token::make_keyword("NORMALIZED");
    }
    for index in authored_normalized.into_iter().rev() {
        let span = tokens[index].span;
        tokens.insert(
            index,
            TokenWithSpan {
                token: Token::make_keyword("NFC"),
                span,
            },
        );
    }
    true
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
