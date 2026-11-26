//! `x OP ANY (array)` / `x OP ALL (array)` for operators the SQL parser does
//! not accept before ANY/ALL (it takes only comparisons, LIKE and regex
//! matches): `@@`, `@>`, `<@`, `&&`, `?`, `?|`, `?&` and custom operators.
//!
//! The text is rewritten, token by token, into a scalar subquery with
//! PostgreSQL's quantified semantics:
//!
//! - ANY is true if some element gives true, else NULL if some element gives
//!   NULL, else false (an empty array gives false);
//! - ALL is false if some element gives false, else NULL if some element
//!   gives NULL, else true (an empty array gives true);
//! - a NULL array gives NULL; `x` is evaluated once.
//!
//! String constants, quoted identifiers, comments and dollar quotes are
//! separate tokens, so text inside them is never taken for an operator.
use crate::*;
use sqlparser::tokenizer::Token;

/// Whether `token` is an operator this rewrite handles.
fn quantifiable_operator(token: &Token) -> Option<String> {
    Some(
        match token {
            Token::AtAt => "@@",
            Token::AtArrow => "@>",
            Token::ArrowAt => "<@",
            Token::Overlap => "&&",
            Token::Question => "?",
            Token::QuestionPipe => "?|",
            Token::QuestionAnd => "?&",
            Token::CustomBinaryOperator(name) => return Some(name.clone()),
            _ => return None,
        }
        .to_string(),
    )
}

fn keyword(token: &Token) -> Option<String> {
    match token {
        Token::Word(word) if word.quote_style.is_none() => Some(word.value.to_ascii_uppercase()),
        _ => None,
    }
}

/// Words that end the left operand of a generic operator (everything that
/// binds less tightly, and clause keywords).
fn left_boundary(token: &Token) -> bool {
    if let Some(word) = keyword(token) {
        return matches!(
            word.as_str(),
            "SELECT"
                | "WHERE"
                | "AND"
                | "OR"
                | "NOT"
                | "ON"
                | "WHEN"
                | "THEN"
                | "ELSE"
                | "CASE"
                | "HAVING"
                | "BY"
                | "RETURNING"
                | "SET"
                | "VALUES"
                | "AS"
                | "FROM"
                | "JOIN"
                | "IS"
                | "IN"
                | "LIKE"
                | "ILIKE"
                | "SIMILAR"
                | "BETWEEN"
                | "USING"
                | "CHECK"
                | "DISTINCT"
                | "ALL"
                | "ANY"
                | "SOME"
                | "LIMIT"
                | "OFFSET"
                | "DO"
                | "UPDATE"
                | "INTO"
                | "DEFAULT"
        );
    }
    matches!(
        token,
        Token::Comma
            | Token::SemiColon
            | Token::Eq
            | Token::Neq
            | Token::Lt
            | Token::Gt
            | Token::LtEq
            | Token::GtEq
            | Token::Spaceship
            | Token::Assignment
            | Token::RArrow
            | Token::DoubleEq
    )
}

fn is_space(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

/// The rewritten statement, or `None` when it has no such construct.
pub(crate) fn rewrite_operator_quantifiers(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !(lower.contains("any") || lower.contains("all") || lower.contains("some")) {
        return None;
    }
    let mut text = sql.to_string();
    let mut changed = false;
    // Each pass rewrites one construct; nested ones are found next pass.
    for _ in 0..32 {
        match rewrite_first(&text) {
            Some(next) => {
                text = next;
                changed = true;
            }
            None => break,
        }
    }
    changed.then_some(text)
}

fn rewrite_first(sql: &str) -> Option<String> {
    let tokens = tokens_with_source(&sqlparser::dialect::PostgreSqlDialect {}, sql)?;
    let significant = tokens
        .iter()
        .enumerate()
        .filter(|(_, (token, _))| !is_space(token))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let starts = {
        let mut offset = 0usize;
        tokens
            .iter()
            .map(|(_, source)| {
                let start = offset;
                offset += source.len();
                start
            })
            .collect::<Vec<_>>()
    };
    for (position, &index) in significant.iter().enumerate() {
        let Some(operator) = quantifiable_operator(&tokens[index].0) else {
            continue;
        };
        let Some(&quantifier_index) = significant.get(position + 1) else {
            continue;
        };
        let all = match keyword(&tokens[quantifier_index].0).as_deref() {
            Some("ANY" | "SOME") => false,
            Some("ALL") => true,
            _ => continue,
        };
        let Some(&open_index) = significant.get(position + 2) else {
            continue;
        };
        if tokens[open_index].0 != Token::LParen {
            continue;
        }
        // The closing parenthesis of the quantified operand.
        let mut depth = 0i32;
        let mut close_index = None;
        for (candidate, (token, _)) in tokens.iter().enumerate().skip(open_index) {
            match token {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => {
                    depth -= 1;
                    if depth == 0 {
                        close_index = Some(candidate);
                        break;
                    }
                }
                _ => {}
            }
        }
        let close_index = close_index?;
        // The left operand: back to a looser-binding token at depth 0.
        let mut depth = 0i32;
        let mut left_start = None;
        for back in (0..position).rev() {
            let token = &tokens[significant[back]].0;
            match token {
                Token::RParen | Token::RBracket => depth += 1,
                Token::LParen | Token::LBracket => {
                    if depth == 0 {
                        left_start = Some(back + 1);
                        break;
                    }
                    depth -= 1;
                }
                token if depth == 0 && left_boundary(token) => {
                    left_start = Some(back + 1);
                    break;
                }
                _ => {}
            }
        }
        let left_start = left_start.unwrap_or(0);
        if left_start >= position {
            continue;
        }
        let left_from = starts[significant[left_start]];
        let left_to = starts[index];
        let left = sql[left_from..left_to].trim();
        let inner_from = starts[open_index] + 1;
        let inner_to = starts[close_index];
        let inner = sql[inner_from..inner_to].trim();
        if left.is_empty() || inner.is_empty() {
            continue;
        }
        // An array written out element by element: ANY is the OR and ALL
        // the AND of the element comparisons, which is exactly PostgreSQL's
        // three-valued result, and each element keeps its own type.
        let replacement = if let Some(elements) = array_elements(inner) {
            if elements.is_empty() {
                (if all { "true" } else { "false" }).to_string()
            } else {
                let joiner = if all { " AND " } else { " OR " };
                format!(
                    "({})",
                    elements
                        .iter()
                        .map(|element| format!("({left}) {operator} ({element})"))
                        .collect::<Vec<_>>()
                        .join(joiner)
                )
            }
        } else {
            let first_word = inner
                .split(|c: char| !c.is_ascii_alphabetic())
                .next()
                .unwrap_or("")
                .to_ascii_uppercase();
            let subquery = matches!(first_word.as_str(), "SELECT" | "WITH" | "VALUES");
            let source = if subquery {
                format!("({inner}) AS __bicdb_qu(__bicdb_qe)")
            } else {
                format!("unnest({inner}) AS __bicdb_qu(__bicdb_qe)")
            };
            let verdict = if all {
                "CASE WHEN bool_or(__bicdb_qr IS FALSE) THEN false \
                 WHEN bool_or(__bicdb_qr IS NULL) THEN NULL ELSE true END"
            } else {
                "CASE WHEN bool_or(__bicdb_qr IS TRUE) THEN true \
                 WHEN bool_or(__bicdb_qr IS NULL) THEN NULL ELSE false END"
            };
            // The left operand stays in the outer scope so its type is kept;
            // it is evaluated per element.
            let quantified = format!(
                "(SELECT {verdict} FROM (SELECT ({left}) {operator} __bicdb_qu.__bicdb_qe \
                 AS __bicdb_qr FROM {source}) AS __bicdb_qq)"
            );
            if subquery {
                quantified
            } else {
                format!("(CASE WHEN ({inner}) IS NULL THEN NULL ELSE {quantified} END)")
            }
        };
        let end = starts[close_index] + tokens[close_index].1.len();
        return Some(format!("{}{replacement}{}", &sql[..left_from], &sql[end..]));
    }
    None
}

/// The elements of an array operand written out literally, as SQL
/// expressions: `ARRAY[a, b]` (with an optional `::type[]` cast applied to
/// each element) or a one-dimensional array constant `'{a,b}'` (its
/// elements become string constants of the cast's element type, or untyped).
fn array_elements(inner: &str) -> Option<Vec<String>> {
    let tokens = tokens_with_source(&sqlparser::dialect::PostgreSqlDialect {}, inner)?;
    let significant = tokens
        .iter()
        .filter(|(token, _)| !is_space(token))
        .collect::<Vec<_>>();
    // A trailing `:: type []` cast.
    let (body, cast) = match significant.as_slice() {
        [body @ .., (Token::DoubleColon, _), (Token::Word(_), name), (Token::LBracket, _), (Token::RBracket, _)] => {
            (body, Some(name.trim().to_string()))
        }
        body => (body, None),
    };
    let element = |text: String| match &cast {
        Some(cast) => format!("({text})::{cast}"),
        None => text,
    };
    match body {
        [(Token::Word(word), _), (Token::LBracket, _), rest @ .., (Token::RBracket, _)]
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("ARRAY") =>
        {
            let mut elements = Vec::new();
            let mut current = String::new();
            let mut depth = 0i32;
            for (token, source) in rest {
                match token {
                    Token::LParen | Token::LBracket | Token::LBrace => depth += 1,
                    Token::RParen | Token::RBracket | Token::RBrace => depth -= 1,
                    Token::Comma if depth == 0 => {
                        elements.push(element(std::mem::take(&mut current)));
                        continue;
                    }
                    _ => {}
                }
                if !current.is_empty() {
                    current.push(' ');
                }
                current.push_str(source);
            }
            // A nested array (`ARRAY[ARRAY[..]]`, `ARRAY[[..]]`) is
            // multidimensional: its elements are not the sub-arrays.
            if rest.iter().any(|(token, _)| {
                matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case("ARRAY"))
                    || *token == Token::LBracket
            }) {
                return None;
            }
            if !current.trim().is_empty() {
                elements.push(element(current));
            } else if !elements.is_empty() {
                return None;
            }
            Some(elements)
        }
        [(Token::SingleQuotedString(text), _)] => {
            let text = text.trim();
            let content = text.strip_prefix('{')?.strip_suffix('}')?;
            if content.contains(['{', '}']) {
                return None;
            }
            let mut elements = Vec::new();
            let mut chars = content.chars().peekable();
            while chars.peek().is_some() {
                while chars.peek().is_some_and(|c| c.is_whitespace()) {
                    chars.next();
                }
                let mut value = String::new();
                let quoted = chars.peek() == Some(&'"');
                if quoted {
                    chars.next();
                    loop {
                        match chars.next()? {
                            '"' => break,
                            '\\' => value.push(chars.next()?),
                            other => value.push(other),
                        }
                    }
                    while chars.peek().is_some_and(|c| c.is_whitespace()) {
                        chars.next();
                    }
                } else {
                    while let Some(&c) = chars.peek() {
                        if c == ',' {
                            break;
                        }
                        if c == '"' || c == '\\' {
                            return None;
                        }
                        value.push(c);
                        chars.next();
                    }
                    value = value.trim().to_string();
                    if value.is_empty() {
                        return None;
                    }
                }
                match chars.next() {
                    None | Some(',') => {}
                    Some(_) => return None,
                }
                elements.push(if !quoted && value.eq_ignore_ascii_case("NULL") {
                    element("NULL".to_string())
                } else {
                    element(format!("'{}'", value.replace('\'', "''")))
                });
            }
            Some(elements)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_only_unparseable_quantified_operators() {
        assert!(rewrite_operator_quantifiers("SELECT a = ANY(b) FROM t").is_none());
        assert!(rewrite_operator_quantifiers("SELECT '@@ ANY(x)' FROM t").is_none());
        assert_eq!(
            rewrite_operator_quantifiers("SELECT v @@ ANY(ARRAY[q1, q2]) AND x FROM t").unwrap(),
            "SELECT ((v) @@ (q1) OR (v) @@ (q2)) AND x FROM t"
        );
        assert_eq!(
            rewrite_operator_quantifiers(r#"SELECT a @@ ALL ('{wr, "q h", NULL}') FROM t"#)
                .unwrap(),
            "SELECT ((a) @@ ('wr') AND (a) @@ ('q h') AND (a) @@ (NULL)) FROM t"
        );
        assert_eq!(
            rewrite_operator_quantifiers("SELECT a @@ ANY (ARRAY[]::tsquery[]) FROM t").unwrap(),
            "SELECT false FROM t"
        );
        let rewritten = rewrite_operator_quantifiers("SELECT a @@ ANY (c) FROM t").unwrap();
        assert!(
            rewritten.starts_with("SELECT (CASE WHEN (c) IS NULL"),
            "{rewritten}"
        );
        let rewritten =
            rewrite_operator_quantifiers("SELECT f(a, b) @> ALL (SELECT c FROM d) FROM t").unwrap();
        assert!(
            rewritten.contains("(f(a, b)) @> __bicdb_qu.__bicdb_qe"),
            "{rewritten}"
        );
        assert!(
            rewritten.contains("(SELECT c FROM d) AS __bicdb_qu"),
            "{rewritten}"
        );
    }
}
