//! `ROWS FROM (f(...), g(...)) [WITH ORDINALITY]` in FROM, which the SQL
//! parser does not read. One function is the plain table-function form.
//! Several functions run in lockstep with NULL padding, which is exactly how
//! set-returning functions in one select list behave, so they become a
//! derived `SELECT f(...), g(...)`; that equivalence holds only for
//! functions returning one scalar column, so only those are rewritten (any
//! other list is left to fail at parse). `WITH ORDINALITY` numbers the zipped
//! rows.
use crate::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::Token;

/// Set-returning functions that return one scalar column.
const SCALAR_SET_FUNCTIONS: &[&str] = &[
    "generate_series",
    "unnest",
    "generate_subscripts",
    "regexp_split_to_table",
    "string_to_table",
    "json_array_elements",
    "json_array_elements_text",
    "jsonb_array_elements",
    "jsonb_array_elements_text",
    "json_object_keys",
    "jsonb_object_keys",
];

const DERIVED_ALIAS: &str = "\"rows_from\"";

/// Whether the argument list of the call `item` has more than one argument.
fn has_top_level_argument_comma(item: &str) -> bool {
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    for ch in item.chars() {
        match (quote, ch) {
            (Some(open), ch) if ch == open => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '"') => quote = Some(ch),
            (None, '(' | '[') => depth += 1,
            (None, ')' | ']') => depth -= 1,
            (None, ',') if depth == 1 => return true,
            _ => {}
        }
    }
    false
}

pub(crate) fn rewrite_rows_from(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    if !bytes
        .windows(4)
        .any(|window| window.eq_ignore_ascii_case(b"rows"))
    {
        return None;
    }
    let source = tokens_with_source(&PostgreSqlDialect {}, input)?;
    let word = |index: usize| -> Option<String> {
        match source.get(index).map(|(token, _)| token) {
            Some(Token::Word(word)) if word.quote_style.is_none() => {
                Some(word.value.to_ascii_uppercase())
            }
            _ => None,
        }
    };
    let next = |from: usize| -> Option<usize> {
        (from..source.len()).find(|index| !matches!(source[*index].0, Token::Whitespace(_)))
    };
    let previous = |before: usize| -> Option<usize> {
        (0..before)
            .rev()
            .find(|index| !matches!(source[*index].0, Token::Whitespace(_)))
    };
    let mut out = String::with_capacity(input.len() + 64);
    let mut changed = false;
    let mut index = 0;
    while index < source.len() {
        let rows_from = word(index).as_deref() == Some("ROWS")
            && previous(index).is_some_and(|before| {
                matches!(source[before].0, Token::Comma)
                    || matches!(word(before).as_deref(), Some("FROM" | "JOIN" | "LATERAL"))
            })
            && next(index + 1).is_some_and(|from| word(from).as_deref() == Some("FROM"))
            && next(index + 1)
                .and_then(|from| next(from + 1))
                .is_some_and(|open| matches!(source[open].0, Token::LParen));
        if !rows_from {
            out.push_str(source[index].1);
            index += 1;
            continue;
        }
        let open = next(next(index + 1)? + 1)?;
        // Split the parenthesized list at its top-level commas.
        let mut depth = 0i32;
        let mut items: Vec<String> = vec![String::new()];
        let mut names: Vec<Option<String>> = vec![None];
        let mut close = None;
        for position in open + 1..source.len() {
            match &source[position].0 {
                Token::LParen => depth += 1,
                Token::RParen if depth == 0 => {
                    close = Some(position);
                    break;
                }
                Token::RParen => depth -= 1,
                Token::Comma if depth == 0 => {
                    items.push(String::new());
                    names.push(None);
                    continue;
                }
                _ => {}
            }
            let last = items.len() - 1;
            if names[last].is_none() && !matches!(source[position].0, Token::Whitespace(_)) {
                names[last] = Some(match &source[position].0 {
                    Token::Word(word) => word.value.to_ascii_lowercase(),
                    _ => String::new(),
                });
            }
            items[last].push_str(source[position].1);
        }
        let close = close?;
        let mut end = close;
        let mut ordinality = false;
        if let Some(with) = next(close + 1).filter(|with| word(*with).as_deref() == Some("WITH")) {
            if let Some(ord) =
                next(with + 1).filter(|ord| word(*ord).as_deref() == Some("ORDINALITY"))
            {
                ordinality = true;
                end = ord;
            }
        }
        let has_alias = next(end + 1).is_some_and(|after| match &source[after].0 {
            Token::Word(word) if word.quote_style.is_some() => true,
            Token::Word(_) => !matches!(
                word(after).as_deref(),
                Some(
                    "WHERE"
                        | "JOIN"
                        | "INNER"
                        | "LEFT"
                        | "RIGHT"
                        | "FULL"
                        | "CROSS"
                        | "NATURAL"
                        | "ON"
                        | "USING"
                        | "GROUP"
                        | "HAVING"
                        | "WINDOW"
                        | "ORDER"
                        | "LIMIT"
                        | "OFFSET"
                        | "FETCH"
                        | "FOR"
                        | "UNION"
                        | "INTERSECT"
                        | "EXCEPT"
                        | "RETURNING"
                )
            ),
            _ => false,
        });
        // unnest with several arrays returns several columns.
        let scalar_only = names.iter().zip(&items).all(|(name, item)| {
            name.as_deref()
                .is_some_and(|name| SCALAR_SET_FUNCTIONS.contains(&name))
                && (name.as_deref() != Some("unnest") || !has_top_level_argument_comma(item))
        });
        let replacement = if let ([item], false) = (items.as_slice(), ordinality && scalar_only) {
            format!(
                "{}{}",
                item.trim(),
                if ordinality { " WITH ORDINALITY" } else { "" }
            )
        } else {
            if items.iter().any(|item| item.trim().is_empty()) || !scalar_only {
                return None;
            }
            let zipped = format!(
                "SELECT {}",
                items
                    .iter()
                    .map(|item| item.trim())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let body = if ordinality {
                format!(
                    "(SELECT *, row_number() OVER () AS ordinality FROM ({zipped}) AS {DERIVED_ALIAS})"
                )
            } else {
                format!("({zipped})")
            };
            if has_alias {
                body
            } else {
                format!("{body} AS {DERIVED_ALIAS}")
            }
        };
        out.push_str(&replacement);
        changed = true;
        index = end + 1;
    }
    changed.then_some(out)
}
