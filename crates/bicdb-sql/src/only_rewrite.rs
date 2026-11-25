//! `ONLY` in `SELECT`/`JOIN`, `UPDATE` and `DELETE`, which the SQL parser
//! does not accept there. `ONLY t` reads or writes the rows stored in `t`
//! itself, not those of tables inheriting from it, so it is rewritten into a
//! restriction on the row's `tableoid`:
//!
//! * `FROM ONLY t [AS] a` becomes
//!   `FROM (SELECT * FROM t WHERE tableoid = 't'::regclass) AS a`;
//! * `UPDATE ONLY t ... [WHERE c]` and `DELETE FROM ONLY t ... [WHERE c]`
//!   keep the target and gain `WHERE (c) AND a.tableoid = 't'::regclass`.
//!
//! The rewrite works on tokens, so string literals, quoted identifiers,
//! comments and dollar quotes are never touched.
use crate::eval::rewrite_helpers::tokens_with_source;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token;

fn keyword(token: &Token, value: &str) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value))
}

fn is_alias_word(token: &Token) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_some() || word.keyword == Keyword::NoKeyword)
}

pub(crate) fn rewrite_only_relations(input: &str) -> Option<String> {
    if !input.to_ascii_lowercase().contains("only") {
        return None;
    }
    let source = tokens_with_source(&PostgreSqlDialect {}, input)?;
    let tokens = source.iter().map(|(token, _)| token).collect::<Vec<_>>();
    let sig = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let text = |from: usize, to: usize| -> String {
        source[from..to]
            .iter()
            .map(|(_, text)| *text)
            .collect::<String>()
    };
    // Replacements as (first token, end token exclusive, new text), plus
    // insertions of predicates as (token index, text, inserted before it).
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    for (position, &index) in sig.iter().enumerate() {
        if !keyword(tokens[index], "ONLY") || position == 0 {
            continue;
        }
        let previous = tokens[sig[position - 1]];
        let target = if keyword(previous, "UPDATE") {
            Target::Update
        } else if keyword(previous, "FROM")
            && position >= 2
            && keyword(tokens[sig[position - 2]], "DELETE")
        {
            Target::Delete
        } else if keyword(previous, "FROM") || keyword(previous, "JOIN") {
            Target::Read
        } else {
            continue;
        };
        // The relation name: identifier ( . identifier )*.
        let mut cursor = position + 1;
        let Some(&first) = sig.get(cursor) else {
            continue;
        };
        if !matches!(tokens[first], Token::Word(_)) {
            continue;
        }
        let mut name_end = first + 1;
        cursor += 1;
        let mut last_part = match tokens[first] {
            Token::Word(word) => word.value.clone(),
            _ => unreachable!(),
        };
        while sig
            .get(cursor)
            .is_some_and(|&i| *tokens[i] == Token::Period)
        {
            let Some(&part) = sig.get(cursor + 1) else {
                break;
            };
            let Token::Word(word) = tokens[part] else {
                break;
            };
            last_part = word.value.clone();
            name_end = part + 1;
            cursor += 2;
        }
        let name_text = text(first, name_end);
        // An optional alias.
        let mut alias = None;
        let mut alias_end = name_end;
        if let Some(&next) = sig.get(cursor) {
            if keyword(tokens[next], "AS") {
                if let Some(&alias_index) = sig.get(cursor + 1) {
                    alias = Some(text(alias_index, alias_index + 1));
                    alias_end = alias_index + 1;
                }
            } else if is_alias_word(tokens[next]) && matches!(target, Target::Read) {
                alias = Some(text(next, next + 1));
                alias_end = next + 1;
            } else if is_alias_word(tokens[next])
                && !keyword(tokens[next], "SET")
                && !keyword(tokens[next], "WHERE")
                && !keyword(tokens[next], "USING")
                && !keyword(tokens[next], "RETURNING")
            {
                alias = Some(text(next, next + 1));
                alias_end = next + 1;
            }
        }
        let qualifier = alias.clone().unwrap_or_else(|| {
            if name_text.contains('"') {
                text(name_end - 1, name_end)
            } else {
                last_part.clone()
            }
        });
        let regclass = format!("'{}'::regclass", name_text.replace('\'', "''"));
        match target {
            Target::Read => {
                let restriction = format!("{qualifier}.tableoid = {regclass}");
                match read_restriction_edits(&tokens, &sig, position, alias_end, &restriction) {
                    Some(mut placed) => {
                        // Keep the relation (system columns stay visible),
                        // drop the ONLY word, restrict where it is read.
                        edits.push((index, first, String::new()));
                        edits.append(&mut placed);
                    }
                    None => edits.push((
                        index,
                        alias_end,
                        format!(
                            "(SELECT * FROM {name_text} WHERE tableoid = {regclass}) AS {qualifier}"
                        ),
                    )),
                }
            }
            Target::Update | Target::Delete => {
                // Drop the ONLY word; restrict the statement's WHERE.
                edits.push((index, first, String::new()));
                let mut depth = 0usize;
                let mut where_at = None;
                let mut end = None;
                for &i in sig.iter().filter(|&&i| i >= alias_end) {
                    match tokens[i] {
                        Token::LParen => depth += 1,
                        Token::RParen => depth = depth.saturating_sub(1),
                        Token::SemiColon if depth == 0 => {
                            end = Some(i);
                            break;
                        }
                        token if depth == 0 && keyword(token, "WHERE") && where_at.is_none() => {
                            where_at = Some(i)
                        }
                        token if depth == 0 && keyword(token, "RETURNING") => {
                            end = Some(i);
                            break;
                        }
                        _ => {}
                    }
                }
                let end = end.unwrap_or(tokens.len());
                let restriction = format!("{qualifier}.tableoid = {regclass}");
                match where_at {
                    Some(where_index) => {
                        edits.push((where_index + 1, where_index + 1, " (".to_string()));
                        edits.push((end, end, format!(") AND {restriction} ")));
                    }
                    None => edits.push((end, end, format!(" WHERE {restriction} "))),
                }
            }
        }
    }
    if edits.is_empty() {
        return None;
    }
    edits.sort_by_key(|(start, end, _)| (*start, *end));
    let mut out = String::with_capacity(input.len() + 64);
    let mut copied = 0usize;
    for (start, end, replacement) in edits {
        if start < copied {
            return None;
        }
        out.push_str(&text(copied, start));
        out.push_str(&replacement);
        copied = end;
    }
    out.push_str(&text(copied, source.len()));
    Some(out)
}

/// Where a read `ONLY` relation's restriction goes, as insertions: into the
/// `ON` clause of the `JOIN` that reads it, or into the `WHERE` clause of the
/// query level whose `FROM` lists it. `None` when neither is safe (a
/// `RIGHT`/`FULL` join at that level could null-extend it, or a join without
/// `ON`), so the caller uses a derived table instead.
fn read_restriction_edits(
    tokens: &[&Token],
    sig: &[usize],
    position: usize,
    alias_end: usize,
    restriction: &str,
) -> Option<Vec<(usize, usize, String)>> {
    let previous = tokens[sig[position - 1]];
    // The paren depth of the relation, relative to the statement start.
    let mut depth = 0isize;
    for &i in &sig[..position] {
        match tokens[i] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ => {}
        }
    }
    let level_tokens = |from: usize| {
        let mut current = depth;
        let mut out = Vec::new();
        for &i in sig.iter().filter(|&&i| i >= from) {
            match tokens[i] {
                Token::LParen => current += 1,
                Token::RParen => {
                    if current == depth {
                        out.push(i);
                        break;
                    }
                    current -= 1;
                }
                _ if current == depth => out.push(i),
                _ => {}
            }
        }
        out
    };
    let is_end = |token: &Token| {
        matches!(token, Token::RParen | Token::SemiColon)
            || [
                "GROUP",
                "HAVING",
                "WINDOW",
                "ORDER",
                "LIMIT",
                "OFFSET",
                "FETCH",
                "UNION",
                "INTERSECT",
                "EXCEPT",
                "FOR",
                "RETURNING",
            ]
            .iter()
            .any(|word| keyword(token, word))
    };
    let after = level_tokens(alias_end);
    if keyword(previous, "JOIN") {
        // The relation is the preserved side of a RIGHT or FULL join: an ON
        // restriction would null-extend its other rows instead of dropping them.
        let join_kind = sig[..position - 1]
            .iter()
            .rev()
            .take(2)
            .any(|&i| keyword(tokens[i], "RIGHT") || keyword(tokens[i], "FULL"));
        if join_kind {
            return None;
        }
        // `JOIN ONLY t ON cond` → `JOIN t ON (cond) AND restriction`.
        let on = *after.first()?;
        if !keyword(tokens[on], "ON") {
            return None;
        }
        let end = after
            .iter()
            .skip(1)
            .copied()
            .find(|&i| {
                is_end(tokens[i])
                    || keyword(tokens[i], "WHERE")
                    || keyword(tokens[i], "JOIN")
                    || keyword(tokens[i], "LEFT")
                    || keyword(tokens[i], "RIGHT")
                    || keyword(tokens[i], "FULL")
                    || keyword(tokens[i], "INNER")
                    || keyword(tokens[i], "CROSS")
                    || keyword(tokens[i], "NATURAL")
                    || *tokens[i] == Token::Comma
            })
            .unwrap_or(tokens.len());
        return Some(vec![
            (on + 1, on + 1, " (".to_string()),
            (end, end, format!(") AND {restriction} ")),
        ]);
    }
    // In a FROM list: the level must not null-extend the relation.
    let level = {
        // Tokens of the whole query level, from its start.
        let mut start = 0usize;
        let mut current = 0isize;
        for (k, &i) in sig[..position].iter().enumerate() {
            match tokens[i] {
                Token::LParen => {
                    current += 1;
                    if current == depth {
                        start = k + 1;
                    }
                }
                Token::RParen => current -= 1,
                _ => {}
            }
        }
        let mut out = sig[start..position].to_vec();
        out.extend(after.iter().copied());
        out
    };
    if level
        .iter()
        .any(|&i| keyword(tokens[i], "RIGHT") || keyword(tokens[i], "FULL"))
    {
        return None;
    }
    let mut where_at = None;
    let mut end = tokens.len();
    for &i in &after {
        if where_at.is_none() && keyword(tokens[i], "WHERE") {
            where_at = Some(i);
        } else if is_end(tokens[i]) {
            end = i;
            break;
        }
    }
    Some(match where_at {
        Some(where_index) => vec![
            (where_index + 1, where_index + 1, " (".to_string()),
            (end, end, format!(") AND {restriction} ")),
        ],
        None => vec![(end, end, format!(" WHERE {restriction} "))],
    })
}

#[derive(Clone, Copy)]
enum Target {
    Read,
    Update,
    Delete,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_becomes_a_tableoid_restriction() {
        assert_eq!(
            rewrite_only_relations("SELECT * FROM ONLY p WHERE a > 1 ORDER BY a").unwrap(),
            "SELECT * FROM p WHERE ( a > 1 ) AND p.tableoid = 'p'::regclass ORDER BY a"
        );
        assert_eq!(
            rewrite_only_relations("SELECT x.a FROM t JOIN ONLY s.p AS x ON true").unwrap(),
            "SELECT x.a FROM t JOIN s.p AS x ON ( true) AND x.tableoid = 's.p'::regclass "
        );
        assert_eq!(
            rewrite_only_relations("SELECT * FROM t FULL JOIN ONLY p USING (a)").unwrap(),
            "SELECT * FROM t FULL JOIN (SELECT * FROM p WHERE tableoid = 'p'::regclass) AS p USING (a)"
        );
        assert_eq!(
            rewrite_only_relations("SELECT (SELECT count(*) FROM ONLY p) FROM t").unwrap(),
            "SELECT (SELECT count(*) FROM p WHERE p.tableoid = 'p'::regclass ) FROM t"
        );
        assert_eq!(
            rewrite_only_relations("UPDATE ONLY p SET a = 1 WHERE a = 2 OR a = 3 RETURNING a").unwrap(),
            "UPDATE p SET a = 1 WHERE ( a = 2 OR a = 3 ) AND p.tableoid = 'p'::regclass RETURNING a"
        );
        assert_eq!(
            rewrite_only_relations("DELETE FROM ONLY p q").unwrap(),
            "DELETE FROM p q WHERE q.tableoid = 'p'::regclass "
        );
        assert_eq!(rewrite_only_relations("TRUNCATE ONLY p"), None);
        assert_eq!(
            rewrite_only_relations("ALTER TABLE ONLY p ADD COLUMN x int"),
            None
        );
        assert_eq!(rewrite_only_relations("SELECT 'FROM ONLY p' FROM t"), None);
    }
}
