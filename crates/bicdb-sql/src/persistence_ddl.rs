//! Token-level rewrites for PostgreSQL DDL the SQL parser does not accept:
//! `CREATE UNLOGGED TABLE`, `ALTER TABLE ... SET LOGGED | UNLOGGED`, and
//! generated columns that are VIRTUAL (written `VIRTUAL`, or with no storage
//! keyword, PostgreSQL 18's default). The rewrite works on tokens, so text in
//! string literals, quoted identifiers, comments and dollar quotes is never
//! touched.
use crate::*;
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};

#[derive(Debug, PartialEq)]
pub(crate) enum PersistenceDdl {
    /// A `CREATE TABLE` or `ALTER TABLE ... ADD COLUMN` rewritten into a form
    /// the parser accepts; the flags are applied to `table` afterwards.
    Rewritten {
        sql: String,
        table: String,
        create: bool,
        if_not_exists: bool,
        unlogged: bool,
        virtual_columns: Vec<String>,
        /// Expressions (as written) of CHECK constraints declared NO INHERIT.
        no_inherit_checks: Vec<String>,
        /// Columns whose NOT NULL is declared NO INHERIT.
        no_inherit_not_null: Vec<String>,
    },
    /// `ALTER TABLE name INHERIT parent` (`true`) or `NO INHERIT parent`,
    /// one or more comma-separated actions applied in order.
    Inherit {
        table: String,
        if_exists: bool,
        parents: Vec<(String, bool)>,
    },
    /// `ALTER TABLE name SET LOGGED` (`false`) or `SET UNLOGGED` (`true`).
    SetPersistence {
        table: String,
        if_exists: bool,
        unlogged: bool,
    },
}

fn is_keyword(token: &Token, keyword: &str) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword))
}

fn identifier(token: &Token) -> Option<String> {
    match token {
        Token::Word(word) if word.quote_style.is_some() => Some(word.value.clone()),
        Token::Word(word) => Some(word.value.to_ascii_lowercase()),
        _ => None,
    }
}

/// Reads a possibly qualified relation name starting at significant index
/// `at`; returns it and the next significant index.
fn relation_at(tokens: &[Token], significant: &[usize], at: usize) -> Option<(String, usize)> {
    let mut parts = vec![identifier(&tokens[*significant.get(at)?])?];
    let mut next = at + 1;
    while significant
        .get(next)
        .is_some_and(|&index| tokens[index] == Token::Period)
    {
        parts.push(identifier(&tokens[*significant.get(next + 1)?])?);
        next += 2;
    }
    Some((relation_name_from_parts(&parts).ok()?, next))
}

pub(crate) fn parse_persistence_ddl(sql: &str) -> Result<Option<PersistenceDdl>> {
    let head = sql.trim_start();
    if !(head.len() >= 6
        && (head[..6].eq_ignore_ascii_case("CREATE") || head[..5].eq_ignore_ascii_case("ALTER")))
    {
        return Ok(None);
    }
    let lower = sql.to_ascii_lowercase();
    if !(lower.contains("logged") || lower.contains("generated") || lower.contains("inherit")) {
        return Ok(None);
    }
    let Ok(mut tokens) = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, sql).tokenize()
    else {
        return Ok(None);
    };
    while matches!(
        tokens.last(),
        Some(Token::Whitespace(_)) | Some(Token::SemiColon)
    ) {
        tokens.pop();
    }
    let significant = |tokens: &[Token]| {
        tokens
            .iter()
            .enumerate()
            .filter(|(_, token)| !matches!(token, Token::Whitespace(_)))
            .map(|(index, _)| index)
            .collect::<Vec<_>>()
    };
    let sig = significant(&tokens);
    if sig.iter().any(|&index| tokens[index] == Token::SemiColon) {
        return Ok(None);
    }
    let at = |position: usize| sig.get(position).map(|&index| &tokens[index]);

    if at(0).is_some_and(|token| is_keyword(token, "ALTER"))
        && at(1).is_some_and(|token| is_keyword(token, "TABLE"))
    {
        let mut position = 2;
        let mut if_exists = false;
        if at(position).is_some_and(|token| is_keyword(token, "IF"))
            && at(position + 1).is_some_and(|token| is_keyword(token, "EXISTS"))
        {
            if_exists = true;
            position += 2;
        }
        if at(position).is_some_and(|token| is_keyword(token, "ONLY")) {
            position += 1;
        }
        let Some((table, next)) = relation_at(&tokens, &sig, position) else {
            return Ok(None);
        };
        if at(next).is_some_and(|token| is_keyword(token, "SET")) && sig.len() == next + 2 {
            if let Some(token) = at(next + 1) {
                if is_keyword(token, "LOGGED") || is_keyword(token, "UNLOGGED") {
                    return Ok(Some(PersistenceDdl::SetPersistence {
                        table,
                        if_exists,
                        unlogged: is_keyword(token, "UNLOGGED"),
                    }));
                }
            }
        }
        // One or more comma-separated `[NO] INHERIT parent` actions.
        let inherit_action = |position: usize| {
            if at(position).is_some_and(|token| is_keyword(token, "NO"))
                && at(position + 1).is_some_and(|token| is_keyword(token, "INHERIT"))
            {
                Some((position + 2, false))
            } else if at(position).is_some_and(|token| is_keyword(token, "INHERIT")) {
                Some((position + 1, true))
            } else {
                None
            }
        };
        if inherit_action(next).is_some() {
            let mut parents = Vec::new();
            let mut position = next;
            while let Some((parent_at, inherit)) = inherit_action(position) {
                let Some((parent, end)) = relation_at(&tokens, &sig, parent_at) else {
                    return Ok(None);
                };
                parents.push((parent, inherit));
                if end == sig.len() {
                    return Ok(Some(PersistenceDdl::Inherit {
                        table,
                        if_exists,
                        parents,
                    }));
                }
                if at(end) != Some(&Token::Comma) {
                    return Ok(None);
                }
                position = end + 1;
            }
            return Ok(None);
        }
        // ALTER TABLE name ADD [COLUMN] [IF NOT EXISTS] column ... GENERATED.
        if !at(next).is_some_and(|token| is_keyword(token, "ADD")) {
            return Ok(None);
        }
        let mut column_at = next + 1;
        if at(column_at).is_some_and(|token| is_keyword(token, "COLUMN")) {
            column_at += 1;
        }
        let mut if_not_exists = false;
        if at(column_at).is_some_and(|token| is_keyword(token, "IF")) {
            if_not_exists = true;
            column_at += 3;
        }
        let (no_inherit_checks, mut edits) = no_inherit_check_edits(&tokens, &sig, next + 1);
        let mut virtual_columns = Vec::new();
        if no_inherit_checks.is_empty() {
            let Some(column) = at(column_at).and_then(identifier) else {
                return Ok(None);
            };
            let Some(edit) = virtual_generated_edit(&tokens, &sig, column_at) else {
                return Ok(None);
            };
            edits.push(edit);
            virtual_columns.push(column);
        } else if !at(2).is_some_and(|token| is_keyword(token, "ONLY"))
            && !at(2).is_some_and(|token| is_keyword(token, "IF"))
        {
            // A NO INHERIT constraint is not added to the children.
            edits.push(Edit::InsertKeyword(sig[1], "ONLY"));
        }
        apply_edits(&mut tokens, edits);
        return Ok(Some(PersistenceDdl::Rewritten {
            no_inherit_checks,
            no_inherit_not_null: Vec::new(),
            sql: tokens
                .iter()
                .map(crate::eval::rewrite_helpers::token_sql)
                .collect(),
            table,
            create: false,
            if_not_exists,
            unlogged: false,
            virtual_columns,
        }));
    }

    if !at(0).is_some_and(|token| is_keyword(token, "CREATE")) {
        return Ok(None);
    }
    let mut position = 1;
    let unlogged = at(position).is_some_and(|token| is_keyword(token, "UNLOGGED"));
    if unlogged {
        position += 1;
    }
    if !at(position).is_some_and(|token| is_keyword(token, "TABLE")) {
        return Ok(None);
    }
    position += 1;
    let mut if_not_exists = false;
    if at(position).is_some_and(|token| is_keyword(token, "IF"))
        && at(position + 1).is_some_and(|token| is_keyword(token, "NOT"))
        && at(position + 2).is_some_and(|token| is_keyword(token, "EXISTS"))
    {
        if_not_exists = true;
        position += 3;
    }
    let Some((table, after_name)) = relation_at(&tokens, &sig, position) else {
        return Ok(None);
    };
    // Column definitions start after the first parenthesis; each element
    // starts after it or a comma at depth one.
    let mut virtual_columns = Vec::new();
    let mut no_inherit_not_null = Vec::new();
    let mut edits = Vec::new();
    if at(after_name) == Some(&Token::LParen) {
        let mut depth = 0usize;
        let mut element_start = None;
        let mut position = after_name;
        while position < sig.len() {
            match &tokens[sig[position]] {
                Token::LParen => {
                    depth += 1;
                    if depth == 1 {
                        element_start = Some(position + 1);
                    }
                }
                Token::RParen => {
                    if depth == 1 {
                        break;
                    }
                    depth = depth.saturating_sub(1);
                }
                Token::Comma if depth == 1 => element_start = Some(position + 1),
                token
                    if depth == 1
                        && is_keyword(token, "NOT")
                        && at(position + 1).is_some_and(|token| is_keyword(token, "NULL"))
                        && at(position + 2).is_some_and(|token| is_keyword(token, "NO"))
                        && at(position + 3).is_some_and(|token| is_keyword(token, "INHERIT")) =>
                {
                    if let Some(column) = element_start
                        .and_then(|start| at(start))
                        .and_then(identifier)
                    {
                        no_inherit_not_null.push(column);
                        edits.push(Edit::Remove(sig[position + 2]));
                        edits.push(Edit::Remove(sig[position + 3]));
                    }
                }
                token if depth == 1 && is_keyword(token, "GENERATED") => {
                    if let Some(column) = element_start
                        .and_then(|start| at(start))
                        .and_then(identifier)
                    {
                        if let Some(edit) = virtual_generated_edit(&tokens, &sig, position) {
                            edits.push(edit);
                            virtual_columns.push(column);
                        }
                    }
                }
                _ => {}
            }
            position += 1;
        }
    }
    let (no_inherit_checks, check_edits) = no_inherit_check_edits(&tokens, &sig, after_name);
    edits.extend(check_edits);
    if !unlogged && edits.is_empty() {
        return Ok(None);
    }
    if unlogged {
        // Drop the UNLOGGED word (and the whitespace after it).
        edits.push(Edit::Remove(sig[1]));
    }
    apply_edits(&mut tokens, edits);
    Ok(Some(PersistenceDdl::Rewritten {
        sql: tokens
            .iter()
            .map(crate::eval::rewrite_helpers::token_sql)
            .collect(),
        table,
        create: true,
        if_not_exists,
        unlogged,
        virtual_columns,
        no_inherit_checks,
        no_inherit_not_null,
    }))
}

enum Edit {
    /// Replace the token (VIRTUAL) with STORED.
    Replace(usize),
    /// Insert ` STORED` after the token (the expression's closing paren).
    InsertAfter(usize),
    /// Remove the token and a whitespace token after it.
    Remove(usize),
    /// Insert ` KEYWORD` after the token.
    InsertKeyword(usize, &'static str),
}

fn apply_edits(tokens: &mut Vec<Token>, mut edits: Vec<Edit>) {
    let index_of = |edit: &Edit| match edit {
        Edit::Replace(index)
        | Edit::InsertAfter(index)
        | Edit::Remove(index)
        | Edit::InsertKeyword(index, _) => *index,
    };
    edits.sort_by_key(|edit| std::cmp::Reverse(index_of(edit)));
    for edit in edits {
        match edit {
            Edit::Replace(index) => tokens[index] = Token::make_keyword("STORED"),
            Edit::InsertAfter(index) => {
                tokens.insert(index + 1, Token::make_keyword("STORED"));
                tokens.insert(index + 1, Token::Whitespace(Whitespace::Space));
            }
            Edit::InsertKeyword(index, keyword) => {
                tokens.insert(index + 1, Token::make_keyword(keyword));
                tokens.insert(index + 1, Token::Whitespace(Whitespace::Space));
            }
            Edit::Remove(index) => {
                tokens.remove(index);
                if matches!(tokens.get(index), Some(Token::Whitespace(_))) {
                    tokens.remove(index);
                }
            }
        }
    }
}

/// Every `CHECK (expr) NO INHERIT` from significant position `from` on: the
/// expressions as written, and the edits dropping the `NO INHERIT` words.
fn no_inherit_check_edits(
    tokens: &[Token],
    sig: &[usize],
    from: usize,
) -> (Vec<String>, Vec<Edit>) {
    let mut expressions = Vec::new();
    let mut edits = Vec::new();
    let keyword = |position: usize, value: &str| {
        sig.get(position)
            .is_some_and(|&index| is_keyword(&tokens[index], value))
    };
    let mut position = from;
    while position < sig.len() {
        if keyword(position, "CHECK")
            && sig.get(position + 1).map(|&index| &tokens[index]) == Some(&Token::LParen)
        {
            let mut depth = 0usize;
            let mut close = position + 1;
            while close < sig.len() {
                match tokens[sig[close]] {
                    Token::LParen => depth += 1,
                    Token::RParen => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                close += 1;
            }
            if close < sig.len() && keyword(close + 1, "NO") && keyword(close + 2, "INHERIT") {
                expressions.push(
                    tokens[sig[position + 1] + 1..sig[close]]
                        .iter()
                        .map(crate::eval::rewrite_helpers::token_sql)
                        .collect::<String>(),
                );
                edits.push(Edit::Remove(sig[close + 1]));
                edits.push(Edit::Remove(sig[close + 2]));
            }
            position = close;
        }
        position += 1;
    }
    (expressions, edits)
}

/// From significant position `from`, finds the first `GENERATED ALWAYS AS
/// (expr)` before the column definition ends; when it is VIRTUAL (written,
/// or by default) returns the edit that makes it STORED.
fn virtual_generated_edit(tokens: &[Token], sig: &[usize], from: usize) -> Option<Edit> {
    let mut position = from;
    let mut depth = 0usize;
    while position < sig.len() {
        let token = &tokens[sig[position]];
        match token {
            Token::LParen => depth += 1,
            Token::RParen if depth == 0 => return None,
            Token::RParen => depth -= 1,
            Token::Comma if depth == 0 => return None,
            _ if depth == 0 && is_keyword(token, "GENERATED") => break,
            _ => {}
        }
        position += 1;
    }
    let keyword = |position: usize, value: &str| {
        sig.get(position)
            .is_some_and(|&index| is_keyword(&tokens[index], value))
    };
    if !(keyword(position + 1, "ALWAYS") && keyword(position + 2, "AS"))
        || sig.get(position + 3).map(|&index| &tokens[index]) != Some(&Token::LParen)
    {
        return None;
    }
    let mut depth = 0usize;
    let mut close = position + 3;
    while close < sig.len() {
        match tokens[sig[close]] {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
        close += 1;
    }
    if close >= sig.len() || keyword(close + 1, "STORED") {
        return None;
    }
    Some(if keyword(close + 1, "VIRTUAL") {
        Edit::Replace(sig[close + 1])
    } else {
        Edit::InsertAfter(sig[close])
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewritten(sql: &str) -> (String, bool, Vec<String>) {
        match parse_persistence_ddl(sql).unwrap().unwrap() {
            PersistenceDdl::Rewritten {
                sql,
                unlogged,
                virtual_columns,
                ..
            } => (sql, unlogged, virtual_columns),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rewrites_only_the_ddl_tokens() {
        let (sql, unlogged, columns) = rewritten(
            "CREATE UNLOGGED TABLE t(a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL, c text DEFAULT 'VIRTUAL', d int GENERATED ALWAYS AS (a + 1), e int GENERATED ALWAYS AS (a) STORED, f int GENERATED ALWAYS AS IDENTITY)",
        );
        assert!(unlogged);
        assert_eq!(columns, vec!["b".to_string(), "d".to_string()]);
        assert_eq!(
            sql,
            "CREATE TABLE t(a int, b int GENERATED ALWAYS AS (a * 2) STORED, c text DEFAULT 'VIRTUAL', d int GENERATED ALWAYS AS (a + 1) STORED, e int GENERATED ALWAYS AS (a) STORED, f int GENERATED ALWAYS AS IDENTITY)"
        );
        let (sql, _, columns) =
            rewritten("ALTER TABLE s.t ADD COLUMN \"Big\" int GENERATED ALWAYS AS (a * 2)");
        assert_eq!(columns, vec!["Big".to_string()]);
        assert!(sql.ends_with("AS (a * 2) STORED"));
        assert_eq!(
            parse_persistence_ddl("ALTER TABLE t SET UNLOGGED").unwrap(),
            Some(PersistenceDdl::SetPersistence {
                table: "t".into(),
                if_exists: false,
                unlogged: true
            })
        );
        assert_eq!(
            parse_persistence_ddl("CREATE TABLE t(a text DEFAULT 'unlogged generated')").unwrap(),
            None
        );
        // Quotes inside literals and identifiers survive the rewrite.
        let (sql, _, _) =
            rewritten("CREATE UNLOGGED TABLE \"q\"\"t\"(a text DEFAULT 'it''s', b int GENERATED ALWAYS AS (1))");
        assert_eq!(
            sql,
            "CREATE TABLE \"q\"\"t\"(a text DEFAULT 'it''s', b int GENERATED ALWAYS AS (1) STORED)"
        );
        assert_eq!(
            parse_persistence_ddl("CREATE TABLE t(a int GENERATED ALWAYS AS (1) STORED)").unwrap(),
            None
        );
    }
}
