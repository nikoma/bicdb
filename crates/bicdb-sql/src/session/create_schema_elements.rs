//! `CREATE SCHEMA ... schema_element ...`: a schema created together with
//! tables, views, sequences, indexes, triggers and grants inside it.
//!
//! As in PostgreSQL the elements run after the schema exists, as the
//! schema's owner (the AUTHORIZATION role), with the new schema first in
//! `search_path`, in the order sequences, tables, views, indexes, triggers,
//! grants. An element naming another schema is an error, and the statement
//! is atomic: an element that fails undoes the schema and every element.
//! `AUTHORIZATION CURRENT_ROLE | CURRENT_USER | SESSION_USER` names the role
//! those keywords denote.
use super::*;
use sqlparser::tokenizer::{Token, Tokenizer};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ElementKind {
    Sequence,
    Table,
    View,
    Index,
    Trigger,
    Grant,
}

#[derive(Debug, PartialEq)]
struct Element {
    kind: ElementKind,
    /// The element's text, split where an unqualified relation name starts:
    /// the schema is written between consecutive segments.
    segments: Vec<String>,
    /// The schema the element's relation is qualified with, if any.
    qualifier: Option<String>,
}

#[derive(Debug, PartialEq)]
enum RoleSpec {
    Named(String),
    CurrentRole,
    SessionUser,
}

#[derive(Debug, PartialEq)]
struct CreateSchemaWithElements {
    if_not_exists: bool,
    name: Option<String>,
    role: Option<RoleSpec>,
    elements: Vec<Element>,
}

fn keyword(token: Option<&&Token>, word: &str) -> bool {
    matches!(token, Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

fn ident(token: Option<&&Token>) -> Option<String> {
    match token? {
        Token::Word(word) if word.quote_style == Some('"') => Some(word.value.clone()),
        Token::Word(word) if word.quote_style.is_none() => Some(word.value.to_ascii_lowercase()),
        _ => None,
    }
}

/// The schema qualifier of a (possibly qualified) name at `at`, and the
/// position after the name.
fn qualifier_at(tokens: &[&Token], at: usize) -> Option<(Option<String>, usize)> {
    let mut parts = vec![ident(tokens.get(at))?];
    let mut next = at + 1;
    while tokens.get(next) == Some(&&Token::Period) {
        parts.push(ident(tokens.get(next + 1))?);
        next += 2;
    }
    let qualifier = match parts.len() {
        1 => None,
        // database.schema.name: the schema is the middle part.
        _ => Some(parts[parts.len() - 2].clone()),
    };
    Some((qualifier, next))
}

/// Classifies one element and finds the relation whose schema PostgreSQL
/// checks (the table of an index or trigger).
fn classify(tokens: &[&Token]) -> Result<(ElementKind, Option<String>, Vec<usize>)> {
    let unsupported = || {
        SqlError::data_exception_public(
            "42601",
            format!(
                "syntax error at or near \"{}\"",
                tokens
                    .get(1)
                    .map(|token| crate::eval::rewrite_helpers::token_sql(token))
                    .unwrap_or_default()
            ),
            None,
        )
    };
    if keyword(tokens.first(), "GRANT") {
        // GRANT ... ON [TABLE | SEQUENCE] name [, ...] TO: those names are
        // looked up in the new schema.
        let mut unqualified = Vec::new();
        if let Some(on) = (1..tokens.len()).find(|&index| keyword(tokens.get(index), "ON")) {
            let mut at = on + 1;
            if keyword(tokens.get(at), "TABLE") || keyword(tokens.get(at), "SEQUENCE") {
                at += 1;
            }
            let plain_relations = ![
                "ALL",
                "FUNCTION",
                "FUNCTIONS",
                "PROCEDURE",
                "ROUTINE",
                "SCHEMA",
                "DATABASE",
                "TYPE",
                "DOMAIN",
                "LANGUAGE",
                "FOREIGN",
                "LARGE",
                "TABLESPACE",
                "PARAMETER",
            ]
            .iter()
            .any(|word| keyword(tokens.get(at), word));
            while plain_relations && at < tokens.len() && !keyword(tokens.get(at), "TO") {
                let Some((qualifier, next)) = qualifier_at(tokens, at) else {
                    break;
                };
                if qualifier.is_none() {
                    unqualified.push(at);
                }
                at = next;
                if tokens.get(at) == Some(&&Token::Comma) {
                    at += 1;
                }
            }
        }
        return Ok((ElementKind::Grant, None, unqualified));
    }
    let mut at = 1;
    if keyword(tokens.get(at), "OR") && keyword(tokens.get(at + 1), "REPLACE") {
        at += 2;
    }
    if keyword(tokens.get(at), "TEMP") || keyword(tokens.get(at), "TEMPORARY") {
        return Err(SqlError::data_exception_public(
            "42P16",
            "cannot create temporary relation in non-temporary schema",
            None,
        ));
    }
    if [
        "TEMP",
        "TEMPORARY",
        "UNLOGGED",
        "RECURSIVE",
        "UNIQUE",
        "CONSTRAINT",
    ]
    .iter()
    .any(|word| keyword(tokens.get(at), word))
    {
        at += 1;
    }
    let kind = if keyword(tokens.get(at), "TABLE") {
        ElementKind::Table
    } else if keyword(tokens.get(at), "VIEW") {
        ElementKind::View
    } else if keyword(tokens.get(at), "SEQUENCE") {
        ElementKind::Sequence
    } else if keyword(tokens.get(at), "INDEX") {
        ElementKind::Index
    } else if keyword(tokens.get(at), "TRIGGER") {
        ElementKind::Trigger
    } else {
        return Err(unsupported());
    };
    at += 1;
    let qualifier = match kind {
        ElementKind::Table | ElementKind::View | ElementKind::Sequence => {
            if keyword(tokens.get(at), "IF") {
                at += 3;
            }
            let qualifier = qualifier_at(tokens, at).ok_or_else(unsupported)?.0;
            (
                qualifier.clone(),
                qualifier.is_none().then_some(at).into_iter().collect(),
            )
        }
        ElementKind::Index | ElementKind::Trigger => {
            let on = (at..tokens.len())
                .find(|&index| keyword(tokens.get(index), "ON"))
                .ok_or_else(unsupported)?;
            let mut table_at = on + 1;
            if keyword(tokens.get(table_at), "ONLY") {
                table_at += 1;
            }
            let qualifier = qualifier_at(tokens, table_at).ok_or_else(unsupported)?.0;
            (
                qualifier.clone(),
                qualifier
                    .is_none()
                    .then_some(table_at)
                    .into_iter()
                    .collect(),
            )
        }
        ElementKind::Grant => (None, Vec::new()),
    };
    let (qualifier, unqualified_at) = qualifier;
    Ok((kind, qualifier, unqualified_at))
}

fn parse(sql: &str) -> Result<Option<CreateSchemaWithElements>> {
    let head = sql.trim_start();
    if !(head.len() >= 6 && head[..6].eq_ignore_ascii_case("CREATE")) {
        return Ok(None);
    }
    if !sql.to_ascii_lowercase().contains("schema") {
        return Ok(None);
    }
    let Ok(raw) = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, sql).tokenize() else {
        return Ok(None);
    };
    let mut tokens: Vec<&Token> = raw
        .iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();
    while tokens.last() == Some(&&Token::SemiColon) {
        tokens.pop();
    }
    if tokens.contains(&&Token::SemiColon)
        || !keyword(tokens.first(), "CREATE")
        || !keyword(tokens.get(1), "SCHEMA")
    {
        return Ok(None);
    }
    let mut at = 2;
    let mut if_not_exists = false;
    if keyword(tokens.get(at), "IF")
        && keyword(tokens.get(at + 1), "NOT")
        && keyword(tokens.get(at + 2), "EXISTS")
    {
        if_not_exists = true;
        at += 3;
    }
    let mut name = None;
    if !keyword(tokens.get(at), "AUTHORIZATION") {
        name = Some(ident(tokens.get(at)).ok_or_else(|| {
            SqlError::data_exception_public("42601", "syntax error in CREATE SCHEMA", None)
        })?);
        at += 1;
    }
    let mut role = None;
    if keyword(tokens.get(at), "AUTHORIZATION") {
        let token = tokens.get(at + 1);
        role = Some(
            if keyword(token, "CURRENT_ROLE") || keyword(token, "CURRENT_USER") {
                RoleSpec::CurrentRole
            } else if keyword(token, "SESSION_USER") {
                RoleSpec::SessionUser
            } else {
                RoleSpec::Named(ident(token).ok_or_else(|| {
                    SqlError::data_exception_public("42601", "syntax error in CREATE SCHEMA", None)
                })?)
            },
        );
        at += 2;
    }
    // Elements start at a CREATE or GRANT at depth zero (not the GRANT of
    // `WITH GRANT OPTION`).
    let mut starts = Vec::new();
    let mut depth = 0i64;
    for index in at..tokens.len() {
        match tokens[index] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ if depth == 0
                && (keyword(tokens.get(index), "CREATE")
                    || (keyword(tokens.get(index), "GRANT")
                        && !keyword(tokens.get(index.wrapping_sub(1)), "WITH"))) =>
            {
                starts.push(index)
            }
            _ => {}
        }
    }
    if starts.first().is_some_and(|&first| first != at) || (starts.is_empty() && at != tokens.len())
    {
        return Ok(None);
    }
    let special_role = matches!(role, Some(RoleSpec::CurrentRole | RoleSpec::SessionUser));
    if starts.is_empty() && !special_role {
        return Ok(None);
    }
    if !starts.is_empty() && if_not_exists {
        return Err(SqlError::Unsupported(
            "CREATE SCHEMA IF NOT EXISTS cannot include schema elements".to_string(),
        ));
    }
    let mut elements = Vec::new();
    for (position, &start) in starts.iter().enumerate() {
        let end = starts.get(position + 1).copied().unwrap_or(tokens.len());
        let element = &tokens[start..end];
        let (kind, qualifier, unqualified_at) = classify(element)?;
        // Rebuild the element's text from the original tokens.
        let first = raw
            .iter()
            .position(|token| std::ptr::eq(token, element[0]))
            .unwrap_or(0);
        let last = raw
            .iter()
            .position(|token| std::ptr::eq(token, *element.last().unwrap()))
            .unwrap_or(raw.len() - 1);
        // The created relation (or an index's or trigger's table, or a
        // granted relation) is qualified with the new schema, as PostgreSQL
        // does.
        let splits = unqualified_at
            .iter()
            .filter_map(|&at| {
                raw.iter()
                    .position(|token| std::ptr::eq(token, element[at]))
            })
            .collect::<Vec<_>>();
        let text = |range: std::ops::Range<usize>| {
            raw[range]
                .iter()
                .map(crate::eval::rewrite_helpers::token_sql)
                .collect::<String>()
        };
        let mut segments = Vec::new();
        let mut from = first;
        for split in splits {
            segments.push(text(from..split));
            from = split;
        }
        segments.push(text(from..last + 1));
        elements.push(Element {
            kind,
            segments,
            qualifier,
        });
    }
    Ok(Some(CreateSchemaWithElements {
        if_not_exists,
        name,
        role,
        elements,
    }))
}

/// The schema name as written before a relation name.
fn qualifier_sql(schema: &str) -> String {
    let simple = schema
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && schema
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if simple {
        schema.to_string()
    } else {
        super::not_null_constraints::quote(schema)
    }
}

impl<'a> SqlSession<'a> {
    pub(crate) fn execute_raw_create_schema_elements(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(statement) = parse(sql)? else {
            return Ok(None);
        };
        let current = current_user_from_gucs(&self.session_gucs);
        let role = statement.role.as_ref().map(|role| match role {
            RoleSpec::Named(name) => normalize_role_name(name),
            RoleSpec::CurrentRole => current.clone(),
            RoleSpec::SessionUser => session_user_from_gucs(&self.session_gucs),
        });
        let schema = statement
            .name
            .clone()
            .or_else(|| role.clone())
            .unwrap_or_default();
        for element in &statement.elements {
            if let Some(qualifier) = element.qualifier.as_ref().filter(|q| **q != schema) {
                return Err(SqlError::data_exception_public(
                    "42P15",
                    format!(
                        "CREATE specifies a schema ({qualifier}) different from the one being created ({schema})"
                    ),
                    None,
                ));
            }
        }
        let quote = super::not_null_constraints::quote;
        let mut header = String::from("CREATE SCHEMA ");
        if statement.if_not_exists {
            header.push_str("IF NOT EXISTS ");
        }
        if let Some(name) = &statement.name {
            header.push_str(&quote(name));
            header.push(' ');
        }
        if let Some(role) = &role {
            header.push_str("AUTHORIZATION ");
            header.push_str(&quote(role));
        }
        if statement.elements.is_empty() {
            return self.execute_inner(&header).map(Some);
        }
        let implicit = self.tx.is_none();
        if implicit {
            self.execute_inner("BEGIN")?;
        }
        let outcome = self.create_schema_and_elements(&header, &schema, role, statement.elements);
        if implicit {
            match &outcome {
                Ok(_) => {
                    self.execute_inner("COMMIT")?;
                }
                Err(_) => {
                    let _ = self.execute_inner("ROLLBACK");
                }
            }
        }
        outcome.map(Some)
    }

    fn create_schema_and_elements(
        &mut self,
        header: &str,
        schema: &str,
        owner: Option<String>,
        mut elements: Vec<Element>,
    ) -> Result<SqlResult> {
        let result = self.execute_inner(header)?;
        elements.sort_by_key(|element| element.kind);
        // Run the elements as the schema's owner, with the schema first in
        // search_path; both are restored whatever happens.
        let gucs = Arc::make_mut(&mut self.session_gucs);
        let previous_role = owner.map(|owner| gucs.insert(CURRENT_ROLE_GUC.to_string(), owner));
        let previous_path = gucs.get("search_path").cloned();
        let path = match &previous_path {
            Some(path) if !path.trim().is_empty() => format!("{}, {path}", qualifier_sql(schema)),
            _ => format!("{}, \"$user\", public", qualifier_sql(schema)),
        };
        gucs.insert("search_path".to_string(), path);
        let outcome = elements.iter().try_for_each(|element| {
            let sql = element
                .segments
                .join(&format!("{}.", qualifier_sql(schema)));
            self.execute_inner(&sql).map(|_| ())
        });
        let gucs = Arc::make_mut(&mut self.session_gucs);
        match previous_path {
            Some(path) => {
                gucs.insert("search_path".to_string(), path);
            }
            None => {
                gucs.remove("search_path");
            }
        }
        match previous_role {
            Some(Some(role)) => {
                gucs.insert(CURRENT_ROLE_GUC.to_string(), role);
            }
            Some(None) => {
                gucs.remove(CURRENT_ROLE_GUC);
            }
            None => {}
        }
        outcome.map(|()| result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_schema_elements() {
        let parsed = parse(
            "CREATE SCHEMA s CREATE VIEW v AS SELECT a FROM t CREATE TABLE t (a int) \
             GRANT SELECT ON t TO r WITH GRANT OPTION CREATE INDEX ON s.t (a)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.name.as_deref(), Some("s"));
        let kinds = parsed
            .elements
            .iter()
            .map(|element| (element.kind, element.qualifier.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                (ElementKind::View, None),
                (ElementKind::Table, None),
                (ElementKind::Grant, None),
                (ElementKind::Index, Some("s".into())),
            ]
        );
        assert_eq!(
            parsed.elements[2].segments,
            vec!["GRANT SELECT ON ", "t TO r WITH GRANT OPTION"]
        );
        assert_eq!(
            parsed.elements[1].segments,
            vec!["CREATE TABLE ", "t (a int)"]
        );
        assert_eq!(parsed.elements[3].segments, vec!["CREATE INDEX ON s.t (a)"]);
        assert_eq!(parse("CREATE SCHEMA s").unwrap(), None);
        assert_eq!(parse("CREATE SCHEMA s AUTHORIZATION r").unwrap(), None);
        assert!(parse("CREATE SCHEMA AUTHORIZATION CURRENT_ROLE")
            .unwrap()
            .is_some());
        assert!(parse("CREATE SCHEMA IF NOT EXISTS s CREATE TABLE t (a int)").is_err());
    }
}
