//! View options (`pg_class.reloptions` of a view): `security_barrier`,
//! `security_invoker` and `check_option`, set by `CREATE VIEW ... WITH (...)`
//! and changed by `ALTER VIEW ... SET (...)` / `RESET (...)`.
//!
//! The options have their PostgreSQL meaning: a `security_invoker` view
//! reads its relations with the privileges and row security of the querying
//! role, a `security_barrier` view never has outside predicates pushed into
//! it, and `check_option` checks rows written through the view. An option
//! written without a value is `true`, as in PostgreSQL.
use super::*;
use sqlparser::tokenizer::{Token, Tokenizer};

pub(crate) enum ViewOption {
    SecurityBarrier(bool),
    SecurityInvoker(bool),
    CheckOption(super::updatable_views::CheckOption),
}

/// The option's name and its value as PostgreSQL stores it.
pub(crate) fn option_text(key: &Ident, value: &Expr) -> (String, String) {
    let text = match value {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text),
            ..
        }) => text.clone(),
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(value),
            ..
        }) => value.to_string(),
        Expr::Identifier(ident) => ident.value.clone(),
        other => other.to_string(),
    };
    (key.value.to_ascii_lowercase(), text)
}

/// PostgreSQL's `parse_bool`: a prefix of true/false/yes/no, on/off, 1/0.
fn parse_bool(text: &str) -> Option<bool> {
    let lower = text.trim().to_ascii_lowercase();
    if lower.is_empty() {
        return None;
    }
    let prefix_of = |word: &str| word.starts_with(lower.as_str());
    match lower.as_str() {
        "1" | "on" => Some(true),
        "0" | "off" | "of" => Some(false),
        _ if prefix_of("true") || prefix_of("yes") => Some(true),
        _ if prefix_of("false") || prefix_of("no") => Some(false),
        _ => None,
    }
}

pub(crate) fn apply_view_option(name: &str, text: &str) -> Result<ViewOption> {
    let boolean = |name: &str| {
        parse_bool(text).ok_or_else(|| {
            SqlError::invalid_parameter_value(format!(
                "invalid value for boolean option \"{name}\": {text}"
            ))
        })
    };
    match name {
        "security_barrier" => boolean(name).map(ViewOption::SecurityBarrier),
        "security_invoker" => boolean(name).map(ViewOption::SecurityInvoker),
        "check_option" => super::updatable_views::CheckOption::parse(text)
            .map(ViewOption::CheckOption)
            .ok_or_else(|| {
                SqlError::with_detail(
                    SqlError::invalid_parameter_value(format!(
                        "invalid value for enum option \"check_option\": {text}"
                    )),
                    "Valid values are \"local\" and \"cascaded\".",
                )
            }),
        other => Err(SqlError::invalid_parameter_value(format!(
            "unrecognized parameter \"{other}\""
        ))),
    }
}

/// Sets `name=text` in place, or appends it.
pub(crate) fn set_reloption(reloptions: &mut Vec<String>, name: &str, text: &str) {
    let entry = format!("{name}={text}");
    match reloptions
        .iter_mut()
        .find(|existing| existing.split('=').next() == Some(name))
    {
        Some(existing) => *existing = entry,
        None => reloptions.push(entry),
    }
}

/// `pg_class.reloptions` of a view, as PostgreSQL prints a `text[]`.
pub(crate) fn view_reloptions_value(view: &ViewSchema) -> SqlValue {
    let mut options = view.reloptions.clone();
    if options.is_empty() {
        // Views stored before reloptions were recorded.
        if view.security_barrier {
            options.push("security_barrier=true".to_string());
        }
        if view.security_invoker {
            options.push("security_invoker=true".to_string());
        }
        if let Some(check) = &view.check_option {
            options.push(format!("check_option={check}"));
        }
    }
    if options.is_empty() {
        return SqlValue::Null;
    }
    let quoted = options
        .iter()
        .map(|option| {
            if option
                .chars()
                .any(|c| c.is_whitespace() || matches!(c, ',' | '{' | '}' | '"' | '\\'))
            {
                format!("\"{}\"", option.replace('\\', "\\\\").replace('"', "\\\""))
            } else {
                option.clone()
            }
        })
        .collect::<Vec<_>>();
    SqlValue::String(format!("{{{}}}", quoted.join(",")))
}

fn keyword(token: Option<&&Token>, word: &str) -> bool {
    matches!(token, Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

/// Option names at the top level of a parenthesized list starting at `open`
/// that have no `= value`; returns their positions (in `tokens`) and the
/// position of the closing parenthesis.
fn bare_options(tokens: &[&Token], open: usize) -> Option<(Vec<usize>, usize)> {
    if tokens.get(open) != Some(&&Token::LParen) {
        return None;
    }
    let mut bare = Vec::new();
    let mut depth = 0;
    let mut element_start = open + 1;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen | Token::Comma if depth == 1 => {
                if index == element_start + 1 {
                    bare.push(element_start);
                }
                if **token == Token::RParen {
                    return Some((bare, index));
                }
                element_start = index + 1;
            }
            Token::RParen => depth -= 1,
            _ => {}
        }
    }
    None
}

#[derive(Debug, PartialEq)]
enum ViewOptionsDdl {
    /// A CREATE VIEW whose bare options were given `= true`.
    Create(String),
    Alter {
        view: Vec<String>,
        if_exists: bool,
        /// `(name, Some(text))` to set, `(name, None)` to reset.
        changes: Vec<(String, Option<String>)>,
    },
}

fn parse(sql: &str) -> Option<ViewOptionsDdl> {
    let head = sql.trim_start();
    if !(head.len() >= 6
        && (head[..6].eq_ignore_ascii_case("CREATE") || head[..5].eq_ignore_ascii_case("ALTER")))
    {
        return None;
    }
    if !sql.to_ascii_lowercase().contains("view") {
        return None;
    }
    let raw = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, sql)
        .tokenize()
        .ok()?;
    let significant: Vec<(usize, &Token)> = raw
        .iter()
        .enumerate()
        .filter(|(_, token)| !matches!(token, Token::Whitespace(_)))
        .collect();
    let tokens: Vec<&Token> = significant.iter().map(|(_, token)| *token).collect();
    if keyword(tokens.first(), "CREATE") {
        let mut at = 1;
        if keyword(tokens.get(at), "OR") && keyword(tokens.get(at + 1), "REPLACE") {
            at += 2;
        }
        while ["TEMP", "TEMPORARY", "RECURSIVE"]
            .iter()
            .any(|word| keyword(tokens.get(at), word))
        {
            at += 1;
        }
        if !keyword(tokens.get(at), "VIEW") {
            return None;
        }
        // The first WITH at depth zero before AS opens the options.
        let mut depth = 0;
        let with = (at..tokens.len()).find(|&index| {
            match tokens[index] {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            depth == 0 && (keyword(tokens.get(index), "WITH") || keyword(tokens.get(index), "AS"))
        })?;
        if !keyword(tokens.get(with), "WITH") {
            return None;
        }
        let (bare, _) = bare_options(&tokens, with + 1)?;
        if bare.is_empty() {
            return None;
        }
        let bare_raw = bare
            .iter()
            .map(|&index| significant[index].0)
            .collect::<Vec<_>>();
        let sql = raw
            .iter()
            .enumerate()
            .map(|(index, token)| {
                let text = crate::eval::rewrite_helpers::token_sql(token);
                if bare_raw.contains(&index) {
                    format!("{text} = true")
                } else {
                    text
                }
            })
            .collect::<String>();
        return Some(ViewOptionsDdl::Create(sql));
    }
    if !(keyword(tokens.first(), "ALTER") && keyword(tokens.get(1), "VIEW")) {
        return None;
    }
    let mut at = 2;
    let mut if_exists = false;
    if keyword(tokens.get(at), "IF") && keyword(tokens.get(at + 1), "EXISTS") {
        if_exists = true;
        at += 2;
    }
    let mut view = Vec::new();
    loop {
        let Token::Word(word) = tokens.get(at)? else {
            return None;
        };
        view.push(if word.quote_style.is_some() {
            word.value.clone()
        } else {
            word.value.to_ascii_lowercase()
        });
        at += 1;
        if tokens.get(at) == Some(&&Token::Period) {
            at += 1;
        } else {
            break;
        }
    }
    let reset = keyword(tokens.get(at), "RESET");
    if !reset && !keyword(tokens.get(at), "SET") {
        return None;
    }
    let (_, close) = bare_options(&tokens, at + 1)?;
    if tokens[close + 1..]
        .iter()
        .any(|token| **token != Token::SemiColon)
    {
        return None;
    }
    let mut changes = Vec::new();
    let mut index = at + 2;
    while index < close {
        let Token::Word(word) = tokens[index] else {
            return None;
        };
        let name = if word.quote_style.is_some() {
            word.value.clone()
        } else {
            word.value.to_ascii_lowercase()
        };
        index += 1;
        let value = if tokens.get(index) == Some(&&Token::Eq) {
            let text = match tokens.get(index + 1)? {
                Token::SingleQuotedString(text) => text.clone(),
                Token::Word(word) => word.value.clone(),
                Token::Number(number, _) => number.clone(),
                _ => return None,
            };
            index += 2;
            Some(text)
        } else {
            (!reset).then(|| "true".to_string())
        };
        if reset && value.is_some() {
            return None;
        }
        changes.push((name, value));
        if tokens.get(index) == Some(&&Token::Comma) {
            index += 1;
        }
    }
    Some(ViewOptionsDdl::Alter {
        view,
        if_exists,
        changes,
    })
}

impl<'a> SqlSession<'a> {
    pub(crate) fn execute_raw_view_options(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        if sql.trim_end().trim_end_matches(';').contains(';') {
            return Ok(None);
        }
        match parse(sql) {
            None => Ok(None),
            Some(ViewOptionsDdl::Create(rewritten)) => self.execute_inner(&rewritten).map(Some),
            Some(ViewOptionsDdl::Alter {
                view,
                if_exists,
                changes,
            }) => self.alter_view_options(&view, if_exists, changes).map(Some),
        }
    }

    fn alter_view_options(
        &mut self,
        view: &[String],
        if_exists: bool,
        changes: Vec<(String, Option<String>)>,
    ) -> Result<SqlResult> {
        let name = crate::eval::rewrite_helpers::relation_name_from_parts(view)?;
        let resolved = resolve_session_relation_name_if_exists(self.db_ref(), &name);
        let Some(mut schema) = load_view(self.db_ref(), &resolved)? else {
            if if_exists {
                crate::statement_context::notice_skipping_missing(
                    "relation",
                    &relation_name_for_message(&name),
                );
                return Ok(SqlResult::command("ALTER VIEW"));
            }
            return Err(SqlError::data_exception_public(
                "42P01",
                format!(
                    "relation \"{}\" does not exist",
                    relation_name_for_message(&name)
                ),
                None,
            ));
        };
        let owner = schema.owner.clone().unwrap_or_else(current_role_name);
        if !self.current_user_is_superuser()? && !self.current_user_holds_role(&owner)? {
            return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                "must be owner of view {}",
                relation_name_for_message(&schema.name)
            ))));
        }
        if schema.materialized {
            return Err(SqlError::Unsupported(
                "storage parameters of materialized views are not supported".to_string(),
            ));
        }
        // Start from what the catalog shows, so older views keep theirs.
        let mut reloptions = schema.reloptions.clone();
        if reloptions.is_empty() {
            if schema.security_barrier {
                reloptions.push("security_barrier=true".to_string());
            }
            if schema.security_invoker {
                reloptions.push("security_invoker=true".to_string());
            }
            if let Some(check) = &schema.check_option {
                reloptions.push(format!("check_option={check}"));
            }
        }
        for (option, value) in changes {
            match value {
                Some(text) => {
                    match apply_view_option(&option, &text)? {
                        ViewOption::SecurityBarrier(enabled) => schema.security_barrier = enabled,
                        ViewOption::SecurityInvoker(enabled) => schema.security_invoker = enabled,
                        ViewOption::CheckOption(check) => {
                            schema.check_option = Some(check.as_str().to_string())
                        }
                    }
                    set_reloption(&mut reloptions, &option, &text);
                }
                None => {
                    match option.as_str() {
                        "security_barrier" => schema.security_barrier = false,
                        "security_invoker" => schema.security_invoker = false,
                        "check_option" => schema.check_option = None,
                        _ => {}
                    }
                    reloptions.retain(|existing| existing.split('=').next() != Some(&option));
                }
            }
        }
        schema.reloptions = reloptions;
        self.validate_view_check_option(&schema)?;
        self.save_session_view(&schema)?;
        Ok(SqlResult::command("ALTER VIEW"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_view_options_become_true() {
        assert_eq!(
            parse("CREATE VIEW v WITH (security_barrier, check_option = local) AS SELECT 1"),
            Some(ViewOptionsDdl::Create(
                "CREATE VIEW v WITH (security_barrier = true, check_option = local) AS SELECT 1"
                    .to_string()
            ))
        );
        assert_eq!(
            parse("CREATE VIEW v WITH (security_barrier = false) AS SELECT 1"),
            None
        );
        assert_eq!(
            parse("ALTER VIEW s.v SET (security_barrier, check_option = 'cascaded')"),
            Some(ViewOptionsDdl::Alter {
                view: vec!["s".to_string(), "v".to_string()],
                if_exists: false,
                changes: vec![
                    ("security_barrier".to_string(), Some("true".to_string())),
                    ("check_option".to_string(), Some("cascaded".to_string())),
                ],
            })
        );
        assert_eq!(
            parse("ALTER VIEW v RESET (security_invoker)"),
            Some(ViewOptionsDdl::Alter {
                view: vec!["v".to_string()],
                if_exists: false,
                changes: vec![("security_invoker".to_string(), None)],
            })
        );
        assert!(parse_bool("of").is_some_and(|value| !value));
        assert_eq!(parse_bool("100"), None);
    }
}
