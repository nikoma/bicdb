//! PostgreSQL `VACUUM` and `ANALYZE` statement forms.
//!
//! `VACUUM [ ( option [, ...] ) ] [ table [ ( column [, ...] ) ] [, ...] ]`,
//! the unparenthesized `VACUUM [ FULL ] [ FREEZE ] [ VERBOSE ] [ ANALYZE ]`
//! form, and `ANALYZE [ ( option [, ...] ) | VERBOSE ] [ table ... ]`.
//!
//! BicDB reclaims dead versions with its store-wide reclaimer (plain
//! `VACUUM`, and automatically in the background); it has no per-table space
//! reclamation, freezing or table rewrite. A table-scoped VACUUM therefore
//! does what PostgreSQL makes visible to SQL: it validates the options and
//! the named relations and columns, refuses to run inside a transaction
//! block, skips relations the role may not maintain with PostgreSQL's
//! warning, and refreshes statistics when ANALYZE is requested.
use super::*;
use sqlparser::tokenizer::{Token, Tokenizer};

#[derive(Debug, Default, PartialEq)]
pub(crate) struct VacuumStatement {
    /// `VACUUM` (true) or `ANALYZE` (false).
    pub vacuum: bool,
    pub analyze: bool,
    pub full: bool,
    pub process_toast: bool,
    pub parallel: bool,
    pub buffer_usage_limit: bool,
    pub only_database_stats: bool,
    pub other_options: bool,
    pub targets: Vec<VacuumTarget>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct VacuumTarget {
    pub name: ObjectName,
    pub columns: Vec<String>,
}

fn syntax_error(message: impl Into<String>) -> SqlError {
    SqlError::data_exception_public("42601", message, None)
}

fn feature_not_supported(message: impl Into<String>) -> SqlError {
    SqlError::data_exception_public("0A000", message, None)
}

fn significant_tokens(sql: &str) -> Option<Vec<Token>> {
    let tokens = Tokenizer::new(&sqlparser::dialect::PostgreSqlDialect {}, sql)
        .tokenize()
        .ok()?;
    let mut tokens = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    while matches!(tokens.last(), Some(Token::SemiColon)) {
        tokens.pop();
    }
    if tokens.contains(&Token::SemiColon) {
        return None;
    }
    Some(tokens)
}

fn keyword(token: Option<&Token>, expected: &str) -> bool {
    matches!(token, Some(Token::Word(word)) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
}

fn token_text(token: &Token) -> String {
    match token {
        Token::Word(word) => word.value.clone(),
        other => other.to_string(),
    }
}

fn unexpected(token: Option<&Token>) -> SqlError {
    match token {
        Some(token) => syntax_error(format!("syntax error at or near \"{}\"", token_text(token))),
        None => syntax_error("syntax error at end of input"),
    }
}

fn ident_from_token(token: Option<&Token>) -> Result<Ident> {
    match token {
        Some(Token::Word(word)) => Ok(match word.quote_style {
            Some(quote) => Ident::with_quote(quote, word.value.clone()),
            None => Ident::new(word.value.to_ascii_lowercase()),
        }),
        other => Err(unexpected(other)),
    }
}

/// Parses a VACUUM or ANALYZE statement. `Ok(None)` when `sql` is neither.
pub(crate) fn parse_vacuum_statement(sql: &str) -> Result<Option<VacuumStatement>> {
    let Some(tokens) = significant_tokens(sql) else {
        return Ok(None);
    };
    let mut at = 0;
    let vacuum = if keyword(tokens.first(), "vacuum") {
        true
    } else if keyword(tokens.first(), "analyze") || keyword(tokens.first(), "analyse") {
        false
    } else {
        return Ok(None);
    };
    at += 1;
    let mut statement = VacuumStatement {
        vacuum,
        analyze: !vacuum,
        process_toast: true,
        ..VacuumStatement::default()
    };
    let statement_name = if vacuum { "VACUUM" } else { "ANALYZE" };
    if tokens.get(at) == Some(&Token::LParen) {
        at += 1;
        loop {
            let name = match tokens.get(at) {
                Some(Token::Word(word)) => word.value.to_ascii_lowercase(),
                other => return Err(unexpected(other)),
            };
            at += 1;
            let mut value: Option<Token> = None;
            if !matches!(tokens.get(at), Some(Token::Comma | Token::RParen)) {
                let negative = tokens.get(at) == Some(&Token::Minus);
                if negative {
                    at += 1;
                }
                match tokens.get(at) {
                    Some(Token::Number(number, long)) => {
                        let number = if negative {
                            format!("-{number}")
                        } else {
                            number.clone()
                        };
                        value = Some(Token::Number(number, *long));
                    }
                    Some(token @ (Token::Word(_) | Token::SingleQuotedString(_))) if !negative => {
                        value = Some(token.clone());
                    }
                    other => return Err(unexpected(other)),
                }
                at += 1;
            }
            apply_vacuum_option(&mut statement, statement_name, &name, value.as_ref())?;
            match tokens.get(at) {
                Some(Token::Comma) => at += 1,
                Some(Token::RParen) => {
                    at += 1;
                    break;
                }
                other => return Err(unexpected(other)),
            }
        }
    } else if vacuum {
        for option in ["full", "freeze", "verbose", "analyze"] {
            if keyword(tokens.get(at), option)
                || (option == "analyze" && keyword(tokens.get(at), "analyse"))
            {
                at += 1;
                match option {
                    "full" => statement.full = true,
                    "analyze" => statement.analyze = true,
                    _ => statement.other_options = true,
                }
            }
        }
    } else if keyword(tokens.get(at), "verbose") {
        at += 1;
    }
    while at < tokens.len() {
        if keyword(tokens.get(at), "only") {
            at += 1;
        }
        let mut parts = vec![ident_from_token(tokens.get(at))?];
        at += 1;
        while tokens.get(at) == Some(&Token::Period) {
            at += 1;
            parts.push(ident_from_token(tokens.get(at))?);
            at += 1;
        }
        let mut columns = Vec::new();
        if tokens.get(at) == Some(&Token::LParen) {
            at += 1;
            loop {
                let ident = ident_from_token(tokens.get(at))?;
                columns.push(ident.value);
                at += 1;
                match tokens.get(at) {
                    Some(Token::Comma) => at += 1,
                    Some(Token::RParen) => {
                        at += 1;
                        break;
                    }
                    other => return Err(unexpected(other)),
                }
            }
        }
        statement.targets.push(VacuumTarget {
            name: ObjectName::from(parts),
            columns,
        });
        match tokens.get(at) {
            None => break,
            Some(Token::Comma) => at += 1,
            other => return Err(unexpected(other)),
        }
    }
    validate_vacuum_statement(&statement)?;
    Ok(Some(statement))
}

fn option_boolean(name: &str, value: Option<&Token>) -> Result<bool> {
    let Some(value) = value else {
        return Ok(true);
    };
    let text = match value {
        Token::Word(word) => word.value.to_ascii_lowercase(),
        Token::SingleQuotedString(text) => text.to_ascii_lowercase(),
        Token::Number(number, _) => number.clone(),
        _ => String::new(),
    };
    match text.as_str() {
        "true" | "on" | "1" => Ok(true),
        "false" | "off" | "0" => Ok(false),
        _ => Err(syntax_error(format!(
            "{} requires a Boolean value",
            name.to_ascii_lowercase()
        ))),
    }
}

fn apply_vacuum_option(
    statement: &mut VacuumStatement,
    statement_name: &str,
    name: &str,
    value: Option<&Token>,
) -> Result<()> {
    let vacuum_only = |statement: &VacuumStatement| statement.vacuum;
    match name {
        "verbose" | "skip_locked" => {
            option_boolean(name, value)?;
        }
        "buffer_usage_limit" => {
            statement.buffer_usage_limit = true;
            if value.is_none() {
                return Err(syntax_error("buffer_usage_limit requires a parameter"));
            }
        }
        "analyze" if vacuum_only(statement) => {
            statement.analyze = option_boolean(name, value)?;
        }
        "full" if vacuum_only(statement) => {
            statement.full = option_boolean(name, value)?;
        }
        "process_toast" if vacuum_only(statement) => {
            statement.process_toast = option_boolean(name, value)?;
        }
        "only_database_stats" if vacuum_only(statement) => {
            statement.only_database_stats = option_boolean(name, value)?;
        }
        "freeze"
        | "disable_page_skipping"
        | "process_main"
        | "truncate"
        | "skip_database_stats"
            if vacuum_only(statement) =>
        {
            if option_boolean(name, value)? {
                statement.other_options = true;
            }
        }
        "index_cleanup" if vacuum_only(statement) => {
            let auto = matches!(value, Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("auto"))
                || matches!(value, Some(Token::SingleQuotedString(text)) if text.eq_ignore_ascii_case("auto"));
            if !auto {
                option_boolean(name, value)?;
            }
            statement.other_options = true;
        }
        "parallel" if vacuum_only(statement) => {
            let Some(value) = value else {
                return Err(syntax_error(
                    "parallel option requires a value between 0 and 1024",
                ));
            };
            let workers = match value {
                Token::Number(number, _) => number.parse::<i64>().ok(),
                _ => None,
            };
            match workers {
                Some(workers) if (0..=1024).contains(&workers) => {
                    statement.parallel = workers > 0;
                    statement.other_options = true;
                }
                _ => {
                    return Err(syntax_error(
                        "parallel workers for vacuum must be between 0 and 1024",
                    ))
                }
            }
        }
        _ => {
            return Err(syntax_error(format!(
                "unrecognized {statement_name} option \"{name}\""
            )))
        }
    }
    Ok(())
}

fn validate_vacuum_statement(statement: &VacuumStatement) -> Result<()> {
    if statement.full && statement.parallel {
        return Err(feature_not_supported(
            "VACUUM FULL cannot be performed in parallel",
        ));
    }
    if statement.full && statement.buffer_usage_limit {
        return Err(feature_not_supported(
            "BUFFER_USAGE_LIMIT cannot be specified for VACUUM FULL",
        ));
    }
    if !statement.analyze
        && statement
            .targets
            .iter()
            .any(|target| !target.columns.is_empty())
    {
        return Err(feature_not_supported(
            "ANALYZE option must be specified when a column list is provided",
        ));
    }
    if statement.only_database_stats {
        if !statement.targets.is_empty() {
            return Err(feature_not_supported(
                "ONLY_DATABASE_STATS cannot be specified with a list of tables",
            ));
        }
        if statement.analyze || statement.full || statement.other_options {
            return Err(feature_not_supported(
                "ONLY_DATABASE_STATS cannot be specified with other VACUUM options",
            ));
        }
    }
    if statement.full && !statement.process_toast {
        return Err(feature_not_supported(
            "PROCESS_TOAST required with VACUUM FULL",
        ));
    }
    Ok(())
}

/// What one named relation of a VACUUM/ANALYZE resolved to.
enum VacuumRelation {
    Table {
        collection: String,
        schema: TableSchema,
    },
    /// A materialized view or a catalog relation: maintained, nothing to do.
    Maintained,
    /// A view: PostgreSQL skips it with a warning.
    NotATable,
}

impl<'db> SqlSession<'db> {
    /// Every `VACUUM` / `ANALYZE` form except BicDB's plain store-wide
    /// `VACUUM` report. `Ok(None)` when `sql` is not such a statement.
    pub(crate) fn execute_raw_vacuum_analyze(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        let mut parsed = Vec::with_capacity(statements.len());
        for statement in &statements {
            // BicDB's operator-facing store-wide reclaimer keeps its report.
            if statement
                .trim()
                .trim_end_matches(';')
                .trim()
                .eq_ignore_ascii_case("VACUUM")
            {
                return Ok(None);
            }
            let Some(vacuum) = parse_vacuum_statement(statement)? else {
                return Ok(None);
            };
            parsed.push(vacuum);
        }
        let mut notices = Vec::new();
        let mut tag = "ANALYZE";
        for statement in parsed {
            if statement.vacuum {
                tag = "VACUUM";
                if self.tx.is_some() || statements.len() > 1 {
                    return Err(SqlError::data_exception_public(
                        "25001",
                        "VACUUM cannot run inside a transaction block",
                        None,
                    ));
                }
            }
            self.execute_vacuum_statement(&statement, &mut notices)?;
        }
        let mut result = SqlResult::command(tag);
        result.notices = notices;
        Ok(Some(result))
    }

    fn execute_vacuum_statement(
        &mut self,
        statement: &VacuumStatement,
        notices: &mut Vec<SqlNotice>,
    ) -> Result<()> {
        if statement.targets.is_empty() {
            if statement.analyze {
                self.analyze_all_permitted_with_type_statistics()?;
            }
            return Ok(());
        }
        // PostgreSQL resolves every named relation before processing any.
        let mut relations = Vec::with_capacity(statement.targets.len());
        for target in &statement.targets {
            relations.push(self.resolve_vacuum_relation(target)?);
        }
        let verb = if statement.vacuum {
            "vacuum"
        } else {
            "analyze"
        };
        for (target, relation) in statement.targets.iter().zip(relations) {
            let display = target
                .name
                .0
                .last()
                .map(|part| part.to_string())
                .map(|name| name.trim_matches('"').to_string())
                .unwrap_or_default();
            match relation {
                VacuumRelation::NotATable => notices.push(SqlNotice {
                    sqlstate: "01000".to_string(),
                    message: format!(
                        "skipping \"{display}\" --- cannot {verb} non-tables or special system tables"
                    ),
                    fields: vec![SqlErrorField::Severity("WARNING".to_string())],
                }),
                VacuumRelation::Maintained => {
                    if !self.current_user_is_superuser()? {
                        notices.push(permission_skip_notice(verb, &display));
                    }
                }
                VacuumRelation::Table { collection, schema } => {
                    if self
                        .require_table_ownership(&collection, "maintain it")
                        .is_err()
                    {
                        notices.push(permission_skip_notice(verb, &display));
                        continue;
                    }
                    if statement.analyze {
                        let mut seen = BTreeSet::new();
                        for column in &target.columns {
                            if !schema
                                .columns
                                .iter()
                                .any(|candidate| candidate.name == *column)
                            {
                                return Err(SqlError::data_exception_public(
                                    "42703",
                                    format!(
                                        "column \"{column}\" of relation \"{display}\" does not exist"
                                    ),
                                    None,
                                ));
                            }
                            if !seen.insert(column.as_str()) {
                                return Err(SqlError::data_exception_public(
                                    "42701",
                                    format!(
                                        "column \"{column}\" of relation \"{display}\" appears more than once"
                                    ),
                                    None,
                                ));
                            }
                        }
                        self.analyze_collection_with_type_statistics(&collection)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn resolve_vacuum_relation(&self, target: &VacuumTarget) -> Result<VacuumRelation> {
        let mut name = target.name.clone();
        crate::search_path::bind_raw_relation(self.db_ref(), &mut name);
        let collection = relation_name(&name)?;
        if let Some(schema) = load_schema(self.db_ref(), &collection)? {
            return Ok(VacuumRelation::Table { collection, schema });
        }
        if let Some(view) = load_view(self.db_ref(), &collection)? {
            return Ok(if view.materialized {
                VacuumRelation::Maintained
            } else {
                VacuumRelation::NotATable
            });
        }
        if (collection.starts_with("pg_catalog.") || !collection.contains('.'))
            && virtual_table_columns(&collection).is_some()
        {
            return Ok(VacuumRelation::Maintained);
        }
        let written = target
            .name
            .0
            .iter()
            .map(|part| part.to_string().trim_matches('"').to_string())
            .collect::<Vec<_>>()
            .join(".");
        Err(SqlError::data_exception_public(
            "42P01",
            format!("relation \"{written}\" does not exist"),
            None,
        ))
    }
}

fn permission_skip_notice(verb: &str, relation: &str) -> SqlNotice {
    SqlNotice {
        sqlstate: "01000".to_string(),
        message: format!("permission denied to {verb} \"{relation}\", skipping it"),
        fields: vec![SqlErrorField::Severity("WARNING".to_string())],
    }
}
