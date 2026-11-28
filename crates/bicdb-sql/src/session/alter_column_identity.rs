//! `ALTER TABLE ... ALTER [COLUMN] c` forms for identity columns and storage:
//!
//! * `ADD GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY [( sequence options )]`
//! * `SET GENERATED {ALWAYS | BY DEFAULT}`, `SET <sequence option>` and
//!   `RESTART [[WITH] n]`, in any number and order
//! * `DROP IDENTITY [IF EXISTS]`
//! * `SET STORAGE {PLAIN | EXTERNAL | EXTENDED | MAIN | DEFAULT}`
//!
//! Storage is catalog-only: BicDB stores every value whole, so the setting
//! is recorded and shown in `pg_attribute.attstorage` but changes nothing
//! about how a value is kept (it never moves out of line or compresses
//! differently), which is invisible to SQL.
use super::*;
use sqlparser::tokenizer::{Token, Tokenizer};

#[derive(Debug, PartialEq)]
enum SeqOption {
    Start(i64),
    Increment(i64),
    MinValue(Option<i64>),
    MaxValue(Option<i64>),
    Cache(i64),
    Cycle(bool),
    Restart(Option<i64>),
    SequenceName(Vec<String>),
}

#[derive(Debug, PartialEq)]
enum ColumnAction {
    AddIdentity {
        always: bool,
        options: Vec<SeqOption>,
    },
    /// `SET GENERATED` (the kind) and sequence changes.
    AlterIdentity {
        always: Option<bool>,
        options: Vec<SeqOption>,
    },
    DropIdentity {
        if_exists: bool,
    },
    SetStorage(Option<char>),
}

#[derive(Debug, PartialEq)]
struct AlterColumn {
    table: Vec<String>,
    if_exists: bool,
    only: bool,
    column: String,
    action: ColumnAction,
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

fn syntax_error(token: Option<&&Token>) -> SqlError {
    SqlError::data_exception_public(
        "42601",
        format!(
            "syntax error at or near \"{}\"",
            token
                .map(|token| crate::eval::rewrite_helpers::token_sql(token))
                .unwrap_or_default()
        ),
        None,
    )
}

/// A signed integer at `*at`.
fn integer(tokens: &[&Token], at: &mut usize) -> Result<i64> {
    let mut negative = false;
    if matches!(tokens.get(*at), Some(Token::Minus | Token::Plus)) {
        negative = tokens[*at] == &Token::Minus;
        *at += 1;
    }
    let Some(Token::Number(text, _)) = tokens.get(*at) else {
        return Err(syntax_error(tokens.get(*at)));
    };
    *at += 1;
    let signed = if negative {
        format!("-{text}")
    } else {
        text.clone()
    };
    let value = signed.parse::<i64>().map_err(|_| {
        SqlError::numeric_value_out_of_range(format!(
            "value \"{signed}\" is out of range for type bigint"
        ))
    })?;
    Ok(value)
}

/// One sequence option at `*at` (an identity's option list).
fn seq_option(tokens: &[&Token], at: &mut usize) -> Result<SeqOption> {
    let here = tokens.get(*at);
    let start = *at;
    let word = |offset: usize, w: &str| keyword(tokens.get(start + offset), w);
    let here_is = |at: usize, w: &str| keyword(tokens.get(at), w);
    let option = if word(0, "START") {
        *at += 1;
        if here_is(*at, "WITH") {
            *at += 1;
        }
        SeqOption::Start(integer(tokens, at)?)
    } else if word(0, "INCREMENT") {
        *at += 1;
        if here_is(*at, "BY") {
            *at += 1;
        }
        SeqOption::Increment(integer(tokens, at)?)
    } else if word(0, "MINVALUE") {
        *at += 1;
        SeqOption::MinValue(Some(integer(tokens, at)?))
    } else if word(0, "MAXVALUE") {
        *at += 1;
        SeqOption::MaxValue(Some(integer(tokens, at)?))
    } else if word(0, "NO") && word(1, "MINVALUE") {
        *at += 2;
        SeqOption::MinValue(None)
    } else if word(0, "NO") && word(1, "MAXVALUE") {
        *at += 2;
        SeqOption::MaxValue(None)
    } else if word(0, "NO") && word(1, "CYCLE") {
        *at += 2;
        SeqOption::Cycle(false)
    } else if word(0, "CYCLE") {
        *at += 1;
        SeqOption::Cycle(true)
    } else if word(0, "CACHE") {
        *at += 1;
        SeqOption::Cache(integer(tokens, at)?)
    } else if word(0, "RESTART") {
        *at += 1;
        if here_is(*at, "WITH") {
            *at += 1;
            SeqOption::Restart(Some(integer(tokens, at)?))
        } else if matches!(
            tokens.get(*at),
            Some(Token::Number(..) | Token::Minus | Token::Plus)
        ) {
            SeqOption::Restart(Some(integer(tokens, at)?))
        } else {
            SeqOption::Restart(None)
        }
    } else if word(0, "SEQUENCE") && word(1, "NAME") {
        *at += 2;
        let mut parts = vec![ident(tokens.get(*at)).ok_or_else(|| syntax_error(tokens.get(*at)))?];
        *at += 1;
        while tokens.get(*at) == Some(&&Token::Period) {
            parts
                .push(ident(tokens.get(*at + 1)).ok_or_else(|| syntax_error(tokens.get(*at + 1)))?);
            *at += 2;
        }
        SeqOption::SequenceName(parts)
    } else if word(0, "AS") || word(0, "OWNED") || word(0, "LOGGED") || word(0, "UNLOGGED") {
        return Err(SqlError::Unsupported(format!(
            "identity sequence option {}",
            crate::eval::rewrite_helpers::token_sql(tokens[*at])
        )));
    } else {
        return Err(syntax_error(here));
    };
    Ok(option)
}

pub(crate) struct SequenceAlterParameters {
    data_type: Option<String>,
    options: Vec<SeqOption>,
}

pub(crate) fn parse_alter_sequence_parameters(
    sql: &str,
) -> Result<Option<SequenceAlterParameters>> {
    let raw = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    let tokens = raw
        .iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::SemiColon))
        .collect::<Vec<_>>();
    let recognized = [
        "AS",
        "START",
        "INCREMENT",
        "MINVALUE",
        "MAXVALUE",
        "CACHE",
        "CYCLE",
        "NO",
        "RESTART",
    ];
    if !recognized.iter().any(|word| keyword(tokens.first(), word)) {
        return Ok(None);
    }
    let mut at = 0;
    let mut parameters = SequenceAlterParameters {
        data_type: None,
        options: Vec::new(),
    };
    while at < tokens.len() {
        if keyword(tokens.get(at), "AS") {
            at += 1;
            let mut name = ident(tokens.get(at)).ok_or_else(|| syntax_error(tokens.get(at)))?;
            at += 1;
            if tokens.get(at) == Some(&&Token::Period) {
                let part =
                    ident(tokens.get(at + 1)).ok_or_else(|| syntax_error(tokens.get(at + 1)))?;
                name = format!("{name}.{part}");
                at += 2;
            }
            if parameters.data_type.replace(name).is_some() {
                return Err(SqlError::coded(
                    "42601",
                    "conflicting or redundant options",
                    None,
                    None,
                ));
            }
        } else {
            let option = seq_option(&tokens, &mut at)?;
            if parameters
                .options
                .iter()
                .any(|previous| std::mem::discriminant(previous) == std::mem::discriminant(&option))
            {
                return Err(SqlError::coded(
                    "42601",
                    "conflicting or redundant options",
                    None,
                    None,
                ));
            }
            parameters.options.push(option);
        }
    }
    // The standalone RESTART form uses its existing path.
    if parameters.data_type.is_none()
        && matches!(parameters.options.as_slice(), [SeqOption::Restart(_)])
    {
        return Ok(None);
    }
    Ok(Some(parameters))
}

impl SequenceAlterParameters {
    pub(crate) fn apply(&self, db: &BicDb, sequence: &mut SequenceSchema) -> Result<()> {
        if let Some(data_type) = &self.data_type {
            let oid = resolve_regtype_oid(db, data_type)?.ok_or_else(|| {
                SqlError::undefined_object(format!("type \"{data_type}\" does not exist"))
            })?;
            let data_type = match oid {
                21 => "int2",
                23 => "int4",
                20 => "int8",
                _ => {
                    return Err(SqlError::invalid_parameter_value(
                        "sequence type must be smallint, integer, or bigint",
                    ))
                }
            };
            let (old_min, old_max) = sequence_type_limits(&sequence.data_type);
            let (new_min, new_max) = sequence_type_limits(data_type);
            if sequence.min_value == old_min {
                sequence.min_value = new_min;
            }
            if sequence.max_value == old_max {
                sequence.max_value = new_max;
            }
            sequence.data_type = data_type.to_owned();
        }
        apply_options(sequence, &self.options, false)?;
        let last_value = sequence.last_value;
        if last_value < sequence.min_value {
            return Err(SqlError::invalid_parameter_value(format!(
                "RESTART value ({last_value}) cannot be less than MINVALUE ({})",
                sequence.min_value
            )));
        }
        if last_value > sequence.max_value {
            return Err(SqlError::invalid_parameter_value(format!(
                "RESTART value ({last_value}) cannot be greater than MAXVALUE ({})",
                sequence.max_value
            )));
        }
        Ok(())
    }
}

fn parse(sql: &str) -> Result<Option<AlterColumn>> {
    let head = sql.trim_start();
    if !(head.len() >= 5 && head[..5].eq_ignore_ascii_case("ALTER")) {
        return Ok(None);
    }
    let lower = sql.to_ascii_lowercase();
    if !(lower.contains("identity")
        || lower.contains("generated")
        || lower.contains("storage")
        || lower.contains("restart")
        || lower.contains(" set "))
    {
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
        || !keyword(tokens.first(), "ALTER")
        || !keyword(tokens.get(1), "TABLE")
    {
        return Ok(None);
    }
    let mut at = 2;
    let mut if_exists = false;
    if keyword(tokens.get(at), "IF") && keyword(tokens.get(at + 1), "EXISTS") {
        if_exists = true;
        at += 2;
    }
    let mut only = false;
    if keyword(tokens.get(at), "ONLY") {
        only = true;
        at += 1;
    }
    let mut table = Vec::new();
    loop {
        let Some(part) = ident(tokens.get(at)) else {
            return Ok(None);
        };
        table.push(part);
        at += 1;
        if tokens.get(at) == Some(&&Token::Period) {
            at += 1;
        } else {
            break;
        }
    }
    if !keyword(tokens.get(at), "ALTER") {
        return Ok(None);
    }
    at += 1;
    if keyword(tokens.get(at), "COLUMN") {
        at += 1;
    }
    let Some(column) = ident(tokens.get(at)) else {
        return Ok(None);
    };
    at += 1;
    // Only a single ALTER COLUMN action is handled here.
    if tokens[at..].contains(&&Token::Comma) {
        return Ok(None);
    }
    let word = |offset: usize, w: &str| keyword(tokens.get(at + offset), w);
    let action = if word(0, "SET") && word(1, "STORAGE") {
        let storage = match ident(tokens.get(at + 2)).as_deref() {
            Some("plain") => Some('p'),
            Some("external") => Some('e'),
            Some("extended") => Some('x'),
            Some("main") => Some('m'),
            Some("default") => None,
            Some(other) => {
                return Err(SqlError::invalid_parameter_value(format!(
                    "invalid storage type \"{other}\""
                )))
            }
            None => return Err(syntax_error(tokens.get(at + 2))),
        };
        if tokens.len() != at + 3 {
            return Err(syntax_error(tokens.get(at + 3)));
        }
        ColumnAction::SetStorage(storage)
    } else if word(0, "DROP") && word(1, "IDENTITY") {
        let if_exists = word(2, "IF") && word(3, "EXISTS");
        let end = at + if if_exists { 4 } else { 2 };
        if tokens.len() != end {
            return Err(syntax_error(tokens.get(end)));
        }
        ColumnAction::DropIdentity { if_exists }
    } else if word(0, "ADD") && word(1, "GENERATED") {
        let (always, mut next) = if word(2, "ALWAYS") {
            (true, at + 3)
        } else if word(2, "BY") && word(3, "DEFAULT") {
            (false, at + 4)
        } else {
            return Ok(None);
        };
        if !(keyword(tokens.get(next), "AS") && keyword(tokens.get(next + 1), "IDENTITY")) {
            return Ok(None);
        }
        next += 2;
        let mut options = Vec::new();
        if tokens.get(next) == Some(&&Token::LParen) {
            next += 1;
            while tokens.get(next) != Some(&&Token::RParen) {
                if next >= tokens.len() {
                    return Err(syntax_error(None));
                }
                options.push(seq_option(&tokens, &mut next)?);
            }
            next += 1;
        }
        if tokens.len() != next {
            return Err(syntax_error(tokens.get(next)));
        }
        ColumnAction::AddIdentity { always, options }
    } else if word(0, "SET") || word(0, "RESTART") {
        let mut always = None;
        let mut options = Vec::new();
        let mut next = at;
        while next < tokens.len() {
            if keyword(tokens.get(next), "RESTART") {
                options.push(seq_option(&tokens, &mut next)?);
                continue;
            }
            if !keyword(tokens.get(next), "SET") {
                return Err(syntax_error(tokens.get(next)));
            }
            next += 1;
            if keyword(tokens.get(next), "GENERATED") {
                if keyword(tokens.get(next + 1), "ALWAYS") {
                    always = Some(true);
                    next += 2;
                } else if keyword(tokens.get(next + 1), "BY")
                    && keyword(tokens.get(next + 2), "DEFAULT")
                {
                    always = Some(false);
                    next += 3;
                } else {
                    return Err(syntax_error(tokens.get(next + 1)));
                }
                continue;
            }
            let starts_option = [
                "START",
                "INCREMENT",
                "MINVALUE",
                "MAXVALUE",
                "NO",
                "CYCLE",
                "CACHE",
            ]
            .iter()
            .any(|w| keyword(tokens.get(next), w));
            if !starts_option {
                // SET DEFAULT, SET NOT NULL, SET DATA TYPE ...: not ours.
                return Ok(None);
            }
            options.push(seq_option(&tokens, &mut next)?);
        }
        ColumnAction::AlterIdentity { always, options }
    } else {
        return Ok(None);
    };
    Ok(Some(AlterColumn {
        table,
        if_exists,
        only,
        column,
        action,
    }))
}

fn not_identity(column: &str, table: &str) -> SqlError {
    SqlError::object_not_in_prerequisite_state(format!(
        "column \"{column}\" of relation \"{table}\" is not an identity column"
    ))
}

/// Applies identity sequence options to `sequence`; `creating` applies the
/// defaults a new sequence takes from its increment's sign.
fn apply_options(
    sequence: &mut SequenceSchema,
    options: &[SeqOption],
    creating: bool,
) -> Result<()> {
    let (type_min, type_max) = sequence_type_limits(&sequence.data_type);
    let mut start_given = false;
    let mut min_given = false;
    let mut max_given = false;
    let mut restart = None;
    for option in options {
        match option {
            SeqOption::Increment(value) => {
                if *value == 0 {
                    return Err(SqlError::invalid_parameter_value(
                        "INCREMENT must not be zero",
                    ));
                }
                sequence.increment_by = *value;
            }
            _ => {}
        }
    }
    for option in options {
        match option {
            SeqOption::Start(value) => {
                sequence.start_value = *value;
                start_given = true;
            }
            SeqOption::Increment(_) | SeqOption::SequenceName(_) => {}
            SeqOption::MinValue(value) => {
                min_given = true;
                sequence.min_value = value.unwrap_or(if sequence.increment_by > 0 {
                    1
                } else {
                    type_min
                });
            }
            SeqOption::MaxValue(value) => {
                max_given = true;
                sequence.max_value = value.unwrap_or(if sequence.increment_by > 0 {
                    type_max
                } else {
                    -1
                });
            }
            SeqOption::Cache(value) => {
                if *value <= 0 {
                    return Err(SqlError::invalid_parameter_value(format!(
                        "CACHE ({value}) must be greater than zero"
                    )));
                }
                sequence.cache_size = *value;
            }
            SeqOption::Cycle(cycle) => sequence.cycle = *cycle,
            SeqOption::Restart(value) => restart = Some(*value),
        }
    }
    if creating {
        if !min_given {
            sequence.min_value = if sequence.increment_by > 0 {
                1
            } else {
                type_min
            };
        }
        if !max_given {
            sequence.max_value = if sequence.increment_by > 0 {
                type_max
            } else {
                -1
            };
        }
        if !start_given {
            sequence.start_value = if sequence.increment_by > 0 {
                sequence.min_value
            } else {
                sequence.max_value
            };
        }
        sequence.last_value = sequence.start_value;
        sequence.is_called = false;
    }
    validate_sequence_schema(sequence)?;
    if let Some(value) = restart {
        let value = value.unwrap_or(sequence.start_value);
        validate_sequence_value(sequence, value, "RESTART")?;
        sequence.last_value = value;
        sequence.is_called = false;
    }
    Ok(())
}

impl<'a> SqlSession<'a> {
    pub(crate) fn execute_raw_alter_column_identity(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(alter) = parse(sql)? else {
            return Ok(None);
        };
        let name = crate::eval::rewrite_helpers::relation_name_from_parts(&alter.table)?;
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &name);
        let Some(mut schema) = load_schema(self.db_ref(), &table)? else {
            if alter.if_exists {
                crate::statement_context::notice_skipping_missing(
                    "relation",
                    &relation_name_for_message(&name),
                );
                return Ok(Some(SqlResult::command("ALTER TABLE")));
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
        self.require_table_ownership(&schema.name, "ALTER TABLE")?;
        let display = relation_name_for_message(&schema.name);
        let index = schema
            .columns
            .iter()
            .position(|column| !column.hidden && column.name == alter.column)
            .ok_or_else(|| {
                SqlError::data_exception_public(
                    "42703",
                    format!(
                        "column \"{}\" of relation \"{display}\" does not exist",
                        alter.column
                    ),
                    None,
                )
            })?;
        match alter.action {
            ColumnAction::SetStorage(storage) => {
                let mut targets = vec![schema];
                if !alter.only {
                    let descendants =
                        crate::inheritance::inheritance_descendants(self.db_ref(), &targets[0])?;
                    targets.extend(descendants);
                    if targets[0].partitioning.is_some() {
                        let partitions = partition::physical_tables(self.db_ref(), &targets[0])?;
                        targets.extend(partitions.into_iter().skip(1));
                    }
                }
                for mut target in targets {
                    let Some(column) = target
                        .columns
                        .iter_mut()
                        .find(|column| column.name == alter.column)
                    else {
                        continue;
                    };
                    if let Some(requested) = storage {
                        if column.type_storage() == 'p' && requested != 'p' {
                            return Err(SqlError::data_exception_public(
                                "42804",
                                format!(
                                    "column data type {} can only have storage PLAIN",
                                    crate::pg_types::pg_format_type(column.type_oid() as i32, -1)
                                        .unwrap_or_else(|| column.pg_type.clone())
                                ),
                                None,
                            ));
                        }
                    }
                    column.attributes.storage = storage;
                    self.save_session_schema(&target)?;
                }
            }
            ColumnAction::DropIdentity { if_exists } => {
                let column = &mut schema.columns[index];
                if column.identity.is_none() {
                    if if_exists {
                        crate::statement_context::report_notice(
                            None,
                            "00000",
                            format!(
                                "column \"{}\" of relation \"{display}\" is not an identity column, skipping",
                                alter.column
                            ),
                        );
                        return Ok(Some(SqlResult::command("ALTER TABLE")));
                    }
                    return Err(not_identity(&alter.column, &display));
                }
                column.identity = None;
                let sequence = column.default_sequence.take();
                self.save_session_schema(&schema)?;
                if let Some(sequence) = sequence {
                    self.delete_session_sequence(&sequence)?;
                }
            }
            ColumnAction::AlterIdentity { always, options } => {
                let column = &mut schema.columns[index];
                if column.identity.is_none() {
                    return Err(not_identity(&alter.column, &display));
                }
                if options
                    .iter()
                    .any(|option| matches!(option, SeqOption::SequenceName(_)))
                {
                    return Err(SqlError::Unsupported(
                        "SEQUENCE NAME when altering an identity column".to_string(),
                    ));
                }
                if let Some(always) = always {
                    column.identity = Some(if always { "a" } else { "d" }.to_string());
                }
                if !options.is_empty() {
                    let sequence_name = column.default_sequence.clone().ok_or_else(|| {
                        SqlError::object_not_in_prerequisite_state(format!(
                            "identity column \"{}\" has no sequence",
                            alter.column
                        ))
                    })?;
                    let mut sequence = load_sequence_required(self.db_ref(), &sequence_name)?;
                    apply_options(&mut sequence, &options, false)?;
                    self.save_session_sequence(&sequence)?;
                }
                if always.is_some() {
                    self.save_session_schema(&schema)?;
                }
            }
            ColumnAction::AddIdentity { always, options } => {
                let column = &schema.columns[index];
                if !matches!(column.pg_type.as_str(), "int2" | "int4" | "int8")
                    || column.user_type.is_some()
                {
                    return Err(SqlError::invalid_parameter_value(
                        "identity column type must be smallint, integer, or bigint",
                    ));
                }
                if column.nullable && !column.primary_key {
                    return Err(SqlError::object_not_in_prerequisite_state(format!(
                        "column \"{}\" of relation \"{display}\" must be declared NOT NULL before identity can be added",
                        alter.column
                    )));
                }
                if column.attributes.not_null_not_valid {
                    return Err(SqlError::with_detail_fields(
                        SqlError::object_not_in_prerequisite_state(format!(
                            "incompatible NOT VALID constraint \"{}\" on relation \"{display}\"",
                            column.not_null_name_in(&schema.name)
                        )),
                        None,
                        Some("You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.".to_string()),
                        None,
                    ));
                }
                if column.identity.is_some() {
                    return Err(SqlError::object_not_in_prerequisite_state(format!(
                        "column \"{}\" of relation \"{display}\" is already an identity column",
                        alter.column
                    )));
                }
                if column.default_expr.is_some()
                    || column.default_value.is_some()
                    || column.default_sequence.is_some()
                {
                    return Err(SqlError::object_not_in_prerequisite_state(format!(
                        "column \"{}\" of relation \"{display}\" already has a default value",
                        alter.column
                    )));
                }
                if column.generated_expr.is_some() {
                    return Err(SqlError::object_not_in_prerequisite_state(format!(
                        "column \"{}\" of relation \"{display}\" is a generated column",
                        alter.column
                    )));
                }
                let sequence_name = match options.iter().find_map(|option| match option {
                    SeqOption::SequenceName(parts) => Some(parts),
                    _ => None,
                }) {
                    Some(parts) => resolve_session_relation_name_if_exists(
                        self.db_ref(),
                        &crate::eval::rewrite_helpers::relation_name_from_parts(parts)?,
                    ),
                    None => sequence_name_for_column(&schema.name, &column.name),
                };
                // pg_dump creates the named sequence first and attaches it.
                if load_sequence(self.db_ref(), &sequence_name)?.is_none() {
                    let mut sequence =
                        SequenceSchema::new_typed(sequence_name.clone(), &column.pg_type, 1);
                    apply_options(&mut sequence, &options, true)?;
                    sequence.owner = schema
                        .owner
                        .clone()
                        .unwrap_or_else(|| BOOTSTRAP_ROLE_NAME.to_string());
                    sequence.owned_by_table = Some(schema.name.clone());
                    sequence.owned_by_column = Some(column.name.clone());
                    self.create_session_sequence_if_missing(sequence, false)?;
                } else if options
                    .iter()
                    .any(|option| !matches!(option, SeqOption::SequenceName(_)))
                {
                    let mut sequence = load_sequence_required(self.db_ref(), &sequence_name)?;
                    apply_options(&mut sequence, &options, false)?;
                    self.save_session_sequence(&sequence)?;
                }
                let column = &mut schema.columns[index];
                column.identity = Some(if always { "a" } else { "d" }.to_string());
                column.default_sequence = Some(sequence_name);
                self.save_session_schema(&schema)?;
            }
        }
        Ok(Some(SqlResult::command("ALTER TABLE")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_identity_and_storage_actions() {
        let parsed = parse(
            "ALTER TABLE t ALTER COLUMN a SET GENERATED BY DEFAULT SET INCREMENT BY 2 SET START WITH 100 RESTART",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            parsed.action,
            ColumnAction::AlterIdentity {
                always: Some(false),
                options: vec![
                    SeqOption::Increment(2),
                    SeqOption::Start(100),
                    SeqOption::Restart(None)
                ],
            }
        );
        let parsed = parse("ALTER TABLE ONLY s.t ALTER a ADD GENERATED ALWAYS AS IDENTITY (START WITH -10 INCREMENT BY -1 SEQUENCE NAME s.q)")
            .unwrap()
            .unwrap();
        assert!(parsed.only);
        assert_eq!(
            parsed.action,
            ColumnAction::AddIdentity {
                always: true,
                options: vec![
                    SeqOption::Start(-10),
                    SeqOption::Increment(-1),
                    SeqOption::SequenceName(vec!["s".into(), "q".into()])
                ],
            }
        );
        assert_eq!(
            parse("ALTER TABLE t ALTER COLUMN a SET STORAGE EXTERNAL")
                .unwrap()
                .unwrap()
                .action,
            ColumnAction::SetStorage(Some('e'))
        );
        assert_eq!(
            parse("ALTER TABLE t ALTER a DROP IDENTITY IF EXISTS")
                .unwrap()
                .unwrap()
                .action,
            ColumnAction::DropIdentity { if_exists: true }
        );
        assert_eq!(parse("ALTER TABLE t ALTER a SET DEFAULT 1").unwrap(), None);
        assert_eq!(parse("ALTER TABLE t ALTER a SET NOT NULL").unwrap(), None);
    }
}
