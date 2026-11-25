//! PostgreSQL partition bounds and physical relation identities. Oracle's
//! partition metadata and routing are deliberately handled by its own module.
use crate::*;
mod hash;

#[derive(Clone, Debug)]
pub(crate) enum BoundValue {
    Min,
    Value(SqlValue),
    Max,
}

#[derive(Clone, Debug)]
pub(crate) enum Bound {
    Default,
    Range(Vec<BoundValue>, Vec<BoundValue>),
    List(Vec<SqlValue>),
    Hash { modulus: u64, remainder: u64 },
}

fn invalid(message: impl Into<String>) -> SqlError {
    SqlError::InvalidSql(message.into())
}

pub(crate) fn violation(table: &str, message: impl Into<String>) -> SqlError {
    SqlError::ConstraintViolation {
        sqlstate: "23514",
        message: message.into(),
        schema: None,
        table: Some(table.into()),
        column: None,
        constraint: None,
        oracle_code: None,
        unique_origin: None,
        detail: None,
    }
}

struct Tokens {
    values: Vec<Token>,
    pos: usize,
}

impl Tokens {
    fn new(sql: &str) -> Result<Self> {
        let values = Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize()
            .map_err(|e| invalid(e.to_string()))?
            .into_iter()
            .filter(|token| !matches!(token, Token::Whitespace(_)))
            .collect();
        Ok(Self { values, pos: 0 })
    }

    fn keyword(&mut self, value: &str) -> bool {
        if matches!(self.values.get(self.pos), Some(Token::Word(word))
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value))
        {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, value: &str) -> Result<()> {
        if self.keyword(value) {
            Ok(())
        } else {
            Err(invalid(format!("partition bound expects {value}")))
        }
    }

    fn group(&mut self) -> Result<Vec<Vec<Token>>> {
        if self.values.get(self.pos) != Some(&Token::LParen) {
            return Err(invalid("partition bound expects a parenthesized list"));
        }
        self.pos += 1;
        let mut depth = 0;
        let mut groups = vec![Vec::new()];
        while let Some(token) = self.values.get(self.pos) {
            self.pos += 1;
            match token {
                Token::RParen if depth == 0 => {
                    if groups.iter().any(Vec::is_empty) {
                        return Err(invalid("empty partition bound value"));
                    }
                    return Ok(groups);
                }
                Token::Comma if depth == 0 => groups.push(Vec::new()),
                token => {
                    if *token == Token::LParen {
                        depth += 1;
                    } else if *token == Token::RParen {
                        depth -= 1;
                    }
                    groups.last_mut().unwrap().push(token.clone());
                }
            }
        }
        Err(invalid("unterminated partition bound"))
    }

    fn finish(self) -> Result<()> {
        if self.pos == self.values.len() {
            Ok(())
        } else {
            Err(invalid("unexpected trailing partition bound clause"))
        }
    }
}

fn bound_literal(tokens: &[Token], column: &ColumnSchema) -> Result<SqlValue> {
    let expression = tokens
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(&expression)
        .map_err(|e| invalid(e.to_string()))?;
    let expr = parser.parse_expr().map_err(|e| invalid(e.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(invalid("unexpected trailing partition bound expression"));
    }
    // Bounds are persisted, not re-evaluated at each row's wall clock. Do not
    // admit session/clock/routine-dependent expressions as catalog constants.
    fn immutable(expr: &Expr) -> bool {
        match expr {
            Expr::Value(_) | Expr::TypedString(_) => true,
            Expr::Cast { expr, .. } | Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
                immutable(expr)
            }
            Expr::BinaryOp { left, right, .. } => immutable(left) && immutable(right),
            _ => false,
        }
    }
    if !immutable(&expr) {
        if let Some(error) = forbidden_bound_expression(&expr) {
            return Err(error);
        }
        return Err(SqlError::Unsupported(
            "partition bounds require immutable literal expressions".into(),
        ));
    }
    cast_value_to_column_type(eval_constant_expr(&expr)?, column)
}

/// PostgreSQL's EXPR_KIND_PARTITION_BOUND diagnostics, in its order: column
/// references are rejected while the arguments are transformed, then
/// subqueries, aggregates, window functions and set-returning functions.
fn forbidden_bound_expression(expr: &Expr) -> Option<SqlError> {
    use sqlparser::ast::visit_expressions;
    use std::ops::ControlFlow;
    let mut column = false;
    let mut subquery = false;
    let mut aggregate = false;
    let mut window = false;
    let mut set_returning = false;
    let _ = visit_expressions(expr, |expr| {
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => column = true,
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => subquery = true,
            Expr::Function(function) => {
                if function.over.is_some() {
                    window = true;
                } else if is_aggregate_function(function) {
                    aggregate = true;
                } else if object_name(&function.name).is_ok_and(|name| {
                    matches!(
                        name.to_ascii_lowercase().as_str(),
                        "generate_series"
                            | "generate_subscripts"
                            | "unnest"
                            | "regexp_matches"
                            | "regexp_split_to_table"
                            | "json_array_elements"
                            | "jsonb_array_elements"
                            | "json_each"
                            | "jsonb_each"
                    )
                }) {
                    set_returning = true;
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    if column {
        Some(SqlError::Unsupported(
            "cannot use column reference in partition bound expression".into(),
        ))
    } else if subquery {
        Some(SqlError::Unsupported(
            "cannot use subquery in partition bound".into(),
        ))
    } else if aggregate {
        Some(SqlError::data_exception(
            "42803",
            "aggregate functions are not allowed in partition bound",
            None,
        ))
    } else if window {
        Some(SqlError::data_exception(
            "42P20",
            "window functions are not allowed in partition bound",
            None,
        ))
    } else if set_returning {
        Some(SqlError::Unsupported(
            "set-returning functions are not allowed in partition bound".into(),
        ))
    } else {
        None
    }
}

/// One partition key position: the column it reads, or for an expression key
/// a descriptor typed as the expression's result (bounds are coerced to it and
/// compared, hashed and NULL-checked with its type) plus the parsed expression.
pub(crate) struct KeyPart {
    pub(crate) column: ColumnSchema,
    pub(crate) expr: Option<Expr>,
}

pub(crate) fn key_parts(parent: &TableSchema) -> Result<Vec<KeyPart>> {
    let partitioning = parent
        .partitioning
        .as_ref()
        .ok_or_else(|| invalid("parent is not partitioned"))?;
    partitioning
        .key_columns
        .iter()
        .enumerate()
        .map(|(index, name)| match partitioning.key_expression(index) {
            None => parent
                .columns
                .iter()
                .find(|column| column.name == *name && !column.hidden)
                .map(|column| KeyPart {
                    column: column.clone(),
                    expr: None,
                })
                .ok_or_else(|| invalid(format!("partition key column {name} does not exist"))),
            Some(sql) => {
                let expr = parse_check_expression(sql)?;
                let (column, _) = key_expression_column(parent, sql, &expr)?;
                Ok(KeyPart {
                    column,
                    expr: Some(expr),
                })
            }
        })
        .collect()
}

/// The typed descriptor of an expression key and the table columns it reads.
fn key_expression_column(
    parent: &TableSchema,
    sql: &str,
    expr: &Expr,
) -> Result<(ColumnSchema, Vec<String>)> {
    let referenced = expression_columns(parent, expr)?;
    let template = referenced
        .first()
        .and_then(|name| parent.columns.iter().find(|column| column.name == *name))
        .ok_or_else(|| invalid("partition key expression reads no column"))?;
    let pg_type = projected_expr_pg_type(expr, Some(parent))
        .or_else(|| key_expression_type(parent, expr))
        .ok_or_else(|| {
            SqlError::Unsupported(format!(
                "could not determine the type of partition key expression {sql}"
            ))
        })?;
    if pg_type.ends_with("[]") || pg_type == "unknown" || pg_type == "record" {
        return Err(SqlError::Unsupported(format!(
            "partition key expression of type {pg_type} is not supported"
        )));
    }
    let mut column = template.clone();
    column.name = sql.to_string();
    column.pg_type = pg_type;
    column.user_type = None;
    column.type_modifier = None;
    column.array_ndims = 0;
    column.primary_key = false;
    column.hidden = false;
    column.nullable = true;
    column.default_sequence = None;
    column.default_value = None;
    column.default_expr = None;
    column.generated_expr = None;
    column.identity = None;
    // A text result keeps the collation of the text column it is computed
    // from (PostgreSQL's collation derivation for lower(a), a || b, ...).
    if !matches!(
        column.pg_type.as_str(),
        "text" | "varchar" | "bpchar" | "name"
    ) {
        column.collation = None;
    }
    Ok((column, referenced))
}

/// Result types of the admitted key functions where the general projector
/// has no answer (PostgreSQL's pg_proc result types for these signatures).
fn key_expression_type(parent: &TableSchema, expr: &Expr) -> Option<String> {
    let argument_type = |expr: &Expr| {
        projected_expr_pg_type(expr, Some(parent)).or_else(|| key_expression_type(parent, expr))
    };
    match expr {
        Expr::Nested(inner) => argument_type(inner),
        Expr::Substring { expr, .. } => argument_type(expr)
            .filter(|pg_type| matches!(pg_type.as_str(), "text" | "varchar" | "bpchar" | "name"))
            .map(|_| "text".to_string()),
        Expr::Identifier(ident) => parent
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&ident.value))
            .filter(|column| column.user_type.is_none() && column.array_ndims == 0)
            .map(|column| column.pg_type.clone()),
        Expr::Function(function) => {
            let name = object_name(&function.name).ok()?.to_ascii_lowercase();
            let name = name
                .strip_prefix("pg_catalog.")
                .unwrap_or(&name)
                .to_string();
            let args = function_args(function);
            let first = args.first().and_then(argument_type);
            let text_arg = first
                .as_deref()
                .is_some_and(|pg_type| matches!(pg_type, "text" | "varchar" | "bpchar" | "name"));
            match name.as_str() {
                "lower" | "upper" | "btrim" | "ltrim" | "rtrim" | "trim" | "substr"
                | "substring" | "left" | "right" | "replace" | "reverse" | "initcap" | "md5"
                    if text_arg =>
                {
                    Some("text".into())
                }
                "length" | "char_length" | "character_length" | "octet_length" if text_arg => {
                    Some("int4".into())
                }
                "abs" | "mod" | "div" | "coalesce" | "nullif" | "greatest" | "least" => first
                    .filter(|pg_type| {
                        matches!(
                            pg_type.as_str(),
                            "int2" | "int4" | "int8" | "numeric" | "float4" | "float8"
                        ) || (text_arg && !matches!(name.as_str(), "abs" | "mod" | "div"))
                    }),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The table columns an expression references, in first-use order. Unknown
/// columns are PostgreSQL's 42703 `column "x" does not exist`.
fn expression_columns(parent: &TableSchema, expr: &Expr) -> Result<Vec<String>> {
    use sqlparser::ast::visit_expressions;
    use std::ops::ControlFlow;
    let mut names = Vec::new();
    let mut missing = None;
    let _ = visit_expressions(expr, |expr| {
        let ident = match expr {
            Expr::Identifier(ident) => Some(ident),
            Expr::CompoundIdentifier(idents) => idents.last(),
            _ => None,
        };
        if let Some(ident) = ident {
            let name = if ident.quote_style.is_some() {
                ident.value.clone()
            } else {
                ident.value.to_ascii_lowercase()
            };
            match parent
                .columns
                .iter()
                .find(|column| !column.hidden && column.name == name)
            {
                Some(column) => {
                    if !names.contains(&column.name) {
                        names.push(column.name.clone());
                    }
                }
                None => {
                    missing.get_or_insert(name);
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    if let Some(name) = missing {
        return Err(SqlError::data_exception(
            "42703",
            format!("column \"{name}\" does not exist"),
            None,
        ));
    }
    Ok(names)
}

/// An expression key as PostgreSQL's ruleutils prints it: every operator
/// parenthesized, `(x)::type` casts, function calls bare.
pub(crate) fn deparse_key_expression(sql: &str) -> String {
    fn render(expr: &Expr) -> String {
        match expr {
            Expr::Identifier(ident) => {
                pg_partition_key_identifier(&if ident.quote_style.is_some() {
                    ident.value.clone()
                } else {
                    ident.value.to_ascii_lowercase()
                })
            }
            Expr::Nested(inner) => render(inner),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut args = vec![render(expr)];
                args.extend(substring_from.iter().map(|from| render(from)));
                args.extend(substring_for.iter().map(|count| render(count)));
                format!("substr({})", args.join(", "))
            }
            Expr::BinaryOp { left, op, right } => {
                format!("({} {op} {})", render(left), render(right))
            }
            Expr::Cast {
                expr, data_type, ..
            } => {
                let inner = render(expr);
                let inner = if inner.starts_with('(') {
                    inner
                } else {
                    format!("({inner})")
                };
                format!("{inner}::{}", data_type.to_string().to_ascii_lowercase())
            }
            Expr::Function(function) => {
                let name = object_name(&function.name)
                    .map(|name| name.to_ascii_lowercase())
                    .unwrap_or_else(|_| function.name.to_string());
                let args = function_args(function)
                    .iter()
                    .map(render)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{name}({args})")
            }
            other => other.to_string(),
        }
    }
    match parse_check_expression(sql) {
        Ok(expr) => render(&expr),
        Err(_) => sql.to_string(),
    }
}

/// `pg_get_partkeydef`'s form of an expression key: parenthesized unless it
/// is a bare function call.
pub(crate) fn deparse_key_definition(sql: &str) -> String {
    let text = deparse_key_expression(sql);
    if matches!(
        parse_check_expression(sql),
        Ok(Expr::Function(_) | Expr::Substring { .. })
    ) {
        text
    } else {
        format!("({text})")
    }
}

/// Whether `column` is a key column of `schema` or is read by one of its key
/// expressions (PostgreSQL's has_partition_attrs).
pub(crate) fn key_references_column(schema: &TableSchema, column: &str) -> bool {
    let Some(partitioning) = &schema.partitioning else {
        return false;
    };
    partitioning
        .key_columns
        .iter()
        .enumerate()
        .any(|(index, key)| match partitioning.key_expression(index) {
            None => key.eq_ignore_ascii_case(column),
            Some(sql) => parse_check_expression(sql)
                .ok()
                .and_then(|expr| expression_columns(schema, &expr).ok())
                .is_none_or(|names| names.iter().any(|name| name.eq_ignore_ascii_case(column))),
        })
}

pub(crate) fn key_columns(parent: &TableSchema) -> Result<Vec<ColumnSchema>> {
    Ok(key_parts(parent)?
        .into_iter()
        .map(|part| part.column)
        .collect())
}

/// The partition key of `record` under `parent`'s key: column values as
/// stored, expression keys evaluated on the row and coerced to their type.
pub(crate) fn key_values(
    db: &BicDb,
    parent: &TableSchema,
    record: &Record,
) -> Result<Vec<SqlValue>> {
    key_values_of(db, parent, &key_parts(parent)?, record)
}

fn key_values_of(
    db: &BicDb,
    parent: &TableSchema,
    parts: &[KeyPart],
    record: &Record,
) -> Result<Vec<SqlValue>> {
    let mut row = None;
    let mut values = Vec::with_capacity(parts.len());
    for part in parts {
        let value = match &part.expr {
            None => record_column_value(record, parent, &part.column.name),
            Some(expr) => {
                if row.is_none() {
                    row = Some(row_from_record(
                        &parent.name,
                        &parent.name,
                        Some(parent),
                        record,
                    )?);
                }
                let value = SqlEngine::new(db).eval_row_value(row.as_ref().unwrap(), expr)?;
                cast_value_to_column_type(value, &part.column)?
            }
        };
        values.push(value);
    }
    Ok(values)
}

/// CREATE-time checks of a partition key, PostgreSQL's ComputePartitionAttrs
/// and EXPR_KIND_PARTITION_EXPRESSION rules in its order. Only creation paths
/// call this; reopening a store never re-judges an accepted definition.
pub(crate) fn validate_new_key(db: &BicDb, schema: &TableSchema) -> Result<()> {
    let Some(partitioning) = &schema.partitioning else {
        return Ok(());
    };
    if partitioning.strategy == "list" && partitioning.key_columns.len() > 1 {
        return Err(SqlError::data_exception(
            "42P17",
            "cannot use \"list\" partition strategy with more than one column",
            None,
        ));
    }
    for (index, name) in partitioning.key_columns.iter().enumerate() {
        let Some(sql) = partitioning.key_expression(index) else {
            let column = schema
                .columns
                .iter()
                .find(|column| !column.hidden && column.name == *name);
            let Some(column) = column else {
                if is_system_column_name(name) {
                    return Err(SqlError::data_exception(
                        "42P17",
                        format!("cannot use system column \"{name}\" in partition key"),
                        None,
                    ));
                }
                return Err(SqlError::data_exception(
                    "42703",
                    format!("column \"{name}\" named in partition key does not exist"),
                    None,
                ));
            };
            if column.generated_expr.is_some() {
                return Err(SqlError::RaisedException {
                    sqlstate: "42P17".into(),
                    message: "cannot use generated column in partition key".into(),
                    detail: Some(format!("Column \"{}\" is a generated column.", column.name)),
                });
            }
            continue;
        };
        let expr = parse_check_expression(sql)?;
        if let Some(error) = forbidden_key_expression(db, &expr) {
            return Err(error);
        }
        let referenced = expression_columns(schema, &expr)?;
        // CheckAttributeType: a row or an untyped literal is a pseudo-type.
        let pseudo_type = match &expr {
            Expr::Tuple(_) => Some("record"),
            Expr::Value(value) if matches!(value.value, Value::SingleQuotedString(_)) => {
                Some("unknown")
            }
            _ => None,
        };
        if let Some(pseudo_type) = pseudo_type {
            return Err(SqlError::data_exception(
                "42P16",
                format!(
                    "partition key column {} has pseudo-type {pseudo_type}",
                    index + 1
                ),
                None,
            ));
        }
        if let Some(generated) = referenced.iter().find(|name| {
            schema
                .columns
                .iter()
                .any(|column| column.name == **name && column.generated_expr.is_some())
        }) {
            return Err(SqlError::RaisedException {
                sqlstate: "42P17".into(),
                message: "cannot use generated column in partition key".into(),
                detail: Some(format!("Column \"{generated}\" is a generated column.")),
            });
        }
        let routine_used = match mutable_key_expression(db, &expr) {
            Err(error) => return Err(error),
            Ok(routine_used) => routine_used,
        };
        if referenced.is_empty() {
            return Err(SqlError::data_exception(
                "42P17",
                "cannot use constant expression as partition key",
                None,
            ));
        }
        if routine_used {
            return Err(SqlError::Unsupported(
                "user-defined functions in partition key expressions are not supported".into(),
            ));
        }
        key_expression_column(schema, sql, &expr)?;
    }
    Ok(())
}

fn is_system_column_name(name: &str) -> bool {
    matches!(
        name,
        "tableoid" | "ctid" | "xmin" | "xmax" | "cmin" | "cmax" | "oid"
    )
}

/// Subqueries, aggregates, window functions and set-returning functions in a
/// key expression, with PostgreSQL's texts for EXPR_KIND_PARTITION_EXPRESSION.
fn forbidden_key_expression(db: &BicDb, expr: &Expr) -> Option<SqlError> {
    use sqlparser::ast::visit_expressions;
    use std::ops::ControlFlow;
    let mut error = None;
    let _ = visit_expressions(expr, |expr| {
        let found = match expr {
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => Some(
                SqlError::Unsupported("cannot use subquery in partition key expression".into()),
            ),
            Expr::Function(function) if function.over.is_some() => Some(SqlError::data_exception(
                "42P20",
                "window functions are not allowed in partition key expressions",
                None,
            )),
            Expr::Function(function) if is_aggregate_function(function) => {
                Some(SqlError::data_exception(
                    "42803",
                    "aggregate functions are not allowed in partition key expressions",
                    None,
                ))
            }
            Expr::Function(function)
                if object_name(&function.name).is_ok_and(|name| {
                    is_set_returning_function_name(&name)
                        || load_routine(db, RoutineKind::Function, &name)
                            .ok()
                            .flatten()
                            .is_some_and(|routine| routine.returns_set)
                }) =>
            {
                Some(SqlError::Unsupported(
                    "set-returning functions are not allowed in partition key expressions".into(),
                ))
            }
            _ => None,
        };
        if let Some(found) = found {
            error = Some(found);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    error
}

fn is_set_returning_function_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "generate_series"
            | "generate_subscripts"
            | "unnest"
            | "regexp_matches"
            | "regexp_split_to_table"
            | "json_array_elements"
            | "jsonb_array_elements"
            | "json_each"
            | "jsonb_each"
    )
}

/// Key expressions are evaluated at every routing decision, so they must be
/// IMMUTABLE as in PostgreSQL. Functions known to be volatile or stable get
/// PostgreSQL's error; a function or operator whose immutability BicDB cannot
/// vouch for (user-defined routines included) is refused as unsupported
/// rather than admitted with possibly different routing.
fn mutable_key_expression(db: &BicDb, expr: &Expr) -> Result<bool> {
    use sqlparser::ast::visit_expressions;
    use std::ops::ControlFlow;
    let mut routine_used = false;
    let immutable_error = || {
        SqlError::data_exception(
            "42P17",
            "functions in partition key expression must be marked IMMUTABLE",
            None,
        )
    };
    let mut error = None;
    let _ = visit_expressions(expr, |expr| {
        let found = match expr {
            Expr::Function(function) => {
                let name = object_name(&function.name)
                    .map(|name| name.to_ascii_lowercase())
                    .unwrap_or_default();
                let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
                if expr_contains_volatile_function(expr)
                    || matches!(
                        name,
                        "now"
                            | "current_date"
                            | "current_time"
                            | "current_timestamp"
                            | "localtime"
                            | "localtimestamp"
                            | "timeofday"
                            | "transaction_timestamp"
                            | "current_user"
                            | "session_user"
                            | "current_setting"
                            | "to_char"
                            | "to_date"
                            | "to_timestamp"
                            | "age"
                            | "date_trunc"
                            | "nextval"
                            | "currval"
                            | "setseed"
                    )
                {
                    Some(immutable_error())
                } else if let Some(routine) = (!IMMUTABLE_KEY_FUNCTIONS.contains(&name))
                    .then(|| load_routine(db, RoutineKind::Function, name).ok().flatten())
                    .flatten()
                {
                    // A user-defined function: PostgreSQL's volatility rule
                    // decides the error; an IMMUTABLE one is still refused
                    // later (after the constant-key rule) as unsupported.
                    if routine.declared_volatility() == "i" {
                        routine_used = true;
                        None
                    } else {
                        Some(immutable_error())
                    }
                } else if !IMMUTABLE_KEY_FUNCTIONS.contains(&name) {
                    Some(SqlError::Unsupported(format!(
                        "function {name} in a partition key expression is not supported"
                    )))
                } else {
                    None
                }
            }
            Expr::Cast { data_type, .. } => {
                let target = data_type.to_string().to_ascii_lowercase();
                if [
                    "date",
                    "time",
                    "timestamp",
                    "interval",
                    "reg",
                    "json",
                    "xml",
                    "money",
                ]
                .iter()
                .any(|prefix| target.starts_with(prefix))
                {
                    Some(SqlError::Unsupported(format!(
                        "cast to {target} in a partition key expression is not supported"
                    )))
                } else {
                    None
                }
            }
            Expr::Value(_)
            | Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Substring { .. }
            | Expr::Nested(_)
            | Expr::UnaryOp { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Case { .. } => None,
            Expr::BinaryOp { op, .. } => match op {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo
                | BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::And
                | BinaryOperator::Or => None,
                other => Some(SqlError::Unsupported(format!(
                    "operator {other} in a partition key expression is not supported"
                ))),
            },
            other => Some(SqlError::Unsupported(format!(
                "partition key expression {other} is not supported"
            ))),
        };
        if let Some(found) = found {
            error = Some(found);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    match error {
        Some(error) => Err(error),
        None => Ok(routine_used),
    }
}

/// Built-in functions PostgreSQL marks IMMUTABLE that BicDB evaluates with
/// PostgreSQL's results, admitted in key expressions.
const IMMUTABLE_KEY_FUNCTIONS: &[&str] = &[
    "abs",
    "lower",
    "upper",
    "length",
    "char_length",
    "character_length",
    "octet_length",
    "substr",
    "substring",
    "left",
    "right",
    "trim",
    "btrim",
    "ltrim",
    "rtrim",
    "md5",
    "mod",
    "div",
    "floor",
    "ceil",
    "ceiling",
    "round",
    "trunc",
    "sign",
    "coalesce",
    "nullif",
    "greatest",
    "least",
    "replace",
    "reverse",
    "initcap",
];

fn is_keyword(tokens: &[Token], keyword: &str) -> bool {
    matches!(tokens, [Token::Word(word)] if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword))
}

pub(crate) fn parse_bound(parent: &TableSchema, sql: &str) -> Result<Bound> {
    let owned = key_columns(parent)?;
    let columns = owned.iter().collect::<Vec<_>>();
    let strategy = &parent.partitioning.as_ref().unwrap().strategy;
    let mut tokens = Tokens::new(sql)?;
    if tokens.keyword("DEFAULT") {
        tokens.finish()?;
        if strategy == "hash" {
            return Err(invalid("a hash partition cannot be a default partition"));
        }
        return Ok(Bound::Default);
    }
    tokens.expect("FOR")?;
    tokens.expect("VALUES")?;
    let bound = match strategy.as_str() {
        "range" => {
            tokens.expect("FROM")?;
            let lower = tokens.group()?;
            tokens.expect("TO")?;
            let upper = tokens.group()?;
            let convert = |values: Vec<Vec<Token>>| -> Result<Vec<BoundValue>> {
                if values.len() != columns.len() {
                    return Err(invalid(
                        "partition bound must have one value for each partition key",
                    ));
                }
                let mut tail = None;
                values
                    .iter()
                    .zip(&columns)
                    .map(|(value, column)| {
                        let value = if is_keyword(value, "MINVALUE") {
                            BoundValue::Min
                        } else if is_keyword(value, "MAXVALUE") {
                            BoundValue::Max
                        } else {
                            let value = bound_literal(value, column)?;
                            if matches!(value, SqlValue::Null) {
                                return Err(invalid(
                                    "NULL is not allowed in a range partition bound",
                                ));
                            }
                            BoundValue::Value(value)
                        };
                        let sentinel = match value {
                            BoundValue::Min => Some(false),
                            BoundValue::Max => Some(true),
                            _ => None,
                        };
                        if tail.is_some() && tail != sentinel {
                            return Err(SqlError::data_exception(
                                "42804",
                                if tail == Some(true) {
                                    "every bound following MAXVALUE must also be MAXVALUE"
                                } else {
                                    "every bound following MINVALUE must also be MINVALUE"
                                },
                                None,
                            ));
                        }
                        tail = sentinel;
                        Ok(value)
                    })
                    .collect()
            };
            let lower = convert(lower)?;
            let upper = convert(upper)?;
            if compare_bound_keys(&columns, &lower, &upper)? != Ordering::Less {
                return Err(invalid("empty range partition bound"));
            }
            Bound::Range(lower, upper)
        }
        "list" => {
            if columns.len() != 1 {
                return Err(invalid(
                    "LIST partitioning requires exactly one partition key",
                ));
            }
            tokens.expect("IN")?;
            let mut values = Vec::new();
            for value in tokens.group()? {
                let value = bound_literal(&value, columns[0])?;
                for old in &values {
                    if equal_value(columns[0], old, &value)? {
                        return Err(invalid("duplicate value in list partition bound"));
                    }
                }
                values.push(value);
            }
            Bound::List(values)
        }
        "hash" => {
            tokens.expect("WITH")?;
            let groups = tokens.group()?;
            let mut modulus = None;
            let mut remainder = None;
            for group in groups {
                let [Token::Word(word), Token::Number(value, _)] = group.as_slice() else {
                    return Err(invalid(
                        "hash partition bound expects MODULUS and REMAINDER",
                    ));
                };
                let value = value
                    .parse::<u64>()
                    .map_err(|_| invalid("invalid hash partition bound"))?;
                let slot = if word.value.eq_ignore_ascii_case("MODULUS") {
                    &mut modulus
                } else if word.value.eq_ignore_ascii_case("REMAINDER") {
                    &mut remainder
                } else {
                    return Err(invalid("unknown hash partition bound option"));
                };
                if slot.replace(value).is_some() {
                    return Err(invalid("duplicate hash partition bound option"));
                }
            }
            let (Some(modulus), Some(remainder)) = (modulus, remainder) else {
                return Err(invalid(
                    "hash partition bound requires MODULUS and REMAINDER",
                ));
            };
            if modulus == 0 || remainder >= modulus || modulus > i32::MAX as u64 {
                return Err(invalid("invalid hash partition modulus or remainder"));
            }
            Bound::Hash { modulus, remainder }
        }
        _ => return Err(invalid("unknown partition strategy")),
    };
    tokens.finish()?;
    Ok(bound)
}

fn compare_value(column: &ColumnSchema, left: &SqlValue, right: &SqlValue) -> Result<Ordering> {
    if let Some(Some(true)) = compare_values_with_collation(
        column.collation.as_deref(),
        left,
        &BinaryOperator::Eq,
        right,
    ) {
        return Ok(Ordering::Equal);
    }
    if let Some(Some(less)) = compare_values_with_collation(
        column.collation.as_deref(),
        left,
        &BinaryOperator::Lt,
        right,
    ) {
        return Ok(if less {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }
    type_codec::column_typed_compare(column, left, right)
}

fn equal_value(column: &ColumnSchema, left: &SqlValue, right: &SqlValue) -> Result<bool> {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => Ok(true),
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(false),
        _ => compare_value(column, left, right).map(|order| order == Ordering::Equal),
    }
}

fn compare_bound_keys(
    columns: &[&ColumnSchema],
    left: &[BoundValue],
    right: &[BoundValue],
) -> Result<Ordering> {
    for ((column, left), right) in columns.iter().zip(left).zip(right) {
        let order = match (left, right) {
            (BoundValue::Min, BoundValue::Min) | (BoundValue::Max, BoundValue::Max) => {
                Ordering::Equal
            }
            (BoundValue::Min, _) | (_, BoundValue::Max) => Ordering::Less,
            (BoundValue::Max, _) | (_, BoundValue::Min) => Ordering::Greater,
            (BoundValue::Value(left), BoundValue::Value(right)) => {
                compare_value(column, left, right)?
            }
        };
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(Ordering::Equal)
}

pub(crate) fn bounds_overlap(parent: &TableSchema, left: &Bound, right: &Bound) -> Result<bool> {
    let owned = key_columns(parent)?;
    let columns = owned.iter().collect::<Vec<_>>();
    Ok(match (left, right) {
        (Bound::Default, Bound::Default) => true,
        (Bound::Default, _) | (_, Bound::Default) => false,
        (Bound::Range(a, b), Bound::Range(c, d)) => {
            compare_bound_keys(&columns, a, d)? == Ordering::Less
                && compare_bound_keys(&columns, c, b)? == Ordering::Less
        }
        (Bound::List(a), Bound::List(b)) => {
            let mut overlap = false;
            for a in a {
                for b in b {
                    overlap |= equal_value(columns[0], a, b)?;
                }
            }
            overlap
        }
        (
            Bound::Hash {
                modulus: a,
                remainder: b,
            },
            Bound::Hash {
                modulus: c,
                remainder: d,
            },
        ) => {
            let (small, small_r, large, large_r) = if a <= c {
                (*a, *b, *c, *d)
            } else {
                (*c, *d, *a, *b)
            };
            if large % small != 0 {
                return Err(invalid(
                    "every hash partition modulus must divide the next larger modulus",
                ));
            }
            large_r % small == small_r
        }
        _ => return Err(invalid("partition bound strategy mismatch")),
    })
}

pub(crate) fn children(db: &BicDb, parent: &TableSchema) -> Result<Vec<TableSchema>> {
    // Compare actual stored identities, including encoded namespaces and case.
    Ok(list_schemas(db)?
        .into_iter()
        .filter(|child| {
            child.partition_of.as_ref().is_some_and(|link| {
                link.parent_table == parent.name && link.parent_schema == parent.schema_name
            })
        })
        .collect())
}

/// Preserve source collection identity alongside every heap row. Parent scans
/// include legacy rows stored in the parent's own heap as well as descendants;
/// an upgrade must not silently hide previously acknowledged data.
pub(crate) fn physical_tables(db: &BicDb, parent: &TableSchema) -> Result<Vec<TableSchema>> {
    fn visit(
        db: &BicDb,
        table: &TableSchema,
        seen: &mut BTreeSet<String>,
        out: &mut Vec<TableSchema>,
    ) -> Result<()> {
        if !seen.insert(table.name.clone()) {
            return Err(invalid("cyclic partition tree"));
        }
        out.push(table.clone());
        if table.partitioning.is_some() {
            for child in children(db, table)? {
                visit(db, &child, seen, out)?;
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    visit(db, parent, &mut BTreeSet::new(), &mut out)?;
    Ok(out)
}

/// Preserve visible values when a physical leaf uses a different Native key
/// than the relation named by the statement. Ordinary single-column PK records
/// may store that value only in their Native ID. The transient row image keeps
/// the real physical ID for writes, system columns and row locks.
pub(crate) fn materialize_primary_key_fields(
    schema: &TableSchema,
    record: &mut Record,
) -> Result<()> {
    for column in schema
        .columns
        .iter()
        .filter(|column| column.primary_key && !column.hidden)
    {
        if record.metadata.get(&column.name).is_none() {
            let value = record_column_value(record, schema, &column.name);
            set_json_object_value(
                &mut record.metadata,
                &column.name,
                sql_value_to_column_storage_json(value, Some(column))?,
            );
        }
    }
    Ok(())
}

pub(crate) fn factor_reads_parent(db: &BicDb, factor: &TableFactor) -> Result<bool> {
    match factor {
        TableFactor::Table {
            name, args: None, ..
        } => {
            let table = resolve_session_relation_name_if_exists(db, &relation_name(name)?);
            Ok(load_schema(db, &table)?.is_some_and(|schema| schema.partitioning.is_some()))
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => from_reads_parent(db, table_with_joins),
        // Derived tables and CTEs execute through their own query scope.
        _ => Ok(false),
    }
}

pub(crate) fn from_reads_parent(db: &BicDb, from: &TableWithJoins) -> Result<bool> {
    if factor_reads_parent(db, &from.relation)? {
        return Ok(true);
    }
    for join in &from.joins {
        if factor_reads_parent(db, &join.relation)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn matches_bound(
    db: &BicDb,
    parent: &TableSchema,
    bound: &Bound,
    record: &Record,
) -> Result<bool> {
    let parts = key_parts(parent)?;
    let columns = parts.iter().map(|part| &part.column).collect::<Vec<_>>();
    let values = key_values_of(db, parent, &parts, record)?;
    match bound {
        Bound::Range(lower, upper) => {
            if values.iter().any(|value| matches!(value, SqlValue::Null)) {
                return Ok(false);
            }
            let key = values
                .into_iter()
                .map(BoundValue::Value)
                .collect::<Vec<_>>();
            Ok(compare_bound_keys(&columns, &key, lower)? != Ordering::Less
                && compare_bound_keys(&columns, &key, upper)? == Ordering::Less)
        }
        Bound::List(list) => {
            for value in list {
                if equal_value(columns[0], &values[0], value)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Bound::Default => Err(invalid("default bounds require sibling exclusion")),
        Bound::Hash { modulus, remainder } => {
            Ok(hash::row_hash(&columns, &values)? % modulus == *remainder)
        }
    }
}

pub(crate) fn matches_partition(
    db: &BicDb,
    parent: &TableSchema,
    child: &TableSchema,
    record: &Record,
) -> Result<bool> {
    let link = child
        .partition_of
        .as_ref()
        .ok_or_else(|| invalid("relation is not a partition"))?;
    let bound = parse_bound(parent, &link.bound)?;
    if !matches!(bound, Bound::Default) {
        return matches_bound(db, parent, &bound, record);
    }
    for sibling in children(db, parent)? {
        let bound = parse_bound(parent, &sibling.partition_of.as_ref().unwrap().bound)?;
        if !matches!(bound, Bound::Default) && matches_bound(db, parent, &bound, record)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn validate_record(db: &BicDb, schema: &TableSchema, record: &Record) -> Result<()> {
    let mut current = schema.clone();
    let mut visited = BTreeSet::new();
    while let Some(link) = current.partition_of.as_ref() {
        if !visited.insert(current.name.clone()) {
            return Err(invalid("cyclic partition ancestry"));
        }
        let parent = load_schema(db, &link.parent_table)?
            .ok_or_else(|| invalid("partition parent is missing"))?;
        if parent.schema_name != link.parent_schema
            || !matches_partition(db, &parent, &current, record)?
        {
            return Err(violation(
                &schema.name,
                format!(
                    "new row for relation \"{}\" violates partition constraint",
                    catalog_display_name(&schema.name)
                ),
            ));
        }
        current = parent;
    }
    Ok(())
}

pub(crate) fn route_record(
    db: &BicDb,
    accessed: &TableSchema,
    record: &Record,
) -> Result<TableSchema> {
    validate_record(db, accessed, record)?;
    let mut current = accessed.clone();
    let mut seen = BTreeSet::new();
    while current.partitioning.is_some() {
        if !seen.insert(current.name.clone()) {
            return Err(invalid("cyclic partition tree"));
        }
        let mut target = None;
        for child in children(db, &current)? {
            if matches_partition(db, &current, &child, record)? {
                if target.is_some() {
                    return Err(invalid("row matches multiple partition bounds"));
                }
                target = Some(child);
            }
        }
        current = target.ok_or_else(|| {
            violation(
                &accessed.name,
                format!(
                    "no partition of relation \"{}\" found for row",
                    catalog_display_name(&current.name)
                ),
            )
        })?;
    }
    Ok(current)
}

fn scan(db: &BicDb, tx: Option<&Transaction>, table: &str) -> Result<Vec<Record>> {
    match tx {
        Some(tx) => tx.scan_collection(table).map_err(SqlError::from),
        None => db.scan_collection(table).map_err(SqlError::from),
    }
}

/// Preflight every catalog and row condition before publishing an attachment.
/// Existing acknowledged rows are checked in place; none are moved or erased.
pub(crate) fn validate_attachment(
    db: &BicDb,
    tx: Option<&Transaction>,
    parent: &TableSchema,
    child: &TableSchema,
    allow_missing_new_child: bool,
) -> Result<()> {
    let link = child
        .partition_of
        .as_ref()
        .ok_or_else(|| invalid("missing partition attachment"))?;
    let bound = parse_bound(parent, &link.bound)?;
    let parent_columns = parent
        .columns
        .iter()
        .filter(|column| !column.hidden)
        .collect::<Vec<_>>();
    let child_columns = child
        .columns
        .iter()
        .filter(|column| !column.hidden)
        .collect::<Vec<_>>();
    // Rows are stored by column name, so a partition may order its columns
    // differently (PostgreSQL maps attributes by name); every column must
    // still match the parent's name, type, typmod, collation and NOT NULL.
    let mismatch = |message: String| SqlError::data_exception("42804", message, None);
    let child_name = catalog_display_name(&child.name);
    let child_name = child_name.rsplit('.').next().unwrap_or(&child_name);
    let parent_name = catalog_display_name(&parent.name);
    let parent_name = parent_name.rsplit('.').next().unwrap_or(&parent_name);
    for column in &child_columns {
        if !parent_columns
            .iter()
            .any(|parent| parent.name == column.name)
        {
            return Err(SqlError::RaisedException {
                sqlstate: "42804".to_string(),
                message: format!(
                    "table \"{child_name}\" contains column \"{}\" not found in parent \"{parent_name}\"",
                    column.name
                ),
                detail: Some(
                    "The new partition may contain only the columns present in parent.".into(),
                ),
            });
        }
    }
    for column in &parent_columns {
        let Some(other) = child_columns.iter().find(|child| child.name == column.name) else {
            return Err(mismatch(format!(
                "child table is missing column \"{}\"",
                column.name
            )));
        };
        if column.pg_type != other.pg_type || column.type_modifier != other.type_modifier {
            return Err(mismatch(format!(
                "child table \"{child_name}\" has different type for column \"{}\"",
                column.name
            )));
        }
        if column.collation != other.collation {
            return Err(SqlError::data_exception(
                "42P21",
                format!(
                    "child table \"{child_name}\" has different collation for column \"{}\"",
                    column.name
                ),
                None,
            ));
        }
        if !column.nullable && other.nullable {
            return Err(mismatch(format!(
                "column \"{}\" in child table \"{child_name}\" must be marked NOT NULL",
                column.name
            )));
        }
    }
    // MergeConstraintsIntoExisting: every CHECK of the parent must already
    // be on the partition, under its name and with the same definition.
    for constraint in &parent.constraints {
        let ConstraintSchema::Check {
            name, expression, ..
        } = constraint
        else {
            continue;
        };
        let found = child
            .constraints
            .iter()
            .find_map(|candidate| match candidate {
                ConstraintSchema::Check {
                    name: other,
                    expression,
                    ..
                } if other == name => Some(expression),
                _ => None,
            });
        let Some(other) = found else {
            return Err(mismatch(format!(
                "child table is missing constraint \"{name}\""
            )));
        };
        let same = match (
            parse_check_expression(expression),
            parse_check_expression(other),
        ) {
            (Ok(left), Ok(right)) => left.to_string() == right.to_string(),
            _ => expression == other,
        };
        if !same {
            return Err(mismatch(format!(
                "child table \"{child_name}\" has different definition for check constraint \"{name}\""
            )));
        }
    }
    let mut ancestor = Some(parent.clone());
    let mut visited = BTreeSet::new();
    while let Some(current) = ancestor {
        if current.name == child.name || !visited.insert(current.name.clone()) {
            return Err(invalid("circular partition attachment"));
        }
        ancestor = current
            .partition_of
            .as_ref()
            .map(|link| load_schema(db, &link.parent_table))
            .transpose()?
            .flatten();
    }
    let siblings = children(db, parent)?;
    for sibling in &siblings {
        if sibling.name == child.name {
            return Err(invalid("relation is already a partition of this parent"));
        }
        let other = parse_bound(parent, &sibling.partition_of.as_ref().unwrap().bound)?;
        if matches!((&bound, &other), (Bound::Default, Bound::Default)) {
            return Err(SqlError::data_exception(
                "42P17",
                format!(
                    "partition \"{}\" conflicts with existing default partition \"{}\"",
                    catalog_display_name(&child.name),
                    catalog_display_name(&sibling.name)
                ),
                None,
            ));
        }
        if bounds_overlap(parent, &bound, &other)? {
            return Err(SqlError::data_exception(
                "42P17",
                format!(
                    "partition \"{}\" would overlap partition \"{}\"",
                    catalog_display_name(&child.name),
                    catalog_display_name(&sibling.name)
                ),
                None,
            ));
        }
    }
    fn physical_relations(
        db: &BicDb,
        table: &TableSchema,
        visited: &mut BTreeSet<String>,
        out: &mut Vec<String>,
    ) -> Result<()> {
        if !visited.insert(table.name.clone()) {
            return Err(invalid("cyclic partition tree"));
        }
        out.push(table.name.clone());
        if table.partitioning.is_some() {
            for child in children(db, table)? {
                physical_relations(db, &child, visited, out)?;
            }
        }
        Ok(())
    }
    let mut relations = Vec::new();
    physical_relations(db, child, &mut BTreeSet::new(), &mut relations)?;
    // CREATE PARTITION preflights a not-yet-created empty relation.
    for table in relations {
        let records = match scan(db, tx, &table) {
            Ok(records) => records,
            Err(SqlError::BicDb(BicDbError::CollectionNotFound(_)))
                if allow_missing_new_child && table == child.name =>
            {
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        for record in records {
            if !matches_partition(db, parent, child, &record)? {
                return Err(violation(
                    &child.name,
                    format!(
                        "partition constraint of relation \"{}\" is violated by some row",
                        catalog_display_name(&table)
                    ),
                ));
            }
            validate_record(db, parent, &record)?;
        }
    }
    if !matches!(bound, Bound::Default) {
        for sibling in siblings {
            if !matches!(
                parse_bound(parent, &sibling.partition_of.as_ref().unwrap().bound)?,
                Bound::Default
            ) {
                continue;
            }
            let mut relations = Vec::new();
            physical_relations(db, &sibling, &mut BTreeSet::new(), &mut relations)?;
            for table in relations {
                for record in scan(db, tx, &table)? {
                    if matches_bound(db, parent, &bound, &record)? {
                        return Err(violation(
                            &sibling.name,
                            format!(
                                "updated partition constraint for default partition \"{}\" would be violated by some row",
                                catalog_display_name(&sibling.name)
                            ),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Validate an already attached forest without treating each child as a new
/// attachment. Exact identities and every sibling bound are preflighted.
pub(crate) fn validate_definition(schema: &TableSchema) -> Result<()> {
    if schema.partitioning.is_none() {
        return Ok(());
    }
    if !schema.has_hidden_primary_key() {
        unique_covers_key(
            schema,
            "PRIMARY KEY",
            &primary_key_columns_for_schema(schema),
        )?;
    }
    for constraint in &schema.constraints {
        if let ConstraintSchema::Unique { columns, .. }
        | ConstraintSchema::UniqueWithNullPolicy { columns, .. } = constraint
        {
            unique_covers_key(schema, "UNIQUE", columns)?;
            if constraint.unique_deferral().is_some() {
                return Err(SqlError::Unsupported(
                    "DEFERRABLE unique constraints on partitioned tables are not supported".into(),
                ));
            }
        }
    }
    Ok(())
}

/// PostgreSQL's DefineIndex rule for a unique index on a partitioned table:
/// every key position, in order, must be a plain column the index includes.
pub(crate) fn unique_covers_key(
    schema: &TableSchema,
    constraint_type: &str,
    columns: &[String],
) -> Result<()> {
    let Some(partitioning) = &schema.partitioning else {
        return Ok(());
    };
    for (index, key) in partitioning.key_columns.iter().enumerate() {
        if partitioning.key_expression(index).is_some() {
            return Err(SqlError::RaisedException {
                sqlstate: "0A000".into(),
                message: format!(
                    "unsupported {constraint_type} constraint with partition key definition"
                ),
                detail: Some(format!(
                    "{constraint_type} constraints cannot be used when partition keys include expressions."
                )),
            });
        }
        if !columns.contains(key) {
            let table = catalog_display_name(&schema.name);
            let table = table.rsplit('.').next().unwrap_or(&table).to_string();
            return Err(SqlError::RaisedException {
                sqlstate: "0A000".into(),
                message:
                    "unique constraint on partitioned table must include all partitioning columns"
                        .into(),
                detail: Some(format!(
                    "{constraint_type} constraint on table \"{table}\" lacks column \"{key}\" which is part of the partition key."
                )),
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_existing_tree(db: &BicDb, schemas: &[TableSchema]) -> Result<()> {
    for parent in schemas
        .iter()
        .filter(|schema| schema.partitioning.is_some())
    {
        validate_definition(parent)?;
        let children = schemas
            .iter()
            .filter(|child| {
                child
                    .partition_of
                    .as_ref()
                    .is_some_and(|link| link.parent_table == parent.name)
            })
            .collect::<Vec<_>>();
        for (index, child) in children.iter().enumerate() {
            let link = child.partition_of.as_ref().unwrap();
            if link.parent_schema != parent.schema_name {
                return Err(invalid("partition namespace mismatch"));
            }
            let a = parent
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .collect::<Vec<_>>();
            let b = child
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .collect::<Vec<_>>();
            // Matched by name, as ATTACH admits them (rows are stored by name).
            if a.len() != b.len()
                || a.iter().any(|a| {
                    b.iter().find(|b| b.name == a.name).is_none_or(|b| {
                        a.pg_type != b.pg_type
                            || a.type_modifier != b.type_modifier
                            || a.collation != b.collation
                            || (!a.nullable && b.nullable)
                    })
                })
            {
                return Err(invalid("legacy partition columns do not match parent"));
            }
            let bound = parse_bound(parent, &link.bound)?;
            for other in &children[..index] {
                let other = parse_bound(parent, &other.partition_of.as_ref().unwrap().bound)?;
                if bounds_overlap(parent, &bound, &other)? {
                    return Err(invalid("overlapping legacy partition bounds"));
                }
            }
        }
    }
    let _ = db;
    Ok(())
}
