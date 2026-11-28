//! Soft input-error evaluation behind `pg_input_is_valid(text, type)` and
//! `pg_input_error_info(text, type)`.
//!
//! PostgreSQL runs the type's input function with an error-saving context:
//! data errors raised by the input function (invalid syntax, out of range,
//! typmod overflow, domain CHECK failures) are reported as values, while
//! everything else (an undefined type, an error raised while evaluating a
//! domain CHECK expression) is still raised.
use super::*;
#[allow(unused_imports)]
use crate::*;

/// One saved input error, in the shape `pg_input_error_info` returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SoftInputError {
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub sql_error_code: String,
}

/// Columns of `pg_input_error_info`, in PostgreSQL's order.
pub(crate) const PG_INPUT_ERROR_INFO_COLUMNS: [&str; 4] =
    ["message", "detail", "hint", "sql_error_code"];

pub(crate) fn is_pg_input_error_info_function(name: &str) -> bool {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    name.eq_ignore_ascii_case("pg_input_error_info")
}

/// Marks an error raised while evaluating a domain CHECK expression, which
/// PostgreSQL does not trap in a soft-error context.
pub(crate) fn hard_input_error(error: SqlError) -> SqlError {
    SqlError::Rethrown(std::sync::Arc::new(error))
}

/// Parses `type_name` the way `parseTypeString` does and runs `text` through
/// that type's input conversion. `Ok(None)` means the input is valid.
pub(crate) fn soft_input_error(
    db: Option<&BicDb>,
    text: &str,
    type_name: &str,
) -> Result<Option<SoftInputError>> {
    let data_type = parse_input_type_name(type_name)?;
    let input = SqlValue::String(text.to_string());
    // The relative date/time inputs are always valid for their types.
    if let Ok((pg_type, _)) = pg_type_from_data_type(&data_type) {
        let special = text.trim().to_ascii_lowercase();
        if matches!(pg_type.as_str(), "date" | "timestamp" | "timestamptz")
            && matches!(special.as_str(), "now" | "today" | "tomorrow" | "yesterday")
        {
            return Ok(None);
        }
    }
    if let Some(converted) = pg_type_from_data_type(&data_type)
        .ok()
        .and_then(|(pg_type, _)| text_input_integer_value(&input, &pg_type))
    {
        return match converted {
            Ok(_) => Ok(None),
            Err(error) => Ok(Some(saved_input_error(&error))),
        };
    }
    let builtin = pg_type_from_data_type(&data_type)
        .ok()
        .map(|(pg_type, _)| pg_type);
    let result = match (db, builtin.clone()) {
        (_, Some(pg_type)) if pg_type == "xml" && crate::xml::xml_input_is_document() => {
            crate::xml::xml_input_value(input)
        }
        // A built-in type's input function applies its type modifier the
        // way assignment does (`char(4)` rejects five characters, `bit(10)`
        // rejects eight digits) rather than truncating like an explicit cast.
        (_, Some(pg_type))
            if !is_oid_alias_type(&pg_type)
                && !pg_type.strip_suffix("[]").is_some_and(is_oid_alias_type) =>
        {
            cast_value_with_assignment_semantics(input, &data_type)
        }
        (Some(db), _) => {
            if builtin.is_none() && user_type_column_from_data_type(db, &data_type)?.is_none() {
                return Err(undefined_input_type(type_name, &data_type));
            }
            cast_value_with_db(db, input, &data_type)
        }
        (None, Some(_)) => cast_value(input, &data_type),
        (None, None) => return Err(undefined_input_type(type_name, &data_type)),
    };
    match result {
        Ok(_) => Ok(None),
        Err(error) if is_soft_input_error(&error) => Ok(Some(saved_input_error(&error))),
        Err(error) => Err(error),
    }
}

fn parse_input_type_name(type_name: &str) -> Result<DataType> {
    let mut parser = sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
        .try_with_sql(type_name)
        .map_err(|_| invalid_type_name(type_name))?;
    let data_type = parser
        .parse_data_type()
        .map_err(|_| invalid_type_name(type_name))?;
    if parser.peek_token().token != sqlparser::tokenizer::Token::EOF {
        return Err(invalid_type_name(type_name));
    }
    Ok(data_type)
}

fn invalid_type_name(type_name: &str) -> SqlError {
    SqlError::InvalidSql(format!("invalid type name \"{type_name}\""))
}

fn undefined_input_type(type_name: &str, data_type: &DataType) -> SqlError {
    let name = match data_type {
        DataType::Custom(name, _) => name.to_string().to_ascii_lowercase(),
        _ => type_name.trim().to_ascii_lowercase(),
    };
    SqlError::undefined_object(format!("type \"{name}\" does not exist"))
}

/// Errors the input conversion reports about the text are soft: data
/// exceptions, domain constraint failures, and the syntax errors of input
/// functions such as `tsvector`'s. An unsupported or undefined type, an
/// error from a domain CHECK expression and internal failures are not.
fn is_soft_input_error(error: &SqlError) -> bool {
    match error {
        SqlError::WithNotices { error, .. } | SqlError::Detailed { error, .. } => {
            is_soft_input_error(error)
        }
        SqlError::Rethrown(_)
        | SqlError::OracleError { .. }
        | SqlError::OracleContext { .. }
        | SqlError::Unsupported(_)
        | SqlError::UnsupportedPlpgsqlFeature(_)
        | SqlError::BicDb(_)
        | SqlError::InvalidSql(_)
        | SqlError::Json(_) => false,
        error => {
            let sqlstate = error.sqlstate();
            sqlstate.starts_with("22") || sqlstate.starts_with("23") || sqlstate == "42601"
        }
    }
}

fn saved_input_error(error: &SqlError) -> SoftInputError {
    let mut detail = None;
    let mut hint = None;
    for field in error.fields() {
        match field {
            SqlErrorField::Detail(text) if detail.is_none() => detail = Some(text),
            SqlErrorField::Hint(text) if hint.is_none() => hint = Some(text),
            _ => {}
        }
    }
    SoftInputError {
        message: error.to_string(),
        detail,
        hint,
        sql_error_code: error.sqlstate().to_string(),
    }
}

/// `pg_input_is_valid(text, type)`: strict, so NULL in gives NULL out.
pub(crate) fn eval_pg_input_is_valid(db: Option<&BicDb>, args: &[SqlValue]) -> Result<SqlValue> {
    if args.len() != 2 {
        return Err(SqlError::undefined_function(format!(
            "function pg_input_is_valid() expects 2 arguments, got {}",
            args.len()
        )));
    }
    if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let invalid = soft_input_error(db, &args[0].to_cell(), &args[1].to_cell())?;
    Ok(SqlValue::Bool(invalid.is_none()))
}

/// `pg_input_error_info(text, type)` as its one output row.
pub(crate) fn eval_pg_input_error_info_row(
    db: Option<&BicDb>,
    args: &[SqlValue],
) -> Result<Vec<SqlValue>> {
    if args.len() != 2 {
        return Err(SqlError::undefined_function(format!(
            "function pg_input_error_info() expects 2 arguments, got {}",
            args.len()
        )));
    }
    if args.iter().any(|arg| matches!(arg, SqlValue::Null)) {
        return Ok(vec![SqlValue::Null; 4]);
    }
    let text = |value: Option<String>| value.map(SqlValue::String).unwrap_or(SqlValue::Null);
    Ok(
        match soft_input_error(db, &args[0].to_cell(), &args[1].to_cell())? {
            None => vec![SqlValue::Null; 4],
            Some(error) => vec![
                SqlValue::String(error.message),
                text(error.detail),
                text(error.hint),
                SqlValue::String(error.sql_error_code),
            ],
        },
    )
}
