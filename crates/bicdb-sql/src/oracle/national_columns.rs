//! Durable AL16UTF16 national table declarations under the STANDARD profile.
//! Routine buffers retain their separate OracleNationalV1 compiler contract.
use super::*;
pub(crate) fn modifier(db: &BicDb, data_type: &DataType) -> Result<Option<PgTypeModifier>> {
    if !preview_routine_language_enabled(db)? {
        return Ok(None);
    }
    let source = tokens(&data_type.to_string())?;
    let Some(Token::Word(word)) = source.first() else {
        return Ok(None);
    };
    let is = |name: &str| {
        if word.quote_style.is_some() {
            word.value == name
        } else {
            word.value.eq_ignore_ascii_case(name)
        }
    };
    let fixed = if is("NCHAR") {
        true
    } else if is("NVARCHAR2") {
        false
    } else {
        return Ok(None);
    };
    let length = match &source[1..] {
        [] if fixed => 1,
        [Token::LParen, Token::Number(n, _), Token::RParen] => n
            .parse::<u32>()
            .map_err(|_| errors::database_error(-907, "42601", "missing right parenthesis"))?,
        _ => {
            return Err(errors::database_error(
                -907,
                "42601",
                "national column declaration requires a scalar length",
            ))
        }
    };
    let maximum = if fixed { 1000 } else { 2000 };
    if length == 0 || length > maximum {
        return Err(errors::database_error(
            -910,
            "42601",
            "specified length too long for its datatype",
        ));
    }
    Ok(Some(PgTypeModifier::OracleNationalColumnV1 {
        fixed,
        length,
    }))
}
pub(crate) fn physical_type(modifier: &PgTypeModifier) -> &'static str {
    if matches!(
        modifier,
        PgTypeModifier::OracleNationalColumnV1 { fixed: true, .. }
    ) {
        "bpchar"
    } else {
        "varchar"
    }
}
pub(crate) fn coerce(value: SqlValue, fixed: bool, length: u32) -> Result<SqlValue> {
    let mut text = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::String(value) => value,
        _ => {
            return Err(unsupported(
                "national column assignment requires resolved text",
            ))
        }
    };
    if text.is_empty() {
        return Ok(SqlValue::Null);
    }
    let units = text.encode_utf16().count();
    if units > length as usize {
        return Err(errors::database_error(
            -12899,
            "22001",
            "value too large for Oracle national character column",
        ));
    }
    if fixed {
        text.extend(std::iter::repeat(' ').take(length as usize - units));
    }
    Ok(SqlValue::String(text))
}
pub(crate) fn validate(db: &BicDb, schema: &TableSchema) -> Result<()> {
    for column in &schema.columns {
        if let Some(modifier @ PgTypeModifier::OracleNationalColumnV1 { fixed, length }) =
            &column.type_modifier
        {
            if column.pg_type != physical_type(modifier)
                || column.user_type.is_some()
                || column.array_ndims != 0
                || *length == 0
                || *length > if *fixed { 1000 } else { 2000 }
                || !metadata_v9_enabled(db)?
            {
                return Err(unsupported(
                    "invalid or unfenced Oracle national column metadata",
                ));
            }
        }
    }
    Ok(())
}
pub(crate) fn prepare_save(db: &mut BicDb, schema: &TableSchema) -> Result<()> {
    if schema.columns.iter().any(|column| {
        matches!(
            column.type_modifier,
            Some(PgTypeModifier::OracleNationalColumnV1 { .. })
        )
    }) {
        enable_metadata_v9(db)?;
    }
    validate(db, schema)
}
