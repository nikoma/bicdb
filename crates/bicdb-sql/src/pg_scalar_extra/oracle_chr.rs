//! CHR under the preview's fixed AL32UTF8 database character set.
use super::*;

pub(super) fn evaluate(args: &[SqlValue]) -> Result<SqlValue> {
    arity("bicdb_oracle_chr", args, 1..=1)?;
    let value = &args[0];
    if matches!(value, SqlValue::Null) || matches!(value, SqlValue::String(text) if text.is_empty())
    {
        return Ok(SqlValue::Null);
    }
    let text = value.to_cell();
    if matches!(text.as_str(), "Infinity" | "-Infinity" | "NaN") {
        return Err(SqlError::numeric_value_out_of_range(
            "Oracle CHR argument is out of range",
        ));
    }
    let number = parse_decimal(text.trim())?;
    let nonzero = !number.mantissa.is_zero();
    let negative = number.mantissa.is_negative();
    let integer = number.mantissa / pow10_bigint(number.scale);
    if negative || nonzero && integer.is_zero() {
        return Err(SqlError::numeric_value_out_of_range(
            "Oracle CHR argument is out of range",
        ));
    }
    let code = integer.to_u32().ok_or_else(|| {
        SqlError::numeric_value_out_of_range("Oracle CHR argument is out of range")
    })?;
    if code == 0 {
        return Err(SqlError::Unsupported(
            "Oracle CHR NUL cannot be represented as PostgreSQL text".into(),
        ));
    }
    let bytes = code.to_be_bytes();
    let start = bytes
        .iter()
        .position(|byte| *byte != 0)
        .expect("nonzero code");
    let character = std::str::from_utf8(&bytes[start..]).map_err(|_| {
        SqlError::Unsupported("Oracle CHR requires one complete AL32UTF8 character; invalid source code points are indeterminate".into())
    })?;
    if character.chars().count() != 1 {
        return Err(SqlError::Unsupported(
            "Oracle CHR requires one complete AL32UTF8 character".into(),
        ));
    }
    Ok(SqlValue::String(character.into()))
}
