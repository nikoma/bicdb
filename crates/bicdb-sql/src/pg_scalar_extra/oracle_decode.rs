//! Runtime conversions for the preview's type-directed DECODE lowering.
use super::*;

fn is_null(value: &SqlValue) -> bool {
    matches!(value, SqlValue::Null) || matches!(value, SqlValue::String(text) if text.is_empty())
}

fn number(value: &SqlValue, source: &str) -> Result<SqlValue> {
    let text = value.to_cell();
    if source == "number" && matches!(text.as_str(), "Infinity" | "-Infinity") {
        return Ok(value.clone());
    }
    if source == "text" {
        static NUMERIC: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
            regex::Regex::new(r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$")
                .expect("numeric input pattern")
        });
        if !NUMERIC.is_match(text.trim()) {
            return Err(SqlError::InvalidTextRepresentation(
                "invalid Oracle NUMBER input in DECODE".into(),
            ));
        }
    }
    oracle_number_arithmetic(value, &SqlValue::Int(1), "*", "sql")
}

fn timestamp(value: &SqlValue, source: &str, date: bool) -> Result<SqlValue> {
    let text = if source == "text" {
        if !date {
            return Err(SqlError::Unsupported(
                "implicit character-to-TIMESTAMP DECODE conversion is not qualified".into(),
            ));
        }
        default_date_input(&value.to_cell())?
    } else {
        value.to_cell()
    };
    if date {
        oracle_date_coerce(SqlValue::String(text))
    } else {
        let stamp = PgTimestamp::from_postgres_text(&text, false)
            .map_err(|error| postgres_timestamp_input_error(&text, error))?;
        let (year, ..) = stamp.components().ok_or_else(|| {
            SqlError::invalid_parameter_value("Oracle TIMESTAMP requires a finite datetime")
        })?;
        if !(-4711..=9999).contains(&year) {
            return Err(SqlError::data_exception(
                "22008",
                "Oracle TIMESTAMP is out of range",
                None,
            ));
        }
        Ok(SqlValue::String(stamp.to_iso_text(false)))
    }
}

pub(super) fn default_date_input(text: &str) -> Result<String> {
    use chrono::Datelike;
    // The preview fixes NLS_DATE_FORMAT=DD-MON-RR and NLS_DATE_LANGUAGE=AMERICAN.
    // Oracle permits punctuation substitution, omitted punctuation and a full
    // month name. Its RR window follows the current year, not PostgreSQL's
    // two-digit-year parser.
    static DATE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?i)^\s*([0-9]{1,2})[^a-z0-9]*([a-z]+)[^a-z0-9]*([0-9]{1,4})\s*$")
            .expect("default Oracle DATE pattern")
    });
    let invalid = || SqlError::data_exception("22007", "invalid Oracle DATE input in DECODE", None);
    let captures = DATE.captures(text).ok_or_else(invalid)?;
    let day = captures[1].parse::<u32>().map_err(|_| invalid())?;
    let name = captures[2].to_ascii_uppercase();
    let months = [
        "JANUARY",
        "FEBRUARY",
        "MARCH",
        "APRIL",
        "MAY",
        "JUNE",
        "JULY",
        "AUGUST",
        "SEPTEMBER",
        "OCTOBER",
        "NOVEMBER",
        "DECEMBER",
    ];
    let month = months
        .iter()
        .position(|month| *month == name || month.get(..3) == Some(name.as_str()))
        .ok_or_else(invalid)? as u32
        + 1;
    let mut year = captures[3].parse::<i32>().map_err(|_| invalid())?;
    if captures[3].len() <= 2 {
        let current = chrono::Utc::now().year();
        year += current / 100 * 100;
        if current % 100 <= 49 && year % 100 >= 50 {
            year -= 100;
        } else if current % 100 >= 50 && year % 100 <= 49 {
            year += 100;
        }
    }
    chrono::NaiveDate::from_ymd_opt(year, month, day).ok_or_else(invalid)?;
    Ok(format!("{year:04}-{month:02}-{day:02} 00:00:00"))
}

fn text(value: &SqlValue, source: &str) -> Result<SqlValue> {
    if source != "timestamp" {
        return oracle_concat::default_to_char(&[
            value.clone(),
            SqlValue::String(source.into()),
            SqlValue::String("sql".into()),
        ]);
    }
    let stamp = PgTimestamp::from_postgres_text(&value.to_cell(), false)
        .map_err(|error| postgres_timestamp_input_error(&value.to_cell(), error))?;
    let (_, _, _, hour, minute, second, micros) = stamp.components().ok_or_else(|| {
        SqlError::invalid_parameter_value("Oracle TIMESTAMP requires a finite datetime")
    })?;
    let date = oracle_concat::default_to_char(&[
        value.clone(),
        SqlValue::String("date".into()),
        SqlValue::String("sql".into()),
    ])?
    .to_cell();
    Ok(SqlValue::String(format!(
        "{date} {:02}.{minute:02}.{second:02}.{micros:06}000 {}",
        if hour % 12 == 0 { 12 } else { hour % 12 },
        if hour < 12 { "AM" } else { "PM" }
    )))
}

pub(super) fn evaluate(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    let key = name == "bicdb_oracle_decode_key";
    arity(name, args, if key { 3..=3 } else { 2..=2 })?;
    if is_null(&args[0]) {
        return Ok(if key {
            SqlValue::String("null:".into())
        } else {
            SqlValue::Null
        });
    }
    let source = args[1].to_cell();
    let target = if key {
        args[2].to_cell()
    } else {
        name.trim_start_matches("bicdb_oracle_decode_").into()
    };
    let value = match target.as_str() {
        "text" => text(&args[0], &source)?,
        "number" => number(&args[0], &source)?,
        "date" => timestamp(&args[0], &source, true)?,
        "timestamp" => timestamp(&args[0], &source, false)?,
        _ => {
            return Err(SqlError::Unsupported(
                "unknown Oracle DECODE conversion".into(),
            ))
        }
    };
    if !key {
        return Ok(value);
    }
    let mut text = value.to_cell();
    if target == "number" && !matches!(text.as_str(), "Infinity" | "-Infinity") {
        let mut decimal = parse_decimal(&text)?;
        while decimal.scale > 0 && (&decimal.mantissa % 10u32).is_zero() {
            decimal.mantissa /= 10u32;
            decimal.scale -= 1;
        }
        text = decimal.format();
    }
    Ok(SqlValue::String(format!("{target}:{text}")))
}
