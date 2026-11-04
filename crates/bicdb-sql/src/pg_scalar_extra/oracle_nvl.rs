//! Eager NVL values and fixed-preview implicit character/datetime conversions.
use super::*;

fn is_null(value: &SqlValue) -> bool {
    matches!(value, SqlValue::Null) || matches!(value,SqlValue::String(text) if text.is_empty())
}

fn timestamp_text(value: &SqlValue, precision: u8, context: &str) -> Result<SqlValue> {
    if precision > 9 {
        return Err(SqlError::Unsupported(
            "NVL TIMESTAMP precision is not qualified".into(),
        ));
    }
    let stamp = PgTimestamp::from_postgres_text(&value.to_cell(), false)
        .map_err(|error| postgres_timestamp_input_error(&value.to_cell(), error))?;
    let (_, _, _, hour, minute, second, micros) = stamp
        .components()
        .ok_or_else(|| SqlError::invalid_parameter_value("NVL requires a finite timestamp"))?;
    let date = oracle_concat::default_to_char(&[
        value.clone(),
        SqlValue::String("date".into()),
        SqlValue::String(context.into()),
    ])?
    .to_cell();
    let fraction = format!("{micros:06}000");
    let fraction = if precision == 0 {
        String::new()
    } else {
        format!(".{}", &fraction[..precision as usize])
    };
    Ok(SqlValue::String(format!(
        "{date} {:02}.{minute:02}.{second:02}{fraction} {}",
        if hour % 12 == 0 { 12 } else { hour % 12 },
        if hour < 12 { "AM" } else { "PM" }
    )))
}

fn timestamp_input(value: &SqlValue, source: &str) -> Result<SqlValue> {
    if is_null(value) {
        return Ok(SqlValue::Null);
    }
    if source == "text_literal" {
        return Err(SqlError::data_exception(
            "42804",
            "incompatible character operand for constant Oracle TIMESTAMP NVL",
            None,
        ));
    }
    if !matches!(source, "text" | "text_literal") {
        return oracle_decode::evaluate(
            "bicdb_oracle_decode_timestamp",
            &[value.clone(), SqlValue::String(source.into())],
        );
    }
    let invalid = || {
        SqlError::data_exception(
            "22007",
            "invalid default Oracle TIMESTAMP input in NVL",
            None,
        )
    };
    static FORMAT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?i)^\s*(.+?)\s+([0-9]{1,2})[.:]([0-9]{1,2})[.:]([0-9]{1,2})(?:[.,]([0-9]{1,9}))?\s+(AM|PM)\s*$").expect("default timestamp pattern")
    });
    let text = value.to_cell();
    let Some(captures) = FORMAT.captures(&text) else {
        // Oracle's fixed default timestamp input also accepts a date with the
        // time omitted. The omitted fields are midnight, not the current time.
        return oracle_decode::default_date_input(&text)
            .map(SqlValue::String)
            .map_err(|_| invalid());
    };
    let date = oracle_decode::default_date_input(&captures[1]).map_err(|_| invalid())?;
    let mut hour = captures[2].parse::<u8>().map_err(|_| invalid())?;
    let minute = captures[3].parse::<u8>().map_err(|_| invalid())?;
    let second = captures[4].parse::<u8>().map_err(|_| invalid())?;
    if !(1..=12).contains(&hour) || minute > 59 || second > 59 {
        return Err(invalid());
    }
    hour = hour % 12
        + if captures[6].eq_ignore_ascii_case("PM") {
            12
        } else {
            0
        };
    let fraction = captures.get(5).map_or("", |value| value.as_str());
    if fraction.len() > 6 && fraction[6..].bytes().any(|b| b != b'0') {
        return Err(SqlError::Unsupported(
            "NVL timestamps above six fractional digits are not supported".into(),
        ));
    }
    let fraction = &fraction[..fraction.len().min(6)];
    let date = date.split(' ').next().expect("date component");
    let text = format!(
        "{date} {hour:02}:{minute:02}:{second:02}.{}",
        if fraction.is_empty() { "0" } else { fraction }
    );
    let stamp = PgTimestamp::from_postgres_text(&text, false).map_err(|_| invalid())?;
    Ok(SqlValue::String(stamp.to_iso_text(false)))
}

pub(super) fn evaluate(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    match name {
        "bicdb_oracle_nvl_convert_text" => {
            arity(name, args, 3..=3)?;
            if is_null(&args[0]) {
                return Ok(SqlValue::Null);
            }
            let source = args[1].to_cell();
            let context = args[2].to_cell();
            if let Some(precision) = source.strip_prefix("timestamp:") {
                return timestamp_text(
                    &args[0],
                    precision.parse().map_err(|_| {
                        SqlError::InvalidSql("invalid NVL timestamp precision".into())
                    })?,
                    &context,
                );
            }
            let value = if source == "number" {
                oracle_decode::evaluate(
                    "bicdb_oracle_decode_number",
                    &[args[0].clone(), SqlValue::String("number".into())],
                )?
            } else {
                args[0].clone()
            };
            oracle_concat::default_to_char(&[
                value,
                SqlValue::String(source),
                SqlValue::String(context),
            ])
        }
        "bicdb_oracle_nvl_convert_timestamp" => {
            arity(name, args, 2..=2)?;
            timestamp_input(&args[0], &args[1].to_cell())
        }
        "bicdb_oracle_nvl2_text"
        | "bicdb_oracle_nvl2_number"
        | "bicdb_oracle_nvl2_date"
        | "bicdb_oracle_nvl2_timestamp" => {
            arity(
                name,
                args,
                if name.ends_with("_timestamp") {
                    4..=4
                } else {
                    3..=3
                },
            )?;
            // All three argument expressions and result conversions have
            // already run once, in order, through the generic evaluator.
            Ok(if is_null(&args[0]) {
                args[2].clone()
            } else {
                args[1].clone()
            })
        }
        "bicdb_oracle_nvl_text"
        | "bicdb_oracle_nvl_number"
        | "bicdb_oracle_nvl_date"
        | "bicdb_oracle_nvl_timestamp" => {
            arity(
                name,
                args,
                if name.ends_with("_timestamp") {
                    3..=3
                } else {
                    2..=2
                },
            )?;
            // The generic function evaluators already evaluated and converted
            // both arguments; selection must not turn NVL into COALESCE.
            Ok(if is_null(&args[0]) {
                args[1].clone()
            } else {
                args[0].clone()
            })
        }
        _ => Err(SqlError::Unsupported("unknown Oracle NVL helper".into())),
    }
}
