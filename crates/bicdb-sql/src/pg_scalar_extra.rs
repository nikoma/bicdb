//! PostgreSQL scalar functions that were missing (found running a production PostgreSQL application on
//! BicDB, 2026-10): string (`strpos`, `lpad`, `rpad`, two-argument `ltrim`/`rtrim`/`btrim`, `repeat`,
//! `reverse`, `starts_with`), numeric (`power`, `sqrt`, `cbrt`, `exp`, `ln`, `log`, `ceil`/`ceiling`),
//! regular expressions (`regexp_match`, `regexp_split_to_array`), date/time (`age`, `make_interval`,
//! `date_bin`, `to_date`, `timezone`) and `similarity` (pg_trgm). Semantics follow PostgreSQL 16-18.
use crate::*;
use regex::RegexBuilder;
mod oracle_chr;
mod oracle_concat;
mod oracle_decode;
mod oracle_nvl;

fn any_null(args: &[SqlValue]) -> bool {
    args.iter().any(|value| matches!(value, SqlValue::Null))
}

pub(crate) fn oracle_date_coerce(value: SqlValue) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(value);
    }
    let timestamp = PgTimestamp::from_postgres_text(&value.to_cell(), false)
        .map_err(|error| postgres_timestamp_input_error(&value.to_cell(), error))?;
    let micros = timestamp.finite_micros().ok_or_else(|| {
        SqlError::invalid_parameter_value("Oracle DATE requires a finite datetime")
    })?;
    let timestamp = PgTimestamp::Finite(micros.div_euclid(1_000_000) * 1_000_000);
    let year = timestamp
        .components()
        .ok_or_else(|| {
            SqlError::data_exception(
                "22008",
                "Oracle DATE is out of range",
                Some("timestamp".into()),
            )
        })?
        .0;
    if !(-4711..=9999).contains(&year) {
        return Err(SqlError::data_exception(
            "22008",
            "Oracle DATE is out of range",
            Some("timestamp".into()),
        ));
    }
    Ok(SqlValue::String(timestamp.to_iso_text(false)))
}

fn oracle_week_trunc(value: &SqlValue, format: &str) -> Result<SqlValue> {
    use chrono::Datelike;
    let value = oracle_date_coerce(value.clone())?;
    let source = PgTimestamp::from_postgres_text(&value.to_cell(), false)
        .map_err(|error| postgres_timestamp_input_error(&value.to_cell(), error))?;
    let (year, month, day, ..) = source.components().expect("finite coerced DATE");
    let epoch_day = days_from_civil(year, month, day);
    let offset = match format {
        "W" => i64::from((day - 1) % 7),
        "WW" => (epoch_day - days_from_civil(year, 1, 1)).rem_euclid(7),
        // The preview's default territory is AMERICA; other NLS territories
        // require an explicit future session contract.
        "D" | "DY" | "DAY" => (epoch_day + 4).rem_euclid(7),
        "I" | "IY" | "IYY" | "IYYY" => {
            let iso_year = chrono::NaiveDate::from_ymd_opt(year, month, day)
                .expect("valid DATE")
                .iso_week()
                .year();
            let january_fourth = days_from_civil(iso_year, 1, 4);
            let start = january_fourth - (january_fourth + 3).rem_euclid(7);
            epoch_day - start
        }
        _ => unreachable!("selected week format"),
    };
    let midnight = source
        .truncate("day")
        .map_err(|error| postgres_timestamp_input_error(&value.to_cell(), error))?;
    let result = midnight
        .checked_sub_interval(PgInterval {
            months: 0,
            days: offset as i32,
            micros: 0,
        })
        .map_err(|_| {
            SqlError::data_exception(
                "22008",
                "Oracle datetime TRUNC result is out of range",
                None,
            )
        })?;
    oracle_date_coerce(SqlValue::String(result.to_iso_text(false)))
}

fn oracle_add_months(date: &SqlValue, months: &SqlValue) -> Result<SqlValue> {
    let range = || {
        SqlError::data_exception(
            "22008",
            "Oracle ADD_MONTHS result is out of range",
            Some("timestamp".into()),
        )
    };
    let date = oracle_date_coerce(date.clone())?;
    let timestamp = PgTimestamp::from_postgres_text(&date.to_cell(), false)
        .map_err(|error| postgres_timestamp_input_error(&date.to_cell(), error))?;
    let (year, month, day, ..) = timestamp.components().ok_or_else(range)?;
    let count = parse_decimal(&months.to_cell())?;
    // Oracle truncates the month count toward zero, without float conversion.
    let months = (count.mantissa / pow10_bigint(count.scale))
        .to_i32()
        .ok_or_else(range)?;
    let mut shifted = timestamp
        .checked_add_interval(PgInterval {
            months,
            days: 0,
            micros: 0,
        })
        .map_err(|_| range())?;
    let (target_year, target_month, target_day, ..) = shifted.components().ok_or_else(range)?;
    if !(-4711..=9999).contains(&target_year) {
        return Err(range());
    }
    // A source month end always maps to the target month end. PostgreSQL's
    // ordinary interval addition only clamps a day that exceeds the target.
    if i64::from(day) == days_in_month(year, month) {
        let days = (days_in_month(target_year, target_month) - i64::from(target_day)) as i32;
        shifted = shifted
            .checked_add_interval(PgInterval {
                months: 0,
                days,
                micros: 0,
            })
            .map_err(|_| range())?;
    }
    oracle_date_coerce(SqlValue::String(shifted.to_iso_text(false)))
}

fn int_arg(name: &str, value: &SqlValue) -> Result<i64> {
    match value {
        SqlValue::Int(number) => Ok(*number),
        SqlValue::Float(number) if number.fract() == 0.0 => Ok(*number as i64),
        other => other.to_cell().trim().parse::<i64>().map_err(|_| {
            SqlError::invalid_parameter_value(format!("{name}: integer argument expected"))
        }),
    }
}

fn float_arg(name: &str, value: &SqlValue) -> Result<f64> {
    sql_value_f64(value)
        .or_else(|| value.to_cell().trim().parse::<f64>().ok())
        .ok_or_else(|| {
            SqlError::invalid_parameter_value(format!("{name}: numeric argument expected"))
        })
}

fn arity(name: &str, args: &[SqlValue], allowed: std::ops::RangeInclusive<usize>) -> Result<()> {
    if allowed.contains(&args.len()) {
        Ok(())
    } else {
        Err(SqlError::InvalidSql(format!(
            "function {name} does not accept {} argument(s)",
            args.len()
        )))
    }
}

fn oracle_number_divide(left: &SqlValue, right: &SqlValue) -> Result<SqlValue> {
    let left = parse_decimal(&left.to_cell())?;
    let right = parse_decimal(&right.to_cell())?;
    if right.mantissa.is_zero() {
        return Err(division_by_zero());
    }
    if left.mantissa.is_zero() {
        return Ok(SqlValue::String("0".into()));
    }
    let overflow =
        || SqlError::numeric_value_out_of_range("Oracle NUMBER division is out of range");
    let exponent = decimal_quotient_exponent(&left, &right)?;
    if !(-130..=125).contains(&exponent) {
        return Err(overflow());
    }
    // Twenty base-100 mantissa digits give 39 or 40 decimal digits,
    // depending on the position of the leading decimal digit.
    let scale = 38 - 2 * exponent.div_euclid(2);
    let shift = i64::from(scale) + i64::from(right.scale) - i64::from(left.scale);
    let (numerator, denominator) = if shift >= 0 {
        (left.mantissa * pow10_bigint(shift as u32), right.mantissa)
    } else {
        (
            left.mantissa,
            right.mantissa * pow10_bigint((-shift) as u32),
        )
    };
    let mut quotient = &numerator / &denominator;
    let remainder = (&numerator % &denominator).abs();
    if &remainder * 2 >= denominator.abs() {
        quotient += if numerator.sign() == denominator.sign() {
            1
        } else {
            -1
        };
    }
    let value = if scale >= 0 {
        Decimal {
            mantissa: quotient,
            scale: scale as u32,
        }
    } else {
        Decimal {
            mantissa: quotient * pow10_bigint((-scale) as u32),
            scale: 0,
        }
    };
    let rounded_exponent =
        value.mantissa.abs().to_string().len() as i64 - i64::from(value.scale) - 1;
    if rounded_exponent > 125 {
        return Err(overflow());
    }
    Ok(SqlValue::String(value.format()))
}

fn oracle_number_arithmetic(
    left: &SqlValue,
    right: &SqlValue,
    operator: &str,
    context: &str,
) -> Result<SqlValue> {
    let procedural = match context {
        "plsql" => true,
        "sql" => false,
        _ => {
            return Err(SqlError::Unsupported(
                "unknown Oracle NUMBER arithmetic context".into(),
            ))
        }
    };
    let infinity = |value: &SqlValue| match value.to_cell().as_str() {
        "Infinity" => Some(false),
        "-Infinity" => Some(true),
        _ => None,
    };
    let left_inf = infinity(left);
    let right_inf = infinity(right);
    let left = if left_inf.is_none() {
        Some(parse_decimal(&left.to_cell())?)
    } else {
        None
    };
    let right = if right_inf.is_none() {
        Some(parse_decimal(&right.to_cell())?)
    } else {
        None
    };
    let zero = |value: &Option<Decimal>| value.as_ref().is_some_and(|v| v.mantissa.is_zero());
    let negative = |value: &Option<Decimal>, inf: Option<bool>| {
        inf.unwrap_or_else(|| value.as_ref().is_some_and(|v| v.mantissa.is_negative()))
    };
    let overflow =
        || SqlError::numeric_value_out_of_range("Oracle NUMBER arithmetic is out of range");
    let finish_infinity = |negative: bool| {
        if procedural {
            Ok(SqlValue::String(
                if negative { "-Infinity" } else { "Infinity" }.into(),
            ))
        } else {
            Err(overflow())
        }
    };
    if operator == "/" && zero(&right) {
        return Err(division_by_zero());
    }
    if left_inf.is_some() || right_inf.is_some() {
        // Oracle NUMBER's infinity operations differ from IEEE arithmetic.
        // These rules are independently measured on the recorded reference.
        let sign = match operator {
            "+" => left_inf.or(right_inf).expect("infinite operand"),
            "-" if left_inf == Some(false) && right_inf == Some(false) => {
                return Ok(SqlValue::String("0".into()))
            }
            "-" => left_inf.unwrap_or_else(|| !right_inf.expect("right infinite")),
            "*" if zero(&left) || zero(&right) => return Ok(SqlValue::String("0".into())),
            "*" => negative(&left, left_inf) != negative(&right, right_inf),
            "/" if !procedural && left_inf.is_none() && right_inf == Some(true) && !zero(&left) => {
                return Err(overflow())
            }
            "/" if left_inf.is_none() => return Ok(SqlValue::String("0".into())),
            "/" => negative(&left, left_inf) != negative(&right, right_inf),
            _ => {
                return Err(SqlError::Unsupported(
                    "unsupported Oracle NUMBER arithmetic operator".into(),
                ))
            }
        };
        return finish_infinity(sign);
    }
    let left = left.expect("finite left");
    let right = right.expect("finite right");
    let canonical_result = |value: SqlValue| -> Result<SqlValue> {
        let mut text = value.to_cell();
        if text.contains('.') {
            text = text.trim_end_matches('0').trim_end_matches('.').to_string();
        }
        Ok(SqlValue::String(text))
    };
    if operator == "/" {
        if left.mantissa.is_zero() {
            return Ok(SqlValue::String("0".into()));
        }
        let exponent = decimal_quotient_exponent(&left, &right)?;
        if exponent < -130 {
            return Ok(SqlValue::String("0".into()));
        }
        let sign = left.mantissa.is_negative() != right.mantissa.is_negative();
        if exponent > 125 {
            return finish_infinity(sign);
        }
        return match oracle_number_divide(
            &SqlValue::String(left.format()),
            &SqlValue::String(right.format()),
        ) {
            Ok(value) => canonical_result(value),
            Err(error) if error.sqlstate() == "22003" => finish_infinity(sign),
            Err(error) => Err(error),
        };
    }
    let value = match operator {
        "+" | "-" => {
            let scale = left.scale.max(right.scale);
            let left = left.mantissa * pow10_bigint(scale - left.scale);
            let right = right.mantissa * pow10_bigint(scale - right.scale);
            Decimal {
                mantissa: if operator == "+" {
                    left + right
                } else {
                    left - right
                },
                scale,
            }
        }
        "*" => Decimal {
            mantissa: left.mantissa * right.mantissa,
            scale: left.scale.checked_add(right.scale).ok_or_else(overflow)?,
        },
        _ => {
            return Err(SqlError::Unsupported(
                "unsupported Oracle NUMBER arithmetic operator".into(),
            ))
        }
    };
    if !value.mantissa.is_zero() {
        let exponent = value.mantissa.abs().to_string().len() as i64 - i64::from(value.scale) - 1;
        if exponent < -130 {
            return Ok(SqlValue::String("0".into()));
        }
        if exponent > 125 {
            return finish_infinity(value.mantissa.is_negative());
        }
    }
    let sign = value.mantissa.is_negative();
    match oracle_number_divide(&SqlValue::String(value.format()), &SqlValue::Int(1)) {
        Ok(value) => canonical_result(value),
        Err(error) if error.sqlstate() == "22003" => finish_infinity(sign),
        Err(error) => Err(error),
    }
}

fn pad(text: &str, length: i64, fill: &str, left: bool) -> String {
    let chars = text.chars().collect::<Vec<_>>();
    let length = length.max(0) as usize;
    if length <= chars.len() || fill.is_empty() {
        return chars[..length.min(chars.len())].iter().collect();
    }
    let padding = fill
        .chars()
        .cycle()
        .take(length - chars.len())
        .collect::<String>();
    if left {
        format!("{padding}{text}")
    } else {
        format!("{text}{padding}")
    }
}

fn trim_chars(text: &str, set: &str, left: bool, right: bool) -> String {
    let set = set.chars().collect::<Vec<_>>();
    let mut out = text;
    if left {
        out = out.trim_start_matches(|ch| set.contains(&ch));
    }
    if right {
        out = out.trim_end_matches(|ch| set.contains(&ch));
    }
    out.to_string()
}

fn pg_regex(pattern: &str, flags: &str) -> Result<regex::Regex> {
    let mut builder = RegexBuilder::new(pattern);
    for flag in flags.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            'c' => {
                builder.case_insensitive(false);
            }
            'n' | 'm' => {
                builder.multi_line(true);
                builder.dot_matches_new_line(false);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'x' => {
                builder.ignore_whitespace(true);
            }
            'g' => {
                return Err(SqlError::invalid_parameter_value(
                    "regexp_match() does not support the \"global\" option",
                ))
            }
            other => {
                return Err(SqlError::invalid_parameter_value(format!(
                    "invalid regular expression option: \"{other}\""
                )))
            }
        }
    }
    builder.build().map_err(|error| {
        SqlError::invalid_parameter_value(format!("invalid regular expression: {error}"))
    })
}

/// Microseconds since 2000-01-01 UTC of a timestamp/timestamptz text value.
fn timestamp_micros(value: &SqlValue) -> Result<i64> {
    let text = value.to_cell();
    parse_timestamptz(&text)
        .map_err(|error| postgres_timestamptz_input_error(&text, error))?
        .finite_micros()
        .ok_or_else(|| SqlError::invalid_parameter_value("timestamp out of range"))
}

/// Whether a timestamp text carries an explicit UTC offset (timestamptz rendering).
fn has_offset(text: &str) -> bool {
    let text = text.trim();
    text.ends_with('Z')
        || text.rfind(['+', '-']).is_some_and(|at| {
            at > 10
                && text[at + 1..]
                    .chars()
                    .all(|ch| ch.is_ascii_digit() || ch == ':')
        })
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn local_parts(micros: i64) -> Result<chrono::NaiveDateTime> {
    let zone = current_timezone_name();
    let offset = timezone_offset_at_timestamp(&zone, PgTimestamp::Finite(micros)).unwrap_or(0);
    let unix = micros + 946_684_800_000_000 + i64::from(offset) * 1_000_000;
    chrono::DateTime::<chrono::Utc>::from_timestamp(
        unix.div_euclid(1_000_000),
        (unix.rem_euclid(1_000_000) * 1000) as u32,
    )
    .map(|value| value.naive_utc())
    .ok_or_else(|| SqlError::invalid_parameter_value("timestamp out of range"))
}

fn days_in_month(year: i32, month: u32) -> i64 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .and_then(|next| next.pred_opt())
        .map(|last| i64::from(chrono::Datelike::day(&last)))
        .unwrap_or(30)
}

/// PostgreSQL `age(a, b)`: symbolic years/months/days plus time, borrowing like `timestamp_age`.
fn age(a: i64, b: i64) -> Result<PgInterval> {
    use chrono::{Datelike, Timelike};
    if a < b {
        let positive = age(b, a)?;
        return Ok(PgInterval {
            months: -positive.months,
            days: -positive.days,
            micros: -positive.micros,
        });
    }
    let (x, y) = (local_parts(a)?, local_parts(b)?);
    let mut micros = i64::from(x.nanosecond() / 1000) - i64::from(y.nanosecond() / 1000);
    let mut seconds = i64::from(x.second()) - i64::from(y.second());
    let mut minutes = i64::from(x.minute()) - i64::from(y.minute());
    let mut hours = i64::from(x.hour()) - i64::from(y.hour());
    let mut days = i64::from(x.day()) - i64::from(y.day());
    let mut months = i64::from(x.month()) - i64::from(y.month());
    let mut years = i64::from(x.year()) - i64::from(y.year());
    if micros < 0 {
        micros += 1_000_000;
        seconds -= 1;
    }
    if seconds < 0 {
        seconds += 60;
        minutes -= 1;
    }
    if minutes < 0 {
        minutes += 60;
        hours -= 1;
    }
    if hours < 0 {
        hours += 24;
        days -= 1;
    }
    if days < 0 {
        days += days_in_month(y.year(), y.month());
        months -= 1;
    }
    if months < 0 {
        months += 12;
        years -= 1;
    }
    Ok(PgInterval {
        months: (years * 12 + months) as i32,
        days: days as i32,
        micros: ((hours * 60 + minutes) * 60 + seconds) * 1_000_000 + micros,
    })
}

fn render_like_source(micros: i64, with_zone: bool) -> String {
    if with_zone {
        render_timestamptz(PgTimestamp::Finite(micros))
    } else {
        PgTimestamp::Finite(micros)
            .to_iso_text(false)
            .replace('T', " ")
    }
}

/// pg_trgm's trigrams: lower-cased alphanumeric words, each padded with two spaces before and one
/// after, cut into every three-character window.
fn trigram_set(text: &str) -> Result<std::collections::BTreeSet<String>> {
    let mut set = std::collections::BTreeSet::new();
    for word in text
        .to_lowercase()
        .split(|ch: char| !ch.is_alphanumeric())
        .filter(|word| !word.is_empty())
    {
        let padded = format!("  {word} ").chars().collect::<Vec<_>>();
        for window in padded.windows(3) {
            set.insert(window.iter().collect::<String>());
        }
    }
    Ok(set)
}

pub(crate) fn eval_pg_scalar_extra(name: &str, args: &[SqlValue]) -> Result<Option<SqlValue>> {
    if matches!(
        name,
        "bicdb_oracle_current_sqlcode" | "bicdb_oracle_current_sqlerrm"
    ) {
        arity(name, args, 0..=0)?;
        return oracle::errors::current_diagnostic(name).map(Some);
    }
    if name.starts_with("bicdb_oracle_nvl_") {
        return oracle_nvl::evaluate(name, args).map(Some);
    }
    if name == "bicdb_oracle_chr" {
        return oracle_chr::evaluate(args).map(Some);
    }
    if matches!(
        name,
        "bicdb_oracle_decode_key"
            | "bicdb_oracle_decode_number"
            | "bicdb_oracle_decode_text"
            | "bicdb_oracle_decode_date"
            | "bicdb_oracle_decode_timestamp"
    ) {
        return oracle_decode::evaluate(name, args).map(Some);
    }
    if name == "bicdb_oracle_default_to_char" {
        return oracle_concat::default_to_char(args).map(Some);
    }
    if name == "bicdb_oracle_concat" {
        // Unlike ordinary strict scalar functions, NULL is an empty operand.
        return oracle_concat::evaluate(args).map(Some);
    }
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    let known = matches!(
        name,
        "strpos"
            | "lpad"
            | "rpad"
            | "ltrim"
            | "rtrim"
            | "btrim"
            | "repeat"
            | "reverse"
            | "starts_with"
            | "power"
            | "pow"
            | "sqrt"
            | "cbrt"
            | "exp"
            | "ln"
            | "log"
            | "ceil"
            | "ceiling"
            | "floor"
            | "regexp_match"
            | "regexp_split_to_array"
            | "age"
            | "make_interval"
            | "date_bin"
            | "to_date"
            | "bicdb_oracle_to_date"
            | "bicdb_oracle_trunc_date"
            | "bicdb_oracle_plsql_trunc_date"
            | "bicdb_oracle_date_add_days"
            | "bicdb_oracle_number_divide"
            | "bicdb_oracle_number_arithmetic"
            | "bicdb_oracle_number_trunc"
            | "bicdb_oracle_plsql_number_trunc"
            | "bicdb_oracle_date_coerce"
            | "bicdb_oracle_date_subtract"
            | "bicdb_oracle_add_months"
            | "timezone"
            | "similarity"
    );
    if !known {
        return Ok(None);
    }
    if matches!(
        name,
        "bicdb_oracle_trunc_date" | "bicdb_oracle_plsql_trunc_date"
    ) {
        arity(name, args, 1..=2)?;
    }
    if name == "bicdb_oracle_date_add_days" {
        arity(name, args, 3..=3)?;
    }
    if matches!(
        name,
        "bicdb_oracle_number_trunc" | "bicdb_oracle_plsql_number_trunc"
    ) {
        arity(name, args, 1..=2)?;
    }
    if name == "bicdb_oracle_number_arithmetic" {
        arity(name, args, 4..=4)?;
    }
    if name == "bicdb_oracle_number_divide" {
        arity(name, args, 2..=2)?;
    }
    if name == "bicdb_oracle_date_coerce" {
        arity(name, args, 1..=1)?;
    }
    if matches!(
        name,
        "bicdb_oracle_date_subtract" | "bicdb_oracle_add_months"
    ) {
        arity(name, args, 2..=2)?;
    }
    // PL/SQL validates a supplied NULL date format after the date argument;
    // SQL TRUNC and numeric TRUNC retain their ordinary strict NULL behavior.
    if name == "bicdb_oracle_plsql_trunc_date"
        && !matches!(args[0], SqlValue::Null)
        && args.get(1).is_some_and(|arg| matches!(arg, SqlValue::Null))
    {
        return Err(SqlError::invalid_parameter_value(
            "Oracle procedural datetime TRUNC requires a non-NULL precision specifier",
        ));
    }
    // make_interval and age(ts) accept missing/NULL-free forms; everything else is strict.
    if name != "make_interval" && any_null(args) {
        return Ok(Some(SqlValue::Null));
    }
    let text = |index: usize| args[index].to_cell();
    let value = match name {
        "strpos" => {
            arity(name, args, 2..=2)?;
            let (haystack, needle) = (text(0), text(1));
            SqlValue::Int(match haystack.find(&needle) {
                Some(byte) => haystack[..byte].chars().count() as i64 + 1,
                None => 0,
            })
        }
        "lpad" | "rpad" => {
            arity(name, args, 2..=3)?;
            let fill = if args.len() == 3 {
                text(2)
            } else {
                " ".to_string()
            };
            SqlValue::String(pad(
                &text(0),
                int_arg(name, &args[1])?,
                &fill,
                name == "lpad",
            ))
        }
        "ltrim" | "rtrim" | "btrim" => {
            arity(name, args, 1..=2)?;
            let set = if args.len() == 2 {
                text(1)
            } else {
                " ".to_string()
            };
            SqlValue::String(trim_chars(&text(0), &set, name != "rtrim", name != "ltrim"))
        }
        "repeat" => {
            arity(name, args, 2..=2)?;
            let count = int_arg(name, &args[1])?.max(0) as usize;
            SqlValue::String(text(0).repeat(count))
        }
        "reverse" => {
            arity(name, args, 1..=1)?;
            SqlValue::String(text(0).chars().rev().collect())
        }
        "starts_with" => {
            arity(name, args, 2..=2)?;
            SqlValue::Bool(text(0).starts_with(&text(1)))
        }
        "power" | "pow" => {
            arity(name, args, 2..=2)?;
            let (base, exponent) = (float_arg(name, &args[0])?, float_arg(name, &args[1])?);
            if base == 0.0 && exponent < 0.0 {
                return Err(SqlError::data_exception(
                    "2201F",
                    "zero raised to a negative power is undefined",
                    None,
                ));
            }
            if base < 0.0 && exponent.fract() != 0.0 {
                return Err(SqlError::data_exception(
                    "2201F",
                    "a negative number raised to a non-integer power yields a complex result",
                    None,
                ));
            }
            SqlValue::Float(base.powf(exponent))
        }
        "sqrt" => {
            arity(name, args, 1..=1)?;
            let value = float_arg(name, &args[0])?;
            if value < 0.0 {
                return Err(SqlError::data_exception(
                    "2201F",
                    "cannot take square root of a negative number",
                    None,
                ));
            }
            SqlValue::Float(value.sqrt())
        }
        "cbrt" => {
            arity(name, args, 1..=1)?;
            SqlValue::Float(float_arg(name, &args[0])?.cbrt())
        }
        "exp" => {
            arity(name, args, 1..=1)?;
            SqlValue::Float(float_arg(name, &args[0])?.exp())
        }
        "ln" | "log" => {
            arity(name, args, 1..=2)?;
            let (base, value) = if args.len() == 2 {
                (float_arg(name, &args[0])?, float_arg(name, &args[1])?)
            } else {
                (
                    if name == "ln" {
                        std::f64::consts::E
                    } else {
                        10.0
                    },
                    float_arg(name, &args[0])?,
                )
            };
            if value <= 0.0 || base <= 0.0 || base == 1.0 {
                return Err(SqlError::data_exception(
                    "2201E",
                    "cannot take logarithm of zero or a negative number",
                    None,
                ));
            }
            SqlValue::Float(value.ln() / base.ln())
        }
        "ceil" | "ceiling" | "floor" => {
            arity(name, args, 1..=1)?;
            let up = name != "floor";
            match &args[0] {
                SqlValue::Int(number) => SqlValue::Int(*number),
                SqlValue::Float(number) => {
                    SqlValue::Float(if up { number.ceil() } else { number.floor() })
                }
                other => {
                    // Exact numeric text: round up in decimal so large values keep their digits.
                    let digits = other.to_cell();
                    let trimmed = digits.trim();
                    match trimmed.split_once('.') {
                        None => SqlValue::String(trimmed.to_string()),
                        Some((whole, fraction)) => {
                            let whole_value = whole.parse::<i128>().map_err(|_| {
                                SqlError::invalid_parameter_value("ceil: numeric argument expected")
                            })?;
                            // Truncation moves toward zero; ceil adds one for positives, floor
                            // subtracts one for negatives, when a fraction is present.
                            let fractional = fraction.chars().any(|ch| ch != '0');
                            let negative = whole.starts_with('-');
                            let adjust = match (up, negative) {
                                (true, false) if fractional => 1,
                                (false, true) if fractional => -1,
                                _ => 0,
                            };
                            let result = whole_value + adjust;
                            SqlValue::String(if result == 0 && negative && up {
                                "0".to_string()
                            } else {
                                result.to_string()
                            })
                        }
                    }
                }
            }
        }
        "regexp_match" => {
            arity(name, args, 2..=3)?;
            let flags = if args.len() == 3 {
                text(2)
            } else {
                String::new()
            };
            let regex = pg_regex(&text(1), &flags)?;
            let source = text(0);
            match regex.captures(&source) {
                None => SqlValue::Null,
                Some(captures) if captures.len() == 1 => {
                    array_value(vec![SqlValue::String(captures[0].to_string())])
                }
                Some(captures) => array_value(
                    captures
                        .iter()
                        .skip(1)
                        .map(|group| {
                            group.map_or(SqlValue::Null, |m| {
                                SqlValue::String(m.as_str().to_string())
                            })
                        })
                        .collect(),
                ),
            }
        }
        "regexp_split_to_array" => {
            arity(name, args, 2..=3)?;
            array_value(regexp_split_to_table_values(args)?)
        }
        "age" => {
            arity(name, args, 1..=2)?;
            let interval = if args.len() == 2 {
                age(timestamp_micros(&args[0])?, timestamp_micros(&args[1])?)?
            } else {
                // age(ts) = age(current_date at midnight, ts) in the session time zone.
                let now = timestamp_micros(&SqlValue::String(unix_now_date_string()))?;
                age(now, timestamp_micros(&args[0])?)?
            };
            SqlValue::String(render_interval(interval))
        }
        "make_interval" => {
            arity(name, args, 0..=7)?;
            let field = |index: usize| -> Result<f64> {
                match args.get(index) {
                    None | Some(SqlValue::Null) => Ok(0.0),
                    Some(value) => float_arg(name, value),
                }
            };
            if any_null(args) {
                return Ok(Some(SqlValue::Null));
            }
            let (years, months, weeks, days) = (
                field(0)? as i64,
                field(1)? as i64,
                field(2)? as i64,
                field(3)? as i64,
            );
            let (hours, minutes, seconds) = (field(4)? as i64, field(5)? as i64, field(6)?);
            SqlValue::String(render_interval(PgInterval {
                months: (years * 12 + months) as i32,
                days: (weeks * 7 + days) as i32,
                micros: (hours * 3600 + minutes * 60) * 1_000_000
                    + (seconds * 1_000_000.0).round() as i64,
            }))
        }
        "date_bin" => {
            arity(name, args, 3..=3)?;
            let stride = PgInterval::from_postgres_text(&text(0))
                .map_err(|_| SqlError::invalid_parameter_value("date_bin: invalid stride"))?;
            if stride.months != 0 {
                return Err(SqlError::Unsupported(
                    "timestamps cannot be binned into intervals containing months or years"
                        .to_string(),
                ));
            }
            let stride = i64::from(stride.days) * MICROS_PER_DAY + stride.micros;
            if stride <= 0 {
                return Err(SqlError::invalid_parameter_value(
                    "stride must be greater than zero",
                ));
            }
            let (source, origin) = (timestamp_micros(&args[1])?, timestamp_micros(&args[2])?);
            let binned = origin + (source - origin).div_euclid(stride) * stride;
            SqlValue::String(render_like_source(binned, has_offset(&text(1))))
        }
        // Oracle DATE includes whole-second time, unlike PostgreSQL date.
        // The explicit Oracle front end selects this separate function, also
        // inside stored procedure bodies. PostgreSQL to_date stays unchanged.
        "bicdb_oracle_to_date" => {
            arity(name, args, 2..=2)?;
            eval_to_timestamp(args)?
        }
        "bicdb_oracle_date_coerce" => oracle_date_coerce(args[0].clone())?,
        "bicdb_oracle_add_months" => oracle_add_months(&args[0], &args[1])?,
        "bicdb_oracle_date_subtract" => {
            let left = oracle_date_coerce(args[0].clone())?;
            let right = oracle_date_coerce(args[1].clone())?;
            let left = PgTimestamp::from_postgres_text(&left.to_cell(), false)
                .map_err(|error| postgres_timestamp_input_error(&left.to_cell(), error))?
                .finite_micros()
                .expect("coerced DATE");
            let right = PgTimestamp::from_postgres_text(&right.to_cell(), false)
                .map_err(|error| postgres_timestamp_input_error(&right.to_cell(), error))?
                .finite_micros()
                .expect("coerced DATE");
            let seconds = left.checked_sub(right).ok_or_else(|| {
                SqlError::data_exception("22008", "Oracle DATE subtraction is out of range", None)
            })? / 1_000_000;
            oracle_number_divide(
                &SqlValue::String(seconds.to_string()),
                &SqlValue::Int(86_400),
            )?
        }
        "bicdb_oracle_number_divide" => oracle_number_divide(&args[0], &args[1])?,
        "bicdb_oracle_number_arithmetic" => {
            oracle_number_arithmetic(&args[0], &args[1], &args[2].to_cell(), &args[3].to_cell())?
        }
        "bicdb_oracle_number_trunc" | "bicdb_oracle_plsql_number_trunc" => {
            let scale = if let Some(value) = args.get(1) {
                let value = parse_decimal(&value.to_cell())?;
                let integer = if name == "bicdb_oracle_plsql_number_trunc" {
                    // PL/SQL's builtin precision parameter is an integer;
                    // conversion rounds halfway away from zero, unlike SQL.
                    round_decimal_half_away(value, 0).parse::<i32>().ok()
                } else {
                    (value.mantissa / pow10_bigint(value.scale)).to_i32()
                };
                integer.ok_or_else(|| {
                    SqlError::numeric_value_out_of_range("Oracle TRUNC precision is out of range")
                })?
            } else {
                0
            };
            truncate_numeric_value(&args[0], i64::from(scale), false)?
        }
        "bicdb_oracle_date_add_days" => {
            let out_of_range = || {
                SqlError::data_exception(
                    "22008",
                    "Oracle DATE arithmetic is out of range",
                    Some("timestamp".into()),
                )
            };
            let date = PgTimestamp::from_postgres_text(&text(0), false)
                .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
            let Some(micros) = date.finite_micros() else {
                return Err(out_of_range());
            };
            let mut days = parse_decimal(&text(1))?;
            days.mantissa *= 86_400_i32;
            let seconds = decimal_to_i64_rounded(days).map_err(|_| out_of_range())?;
            let direction = int_arg(name, &args[2])?;
            if !matches!(direction, -1 | 1) {
                return Err(SqlError::invalid_parameter_value(
                    "Oracle DATE day direction must be -1 or 1",
                ));
            }
            // Oracle numeric timestamp arithmetic converts to DATE first,
            // discarding fractional seconds. Offset rounding is exact and
            // symmetric around zero, including sub-second day fractions.
            let micros = micros
                .div_euclid(1_000_000)
                .checked_mul(1_000_000)
                .and_then(|base| {
                    seconds
                        .checked_mul(direction)?
                        .checked_mul(1_000_000)?
                        .checked_add(base)
                })
                .ok_or_else(out_of_range)?;
            let result = PgTimestamp::Finite(micros);
            let year = result.components().ok_or_else(out_of_range)?.0;
            if !(-4711..=9999).contains(&year) {
                return Err(out_of_range());
            }
            SqlValue::String(result.to_iso_text(false))
        }
        "bicdb_oracle_trunc_date" | "bicdb_oracle_plsql_trunc_date" => {
            let format = args
                .get(1)
                .map(SqlValue::to_cell)
                .unwrap_or_else(|| "DD".into())
                .to_ascii_uppercase();
            if matches!(
                format.as_str(),
                "W" | "WW" | "D" | "DY" | "DAY" | "I" | "IY" | "IYY" | "IYYY"
            ) {
                return oracle_week_trunc(&args[0], &format).map(Some);
            }
            let field = match format.as_str() {
                "DD" | "DDD" | "J" => "day",
                "MM" | "MON" | "MONTH" | "RM" => "month",
                "YYYY" | "YYY" | "YY" | "Y" | "YEAR" | "SYEAR" | "SYYYY" => "year",
                "Q" => "quarter",
                "IW" => "week",
                "HH" | "HH12" | "HH24" => "hour",
                "MI" => "minute",
                "CC" | "SCC" => "century",
                _ => {
                    return Err(SqlError::Unsupported(format!(
                        "Oracle datetime TRUNC format {format} is not implemented"
                    )))
                }
            };
            let source = PgTimestamp::from_postgres_text(&text(0), false)
                .map_err(|error| postgres_timestamp_input_error(&text(0), error))?;
            if source.finite_micros().is_none() {
                return Err(SqlError::invalid_parameter_value(
                    "Oracle datetime TRUNC requires a finite datetime",
                ));
            }
            SqlValue::String(
                source
                    .truncate(field)
                    .map_err(|error| postgres_timestamp_input_error(&text(0), error))?
                    .to_iso_text(false),
            )
        }
        "to_date" => {
            arity(name, args, 2..=2)?;
            let parts = parse_timestamp_template(&text(0), &text(1))?;
            let year = parts.year.ok_or_else(|| {
                SqlError::invalid_parameter_value("to_date: format must include a year")
            })?;
            let date = chrono::NaiveDate::from_ymd_opt(
                year,
                parts.month.unwrap_or(1),
                parts.day.unwrap_or(1),
            )
            .ok_or_else(|| {
                SqlError::data_exception("22008", "date/time field value out of range", None)
            })?;
            SqlValue::String(date.format("%Y-%m-%d").to_string())
        }
        "timezone" => {
            arity(name, args, 2..=2)?;
            let source_type = if has_offset(&text(1)) {
                "timestamptz"
            } else {
                "timestamp"
            };
            eval_at_time_zone_value(args[1].clone(), args[0].clone(), Some(source_type))?
        }
        "similarity" => {
            arity(name, args, 2..=2)?;
            let (left, right) = (trigram_set(&text(0))?, trigram_set(&text(1))?);
            let union = left.union(&right).count();
            SqlValue::Float(if union == 0 {
                0.0
            } else {
                left.intersection(&right).count() as f64 / union as f64
            })
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}
