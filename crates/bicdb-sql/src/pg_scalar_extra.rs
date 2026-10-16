//! PostgreSQL scalar functions that were missing (found running a production PostgreSQL application on
//! BicDB, 2026-10): string (`strpos`, `lpad`, `rpad`, two-argument `ltrim`/`rtrim`/`btrim`, `repeat`,
//! `reverse`, `starts_with`), numeric (`power`, `sqrt`, `cbrt`, `exp`, `ln`, `log`, `ceil`/`ceiling`),
//! regular expressions (`regexp_match`, `regexp_split_to_array`), date/time (`age`, `make_interval`,
//! `date_bin`, `to_date`, `timezone`) and `similarity` (pg_trgm). Semantics follow PostgreSQL 16-18.
use crate::*;
use regex::RegexBuilder;

fn any_null(args: &[SqlValue]) -> bool {
    args.iter().any(|value| matches!(value, SqlValue::Null))
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
            | "timezone"
            | "similarity"
    );
    if !known {
        return Ok(None);
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
