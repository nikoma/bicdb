//! PostgreSQL scalar functions shared by every evaluator (FROM-less SELECT,
//! row evaluation, routines): integer formatting and arithmetic, hyperbolic
//! functions, numeric scale helpers, `isfinite`, and the POSIX regular
//! expression functions `regexp_count/instr/like/substr`.
use crate::*;

fn bare(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

/// Result type of a function implemented here, given its argument types.
pub(crate) fn pg_function_more_result_type(
    name: &str,
    arg_types: &[Option<String>],
) -> Option<String> {
    let first = arg_types.first().cloned().flatten();
    Some(
        match bare(name) {
            "to_hex" | "to_oct" | "to_bin" => "text",
            "gcd" | "lcm" => {
                return Some(match first.as_deref() {
                    Some("int8") => "int8".to_string(),
                    Some("int2" | "int4") | None => {
                        if arg_types.iter().flatten().any(|t| t == "int8") {
                            "int8".to_string()
                        } else {
                            "int4".to_string()
                        }
                    }
                    Some(other) => other.to_string(),
                })
            }
            "num_nulls" | "num_nonnulls" => "int4",
            "sinh" | "cosh" | "tanh" | "asinh" | "acosh" | "atanh" => "float8",
            "getdatabaseencoding" => "name",
            "isfinite" => "bool",
            "scale" | "min_scale" => "int4",
            "trim_scale" => "numeric",
            "regexp_count" | "regexp_instr" => "int4",
            "regexp_like" => "bool",
            "regexp_substr" => "text",
            _ => return None,
        }
        .to_string(),
    )
}

fn integer_width(value: i64, pg_type: Option<&str>) -> u32 {
    match pg_type {
        Some("int8") => 64,
        Some("int2" | "int4") => 32,
        _ if i32::try_from(value).is_ok() => 32,
        _ => 64,
    }
}

fn integer_arg(name: &str, value: &SqlValue) -> Result<Option<i64>> {
    Ok(match value {
        SqlValue::Null => None,
        SqlValue::Int(number) => Some(*number),
        other => Some(other.to_cell().trim().parse::<i64>().map_err(|_| {
            SqlError::undefined_function(format!("function {name}(unknown) does not exist"))
        })?),
    })
}

fn radix_text(value: i64, width: u32, radix: u32) -> String {
    let unsigned = if width == 32 {
        u64::from(value as i32 as u32)
    } else {
        value as u64
    };
    match radix {
        16 => format!("{unsigned:x}"),
        8 => format!("{unsigned:o}"),
        _ => format!("{unsigned:b}"),
    }
}

fn integer_gcd(left: i64, right: i64) -> i64 {
    let (mut a, mut b) = (left, right);
    // Euclid on the magnitudes, kept negative to cover i64::MIN.
    if a > 0 {
        a = -a;
    }
    if b > 0 {
        b = -b;
    }
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}

fn out_of_range(width: u32) -> SqlError {
    SqlError::numeric_value_out_of_range(if width == 32 {
        "integer out of range"
    } else {
        "bigint out of range"
    })
}

fn float_value(value: &SqlValue) -> Result<f64> {
    sql_value_f64(value)
        .or_else(|| {
            let text = value.to_cell();
            match text.trim().to_ascii_lowercase().as_str() {
                "nan" => Some(f64::NAN),
                "infinity" | "inf" | "+infinity" => Some(f64::INFINITY),
                "-infinity" | "-inf" => Some(f64::NEG_INFINITY),
                other => other.parse::<f64>().ok(),
            }
        })
        .ok_or_else(|| {
            SqlError::invalid_text_representation(
                "double precision",
                format!("\"{}\"", value.to_cell()),
            )
        })
}

/// Digits after the decimal point of a numeric's text, `None` for NaN and
/// the infinities.
fn numeric_fraction(text: &str) -> Option<&str> {
    let text = text.trim();
    let lower = text.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "nan" | "infinity" | "-infinity" | "+infinity" | "inf" | "-inf"
    ) {
        return None;
    }
    Some(
        text.split_once('.')
            .map(|(_, fraction)| fraction)
            .unwrap_or(""),
    )
}

fn numeric_text(value: &SqlValue) -> String {
    match value {
        SqlValue::Float(number) if number.fract() == 0.0 && number.is_finite() => {
            format!("{number:.0}")
        }
        other => other.to_cell(),
    }
}

fn invalid_parameter(name: &str, value: i64) -> SqlError {
    SqlError::invalid_parameter_value(format!("invalid value for parameter \"{name}\": {value}"))
}

/// Regular expression for the `regexp_*` functions: PostgreSQL's flags, with
/// `g` refused like PostgreSQL does for these functions.
fn function_regex(function: &str, pattern: &str, flags: &str) -> Result<regex::Regex> {
    crate::schema_meta::reject_oversized_regex(pattern)?;
    let mut builder = regex::RegexBuilder::new(pattern);
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
                return Err(SqlError::invalid_parameter_value(format!(
                    "{function}() does not support the \"global\" option"
                )))
            }
            other => {
                return Err(SqlError::invalid_parameter_value(format!(
                    "invalid regular expression option: \"{other}\""
                )))
            }
        }
    }
    builder.build().map_err(|error| {
        SqlError::data_exception_public(
            "2201B",
            format!("invalid regular expression: {error}"),
            None,
        )
    })
}

/// Byte offset of 1-based character position `start` in `text` (the end of
/// the text when it is past the end).
fn char_byte_offset(text: &str, start: usize) -> usize {
    text.char_indices()
        .nth(start.saturating_sub(1))
        .map(|(offset, _)| offset)
        .unwrap_or(text.len())
}

fn char_position(text: &str, byte_offset: usize) -> i64 {
    text[..byte_offset].chars().count() as i64 + 1
}

/// Successive non-overlapping matches from byte offset `from`.
fn matches_from<'t>(regex: &regex::Regex, text: &'t str, from: usize) -> Vec<regex::Captures<'t>> {
    let mut found = Vec::new();
    let mut at = from;
    while at <= text.len() {
        let Some(captures) = regex.captures_at(text, at) else {
            break;
        };
        let whole = captures.get(0).expect("group 0 always matches");
        at = if whole.end() == whole.start() {
            // An empty match advances by one character.
            match text[whole.end()..].chars().next() {
                Some(c) => whole.end() + c.len_utf8(),
                None => text.len() + 1,
            }
        } else {
            whole.end()
        };
        found.push(captures);
    }
    found
}

fn text_arg(args: &[SqlValue], index: usize) -> Option<String> {
    args.get(index).map(SqlValue::to_cell)
}

fn int_option(name: &str, args: &[SqlValue], index: usize, default: i64) -> Result<i64> {
    match args.get(index) {
        None => Ok(default),
        Some(value) => integer_arg(name, value).map(|value| value.unwrap_or(default)),
    }
}

pub(crate) fn eval_pg_function_more(
    name: &str,
    args: &[SqlValue],
    arg_types: Option<&[Option<String>]>,
) -> Result<Option<SqlValue>> {
    let name = bare(name);
    let arg_type = |index: usize| -> Option<&str> {
        arg_types
            .and_then(|types| types.get(index))
            .and_then(Option::as_deref)
    };
    let strict = |args: &[SqlValue]| args.iter().any(|arg| matches!(arg, SqlValue::Null));
    Ok(Some(match (name, args.len()) {
        ("to_hex" | "to_oct" | "to_bin", 1) => {
            let Some(value) = integer_arg(name, &args[0])? else {
                return Ok(Some(SqlValue::Null));
            };
            let radix = match name {
                "to_hex" => 16,
                "to_oct" => 8,
                _ => 2,
            };
            SqlValue::String(radix_text(value, integer_width(value, arg_type(0)), radix))
        }
        ("gcd" | "lcm", 2) => {
            if arg_types.is_some_and(|types| {
                types
                    .iter()
                    .flatten()
                    .any(|pg_type| !matches!(pg_type.as_str(), "int2" | "int4" | "int8"))
            }) {
                return Ok(None);
            }
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let (Some(left), Some(right)) =
                (integer_arg(name, &args[0])?, integer_arg(name, &args[1])?)
            else {
                return Ok(Some(SqlValue::Null));
            };
            let width = if arg_type(0) == Some("int8")
                || arg_type(1) == Some("int8")
                || integer_width(left, None) == 64
                || integer_width(right, None) == 64
            {
                64
            } else {
                32
            };
            let (min, max) = if width == 32 {
                (i64::from(i32::MIN), i64::from(i32::MAX))
            } else {
                (i64::MIN, i64::MAX)
            };
            let negative_gcd = integer_gcd(left, right);
            let gcd = negative_gcd
                .checked_neg()
                .filter(|gcd| *gcd <= max)
                .ok_or_else(|| out_of_range(width))?;
            if name == "gcd" {
                SqlValue::Int(gcd)
            } else if left == 0 || right == 0 {
                SqlValue::Int(0)
            } else {
                let lcm = (left / gcd)
                    .checked_mul(right)
                    .and_then(i64::checked_abs)
                    .filter(|lcm| (min..=max).contains(lcm))
                    .ok_or_else(|| out_of_range(width))?;
                SqlValue::Int(lcm)
            }
        }
        ("num_nulls", _) => SqlValue::Int(
            args.iter()
                .filter(|arg| matches!(arg, SqlValue::Null))
                .count() as i64,
        ),
        ("num_nonnulls", _) => SqlValue::Int(
            args.iter()
                .filter(|arg| !matches!(arg, SqlValue::Null))
                .count() as i64,
        ),
        ("sinh" | "cosh" | "tanh" | "asinh" | "acosh" | "atanh", 1) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let value = float_value(&args[0])?;
            let result = match name {
                "sinh" => value.sinh(),
                "cosh" => value.cosh(),
                "tanh" => value.tanh(),
                "asinh" => value.asinh(),
                "acosh" => {
                    if value < 1.0 {
                        return Err(SqlError::numeric_value_out_of_range(
                            "input is out of range",
                        ));
                    }
                    value.acosh()
                }
                _ => {
                    if !value.is_nan() && !(-1.0..=1.0).contains(&value) {
                        return Err(SqlError::numeric_value_out_of_range(
                            "input is out of range",
                        ));
                    }
                    if value == 1.0 {
                        f64::INFINITY
                    } else if value == -1.0 {
                        f64::NEG_INFINITY
                    } else {
                        value.atanh()
                    }
                }
            };
            if result.is_infinite() && value.is_finite() && name == "cosh" {
                return Err(SqlError::numeric_value_out_of_range(
                    "value out of range: overflow",
                ));
            }
            SqlValue::Float(result)
        }
        ("getdatabaseencoding", 0) => SqlValue::String("UTF8".to_string()),
        ("isfinite", 1) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let text = args[0].to_cell();
            let text = text.trim();
            SqlValue::Bool(
                !(text.eq_ignore_ascii_case("infinity") || text.eq_ignore_ascii_case("-infinity")),
            )
        }
        ("scale" | "min_scale" | "trim_scale", 1) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let text = numeric_text(&args[0]);
            let Some(fraction) = numeric_fraction(&text) else {
                return Ok(Some(if name == "trim_scale" {
                    args[0].clone()
                } else {
                    SqlValue::Null
                }));
            };
            let trimmed = fraction.trim_end_matches('0');
            match name {
                "scale" => SqlValue::Int(fraction.len() as i64),
                "min_scale" => SqlValue::Int(trimmed.len() as i64),
                _ => {
                    let integer = text.trim().split('.').next().unwrap_or("0");
                    let rendered = if trimmed.is_empty() {
                        integer.to_string()
                    } else {
                        format!("{integer}.{trimmed}")
                    };
                    let rendered = if rendered == "-0" {
                        "0".to_string()
                    } else {
                        rendered
                    };
                    SqlValue::String(rendered)
                }
            }
        }
        ("regexp_like", 2..=3) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let flags = text_arg(args, 2).unwrap_or_default();
            let regex = function_regex(name, &args[1].to_cell(), &flags)?;
            SqlValue::Bool(regex.is_match(&args[0].to_cell()))
        }
        ("regexp_count", 2..=4) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let start = int_option(name, args, 2, 1)?;
            if start < 1 {
                return Err(invalid_parameter("start", start));
            }
            let flags = text_arg(args, 3).unwrap_or_default();
            let regex = function_regex(name, &args[1].to_cell(), &flags)?;
            let text = args[0].to_cell();
            let from = char_byte_offset(&text, start as usize);
            SqlValue::Int(matches_from(&regex, &text, from).len() as i64)
        }
        ("regexp_instr", 2..=7) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let start = int_option(name, args, 2, 1)?;
            if start < 1 {
                return Err(invalid_parameter("start", start));
            }
            let n = int_option(name, args, 3, 1)?;
            if n < 1 {
                return Err(invalid_parameter("n", n));
            }
            let end_option = int_option(name, args, 4, 0)?;
            if end_option != 0 && end_option != 1 {
                return Err(invalid_parameter("endoption", end_option));
            }
            let flags = text_arg(args, 5).unwrap_or_default();
            let subexpr = int_option(name, args, 6, 0)?;
            if subexpr < 0 {
                return Err(invalid_parameter("subexpr", subexpr));
            }
            let regex = function_regex(name, &args[1].to_cell(), &flags)?;
            let text = args[0].to_cell();
            let from = char_byte_offset(&text, start as usize);
            let found = matches_from(&regex, &text, from);
            let group = found
                .get(n as usize - 1)
                .and_then(|captures| captures.get(subexpr as usize));
            SqlValue::Int(match group {
                None => 0,
                Some(group) => char_position(
                    &text,
                    if end_option == 1 {
                        group.end()
                    } else {
                        group.start()
                    },
                ),
            })
        }
        ("regexp_substr", 2..=6) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let start = int_option(name, args, 2, 1)?;
            if start < 1 {
                return Err(invalid_parameter("start", start));
            }
            let n = int_option(name, args, 3, 1)?;
            if n < 1 {
                return Err(invalid_parameter("n", n));
            }
            let flags = text_arg(args, 4).unwrap_or_default();
            let subexpr = int_option(name, args, 5, 0)?;
            if subexpr < 0 {
                return Err(invalid_parameter("subexpr", subexpr));
            }
            let regex = function_regex(name, &args[1].to_cell(), &flags)?;
            let text = args[0].to_cell();
            let from = char_byte_offset(&text, start as usize);
            let found = matches_from(&regex, &text, from);
            found
                .get(n as usize - 1)
                .and_then(|captures| captures.get(subexpr as usize))
                .map(|group| SqlValue::String(group.as_str().to_string()))
                .unwrap_or(SqlValue::Null)
        }
        _ => return Ok(None),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(name: &str, args: &[SqlValue]) -> SqlValue {
        eval_pg_function_more(name, args, None).unwrap().unwrap()
    }

    #[test]
    fn integer_formatting_matches_postgres() {
        assert_eq!(
            call("to_hex", &[SqlValue::Int(-1234)]),
            SqlValue::String("fffffb2e".into())
        );
        assert_eq!(
            call("to_oct", &[SqlValue::Int(-1234)]),
            SqlValue::String("37777775456".into())
        );
        assert_eq!(
            call("to_bin", &[SqlValue::Int(-1234)]),
            SqlValue::String("11111111111111111111101100101110".into())
        );
        assert_eq!(
            eval_pg_function_more(
                "to_hex",
                &[SqlValue::Int(-1234)],
                Some(&[Some("int8".into())])
            )
            .unwrap()
            .unwrap(),
            SqlValue::String("fffffffffffffb2e".into())
        );
        assert_eq!(
            call("gcd", &[SqlValue::Int(-12), SqlValue::Int(18)]),
            SqlValue::Int(6)
        );
        assert_eq!(
            call("lcm", &[SqlValue::Int(4), SqlValue::Int(-6)]),
            SqlValue::Int(12)
        );
        assert!(eval_pg_function_more(
            "gcd",
            &[SqlValue::Int(-2147483648), SqlValue::Int(0)],
            None
        )
        .is_err());
    }

    #[test]
    fn regexp_functions_match_postgres() {
        let text = |value: &str| SqlValue::String(value.into());
        assert_eq!(
            call("regexp_count", &[text("123123123123123"), text("(12)3")]),
            SqlValue::Int(5)
        );
        assert_eq!(
            call("regexp_instr", &[text("abcdefghi"), text("d.f")]),
            SqlValue::Int(4)
        );
        assert_eq!(
            call(
                "regexp_instr",
                &[
                    text("abcdefghi"),
                    text("d.f"),
                    SqlValue::Int(1),
                    SqlValue::Int(1),
                    SqlValue::Int(1)
                ]
            ),
            SqlValue::Int(7)
        );
        assert_eq!(
            call("regexp_substr", &[text("abcdefghi"), text("d.f")]),
            text("def")
        );
        assert_eq!(
            call("regexp_like", &[text("Steven"), text("^Ste(v|ph)en$")]),
            SqlValue::Bool(true)
        );
    }
}
