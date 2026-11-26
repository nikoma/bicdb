//! PostgreSQL scalar functions shared by every evaluator (FROM-less SELECT,
//! row evaluation, routines): integer formatting and arithmetic, hyperbolic
//! functions, numeric scale helpers, `isfinite`, and the POSIX regular
//! expression functions `regexp_count/instr/like/substr`.
use crate::*;
use num_traits::Zero;

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
            "width_bucket" => "int4",
            "sin" | "cos" | "tan" | "cot" | "asin" | "acos" | "atan" | "atan2" | "sind"
            | "cosd" | "tand" | "cotd" | "asind" | "acosd" | "atand" | "atan2d" | "degrees"
            | "radians" | "pi" => "float8",
            "div" | "factorial" => "numeric",
            "array_reverse" | "array_sort" => return first,
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
        ("pi", 0) => SqlValue::Float(std::f64::consts::PI),
        (
            "sin" | "cos" | "tan" | "cot" | "asin" | "acos" | "atan" | "sind" | "cosd" | "tand"
            | "cotd" | "asind" | "acosd" | "atand" | "degrees" | "radians",
            1,
        ) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            SqlValue::Float(trig(name, float_value(&args[0])?)?)
        }
        ("atan2" | "atan2d", 2) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let (y, x) = (float_value(&args[0])?, float_value(&args[1])?);
            let radians = y.atan2(x);
            SqlValue::Float(if name == "atan2" {
                radians
            } else {
                radians / 1f64.atan() * 45.0
            })
        }
        ("div", 2) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let left = parse_decimal(&numeric_text(&args[0]))?;
            let right = parse_decimal(&numeric_text(&args[1]))?;
            if right.mantissa.is_zero() {
                return Err(SqlError::data_exception_public(
                    "22012",
                    "division by zero",
                    None,
                ));
            }
            let scale = left.scale.max(right.scale);
            let numerator = &left.mantissa * pow10_bigint(scale - left.scale);
            let denominator = &right.mantissa * pow10_bigint(scale - right.scale);
            SqlValue::String((numerator / denominator).to_string())
        }
        ("factorial", 1) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let Some(n) = integer_arg(name, &args[0])? else {
                return Ok(Some(SqlValue::Null));
            };
            if n < 0 {
                return Err(SqlError::numeric_value_out_of_range(
                    "factorial of a negative number is undefined",
                ));
            }
            if n > 32177 {
                return Err(SqlError::numeric_value_out_of_range(
                    "value overflows numeric format",
                ));
            }
            let mut product = num_bigint::BigInt::from(1);
            for factor in 2..=n {
                product *= factor;
            }
            SqlValue::String(product.to_string())
        }
        ("width_bucket", 4) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let Some(count) = integer_arg(name, &args[3])? else {
                return Ok(Some(SqlValue::Null));
            };
            let numeric = (0..3).any(|index| arg_type(index) == Some("numeric"));
            SqlValue::Int(if numeric {
                width_bucket_numeric(&args[0], &args[1], &args[2], count)?
            } else {
                width_bucket_float(
                    float_value(&args[0])?,
                    float_value(&args[1])?,
                    float_value(&args[2])?,
                    count,
                )?
            })
        }
        ("array_reverse", 1) => {
            let Some(mut elements) = array_elements(&args[0]) else {
                return Ok(Some(SqlValue::Null));
            };
            elements.reverse();
            SqlValue::Json(std::sync::Arc::new(JsonValue::Array(elements)))
        }
        ("array_sort", 1..=3) => {
            let Some(mut elements) = array_elements(&args[0]) else {
                return Ok(Some(SqlValue::Null));
            };
            let flag = |index: usize, default: bool| -> Result<Option<bool>> {
                match args.get(index) {
                    None => Ok(Some(default)),
                    Some(SqlValue::Null) => Ok(None),
                    Some(value) => Ok(Some(sql_value_truth(value.clone())?.unwrap_or(default))),
                }
            };
            let Some(descending) = flag(1, false)? else {
                return Ok(Some(SqlValue::Null));
            };
            let Some(nulls_first) = flag(2, descending)? else {
                return Ok(Some(SqlValue::Null));
            };
            elements.sort_by(|left, right| {
                use std::cmp::Ordering;
                match (left.is_null(), right.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => {
                        if nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (false, true) => {
                        if nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    (false, false) => {
                        let ordering = json_scalar_cmp(left, right);
                        if descending {
                            ordering.reverse()
                        } else {
                            ordering
                        }
                    }
                }
            });
            SqlValue::Json(std::sync::Arc::new(JsonValue::Array(elements)))
        }
        _ => return Ok(None),
    }))
}

fn out_of_range_input() -> SqlError {
    SqlError::numeric_value_out_of_range("input is out of range")
}

const RADIANS_PER_DEGREE: f64 = std::f64::consts::PI / 180.0;

fn sind_0_to_30(x: f64) -> f64 {
    let sin_30 = (30.0 * RADIANS_PER_DEGREE).sin();
    ((x * RADIANS_PER_DEGREE).sin() / sin_30) / 2.0
}

fn cosd_0_to_60(x: f64) -> f64 {
    let one_minus_cos_60 = 1.0 - (60.0 * RADIANS_PER_DEGREE).cos();
    let one_minus_cos_x = 1.0 - (x * RADIANS_PER_DEGREE).cos();
    1.0 - (one_minus_cos_x / one_minus_cos_60) / 2.0
}

fn sind_q1(x: f64) -> f64 {
    if x <= 30.0 {
        sind_0_to_30(x)
    } else {
        cosd_0_to_60(90.0 - x)
    }
}

fn cosd_q1(x: f64) -> f64 {
    if x <= 60.0 {
        cosd_0_to_60(x)
    } else {
        sind_0_to_30(90.0 - x)
    }
}

fn asind_q1(x: f64) -> f64 {
    if x <= 0.5 {
        (x.asin() / 0.5f64.asin()) * 30.0
    } else {
        90.0 - (x.acos() / 0.5f64.acos()) * 60.0
    }
}

fn acosd_q1(x: f64) -> f64 {
    if x <= 0.5 {
        90.0 - (x.asin() / 0.5f64.asin()) * 30.0
    } else {
        (x.acos() / 0.5f64.acos()) * 60.0
    }
}

/// PostgreSQL's float8 trigonometric functions, including the degree
/// variants that are exact at the standard angles (`sind(30)` is 0.5).
fn trig(name: &str, x: f64) -> Result<f64> {
    if x.is_nan() {
        return Ok(f64::NAN);
    }
    let finite = |x: f64| {
        if x.is_infinite() {
            Err(out_of_range_input())
        } else {
            Ok(x)
        }
    };
    Ok(match name {
        "sin" => finite(x)?.sin(),
        "cos" => finite(x)?.cos(),
        "tan" => finite(x)?.tan(),
        "cot" => 1.0 / finite(x)?.tan(),
        "asin" | "acos" => {
            if !(-1.0..=1.0).contains(&x) {
                return Err(out_of_range_input());
            }
            if name == "asin" {
                x.asin()
            } else {
                x.acos()
            }
        }
        "atan" => x.atan(),
        "degrees" => x / RADIANS_PER_DEGREE,
        "radians" => x * RADIANS_PER_DEGREE,
        "asind" => {
            if !(-1.0..=1.0).contains(&x) {
                return Err(out_of_range_input());
            }
            if x >= 0.0 {
                asind_q1(x)
            } else {
                -asind_q1(-x)
            }
        }
        "acosd" => {
            if !(-1.0..=1.0).contains(&x) {
                return Err(out_of_range_input());
            }
            if x >= 0.0 {
                acosd_q1(x)
            } else {
                90.0 + asind_q1(-x)
            }
        }
        "atand" => x.atan() / 1f64.atan() * 45.0,
        "cosd" => {
            let mut arg = finite(x)? % 360.0;
            if arg < 0.0 {
                arg = -arg;
            }
            if arg > 180.0 {
                arg = 360.0 - arg;
            }
            let mut sign = 1.0;
            if arg > 90.0 {
                arg = 180.0 - arg;
                sign = -sign;
            }
            sign * cosd_q1(arg)
        }
        _ => {
            // sind, tand, cotd share the reduction to the first quadrant.
            let mut arg = finite(x)? % 360.0;
            let mut sign = 1.0;
            if arg < 0.0 {
                arg = -arg;
                sign = -sign;
            }
            if arg > 180.0 {
                arg = 360.0 - arg;
                sign = -sign;
            }
            if arg > 90.0 {
                arg = 180.0 - arg;
                sign = -sign;
            }
            let result = match name {
                "sind" => sign * sind_q1(arg),
                "tand" => {
                    let tan_45 = sind_q1(45.0) / cosd_q1(45.0);
                    sign * ((sind_q1(arg) / cosd_q1(arg)) / tan_45)
                }
                _ => {
                    let cot_45 = cosd_q1(45.0) / sind_q1(45.0);
                    sign * ((cosd_q1(arg) / sind_q1(arg)) / cot_45)
                }
            };
            if result == 0.0 {
                0.0
            } else {
                result
            }
        }
    })
}

/// The first dimension of an array value, or `None` for NULL.
fn array_elements(value: &SqlValue) -> Option<Vec<JsonValue>> {
    match value {
        SqlValue::Null => None,
        SqlValue::Json(json) => json.as_array().cloned(),
        other => match cast_value_to_array(other.clone(), "text").ok()? {
            SqlValue::Json(json) => json.as_array().cloned(),
            _ => None,
        },
    }
}

fn json_scalar_cmp(left: &JsonValue, right: &JsonValue) -> std::cmp::Ordering {
    match (left, right) {
        (JsonValue::Number(left), JsonValue::Number(right)) => left
            .as_f64()
            .partial_cmp(&right.as_f64())
            .unwrap_or(std::cmp::Ordering::Equal),
        (JsonValue::String(left), JsonValue::String(right)) => {
            match (parse_decimal(left), parse_decimal(right)) {
                (Ok(l), Ok(r)) => l.cmp(&r),
                _ => left.cmp(right),
            }
        }
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left.cmp(right),
        (JsonValue::Array(left), JsonValue::Array(right)) => {
            for (l, r) in left.iter().zip(right) {
                let ordering = json_scalar_cmp(l, r);
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            left.len().cmp(&right.len())
        }
        _ => left.to_string().cmp(&right.to_string()),
    }
}

fn width_bucket_error(message: &str) -> SqlError {
    SqlError::data_exception_public("2201G", message, None)
}

/// `width_bucket(float8, float8, float8, int)`, PostgreSQL's arithmetic.
fn width_bucket_float(operand: f64, low: f64, high: f64, count: i64) -> Result<i64> {
    if count <= 0 {
        return Err(width_bucket_error("count must be greater than zero"));
    }
    if operand.is_nan() || low.is_nan() || high.is_nan() {
        return Err(width_bucket_error(
            "operand, lower bound, and upper bound cannot be NaN",
        ));
    }
    if !low.is_finite() || !high.is_finite() {
        return Err(width_bucket_error("lower and upper bounds must be finite"));
    }
    let count_f = count as f64;
    let inside = |fraction: f64| -> i64 {
        let mut result = (count_f * fraction) as i64;
        if result >= count {
            result = count - 1;
        }
        result + 1
    };
    let above = || {
        count
            .checked_add(1)
            .filter(|value| *value <= i64::from(i32::MAX))
            .ok_or_else(|| SqlError::numeric_value_out_of_range("integer out of range"))
    };
    if low < high {
        if operand < low {
            Ok(0)
        } else if operand >= high {
            above()
        } else if (high - low).is_finite() {
            Ok(inside((operand - low) / (high - low)))
        } else {
            Ok(inside(
                (operand / 2.0 - low / 2.0) / (high / 2.0 - low / 2.0),
            ))
        }
    } else if low > high {
        if operand > low {
            Ok(0)
        } else if operand <= high {
            above()
        } else if (low - high).is_finite() {
            Ok(inside((low - operand) / (low - high)))
        } else {
            Ok(inside(
                (low / 2.0 - operand / 2.0) / (low / 2.0 - high / 2.0),
            ))
        }
    } else {
        Err(width_bucket_error("lower bound cannot equal upper bound"))
    }
}

/// `width_bucket(numeric, numeric, numeric, int)`, exact.
fn width_bucket_numeric(
    operand: &SqlValue,
    low: &SqlValue,
    high: &SqlValue,
    count: i64,
) -> Result<i64> {
    if count <= 0 {
        return Err(width_bucket_error("count must be greater than zero"));
    }
    let special = |value: &SqlValue| numeric_text(value).trim().to_ascii_lowercase();
    let (o, l, h) = (special(operand), special(low), special(high));
    if [&o, &l, &h].iter().any(|text| text.as_str() == "nan") {
        return Err(width_bucket_error(
            "operand, lower bound, and upper bound cannot be NaN",
        ));
    }
    let infinite = |text: &str| text.trim_start_matches(['+', '-']).starts_with("inf");
    if infinite(&l) || infinite(&h) {
        return Err(width_bucket_error("lower and upper bounds must be finite"));
    }
    let low = parse_decimal(&l)?;
    let high = parse_decimal(&h)?;
    let above = || {
        count
            .checked_add(1)
            .filter(|value| *value <= i64::from(i32::MAX))
            .ok_or_else(|| SqlError::numeric_value_out_of_range("integer out of range"))
    };
    use std::cmp::Ordering;
    let ascending = match low.cmp(&high) {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => return Err(width_bucket_error("lower bound cannot equal upper bound")),
    };
    if infinite(&o) {
        let positive = !o.starts_with('-');
        return if positive == ascending {
            above()
        } else {
            Ok(0)
        };
    }
    let operand = parse_decimal(&o)?;
    let scale = operand.scale.max(low.scale).max(high.scale);
    let at = |value: &Decimal| &value.mantissa * pow10_bigint(scale - value.scale);
    let (operand, low, high) = (at(&operand), at(&low), at(&high));
    let (offset, width) = if ascending {
        if operand < low {
            return Ok(0);
        }
        if operand >= high {
            return above();
        }
        (&operand - &low, &high - &low)
    } else {
        if operand > low {
            return Ok(0);
        }
        if operand <= high {
            return above();
        }
        (&low - &operand, &low - &high)
    };
    let bucket = (offset * num_bigint::BigInt::from(count)) / width;
    let bucket = i64::try_from(bucket).unwrap_or(count - 1).min(count - 1);
    Ok(bucket + 1)
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
