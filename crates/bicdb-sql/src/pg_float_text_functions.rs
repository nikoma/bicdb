//! PostgreSQL scalar functions over float bits and text code points:
//! `float4send`/`float8send`, `hashfloat4`/`hashfloat8` (PostgreSQL's
//! `hash_bytes`), `ascii`, `unistr` and `pg_size_bytes`.
use crate::*;

/// PostgreSQL's `hash_bytes` (Bob Jenkins' lookup3 `hashlittle`), as the
/// little-endian builds that every supported platform uses compute it.
pub(crate) fn pg_hash_bytes(key: &[u8]) -> u32 {
    fn mix(a: &mut u32, b: &mut u32, c: &mut u32) {
        *a = a.wrapping_sub(*c);
        *a ^= c.rotate_left(4);
        *c = c.wrapping_add(*b);
        *b = b.wrapping_sub(*a);
        *b ^= a.rotate_left(6);
        *a = a.wrapping_add(*c);
        *c = c.wrapping_sub(*b);
        *c ^= b.rotate_left(8);
        *b = b.wrapping_add(*a);
        *a = a.wrapping_sub(*c);
        *a ^= c.rotate_left(16);
        *c = c.wrapping_add(*b);
        *b = b.wrapping_sub(*a);
        *b ^= a.rotate_left(19);
        *a = a.wrapping_add(*c);
        *c = c.wrapping_sub(*b);
        *c ^= b.rotate_left(4);
        *b = b.wrapping_add(*a);
    }
    fn finish(a: &mut u32, b: &mut u32, c: &mut u32) {
        *c ^= *b;
        *c = c.wrapping_sub(b.rotate_left(14));
        *a ^= *c;
        *a = a.wrapping_sub(c.rotate_left(11));
        *b ^= *a;
        *b = b.wrapping_sub(a.rotate_left(25));
        *c ^= *b;
        *c = c.wrapping_sub(b.rotate_left(16));
        *a ^= *c;
        *a = a.wrapping_sub(c.rotate_left(4));
        *b ^= *a;
        *b = b.wrapping_sub(a.rotate_left(14));
        *c ^= *b;
        *c = c.wrapping_sub(b.rotate_left(24));
    }
    let word = |bytes: &[u8]| -> u32 {
        let mut value = 0u32;
        for (index, byte) in bytes.iter().enumerate().take(4) {
            value |= u32::from(*byte) << (8 * index);
        }
        value
    };
    let init = 0x9e37_79b9u32
        .wrapping_add(key.len() as u32)
        .wrapping_add(3_923_095);
    let (mut a, mut b, mut c) = (init, init, init);
    let mut rest = key;
    while rest.len() >= 12 {
        a = a.wrapping_add(word(&rest[0..4]));
        b = b.wrapping_add(word(&rest[4..8]));
        c = c.wrapping_add(word(&rest[8..12]));
        mix(&mut a, &mut b, &mut c);
        rest = &rest[12..];
    }
    // The lowest byte of c is reserved for the length.
    let len = rest.len();
    if len > 8 {
        c = c.wrapping_add(word(&rest[8..]) << 8);
    }
    if len > 4 {
        b = b.wrapping_add(word(&rest[4..len.min(8)]));
    }
    if len > 0 {
        a = a.wrapping_add(word(&rest[..len.min(4)]));
    }
    finish(&mut a, &mut b, &mut c);
    c
}

/// `hashfloat8`: zero (either sign) hashes to 0 and every NaN alike.
fn hash_float8(value: f64) -> i64 {
    if value == 0.0 {
        return 0;
    }
    let value = if value.is_nan() { f64::NAN } else { value };
    i64::from(pg_hash_bytes(&value.to_le_bytes()) as i32)
}

fn float_arg(name: &str, value: &SqlValue) -> Result<f64> {
    match value {
        SqlValue::Float(value) => Ok(*value),
        SqlValue::Int(value) => Ok(*value as f64),
        other => parse_postgres_float_input(&other.to_cell(), "float8").map_err(|_| {
            SqlError::undefined_function(format!("function {name}(unknown) does not exist"))
        }),
    }
}

/// `unistr(text)`: `\XXXX`, `\+XXXXXX`, `\uXXXX`, `\UXXXXXXXX` and `\\`.
fn unistr(text: &str) -> Result<String> {
    let invalid_pair =
        || SqlError::data_exception_public("42601", "invalid Unicode surrogate pair", None);
    let bytes = text.as_bytes();
    let hex = |slice: &[u8]| -> Option<u32> {
        slice
            .iter()
            .all(u8::is_ascii_hexdigit)
            .then(|| u32::from_str_radix(std::str::from_utf8(slice).ok()?, 16).ok())
            .flatten()
    };
    let mut out = Vec::with_capacity(bytes.len());
    let mut pair_first: Option<u32> = None;
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] != b'\\' {
            if pair_first.is_some() {
                return Err(invalid_pair());
            }
            out.push(bytes[at]);
            at += 1;
            continue;
        }
        let rest = &bytes[at..];
        if rest.get(1) == Some(&b'\\') {
            out.push(b'\\');
            at += 2;
            continue;
        }
        let (code, width) = if rest.len() >= 5 && hex(&rest[1..5]).is_some() {
            (hex(&rest[1..5]), 5)
        } else if rest.len() >= 6 && rest[1] == b'u' && hex(&rest[2..6]).is_some() {
            (hex(&rest[2..6]), 6)
        } else if rest.len() >= 8 && rest[1] == b'+' && hex(&rest[2..8]).is_some() {
            (hex(&rest[2..8]), 8)
        } else if rest.len() >= 10 && rest[1] == b'U' && hex(&rest[2..10]).is_some() {
            (hex(&rest[2..10]), 10)
        } else {
            return Err(SqlError::with_detail_fields(
                SqlError::data_exception_public("42601", "invalid Unicode escape", None),
                None,
                Some("Unicode escapes must be \\XXXX, \\+XXXXXX, \\uXXXX, or \\UXXXXXXXX.".into()),
                None,
            ));
        };
        let mut code = code.expect("hex digits checked");
        if code == 0 || code > 0x10FFFF {
            return Err(SqlError::invalid_parameter_value(format!(
                "invalid Unicode code point: {code:04X}"
            )));
        }
        if let Some(first) = pair_first.take() {
            if !(0xDC00..=0xDFFF).contains(&code) {
                return Err(invalid_pair());
            }
            code = 0x10000 + ((first - 0xD800) << 10) + (code - 0xDC00);
        } else if (0xDC00..=0xDFFF).contains(&code) {
            return Err(invalid_pair());
        }
        if (0xD800..=0xDBFF).contains(&code) {
            pair_first = Some(code);
        } else {
            let ch = char::from_u32(code).ok_or_else(invalid_pair)?;
            let mut buffer = [0u8; 4];
            out.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
        }
        at += width;
    }
    if pair_first.is_some() {
        return Err(invalid_pair());
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

/// `pg_size_bytes(text)`: a number with an optional `bytes`/`B`/`kB`/`MB`/
/// `GB`/`TB`/`PB` unit (case-insensitive), in bytes.
fn pg_size_bytes(text: &str) -> Result<SqlValue> {
    let invalid = || SqlError::invalid_parameter_value(format!("invalid size: \"{text}\""));
    let bytes = text.as_bytes();
    let mut at = 0;
    while at < bytes.len() && bytes[at].is_ascii_whitespace() {
        at += 1;
    }
    let start = at;
    if matches!(bytes.get(at), Some(b'-' | b'+')) {
        at += 1;
    }
    let mut digits = false;
    while bytes.get(at).is_some_and(u8::is_ascii_digit) {
        at += 1;
        digits = true;
    }
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        while bytes.get(at).is_some_and(u8::is_ascii_digit) {
            at += 1;
            digits = true;
        }
    }
    if !digits {
        return Err(invalid());
    }
    if matches!(bytes.get(at), Some(b'e' | b'E')) {
        let mut exponent = at + 1;
        if matches!(bytes.get(exponent), Some(b'-' | b'+')) {
            exponent += 1;
        }
        let digits_start = exponent;
        while bytes.get(exponent).is_some_and(u8::is_ascii_digit) {
            exponent += 1;
        }
        if exponent > digits_start {
            at = exponent;
        }
    }
    let number = cast_value_to_pg_type(SqlValue::String(text[start..at].to_string()), "numeric")?;
    let unit = text[at..].trim_matches(|c: char| c.is_ascii_whitespace());
    let shift = if unit.is_empty() {
        0
    } else {
        match unit.to_ascii_lowercase().as_str() {
            "bytes" | "b" => 0,
            "kb" => 10,
            "mb" => 20,
            "gb" => 30,
            "tb" => 40,
            "pb" => 50,
            _ => return Err(SqlError::with_detail_fields(
                invalid(),
                Some(format!("Invalid size unit: \"{unit}\".")),
                Some(
                    "Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", \"TB\", and \"PB\"."
                        .into(),
                ),
                None,
            )),
        }
    };
    let decimal = parse_decimal(&number.to_cell())?;
    let scaled = Decimal {
        mantissa: decimal.mantissa << shift,
        scale: decimal.scale,
    };
    decimal_to_i64_rounded(scaled)
        .map(SqlValue::Int)
        .map_err(|_| SqlError::numeric_value_out_of_range("bigint out of range"))
}

/// Result type of a function implemented here.
pub(crate) fn pg_float_text_result_type(name: &str) -> Option<String> {
    Some(
        match name.strip_prefix("pg_catalog.").unwrap_or(name) {
            "float4send" | "float8send" => "bytea",
            "hashfloat4" | "hashfloat8" | "ascii" => "int4",
            "unistr" => "text",
            "pg_size_bytes" => "int8",
            _ => return None,
        }
        .to_string(),
    )
}

/// Evaluates the functions of this module; `None` for other functions.
pub(crate) fn eval_pg_float_text_function(
    name: &str,
    args: &[SqlValue],
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    let [arg] = args else {
        return Ok(None);
    };
    if !matches!(
        name,
        "float4send"
            | "float8send"
            | "hashfloat4"
            | "hashfloat8"
            | "ascii"
            | "unistr"
            | "pg_size_bytes"
    ) {
        return Ok(None);
    }
    if matches!(arg, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    Ok(Some(match name {
        "float4send" => {
            let value = float_arg(name, arg)? as f32;
            SqlValue::String(format_bytea_hex(&value.to_be_bytes()))
        }
        "float8send" => {
            let value = float_arg(name, arg)?;
            SqlValue::String(format_bytea_hex(&value.to_be_bytes()))
        }
        // float4 hashes as the float8 of the same value.
        "hashfloat4" => SqlValue::Int(hash_float8(f64::from(float_arg(name, arg)? as f32))),
        "hashfloat8" => SqlValue::Int(hash_float8(float_arg(name, arg)?)),
        "ascii" => SqlValue::Int(
            arg.to_cell()
                .chars()
                .next()
                .map(|c| i64::from(u32::from(c)))
                .unwrap_or(0),
        ),
        "unistr" => SqlValue::String(unistr(&arg.to_cell())?),
        "pg_size_bytes" => pg_size_bytes(&arg.to_cell())?,
        _ => unreachable!("name checked"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_and_send_formats_match_postgres() {
        assert_eq!(hash_float8(1.5), 630_860_146);
        assert_eq!(hash_float8(-0.0), 0);
        assert_eq!(
            eval_pg_float_text_function("float4send", &[SqlValue::Float(1.5)])
                .unwrap()
                .unwrap()
                .to_cell(),
            "\\x3fc00000"
        );
    }

    #[test]
    fn unistr_and_size_bytes_match_postgres() {
        assert_eq!(unistr("\\0064at\\+0000610").unwrap(), "data0");
        assert_eq!(unistr("d\\u0061t\\U000000610").unwrap(), "data0");
        assert_eq!(unistr("a\\\\b").unwrap(), "a\\b");
        assert_eq!(
            unistr("wrong: \\db99").unwrap_err().to_string(),
            "invalid Unicode surrogate pair"
        );
        assert_eq!(
            unistr("wrong: \\+2FFFFF").unwrap_err().to_string(),
            "invalid Unicode code point: 2FFFFF"
        );
        assert_eq!(
            pg_size_bytes("1.5 GB").unwrap(),
            SqlValue::Int(1_610_612_736)
        );
        assert_eq!(pg_size_bytes("-1 MB").unwrap(), SqlValue::Int(-1_048_576));
        assert_eq!(
            pg_size_bytes("1 AB A    ").unwrap_err().to_string(),
            "invalid size: \"1 AB A    \""
        );
    }
}
