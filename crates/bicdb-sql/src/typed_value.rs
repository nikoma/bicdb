//! Lossless, locale-independent in-memory representations of PostgreSQL values.
//!
//! These types are additive to the legacy [`crate::SqlValue`] API. They define
//! the canonical carriers used by the typed execution and storage work without
//! forcing existing embedders to migrate in one release.

use crate::{pg_type_oid_by_name, PgTsQuery, PgTsVector, SqlResult, SqlRowStream, SqlValue};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::net::Ipv6Addr;
use thiserror::Error;

pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;
const POSTGRES_EPOCH_UNIX_DAYS: i64 = 10_957;
const POSTGRES_EPOCH_JULIAN_DAY: i64 = 2_451_545;
const POSTGRES_DATE_MIN_DAYS: i32 = -2_451_545;
const POSTGRES_DATE_MAX_DAYS: i32 = 2_145_031_948;
const MAX_TIMEZONE_OFFSET_SECONDS: i32 = 15 * 60 * 60 + 59 * 60;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PgCanonicalValueError {
    #[error("numeric coefficient must contain canonical decimal digits")]
    InvalidNumericCoefficient,
    #[error("value overflows numeric format")]
    NumericOverflow,
    #[error("floating-point value {0} is out of range")]
    FloatOverflow(String),
    #[error("bit length {bit_len} exceeds the {available_bits} available bits")]
    InvalidBitLength {
        bit_len: usize,
        available_bits: usize,
    },
    #[error("unused low bits in the final bit-string byte must be zero")]
    NonCanonicalBitPadding,
    #[error("invalid bytea text representation")]
    InvalidBytea,
    #[error("invalid hexadecimal bytea representation")]
    InvalidByteaHex,
    #[error("bit strings may contain only zero and one digits")]
    InvalidBitString,
    #[error("time value {0} is outside 00:00:00 through 24:00:00")]
    InvalidTime(i64),
    #[error("timezone offset {0} seconds exceeds PostgreSQL's supported range")]
    InvalidTimezoneOffset(i32),
    #[error("invalid {kind} value: {value}")]
    InvalidTemporal { kind: &'static str, value: String },
    #[error("{0} field value is out of range")]
    TemporalFieldOverflow(&'static str),
    #[error("{0} value exceeds its canonical storage range")]
    TemporalOverflow(&'static str),
    #[error("network prefix {prefix} exceeds address width {address_bits}")]
    InvalidNetworkPrefix { prefix: u8, address_bits: u8 },
    #[error("invalid octet value in macaddr value")]
    MacAddressOctetOutOfRange,
    #[error("array dimensions overflow the addressable element count")]
    ArrayDimensionsOverflow,
    #[error("array dimensions require {expected} elements but received {actual}")]
    ArrayElementCount { expected: usize, actual: usize },
    #[error("snapshot boundaries are invalid")]
    InvalidSnapshotBounds,
    #[error("snapshot in-progress transaction IDs must be sorted, unique, and inside the bounds")]
    InvalidSnapshotTransactions,
    #[error("range lower bound must be less than or equal to range upper bound")]
    InvalidRangeBounds,
    #[error("{0} out of range")]
    RangeCanonicalOverflow(&'static str),
    #[error("invalid input syntax for type {pg_type}: \"{value}\"")]
    InvalidSpecialValue { pg_type: String, value: String },
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PgOidParseError {
    #[error("invalid OID syntax")]
    InvalidSyntax,
    #[error("OID is out of range")]
    OutOfRange,
}

/// Parse PostgreSQL's `oidin` syntax. It uses C integer notation and accepts
/// signed 32-bit input by reinterpreting its bits as an unsigned OID.
pub fn parse_pg_oid(value: &str) -> Result<u32, PgOidParseError> {
    let value = value.trim();
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if unsigned.is_empty() {
        return Err(PgOidParseError::InvalidSyntax);
    }
    let (digits, radix) = if let Some(hex) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        (hex, 16)
    } else if unsigned.len() > 1 && unsigned.starts_with('0') {
        (&unsigned[1..], 8)
    } else {
        (unsigned, 10)
    };
    if digits.is_empty() {
        return Err(PgOidParseError::InvalidSyntax);
    }
    let magnitude = u64::from_str_radix(digits, radix).map_err(|error| {
        if error.kind() == &std::num::IntErrorKind::PosOverflow {
            PgOidParseError::OutOfRange
        } else {
            PgOidParseError::InvalidSyntax
        }
    })?;
    if negative {
        if magnitude > i32::MIN.unsigned_abs().into() {
            return Err(PgOidParseError::OutOfRange);
        }
        Ok(0_u32.wrapping_sub(magnitude as u32))
    } else {
        u32::try_from(magnitude).map_err(|_| PgOidParseError::OutOfRange)
    }
}

pub fn parse_postgres_uuid(value: &str) -> Result<[u8; 16], PgCanonicalValueError> {
    let original = value;
    let value = value
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
        .unwrap_or(value);
    let mut hex = String::with_capacity(32);
    let mut last_was_hyphen = false;
    for character in value.chars() {
        if character == '-' {
            if last_was_hyphen || hex.is_empty() || hex.len() % 4 != 0 || hex.len() == 32 {
                return Err(PgCanonicalValueError::InvalidSpecialValue {
                    pg_type: "uuid".to_string(),
                    value: original.to_string(),
                });
            }
            last_was_hyphen = true;
        } else if character.is_ascii_hexdigit() {
            hex.push(character);
            last_was_hyphen = false;
        } else {
            return Err(PgCanonicalValueError::InvalidSpecialValue {
                pg_type: "uuid".to_string(),
                value: original.to_string(),
            });
        }
    }
    if last_was_hyphen || hex.len() != 32 {
        return Err(PgCanonicalValueError::InvalidSpecialValue {
            pg_type: "uuid".to_string(),
            value: original.to_string(),
        });
    }
    uuid::Uuid::parse_str(&hex)
        .map(uuid::Uuid::into_bytes)
        .map_err(|_| PgCanonicalValueError::InvalidSpecialValue {
            pg_type: "uuid".to_string(),
            value: original.to_string(),
        })
}

pub fn format_postgres_uuid(value: [u8; 16]) -> String {
    uuid::Uuid::from_bytes(value).to_string()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PgFloat4(u32);

impl PgFloat4 {
    pub fn from_value(value: f32) -> Self {
        Self(value.to_bits())
    }

    pub fn to_value(self) -> f32 {
        f32::from_bits(self.0)
    }

    pub fn bits(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PgFloat8(u64);

impl PgFloat8 {
    pub fn from_value(value: f64) -> Self {
        Self(value.to_bits())
    }

    pub fn to_value(self) -> f64 {
        f64::from_bits(self.0)
    }

    pub fn bits(self) -> u64 {
        self.0
    }
}

pub fn postgres_float_text(value: f64, pg_type: &str) -> String {
    let (value, rendered, scientific_threshold) = if pg_type == "float4" {
        let value = value as f32;
        (f64::from(value), value.to_string(), 6_i32)
    } else {
        (value, value.to_string(), 15_i32)
    };
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value == f64::INFINITY {
        return "Infinity".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_string();
    }

    let (negative, rendered) = rendered
        .strip_prefix('-')
        .map(|rendered| (true, rendered))
        .unwrap_or((false, rendered.as_str()));
    let (mantissa, explicit_exponent) = rendered
        .split_once(['e', 'E'])
        .map(|(mantissa, exponent)| (mantissa, exponent.parse::<i32>().unwrap_or(0)))
        .unwrap_or((rendered, 0));
    let decimal_position = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let digits = mantissa
        .bytes()
        .filter(u8::is_ascii_digit)
        .collect::<Vec<_>>();
    let leading_zeroes = digits.iter().take_while(|digit| **digit == b'0').count();
    let significant = digits[leading_zeroes..].iter().copied().collect::<Vec<_>>();
    let significant = String::from_utf8(significant)
        .expect("floating-point display contains only ASCII digits")
        .trim_end_matches('0')
        .to_string();
    let exponent = decimal_position + explicit_exponent - leading_zeroes as i32 - 1;
    let body = if exponent < -4 || exponent >= scientific_threshold {
        let mut mantissa = significant[..1].to_string();
        if significant.len() > 1 {
            mantissa.push('.');
            mantissa.push_str(&significant[1..]);
        }
        format!(
            "{mantissa}e{}{magnitude:02}",
            if exponent < 0 { '-' } else { '+' },
            magnitude = exponent.unsigned_abs()
        )
    } else {
        let decimal_position = exponent + 1;
        if decimal_position <= 0 {
            format!(
                "0.{}{}",
                "0".repeat(decimal_position.unsigned_abs() as usize),
                significant
            )
        } else if decimal_position as usize >= significant.len() {
            format!(
                "{}{}",
                significant,
                "0".repeat(decimal_position as usize - significant.len())
            )
        } else {
            let decimal_position = decimal_position as usize;
            format!(
                "{}.{}",
                &significant[..decimal_position],
                &significant[decimal_position..]
            )
        }
    };
    if negative {
        format!("-{body}")
    } else {
        body
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PgNumeric {
    Finite {
        negative: bool,
        coefficient: String,
        display_scale: i32,
    },
    NaN,
    PositiveInfinity,
    NegativeInfinity,
}

pub const PG_NUMERIC_MAX_INTEGER_DIGITS: i32 = 131_072;
pub const PG_NUMERIC_MAX_FRACTIONAL_DIGITS: i32 = 16_383;

impl PgNumeric {
    pub fn finite(
        negative: bool,
        coefficient: impl Into<String>,
        display_scale: i32,
    ) -> Result<Self, PgCanonicalValueError> {
        let coefficient = coefficient.into();
        let canonical = !coefficient.is_empty()
            && coefficient.bytes().all(|byte| byte.is_ascii_digit())
            && (coefficient == "0" || !coefficient.starts_with('0'));
        if !canonical {
            return Err(PgCanonicalValueError::InvalidNumericCoefficient);
        }
        Ok(Self::Finite {
            negative: negative && coefficient != "0",
            coefficient,
            display_scale,
        })
    }

    pub fn from_decimal_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        let value = value.trim();
        let negative = value.starts_with('-');
        let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
        let (mantissa, exponent) = unsigned
            .split_once(['e', 'E'])
            .map(|(mantissa, exponent)| {
                if exponent.contains(['e', 'E']) {
                    return Err(PgCanonicalValueError::InvalidNumericCoefficient);
                }
                let exponent_digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
                if exponent_digits.is_empty()
                    || !exponent_digits.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return Err(PgCanonicalValueError::InvalidNumericCoefficient);
                }
                let exponent = exponent
                    .parse::<i32>()
                    .map_err(|_| PgCanonicalValueError::NumericOverflow)?;
                Ok((mantissa, exponent))
            })
            .transpose()?
            .unwrap_or((unsigned, 0));
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if whole.is_empty() && fraction.is_empty()
            || !whole.bytes().all(|byte| byte.is_ascii_digit())
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(PgCanonicalValueError::InvalidNumericCoefficient);
        }
        // The coefficient is `whole ++ fraction` without leading zeros, built
        // once at its final size: it becomes the value's own string.
        let whole = whole.trim_start_matches('0');
        let fraction_digits = if whole.is_empty() {
            fraction.trim_start_matches('0')
        } else {
            fraction
        };
        let mut coefficient = String::with_capacity(whole.len() + fraction_digits.len() + 1);
        coefficient.push_str(whole);
        coefficient.push_str(fraction_digits);
        if coefficient.is_empty() {
            coefficient.push('0');
        }
        let display_scale = i32::try_from(fraction.len())
            .map_err(|_| PgCanonicalValueError::NumericOverflow)?
            .checked_sub(exponent)
            .ok_or(PgCanonicalValueError::NumericOverflow)?;
        let value = Self::finite(negative, coefficient, display_scale)?;
        value.validate_unconstrained_range()?;
        Ok(value)
    }

    pub fn from_postgres_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "nan" => Ok(Self::NaN),
            "infinity" | "+infinity" | "inf" | "+inf" => Ok(Self::PositiveInfinity),
            "-infinity" | "-inf" => Ok(Self::NegativeInfinity),
            _ => Self::from_decimal_text(value),
        }
    }

    pub fn to_decimal_text(&self) -> String {
        let Self::Finite {
            negative,
            coefficient,
            display_scale,
        } = self
        else {
            return match self {
                Self::NaN => "NaN".to_string(),
                Self::PositiveInfinity => "Infinity".to_string(),
                Self::NegativeInfinity => "-Infinity".to_string(),
                Self::Finite { .. } => unreachable!(),
            };
        };

        let rendered = if coefficient == "0" && *display_scale <= 0 {
            "0".to_string()
        } else if *display_scale == 0 {
            coefficient.clone()
        } else if *display_scale < 0 {
            format!(
                "{coefficient}{}",
                "0".repeat(display_scale.unsigned_abs() as usize)
            )
        } else {
            let scale = *display_scale as usize;
            if coefficient.len() <= scale {
                format!("0.{}{coefficient}", "0".repeat(scale - coefficient.len()))
            } else {
                let split = coefficient.len() - scale;
                format!("{}.{}", &coefficient[..split], &coefficient[split..])
            }
        };
        if *negative {
            format!("-{rendered}")
        } else {
            rendered
        }
    }

    pub fn validate_unconstrained_range(&self) -> Result<(), PgCanonicalValueError> {
        let Self::Finite {
            coefficient,
            display_scale,
            ..
        } = self
        else {
            return Ok(());
        };
        if *display_scale > PG_NUMERIC_MAX_FRACTIONAL_DIGITS {
            return Err(PgCanonicalValueError::NumericOverflow);
        }
        if coefficient != "0" {
            let integer_digits = i32::try_from(coefficient.len())
                .map_err(|_| PgCanonicalValueError::NumericOverflow)?
                .checked_sub(*display_scale)
                .ok_or(PgCanonicalValueError::NumericOverflow)?;
            if integer_digits > PG_NUMERIC_MAX_INTEGER_DIGITS {
                return Err(PgCanonicalValueError::NumericOverflow);
            }
        }
        Ok(())
    }

    pub fn with_typmod(&self, precision: u16, scale: i16) -> Result<Self, PgCanonicalValueError> {
        match self {
            Self::NaN => Ok(Self::NaN),
            Self::PositiveInfinity | Self::NegativeInfinity => {
                Err(PgCanonicalValueError::NumericOverflow)
            }
            Self::Finite {
                negative,
                coefficient,
                display_scale,
            } => {
                let target_scale = i32::from(scale);
                let mut coefficient = coefficient.clone();
                let remove = display_scale.saturating_sub(target_scale);
                if remove > 0 {
                    let remove = usize::try_from(remove)
                        .map_err(|_| PgCanonicalValueError::NumericOverflow)?;
                    let keep = coefficient.len().saturating_sub(remove);
                    let round_up = coefficient
                        .as_bytes()
                        .get(keep)
                        .is_some_and(|digit| *digit >= b'5');
                    coefficient.truncate(keep);
                    if coefficient.is_empty() {
                        coefficient.push('0');
                    }
                    if round_up {
                        increment_decimal_digits(&mut coefficient);
                    }
                } else if remove < 0 {
                    coefficient.push_str(&"0".repeat(remove.unsigned_abs() as usize));
                }
                let coefficient = coefficient.trim_start_matches('0');
                let coefficient = if coefficient.is_empty() {
                    "0".to_string()
                } else {
                    coefficient.to_string()
                };
                let integer_digits = if coefficient == "0" {
                    i32::MIN
                } else {
                    i32::try_from(coefficient.len())
                        .map_err(|_| PgCanonicalValueError::NumericOverflow)?
                        .checked_sub(target_scale)
                        .ok_or(PgCanonicalValueError::NumericOverflow)?
                };
                let allowed_integer_digits = i32::from(precision) - target_scale;
                if integer_digits > allowed_integer_digits {
                    return Err(PgCanonicalValueError::NumericOverflow);
                }
                Self::finite(*negative, coefficient, target_scale)
            }
        }
    }
}

fn increment_decimal_digits(value: &mut String) {
    let mut bytes = value.as_bytes().to_vec();
    for digit in bytes.iter_mut().rev() {
        if *digit < b'9' {
            *digit += 1;
            *value = String::from_utf8(bytes).expect("decimal digits are ASCII");
            return;
        }
        *digit = b'0';
    }
    bytes.insert(0, b'1');
    *value = String::from_utf8(bytes).expect("decimal digits are ASCII");
}

pub fn pg_money_cents_from_text(value: &str) -> Result<i64, PgCanonicalValueError> {
    let value = value.trim();
    let (negative_parentheses, value) = if value.starts_with('(') && value.ends_with(')') {
        (true, &value[1..value.len() - 1])
    } else {
        (false, value)
    };
    let mut normalized = value.trim().replace(',', "");
    if let Some(without_currency) = normalized.strip_prefix('$') {
        normalized = without_currency.to_string();
    } else if let Some(without_sign) = normalized.strip_prefix('-') {
        normalized = format!(
            "-{}",
            without_sign.strip_prefix('$').unwrap_or(without_sign)
        );
    } else if let Some(without_sign) = normalized.strip_prefix('+') {
        normalized = without_sign
            .strip_prefix('$')
            .unwrap_or(without_sign)
            .to_string();
    }
    if negative_parentheses {
        if normalized.starts_with('-') {
            return Err(PgCanonicalValueError::InvalidNumericCoefficient);
        }
        normalized.insert(0, '-');
    }
    let PgNumeric::Finite {
        negative,
        coefficient,
        display_scale,
    } = PgNumeric::from_decimal_text(&normalized)?
    else {
        return Err(PgCanonicalValueError::InvalidNumericCoefficient);
    };
    let mut cents = BigInt::parse_bytes(coefficient.as_bytes(), 10)
        .expect("PgNumeric coefficients contain decimal digits");
    if display_scale <= 2 {
        cents *= BigInt::from(10_u8).pow((2 - display_scale) as u32);
    } else {
        let divisor = BigInt::from(10_u8).pow((display_scale - 2) as u32);
        let mut quotient = &cents / &divisor;
        let remainder = &cents % &divisor;
        if remainder * 2 >= divisor {
            quotient += 1;
        }
        cents = quotient;
    }
    if negative {
        cents = -cents;
    }
    cents.to_i64().ok_or(PgCanonicalValueError::NumericOverflow)
}

pub fn pg_money_text_from_cents(cents: i64) -> String {
    let negative = cents.is_negative();
    let magnitude = i128::from(cents).abs();
    let rendered = format!("{}.{:02}", magnitude / 100, magnitude % 100);
    if negative {
        format!("-{rendered}")
    } else {
        rendered
    }
}

pub fn pg_money_display_from_cents(cents: i64) -> String {
    let negative = cents.is_negative();
    let magnitude = i128::from(cents).abs();
    let whole = magnitude / 100;
    let digits = whole.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, digit) in digits.bytes().enumerate() {
        if idx > 0 && (digits.len() - idx).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit as char);
    }
    let rendered = format!("${grouped}.{:02}", magnitude % 100);
    if negative {
        format!("-{rendered}")
    } else {
        rendered
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PgBitString {
    bytes: Vec<u8>,
    bit_len: usize,
}

impl PgBitString {
    pub fn new(bytes: Vec<u8>, bit_len: usize) -> Result<Self, PgCanonicalValueError> {
        let available_bits = bytes.len().saturating_mul(8);
        if bit_len > available_bits || available_bits.saturating_sub(bit_len) >= 8 {
            return Err(PgCanonicalValueError::InvalidBitLength {
                bit_len,
                available_bits,
            });
        }
        let unused = available_bits - bit_len;
        if unused > 0
            && bytes
                .last()
                .is_some_and(|byte| byte & ((1 << unused) - 1) != 0)
        {
            return Err(PgCanonicalValueError::NonCanonicalBitPadding);
        }
        Ok(Self { bytes, bit_len })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn bit_len(&self) -> usize {
        self.bit_len
    }

    pub fn from_bit_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        if !value.bytes().all(|byte| matches!(byte, b'0' | b'1')) {
            return Err(PgCanonicalValueError::InvalidBitString);
        }
        let mut bytes = vec![0_u8; value.len().div_ceil(8)];
        for (index, bit) in value.bytes().enumerate() {
            if bit == b'1' {
                bytes[index / 8] |= 1 << (7 - index % 8);
            }
        }
        Self::new(bytes, value.len())
    }

    pub fn from_postgres_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        match value.as_bytes().split_first() {
            Some((b'b' | b'B', bits)) => {
                Self::from_bit_text(std::str::from_utf8(bits).unwrap_or_default())
            }
            Some((b'x' | b'X', hex)) => {
                Self::from_hex_text(std::str::from_utf8(hex).unwrap_or_default())
            }
            _ => Self::from_bit_text(value),
        }
    }

    pub fn from_hex_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        if !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(PgCanonicalValueError::InvalidBitString);
        }
        let mut bits = String::with_capacity(value.len().saturating_mul(4));
        for digit in value.bytes() {
            let value = match digit {
                b'0'..=b'9' => digit - b'0',
                b'a'..=b'f' => digit - b'a' + 10,
                b'A'..=b'F' => digit - b'A' + 10,
                _ => unreachable!("hex input was validated"),
            };
            for shift in (0..4).rev() {
                bits.push(if value & (1 << shift) == 0 { '0' } else { '1' });
            }
        }
        Self::from_bit_text(&bits)
    }

    pub fn to_bit_text(&self) -> String {
        (0..self.bit_len)
            .map(|index| {
                if self.bytes[index / 8] & (1 << (7 - index % 8)) == 0 {
                    '0'
                } else {
                    '1'
                }
            })
            .collect()
    }
}

pub fn parse_bytea_text(value: &str) -> Result<Vec<u8>, PgCanonicalValueError> {
    if let Some(hex) = value.strip_prefix("\\x") {
        if hex.len() % 2 != 0 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(PgCanonicalValueError::InvalidByteaHex);
        }
        return (0..hex.len())
            .step_by(2)
            .map(|index| {
                u8::from_str_radix(&hex[index..index + 2], 16)
                    .map_err(|_| PgCanonicalValueError::InvalidByteaHex)
            })
            .collect();
    }

    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        if bytes.get(index + 1) == Some(&b'\\') {
            decoded.push(b'\\');
            index += 2;
            continue;
        }
        let Some(octal) = bytes.get(index + 1..index + 4) else {
            return Err(PgCanonicalValueError::InvalidBytea);
        };
        if !octal.iter().all(|byte| matches!(byte, b'0'..=b'7')) {
            return Err(PgCanonicalValueError::InvalidBytea);
        }
        let decoded_byte = u16::from(octal[0] - b'0') * 64
            + u16::from(octal[1] - b'0') * 8
            + u16::from(octal[2] - b'0');
        decoded.push(u8::try_from(decoded_byte).map_err(|_| PgCanonicalValueError::InvalidBytea)?);
        index += 4;
    }
    Ok(decoded)
}

pub fn format_bytea_hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(2 + value.len() * 2);
    encoded.push_str("\\x");
    for byte in value {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

pub fn format_bytea_escape(value: &[u8]) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value {
        match byte {
            b'\\' => encoded.push_str("\\\\"),
            0x20..=0x7e => encoded.push(char::from(*byte)),
            _ => encoded.push_str(&format!("\\{byte:03o}")),
        }
    }
    encoded
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "days", rename_all = "snake_case")]
pub enum PgDate {
    /// Days since PostgreSQL's 2000-01-01 epoch.
    Finite(i32),
    PositiveInfinity,
    NegativeInfinity,
}

/// JavaScript and several JSON database drivers materialize a SQL DATE as a
/// UTC-midnight `Date` and serialize it back as RFC 3339. Accept that exact,
/// lossless representation at the SQL boundary while continuing to reject
/// timestamps whose time or offset could change the calendar date.
fn iso_utc_midnight_date_prefix(value: &str) -> Option<&str> {
    let date = value.get(..10)?;
    let timestamp = value.get(10..)?.strip_prefix('T')?;
    let clock = timestamp
        .strip_suffix('Z')
        .or_else(|| timestamp.strip_suffix('z'))
        .or_else(|| timestamp.strip_suffix("+00:00"))
        .or_else(|| timestamp.strip_suffix("-00:00"))?;
    let midnight = clock == "00:00:00"
        || clock.strip_prefix("00:00:00.").is_some_and(|fraction| {
            !fraction.is_empty() && fraction.bytes().all(|byte| byte.is_ascii_digit())
        });
    midnight.then_some(date)
}

/// The date prefix of a `YYYY-MM-DD HH:MM[:SS[.f]][zone]` timestamp text,
/// with either the space or `T` separator; `None` when the text is not
/// timestamp-shaped. PostgreSQL's date reader truncates such input to its
/// date part.
fn timestamp_text_date_prefix(value: &str) -> Option<&str> {
    let date = value.get(..10)?;
    if !date.bytes().enumerate().all(|(index, byte)| match index {
        4 | 7 => byte == b'-',
        _ => byte.is_ascii_digit(),
    }) {
        return None;
    }
    let rest = value.get(10..)?;
    let clock = rest.strip_prefix([' ', 'T'])?;
    if clock.len() < 5 || !clock.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let valid = clock.bytes().all(|byte| {
        byte.is_ascii_digit() || matches!(byte, b':' | b'.' | b'+' | b'-' | b' ' | b'Z' | b'z')
    });
    valid.then_some(date)
}

impl PgDate {
    pub fn from_iso_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        let value = value.trim();
        if let Some(date) = iso_utc_midnight_date_prefix(value) {
            return Self::from_postgres_text(date);
        }
        if timestamp_text_date_prefix(value).is_some() {
            return Err(PgCanonicalValueError::InvalidTemporal {
                kind: "date",
                value: value.to_string(),
            });
        }
        Self::from_postgres_text(value)
    }

    pub fn from_postgres_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        let value = value.trim();
        // PostgreSQL date input accepts full timestamp text and truncates to
        // its date part (`'2026-08-01 00:00:00'::date`, any time of day).
        let value = timestamp_text_date_prefix(value).unwrap_or(value);
        match value.to_ascii_lowercase().as_str() {
            "infinity" | "+infinity" => return Ok(Self::PositiveInfinity),
            "-infinity" => return Ok(Self::NegativeInfinity),
            "epoch" => return Self::from_ymd(1970, 1, 1),
            _ => {}
        }

        if let Some(julian_day) = value
            .strip_prefix(['J', 'j'])
            .filter(|julian| !julian.is_empty() && julian.bytes().all(|byte| byte.is_ascii_digit()))
        {
            let julian_day = julian_day
                .parse::<i64>()
                .map_err(|_| PgCanonicalValueError::TemporalOverflow("date"))?;
            return Self::from_epoch_days(julian_day - POSTGRES_EPOCH_JULIAN_DAY);
        }

        let (date, bc) = split_date_era(value)?;
        let (year, month, day) = parse_postgres_date(date, "date")?;
        if year == 0 {
            return Err(PgCanonicalValueError::TemporalFieldOverflow("date"));
        }
        let year = if bc { 1 - year } else { year };
        Self::from_ymd(year, month, day)
    }

    pub fn from_ymd(year: i32, month: u32, day: u32) -> Result<Self, PgCanonicalValueError> {
        validate_calendar_date(year, month, day, "date")?;
        let days = days_from_civil(year, month, day)
            .checked_sub(POSTGRES_EPOCH_UNIX_DAYS)
            .ok_or(PgCanonicalValueError::TemporalOverflow("date"))?;
        Self::from_epoch_days(days)
    }

    pub fn from_epoch_days(days: i64) -> Result<Self, PgCanonicalValueError> {
        if !(i64::from(POSTGRES_DATE_MIN_DAYS)..=i64::from(POSTGRES_DATE_MAX_DAYS)).contains(&days)
        {
            return Err(PgCanonicalValueError::TemporalOverflow("date"));
        }
        Ok(Self::Finite(days as i32))
    }

    pub fn epoch_days(self) -> Option<i32> {
        match self {
            Self::Finite(days) => Some(days),
            Self::PositiveInfinity | Self::NegativeInfinity => None,
        }
    }

    pub fn checked_add_days(self, days: i32) -> Result<Self, PgCanonicalValueError> {
        match self {
            Self::Finite(value) => Self::from_epoch_days(i64::from(value) + i64::from(days)),
            infinity => Ok(infinity),
        }
    }

    pub fn to_iso_text(self) -> String {
        match self {
            Self::PositiveInfinity => "infinity".to_string(),
            Self::NegativeInfinity => "-infinity".to_string(),
            Self::Finite(days) => {
                let (year, month, day) =
                    civil_from_days(i64::from(days) + POSTGRES_EPOCH_UNIX_DAYS);
                if year <= 0 {
                    format!("{:04}-{month:02}-{day:02} BC", 1_i64 - i64::from(year))
                } else {
                    format!("{year:04}-{month:02}-{day:02}")
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PgTime(i64);

impl PgTime {
    pub fn from_micros_since_midnight(micros: i64) -> Result<Self, PgCanonicalValueError> {
        if !(0..=MICROS_PER_DAY).contains(&micros) {
            return Err(PgCanonicalValueError::InvalidTime(micros));
        }
        Ok(Self(micros))
    }

    pub fn micros_since_midnight(self) -> i64 {
        self.0
    }

    pub fn from_iso_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        let (clock, offset) = split_clock_offset(value.trim(), "time")?;
        if offset.is_some() {
            return Err(invalid_temporal("time", value));
        }
        Self::from_micros_since_midnight(parse_clock_micros(clock, "time")?)
    }

    pub fn from_postgres_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        let original = value.trim();
        if original.eq_ignore_ascii_case("allballs") {
            return Ok(Self(0));
        }

        let mut clock = original;
        let mut meridiem = None;
        if clock.len() >= 2 {
            let suffix = &clock[clock.len() - 2..];
            if suffix.eq_ignore_ascii_case("am") || suffix.eq_ignore_ascii_case("pm") {
                meridiem = Some(suffix.eq_ignore_ascii_case("pm"));
                clock = clock[..clock.len() - 2].trim_end();
            }
        }
        if let Some((clock_part, abbreviation)) = clock.split_once(char::is_whitespace) {
            let abbreviation = abbreviation.trim();
            if abbreviation.is_empty() || !valid_time_zone_abbreviation(abbreviation) {
                return Err(invalid_temporal("time", original));
            }
            clock = clock_part;
        }
        let (clock_without_offset, offset) = split_clock_offset(clock, "time")?;
        if offset.is_some() {
            clock = clock_without_offset;
        }
        clock = clock.strip_prefix('T').unwrap_or(clock);

        let expanded;
        if !clock.contains(':') {
            let (digits, fraction) = clock.split_once('.').unwrap_or((clock, ""));
            if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid_temporal("time", original));
            }
            expanded = match digits.len() {
                4 => format!(
                    "{}:{}{}",
                    &digits[..2],
                    &digits[2..],
                    fraction_suffix(fraction)
                ),
                6 => format!(
                    "{}:{}:{}{}",
                    &digits[..2],
                    &digits[2..4],
                    &digits[4..],
                    fraction_suffix(fraction)
                ),
                _ => return Err(invalid_temporal("time", original)),
            };
            clock = &expanded;
        }

        let mut micros = parse_clock_micros(clock, "time")?;
        if let Some(pm) = meridiem {
            let hour = micros / 3_600_000_000;
            if !(1..=12).contains(&hour) {
                return Err(PgCanonicalValueError::TemporalFieldOverflow("time"));
            }
            if pm && hour != 12 {
                micros += 12 * 3_600_000_000;
            } else if !pm && hour == 12 {
                micros -= 12 * 3_600_000_000;
            }
        }
        Self::from_micros_since_midnight(micros)
    }

    pub fn with_precision(self, precision: u8) -> Result<Self, PgCanonicalValueError> {
        if precision > 6 {
            return Err(PgCanonicalValueError::TemporalFieldOverflow("time"));
        }
        let quantum = 10_i64.pow(u32::from(6 - precision));
        let rounded = self
            .0
            .checked_add(quantum / 2)
            .map(|value| value / quantum * quantum)
            .ok_or(PgCanonicalValueError::TemporalOverflow("time"))?;
        Self::from_micros_since_midnight(rounded)
    }

    pub fn to_iso_text(self) -> String {
        format_clock_micros(self.0)
    }
}

fn fraction_suffix(fraction: &str) -> String {
    if fraction.is_empty() {
        String::new()
    } else {
        format!(".{fraction}")
    }
}

fn valid_time_zone_abbreviation(value: &str) -> bool {
    matches!(
        value.to_ascii_uppercase().as_str(),
        "UTC"
            | "GMT"
            | "PST"
            | "PDT"
            | "MST"
            | "MDT"
            | "CST"
            | "CDT"
            | "EST"
            | "EDT"
            | "CET"
            | "CEST"
            | "EET"
            | "EEST"
            | "BST"
            | "IST"
            | "JST"
            | "AEST"
            | "AEDT"
            | "ACST"
            | "ACDT"
            | "NZST"
            | "NZDT"
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PgTimeTz {
    pub time: PgTime,
    /// Signed seconds east of UTC, independent of the host timezone.
    pub utc_offset_seconds: i32,
}

impl PgTimeTz {
    pub fn new(time: PgTime, utc_offset_seconds: i32) -> Result<Self, PgCanonicalValueError> {
        if utc_offset_seconds.unsigned_abs() > MAX_TIMEZONE_OFFSET_SECONDS as u32 {
            return Err(PgCanonicalValueError::InvalidTimezoneOffset(
                utc_offset_seconds,
            ));
        }
        Ok(Self {
            time,
            utc_offset_seconds,
        })
    }

    pub fn from_iso_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        let (clock, offset) = split_clock_offset(value.trim(), "timetz")?;
        let time = PgTime::from_micros_since_midnight(parse_clock_micros(clock, "timetz")?)?;
        Self::new(time, offset.unwrap_or(0))
    }

    pub fn from_postgres_text(
        value: &str,
        default_offset_seconds: i32,
    ) -> Result<Self, PgCanonicalValueError> {
        let (clock, offset) = split_clock_offset(value.trim(), "timetz")?;
        let time = PgTime::from_postgres_text(clock)?;
        Self::new(time, offset.unwrap_or(default_offset_seconds))
    }

    pub fn with_precision(self, precision: u8) -> Result<Self, PgCanonicalValueError> {
        Self::new(
            self.time.with_precision(precision)?,
            self.utc_offset_seconds,
        )
    }

    pub fn to_iso_text(self) -> String {
        let offset = self.utc_offset_seconds;
        let sign = if offset < 0 { '-' } else { '+' };
        let magnitude = offset.unsigned_abs();
        let hours = magnitude / 3_600;
        let minutes = magnitude % 3_600 / 60;
        let seconds = magnitude % 60;
        let suffix = if minutes == 0 && seconds == 0 {
            format!("{sign}{hours:02}")
        } else if seconds == 0 {
            format!("{sign}{hours:02}:{minutes:02}")
        } else {
            format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
        };
        format!("{}{suffix}", self.time.to_iso_text())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "micros", rename_all = "snake_case")]
pub enum PgTimestamp {
    /// Microseconds since PostgreSQL's 2000-01-01 epoch.
    Finite(i64),
    PositiveInfinity,
    NegativeInfinity,
}

impl PgTimestamp {
    pub fn from_iso_text(value: &str, with_timezone: bool) -> Result<Self, PgCanonicalValueError> {
        Self::from_postgres_text(value, with_timezone)
    }

    pub fn from_postgres_text(
        value: &str,
        with_timezone: bool,
    ) -> Result<Self, PgCanonicalValueError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "infinity" => return Ok(Self::PositiveInfinity),
            "+infinity" => return Ok(Self::PositiveInfinity),
            "-infinity" => return Ok(Self::NegativeInfinity),
            "epoch" => return Ok(Self::Finite(-POSTGRES_EPOCH_UNIX_DAYS * MICROS_PER_DAY)),
            _ => {}
        }
        let original = value.trim();
        let (value, bc) = split_timestamp_era(original)?;
        let (date, clock) = split_timestamp_date_clock(value)?;
        let date_value = if bc {
            format!("{date} BC")
        } else {
            date.to_string()
        };
        let date = PgDate::from_postgres_text(&date_value)?;
        let PgDate::Finite(date_days) = date else {
            return Ok(match date {
                PgDate::PositiveInfinity => Self::PositiveInfinity,
                PgDate::NegativeInfinity => Self::NegativeInfinity,
                PgDate::Finite(_) => unreachable!(),
            });
        };
        let clock = clock.unwrap_or("00:00:00");
        let (clock, offset) = split_clock_offset(clock, "timestamp")?;
        let mut micros = i64::from(date_days)
            .checked_mul(MICROS_PER_DAY)
            .and_then(|base| {
                base.checked_add(
                    PgTime::from_postgres_text(clock)
                        .ok()?
                        .micros_since_midnight(),
                )
            })
            .ok_or(PgCanonicalValueError::TemporalOverflow("timestamp"))?;
        if with_timezone {
            micros = micros
                .checked_sub(i64::from(offset.unwrap_or(0)) * 1_000_000)
                .ok_or(PgCanonicalValueError::TemporalOverflow("timestamptz"))?;
        }
        Ok(Self::Finite(micros))
    }

    pub fn with_precision(self, precision: u8) -> Result<Self, PgCanonicalValueError> {
        if precision > 6 {
            return Err(PgCanonicalValueError::TemporalFieldOverflow("timestamp"));
        }
        let Self::Finite(micros) = self else {
            return Ok(self);
        };
        let quantum = 10_i64.pow(u32::from(6 - precision));
        let half = quantum / 2;
        let adjusted = if micros >= 0 {
            micros.checked_add(half)
        } else {
            micros.checked_sub(half)
        }
        .ok_or(PgCanonicalValueError::TemporalOverflow("timestamp"))?;
        Ok(Self::Finite(adjusted / quantum * quantum))
    }

    pub fn checked_add_interval(self, interval: PgInterval) -> Result<Self, PgCanonicalValueError> {
        let Self::Finite(micros) = self else {
            return Ok(self);
        };
        let days = micros.div_euclid(MICROS_PER_DAY);
        let clock = micros.rem_euclid(MICROS_PER_DAY);
        let (year, month, day) = civil_from_days(days + POSTGRES_EPOCH_UNIX_DAYS);
        let month_index = i64::from(year)
            .checked_mul(12)
            .and_then(|value| value.checked_add(i64::from(month) - 1))
            .and_then(|value| value.checked_add(i64::from(interval.months)))
            .ok_or(PgCanonicalValueError::TemporalOverflow("timestamp"))?;
        let target_year = i32::try_from(month_index.div_euclid(12))
            .map_err(|_| PgCanonicalValueError::TemporalOverflow("timestamp"))?;
        let target_month = month_index.rem_euclid(12) as u32 + 1;
        let target_day = day.min(calendar_month_days(target_year, target_month));
        let target_days = days_from_civil(target_year, target_month, target_day)
            .checked_sub(POSTGRES_EPOCH_UNIX_DAYS)
            .and_then(|value| value.checked_add(i64::from(interval.days)))
            .ok_or(PgCanonicalValueError::TemporalOverflow("timestamp"))?;
        let target = target_days
            .checked_mul(MICROS_PER_DAY)
            .and_then(|value| value.checked_add(clock))
            .and_then(|value| value.checked_add(interval.micros))
            .ok_or(PgCanonicalValueError::TemporalOverflow("timestamp"))?;
        Ok(Self::Finite(target))
    }

    pub fn checked_sub_interval(self, interval: PgInterval) -> Result<Self, PgCanonicalValueError> {
        self.checked_add_interval(PgInterval {
            months: interval
                .months
                .checked_neg()
                .ok_or(PgCanonicalValueError::TemporalOverflow("timestamp"))?,
            days: interval
                .days
                .checked_neg()
                .ok_or(PgCanonicalValueError::TemporalOverflow("timestamp"))?,
            micros: interval
                .micros
                .checked_neg()
                .ok_or(PgCanonicalValueError::TemporalOverflow("timestamp"))?,
        })
    }

    pub fn checked_difference(self, other: Self) -> Result<PgInterval, PgCanonicalValueError> {
        let (Self::Finite(left), Self::Finite(right)) = (self, other) else {
            return Err(PgCanonicalValueError::TemporalOverflow("timestamp"));
        };
        let difference = left
            .checked_sub(right)
            .ok_or(PgCanonicalValueError::TemporalOverflow("timestamp"))?;
        Ok(PgInterval {
            months: 0,
            days: i32::try_from(difference / MICROS_PER_DAY)
                .map_err(|_| PgCanonicalValueError::TemporalOverflow("interval"))?,
            micros: difference % MICROS_PER_DAY,
        })
    }

    pub fn finite_micros(self) -> Option<i64> {
        match self {
            Self::Finite(micros) => Some(micros),
            Self::PositiveInfinity | Self::NegativeInfinity => None,
        }
    }

    pub fn components(self) -> Option<(i32, u32, u32, u32, u32, u32, u32)> {
        let Self::Finite(micros) = self else {
            return None;
        };
        let days = micros.div_euclid(MICROS_PER_DAY);
        let clock = micros.rem_euclid(MICROS_PER_DAY);
        let (year, month, day) = civil_from_days(days + POSTGRES_EPOCH_UNIX_DAYS);
        Some((
            year,
            month,
            day,
            (clock / 3_600_000_000) as u32,
            (clock % 3_600_000_000 / 60_000_000) as u32,
            (clock % 60_000_000 / 1_000_000) as u32,
            (clock % 1_000_000) as u32,
        ))
    }

    pub fn truncate(self, field: &str) -> Result<Self, PgCanonicalValueError> {
        let Self::Finite(micros) = self else {
            return Ok(self);
        };
        let days = micros.div_euclid(MICROS_PER_DAY);
        let clock = micros.rem_euclid(MICROS_PER_DAY);
        let (mut year, mut month, mut day) = civil_from_days(days + POSTGRES_EPOCH_UNIX_DAYS);
        let mut truncated_clock = clock;
        match field.to_ascii_lowercase().as_str() {
            "microseconds" | "microsecond" => {}
            "milliseconds" | "millisecond" => truncated_clock = clock / 1_000 * 1_000,
            "second" => truncated_clock = clock / 1_000_000 * 1_000_000,
            "minute" => truncated_clock = clock / 60_000_000 * 60_000_000,
            "hour" => truncated_clock = clock / 3_600_000_000 * 3_600_000_000,
            "day" => truncated_clock = 0,
            "week" => {
                let monday_offset = (days + POSTGRES_EPOCH_UNIX_DAYS + 3).rem_euclid(7);
                let monday = days - monday_offset;
                (year, month, day) = civil_from_days(monday + POSTGRES_EPOCH_UNIX_DAYS);
                truncated_clock = 0;
            }
            "month" => {
                day = 1;
                truncated_clock = 0;
            }
            "quarter" => {
                month = (month - 1) / 3 * 3 + 1;
                day = 1;
                truncated_clock = 0;
            }
            "year" => {
                month = 1;
                day = 1;
                truncated_clock = 0;
            }
            "decade" => {
                year = year.div_euclid(10) * 10;
                month = 1;
                day = 1;
                truncated_clock = 0;
            }
            "century" => {
                year = if year > 0 {
                    (year - 1).div_euclid(100) * 100 + 1
                } else {
                    year.div_euclid(100) * 100
                };
                month = 1;
                day = 1;
                truncated_clock = 0;
            }
            "millennium" => {
                year = if year > 0 {
                    (year - 1).div_euclid(1_000) * 1_000 + 1
                } else {
                    year.div_euclid(1_000) * 1_000
                };
                month = 1;
                day = 1;
                truncated_clock = 0;
            }
            _ => return Err(invalid_temporal("timestamp truncation field", field)),
        }
        let target_days = days_from_civil(year, month, day) - POSTGRES_EPOCH_UNIX_DAYS;
        let target = target_days
            .checked_mul(MICROS_PER_DAY)
            .and_then(|value| value.checked_add(truncated_clock))
            .ok_or(PgCanonicalValueError::TemporalOverflow("timestamp"))?;
        Ok(Self::Finite(target))
    }

    pub fn extract_field(self, field: &str) -> Option<String> {
        let Self::Finite(micros) = self else {
            return None;
        };
        let days = micros.div_euclid(MICROS_PER_DAY);
        let clock = micros.rem_euclid(MICROS_PER_DAY);
        let (year, month, day) = civil_from_days(days + POSTGRES_EPOCH_UNIX_DAYS);
        let display_year = if year <= 0 { year - 1 } else { year };
        let hour = clock / 3_600_000_000;
        let minute = clock % 3_600_000_000 / 60_000_000;
        let second = clock % 60_000_000;
        let value = match field.to_ascii_lowercase().as_str() {
            "year" => display_year.to_string(),
            "decade" => (display_year / 10).to_string(),
            "century" => {
                let century = if display_year > 0 {
                    (display_year - 1) / 100 + 1
                } else {
                    display_year / 100 - 1
                };
                century.to_string()
            }
            "millennium" => {
                let millennium = if display_year > 0 {
                    (display_year - 1) / 1_000 + 1
                } else {
                    display_year / 1_000 - 1
                };
                millennium.to_string()
            }
            "quarter" => ((month - 1) / 3 + 1).to_string(),
            "month" => month.to_string(),
            "day" => day.to_string(),
            "hour" => hour.to_string(),
            "minute" => minute.to_string(),
            "second" => format!("{}.{:06}", second / 1_000_000, second % 1_000_000),
            "dow" => (days + POSTGRES_EPOCH_UNIX_DAYS + 4)
                .rem_euclid(7)
                .to_string(),
            "isodow" => ((days + POSTGRES_EPOCH_UNIX_DAYS + 3).rem_euclid(7) + 1).to_string(),
            "doy" => {
                let ordinal = days_from_civil(year, month, day) - days_from_civil(year, 1, 1) + 1;
                ordinal.to_string()
            }
            "epoch" => format_decimal_micros(
                micros.checked_add(POSTGRES_EPOCH_UNIX_DAYS * MICROS_PER_DAY)?,
            ),
            _ => return None,
        };
        Some(value)
    }

    pub fn to_iso_text(self, with_timezone: bool) -> String {
        match self {
            Self::PositiveInfinity => "infinity".to_string(),
            Self::NegativeInfinity => "-infinity".to_string(),
            Self::Finite(micros) => {
                let days = micros.div_euclid(MICROS_PER_DAY);
                let clock = PgTime::from_micros_since_midnight(micros.rem_euclid(MICROS_PER_DAY))
                    .expect("timestamp remainder is a valid time");
                let (year, month, day) = civil_from_days(days + POSTGRES_EPOCH_UNIX_DAYS);
                let timezone = if with_timezone { "+00" } else { "" };
                let date = if year <= 0 {
                    format!("{:04}-{month:02}-{day:02}", 1_i64 - i64::from(year))
                } else {
                    format!("{year:04}-{month:02}-{day:02}")
                };
                let era = if year <= 0 { " BC" } else { "" };
                format!("{date} {}{timezone}{era}", clock.to_iso_text())
            }
        }
    }
}

fn format_decimal_micros(value: i64) -> String {
    let negative = value < 0;
    let magnitude = i128::from(value).abs();
    let whole = magnitude / 1_000_000;
    let fraction = magnitude % 1_000_000;
    format!("{}{whole}.{fraction:06}", if negative { "-" } else { "" })
}

fn split_timestamp_era(value: &str) -> Result<(&str, bool), PgCanonicalValueError> {
    let upper = value.to_ascii_uppercase();
    for (suffix, bc) in [(" BC", true), (" AD", false)] {
        if upper.ends_with(suffix) {
            let timestamp = value[..value.len() - suffix.len()].trim_end();
            if timestamp.is_empty() {
                return Err(invalid_temporal("timestamp", value));
            }
            return Ok((timestamp, bc));
        }
    }
    Ok((value, false))
}

fn split_timestamp_date_clock(value: &str) -> Result<(&str, Option<&str>), PgCanonicalValueError> {
    if let Some(index) = value.find(['T', 't']) {
        let date = value[..index].trim();
        let clock = value[index + 1..].trim();
        if date.is_empty() || clock.is_empty() {
            return Err(invalid_temporal("timestamp", value));
        }
        return Ok((date, Some(clock)));
    }
    let parts = value.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        return Err(invalid_temporal("timestamp", value));
    }
    let date_parts = if postgres_month_number(parts[0]).is_some() {
        3
    } else if parts
        .get(1)
        .and_then(|part| postgres_month_number(part))
        .is_some()
    {
        3
    } else {
        1
    };
    if parts.len() < date_parts {
        return Err(invalid_temporal("timestamp", value));
    }
    let date_end = parts[..date_parts].join(" ");
    let date = value
        .get(..date_end.len())
        .ok_or_else(|| invalid_temporal("timestamp", value))?;
    let clock = value[date_end.len()..].trim();
    Ok((date, (!clock.is_empty()).then_some(clock)))
}

fn calendar_month_days(year: i32, month: u32) -> u32 {
    match month {
        2 if is_gregorian_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PgInterval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl PgInterval {
    pub fn from_postgres_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        let original = value.trim();
        if original.is_empty() {
            return Err(invalid_temporal("interval", original));
        }
        if original
            .trim_start_matches(['+', '-'])
            .starts_with(['P', 'p'])
        {
            return parse_iso_interval(original);
        }
        let mut value = original;
        if let Some(stripped) = value.strip_prefix('@') {
            value = stripped.trim_start();
        }
        let ago = value
            .get(value.len().saturating_sub(3)..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case("ago"));
        if ago {
            value = value[..value.len() - 3].trim_end();
        }
        if !value
            .chars()
            .any(|character| character.is_ascii_alphabetic())
        {
            let mut interval = parse_sql_standard_interval(value)?;
            if ago {
                interval = interval.checked_neg()?;
            }
            return Ok(interval);
        }
        let parts = value.split_whitespace().collect::<Vec<_>>();
        let mut months = 0_i64;
        let mut days = 0_i64;
        let mut micros = 0_i64;
        let mut index = 0;
        while index < parts.len() {
            if parts[index].contains(':') {
                micros = micros
                    .checked_add(parse_interval_clock_micros(parts[index])?)
                    .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
                index += 1;
                continue;
            }
            let amount_text = parts[index];
            let unit = parts
                .get(index + 1)
                .ok_or_else(|| invalid_temporal("interval", original))?
                .to_ascii_lowercase();
            add_interval_unit(
                amount_text,
                &unit,
                &mut months,
                &mut days,
                &mut micros,
                original,
            )?;
            index += 2;
        }
        let mut interval = Self {
            months: i32::try_from(months)
                .map_err(|_| PgCanonicalValueError::TemporalOverflow("interval"))?,
            days: i32::try_from(days)
                .map_err(|_| PgCanonicalValueError::TemporalOverflow("interval"))?,
            micros,
        };
        if ago {
            interval = interval.checked_neg()?;
        }
        Ok(interval)
    }

    pub fn to_postgres_text(self) -> String {
        let mut parts = Vec::new();
        if self.months != 0 {
            let years = self.months / 12;
            let months = self.months % 12;
            if years != 0 {
                parts.push(format!(
                    "{years} {}",
                    if years == 1 { "year" } else { "years" }
                ));
            }
            if months != 0 {
                parts.push(format!(
                    "{months} {}",
                    if months == 1 { "mon" } else { "mons" }
                ));
            }
        }
        if self.days != 0 {
            parts.push(format!(
                "{} {}",
                self.days,
                if self.days == 1 { "day" } else { "days" }
            ));
        }
        if self.micros != 0 || parts.is_empty() {
            let force_positive = self.micros > 0 && (self.months < 0 || self.days < 0);
            parts.push(format_interval_clock(self.micros, force_positive, true));
        }
        parts.join(" ")
    }

    pub fn to_postgres_verbose_text(self) -> String {
        if self.months <= 0 && self.days <= 0 && self.micros <= 0 {
            let positive = self.checked_neg().unwrap_or(self);
            if positive == Self::ZERO {
                return "@ 0".to_string();
            }
            return format!("{} ago", positive.to_verbose_components());
        }
        self.to_verbose_components()
    }

    fn to_verbose_components(self) -> String {
        let mut parts = vec!["@".to_string()];
        let years = self.months / 12;
        let months = self.months % 12;
        push_verbose_interval_part(&mut parts, i64::from(years), "year", "years");
        push_verbose_interval_part(&mut parts, i64::from(months), "mon", "mons");
        push_verbose_interval_part(&mut parts, i64::from(self.days), "day", "days");
        let sign = if self.micros < 0 { -1 } else { 1 };
        let magnitude = i128::from(self.micros).abs();
        let hours = i64::try_from(magnitude / 3_600_000_000).unwrap_or(i64::MAX) * sign;
        let minutes =
            i64::try_from(magnitude % 3_600_000_000 / 60_000_000).unwrap_or(i64::MAX) * sign;
        let seconds = magnitude % 60_000_000;
        push_verbose_interval_part(&mut parts, hours, "hour", "hours");
        push_verbose_interval_part(&mut parts, minutes, "min", "mins");
        if seconds != 0 {
            let mut value = format_interval_decimal_micros(seconds);
            if sign < 0 {
                value.insert(0, '-');
            }
            parts.push(format!("{value} secs"));
        }
        if parts.len() == 1 {
            parts.push("0".to_string());
        }
        parts.join(" ")
    }

    pub fn to_sql_standard_text(self) -> String {
        if self == Self::ZERO {
            return "0".to_string();
        }
        let positive = self.months >= 0 && self.days >= 0 && self.micros >= 0;
        let negative = self.months <= 0 && self.days <= 0 && self.micros <= 0;
        let mixed = !positive && !negative;
        let groups = usize::from(self.months != 0)
            + usize::from(self.days != 0)
            + usize::from(self.micros != 0);
        let explicit_positive = mixed || groups > 1;
        let mut parts = Vec::new();
        if self.months != 0 {
            let sign = if self.months < 0 {
                "-"
            } else if explicit_positive {
                "+"
            } else {
                ""
            };
            let magnitude = self.months.unsigned_abs();
            parts.push(format!("{sign}{}-{}", magnitude / 12, magnitude % 12));
        }
        if self.days != 0 || (self.months == 0 && self.micros == 0) {
            let sign = if self.days < 0 {
                "-"
            } else if explicit_positive {
                "+"
            } else {
                ""
            };
            parts.push(format!("{sign}{}", self.days.unsigned_abs()));
        }
        if self.micros != 0 || self.days != 0 {
            let force_positive = explicit_positive && self.micros >= 0;
            parts.push(format_interval_clock(self.micros, force_positive, false));
        }
        parts.join(" ")
    }

    pub fn to_iso_8601_text(self) -> String {
        if self == Self::ZERO {
            return "PT0S".to_string();
        }
        let mut output = String::from("P");
        let years = self.months / 12;
        let months = self.months % 12;
        if years != 0 {
            output.push_str(&format!("{years}Y"));
        }
        if months != 0 {
            output.push_str(&format!("{months}M"));
        }
        if self.days != 0 {
            output.push_str(&format!("{}D", self.days));
        }
        if self.micros != 0 {
            output.push('T');
            let sign = if self.micros < 0 { -1 } else { 1 };
            let magnitude = i128::from(self.micros).abs();
            let hours = i64::try_from(magnitude / 3_600_000_000).unwrap_or(i64::MAX) * sign;
            let minutes =
                i64::try_from(magnitude % 3_600_000_000 / 60_000_000).unwrap_or(i64::MAX) * sign;
            let seconds = magnitude % 60_000_000;
            if hours != 0 {
                output.push_str(&format!("{hours}H"));
            }
            if minutes != 0 {
                output.push_str(&format!("{minutes}M"));
            }
            if seconds != 0 {
                let mut value = format_interval_decimal_micros(seconds);
                if sign < 0 {
                    value.insert(0, '-');
                }
                output.push_str(&format!("{value}S"));
            }
        }
        output
    }

    pub fn to_style_text(self, style: &str) -> String {
        match style.to_ascii_lowercase().as_str() {
            "postgres_verbose" => self.to_postgres_verbose_text(),
            "sql_standard" => self.to_sql_standard_text(),
            "iso_8601" => self.to_iso_8601_text(),
            _ => self.to_postgres_text(),
        }
    }

    pub const ZERO: Self = Self {
        months: 0,
        days: 0,
        micros: 0,
    };

    pub fn with_typmod(
        self,
        fields: Option<&str>,
        precision: Option<u8>,
    ) -> Result<Self, PgCanonicalValueError> {
        let mut value = self;
        if let Some(precision) = precision {
            if precision > 6 {
                return Err(PgCanonicalValueError::TemporalFieldOverflow("interval"));
            }
            let quantum = 10_i64.pow(u32::from(6 - precision));
            let half = quantum / 2;
            value.micros = if value.micros >= 0 {
                value.micros.checked_add(half)
            } else {
                value.micros.checked_sub(half)
            }
            .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?
                / quantum
                * quantum;
        }
        let end = fields
            .map(|fields| fields.rsplit_once(" TO ").map_or(fields, |(_, end)| end))
            .unwrap_or("SECOND");
        match end {
            "YEAR" => {
                value.months = value.months / 12 * 12;
                value.days = 0;
                value.micros = 0;
            }
            "MONTH" => {
                value.days = 0;
                value.micros = 0;
            }
            "DAY" => value.micros = 0,
            "HOUR" => value.micros = value.micros / 3_600_000_000 * 3_600_000_000,
            "MINUTE" => value.micros = value.micros / 60_000_000 * 60_000_000,
            "SECOND" => {}
            _ => return Err(invalid_temporal("interval", fields.unwrap_or_default())),
        }
        Ok(value)
    }

    pub fn checked_neg(self) -> Result<Self, PgCanonicalValueError> {
        Ok(Self {
            months: self
                .months
                .checked_neg()
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?,
            days: self
                .days
                .checked_neg()
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?,
            micros: self
                .micros
                .checked_neg()
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?,
        })
    }

    pub fn checked_add(self, other: Self) -> Result<Self, PgCanonicalValueError> {
        Ok(Self {
            months: self
                .months
                .checked_add(other.months)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?,
            days: self
                .days
                .checked_add(other.days)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?,
            micros: self
                .micros
                .checked_add(other.micros)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?,
        })
    }

    pub fn checked_sub(self, other: Self) -> Result<Self, PgCanonicalValueError> {
        self.checked_add(other.checked_neg()?)
    }

    pub fn checked_scale(self, factor: f64) -> Result<Self, PgCanonicalValueError> {
        if !factor.is_finite() {
            return Err(PgCanonicalValueError::TemporalOverflow("interval"));
        }
        let months_exact = f64::from(self.months) * factor;
        let months = months_exact.trunc();
        let month_remainder_days = (months_exact - months) * 30.0;
        let days_exact = f64::from(self.days) * factor + month_remainder_days;
        let days = days_exact.trunc();
        let day_remainder_micros = (days_exact - days) * MICROS_PER_DAY as f64;
        let micros = (self.micros as f64 * factor + day_remainder_micros).round_ties_even();
        Ok(Self {
            months: f64_to_i32_interval(months)?,
            days: f64_to_i32_interval(days)?,
            micros: f64_to_i64_interval(micros)?,
        })
    }

    pub fn checked_div(self, divisor: f64) -> Result<Self, PgCanonicalValueError> {
        if divisor == 0.0 || !divisor.is_finite() {
            return Err(PgCanonicalValueError::TemporalOverflow("interval"));
        }
        self.checked_scale(1.0 / divisor)
    }

    pub fn justify_hours(self) -> Result<Self, PgCanonicalValueError> {
        let extra_days = self.micros / MICROS_PER_DAY;
        Ok(Self {
            months: self.months,
            days: self
                .days
                .checked_add(
                    i32::try_from(extra_days)
                        .map_err(|_| PgCanonicalValueError::TemporalOverflow("interval"))?,
                )
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?,
            micros: self.micros % MICROS_PER_DAY,
        })
    }

    pub fn justify_days(self) -> Result<Self, PgCanonicalValueError> {
        let extra_months = self.days / 30;
        Ok(Self {
            months: self
                .months
                .checked_add(extra_months)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?,
            days: self.days % 30,
            micros: self.micros,
        })
    }

    pub fn justify_interval(self) -> Result<Self, PgCanonicalValueError> {
        let mut value = self.justify_hours()?.justify_days()?;
        if value.months > 0 && (value.days < 0 || (value.days == 0 && value.micros < 0)) {
            value.months -= 1;
            value.days += 30;
        } else if value.months < 0 && (value.days > 0 || (value.days == 0 && value.micros > 0)) {
            value.months += 1;
            value.days -= 30;
        }
        if value.days > 0 && value.micros < 0 {
            value.days -= 1;
            value.micros += MICROS_PER_DAY;
        } else if value.days < 0 && value.micros > 0 {
            value.days += 1;
            value.micros -= MICROS_PER_DAY;
        }
        Ok(value)
    }

    pub fn comparison_micros(self) -> i128 {
        i128::from(self.months) * 30 * i128::from(MICROS_PER_DAY)
            + i128::from(self.days) * i128::from(MICROS_PER_DAY)
            + i128::from(self.micros)
    }

    pub fn extract_field(self, field: &str) -> Option<String> {
        let value = match field.to_ascii_lowercase().as_str() {
            "millennium" | "millennia" => (self.months / 12 / 1_000).to_string(),
            "century" | "centuries" => (self.months / 12 / 100).to_string(),
            "decade" | "decades" => (self.months / 12 / 10).to_string(),
            "year" | "years" => (self.months / 12).to_string(),
            "month" | "months" => (self.months % 12).to_string(),
            "day" | "days" => self.days.to_string(),
            "hour" | "hours" => (self.micros / 3_600_000_000).to_string(),
            "minute" | "minutes" => (self.micros % 3_600_000_000 / 60_000_000).to_string(),
            "second" | "seconds" => {
                format_signed_decimal_micros_fixed_six(i128::from(self.micros % 60_000_000))
            }
            "epoch" => format_signed_decimal_micros_fixed_six(self.comparison_micros()),
            _ => return None,
        };
        Some(value)
    }
}

fn interval_decimal_ratio(value: &str) -> Result<(BigInt, BigInt), PgCanonicalValueError> {
    let PgNumeric::Finite {
        negative,
        coefficient,
        display_scale,
    } = PgNumeric::from_decimal_text(value)?
    else {
        return Err(invalid_temporal("interval", value));
    };
    let mut numerator = BigInt::parse_bytes(coefficient.as_bytes(), 10)
        .ok_or(PgCanonicalValueError::NumericOverflow)?;
    let denominator = if display_scale > 0 {
        BigInt::from(10_u8).pow(display_scale as u32)
    } else {
        if display_scale < 0 {
            numerator *= BigInt::from(10_u8).pow(display_scale.unsigned_abs());
        }
        BigInt::from(1_u8)
    };
    if negative {
        numerator = -numerator;
    }
    Ok((numerator, denominator))
}

fn rounded_interval_ratio(
    numerator: BigInt,
    denominator: &BigInt,
) -> Result<i64, PgCanonicalValueError> {
    let quotient = &numerator / denominator;
    let remainder = &numerator % denominator;
    let magnitude = if remainder < BigInt::from(0_u8) {
        -&remainder
    } else {
        remainder.clone()
    };
    let doubled = &magnitude * 2;
    let odd = (&quotient % BigInt::from(2_u8)) != BigInt::from(0_u8);
    let round = doubled > *denominator || (doubled == *denominator && odd);
    let rounded = if round {
        quotient
            + if numerator < BigInt::from(0_u8) {
                BigInt::from(-1_i8)
            } else {
                BigInt::from(1_u8)
            }
    } else {
        quotient
    };
    rounded
        .to_i64()
        .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))
}

fn parse_interval_decimal_scaled(value: &str, scale: i64) -> Result<i64, PgCanonicalValueError> {
    let (numerator, denominator) = interval_decimal_ratio(value)?;
    rounded_interval_ratio(numerator * BigInt::from(scale), &denominator)
}

fn add_interval_unit(
    amount: &str,
    unit: &str,
    months: &mut i64,
    days: &mut i64,
    micros: &mut i64,
    original: &str,
) -> Result<(), PgCanonicalValueError> {
    let unit = unit.trim_end_matches('s');
    match unit {
        "millennium" | "millennia" | "millenniu" => {
            *months = checked_add_scaled(
                *months,
                parse_interval_decimal_scaled(amount, 12_000)?,
                1,
                "interval",
            )?;
        }
        "century" | "centurie" => {
            *months = checked_add_scaled(
                *months,
                parse_interval_decimal_scaled(amount, 1_200)?,
                1,
                "interval",
            )?;
        }
        "decade" => {
            *months = checked_add_scaled(
                *months,
                parse_interval_decimal_scaled(amount, 120)?,
                1,
                "interval",
            )?;
        }
        "year" | "yr" => {
            *months = checked_add_scaled(
                *months,
                parse_interval_decimal_scaled(amount, 12)?,
                1,
                "interval",
            )?;
        }
        "mon" | "month" => {
            let (numerator, denominator) = interval_decimal_ratio(amount)?;
            let whole = (&numerator / &denominator)
                .to_i64()
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
            *months = months
                .checked_add(whole)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
            let remainder_days = (numerator % &denominator) * BigInt::from(30_i8);
            let whole_days = (&remainder_days / &denominator)
                .to_i64()
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
            *days = days
                .checked_add(whole_days)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
            let day_remainder = remainder_days % &denominator;
            *micros = micros
                .checked_add(rounded_interval_ratio(
                    day_remainder * BigInt::from(MICROS_PER_DAY),
                    &denominator,
                )?)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
        }
        "week" => add_interval_day_fraction(amount, 7, days, micros)?,
        "day" => add_interval_day_fraction(amount, 1, days, micros)?,
        "hour" | "hr" => {
            *micros = micros
                .checked_add(parse_interval_decimal_scaled(amount, 3_600_000_000)?)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
        }
        "minute" | "min" => {
            *micros = micros
                .checked_add(parse_interval_decimal_scaled(amount, 60_000_000)?)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
        }
        "second" | "sec" => {
            *micros = micros
                .checked_add(parse_interval_decimal_scaled(amount, 1_000_000)?)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
        }
        "millisecond" | "msec" => {
            *micros = micros
                .checked_add(parse_interval_decimal_scaled(amount, 1_000)?)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
        }
        "microsecond" | "usec" => {
            *micros = micros
                .checked_add(parse_interval_decimal_scaled(amount, 1)?)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
        }
        _ => return Err(invalid_temporal("interval", original)),
    }
    Ok(())
}

fn add_interval_day_fraction(
    amount: &str,
    multiplier: i64,
    days: &mut i64,
    micros: &mut i64,
) -> Result<(), PgCanonicalValueError> {
    let (numerator, denominator) = interval_decimal_ratio(amount)?;
    let total = rounded_interval_ratio(
        numerator * BigInt::from(multiplier) * BigInt::from(MICROS_PER_DAY),
        &denominator,
    )?;
    *days = days
        .checked_add(total / MICROS_PER_DAY)
        .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
    *micros = micros
        .checked_add(total % MICROS_PER_DAY)
        .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
    Ok(())
}

fn parse_interval_clock_micros(value: &str) -> Result<i64, PgCanonicalValueError> {
    let (sign, clock) = if let Some(value) = value.strip_prefix('-') {
        (-1_i64, value)
    } else if let Some(value) = value.strip_prefix('+') {
        (1_i64, value)
    } else {
        (1_i64, value)
    };
    let parts = clock.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) {
        return Err(invalid_temporal("interval", value));
    }
    let hours = parts[0]
        .parse::<i64>()
        .map_err(|_| invalid_temporal("interval", value))?;
    let minutes = parts[1]
        .parse::<i64>()
        .map_err(|_| invalid_temporal("interval", value))?;
    if minutes > 59 {
        return Err(PgCanonicalValueError::TemporalFieldOverflow("interval"));
    }
    let seconds = parts.get(2).copied().unwrap_or("0");
    let micros = parse_interval_decimal_scaled(seconds, 1_000_000)?;
    if !(0..60_000_000).contains(&micros) {
        return Err(PgCanonicalValueError::TemporalFieldOverflow("interval"));
    }
    hours
        .checked_mul(3_600_000_000)
        .and_then(|value| value.checked_add(minutes * 60_000_000))
        .and_then(|value| value.checked_add(micros))
        .and_then(|value| value.checked_mul(sign))
        .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))
}

fn parse_sql_standard_interval(value: &str) -> Result<PgInterval, PgCanonicalValueError> {
    let parts = value.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 3 {
        return Err(invalid_temporal("interval", value));
    }
    let part_count = parts.len();
    let mut interval = PgInterval::ZERO;
    for part in parts {
        if part.contains(':') {
            interval.micros = interval
                .micros
                .checked_add(parse_interval_clock_micros(part)?)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
        } else if let Some(months) = parse_sql_year_month(part)? {
            interval.months = interval
                .months
                .checked_add(months)
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
        } else if part_count == 1 {
            interval.micros = parse_interval_decimal_scaled(part, 1_000_000)?;
        } else {
            interval.days = interval
                .days
                .checked_add(
                    part.parse::<i32>()
                        .map_err(|_| invalid_temporal("interval", value))?,
                )
                .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
        }
    }
    Ok(interval)
}

fn parse_sql_year_month(value: &str) -> Result<Option<i32>, PgCanonicalValueError> {
    let (sign, value) = if let Some(value) = value.strip_prefix('-') {
        (-1_i64, value)
    } else if let Some(value) = value.strip_prefix('+') {
        (1_i64, value)
    } else {
        (1_i64, value)
    };
    let Some((years, months)) = value.split_once('-') else {
        return Ok(None);
    };
    if years.is_empty() || months.is_empty() || months.parse::<u32>().ok().is_none_or(|m| m > 11) {
        return Err(invalid_temporal("interval", value));
    }
    let years = years
        .parse::<i64>()
        .map_err(|_| invalid_temporal("interval", value))?;
    let months = months
        .parse::<i64>()
        .map_err(|_| invalid_temporal("interval", value))?;
    let total = years
        .checked_mul(12)
        .and_then(|value| value.checked_add(months))
        .and_then(|value| value.checked_mul(sign))
        .ok_or(PgCanonicalValueError::TemporalOverflow("interval"))?;
    i32::try_from(total)
        .map(Some)
        .map_err(|_| PgCanonicalValueError::TemporalOverflow("interval"))
}

fn parse_iso_interval(value: &str) -> Result<PgInterval, PgCanonicalValueError> {
    let (global_sign, value) = if let Some(value) = value.strip_prefix('-') {
        (-1_i64, value)
    } else if let Some(value) = value.strip_prefix('+') {
        (1_i64, value)
    } else {
        (1_i64, value)
    };
    let value = value
        .strip_prefix(['P', 'p'])
        .ok_or_else(|| invalid_temporal("interval", value))?;
    let mut months = 0_i64;
    let mut days = 0_i64;
    let mut micros = 0_i64;
    let mut in_time = false;
    let mut start = 0;
    let bytes = value.as_bytes();
    let mut saw_component = false;
    for index in 0..bytes.len() {
        let byte = bytes[index];
        if matches!(byte, b'T' | b't') {
            if start != index {
                return Err(invalid_temporal("interval", value));
            }
            in_time = true;
            start = index + 1;
            continue;
        }
        if !byte.is_ascii_alphabetic() {
            continue;
        }
        let amount = value
            .get(start..index)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_temporal("interval", value))?;
        let unit = match (byte.to_ascii_uppercase(), in_time) {
            (b'Y', false) => "year",
            (b'M', false) => "month",
            (b'W', false) => "week",
            (b'D', false) => "day",
            (b'H', true) => "hour",
            (b'M', true) => "minute",
            (b'S', true) => "second",
            _ => return Err(invalid_temporal("interval", value)),
        };
        add_interval_unit(amount, unit, &mut months, &mut days, &mut micros, value)?;
        saw_component = true;
        start = index + 1;
    }
    if !saw_component || start != value.len() {
        return Err(invalid_temporal("interval", value));
    }
    let mut interval = PgInterval {
        months: i32::try_from(months)
            .map_err(|_| PgCanonicalValueError::TemporalOverflow("interval"))?,
        days: i32::try_from(days)
            .map_err(|_| PgCanonicalValueError::TemporalOverflow("interval"))?,
        micros,
    };
    if global_sign < 0 {
        interval = interval.checked_neg()?;
    }
    Ok(interval)
}

fn format_interval_clock(micros: i64, force_positive: bool, pad_hours: bool) -> String {
    let sign = if micros < 0 {
        "-"
    } else if force_positive {
        "+"
    } else {
        ""
    };
    let magnitude = i128::from(micros).abs();
    let hours = magnitude / 3_600_000_000;
    let minutes = magnitude % 3_600_000_000 / 60_000_000;
    let seconds = magnitude % 60_000_000 / 1_000_000;
    let fraction = magnitude % 1_000_000;
    let hour = if pad_hours {
        format!("{hours:02}")
    } else {
        hours.to_string()
    };
    let mut clock = format!("{sign}{hour}:{minutes:02}:{seconds:02}");
    if fraction != 0 {
        let fraction = format!("{fraction:06}");
        clock.push('.');
        clock.push_str(fraction.trim_end_matches('0'));
    }
    clock
}

fn format_interval_decimal_micros(micros: i128) -> String {
    let seconds = micros / 1_000_000;
    let fraction = micros % 1_000_000;
    if fraction == 0 {
        seconds.to_string()
    } else {
        format!(
            "{seconds}.{}",
            format!("{fraction:06}").trim_end_matches('0')
        )
    }
}

fn format_signed_decimal_micros_fixed_six(micros: i128) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let magnitude = micros.abs();
    format!(
        "{sign}{}.{:06}",
        magnitude / 1_000_000,
        magnitude % 1_000_000
    )
}

fn push_verbose_interval_part(parts: &mut Vec<String>, value: i64, one: &str, many: &str) {
    if value != 0 {
        parts.push(format!("{value} {}", if value == 1 { one } else { many }));
    }
}

fn f64_to_i32_interval(value: f64) -> Result<i32, PgCanonicalValueError> {
    if value < f64::from(i32::MIN) || value > f64::from(i32::MAX) {
        return Err(PgCanonicalValueError::TemporalOverflow("interval"));
    }
    Ok(value as i32)
}

fn f64_to_i64_interval(value: f64) -> Result<i64, PgCanonicalValueError> {
    if value < i64::MIN as f64 || value > i64::MAX as f64 {
        return Err(PgCanonicalValueError::TemporalOverflow("interval"));
    }
    Ok(value as i64)
}

fn format_clock_micros(micros: i64) -> String {
    let hours = micros / 3_600_000_000;
    let minutes = micros % 3_600_000_000 / 60_000_000;
    let seconds = micros % 60_000_000 / 1_000_000;
    let fraction = micros % 1_000_000;
    let mut value = format!("{hours:02}:{minutes:02}:{seconds:02}");
    if fraction != 0 {
        let fraction = format!("{fraction:06}");
        value.push('.');
        value.push_str(fraction.trim_end_matches('0'));
    }
    value
}

fn invalid_temporal(kind: &'static str, value: impl Into<String>) -> PgCanonicalValueError {
    PgCanonicalValueError::InvalidTemporal {
        kind,
        value: value.into(),
    }
}

fn split_date_era(value: &str) -> Result<(&str, bool), PgCanonicalValueError> {
    let value = value.trim();
    let upper = value.to_ascii_uppercase();
    for (suffix, bc) in [(" BC", true), (" AD", false)] {
        if upper.ends_with(suffix) {
            let date = value[..value.len() - suffix.len()].trim_end();
            if date.is_empty() {
                return Err(invalid_temporal("date", value));
            }
            return Ok((date, bc));
        }
    }
    Ok((value, false))
}

fn parse_postgres_date(
    value: &str,
    kind: &'static str,
) -> Result<(i32, u32, u32), PgCanonicalValueError> {
    if value.bytes().any(|byte| byte.is_ascii_alphabetic()) {
        return parse_month_name_date(value, kind);
    }
    if value.contains('/') {
        let parts = value.split('/').collect::<Vec<_>>();
        if parts.len() != 3 {
            return Err(invalid_temporal(kind, value));
        }
        let month = parse_date_u32(parts[0], kind, value)?;
        let day = parse_date_u32(parts[1], kind, value)?;
        let year = parse_date_year(parts[2], kind, value)?;
        validate_calendar_date(year, month, day, kind)?;
        return Ok((year, month, day));
    }
    if value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit()) {
        let year = parse_date_year(&value[..4], kind, value)?;
        let month = parse_date_u32(&value[4..6], kind, value)?;
        let day = parse_date_u32(&value[6..], kind, value)?;
        validate_calendar_date(year, month, day, kind)?;
        return Ok((year, month, day));
    }
    let parts = value.split('-').collect::<Vec<_>>();
    if parts.len() == 2 {
        let year = parse_date_year(parts[0], kind, value)?;
        let ordinal = parse_date_u32(parts[1], kind, value)?;
        let max_ordinal = if is_gregorian_leap_year(year) {
            366
        } else {
            365
        };
        if ordinal == 0 || ordinal > max_ordinal {
            return Err(PgCanonicalValueError::TemporalFieldOverflow(kind));
        }
        let days = days_from_civil(year, 1, 1) + i64::from(ordinal - 1);
        return Ok(civil_from_days(days));
    }
    parse_iso_date(value, kind)
}

fn parse_month_name_date(
    value: &str,
    kind: &'static str,
) -> Result<(i32, u32, u32), PgCanonicalValueError> {
    let normalized = value.replace(',', " ");
    let parts = normalized.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(invalid_temporal(kind, value));
    }
    let first_month = postgres_month_number(parts[0]);
    let second_month = postgres_month_number(parts[1]);
    let (year, month, day) = match (first_month, second_month) {
        (Some(month), None) => (
            parse_date_year(parts[2], kind, value)?,
            month,
            parse_date_u32(parts[1], kind, value)?,
        ),
        (None, Some(month)) => (
            parse_date_year(parts[2], kind, value)?,
            month,
            parse_date_u32(parts[0], kind, value)?,
        ),
        _ => return Err(invalid_temporal(kind, value)),
    };
    validate_calendar_date(year, month, day, kind)?;
    Ok((year, month, day))
}

fn postgres_month_number(value: &str) -> Option<u32> {
    let lower = value.to_ascii_lowercase();
    let prefix = lower.get(..lower.len().min(3))?;
    [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ]
    .iter()
    .position(|month| *month == prefix)
    .map(|month| month as u32 + 1)
}

fn parse_date_year(
    value: &str,
    kind: &'static str,
    original: &str,
) -> Result<i32, PgCanonicalValueError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_temporal(kind, original));
    }
    value
        .parse::<i32>()
        .map_err(|_| PgCanonicalValueError::TemporalOverflow(kind))
}

fn parse_date_u32(
    value: &str,
    kind: &'static str,
    original: &str,
) -> Result<u32, PgCanonicalValueError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_temporal(kind, original));
    }
    value
        .parse::<u32>()
        .map_err(|_| PgCanonicalValueError::TemporalFieldOverflow(kind))
}

fn is_gregorian_leap_year(year: i32) -> bool {
    year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0)
}

fn validate_calendar_date(
    year: i32,
    month: u32,
    day: u32,
    kind: &'static str,
) -> Result<(), PgCanonicalValueError> {
    if !(1..=12).contains(&month) {
        return Err(PgCanonicalValueError::TemporalFieldOverflow(kind));
    }
    let max_day = match month {
        2 if is_gregorian_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day == 0 || day > max_day {
        return Err(PgCanonicalValueError::TemporalFieldOverflow(kind));
    }
    Ok(())
}

fn parse_iso_date(
    value: &str,
    kind: &'static str,
) -> Result<(i32, u32, u32), PgCanonicalValueError> {
    let mut parts = value.split('-');
    let year = parts
        .next()
        .and_then(|part| part.parse::<i32>().ok())
        .ok_or_else(|| invalid_temporal(kind, value))?;
    let month = parts
        .next()
        .and_then(|part| part.parse::<u32>().ok())
        .ok_or_else(|| invalid_temporal(kind, value))?;
    let day = parts
        .next()
        .and_then(|part| part.parse::<u32>().ok())
        .ok_or_else(|| invalid_temporal(kind, value))?;
    if parts.next().is_some() {
        return Err(invalid_temporal(kind, value));
    }
    validate_calendar_date(year, month, day, kind)?;
    Ok((year, month, day))
}

fn split_clock_offset<'a>(
    value: &'a str,
    kind: &'static str,
) -> Result<(&'a str, Option<i32>), PgCanonicalValueError> {
    let offset_index = value
        .char_indices()
        .find(|(index, ch)| *index > 0 && matches!(ch, '+' | '-' | 'Z' | 'z'))
        .map(|(index, _)| index);
    let Some(index) = offset_index else {
        return Ok((value, None));
    };
    let (clock, offset) = value.split_at(index);
    Ok((clock, Some(parse_timezone_offset(offset, kind)?)))
}

fn parse_timezone_offset(value: &str, kind: &'static str) -> Result<i32, PgCanonicalValueError> {
    if matches!(value, "Z" | "z") {
        return Ok(0);
    }
    let sign = match value.as_bytes().first() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return Err(invalid_temporal(kind, value)),
    };
    let body = &value[1..];
    let components = if body.contains(':') {
        body.split(':').collect::<Vec<_>>()
    } else {
        match body.len() {
            1 | 2 => vec![body],
            4 => vec![&body[..2], &body[2..]],
            6 => vec![&body[..2], &body[2..4], &body[4..]],
            _ => return Err(invalid_temporal(kind, value)),
        }
    };
    if components.is_empty() || components.len() > 3 {
        return Err(invalid_temporal(kind, value));
    }
    let hour = components[0]
        .parse::<i32>()
        .map_err(|_| invalid_temporal(kind, value))?;
    let minute = components
        .get(1)
        .unwrap_or(&"0")
        .parse::<i32>()
        .map_err(|_| invalid_temporal(kind, value))?;
    let second = components
        .get(2)
        .unwrap_or(&"0")
        .parse::<i32>()
        .map_err(|_| invalid_temporal(kind, value))?;
    if minute > 59 || second > 59 {
        return Err(invalid_temporal(kind, value));
    }
    let total = sign * (hour * 3_600 + minute * 60 + second);
    if total.unsigned_abs() > MAX_TIMEZONE_OFFSET_SECONDS as u32 {
        return Err(PgCanonicalValueError::InvalidTimezoneOffset(total));
    }
    Ok(total)
}

fn parse_clock_micros(value: &str, kind: &'static str) -> Result<i64, PgCanonicalValueError> {
    let mut parts = value.split(':');
    let hour = parts.next().and_then(|part| part.parse::<i64>().ok());
    let minute = parts.next().and_then(|part| part.parse::<i64>().ok());
    let second_part = parts.next().unwrap_or("0");
    if parts.next().is_some() {
        return Err(invalid_temporal(kind, value));
    }
    let (second_text, fraction) = second_part.split_once('.').unwrap_or((second_part, ""));
    let second = second_text.parse::<i64>().ok();
    let (Some(hour), Some(minute), Some(second)) = (hour, minute, second) else {
        return Err(invalid_temporal(kind, value));
    };
    if hour < 0 || hour > 24 || minute < 0 || minute > 59 || second < 0 || second > 59 {
        return Err(PgCanonicalValueError::TemporalFieldOverflow(kind));
    }
    let fraction_micros = parse_fraction_micros(fraction, kind, value)?;
    if hour == 24 && (minute != 0 || second != 0 || fraction_micros != 0) {
        return Err(PgCanonicalValueError::TemporalFieldOverflow(kind));
    }
    (hour * 3_600_000_000)
        .checked_add(minute * 60_000_000)
        .and_then(|total| total.checked_add(second * 1_000_000))
        .and_then(|total| total.checked_add(fraction_micros))
        .ok_or(PgCanonicalValueError::TemporalOverflow(kind))
}

fn parse_fraction_micros(
    fraction: &str,
    kind: &'static str,
    original: &str,
) -> Result<i64, PgCanonicalValueError> {
    if fraction.is_empty() {
        return Ok(0);
    }
    if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_temporal(kind, original));
    }
    let retained = &fraction[..fraction.len().min(6)];
    let mut micros = retained
        .parse::<i64>()
        .map_err(|_| invalid_temporal(kind, original))?
        * 10_i64.pow(6 - retained.len() as u32);
    let discarded = fraction.as_bytes().get(6..).unwrap_or_default();
    let round_up = discarded.first().is_some_and(|digit| {
        *digit > b'5'
            || (*digit == b'5'
                && (discarded[1..].iter().any(|digit| *digit != b'0') || micros % 2 != 0))
    });
    if round_up {
        micros += 1;
    }
    Ok(micros)
}

fn checked_add_scaled(
    total: i64,
    amount: i64,
    scale: i64,
    kind: &'static str,
) -> Result<i64, PgCanonicalValueError> {
    total
        .checked_add(
            amount
                .checked_mul(scale)
                .ok_or(PgCanonicalValueError::TemporalOverflow(kind))?,
        )
        .ok_or(PgCanonicalValueError::TemporalOverflow(kind))
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = (if year >= 0 { year } else { year - 399 }) / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) as i64
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = (if days >= 0 { days } else { days - 146_096 }) / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "family", content = "octets", rename_all = "snake_case")]
pub enum PgIpAddress {
    V4([u8; 4]),
    V6([u8; 16]),
}

impl PgIpAddress {
    pub fn bit_len(self) -> u8 {
        match self {
            Self::V4(_) => 32,
            Self::V6(_) => 128,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PgNetworkKind {
    Inet,
    Cidr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PgNetwork {
    pub kind: PgNetworkKind,
    pub address: PgIpAddress,
    pub prefix: u8,
}

impl PgNetwork {
    pub fn new(
        kind: PgNetworkKind,
        address: PgIpAddress,
        prefix: u8,
    ) -> Result<Self, PgCanonicalValueError> {
        let address_bits = address.bit_len();
        if prefix > address_bits {
            return Err(PgCanonicalValueError::InvalidNetworkPrefix {
                prefix,
                address_bits,
            });
        }
        Ok(Self {
            kind,
            address,
            prefix,
        })
    }

    pub fn from_postgres_text(
        value: &str,
        kind: PgNetworkKind,
    ) -> Result<Self, PgCanonicalValueError> {
        let pg_type = match kind {
            PgNetworkKind::Inet => "inet",
            PgNetworkKind::Cidr => "cidr",
        };
        if value.is_empty() || value.trim() != value {
            return Err(invalid_special(pg_type, value));
        }
        let (address, prefix) = value
            .split_once('/')
            .map(|(address, prefix)| (address, Some(prefix)))
            .unwrap_or((value, None));
        if address.is_empty() || prefix.is_some_and(|prefix| prefix.is_empty()) {
            return Err(invalid_special(pg_type, value));
        }
        let (address, default_prefix) = if address.contains(':') {
            (
                PgIpAddress::V6(
                    address
                        .parse::<Ipv6Addr>()
                        .map_err(|_| invalid_special(pg_type, value))?
                        .octets(),
                ),
                128,
            )
        } else {
            let (address, default_prefix) =
                parse_postgres_ipv4(address, pg_type, value, kind == PgNetworkKind::Cidr)?;
            (PgIpAddress::V4(address), default_prefix)
        };
        let prefix = match prefix {
            Some(prefix)
                if prefix.bytes().all(|byte| byte.is_ascii_digit())
                    && !(matches!(address, PgIpAddress::V6(_))
                        && prefix.len() > 1
                        && prefix.starts_with('0')) =>
            {
                prefix
                    .parse::<u16>()
                    .ok()
                    .and_then(|prefix| u8::try_from(prefix).ok())
                    .filter(|prefix| *prefix <= address.bit_len())
                    .ok_or_else(|| invalid_special(pg_type, value))?
            }
            Some(_) => return Err(invalid_special(pg_type, value)),
            None => default_prefix,
        };
        let network =
            Self::new(kind, address, prefix).map_err(|_| invalid_special(pg_type, value))?;
        if kind == PgNetworkKind::Cidr && network_has_host_bits(network) {
            return Err(invalid_special("cidr", value));
        }
        Ok(network)
    }

    pub fn to_postgres_text(self) -> String {
        let address = match self.address {
            PgIpAddress::V4(octets) => std::net::Ipv4Addr::from(octets).to_string(),
            PgIpAddress::V6(octets)
                if octets[..10] == [0; 10] && octets[10..12] == [0xff, 0xff] =>
            {
                format!(
                    "::ffff:{}",
                    std::net::Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15])
                )
            }
            PgIpAddress::V6(octets)
                if octets[..12] == [0; 12]
                    && u32::from_be_bytes(octets[12..16].try_into().expect("IPv4 tail"))
                        > u32::from(u16::MAX) =>
            {
                format!(
                    "::{}",
                    std::net::Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15])
                )
            }
            PgIpAddress::V6(octets) => Ipv6Addr::from(octets).to_string(),
        };
        format!("{address}/{}", self.prefix)
    }

    pub fn to_postgres_output_text(self) -> String {
        let mut text = self.to_postgres_text();
        if self.kind == PgNetworkKind::Inet && self.prefix == self.address.bit_len() {
            text.truncate(text.rfind('/').expect("canonical network includes a mask"));
        }
        text
    }
}

fn parse_postgres_ipv4(
    address: &str,
    pg_type: &str,
    original: &str,
    allow_shorthand: bool,
) -> Result<([u8; 4], u8), PgCanonicalValueError> {
    let octets = address.split('.').collect::<Vec<_>>();
    if octets.is_empty() || octets.len() > 4 || (!allow_shorthand && octets.len() != 4) {
        return Err(invalid_special(pg_type, original));
    }
    let mut parsed = [0_u8; 4];
    let specified = octets.len();
    for (index, octet) in octets.into_iter().enumerate() {
        if octet.is_empty() || !octet.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid_special(pg_type, original));
        }
        let significant = octet.trim_start_matches('0');
        let significant = if significant.is_empty() {
            "0"
        } else {
            significant
        };
        parsed[index] = significant
            .parse::<u16>()
            .ok()
            .and_then(|value| u8::try_from(value).ok())
            .ok_or_else(|| invalid_special(pg_type, original))?;
    }
    Ok((
        parsed,
        u8::try_from(specified * 8).expect("IPv4 component prefix fits in u8"),
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "width", content = "octets", rename_all = "snake_case")]
pub enum PgMacAddress {
    Mac48([u8; 6]),
    Mac64([u8; 8]),
}

impl PgMacAddress {
    pub fn from_postgres_text(value: &str, extended: bool) -> Result<Self, PgCanonicalValueError> {
        if extended {
            let bytes = parse_postgres_macaddr8(value)?;
            Ok(Self::Mac64(bytes))
        } else {
            let bytes = parse_postgres_macaddr(value)?;
            Ok(Self::Mac48(bytes))
        }
    }

    pub fn to_postgres_text(self) -> String {
        let octets: &[u8] = match &self {
            Self::Mac48(octets) => octets,
            Self::Mac64(octets) => octets,
        };
        octets
            .iter()
            .map(|octet| format!("{octet:02x}"))
            .collect::<Vec<_>>()
            .join(":")
    }
}

fn parse_postgres_macaddr(value: &str) -> Result<[u8; 6], PgCanonicalValueError> {
    let input = value.trim();
    for separator in [':', '-'] {
        let groups = input.split(separator).collect::<Vec<_>>();
        if groups.len() == 6
            && groups.iter().all(|group| {
                !group.is_empty() && group.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            let mut address = [0_u8; 6];
            for (index, group) in groups.into_iter().enumerate() {
                let octet = u64::from_str_radix(group, 16)
                    .map_err(|_| PgCanonicalValueError::MacAddressOctetOutOfRange)?;
                address[index] = u8::try_from(octet)
                    .map_err(|_| PgCanonicalValueError::MacAddressOctetOutOfRange)?;
            }
            return Ok(address);
        }
    }
    for (separator, fields) in [
        (Some(':'), &[3_usize, 3_usize][..]),
        (Some('-'), &[3, 3][..]),
        (Some('.'), &[2, 2, 2][..]),
        (Some('-'), &[2, 2, 2][..]),
        (None, &[6][..]),
    ] {
        if let Some(address) = parse_fixed_width_macaddr(input, separator, fields) {
            return Ok(address);
        }
    }
    Err(invalid_special("macaddr", value))
}

fn parse_fixed_width_macaddr(
    input: &str,
    separator: Option<char>,
    fields: &[usize],
) -> Option<[u8; 6]> {
    let groups = match separator {
        Some(separator) => input.split(separator).collect::<Vec<_>>(),
        None => vec![input],
    };
    if groups.len() != fields.len() {
        return None;
    }
    let mut address = Vec::with_capacity(6);
    for (group, field_count) in groups.into_iter().zip(fields) {
        let mut offset = 0;
        for _ in 0..*field_count {
            let end = (offset + 2).min(group.len());
            if end == offset
                || !group[offset..end]
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                return None;
            }
            address.push(u8::from_str_radix(&group[offset..end], 16).ok()?);
            offset = end;
        }
        if offset != group.len() {
            return None;
        }
    }
    address.try_into().ok()
}

fn parse_postgres_macaddr8(value: &str) -> Result<[u8; 8], PgCanonicalValueError> {
    let input = value.trim();
    let bytes = input.as_bytes();
    let mut address = Vec::with_capacity(8);
    let mut separator = None;
    let mut offset = 0;
    while offset + 1 < bytes.len() {
        if !bytes[offset].is_ascii_hexdigit() || !bytes[offset + 1].is_ascii_hexdigit() {
            return Err(invalid_special("macaddr8", value));
        }
        address.push(
            u8::from_str_radix(&input[offset..offset + 2], 16)
                .map_err(|_| invalid_special("macaddr8", value))?,
        );
        if address.len() > 8 {
            return Err(invalid_special("macaddr8", value));
        }
        offset += 2;
        if offset < bytes.len() && matches!(bytes[offset], b':' | b'-' | b'.') {
            if separator.is_some_and(|separator| separator != bytes[offset]) {
                return Err(invalid_special("macaddr8", value));
            }
            separator = Some(bytes[offset]);
            offset += 1;
        }
    }
    if offset != bytes.len() || !matches!(address.len(), 6 | 8) {
        return Err(invalid_special("macaddr8", value));
    }
    if address.len() == 6 {
        address.splice(3..3, [0xff, 0xfe]);
    }
    Ok(address.try_into().expect("validated EUI-64 width"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PgPoint {
    pub x: PgFloat8,
    pub y: PgFloat8,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PgGeometric {
    Point(PgPoint),
    Line {
        a: PgFloat8,
        b: PgFloat8,
        c: PgFloat8,
    },
    LineSegment {
        start: PgPoint,
        end: PgPoint,
    },
    Box {
        high: PgPoint,
        low: PgPoint,
    },
    Path {
        closed: bool,
        points: Vec<PgPoint>,
    },
    Polygon {
        points: Vec<PgPoint>,
    },
    Circle {
        center: PgPoint,
        radius: PgFloat8,
    },
}

impl PgGeometric {
    pub fn to_postgres_text(&self) -> String {
        let number = |value: PgFloat8| postgres_float_text(value.to_value(), "float8");
        let point = |value: &PgPoint| format!("({},{})", number(value.x), number(value.y));
        match self {
            Self::Point(value) => point(value),
            Self::Line { a, b, c } => {
                format!("{{{},{},{}}}", number(*a), number(*b), number(*c))
            }
            Self::LineSegment { start, end } => format!("[{},{}]", point(start), point(end)),
            Self::Box { high, low } => format!("{},{}", point(high), point(low)),
            Self::Path { closed, points } => {
                let points = points.iter().map(point).collect::<Vec<_>>().join(",");
                if *closed {
                    format!("({points})")
                } else {
                    format!("[{points}]")
                }
            }
            Self::Polygon { points } => {
                format!(
                    "({})",
                    points.iter().map(point).collect::<Vec<_>>().join(",")
                )
            }
            Self::Circle { center, radius } => {
                format!("<{},{}>", point(center), number(*radius))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum PgRangeBound {
    Unbounded,
    Inclusive(Box<PgCanonicalValue>),
    Exclusive(Box<PgCanonicalValue>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PgRange {
    pub subtype: String,
    pub empty: bool,
    pub lower: PgRangeBound,
    pub upper: PgRangeBound,
}

impl PgRange {
    pub fn from_postgres_text(
        value: &str,
        range_type: &str,
    ) -> Result<Self, PgCanonicalValueError> {
        let subtype =
            range_subtype(range_type).ok_or_else(|| invalid_special(range_type, value))?;
        Self::from_postgres_text_with_policy(
            value,
            range_type,
            subtype,
            matches!(range_type, "int4range" | "int8range" | "daterange"),
        )
    }

    pub(crate) fn from_postgres_text_with_policy(
        value: &str,
        range_type: &str,
        subtype: &str,
        canonicalize_discrete: bool,
    ) -> Result<Self, PgCanonicalValueError> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("empty") {
            return Ok(Self {
                subtype: subtype.to_string(),
                empty: true,
                lower: PgRangeBound::Unbounded,
                upper: PgRangeBound::Unbounded,
            });
        }
        let lower_marker = value
            .chars()
            .next()
            .filter(|marker| matches!(marker, '[' | '('))
            .ok_or_else(|| invalid_special(range_type, value))?;
        let upper_marker = value
            .chars()
            .next_back()
            .filter(|marker| matches!(marker, ']' | ')'))
            .ok_or_else(|| invalid_special(range_type, value))?;
        let inner = &value[1..value.len() - 1];
        let bounds = split_quoted_top_level(inner, ',');
        if bounds.len() != 2 {
            return Err(invalid_special(range_type, value));
        }
        let lower = parse_range_bound(bounds[0], subtype, lower_marker == '[')?;
        let upper = parse_range_bound(bounds[1], subtype, upper_marker == ']')?;
        Self {
            subtype: subtype.to_string(),
            empty: false,
            lower,
            upper,
        }
        .canonicalized_with_policy(canonicalize_discrete)
    }

    pub fn to_postgres_text(&self) -> String {
        if self.empty {
            return "empty".to_string();
        }
        let (lower_marker, lower) = render_range_bound(&self.lower, true);
        let (upper_marker, upper) = render_range_bound(&self.upper, false);
        format!("{lower_marker}{lower},{upper}{upper_marker}")
    }

    pub fn canonicalized(mut self) -> Result<Self, PgCanonicalValueError> {
        let canonicalize_discrete = matches!(self.subtype.as_str(), "int4" | "int8" | "date");
        self = self.canonicalized_with_policy(canonicalize_discrete)?;
        Ok(self)
    }

    pub(crate) fn canonicalized_with_policy(
        mut self,
        canonicalize_discrete: bool,
    ) -> Result<Self, PgCanonicalValueError> {
        if range_bound_value_order(&self.lower, &self.upper) == Some(Ordering::Greater) {
            return Err(PgCanonicalValueError::InvalidRangeBounds);
        }
        if canonicalize_discrete {
            self.canonicalize_discrete()?;
        }
        Ok(self)
    }

    fn canonicalize_discrete(&mut self) -> Result<(), PgCanonicalValueError> {
        if !matches!(self.subtype.as_str(), "int4" | "int8" | "date") {
            return Ok(());
        }
        if discrete_range_bound_order(&self.lower, &self.upper) == Some(std::cmp::Ordering::Greater)
        {
            return Err(PgCanonicalValueError::InvalidRangeBounds);
        }

        if let PgRangeBound::Exclusive(value) = &self.lower {
            if let Some(successor) = discrete_range_successor(value)? {
                self.lower = PgRangeBound::Inclusive(Box::new(successor));
            }
        }
        if let PgRangeBound::Inclusive(value) = &self.upper {
            if let Some(successor) = discrete_range_successor(value)? {
                self.upper = PgRangeBound::Exclusive(Box::new(successor));
            }
        }

        let equal =
            discrete_range_bound_order(&self.lower, &self.upper) == Some(std::cmp::Ordering::Equal);
        if equal
            && (!matches!(self.lower, PgRangeBound::Inclusive(_))
                || !matches!(self.upper, PgRangeBound::Inclusive(_)))
        {
            self.empty = true;
            self.lower = PgRangeBound::Unbounded;
            self.upper = PgRangeBound::Unbounded;
        }
        Ok(())
    }
}

fn discrete_range_successor(
    value: &PgCanonicalValue,
) -> Result<Option<PgCanonicalValue>, PgCanonicalValueError> {
    match value {
        PgCanonicalValue::Int4(value) => value
            .checked_add(1)
            .map(PgCanonicalValue::Int4)
            .map(Some)
            .ok_or(PgCanonicalValueError::RangeCanonicalOverflow("integer")),
        PgCanonicalValue::Int8(value) => value
            .checked_add(1)
            .map(PgCanonicalValue::Int8)
            .map(Some)
            .ok_or(PgCanonicalValueError::RangeCanonicalOverflow("bigint")),
        PgCanonicalValue::Date(PgDate::Finite(value)) => PgDate::Finite(*value)
            .checked_add_days(1)
            .map(PgCanonicalValue::Date)
            .map(Some),
        PgCanonicalValue::Date(PgDate::PositiveInfinity | PgDate::NegativeInfinity) => Ok(None),
        _ => Ok(None),
    }
}

fn discrete_range_bound_order(
    lower: &PgRangeBound,
    upper: &PgRangeBound,
) -> Option<std::cmp::Ordering> {
    let lower = match lower {
        PgRangeBound::Inclusive(value) | PgRangeBound::Exclusive(value) => value.as_ref(),
        PgRangeBound::Unbounded => return None,
    };
    let upper = match upper {
        PgRangeBound::Inclusive(value) | PgRangeBound::Exclusive(value) => value.as_ref(),
        PgRangeBound::Unbounded => return None,
    };
    match (lower, upper) {
        (PgCanonicalValue::Int4(left), PgCanonicalValue::Int4(right)) => Some(left.cmp(right)),
        (PgCanonicalValue::Int8(left), PgCanonicalValue::Int8(right)) => Some(left.cmp(right)),
        (PgCanonicalValue::Date(left), PgCanonicalValue::Date(right)) => {
            Some(pg_date_order_key(*left).cmp(&pg_date_order_key(*right)))
        }
        _ => None,
    }
}

pub(crate) fn range_scalar_order(
    subtype: &str,
    left: &PgCanonicalValue,
    right: &PgCanonicalValue,
) -> Option<Ordering> {
    let spec = crate::pg_type_spec(subtype)?;
    let left = crate::type_codec::canonical_index_key(spec, left).ok()?;
    let right = crate::type_codec::canonical_index_key(spec, right).ok()?;
    Some(left.cmp(&right))
}

fn multirange_lower_order(left: &PgRangeBound, right: &PgRangeBound, subtype: &str) -> Ordering {
    match (left, right) {
        (PgRangeBound::Unbounded, PgRangeBound::Unbounded) => Ordering::Equal,
        (PgRangeBound::Unbounded, _) => Ordering::Less,
        (_, PgRangeBound::Unbounded) => Ordering::Greater,
        (
            PgRangeBound::Inclusive(left_value) | PgRangeBound::Exclusive(left_value),
            PgRangeBound::Inclusive(right_value) | PgRangeBound::Exclusive(right_value),
        ) => range_scalar_order(subtype, left_value, right_value)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                matches!(right, PgRangeBound::Inclusive(_))
                    .cmp(&matches!(left, PgRangeBound::Inclusive(_)))
            }),
    }
}

fn multirange_upper_order(left: &PgRangeBound, right: &PgRangeBound, subtype: &str) -> Ordering {
    match (left, right) {
        (PgRangeBound::Unbounded, PgRangeBound::Unbounded) => Ordering::Equal,
        (PgRangeBound::Unbounded, _) => Ordering::Greater,
        (_, PgRangeBound::Unbounded) => Ordering::Less,
        (
            PgRangeBound::Inclusive(left_value) | PgRangeBound::Exclusive(left_value),
            PgRangeBound::Inclusive(right_value) | PgRangeBound::Exclusive(right_value),
        ) => range_scalar_order(subtype, left_value, right_value)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                matches!(left, PgRangeBound::Inclusive(_))
                    .cmp(&matches!(right, PgRangeBound::Inclusive(_)))
            }),
    }
}

fn multirange_components_connect(left: &PgRange, right: &PgRange) -> bool {
    match (&left.upper, &right.lower) {
        (PgRangeBound::Unbounded, _) | (_, PgRangeBound::Unbounded) => true,
        (
            PgRangeBound::Inclusive(left_value) | PgRangeBound::Exclusive(left_value),
            PgRangeBound::Inclusive(right_value) | PgRangeBound::Exclusive(right_value),
        ) => match range_scalar_order(&left.subtype, left_value, right_value) {
            Some(Ordering::Greater) => true,
            Some(Ordering::Equal) => {
                matches!(left.upper, PgRangeBound::Inclusive(_))
                    || matches!(right.lower, PgRangeBound::Inclusive(_))
            }
            _ => false,
        },
    }
}

pub(crate) fn canonicalize_pg_multirange(
    ranges: Vec<PgRange>,
) -> Result<Vec<PgRange>, PgCanonicalValueError> {
    canonicalize_pg_multirange_with_policy(ranges, true)
}

pub(crate) fn canonicalize_pg_multirange_with_policy(
    ranges: Vec<PgRange>,
    canonicalize_discrete: bool,
) -> Result<Vec<PgRange>, PgCanonicalValueError> {
    let mut ranges = ranges
        .into_iter()
        .map(|range| range.canonicalized_with_policy(canonicalize_discrete))
        .collect::<Result<Vec<_>, _>>()?;
    ranges.retain(|range| !range.empty);
    ranges.sort_by(|left, right| {
        multirange_lower_order(&left.lower, &right.lower, &left.subtype)
            .then_with(|| multirange_upper_order(&left.upper, &right.upper, &left.subtype))
    });

    let mut canonical = Vec::<PgRange>::with_capacity(ranges.len());
    for range in ranges {
        let Some(previous) = canonical.last_mut() else {
            canonical.push(range);
            continue;
        };
        if !multirange_components_connect(previous, &range) {
            canonical.push(range);
            continue;
        }
        if multirange_upper_order(&previous.upper, &range.upper, &previous.subtype)
            == Ordering::Less
        {
            previous.upper = range.upper;
        }
    }
    Ok(canonical)
}

pub(crate) fn parse_pg_multirange_with_policy(
    value: &str,
    multirange_type: &str,
    subtype: &str,
    canonicalize_discrete: bool,
) -> Result<Vec<PgRange>, PgCanonicalValueError> {
    let value = value.trim();
    let inner = value
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
        .ok_or_else(|| invalid_special(multirange_type, value))?;
    let ranges = if inner.trim().is_empty() {
        Vec::new()
    } else {
        split_delimited_ranges(inner)
            .into_iter()
            .map(|range| {
                PgRange::from_postgres_text_with_policy(
                    range,
                    multirange_type,
                    subtype,
                    canonicalize_discrete,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    canonicalize_pg_multirange_with_policy(ranges, canonicalize_discrete)
}

fn range_bound_value_order(lower: &PgRangeBound, upper: &PgRangeBound) -> Option<Ordering> {
    let lower = match lower {
        PgRangeBound::Inclusive(value) | PgRangeBound::Exclusive(value) => value.as_ref(),
        PgRangeBound::Unbounded => return None,
    };
    let upper = match upper {
        PgRangeBound::Inclusive(value) | PgRangeBound::Exclusive(value) => value.as_ref(),
        PgRangeBound::Unbounded => return None,
    };
    let subtype = match lower {
        PgCanonicalValue::Int4(_) => "int4",
        PgCanonicalValue::Int8(_) => "int8",
        PgCanonicalValue::Numeric(_) => "numeric",
        PgCanonicalValue::Date(_) => "date",
        PgCanonicalValue::Timestamp(_) => "timestamp",
        PgCanonicalValue::TimestampTz(_) => "timestamptz",
        _ => return None,
    };
    range_scalar_order(subtype, lower, upper)
}

fn pg_date_order_key(value: PgDate) -> (u8, i32) {
    match value {
        PgDate::NegativeInfinity => (0, 0),
        PgDate::Finite(days) => (1, days),
        PgDate::PositiveInfinity => (2, 0),
    }
}

pub fn format_pg_multirange(ranges: &[PgRange]) -> String {
    format!(
        "{{{}}}",
        ranges
            .iter()
            .map(PgRange::to_postgres_text)
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn render_range_bound(bound: &PgRangeBound, lower: bool) -> (char, String) {
    match bound {
        PgRangeBound::Unbounded => (if lower { '(' } else { ')' }, String::new()),
        PgRangeBound::Inclusive(value) => {
            (if lower { '[' } else { ']' }, render_range_scalar(value))
        }
        PgRangeBound::Exclusive(value) => {
            (if lower { '(' } else { ')' }, render_range_scalar(value))
        }
    }
}

fn render_range_scalar(value: &PgCanonicalValue) -> String {
    match value {
        PgCanonicalValue::Int4(value) => value.to_string(),
        PgCanonicalValue::Int8(value) => value.to_string(),
        PgCanonicalValue::Numeric(value) => value.to_decimal_text(),
        PgCanonicalValue::Date(value) => value.to_iso_text(),
        PgCanonicalValue::Timestamp(value) => quote_range_scalar(&value.to_iso_text(false)),
        PgCanonicalValue::TimestampTz(value) => quote_range_scalar(&value.to_iso_text(true)),
        other => quote_range_scalar(&serde_json::to_string(other).unwrap_or_default()),
    }
}

fn quote_range_scalar(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PgOidAlias {
    pub oid: Option<u32>,
    pub symbolic_name: Option<String>,
}

impl PgOidAlias {
    pub fn from_postgres_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        let value = value.trim();
        if let Ok(oid) = value.parse::<u32>() {
            return Ok(Self {
                oid: Some(oid),
                symbolic_name: None,
            });
        }
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(invalid_special("oid alias", value));
        }
        Ok(Self {
            oid: None,
            symbolic_name: Some(value.to_string()),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PgTupleId {
    pub block: u32,
    pub offset: u16,
}

impl PgTupleId {
    pub fn from_postgres_text(value: &str) -> Result<Self, PgCanonicalValueError> {
        let value = value.trim();
        let inner = value
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .ok_or_else(|| invalid_special("tid", value))?;
        let (block, offset) = inner
            .split_once(',')
            .ok_or_else(|| invalid_special("tid", value))?;
        if offset.contains(',') {
            return Err(invalid_special("tid", value));
        }
        let block = block
            .trim()
            .parse::<u32>()
            .map_err(|_| invalid_special("tid", value))?;
        let offset = offset
            .trim()
            .parse::<u16>()
            .map_err(|_| invalid_special("tid", value))?;
        Ok(Self { block, offset })
    }

    pub fn to_postgres_text(self) -> String {
        format!("({},{})", self.block, self.offset)
    }
}

pub fn parse_pg_xid32(value: &str, pg_type: &str) -> Result<u32, PgCanonicalValueError> {
    let value = value.trim();
    let parsed = value
        .parse::<i128>()
        .map_err(|_| invalid_special(pg_type, value))?;
    if parsed >= 0 {
        return u32::try_from(parsed).map_err(|_| PgCanonicalValueError::NumericOverflow);
    }
    i32::try_from(parsed)
        .map(|value| value as u32)
        .map_err(|_| PgCanonicalValueError::NumericOverflow)
}

pub fn parse_pg_xid8(value: &str) -> Result<u64, PgCanonicalValueError> {
    let value = value.trim();
    let (negative, digits) = value
        .strip_prefix('-')
        .map_or((false, value), |digits| (true, digits));
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_special("xid8", value));
    }
    let magnitude = digits
        .parse::<u128>()
        .map_err(|_| PgCanonicalValueError::NumericOverflow)
        .and_then(|value| {
            u64::try_from(value).map_err(|_| PgCanonicalValueError::NumericOverflow)
        })?;
    if !negative {
        return Ok(magnitude);
    }
    Ok(0_u64.wrapping_sub(magnitude))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PgArrayDimension {
    pub lower_bound: i32,
    pub length: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PgArray {
    pub element_type: String,
    pub dimensions: Vec<PgArrayDimension>,
    pub elements: Vec<PgCanonicalValue>,
}

impl PgArray {
    pub fn new(
        element_type: impl Into<String>,
        dimensions: Vec<PgArrayDimension>,
        elements: Vec<PgCanonicalValue>,
    ) -> Result<Self, PgCanonicalValueError> {
        let expected = if dimensions.is_empty() {
            Some(0)
        } else {
            dimensions.iter().try_fold(1usize, |total, dimension| {
                total.checked_mul(dimension.length)
            })
        };
        let Some(expected) = expected else {
            return Err(PgCanonicalValueError::ArrayDimensionsOverflow);
        };
        if expected != elements.len() {
            return Err(PgCanonicalValueError::ArrayElementCount {
                expected,
                actual: elements.len(),
            });
        }
        Ok(Self {
            element_type: element_type.into(),
            dimensions,
            elements,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PgCompositeField {
    pub name: String,
    pub pg_type: String,
    pub value: PgCanonicalValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PgComposite {
    pub type_oid: Option<u32>,
    pub type_name: String,
    pub fields: Vec<PgCompositeField>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PgSnapshot {
    pub xmin: u64,
    pub xmax: u64,
    pub in_progress: Vec<u64>,
}

impl PgSnapshot {
    pub fn new(
        xmin: u64,
        xmax: u64,
        mut in_progress: Vec<u64>,
    ) -> Result<Self, PgCanonicalValueError> {
        if xmin == 0 || xmin > xmax {
            return Err(PgCanonicalValueError::InvalidSnapshotBounds);
        }
        let valid = in_progress.windows(2).all(|window| window[0] <= window[1])
            && in_progress.iter().all(|xid| *xid >= xmin && *xid < xmax);
        if !valid {
            return Err(PgCanonicalValueError::InvalidSnapshotTransactions);
        }
        in_progress.dedup();
        Ok(Self {
            xmin,
            xmax,
            in_progress,
        })
    }

    pub fn to_postgres_text(&self) -> String {
        format!(
            "{}:{}:{}",
            self.xmin,
            self.xmax,
            self.in_progress
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

pub fn format_pg_lsn(value: u64) -> String {
    format!("{:X}/{:X}", value >> 32, value as u32)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum PgCanonicalValue {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(PgFloat4),
    Float8(PgFloat8),
    Numeric(PgNumeric),
    Money(i64),
    Text(String),
    Bytes(Vec<u8>),
    BitString(PgBitString),
    Date(PgDate),
    Time(PgTime),
    TimeTz(PgTimeTz),
    Timestamp(PgTimestamp),
    TimestampTz(PgTimestamp),
    Interval(PgInterval),
    Uuid([u8; 16]),
    JsonText(String),
    Json(JsonValue),
    Xml(String),
    JsonPath(String),
    TsVector(PgTsVector),
    TsQuery(PgTsQuery),
    Network(PgNetwork),
    MacAddress(PgMacAddress),
    Geometric(PgGeometric),
    Range(PgRange),
    Multirange(Vec<PgRange>),
    Array(PgArray),
    Composite(PgComposite),
    Oid(u32),
    OidAlias(PgOidAlias),
    TransactionId32(u32),
    TransactionId64(u64),
    CommandId(u32),
    TupleId(PgTupleId),
    Lsn(u64),
    Snapshot(PgSnapshot),
    Vector(Vec<PgFloat4>),
}

/// Parse types whose legacy SQL representation is text but whose durable form
/// must retain PostgreSQL semantics. `None` means the type is outside this
/// parser's domain.
pub fn parse_pg_canonical_special(
    pg_type: &str,
    value: &str,
) -> Result<Option<PgCanonicalValue>, PgCanonicalValueError> {
    let original = value;
    let value = value.trim();
    let parsed = match pg_type {
        "inet" => PgCanonicalValue::Network(PgNetwork::from_postgres_text(
            original,
            PgNetworkKind::Inet,
        )?),
        "cidr" => PgCanonicalValue::Network(PgNetwork::from_postgres_text(
            original,
            PgNetworkKind::Cidr,
        )?),
        "macaddr" => PgCanonicalValue::MacAddress(PgMacAddress::from_postgres_text(value, false)?),
        "macaddr8" => PgCanonicalValue::MacAddress(PgMacAddress::from_postgres_text(value, true)?),
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            PgCanonicalValue::Geometric(parse_geometric(pg_type, value)?)
        }
        "int4range" | "numrange" | "tsrange" | "tstzrange" | "daterange" | "int8range" => {
            PgCanonicalValue::Range(PgRange::from_postgres_text(value, pg_type)?)
        }
        "int4multirange" | "nummultirange" | "tsmultirange" | "tstzmultirange"
        | "datemultirange" | "int8multirange" => {
            let range_type = pg_type.replacen("multirange", "range", 1);
            let inner = value
                .strip_prefix('{')
                .and_then(|value| value.strip_suffix('}'))
                .ok_or_else(|| invalid_special(pg_type, value))?;
            let ranges = if inner.trim().is_empty() {
                Vec::new()
            } else {
                split_delimited_ranges(inner)
                    .into_iter()
                    .map(|range| PgRange::from_postgres_text(range, &range_type))
                    .collect::<Result<Vec<_>, _>>()?
            };
            PgCanonicalValue::Multirange(canonicalize_pg_multirange(ranges)?)
        }
        "oid" => {
            PgCanonicalValue::Oid(parse_pg_oid(value).map_err(|_| invalid_special(pg_type, value))?)
        }
        "xid" => PgCanonicalValue::TransactionId32(parse_pg_xid32(value, pg_type)?),
        "xid8" => PgCanonicalValue::TransactionId64(parse_pg_xid8(value)?),
        "cid" => PgCanonicalValue::CommandId(parse_pg_xid32(value, pg_type)?),
        "tid" => PgCanonicalValue::TupleId(PgTupleId::from_postgres_text(value)?),
        "regproc" | "regprocedure" | "regoper" | "regoperator" | "regclass" | "regcollation"
        | "regtype" | "regrole" | "regnamespace" | "regconfig" | "regdictionary" => {
            PgCanonicalValue::OidAlias(PgOidAlias::from_postgres_text(value)?)
        }
        "pg_lsn" => PgCanonicalValue::Lsn(parse_pg_lsn(value)?),
        "pg_snapshot" | "txid_snapshot" => PgCanonicalValue::Snapshot(parse_pg_snapshot(original)?),
        "tsvector" => PgCanonicalValue::TsVector(
            PgTsVector::from_postgres_text(value).map_err(|_| invalid_special(pg_type, value))?,
        ),
        "tsquery" => PgCanonicalValue::TsQuery(
            PgTsQuery::from_postgres_text(value).map_err(|_| invalid_special(pg_type, value))?,
        ),
        _ => return Ok(None),
    };
    Ok(Some(parsed))
}

pub fn is_pg_canonical_special_type(pg_type: &str) -> bool {
    matches!(
        pg_type,
        "inet"
            | "cidr"
            | "macaddr"
            | "macaddr8"
            | "point"
            | "line"
            | "lseg"
            | "box"
            | "path"
            | "polygon"
            | "circle"
            | "int4range"
            | "numrange"
            | "tsrange"
            | "tstzrange"
            | "daterange"
            | "int8range"
            | "int4multirange"
            | "nummultirange"
            | "tsmultirange"
            | "tstzmultirange"
            | "datemultirange"
            | "int8multirange"
            | "oid"
            | "xid"
            | "xid8"
            | "cid"
            | "tid"
            | "regproc"
            | "regprocedure"
            | "regoper"
            | "regoperator"
            | "regclass"
            | "regcollation"
            | "regtype"
            | "regrole"
            | "regnamespace"
            | "regconfig"
            | "regdictionary"
            | "pg_lsn"
            | "pg_snapshot"
            | "txid_snapshot"
            | "tsvector"
            | "tsquery"
    )
}

fn invalid_special(pg_type: impl Into<String>, value: impl Into<String>) -> PgCanonicalValueError {
    PgCanonicalValueError::InvalidSpecialValue {
        pg_type: pg_type.into(),
        value: value.into(),
    }
}

fn network_has_host_bits(network: PgNetwork) -> bool {
    match network.address {
        PgIpAddress::V4(octets) => {
            let value = u32::from_be_bytes(octets);
            let mask = if network.prefix == 0 {
                0
            } else {
                u32::MAX << (32 - network.prefix)
            };
            value & !mask != 0
        }
        PgIpAddress::V6(octets) => {
            let value = u128::from_be_bytes(octets);
            let mask = if network.prefix == 0 {
                0
            } else {
                u128::MAX << (128 - network.prefix)
            };
            value & !mask != 0
        }
    }
}

fn range_subtype(range_type: &str) -> Option<&'static str> {
    match range_type {
        "int4range" => Some("int4"),
        "numrange" => Some("numeric"),
        "tsrange" => Some("timestamp"),
        "tstzrange" => Some("timestamptz"),
        "daterange" => Some("date"),
        "int8range" => Some("int8"),
        _ => None,
    }
}

fn parse_range_bound(
    value: &str,
    subtype: &str,
    inclusive: bool,
) -> Result<PgRangeBound, PgCanonicalValueError> {
    let value = unquote_pg_text(value.trim())?;
    if value.is_empty() {
        return Ok(PgRangeBound::Unbounded);
    }
    let value = match subtype {
        "int4" => PgCanonicalValue::Int4(
            value
                .parse::<i32>()
                .map_err(|_| invalid_special(subtype, &value))?,
        ),
        "int8" => PgCanonicalValue::Int8(
            value
                .parse::<i64>()
                .map_err(|_| invalid_special(subtype, &value))?,
        ),
        "numeric" => PgCanonicalValue::Numeric(PgNumeric::from_decimal_text(&value)?),
        "date" => PgCanonicalValue::Date(PgDate::from_iso_text(&value)?),
        "timestamp" => PgCanonicalValue::Timestamp(PgTimestamp::from_iso_text(&value, false)?),
        "timestamptz" => PgCanonicalValue::TimestampTz(PgTimestamp::from_iso_text(&value, true)?),
        _ => return Err(invalid_special(subtype, value)),
    };
    Ok(if inclusive {
        PgRangeBound::Inclusive(Box::new(value))
    } else {
        PgRangeBound::Exclusive(Box::new(value))
    })
}

fn unquote_pg_text(value: &str) -> Result<String, PgCanonicalValueError> {
    if !value.starts_with('"') {
        return Ok(value.to_string());
    }
    if value.len() < 2 || !value.ends_with('"') {
        return Err(invalid_special("quoted value", value));
    }
    let mut output = String::new();
    let mut escaped = false;
    for character in value[1..value.len() - 1].chars() {
        if escaped {
            output.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else {
            output.push(character);
        }
    }
    if escaped {
        return Err(invalid_special("quoted value", value));
    }
    Ok(output)
}

fn split_quoted_top_level(value: &str, delimiter: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut start = 0;
    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
        } else if quoted && character == '\\' {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if character == delimiter && !quoted {
            parts.push(&value[start..index]);
            start = index + character.len_utf8();
        }
    }
    parts.push(&value[start..]);
    parts
}

fn split_delimited_ranges(value: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut depth = 0_i32;
    let mut start = 0;
    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
            continue;
        }
        if quoted {
            continue;
        }
        match character {
            '[' | '(' => depth += 1,
            ']' | ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(value[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(value[start..].trim());
    parts
}

fn parse_pg_lsn(value: &str) -> Result<u64, PgCanonicalValueError> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| invalid_special("pg_lsn", value))?;
    let high = u32::from_str_radix(high, 16).map_err(|_| invalid_special("pg_lsn", value))?;
    let low = u32::from_str_radix(low, 16).map_err(|_| invalid_special("pg_lsn", value))?;
    Ok((u64::from(high) << 32) | u64::from(low))
}

fn parse_pg_snapshot(value: &str) -> Result<PgSnapshot, PgCanonicalValueError> {
    let mut parts = value.split(':');
    let xmin = parts
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| invalid_special("pg_snapshot", value))?;
    let xmax = parts
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| invalid_special("pg_snapshot", value))?;
    let in_progress = parts
        .next()
        .ok_or_else(|| invalid_special("pg_snapshot", value))?;
    if parts.next().is_some() {
        return Err(invalid_special("pg_snapshot", value));
    }
    let in_progress = in_progress.strip_suffix(',').unwrap_or(in_progress);
    let in_progress = if in_progress.is_empty() {
        Vec::new()
    } else {
        in_progress
            .split(',')
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| invalid_special("pg_snapshot", value))
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    PgSnapshot::new(xmin, xmax, in_progress)
}

fn parse_geometric(pg_type: &str, value: &str) -> Result<PgGeometric, PgCanonicalValueError> {
    let value = value.trim();
    match pg_type {
        "point" => parse_point(value).map(PgGeometric::Point),
        "line" => {
            if value.starts_with('{') {
                if !value.ends_with('}') {
                    return Err(invalid_special(pg_type, value));
                }
                let values = parse_geometric_components(pg_type, value, "{}")?;
                if values.len() != 3 || pg_float_eq(values[0], 0.0) && pg_float_eq(values[1], 0.0) {
                    return Err(invalid_special(pg_type, value));
                }
                return Ok(PgGeometric::Line {
                    a: PgFloat8::from_value(values[0]),
                    b: PgFloat8::from_value(values[1]),
                    c: PgFloat8::from_value(values[2]),
                });
            }
            let points = points_from_components(pg_type, value, "()[]")?;
            if points.len() != 2 || pg_point_eq(points[0], points[1]) {
                return Err(invalid_special(pg_type, value));
            }
            Ok(line_from_points(points[0], points[1]))
        }
        "lseg" => {
            let points = points_from_components(pg_type, value, "()[]")?;
            if points.len() != 2 {
                return Err(invalid_special(pg_type, value));
            }
            Ok(PgGeometric::LineSegment {
                start: points[0],
                end: points[1],
            })
        }
        "box" => {
            let points = points_from_components(pg_type, value, "()")?;
            if points.len() != 2 {
                return Err(invalid_special(pg_type, value));
            }
            let (first, second) = (points[0], points[1]);
            Ok(PgGeometric::Box {
                high: PgPoint {
                    x: PgFloat8::from_value(pg_float_max(first.x.to_value(), second.x.to_value())),
                    y: PgFloat8::from_value(pg_float_max(first.y.to_value(), second.y.to_value())),
                },
                low: PgPoint {
                    x: PgFloat8::from_value(pg_float_min(first.x.to_value(), second.x.to_value())),
                    y: PgFloat8::from_value(pg_float_min(first.y.to_value(), second.y.to_value())),
                },
            })
        }
        "path" => {
            let closed = !value.starts_with('[');
            if value.contains(['[', ']']) && !(value.starts_with('[') && value.ends_with(']')) {
                return Err(invalid_special(pg_type, value));
            }
            let points = points_from_components(pg_type, value, "()[]")?;
            if points.is_empty() {
                return Err(invalid_special(pg_type, value));
            }
            Ok(PgGeometric::Path { closed, points })
        }
        "polygon" => {
            let points = points_from_components(pg_type, value, "()")?;
            if points.is_empty() {
                return Err(invalid_special(pg_type, value));
            }
            Ok(PgGeometric::Polygon { points })
        }
        "circle" => {
            let values = parse_geometric_components(pg_type, value, "()<>")?;
            if values.len() != 3 || values[2] < 0.0 {
                return Err(invalid_special(pg_type, value));
            }
            Ok(PgGeometric::Circle {
                center: PgPoint {
                    x: PgFloat8::from_value(values[0]),
                    y: PgFloat8::from_value(values[1]),
                },
                radius: PgFloat8::from_value(values[2]),
            })
        }
        _ => Err(invalid_special(pg_type, value)),
    }
}

fn parse_point(value: &str) -> Result<PgPoint, PgCanonicalValueError> {
    let value = value.trim();
    let inner = if let Some(inner) = value.strip_prefix('(') {
        inner
            .strip_suffix(')')
            .ok_or_else(|| invalid_special("point", value))?
    } else if value.contains(['(', ')']) {
        return Err(invalid_special("point", value));
    } else {
        value
    };
    let values = parse_geometric_float_list(inner)?;
    if values.len() != 2 {
        return Err(invalid_special("point", value));
    }
    Ok(PgPoint {
        x: PgFloat8::from_value(values[0]),
        y: PgFloat8::from_value(values[1]),
    })
}

fn parse_geometric_float_list(value: &str) -> Result<Vec<f64>, PgCanonicalValueError> {
    split_quoted_top_level(value, ',')
        .into_iter()
        .map(parse_geometric_float)
        .collect()
}

fn parse_geometric_float(value: &str) -> Result<f64, PgCanonicalValueError> {
    let value = value.trim();
    match value.to_ascii_lowercase().as_str() {
        "nan" => return Ok(f64::NAN),
        "infinity" | "+infinity" | "inf" | "+inf" => return Ok(f64::INFINITY),
        "-infinity" | "-inf" => return Ok(f64::NEG_INFINITY),
        _ => {}
    }
    let unsigned = value.trim_start_matches(['+', '-']);
    if unsigned.starts_with("0x") || unsigned.starts_with("0X") {
        return parse_geometric_hex_float(value);
    }
    let parsed = value
        .parse::<f64>()
        .map_err(|_| invalid_special("geometric", value))?;
    if parsed.is_infinite() || parsed == 0.0 && geometric_float_has_nonzero_digit(value) {
        return Err(PgCanonicalValueError::FloatOverflow(value.to_string()));
    }
    Ok(parsed)
}

fn parse_geometric_hex_float(value: &str) -> Result<f64, PgCanonicalValueError> {
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map(|value| (true, value))
        .or_else(|| value.strip_prefix('+').map(|value| (false, value)))
        .unwrap_or((false, value));
    let digits = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
        .ok_or_else(|| invalid_special("geometric", value))?;
    let mut exponent_parts = digits.split(['p', 'P']);
    let significand = exponent_parts.next().unwrap_or_default();
    let exponent = exponent_parts.next();
    if exponent_parts.next().is_some() {
        return Err(invalid_special("geometric", value));
    }
    let exponent = exponent
        .map(|value| {
            value
                .parse::<i32>()
                .map_err(|_| PgCanonicalValueError::FloatOverflow(value.to_string()))
        })
        .transpose()?
        .unwrap_or(0);
    let mut parts = significand.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid_special("geometric", value));
    }
    let mut parsed = 0.0;
    for digit in whole.bytes() {
        parsed = parsed * 16.0 + f64::from(hex_float_digit(digit));
    }
    let mut scale = 1.0 / 16.0;
    for digit in fraction.bytes() {
        parsed += f64::from(hex_float_digit(digit)) * scale;
        scale /= 16.0;
    }
    parsed *= 2_f64.powi(exponent);
    if negative {
        parsed = -parsed;
    }
    if parsed.is_infinite()
        || parsed == 0.0
            && whole
                .bytes()
                .chain(fraction.bytes())
                .any(|byte| hex_float_digit(byte) != 0)
    {
        return Err(PgCanonicalValueError::FloatOverflow(value.to_string()));
    }
    Ok(parsed)
}

fn hex_float_digit(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => unreachable!("hex float input was validated"),
    }
}

fn geometric_float_has_nonzero_digit(value: &str) -> bool {
    value
        .split_once(['e', 'E'])
        .map_or(value, |(mantissa, _)| mantissa)
        .bytes()
        .any(|byte| matches!(byte, b'1'..=b'9'))
}

fn parse_geometric_components(
    pg_type: &str,
    value: &str,
    allowed_delimiters: &str,
) -> Result<Vec<f64>, PgCanonicalValueError> {
    let mut stack = Vec::new();
    let mut flattened = String::with_capacity(value.len());
    for character in value.chars() {
        let expected_close = match character {
            '(' => Some(')'),
            '[' => Some(']'),
            '{' => Some('}'),
            '<' => Some('>'),
            _ => None,
        };
        if let Some(expected_close) = expected_close {
            if !allowed_delimiters.contains(character) {
                return Err(invalid_special(pg_type, value));
            }
            stack.push(expected_close);
            continue;
        }
        if matches!(character, ')' | ']' | '}' | '>') {
            if !allowed_delimiters.contains(character) || stack.pop() != Some(character) {
                return Err(invalid_special(pg_type, value));
            }
            continue;
        }
        flattened.push(character);
    }
    if !stack.is_empty() {
        return Err(invalid_special(pg_type, value));
    }
    parse_geometric_float_list(&flattened).map_err(|error| match error {
        PgCanonicalValueError::FloatOverflow(_) => error,
        _ => invalid_special(pg_type, value),
    })
}

fn points_from_components(
    pg_type: &str,
    value: &str,
    allowed_delimiters: &str,
) -> Result<Vec<PgPoint>, PgCanonicalValueError> {
    let values = parse_geometric_components(pg_type, value, allowed_delimiters)?;
    if values.len() % 2 != 0 {
        return Err(invalid_special(pg_type, value));
    }
    Ok(values
        .chunks_exact(2)
        .map(|coordinates| PgPoint {
            x: PgFloat8::from_value(coordinates[0]),
            y: PgFloat8::from_value(coordinates[1]),
        })
        .collect())
}

fn line_from_points(first: PgPoint, second: PgPoint) -> PgGeometric {
    let x1 = first.x.to_value();
    let y1 = first.y.to_value();
    let x2 = second.x.to_value();
    let y2 = second.y.to_value();
    let (a, b, c) = if pg_float_eq(x1, x2) {
        (-1.0, 0.0, x1)
    } else {
        let a = (y2 - y1) / (x2 - x1);
        (a, -1.0, y1 - a * x1)
    };
    PgGeometric::Line {
        a: PgFloat8::from_value(a),
        b: PgFloat8::from_value(b),
        c: PgFloat8::from_value(c),
    }
}

fn pg_point_eq(left: PgPoint, right: PgPoint) -> bool {
    pg_float_eq(left.x.to_value(), right.x.to_value())
        && pg_float_eq(left.y.to_value(), right.y.to_value())
}

fn pg_float_eq(left: f64, right: f64) -> bool {
    left == right || left.is_nan() && right.is_nan()
}

fn pg_float_greater(left: f64, right: f64) -> bool {
    left.is_nan() && !right.is_nan() || !left.is_nan() && !right.is_nan() && left > right
}

fn pg_float_max(left: f64, right: f64) -> f64 {
    if pg_float_greater(left, right) {
        left
    } else {
        right
    }
}

fn pg_float_min(left: f64, right: f64) -> f64 {
    if pg_float_greater(left, right) {
        right
    } else {
        left
    }
}

/// Logical PostgreSQL type metadata attached to a legacy [`SqlValue`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqlLogicalTypeRef<'a> {
    name: &'a str,
    oid: Option<i32>,
}

impl<'a> SqlLogicalTypeRef<'a> {
    fn new(name: &'a str) -> Self {
        Self {
            name,
            oid: pg_type_oid_by_name(name),
        }
    }

    pub fn name(self) -> &'a str {
        self.name
    }

    pub fn oid(self) -> Option<i32> {
        self.oid
    }
}

/// A borrowed result cell with its planner-derived column identity and type.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SqlTypedValueRef<'a> {
    column_name: Option<&'a str>,
    logical_type: Option<SqlLogicalTypeRef<'a>>,
    value: &'a SqlValue,
}

impl<'a> SqlTypedValueRef<'a> {
    pub fn column_name(self) -> Option<&'a str> {
        self.column_name
    }

    pub fn logical_type(self) -> Option<SqlLogicalTypeRef<'a>> {
        self.logical_type
    }

    pub fn value(self) -> &'a SqlValue {
        self.value
    }

    pub fn into_owned(self) -> SqlTypedValue {
        SqlTypedValue {
            column_name: self.column_name.map(str::to_string),
            logical_type: self
                .logical_type
                .map(|logical_type| logical_type.name.to_string()),
            value: self.value.clone(),
        }
    }
}

/// An owned typed result cell for streaming and cross-thread handoff.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SqlTypedValue {
    pub column_name: Option<String>,
    pub logical_type: Option<String>,
    pub value: SqlValue,
}

impl SqlTypedValue {
    pub fn logical_type_ref(&self) -> Option<SqlLogicalTypeRef<'_>> {
        self.logical_type.as_deref().map(SqlLogicalTypeRef::new)
    }

    pub fn into_value(self) -> SqlValue {
        self.value
    }
}

/// A borrowed row whose cells expose the result's parallel logical-type data.
#[derive(Clone, Copy, Debug)]
pub struct SqlTypedRowRef<'a> {
    columns: &'a [String],
    column_types: &'a [Option<String>],
    values: &'a [SqlValue],
}

impl<'a> SqlTypedRowRef<'a> {
    pub fn len(self) -> usize {
        self.values.len()
    }

    pub fn is_empty(self) -> bool {
        self.values.is_empty()
    }

    pub fn get(self, index: usize) -> Option<SqlTypedValueRef<'a>> {
        let value = self.values.get(index)?;
        Some(SqlTypedValueRef {
            column_name: self.columns.get(index).map(String::as_str),
            logical_type: self
                .column_types
                .get(index)
                .and_then(Option::as_deref)
                .map(SqlLogicalTypeRef::new),
            value,
        })
    }

    pub fn iter(self) -> SqlTypedRowIter<'a> {
        SqlTypedRowIter {
            row: self,
            position: 0,
        }
    }
}

pub struct SqlTypedRowIter<'a> {
    row: SqlTypedRowRef<'a>,
    position: usize,
}

impl<'a> Iterator for SqlTypedRowIter<'a> {
    type Item = SqlTypedValueRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.row.get(self.position)?;
        self.position += 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.row.len().saturating_sub(self.position);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for SqlTypedRowIter<'_> {}

pub struct SqlTypedRows<'a> {
    result: &'a SqlResult,
    position: usize,
}

impl<'a> Iterator for SqlTypedRows<'a> {
    type Item = SqlTypedRowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let values = self.result.rows.get(self.position)?;
        self.position += 1;
        Some(SqlTypedRowRef {
            columns: &self.result.columns,
            column_types: &self.result.column_types,
            values,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.result.rows.len().saturating_sub(self.position);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for SqlTypedRows<'_> {}

impl SqlResult {
    /// Iterates rows without changing the source-compatible `rows`/`SqlValue`
    /// representation while exposing planner-derived logical types per cell.
    pub fn typed_rows(&self) -> SqlTypedRows<'_> {
        SqlTypedRows {
            result: self,
            position: 0,
        }
    }

    pub fn typed_row(&self, index: usize) -> Option<SqlTypedRowRef<'_>> {
        self.rows.get(index).map(|values| SqlTypedRowRef {
            columns: &self.columns,
            column_types: &self.column_types,
            values,
        })
    }
}

impl SqlRowStream {
    /// Returns an owned batch retaining the same logical type metadata as the
    /// originating result. The existing `next_batch` method remains unchanged.
    pub fn next_typed_batch(&mut self, max_rows: usize) -> Vec<Vec<SqlTypedValue>> {
        let columns = self.columns().to_vec();
        let column_types = self.column_types().to_vec();
        self.next_batch(max_rows)
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .map(|(index, value)| SqlTypedValue {
                        column_name: columns.get(index).cloned(),
                        logical_type: column_types.get(index).cloned().flatten(),
                        value,
                    })
                    .collect()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floating_point_text_matches_postgresql_thresholds_and_special_values() {
        assert_eq!(postgres_float_text(f64::from(0.1_f32), "float4"), "0.1");
        assert_eq!(postgres_float_text(0.1, "float8"), "0.1");
        assert_eq!(postgres_float_text(1e-5, "float8"), "1e-05");
        assert_eq!(postgres_float_text(1e-4, "float8"), "0.0001");
        assert_eq!(postgres_float_text(1e15, "float8"), "1e+15");
        assert_eq!(postgres_float_text(1e6, "float4"), "1e+06");
        assert_eq!(postgres_float_text(-0.0, "float8"), "-0");
        assert_eq!(postgres_float_text(f64::NAN, "float8"), "NaN");
        assert_eq!(postgres_float_text(f64::INFINITY, "float4"), "Infinity");
        assert_eq!(
            postgres_float_text(f64::NEG_INFINITY, "float8"),
            "-Infinity"
        );
    }

    #[test]
    fn floating_values_preserve_bits_including_nan_and_signed_zero() {
        for value in [f64::NAN, f64::INFINITY, -0.0, f64::from_bits(1)] {
            let canonical = PgFloat8::from_value(value);
            assert_eq!(canonical.to_value().to_bits(), value.to_bits());
            let json = serde_json::to_string(&canonical).unwrap();
            assert_eq!(serde_json::from_str::<PgFloat8>(&json).unwrap(), canonical);
        }
    }

    #[test]
    fn money_parsing_rounding_ranges_and_display_are_exact() {
        assert_eq!(pg_money_cents_from_text("$0.005").unwrap(), 1);
        assert_eq!(pg_money_cents_from_text("-$0.005").unwrap(), -1);
        assert_eq!(pg_money_cents_from_text("(1,234.56)").unwrap(), -123_456);
        assert_eq!(
            pg_money_cents_from_text("92233720368547758.07").unwrap(),
            i64::MAX
        );
        assert_eq!(
            pg_money_cents_from_text("-92233720368547758.08").unwrap(),
            i64::MIN
        );
        assert_eq!(
            pg_money_cents_from_text("92233720368547758.08"),
            Err(PgCanonicalValueError::NumericOverflow)
        );
        assert_eq!(
            pg_money_cents_from_text("-92233720368547758.09"),
            Err(PgCanonicalValueError::NumericOverflow)
        );
        assert_eq!(pg_money_display_from_cents(123_456), "$1,234.56");
        assert_eq!(pg_money_display_from_cents(-123_456), "-$1,234.56");
    }

    #[test]
    fn from_decimal_text_builds_the_coefficient_once_and_trims_leading_zeros() {
        for (text, expected) in [
            ("12.50", "12.50"),
            ("000.500", "0.500"),
            (".5", "0.5"),
            ("0.0", "0.0"),
            ("-0", "0"),
            ("-0.00", "0.00"),
            ("007", "7"),
            ("1e3", "1000"),
            ("12.50e-1", "1.250"),
            (
                "-123456789012345678901234567890.5",
                "-123456789012345678901234567890.5",
            ),
            ("+42", "42"),
            ("0", "0"),
        ] {
            let value = PgNumeric::from_decimal_text(text).unwrap();
            assert_eq!(value.to_decimal_text(), expected, "{text}");
        }
        for bad in ["", ".", "1.2.3", "1e", "abc", "1e2e3"] {
            assert!(PgNumeric::from_decimal_text(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn numeric_coefficient_and_display_scale_are_exact() {
        let numeric = PgNumeric::finite(false, "900719925474099301", 2).unwrap();
        assert_eq!(
            numeric,
            PgNumeric::Finite {
                negative: false,
                coefficient: "900719925474099301".to_string(),
                display_scale: 2,
            }
        );
        assert!(PgNumeric::finite(false, "090", 0).is_err());
        assert_eq!(
            PgNumeric::finite(true, "0", 4).unwrap(),
            PgNumeric::Finite {
                negative: false,
                coefficient: "0".to_string(),
                display_scale: 4,
            }
        );
        assert_eq!(
            PgNumeric::from_decimal_text(" +0009007199254740993.0100 ")
                .unwrap()
                .to_decimal_text(),
            "9007199254740993.0100"
        );
        assert_eq!(
            PgNumeric::from_decimal_text("-.50")
                .unwrap()
                .to_decimal_text(),
            "-0.50"
        );
        assert_eq!(
            PgNumeric::from_decimal_text("1.2300e2")
                .unwrap()
                .to_decimal_text(),
            "123.00"
        );
        assert_eq!(
            PgNumeric::from_decimal_text("0e131073")
                .unwrap()
                .to_decimal_text(),
            "0"
        );
        assert!(matches!(
            PgNumeric::from_decimal_text("1ehello"),
            Err(PgCanonicalValueError::InvalidNumericCoefficient)
        ));
        assert!(matches!(
            PgNumeric::from_decimal_text("1e999999999999"),
            Err(PgCanonicalValueError::NumericOverflow)
        ));
        assert_eq!(
            PgNumeric::from_decimal_text("-1.235")
                .unwrap()
                .with_typmod(4, 2)
                .unwrap()
                .to_decimal_text(),
            "-1.24"
        );
        assert_eq!(
            PgNumeric::from_decimal_text("149")
                .unwrap()
                .with_typmod(2, -1)
                .unwrap()
                .to_decimal_text(),
            "150"
        );
        assert!(PgNumeric::from_decimal_text("999")
            .unwrap()
            .with_typmod(2, -1)
            .is_err());
    }

    #[test]
    fn temporal_text_parses_to_postgres_epoch_values_without_host_state() {
        assert_eq!(
            PgDate::from_iso_text("2024-02-29").unwrap(),
            PgDate::Finite(8_825)
        );
        assert_eq!(
            PgDate::from_iso_text("2024-02-29T00:00:00.000Z")
                .unwrap()
                .to_iso_text(),
            "2024-02-29"
        );
        assert_eq!(
            PgDate::from_iso_text("2024-02-29T00:00:00+00:00")
                .unwrap()
                .to_iso_text(),
            "2024-02-29"
        );
        assert!(PgDate::from_iso_text("2024-02-29T00:00:01.000Z").is_err());
        assert!(PgDate::from_iso_text("2024-02-29T00:00:00-08:00").is_err());
        assert!(PgDate::from_iso_text("2023-02-29").is_err());
        assert_eq!(
            PgDate::from_postgres_text("J2451545").unwrap(),
            PgDate::Finite(0)
        );
        assert_eq!(
            PgDate::from_postgres_text("0001-01-01 BC")
                .unwrap()
                .to_iso_text(),
            "0001-01-01 BC"
        );
        assert_eq!(
            PgDate::from_postgres_text("February 3, 2024")
                .unwrap()
                .to_iso_text(),
            "2024-02-03"
        );
        assert_eq!(
            PgDate::from_postgres_text("2024-034")
                .unwrap()
                .to_iso_text(),
            "2024-02-03"
        );
        assert_eq!(
            PgDate::from_postgres_text("4714-11-24 BC").unwrap(),
            PgDate::Finite(POSTGRES_DATE_MIN_DAYS)
        );
        assert_eq!(
            PgDate::from_postgres_text("5874897-12-31").unwrap(),
            PgDate::Finite(POSTGRES_DATE_MAX_DAYS)
        );
        assert!(PgDate::from_postgres_text("0000-01-01").is_err());
        assert!(PgDate::from_postgres_text("5874898-01-01").is_err());
        assert_eq!(
            PgTime::from_iso_text("23:59:58.123").unwrap(),
            PgTime::from_micros_since_midnight(86_398_123_000).unwrap()
        );
        assert_eq!(
            PgTimeTz::from_iso_text("23:59:58.123+02").unwrap(),
            PgTimeTz::new(
                PgTime::from_micros_since_midnight(86_398_123_000).unwrap(),
                7_200,
            )
            .unwrap()
        );
        let local = PgTimestamp::from_iso_text("2024-02-29 23:59:58.123", false).unwrap();
        let instant = PgTimestamp::from_iso_text("2024-02-29T23:59:58.123+02", true).unwrap();
        let (PgTimestamp::Finite(local), PgTimestamp::Finite(instant)) = (local, instant) else {
            panic!("finite timestamps expected");
        };
        assert_eq!(local - instant, 7_200_000_000);
        assert_eq!(
            PgInterval::from_postgres_text("1 year 2 mons 3 weeks 3 days 04:05:06.000007").unwrap(),
            PgInterval {
                months: 14,
                days: 24,
                micros: 14_706_000_007,
            }
        );
        for date in ["2000-01-01", "2024-02-29", "infinity", "-infinity"] {
            let parsed = PgDate::from_iso_text(date).unwrap();
            assert_eq!(
                PgDate::from_iso_text(&parsed.to_iso_text()).unwrap(),
                parsed
            );
        }
        for time in ["00:00:00", "03:04:05.000006", "24:00:00"] {
            let parsed = PgTime::from_iso_text(time).unwrap();
            assert_eq!(
                PgTime::from_iso_text(&parsed.to_iso_text()).unwrap(),
                parsed
            );
        }
        for timetz in ["03:04:05+02", "23:59:59.5-03:30:15"] {
            let parsed = PgTimeTz::from_iso_text(timetz).unwrap();
            assert_eq!(
                PgTimeTz::from_iso_text(&parsed.to_iso_text()).unwrap(),
                parsed
            );
        }
        for (timestamp, with_timezone) in [
            ("2000-01-01 00:00:00.000042", false),
            ("2024-02-29 21:59:58.123+00", true),
            ("infinity", true),
            ("-infinity", false),
        ] {
            let parsed = PgTimestamp::from_iso_text(timestamp, with_timezone).unwrap();
            assert_eq!(
                PgTimestamp::from_iso_text(&parsed.to_iso_text(with_timezone), with_timezone)
                    .unwrap(),
                parsed
            );
        }
        for interval in [
            PgInterval {
                months: 14,
                days: 24,
                micros: 14_706_000_007,
            },
            PgInterval {
                months: -1,
                days: 2,
                micros: -3,
            },
        ] {
            assert_eq!(
                PgInterval::from_postgres_text(&interval.to_postgres_text()).unwrap(),
                interval
            );
        }
    }

    #[test]
    fn interval_matches_postgresql_input_styles_typmods_arithmetic_and_extraction() {
        let expected = PgInterval {
            months: 14,
            days: 3,
            micros: 14_706_700_000,
        };
        for input in [
            "1 year 2 mons 3 days 04:05:06.7",
            "@ 1 year 2 mons 3 days 4 hours 5 mins 6.7 secs",
            "1-2 3 04:05:06.7",
            "P1Y2M3DT4H5M6.7S",
        ] {
            assert_eq!(PgInterval::from_postgres_text(input).unwrap(), expected);
        }
        assert_eq!(
            expected.to_postgres_text(),
            "1 year 2 mons 3 days 04:05:06.7"
        );
        assert_eq!(
            expected.to_postgres_verbose_text(),
            "@ 1 year 2 mons 3 days 4 hours 5 mins 6.7 secs"
        );
        assert_eq!(expected.to_sql_standard_text(), "+1-2 +3 +4:05:06.7");
        assert_eq!(expected.to_iso_8601_text(), "P1Y2M3DT4H5M6.7S");

        let negative = expected.checked_neg().unwrap();
        assert_eq!(
            negative.to_postgres_verbose_text(),
            "@ 1 year 2 mons 3 days 4 hours 5 mins 6.7 secs ago"
        );
        assert_eq!(negative.to_sql_standard_text(), "-1-2 -3 -4:05:06.7");
        assert_eq!(negative.to_iso_8601_text(), "P-1Y-2M-3DT-4H-5M-6.7S");

        let value = PgInterval::from_postgres_text("1 year 2 mons 3 days 04:05:06.555555").unwrap();
        assert_eq!(
            value
                .with_typmod(Some("YEAR"), None)
                .unwrap()
                .to_postgres_text(),
            "1 year"
        );
        assert_eq!(
            value
                .with_typmod(Some("YEAR TO MONTH"), None)
                .unwrap()
                .to_postgres_text(),
            "1 year 2 mons"
        );
        assert_eq!(
            value
                .with_typmod(Some("DAY"), None)
                .unwrap()
                .to_postgres_text(),
            "1 year 2 mons 3 days"
        );
        assert_eq!(
            value.with_typmod(None, Some(3)).unwrap().to_postgres_text(),
            "1 year 2 mons 3 days 04:05:06.556"
        );

        assert_eq!(
            PgInterval::from_postgres_text("1 mon")
                .unwrap()
                .checked_div(2.0)
                .unwrap()
                .to_postgres_text(),
            "15 days"
        );
        assert_eq!(
            PgInterval::from_postgres_text("1 mon")
                .unwrap()
                .checked_scale(1.5)
                .unwrap()
                .to_postgres_text(),
            "1 mon 15 days"
        );
        assert_eq!(
            PgInterval::from_postgres_text("1 mon 2 days 3 seconds")
                .unwrap()
                .extract_field("epoch")
                .as_deref(),
            Some("2764803.000000")
        );
        assert_eq!(
            PgInterval::from_postgres_text("1 mon -1 day -1 hour")
                .unwrap()
                .justify_interval()
                .unwrap()
                .to_postgres_text(),
            "28 days 23:00:00"
        );
        assert_eq!(
            PgInterval::from_postgres_text("1 mon")
                .unwrap()
                .comparison_micros(),
            PgInterval::from_postgres_text("30 days")
                .unwrap()
                .comparison_micros()
        );
    }

    #[test]
    fn bytes_and_bits_remain_distinct_and_lossless() {
        let bits = PgBitString::new(vec![0b1010_0000], 4).unwrap();
        assert_eq!(bits.bytes(), &[0b1010_0000]);
        assert_eq!(bits.bit_len(), 4);
        assert!(PgBitString::new(vec![0b1010_0001], 4).is_err());
        assert!(PgBitString::new(vec![0, 0], 4).is_err());
        assert_eq!(
            PgBitString::from_bit_text("001001").unwrap().bytes(),
            &[0b0010_0100]
        );
        assert_eq!(
            PgBitString::from_bit_text("001001").unwrap().to_bit_text(),
            "001001"
        );
        assert!(PgBitString::from_bit_text("102").is_err());
        assert_eq!(
            PgBitString::from_hex_text("aF").unwrap().to_bit_text(),
            "10101111"
        );
        assert_eq!(
            PgBitString::from_postgres_text("xAF")
                .unwrap()
                .to_bit_text(),
            "10101111"
        );
        assert_eq!(
            PgBitString::from_postgres_text("B001001")
                .unwrap()
                .to_bit_text(),
            "001001"
        );
        assert!(PgBitString::from_hex_text("ag").is_err());

        for (input, expected) in [
            ("\\x00ff10", vec![0, 255, 16]),
            ("hello", b"hello".to_vec()),
            ("\\001\\\\A", vec![1, b'\\', b'A']),
        ] {
            let parsed = parse_bytea_text(input).unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(
                parse_bytea_text(&format_bytea_hex(&parsed)).unwrap(),
                parsed
            );
        }
        assert!(parse_bytea_text("\\x0").is_err());
        assert!(parse_bytea_text("\\400").is_err());
        assert!(parse_bytea_text("\\777").is_err());
        assert_eq!(
            format_bytea_escape(&[0, b'\\', b'\'', b'A', 0x7f, 0xff, 0x80]),
            "\\000\\\\'A\\177\\377\\200"
        );
        assert_eq!(
            parse_bytea_text("\\000\\\\'A\\177\\377\\200").unwrap(),
            vec![0, b'\\', b'\'', b'A', 0x7f, 0xff, 0x80]
        );
    }

    #[test]
    fn temporal_and_network_values_validate_canonical_bounds() {
        assert!(PgTime::from_micros_since_midnight(MICROS_PER_DAY).is_ok());
        assert!(PgTime::from_micros_since_midnight(MICROS_PER_DAY + 1).is_err());
        let address = PgIpAddress::V4([192, 0, 2, 1]);
        assert!(PgNetwork::new(PgNetworkKind::Inet, address, 32).is_ok());
        assert!(PgNetwork::new(PgNetworkKind::Cidr, address, 33).is_err());
    }

    #[test]
    fn inet_input_matches_postgres_ipv4_ipv6_and_mask_spelling() {
        for (input, expected) in [
            ("192.0.2.1", "192.0.2.1/32"),
            ("192.000.002.001/024", "192.0.2.1/24"),
            ("001.002.003.004/00", "1.2.3.4/0"),
            ("2001:0DB8:0:0:0:0:0:1/64", "2001:db8::1/64"),
            ("::ffff:192.0.2.1/120", "::ffff:192.0.2.1/120"),
            ("::192.0.2.1", "::192.0.2.1/128"),
        ] {
            assert_eq!(
                PgNetwork::from_postgres_text(input, PgNetworkKind::Inet)
                    .unwrap()
                    .to_postgres_text(),
                expected,
                "input: {input}",
            );
        }
        for input in [
            "",
            " 192.0.2.1",
            "192.0.2.1 ",
            "192.0.2.1/33",
            "192.0.2.1/+1",
            "192.0.2.1/ 24",
            "192.168.1",
            "256.0.0.1",
            "2001:db8::1/064",
            "2001:db8::1/129",
            "fe80::1%eth0",
        ] {
            assert!(
                PgNetwork::from_postgres_text(input, PgNetworkKind::Inet).is_err(),
                "input unexpectedly accepted: {input}",
            );
        }
    }

    #[test]
    fn cidr_input_matches_postgres_abbreviated_networks_and_host_bit_validation() {
        for (input, expected) in [
            ("10", "10.0.0.0/8"),
            ("10/16", "10.0.0.0/16"),
            ("10.1", "10.1.0.0/16"),
            ("10.1/24", "10.1.0.0/24"),
            ("192.0.2", "192.0.2.0/24"),
            ("192.000.002.000/024", "192.0.2.0/24"),
            ("192.0.2.1", "192.0.2.1/32"),
            ("2001:0DB8:0:0:0:0:0:0/32", "2001:db8::/32"),
            ("::ffff:c000:200/120", "::ffff:192.0.2.0/120"),
        ] {
            assert_eq!(
                PgNetwork::from_postgres_text(input, PgNetworkKind::Cidr)
                    .unwrap()
                    .to_postgres_text(),
                expected,
                "input: {input}",
            );
        }
        for input in [
            "",
            " 192.0.2.0/24",
            "192.0.2.0/24 ",
            "192.0.2.1/24",
            "10.1/8",
            "192.0.2.0/33",
            "256.0.0.0/24",
            "2001:db8::1/64",
            "2001:db8::/064",
            "fe80::%eth0/64",
        ] {
            assert!(
                PgNetwork::from_postgres_text(input, PgNetworkKind::Cidr).is_err(),
                "input unexpectedly accepted: {input}",
            );
        }
    }

    #[test]
    fn mac_address_input_matches_postgres_notations_and_eui48_expansion() {
        for input in [
            "08:00:2b:01:02:03",
            "08-00-2b-01-02-03",
            "08002b:010203",
            "08002b-010203",
            "0800.2b01.0203",
            "0800-2b01-0203",
            "08002b010203",
            "8:0:2b:1:2:3",
            "08002b01023",
        ] {
            assert_eq!(
                PgMacAddress::from_postgres_text(input, false)
                    .unwrap()
                    .to_postgres_text(),
                "08:00:2b:01:02:03",
                "input: {input}",
            );
        }
        for input in [
            "08:00:2b:01:02:03",
            "0800:2b01:0203",
            "08.00.2b.01.02.03",
            "08002b010203",
        ] {
            assert_eq!(
                PgMacAddress::from_postgres_text(input, true)
                    .unwrap()
                    .to_postgres_text(),
                "08:00:2b:ff:fe:01:02:03",
                "input: {input}",
            );
        }
        assert_eq!(
            PgMacAddress::from_postgres_text("08-00-2b-01-02-03-04-05", true)
                .unwrap()
                .to_postgres_text(),
            "08:00:2b:01:02:03:04:05",
        );
        for (input, extended) in [
            ("08:00-2b:01:02:03", false),
            ("08.00.2b.01.02.03", false),
            ("08:00:2b:01:02:03:04:05", false),
            ("8:0:2b:1:2:3", true),
            ("08:00-2b:01:02:03", true),
            ("08:00:2b:01:02:03:04", true),
        ] {
            assert!(
                PgMacAddress::from_postgres_text(input, extended).is_err(),
                "input unexpectedly accepted: {input}",
            );
        }
        assert_eq!(
            PgMacAddress::from_postgres_text("100:00:2b:01:02:03", false),
            Err(PgCanonicalValueError::MacAddressOctetOutOfRange),
        );
    }

    #[test]
    fn postgres_special_text_parses_to_typed_values_and_rejects_invalid_input() {
        assert!(matches!(
            parse_pg_canonical_special("inet", "2001:db8::1/64").unwrap(),
            Some(PgCanonicalValue::Network(PgNetwork {
                kind: PgNetworkKind::Inet,
                prefix: 64,
                ..
            }))
        ));
        assert!(parse_pg_canonical_special("cidr", "192.0.2.1/24").is_err());
        assert!(matches!(
            parse_pg_canonical_special("macaddr8", "08:00:2b:01:02:03:04:05").unwrap(),
            Some(PgCanonicalValue::MacAddress(PgMacAddress::Mac64(_)))
        ));

        let Some(PgCanonicalValue::Range(range)) =
            parse_pg_canonical_special("numrange", "[0.10,9007199254740993.01)").unwrap()
        else {
            panic!("numeric range expected");
        };
        assert_eq!(range.subtype, "numeric");
        assert!(matches!(range.lower, PgRangeBound::Inclusive(_)));
        assert!(matches!(range.upper, PgRangeBound::Exclusive(_)));
        assert!(matches!(
            parse_pg_canonical_special("int4multirange", "{[1,3),[8,12)}").unwrap(),
            Some(PgCanonicalValue::Multirange(ranges)) if ranges.len() == 2
        ));
        let Some(PgCanonicalValue::Multirange(ranges)) =
            parse_pg_canonical_special("int4multirange", "{[8,10),[1,5),[4,9),empty}").unwrap()
        else {
            panic!("integer multirange expected");
        };
        assert_eq!(format_pg_multirange(&ranges), "{[1,10)}");
        let Some(PgCanonicalValue::Multirange(ranges)) =
            parse_pg_canonical_special("nummultirange", "{[1,2),(2,3)}").unwrap()
        else {
            panic!("numeric multirange expected");
        };
        assert_eq!(format_pg_multirange(&ranges), "{[1,2),(2,3)}");

        for (pg_type, text) in [
            ("point", "(1.5,-2)"),
            ("line", "{1,2,3}"),
            ("lseg", "[(0,0),(1,1)]"),
            ("box", "(0,0),(2,3)"),
            ("path", "[(0,0),(1,1)]"),
            ("polygon", "((0,0),(1,0),(0,1))"),
            ("circle", "<(1,2),3>"),
        ] {
            assert!(matches!(
                parse_pg_canonical_special(pg_type, text).unwrap(),
                Some(PgCanonicalValue::Geometric(_))
            ));
        }
        assert_eq!(
            parse_pg_canonical_special("pg_lsn", "16/B6C50").unwrap(),
            Some(PgCanonicalValue::Lsn(0x16_000B_6C50))
        );
        assert!(matches!(
            parse_pg_canonical_special("pg_snapshot", "10:20:11,14,19").unwrap(),
            Some(PgCanonicalValue::Snapshot(PgSnapshot {
                xmin: 10,
                xmax: 20,
                ..
            }))
        ));
        assert!(parse_pg_canonical_special("pg_snapshot", "10:20:14,11").is_err());
    }

    #[test]
    fn discrete_ranges_use_postgresql_canonical_bounds() {
        for (pg_type, input, expected) in [
            ("int4range", "[1,5]", "[1,6)"),
            ("int4range", "(1,5)", "[2,5)"),
            ("int4range", "(1,1]", "empty"),
            (
                "int8range",
                "[9007199254740993,9007199254741000]",
                "[9007199254740993,9007199254741001)",
            ),
            (
                "daterange",
                "[2024-01-01,2024-01-31]",
                "[2024-01-01,2024-02-01)",
            ),
            (
                "daterange",
                "(2024-01-01,2024-01-31)",
                "[2024-01-02,2024-01-31)",
            ),
            ("daterange", "[infinity,infinity]", "[infinity,infinity]"),
        ] {
            let Some(PgCanonicalValue::Range(range)) =
                parse_pg_canonical_special(pg_type, input).unwrap()
            else {
                panic!("{pg_type} did not parse as a range")
            };
            assert_eq!(range.to_postgres_text(), expected, "{pg_type} {input}");
        }

        assert_eq!(
            PgRange::from_postgres_text("[2,1)", "int4range"),
            Err(PgCanonicalValueError::InvalidRangeBounds)
        );
        assert_eq!(
            PgRange::from_postgres_text("(2147483647,)", "int4range"),
            Err(PgCanonicalValueError::RangeCanonicalOverflow("integer"))
        );
        assert_eq!(
            PgRange::from_postgres_text("(9223372036854775807,)", "int8range"),
            Err(PgCanonicalValueError::RangeCanonicalOverflow("bigint"))
        );
    }

    #[test]
    fn arrays_validate_dimensions_and_preserve_lower_bounds() {
        let dimensions = vec![
            PgArrayDimension {
                lower_bound: -2,
                length: 2,
            },
            PgArrayDimension {
                lower_bound: 4,
                length: 2,
            },
        ];
        let values = vec![
            PgCanonicalValue::Int4(1),
            PgCanonicalValue::Null,
            PgCanonicalValue::Int4(3),
            PgCanonicalValue::Int4(4),
        ];
        let array = PgArray::new("int4", dimensions.clone(), values).unwrap();
        assert_eq!(array.dimensions, dimensions);
        assert!(PgArray::new("int4", dimensions, vec![]).is_err());
    }

    #[test]
    fn recursive_values_and_snapshots_round_trip_without_json_coercion() {
        let snapshot = PgSnapshot::new(10, 20, vec![11, 14, 19]).unwrap();
        let value = PgCanonicalValue::Composite(PgComposite {
            type_oid: Some(42),
            type_name: "accounting_entry".to_string(),
            fields: vec![PgCompositeField {
                name: "amount".to_string(),
                pg_type: "numeric".to_string(),
                value: PgCanonicalValue::Numeric(PgNumeric::finite(true, "123450", 2).unwrap()),
            }],
        });
        for value in [value, PgCanonicalValue::Snapshot(snapshot)] {
            let encoded = serde_json::to_vec(&value).unwrap();
            assert_eq!(
                serde_json::from_slice::<PgCanonicalValue>(&encoded).unwrap(),
                value
            );
        }
        assert!(PgSnapshot::new(20, 10, vec![]).is_err());
        assert!(PgSnapshot::new(10, 20, vec![14, 11]).is_err());
    }

    #[test]
    fn oid_parser_matches_postgresql_c_integer_and_signed_input_rules() {
        for (input, expected) in [
            ("0", 0),
            ("+1", 1),
            ("00026", 22),
            ("0x1a", 26),
            ("037777777777", u32::MAX),
            ("4294967295", u32::MAX),
            ("-1", u32::MAX),
            ("-2147483648", 2_147_483_648),
        ] {
            assert_eq!(parse_pg_oid(input), Ok(expected), "{input}");
        }
        assert_eq!(parse_pg_oid("08"), Err(PgOidParseError::InvalidSyntax));
        assert_eq!(parse_pg_oid("4294967296"), Err(PgOidParseError::OutOfRange));
        assert_eq!(
            parse_pg_oid("-2147483649"),
            Err(PgOidParseError::OutOfRange)
        );
    }

    #[test]
    fn every_canonical_family_has_a_stable_serialized_carrier() {
        let zero = PgFloat8::from_value(0.0);
        let point = PgPoint { x: zero, y: zero };
        let values = vec![
            PgCanonicalValue::Null,
            PgCanonicalValue::Bool(true),
            PgCanonicalValue::Int2(-2),
            PgCanonicalValue::Int4(-4),
            PgCanonicalValue::Int8(-8),
            PgCanonicalValue::Float4(PgFloat4::from_value(-0.0)),
            PgCanonicalValue::Float8(PgFloat8::from_value(f64::NAN)),
            PgCanonicalValue::Numeric(PgNumeric::finite(false, "10", 1).unwrap()),
            PgCanonicalValue::Money(125),
            PgCanonicalValue::Text("text".to_string()),
            PgCanonicalValue::Bytes(vec![0, 255]),
            PgCanonicalValue::BitString(PgBitString::new(vec![0b1000_0000], 1).unwrap()),
            PgCanonicalValue::Date(PgDate::Finite(0)),
            PgCanonicalValue::Time(PgTime::from_micros_since_midnight(1).unwrap()),
            PgCanonicalValue::TimeTz(
                PgTimeTz::new(PgTime::from_micros_since_midnight(2).unwrap(), -3600).unwrap(),
            ),
            PgCanonicalValue::Timestamp(PgTimestamp::Finite(3)),
            PgCanonicalValue::TimestampTz(PgTimestamp::PositiveInfinity),
            PgCanonicalValue::Interval(PgInterval {
                months: 1,
                days: 2,
                micros: 3,
            }),
            PgCanonicalValue::Uuid([7; 16]),
            PgCanonicalValue::Json(serde_json::json!({"exact": 1})),
            PgCanonicalValue::Xml("<value/>".to_string()),
            PgCanonicalValue::JsonPath("$.value".to_string()),
            PgCanonicalValue::Network(
                PgNetwork::new(PgNetworkKind::Inet, PgIpAddress::V6([0; 16]), 128).unwrap(),
            ),
            PgCanonicalValue::MacAddress(PgMacAddress::Mac48([0; 6])),
            PgCanonicalValue::Geometric(PgGeometric::Circle {
                center: point,
                radius: PgFloat8::from_value(1.0),
            }),
            PgCanonicalValue::Range(PgRange {
                subtype: "int4".to_string(),
                empty: false,
                lower: PgRangeBound::Inclusive(Box::new(PgCanonicalValue::Int4(1))),
                upper: PgRangeBound::Exclusive(Box::new(PgCanonicalValue::Int4(2))),
            }),
            PgCanonicalValue::Multirange(vec![]),
            PgCanonicalValue::Array(PgArray::new("text", vec![], vec![]).unwrap()),
            PgCanonicalValue::Composite(PgComposite {
                type_oid: None,
                type_name: "record".to_string(),
                fields: vec![],
            }),
            PgCanonicalValue::Oid(26),
            PgCanonicalValue::OidAlias(PgOidAlias {
                oid: None,
                symbolic_name: Some("public.accounts".to_string()),
            }),
            PgCanonicalValue::Lsn(0x16B6_C50),
            PgCanonicalValue::Snapshot(PgSnapshot::new(1, 2, vec![1]).unwrap()),
            PgCanonicalValue::Vector(vec![PgFloat4::from_value(1.0)]),
        ];

        for value in values {
            let encoded = serde_json::to_vec(&value).unwrap();
            assert_eq!(
                serde_json::from_slice::<PgCanonicalValue>(&encoded).unwrap(),
                value
            );
        }
    }
}
