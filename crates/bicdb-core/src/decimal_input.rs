//! Exact admission for the application's 96-bit Decimal scalar.
//!
//! Rust Decimal's ordinary FromStr intentionally rounds excess coefficient
//! digits and scientific mantissas. That is useful arithmetic behavior but
//! cannot validate a caller's money. Preserve accepted, exactly representable
//! syntax (including scientific notation and redundant zeros) and reject any
//! conversion that changes the numeric value.
use bigdecimal::BigDecimal;
use rust_decimal::Decimal;
use serde_json::Value;

pub fn parse_decimal_exact(text: &str) -> Result<Decimal, rust_decimal::Error> {
    if let Ok(value) = Decimal::from_str_exact(text) {
        return Ok(value);
    }
    let source = text
        .parse::<BigDecimal>()
        .map_err(|_| rust_decimal::Error::from("Decimal input has invalid exact numeric syntax"))?;
    let (coefficient, scale) = source.as_bigint_and_exponent();
    if coefficient.bits() == 0 {
        // Preserve ordinary zero's existing scale where possible. Zero never
        // requires materializing a power of ten, even for a huge exponent.
        return Ok(text.parse::<Decimal>().unwrap_or(Decimal::ZERO));
    }
    // Bound normalization before subtracting trailing-zero counts from scale.
    // A nonzero value with an exponent below -28 is already too large, while
    // at most the input's digit count can be removed from a positive scale.
    // This also avoids allocating enormous exponent expansions or overflowing
    // i64 at either end of the scientific-notation grammar.
    let maximum_reducible_scale = i64::try_from(text.len())
        .unwrap_or(i64::MAX)
        .saturating_add(28);
    if scale < -28 || scale > maximum_reducible_scale {
        return Err(rust_decimal::Error::from(
            "Decimal input cannot be represented exactly without rounding",
        ));
    }
    let normalized = source.normalized();
    let (coefficient, scale) = normalized.as_bigint_and_exponent();
    if coefficient.bits() > 96 || scale > 28 || scale < -28 {
        return Err(rust_decimal::Error::from(
            "Decimal input cannot be represented exactly without rounding",
        ));
    }
    if let Ok(candidate) = text.parse::<Decimal>() {
        let represented =
            BigDecimal::new(candidate.mantissa().into(), i64::from(candidate.scale()));
        if normalized == represented {
            return Ok(candidate);
        }
    }
    // A scientific mantissa can itself exceed 96 bits although its normalized
    // value is exact (MAX*10 followed by e-1). Construct only bounded plain text
    // or a bounded integer coefficient, then use the no-rounding constructor.
    if scale < 0 {
        Decimal::from_str_exact(&format!("{}{}", coefficient, "0".repeat((-scale) as usize)))
    } else {
        let coefficient = coefficient.to_string().parse::<i128>().map_err(|_| {
            rust_decimal::Error::from("Decimal coefficient exceeds its exact range")
        })?;
        Decimal::try_from_i128_with_scale(coefficient, scale as u32)
    }
}

pub fn json_decimal_exact(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(value) => parse_decimal_exact(value).ok(),
        Value::Number(value) => parse_decimal_exact(&value.to_string()).ok(),
        _ => None,
    }
}
