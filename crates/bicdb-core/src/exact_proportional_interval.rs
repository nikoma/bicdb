//! Bounded exact proportional allocation. Absolute rounded prefixes stay as
//! integers; only their final difference enters the native Decimal domain.
use bigdecimal::num_bigint::BigInt;
use rust_decimal::Decimal;
use serde_json::Value;

use crate::{BicDbError, Result};

fn invalid(reason: &str) -> BicDbError {
    BicDbError::CommitValidation(format!("exact proportional interval: {reason}"))
}

fn endpoint(text: &str) -> Result<BigInt> {
    let bytes = text.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 41
        || (bytes.len() > 1 && bytes[0] == b'0')
        || !bytes.iter().all(u8::is_ascii_digit)
    {
        return Err(invalid(
            "endpoints require canonical unsigned ASCII integers of at most 41 digits",
        ));
    }
    BigInt::parse_bytes(bytes, 10).ok_or_else(|| invalid("invalid endpoint"))
}

/// Shared behavior/native-guard decoder: no lossy numeric or Int coercions.
pub fn exact_proportional_interval_value(arguments: &[Value]) -> Result<Value> {
    if arguments.len() != 6 {
        return Err(invalid("requires exactly six scalar arguments"));
    }
    let total = crate::json_decimal_exact(&arguments[0])
        .ok_or_else(|| invalid("total requires an exactly representable Decimal"))?;
    let quantity = crate::json_decimal_exact(&arguments[1])
        .ok_or_else(|| invalid("quantity requires an exactly representable Decimal"))?;
    let precision = arguments[2]
        .as_i64()
        .ok_or_else(|| invalid("quantity precision requires an exact Int64"))?;
    let start = arguments[3]
        .as_str()
        .ok_or_else(|| invalid("start endpoint requires String"))?;
    let end = arguments[4]
        .as_str()
        .ok_or_else(|| invalid("end endpoint requires String"))?;
    let scale = arguments[5]
        .as_i64()
        .ok_or_else(|| invalid("amount scale requires an exact Int64"))?;
    exact_proportional_interval(total, quantity, precision, start, end, scale)
        .map(|amount| Value::String(amount.to_string()))
}

/// Pure scalar calculation. This grants no posting, database or source authority.
/// The largest numerator is below 2^259 and denominator below 2^230, derived
/// from bounded native Decimal operands and endpoint <= actual quantity atoms.
pub fn exact_proportional_interval(
    total: Decimal,
    quantity: Decimal,
    quantity_precision: i64,
    start_atoms: &str,
    end_atoms: &str,
    amount_scale: i64,
) -> Result<Decimal> {
    if total < Decimal::ZERO || quantity <= Decimal::ZERO {
        return Err(invalid("total must be nonnegative and quantity positive"));
    }
    if !(0..=12).contains(&quantity_precision) || !(0..=8).contains(&amount_scale) {
        return Err(invalid(
            "quantity precision must be 0..12 and amount scale 0..8",
        ));
    }
    let ten = BigInt::from(10u8);
    let mut quantity_atoms = BigInt::from(quantity.mantissa());
    let precision_delta = quantity_precision - i64::from(quantity.scale());
    if precision_delta >= 0 {
        quantity_atoms *= ten.pow(precision_delta as u32);
    } else {
        let divisor = ten.pow((-precision_delta) as u32);
        if &quantity_atoms % &divisor != BigInt::from(0u8) {
            return Err(invalid("quantity is not exact at the declared precision"));
        }
        quantity_atoms /= divisor;
    }
    let start = endpoint(start_atoms)?;
    let end = endpoint(end_atoms)?;
    if start >= end || end > quantity_atoms {
        return Err(invalid(
            "endpoints require 0 <= start < end <= actual quantity atoms",
        ));
    }
    // Do not skip endpoint or quantity validation for a zero total.
    let coefficient = BigInt::from(total.mantissa());
    let multiplier = ten.pow(amount_scale as u32);
    let denominator = ten.pow(total.scale()) * quantity_atoms;
    let prefix = |endpoint: &BigInt| {
        let numerator = &coefficient * endpoint * &multiplier;
        let quotient = &numerator / &denominator;
        let remainder_twice = (&numerator % &denominator) * 2u8;
        if remainder_twice > denominator
            || (remainder_twice == denominator && &quotient % 2u8 != BigInt::from(0u8))
        {
            quotient + 1u8
        } else {
            quotient
        }
    };
    let mut difference = prefix(&end) - prefix(&start);
    let mut scale = amount_scale as u32;
    while scale > 0 && &difference % 10u8 == BigInt::from(0u8) {
        difference /= 10u8;
        scale -= 1;
    }
    if difference.bits() > 96 {
        return Err(invalid(
            "final amount cannot be represented without rounding",
        ));
    }
    let coefficient = difference
        .to_string()
        .parse::<i128>()
        .map_err(|_| invalid("final coefficient exceeds native Decimal"))?;
    Decimal::try_from_i128_with_scale(coefficient, scale)
        .map_err(|_| invalid("final amount cannot be represented without rounding"))
}
