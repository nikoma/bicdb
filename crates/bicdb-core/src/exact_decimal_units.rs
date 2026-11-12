//! Bounded exact unit arithmetic. These leaves grant no model, tenant, source,
//! currency or posting authority and do not change ordinary Decimal operators.
use bigdecimal::num_bigint::BigInt;
use rust_decimal::Decimal;
use serde_json::Value;

use crate::{BicDbError, Result, MAX_EXACT_DECIMAL_LIST_SUM_ITEMS};

const MAX_UNITS_BITS: u64 = 254;
const MAX_UNITS_DIGITS: usize = 77;

fn invalid(operation: &str, reason: &str) -> BicDbError {
    BicDbError::CommitValidation(format!("{operation}: {reason}"))
}

fn int_scale(value: &Value, maximum: i64, operation: &str, label: &str) -> Result<u32> {
    let scale = value
        .as_i64()
        .ok_or_else(|| invalid(operation, &format!("{label} requires an exact Int64")))?;
    if !(0..=maximum).contains(&scale) {
        return Err(invalid(operation, &format!("{label} must be 0..{maximum}")));
    }
    Ok(scale as u32)
}

/// Convert an already admitted native Decimal without changing its value. A
/// subunit term is refused individually, even when another term would cancel it.
fn decimal_units(value: Decimal, scale: u32, operation: &str) -> Result<BigInt> {
    let coefficient = BigInt::from(value.mantissa());
    let exponent = i64::from(scale) - i64::from(value.scale());
    let ten = BigInt::from(10u8);
    if exponent >= 0 {
        Ok(coefficient * ten.pow(exponent as u32))
    } else {
        let divisor = ten.pow((-exponent) as u32);
        if &coefficient % &divisor != BigInt::from(0u8) {
            return Err(invalid(
                operation,
                "each Decimal must be integral at the declared unit scale",
            ));
        }
        Ok(coefficient / divisor)
    }
}

/// Streaming exact aggregate, shared by list and database execution. The list
/// wrapper retains the existing list cap; a database caller need not materialize
/// history. A u64 count and native terms at scale <=28 imply magnitude <2^254.
#[derive(Clone, Debug)]
pub struct ExactDecimalUnitsSum {
    scale: u32,
    count: u64,
    units: BigInt,
}

impl ExactDecimalUnitsSum {
    pub fn new(scale: i64) -> Result<Self> {
        if !(0..=28).contains(&scale) {
            return Err(invalid(
                "exact Decimal units sum",
                "unit scale must be 0..28",
            ));
        }
        Ok(Self {
            scale: scale as u32,
            count: 0,
            units: BigInt::from(0u8),
        })
    }

    pub fn add(&mut self, value: &Value) -> Result<()> {
        let operation = "exact Decimal units sum";
        let value = crate::json_decimal_exact(value).ok_or_else(|| {
            invalid(
                operation,
                "each item must be a nonnull exact native Decimal",
            )
        })?;
        let term = decimal_units(value, self.scale, operation)?;
        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| invalid(operation, "term count exceeds u64"))?;
        self.units += term;
        self.count = count;
        Ok(())
    }

    pub fn finish(self) -> Result<String> {
        if self.units.bits() > MAX_UNITS_BITS {
            return Err(invalid(
                "exact Decimal units sum",
                "final signed units magnitude must be below 2^254",
            ));
        }
        Ok(self.units.to_string())
    }
}

/// `(Decimal[], Int scale) -> String`. No null skipping, numeric scale coercion,
/// intermediate Decimal total, second rounding or list-cap increase.
pub fn exact_decimal_sum_units_value(arguments: &[Value]) -> Result<Value> {
    let operation = "exact Decimal units sum";
    if arguments.len() != 2 {
        return Err(invalid(operation, "requires exactly two arguments"));
    }
    let values = arguments[0]
        .as_array()
        .ok_or_else(|| invalid(operation, "values require a Decimal list"))?;
    if values.len() > MAX_EXACT_DECIMAL_LIST_SUM_ITEMS {
        return Err(invalid(operation, "list exceeds its bounded element count"));
    }
    let scale = int_scale(&arguments[1], 28, operation, "unit scale")?;
    let mut sum = ExactDecimalUnitsSum::new(i64::from(scale))?;
    for value in values {
        sum.add(value)?;
    }
    sum.finish().map(Value::String)
}

fn unsigned_units(value: &Value, operation: &str, label: &str) -> Result<BigInt> {
    let text = value
        .as_str()
        .ok_or_else(|| invalid(operation, &format!("{label} requires String")))?;
    let bytes = text.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_UNITS_DIGITS
        || (bytes.len() > 1 && bytes[0] == b'0')
        || !bytes.iter().all(u8::is_ascii_digit)
    {
        return Err(invalid(
            operation,
            &format!(
                "{label} requires canonical unsigned ASCII integer units of at most 77 digits"
            ),
        ));
    }
    let units = BigInt::parse_bytes(bytes, 10)
        .ok_or_else(|| invalid(operation, &format!("{label} contains invalid units")))?;
    if units.bits() > MAX_UNITS_BITS {
        return Err(invalid(
            operation,
            &format!("{label} magnitude must be below 2^254"),
        ));
    }
    Ok(units)
}

/// Both operands have already been validated nonnegative/positive. Quotient and
/// remainder stay wide; parity is exact and rounding occurs exactly once.
fn half_even_ratio(numerator: &BigInt, denominator: &BigInt) -> BigInt {
    let quotient = numerator / denominator;
    let remainder_twice = (numerator % denominator) * 2u8;
    if remainder_twice > *denominator
        || (remainder_twice == *denominator && &quotient % 2u8 != BigInt::from(0u8))
    {
        quotient + 1u8
    } else {
        quotient
    }
}

/// Exact admission of a nonnegative minor-unit coefficient. Removing trailing
/// zeros is normalization, not another rounding operation.
fn decimal_from_units(mut coefficient: BigInt, mut scale: u32, operation: &str) -> Result<Value> {
    while scale > 0 && &coefficient % 10u8 == BigInt::from(0u8) {
        coefficient /= 10u8;
        scale -= 1;
    }
    if coefficient.bits() > 96 {
        return Err(invalid(
            operation,
            "final amount cannot be represented without rounding",
        ));
    }
    let coefficient = coefficient
        .to_string()
        .parse::<i128>()
        .map_err(|_| invalid(operation, "final coefficient exceeds native Decimal"))?;
    Decimal::try_from_i128_with_scale(coefficient, scale)
        .map(|amount| Value::String(amount.to_string()))
        .map_err(|_| {
            invalid(
                operation,
                "final amount cannot be represented without rounding",
            )
        })
}

/// `(String C, String Q, Decimal operated, Int precision, Int scale) -> Decimal`.
/// Sequential remaining-state proration; this is not original-lot prefix
/// allocation. C,Q <2^254 and operated atoms <2^136 bound the product below 2^390.
pub fn exact_prorate_units_value(arguments: &[Value]) -> Result<Value> {
    let operation = "exact unit proration";
    if arguments.len() != 5 {
        return Err(invalid(operation, "requires exactly five scalar arguments"));
    }
    let total = unsigned_units(&arguments[0], operation, "total minor units")?;
    let quantity = unsigned_units(&arguments[1], operation, "remaining quantity atoms")?;
    let operated = crate::json_decimal_exact(&arguments[2]).ok_or_else(|| {
        invalid(
            operation,
            "operated quantity requires an exact native Decimal",
        )
    })?;
    let precision = int_scale(&arguments[3], 12, operation, "quantity precision")?;
    let scale = int_scale(&arguments[4], 8, operation, "amount scale")?;
    if quantity == BigInt::from(0u8) || operated <= Decimal::ZERO {
        return Err(invalid(
            operation,
            "remaining quantity and operated quantity must be positive",
        ));
    }
    let operated_atoms = decimal_units(operated, precision, operation)?;
    if operated_atoms > quantity {
        return Err(invalid(
            operation,
            "operated quantity exceeds remaining quantity",
        ));
    }
    // Do not skip any quantity, precision or capacity validation when C is zero.
    let numerator = total * operated_atoms;
    decimal_from_units(half_even_ratio(&numerator, &quantity), scale, operation)
}

/// `(String C, String Q, Int precision, Int scale) -> Decimal`. This explicitly
/// nonauthoritative derived-rate operation chooses the greatest native scale
/// whose once-rounded coefficient fits 96 bits. It does not change scalar `/` or
/// monetary proration's final-only exact admission. Wide inputs are new-domain
/// semantics, not a claim of ordinary Decimal-operand equivalence.
pub fn nearest_native_display_ratio_value(arguments: &[Value]) -> Result<Value> {
    let operation = "nearest native display ratio";
    if arguments.len() != 4 {
        return Err(invalid(operation, "requires exactly four scalar arguments"));
    }
    let total = unsigned_units(&arguments[0], operation, "total minor units")?;
    let quantity = unsigned_units(&arguments[1], operation, "quantity atoms")?;
    let precision = int_scale(&arguments[2], 12, operation, "quantity precision")?;
    let amount_scale = int_scale(&arguments[3], 8, operation, "amount scale")?;
    if quantity == BigInt::from(0u8) {
        return Err(invalid(operation, "quantity atoms must be positive"));
    }
    let ten = BigInt::from(10u8);
    let numerator = total * ten.pow(precision);
    let denominator = quantity * ten.pow(amount_scale);
    for scale in (0..=28u32).rev() {
        let coefficient = half_even_ratio(&(&numerator * ten.pow(scale)), &denominator);
        if coefficient.bits() <= 96 {
            return decimal_from_units(coefficient, scale, operation);
        }
    }
    Err(invalid(
        operation,
        "derived rate exceeds all supported native Decimal scales",
    ))
}
