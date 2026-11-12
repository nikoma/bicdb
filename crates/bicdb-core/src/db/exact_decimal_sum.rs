//! Exact internal totals for native financial predicates.
//!
//! A valid Decimal input can need more than 96 coefficient bits when added to
//! another valid input. Rounding that intermediate makes SUM depend on row
//! order, even if the final total is zero. This accumulator does no rounding;
//! it does not change field admission or scalar arithmetic.
use super::{BicDbError, Result, Value};
use bigdecimal::{num_bigint::BigInt, BigDecimal};

#[derive(Default)]
pub(super) struct ExactDecimalSum(BigDecimal);

impl ExactDecimalSum {
    pub(super) fn add(&mut self, value: &Value) -> Result<()> {
        let value = crate::json_decimal_exact(value).ok_or_else(invalid_operand)?;
        self.0 += BigDecimal::new(BigInt::from(value.mantissa()), i64::from(value.scale()));
        Ok(())
    }

    pub(super) fn finish(self) -> Value {
        // Inputs have at most Decimal's 28 fractional digits. Plain output is
        // bounded by that scale and the accumulated integer coefficient, and
        // remains compatible with exact native Decimal predicate comparison.
        Value::String(self.0.to_plain_string())
    }
}

fn invalid_operand() -> BicDbError {
    BicDbError::CommitValidation("invariant decimal operand is invalid".to_string())
}
