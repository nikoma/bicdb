//! Exact memory Decimal aggregates. Each item enters the native scalar domain
//! before widening; intermediate totals never round or enter that domain.
use bigdecimal::{num_bigint::BigInt, BigDecimal};
use serde_json::Value;

use crate::{BicDbError, Result};

pub const MAX_EXACT_DECIMAL_LIST_SUM_ITEMS: usize = 100_000;

pub fn exact_decimal_list_sum_value(value: &Value) -> Result<Value> {
    let invalid =
        |reason: &str| BicDbError::CommitValidation(format!("exact memory Decimal sum: {reason}"));
    let values = value.as_array().ok_or_else(|| invalid("requires a list"))?;
    if values.len() > MAX_EXACT_DECIMAL_LIST_SUM_ITEMS {
        return Err(invalid("list exceeds its bounded element count"));
    }
    let mut sum = BigDecimal::from(0);
    for value in values {
        if value.is_null() {
            continue;
        }
        let value = crate::json_decimal_exact(value)
            .ok_or_else(|| invalid("each nonnull item must be an exact native Decimal"))?;
        sum += BigDecimal::new(BigInt::from(value.mantissa()), i64::from(value.scale()));
    }
    crate::parse_decimal_exact(&sum.to_plain_string())
        .map(|value| Value::String(value.to_string()))
        .map_err(|_| invalid("final total cannot be represented without rounding"))
}
