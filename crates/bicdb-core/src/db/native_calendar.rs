//! Native invariant calendar operations. No timestamp, timezone or floating-point conversion.
use chrono::{Days, NaiveDate};
use serde_json::Value;

use crate::{BicDbError, Result};

fn date(value: &Value) -> Result<NaiveDate> {
    let text = value.as_str().ok_or_else(|| {
        BicDbError::CommitValidation("native calendar operand requires a Date".into())
    })?;
    NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| {
        BicDbError::CommitValidation("native calendar operand is not a valid ISO Date".into())
    })
}

pub(super) fn add_days(base: &Value, offset: &Value) -> Result<Value> {
    let base = date(base)?;
    let offset = offset.as_i64().ok_or_else(|| {
        BicDbError::CommitValidation("native calendar day offset requires an exact Int64".into())
    })?;
    // unsigned_abs handles i64::MIN without a negation overflow. Chrono checks
    // both the offset and its existing Date domain; it never clamps or wraps.
    let shifted = if offset >= 0 {
        base.checked_add_days(Days::new(offset as u64))
    } else {
        base.checked_sub_days(Days::new(offset.unsigned_abs()))
    }
    .ok_or_else(|| BicDbError::CommitValidation("native calendar DateAddDays overflowed".into()))?;
    Ok(Value::String(shifted.to_string()))
}

pub(super) fn difference_days(end: &Value, start: &Value) -> Result<Value> {
    Ok(Value::from(
        date(end)?.signed_duration_since(date(start)?).num_days(),
    ))
}

pub(super) fn compare(left: &Value, right: &Value) -> Result<std::cmp::Ordering> {
    Ok(date(left)?.cmp(&date(right)?))
}
