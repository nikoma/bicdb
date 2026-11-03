//! Dense scalar multiset comparison, multiplicity and bounded result allocation.
use super::*;

#[derive(Hash, PartialEq, Eq)]
enum Key {
    Null,
    Text(String),
    Number(BigInt, u32),
}
fn key(value: &JsonValue, element: &str, budget: &mut usize) -> Result<Key> {
    let result = if value.is_null() {
        Key::Null
    } else {
        let text = json_to_sql_value(value).to_cell();
        match element {
            "varchar" | "text" => Key::Text(text),
            "bpchar" | "char" => Key::Text(text.trim_end_matches(' ').into()),
            "numeric" | "int2" | "int4" | "int8" => {
                let mut number = parse_decimal(&text)?;
                if number.mantissa.is_zero() {
                    number.scale = 0;
                } else if number.scale > 0 {
                    let zeros = number
                        .mantissa
                        .to_str_radix(10)
                        .bytes()
                        .rev()
                        .take_while(|byte| *byte == b'0')
                        .count()
                        .min(number.scale as usize);
                    if zeros > 0 {
                        number.mantissa /= pow10_bigint(zeros as u32);
                        number.scale -= zeros as u32;
                    }
                }
                Key::Number(number.mantissa, number.scale)
            }
            _ => return Err(unsupported("multiset element type is not implemented")),
        }
    };
    let bytes = std::mem::size_of::<Key>()
        + match &result {
            Key::Null => 0,
            Key::Text(value) => value.len(),
            Key::Number(value, _) => value.bits().div_ceil(8) as usize,
        };
    *budget = budget.checked_sub(bytes).ok_or_else(limit)?;
    Ok(result)
}
const MAX_ELEMENTS: usize = 100_000;
const MAX_BYTES: usize = 8 * 1024 * 1024;
fn bytes(value: &JsonValue) -> Result<usize> {
    let payload = match value {
        JsonValue::Null => 0,
        JsonValue::String(text) => text.len(),
        JsonValue::Number(number) => number.to_string().len(),
        _ => {
            return Err(unsupported(
                "multisets require scalar character or NUMBER elements",
            ))
        }
    };
    std::mem::size_of::<JsonValue>()
        .checked_add(payload)
        .ok_or_else(limit)
}

fn limit() -> SqlError {
    SqlError::data_exception(
        "54000",
        "multiset operation exceeds platform element or byte limits",
        None,
    )
}

pub(crate) fn evaluate(args: &[SqlValue]) -> Result<SqlValue> {
    let [left, right, operation, element] = args else {
        return Err(unsupported("multiset helper requires four arguments"));
    };
    let operation = grammar::Operation::from_marker(&operation.to_cell())
        .ok_or_else(|| unsupported("unknown multiset operation"))?;
    if matches!(
        operation,
        grammar::Operation::IsEmpty | grammar::Operation::IsNotEmpty
    ) {
        return Err(unsupported("invalid multiset value operation"));
    }
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let left = dense_elements(left)?;
    let right = dense_elements(right)?;
    if left
        .len()
        .checked_add(right.len())
        .is_none_or(|count| count > MAX_ELEMENTS)
    {
        return Err(limit());
    }
    let mut total = 0usize;
    for value in left.iter().chain(right) {
        total = total.checked_add(bytes(value)?).ok_or_else(limit)?;
    }
    if total > MAX_BYTES {
        return Err(limit());
    }
    let element = element.to_cell();
    let mut key_budget = 16 * 1024 * 1024;
    let mut counts = std::collections::HashMap::<Key, usize>::new();
    if !matches!(
        operation,
        grammar::Operation::UnionAll | grammar::Operation::UnionDistinct
    ) {
        for value in right {
            *counts
                .entry(key(value, &element, &mut key_budget)?)
                .or_default() += 1;
        }
    }
    let mut seen = std::collections::HashSet::new();
    let mut output = Vec::new();
    output
        .try_reserve(left.len() + right.len())
        .map_err(|_| limit())?;
    match operation {
        grammar::Operation::UnionAll => output.extend(left.iter().chain(right).cloned()),
        grammar::Operation::UnionDistinct => {
            for value in left.iter().chain(right) {
                if seen.insert(key(value, &element, &mut key_budget)?) {
                    output.push(value.clone());
                }
            }
        }
        grammar::Operation::IntersectAll | grammar::Operation::IntersectDistinct => {
            for value in left {
                let k = key(value, &element, &mut key_budget)?;
                if let Some(count) = counts.get_mut(&k).filter(|count| **count > 0) {
                    output.push(value.clone());
                    if operation == grammar::Operation::IntersectAll {
                        *count -= 1;
                    } else {
                        *count = 0;
                    }
                }
            }
        }
        grammar::Operation::ExceptAll | grammar::Operation::ExceptDistinct => {
            for value in left {
                let k = key(value, &element, &mut key_budget)?;
                if operation == grammar::Operation::ExceptDistinct {
                    if !counts.contains_key(&k) && seen.insert(k) {
                        output.push(value.clone());
                    }
                } else if let Some(count) = counts.get_mut(&k).filter(|count| **count > 0) {
                    *count -= 1;
                } else {
                    output.push(value.clone());
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(SqlValue::Json(JsonValue::Array(output).into()))
}

pub(crate) fn is_empty(args: &[SqlValue]) -> Result<SqlValue> {
    let [value, SqlValue::Bool(negated)] = args else {
        return Err(unsupported(
            "IS EMPTY helper requires a collection and a predicate flag",
        ));
    };
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let empty = dense_elements(value)?.is_empty();
    Ok(SqlValue::Bool(if *negated { !empty } else { empty }))
}
