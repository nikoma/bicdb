//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn eval_substring_expr<F>(
    expr: &Expr,
    substring_from: Option<&Expr>,
    substring_for: Option<&Expr>,
    mut eval: F,
    bytea: bool,
) -> Result<SqlValue>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    let Some(substring_from) = substring_from else {
        return Err(SqlError::InvalidSql(
            "substring requires a start expression".to_string(),
        ));
    };
    let mut args = vec![eval(expr)?, eval(substring_from)?];
    if let Some(substring_for) = substring_for {
        args.push(eval(substring_for)?);
    }
    if bytea {
        let arg_types = [Some("bytea".to_string())];
        return eval_bytea_function_value("substring", &args, Some(&arg_types))?
            .ok_or_else(|| SqlError::undefined_function("bytea substring overload is missing"));
    }
    substr_value(&args)
}

pub(crate) fn substr_value(args: &[SqlValue]) -> Result<SqlValue> {
    if !(args.len() == 2 || args.len() == 3) {
        return Err(SqlError::InvalidSql(format!(
            "substr expects 2 or 3 argument(s), got {}",
            args.len()
        )));
    }
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let value = args[0].to_cell();
    let start = sql_value_i64(&args[1])
        .ok_or_else(|| SqlError::InvalidSql("substr start must be an integer".to_string()))?;
    let start_offset = i128::from(start) - 1;
    let skip = if start_offset > 0 {
        usize::try_from(start_offset).unwrap_or(usize::MAX)
    } else {
        0
    };
    let take = if let Some(length) = args.get(2) {
        let length = sql_value_i64(length)
            .ok_or_else(|| SqlError::InvalidSql("substr length must be an integer".to_string()))?;
        if length < 0 {
            return Err(SqlError::data_exception(
                "22011",
                "negative substring length not allowed",
                None,
            ));
        }
        let visible = i128::from(length) + start_offset.min(0);
        if visible <= 0 {
            0
        } else {
            usize::try_from(visible).unwrap_or(usize::MAX)
        }
    } else {
        usize::MAX
    };
    Ok(SqlValue::String(
        value.chars().skip(skip).take(take).collect(),
    ))
}

pub(crate) fn eval_polymorphic_array_function_value(
    name: &str,
    args: &[SqlValue],
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    let value = match name {
        "array_append" => {
            require_arg_count(name, args, 2)?;
            let Some((mut values, lower_bounds)) = nullable_array_parts(&args[0], name)? else {
                return Ok(Some(array_value(vec![args[1].clone()])));
            };
            require_one_dimensional_array(
                &args[0],
                "22000",
                "argument must be empty or one-dimensional array",
            )?;
            values.push(args[1].clone());
            array_value_with_lower_bounds(values, lower_bounds)
        }
        "array_prepend" => {
            require_arg_count(name, args, 2)?;
            let Some((mut values, lower_bounds)) = nullable_array_parts(&args[1], name)? else {
                return Ok(Some(array_value(vec![args[0].clone()])));
            };
            require_one_dimensional_array(
                &args[1],
                "22000",
                "argument must be empty or one-dimensional array",
            )?;
            values.insert(0, args[0].clone());
            array_value_with_lower_bounds(values, lower_bounds)
        }
        "array_cat" => {
            require_arg_count(name, args, 2)?;
            concat_array_values(&args[0], &args[1])?
        }
        "array_remove" => {
            require_arg_count(name, args, 2)?;
            require_one_dimensional_array(
                &args[0],
                "0A000",
                "removing elements from multidimensional arrays is not supported",
            )?;
            let Some(values) = nullable_array_values(&args[0], name)? else {
                return Ok(Some(SqlValue::Null));
            };
            array_value(
                values
                    .into_iter()
                    .filter(|value| !values_not_distinct(value, &args[1]))
                    .collect(),
            )
        }
        "array_replace" => {
            require_arg_count(name, args, 3)?;
            require_one_dimensional_array(
                &args[0],
                "0A000",
                "replacing elements in multidimensional arrays is not supported",
            )?;
            let Some(values) = nullable_array_values(&args[0], name)? else {
                return Ok(Some(SqlValue::Null));
            };
            array_value(
                values
                    .into_iter()
                    .map(|value| {
                        if values_not_distinct(&value, &args[1]) {
                            args[2].clone()
                        } else {
                            value
                        }
                    })
                    .collect(),
            )
        }
        "array_position" => {
            if !(args.len() == 2 || args.len() == 3) {
                return Err(SqlError::InvalidSql(format!(
                    "array_position expects 2 or 3 argument(s), got {}",
                    args.len()
                )));
            }
            require_one_dimensional_array(
                &args[0],
                "0A000",
                "searching for elements in multidimensional arrays is not supported",
            )?;
            let Some((values, lower_bounds)) = nullable_array_parts(&args[0], name)? else {
                return Ok(Some(SqlValue::Null));
            };
            let lower = i64::from(lower_bounds.first().copied().unwrap_or(1));
            let start = array_search_start(args.get(2), lower)?;
            values
                .iter()
                .enumerate()
                .skip(usize::try_from(start.saturating_sub(lower)).unwrap_or(usize::MAX))
                .find_map(|(index, value)| {
                    values_not_distinct(value, &args[1])
                        .then(|| SqlValue::Int(index as i64 + lower))
                })
                .unwrap_or(SqlValue::Null)
        }
        "array_positions" => {
            require_arg_count(name, args, 2)?;
            require_one_dimensional_array(
                &args[0],
                "0A000",
                "searching for elements in multidimensional arrays is not supported",
            )?;
            let Some((values, lower_bounds)) = nullable_array_parts(&args[0], name)? else {
                return Ok(Some(SqlValue::Null));
            };
            let lower = i64::from(lower_bounds.first().copied().unwrap_or(1));
            array_value(
                values
                    .iter()
                    .enumerate()
                    .filter_map(|(index, value)| {
                        values_not_distinct(value, &args[1])
                            .then(|| SqlValue::Int(index as i64 + lower))
                    })
                    .collect(),
            )
        }
        "array_fill" => eval_array_fill(args)?,
        "trim_array" => eval_trim_array(args)?,
        "array_sample" => eval_array_sample(args)?,
        "array_shuffle" => eval_array_shuffle(args)?,
        "string_to_array" => eval_string_to_array(args)?,
        "bicdb_array_assign" => eval_array_assign(args)?,
        "cardinality" => {
            require_arg_count(name, args, 1)?;
            let Some((array, _)) = array_json_parts(&args[0], name)? else {
                return Ok(Some(SqlValue::Null));
            };
            SqlValue::Int(array_cardinality(array) as i64)
        }
        "array_ndims" => {
            require_arg_count(name, args, 1)?;
            let Some((array, _)) = array_json_parts(&args[0], name)? else {
                return Ok(Some(SqlValue::Null));
            };
            let dimensions = array_dimensions(array);
            if dimensions.is_empty() {
                SqlValue::Null
            } else {
                SqlValue::Int(dimensions.len() as i64)
            }
        }
        "array_dims" => {
            require_arg_count(name, args, 1)?;
            let Some((array, lower_bounds)) = array_json_parts(&args[0], name)? else {
                return Ok(Some(SqlValue::Null));
            };
            let dimensions = array_dimensions(array);
            if dimensions.is_empty() || dimensions.iter().any(|length| *length == 0) {
                SqlValue::Null
            } else {
                SqlValue::String(
                    dimensions
                        .into_iter()
                        .zip(lower_bounds)
                        .map(|(length, lower)| {
                            format!("[{lower}:{}]", i64::from(lower) + length as i64 - 1)
                        })
                        .collect(),
                )
            }
        }
        "array_length" | "array_lower" | "array_upper" => {
            require_arg_count(name, args, 2)?;
            let Some((array, lower_bounds)) = array_json_parts(&args[0], name)? else {
                return Ok(Some(SqlValue::Null));
            };
            if matches!(args[1], SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let dimension = sql_value_i64(&args[1]).ok_or_else(|| {
                SqlError::InvalidSql(format!("{name} dimension must be an integer"))
            })?;
            let dimension_index = usize::try_from(dimension - 1).ok();
            let length =
                dimension_index.and_then(|index| array_dimensions(array).get(index).copied());
            let lower_bound = dimension_index.and_then(|index| lower_bounds.get(index).copied());
            match (name, length, lower_bound) {
                (_, None | Some(0), _) => SqlValue::Null,
                ("array_lower", Some(_), Some(lower)) => SqlValue::Int(i64::from(lower)),
                ("array_upper", Some(length), Some(lower)) => {
                    SqlValue::Int(i64::from(lower) + length as i64 - 1)
                }
                ("array_length", Some(length), _) => SqlValue::Int(length as i64),
                _ => SqlValue::Null,
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

pub(crate) fn nullable_array_values(
    value: &SqlValue,
    function: &str,
) -> Result<Option<Vec<SqlValue>>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    array_like_values(value).map(Some).ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "{function} expects an array, got {}",
            value.to_cell()
        ))
    })
}

pub(crate) fn nullable_array_parts(
    value: &SqlValue,
    function: &str,
) -> Result<Option<(Vec<SqlValue>, Vec<i32>)>> {
    let Some((array, lower_bounds)) = array_json_parts(value, function)? else {
        return Ok(None);
    };
    let values = array
        .as_array()
        .expect("validated array value")
        .iter()
        .map(json_to_sql_value)
        .collect();
    Ok(Some((values, lower_bounds)))
}

pub(crate) fn require_one_dimensional_array(
    value: &SqlValue,
    sqlstate: &'static str,
    message: &str,
) -> Result<()> {
    if matches!(value, SqlValue::Null) {
        return Ok(());
    }
    let Some((array, _)) = array_json_parts(value, "array operation")? else {
        return Ok(());
    };
    if array_dimensions(array).len() > 1 {
        return Err(SqlError::data_exception(sqlstate, message, None));
    }
    Ok(())
}

pub(crate) fn concat_array_values(left: &SqlValue, right: &SqlValue) -> Result<SqlValue> {
    let left = array_json_parts(left, "array concatenation")?;
    let right = array_json_parts(right, "array concatenation")?;
    let clone_array = |array: &JsonValue, bounds: Vec<i32>| {
        array_json_value_with_lower_bounds(array.clone(), bounds)
    };
    let (left, left_bounds, right, right_bounds) = match (left, right) {
        (None, None) => return Ok(SqlValue::Null),
        (Some((array, bounds)), None) | (None, Some((array, bounds))) => {
            return Ok(clone_array(array, bounds));
        }
        (Some((left, left_bounds)), Some((right, right_bounds))) => {
            (left, left_bounds, right, right_bounds)
        }
    };
    let left_dimensions = array_dimensions(left);
    let right_dimensions = array_dimensions(right);
    if left_dimensions.is_empty() {
        return Ok(clone_array(right, right_bounds));
    }
    if right_dimensions.is_empty() {
        return Ok(clone_array(left, left_bounds));
    }
    let left_rank = left_dimensions.len();
    let right_rank = right_dimensions.len();
    if left_rank.abs_diff(right_rank) > 1 {
        return Err(SqlError::data_exception(
            "2202E",
            "cannot concatenate incompatible arrays",
            None,
        ));
    }
    let (mut output, bounds) = if left_rank == right_rank {
        if left_dimensions[1..] != right_dimensions[1..] {
            return Err(SqlError::data_exception(
                "2202E",
                "cannot concatenate incompatible arrays",
                None,
            ));
        }
        let mut output = left.as_array().expect("validated array").clone();
        output.extend(right.as_array().expect("validated array").iter().cloned());
        (output, left_bounds)
    } else if left_rank + 1 == right_rank {
        if left_dimensions != right_dimensions[1..] {
            return Err(SqlError::data_exception(
                "2202E",
                "cannot concatenate incompatible arrays",
                None,
            ));
        }
        let mut output = vec![left.clone()];
        output.extend(right.as_array().expect("validated array").iter().cloned());
        (output, right_bounds)
    } else {
        if right_dimensions != left_dimensions[1..] {
            return Err(SqlError::data_exception(
                "2202E",
                "cannot concatenate incompatible arrays",
                None,
            ));
        }
        let mut output = left.as_array().expect("validated array").clone();
        output.push(right.clone());
        (output, left_bounds)
    };
    Ok(array_json_value_with_lower_bounds(
        JsonValue::Array(std::mem::take(&mut output)),
        bounds,
    ))
}

pub(crate) fn array_value(values: Vec<SqlValue>) -> SqlValue {
    SqlValue::Json(JsonValue::Array(
        values.into_iter().map(sql_value_to_json).collect(),
    ))
}

pub(crate) fn array_value_with_lower_bounds(
    values: Vec<SqlValue>,
    lower_bounds: Vec<i32>,
) -> SqlValue {
    let value = JsonValue::Array(values.into_iter().map(sql_value_to_json).collect());
    array_json_value_with_lower_bounds(value, lower_bounds)
}

pub(crate) fn array_json_value_with_lower_bounds(
    value: JsonValue,
    lower_bounds: Vec<i32>,
) -> SqlValue {
    if value.as_array().is_some_and(Vec::is_empty) || lower_bounds.iter().all(|lower| *lower == 1) {
        SqlValue::Json(value)
    } else {
        SqlValue::Json(serde_json::json!({
            "$bicdb_array_input": {
                "lower_bounds": lower_bounds,
                "value": value,
            }
        }))
    }
}

pub(crate) fn array_search_start(value: Option<&SqlValue>, lower: i64) -> Result<i64> {
    let Some(value) = value else {
        return Ok(lower);
    };
    if matches!(value, SqlValue::Null) {
        return Err(SqlError::data_exception(
            "22004",
            "initial position must not be null",
            None,
        ));
    }
    let start = sql_value_i64(value)
        .ok_or_else(|| SqlError::InvalidSql("array position must be an integer".to_string()))?;
    Ok(start.max(lower))
}

pub(crate) fn array_integer_vector(value: &SqlValue, function: &str) -> Result<Vec<i32>> {
    let values = nullable_array_values(value, function)?.ok_or_else(|| {
        SqlError::data_exception(
            "22004",
            format!("{function} dimensions must not be null"),
            None,
        )
    })?;
    values
        .into_iter()
        .map(|value| {
            sql_value_i64(&value)
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| SqlError::InvalidSql(format!("{function} dimensions must be int4")))
        })
        .collect()
}

pub(crate) fn filled_array_json(value: &SqlValue, dimensions: &[i32]) -> Result<JsonValue> {
    let Some((&length, remaining)) = dimensions.split_first() else {
        return Ok(sql_value_to_json(value.clone()));
    };
    if length < 0 {
        return Err(SqlError::data_exception(
            "2202E",
            "array size exceeds the maximum allowed",
            None,
        ));
    }
    let length = usize::try_from(length).unwrap_or_default();
    let child = filled_array_json(value, remaining)?;
    Ok(JsonValue::Array(vec![child; length]))
}

/// Upper bound on the number of elements any single array may hold, shared
/// by `array_fill` and subscript assignment.
pub(crate) const MAX_ARRAY_CARDINALITY: usize = 1_000_000;

/// Elements an array subscript assignment may materialize.
///
/// `arr[2000000000] = 9` on a two-element array asks for two billion
/// `Null`s — the subscript is client-supplied and the span between it and
/// the existing bounds becomes an allocation. A count derived from wire
/// input must never size an allocation directly
/// (`bicdb_core::parse_budget`), and the same 1M ceiling `array_fill`
/// already enforces applies here.
pub(crate) fn checked_array_expansion_span(new_lower: i64, new_upper: i64) -> Result<usize> {
    let too_large =
        || SqlError::data_exception("54000", "array size exceeds the maximum allowed", None);
    let span = new_upper
        .checked_sub(new_lower)
        .and_then(|span| span.checked_add(1))
        .ok_or_else(too_large)?;
    let span = usize::try_from(span).map_err(|_| too_large())?;
    if span > MAX_ARRAY_CARDINALITY {
        return Err(too_large());
    }
    Ok(span)
}

pub(crate) fn eval_array_fill(args: &[SqlValue]) -> Result<SqlValue> {
    if !(args.len() == 2 || args.len() == 3) {
        return Err(SqlError::InvalidSql(format!(
            "array_fill expects 2 or 3 argument(s), got {}",
            args.len()
        )));
    }
    if matches!(args[1], SqlValue::Null)
        || args
            .get(2)
            .is_some_and(|value| matches!(value, SqlValue::Null))
    {
        return Ok(SqlValue::Null);
    }
    let dimensions = array_integer_vector(&args[1], "array_fill")?;
    if dimensions.len() > 6 {
        return Err(SqlError::data_exception(
            "54000",
            "number of array dimensions exceeds the maximum allowed (6)",
            None,
        ));
    }
    let cardinality = dimensions.iter().try_fold(1usize, |total, dimension| {
        usize::try_from(*dimension)
            .ok()
            .and_then(|dimension| total.checked_mul(dimension))
    });
    if cardinality.is_none_or(|cardinality| cardinality > MAX_ARRAY_CARDINALITY) {
        return Err(SqlError::data_exception(
            "54000",
            "array size exceeds the maximum allowed",
            None,
        ));
    }
    let lower_bounds = if let Some(value) = args.get(2) {
        let lower = array_integer_vector(value, "array_fill")?;
        if lower.len() != dimensions.len() {
            return Err(SqlError::InvalidSql(
                "wrong number of array subscripts".to_string(),
            ));
        }
        lower
    } else {
        vec![1; dimensions.len()]
    };
    let value = filled_array_json(&args[0], &dimensions)?;
    let values = value
        .as_array()
        .expect("array_fill dimensions produce an array")
        .iter()
        .map(json_to_sql_value)
        .collect();
    Ok(array_value_with_lower_bounds(values, lower_bounds))
}

pub(crate) fn eval_trim_array(args: &[SqlValue]) -> Result<SqlValue> {
    require_arg_count("trim_array", args, 2)?;
    if matches!(args[0], SqlValue::Null) || matches!(args[1], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let (mut values, lower_bounds) =
        nullable_array_parts(&args[0], "trim_array")?.expect("non-null array was validated");
    let count = sql_value_i64(&args[1])
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            SqlError::InvalidSql("number of elements to trim must not be negative".to_string())
        })?;
    if count > values.len() {
        return Err(SqlError::InvalidSql(
            "number of elements to trim must be between 0 and the first array dimension"
                .to_string(),
        ));
    }
    values.truncate(values.len() - count);
    Ok(array_value_with_lower_bounds(values, lower_bounds))
}

pub(crate) fn shuffle_array_values(values: &mut [SqlValue]) {
    for index in (1..values.len()).rev() {
        let selected = (sql_random_value() * (index + 1) as f64) as usize;
        values.swap(index, selected.min(index));
    }
}

pub(crate) fn eval_array_shuffle(args: &[SqlValue]) -> Result<SqlValue> {
    require_arg_count("array_shuffle", args, 1)?;
    let Some((mut values, lower_bounds)) = nullable_array_parts(&args[0], "array_shuffle")? else {
        return Ok(SqlValue::Null);
    };
    shuffle_array_values(&mut values);
    Ok(array_value_with_lower_bounds(values, lower_bounds))
}

pub(crate) fn eval_array_sample(args: &[SqlValue]) -> Result<SqlValue> {
    require_arg_count("array_sample", args, 2)?;
    if matches!(args[1], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let Some((mut values, lower_bounds)) = nullable_array_parts(&args[0], "array_sample")? else {
        return Ok(SqlValue::Null);
    };
    let count = sql_value_i64(&args[1])
        .and_then(|value| usize::try_from(value).ok())
        .filter(|count| *count <= values.len())
        .ok_or_else(|| {
            SqlError::InvalidSql(
                "sample size must be between 0 and the first array dimension".to_string(),
            )
        })?;
    shuffle_array_values(&mut values);
    values.truncate(count);
    Ok(array_value_with_lower_bounds(values, lower_bounds))
}

pub(crate) fn eval_string_to_array(args: &[SqlValue]) -> Result<SqlValue> {
    if !(args.len() == 2 || args.len() == 3) {
        return Err(SqlError::InvalidSql(format!(
            "string_to_array expects 2 or 3 argument(s), got {}",
            args.len()
        )));
    }
    let Some(input) = sql_value_text(&args[0]) else {
        return Ok(SqlValue::Null);
    };
    let null_marker = args.get(2).and_then(sql_value_text);
    let parts = match sql_value_text(&args[1]) {
        Some(delimiter) if delimiter.is_empty() => vec![input],
        Some(delimiter) => input.split(&delimiter).map(str::to_string).collect(),
        None if matches!(args[1], SqlValue::Null) => {
            input.chars().map(|ch| ch.to_string()).collect()
        }
        None => return Ok(SqlValue::Null),
    };
    Ok(array_value(
        parts
            .into_iter()
            .map(|part| {
                if null_marker.as_deref() == Some(part.as_str()) {
                    SqlValue::Null
                } else {
                    SqlValue::String(part)
                }
            })
            .collect(),
    ))
}

pub(crate) fn nullable_subscript_vector(
    value: &SqlValue,
    function: &str,
) -> Result<Vec<Option<i64>>> {
    nullable_array_values(value, function)?
        .ok_or_else(|| SqlError::InvalidSql(format!("{function} subscripts must not be null")))?
        .into_iter()
        .map(|value| {
            if matches!(value, SqlValue::Null) {
                Ok(None)
            } else {
                sql_value_i64(&value).map(Some).ok_or_else(|| {
                    SqlError::InvalidSql(format!("{function} subscripts must be integers"))
                })
            }
        })
        .collect()
}

pub(crate) fn array_set_existing_nd(
    value: &mut JsonValue,
    lower_bounds: &[i32],
    indices: &[i64],
    replacement: JsonValue,
) -> Result<()> {
    let Some((&index, remaining)) = indices.split_first() else {
        *value = replacement;
        return Ok(());
    };
    let dimension = lower_bounds.len() - indices.len();
    let lower = i64::from(lower_bounds[dimension]);
    let values = value
        .as_array_mut()
        .ok_or_else(|| SqlError::InvalidSql("wrong number of array subscripts".to_string()))?;
    let offset = index
        .checked_sub(lower)
        .and_then(|offset| usize::try_from(offset).ok());
    let child = offset
        .and_then(|offset| values.get_mut(offset))
        .ok_or_else(|| SqlError::data_exception("2202E", "array subscript out of range", None))?;
    array_set_existing_nd(child, lower_bounds, remaining, replacement)
}

pub(crate) fn array_set_slice_nd(
    value: &mut JsonValue,
    selections: &[(usize, usize)],
    dimension: usize,
    replacements: &[JsonValue],
    replacement_offset: &mut usize,
) -> Result<()> {
    let values = value
        .as_array_mut()
        .ok_or_else(|| SqlError::InvalidSql("wrong number of array subscripts".to_string()))?;
    let (start, length) = selections[dimension];
    for value in values.iter_mut().skip(start).take(length) {
        if dimension + 1 == selections.len() {
            *value = replacements
                .get(*replacement_offset)
                .cloned()
                .ok_or_else(|| {
                    SqlError::InvalidSql("source array too small for destination slice".to_string())
                })?;
            *replacement_offset += 1;
        } else {
            array_set_slice_nd(
                value,
                selections,
                dimension + 1,
                replacements,
                replacement_offset,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn eval_array_assign(args: &[SqlValue]) -> Result<SqlValue> {
    require_arg_count("bicdb_array_assign", args, 5)?;
    let lower = nullable_subscript_vector(&args[1], "array assignment")?;
    let upper = nullable_subscript_vector(&args[2], "array assignment")?;
    let slices = nullable_array_values(&args[3], "array assignment")?
        .ok_or_else(|| SqlError::InvalidSql("array assignment modes must not be null".to_string()))?
        .into_iter()
        .map(|value| {
            sql_value_bool(&value).ok_or_else(|| {
                SqlError::InvalidSql("array assignment modes must be boolean".to_string())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if lower.len() != upper.len() || lower.len() != slices.len() || lower.is_empty() {
        return Err(SqlError::InvalidSql(
            "wrong number of array subscripts".to_string(),
        ));
    }
    if slices.iter().any(|slice| *slice) {
        let Some((current, current_bounds)) = array_json_parts(&args[0], "array assignment")?
        else {
            return Err(SqlError::InvalidSql(
                "cannot assign a slice to a null array".to_string(),
            ));
        };
        let (replacement, _) =
            array_json_parts(&args[4], "array assignment")?.ok_or_else(|| {
                SqlError::InvalidSql("array slice assignment requires an array".to_string())
            })?;
        if slices.len() != current_bounds.len() {
            return Err(SqlError::InvalidSql(
                "wrong number of array subscripts".to_string(),
            ));
        }
        if slices.len() > 1 {
            let dimensions = array_dimensions(current);
            let mut selections = Vec::with_capacity(dimensions.len());
            let mut destination_cardinality = 1_usize;
            for dimension in 0..dimensions.len() {
                let array_lower = i64::from(current_bounds[dimension]);
                let array_upper = array_lower + dimensions[dimension] as i64 - 1;
                let start = lower[dimension].unwrap_or(array_lower);
                let end = upper[dimension].unwrap_or(array_upper);
                if start < array_lower || end > array_upper {
                    return Err(SqlError::data_exception(
                        "2202E",
                        "array subscript out of range",
                        None,
                    ));
                }
                if end < start {
                    return Err(SqlError::data_exception(
                        "2202E",
                        "upper bound cannot be less than lower bound",
                        None,
                    ));
                }
                let length = usize::try_from(end - start + 1).map_err(|_| {
                    SqlError::numeric_value_out_of_range("array slice is too large")
                })?;
                destination_cardinality =
                    destination_cardinality.checked_mul(length).ok_or_else(|| {
                        SqlError::numeric_value_out_of_range("array slice is too large")
                    })?;
                selections.push((
                    usize::try_from(start - array_lower).expect("validated array offset"),
                    length,
                ));
            }
            let mut replacement_values = Vec::new();
            flatten_array_json(replacement, &mut replacement_values);
            if replacement_values.len() != destination_cardinality {
                return Err(SqlError::InvalidSql(
                    "source array too small for destination slice".to_string(),
                ));
            }
            let replacements = replacement_values.into_iter().cloned().collect::<Vec<_>>();
            let mut result = current.clone();
            let mut replacement_offset = 0;
            array_set_slice_nd(
                &mut result,
                &selections,
                0,
                &replacements,
                &mut replacement_offset,
            )?;
            return Ok(array_value_with_lower_bounds(
                result
                    .as_array()
                    .expect("validated array")
                    .iter()
                    .map(json_to_sql_value)
                    .collect(),
                current_bounds,
            ));
        }
        let mut values = current.as_array().expect("validated array").clone();
        let replacement = replacement.as_array().expect("validated array");
        let array_lower = i64::from(current_bounds[0]);
        let array_upper = array_lower + values.len() as i64 - 1;
        let start = lower[0].unwrap_or(array_lower);
        let end = upper[0].unwrap_or(array_upper);
        if end < start || usize::try_from(end - start + 1).ok() != Some(replacement.len()) {
            return Err(SqlError::InvalidSql(
                "source array too small for destination slice".to_string(),
            ));
        }
        let new_lower = array_lower.min(start);
        let new_upper = array_upper.max(end);
        let span = checked_array_expansion_span(new_lower, new_upper)?;
        let mut expanded = vec![JsonValue::Null; span];
        for (offset, value) in values.drain(..).enumerate() {
            expanded[usize::try_from(array_lower - new_lower).unwrap() + offset] = value;
        }
        for (offset, value) in replacement.iter().enumerate() {
            expanded[usize::try_from(start - new_lower).unwrap() + offset] = value.clone();
        }
        return Ok(array_value_with_lower_bounds(
            expanded.iter().map(json_to_sql_value).collect(),
            vec![i32::try_from(new_lower).map_err(|_| {
                SqlError::numeric_value_out_of_range("array lower bound is out of range")
            })?],
        ));
    }
    let indices = lower
        .iter()
        .zip(&upper)
        .map(|(lower, upper)| match (lower, upper) {
            (Some(lower), Some(upper)) if lower == upper => Ok(*lower),
            _ => Err(SqlError::InvalidSql(
                "array subscript must not be null".to_string(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    if indices.len() == 1 {
        let index = indices[0];
        let (mut values, mut bounds) = match nullable_array_parts(&args[0], "array assignment")? {
            Some(parts) => parts,
            None => (
                Vec::new(),
                vec![i32::try_from(index).map_err(|_| {
                    SqlError::numeric_value_out_of_range("array subscript is out of range")
                })?],
            ),
        };
        if bounds.is_empty() {
            bounds.push(i32::try_from(index).map_err(|_| {
                SqlError::numeric_value_out_of_range("array subscript is out of range")
            })?);
        }
        let old_lower = i64::from(bounds.first().copied().unwrap_or(1));
        let old_upper = old_lower + values.len() as i64 - 1;
        let new_lower = if values.is_empty() {
            index
        } else {
            old_lower.min(index)
        };
        let new_upper = if values.is_empty() {
            index
        } else {
            old_upper.max(index)
        };
        let span = checked_array_expansion_span(new_lower, new_upper)?;
        let mut expanded = vec![SqlValue::Null; span];
        for (offset, value) in values.drain(..).enumerate() {
            expanded[usize::try_from(old_lower - new_lower).unwrap() + offset] = value;
        }
        expanded[usize::try_from(index - new_lower).unwrap()] = args[4].clone();
        bounds[0] = i32::try_from(new_lower).map_err(|_| {
            SqlError::numeric_value_out_of_range("array lower bound is out of range")
        })?;
        return Ok(array_value_with_lower_bounds(expanded, bounds));
    }
    let Some((array, bounds)) = array_json_parts(&args[0], "array assignment")? else {
        return Err(SqlError::InvalidSql(
            "cannot assign to a null multidimensional array".to_string(),
        ));
    };
    if indices.len() != bounds.len() {
        return Err(SqlError::InvalidSql(
            "wrong number of array subscripts".to_string(),
        ));
    }
    let mut array = array.clone();
    array_set_existing_nd(
        &mut array,
        &bounds,
        &indices,
        sql_value_to_json(args[4].clone()),
    )?;
    Ok(array_value_with_lower_bounds(
        array
            .as_array()
            .expect("validated array")
            .iter()
            .map(json_to_sql_value)
            .collect(),
        bounds,
    ))
}

pub(crate) fn array_json_parts<'a>(
    value: &'a SqlValue,
    function: &str,
) -> Result<Option<(&'a JsonValue, Vec<i32>)>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    match value {
        SqlValue::Json(value @ JsonValue::Array(_)) => {
            Ok(Some((value, vec![1; array_dimensions(value).len()])))
        }
        SqlValue::Json(JsonValue::Object(object)) => {
            let input = object
                .get("$bicdb_array_input")
                .and_then(JsonValue::as_object)
                .ok_or_else(|| {
                    SqlError::InvalidSql(format!(
                        "{function} expects an array, got {}",
                        value.to_cell()
                    ))
                })?;
            let array = input
                .get("value")
                .filter(|value| value.is_array())
                .ok_or_else(|| SqlError::InvalidSql("invalid internal array value".to_string()))?;
            let lower_bounds = serde_json::from_value::<Vec<i32>>(
                input.get("lower_bounds").cloned().ok_or_else(|| {
                    SqlError::InvalidSql("invalid internal array lower bounds".to_string())
                })?,
            )
            .map_err(|_| SqlError::InvalidSql("invalid internal array lower bounds".to_string()))?;
            if lower_bounds.len() != array_dimensions(array).len() {
                return Err(SqlError::InvalidSql(
                    "internal array lower bounds do not match array rank".to_string(),
                ));
            }
            Ok(Some((array, lower_bounds)))
        }
        _ => Err(SqlError::InvalidSql(format!(
            "{function} expects an array, got {}",
            value.to_cell()
        ))),
    }
}

pub(crate) fn array_dimensions(value: &JsonValue) -> Vec<usize> {
    let JsonValue::Array(values) = value else {
        return Vec::new();
    };
    if values.is_empty() {
        return Vec::new();
    }
    let mut dimensions = vec![values.len()];
    if let Some(first) = values.first() {
        dimensions.extend(array_dimensions(first));
    }
    dimensions
}

pub(crate) fn array_cardinality(value: &JsonValue) -> usize {
    match value {
        JsonValue::Array(values) => values.iter().map(array_cardinality).sum(),
        _ => 1,
    }
}

pub(crate) fn eval_position_typed_value(
    needle: SqlValue,
    haystack: SqlValue,
    bytea: bool,
) -> Result<SqlValue> {
    if matches!(needle, SqlValue::Null) || matches!(haystack, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    if bytea {
        let needle = bytea_argument(&needle)?.unwrap();
        let haystack = bytea_argument(&haystack)?.unwrap();
        let position = if needle.is_empty() {
            1
        } else {
            haystack
                .windows(needle.len())
                .position(|window| window == needle)
                .map(|index| index as i64 + 1)
                .unwrap_or(0)
        };
        return Ok(SqlValue::Int(position));
    }
    let needle = needle.to_cell();
    let haystack = haystack.to_cell();
    let position = haystack
        .find(&needle)
        .map(|index| haystack[..index].chars().count() as i64 + 1)
        .unwrap_or(0);
    Ok(SqlValue::Int(position))
}

pub(crate) fn eval_overlay_expr<F>(
    expr: &Expr,
    overlay_what: &Expr,
    overlay_from: &Expr,
    overlay_for: Option<&Expr>,
    mut eval: F,
    bytea: bool,
) -> Result<SqlValue>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    let value = eval(expr)?;
    let replacement = eval(overlay_what)?;
    let start = eval(overlay_from)?;
    let length = overlay_for.map(&mut eval).transpose()?;
    if matches!(value, SqlValue::Null)
        || matches!(replacement, SqlValue::Null)
        || matches!(start, SqlValue::Null)
        || length
            .as_ref()
            .is_some_and(|value| matches!(value, SqlValue::Null))
    {
        return Ok(SqlValue::Null);
    }
    let start = sql_value_i64(&start)
        .ok_or_else(|| SqlError::InvalidSql("overlay start must be an integer".to_string()))?;
    if start <= 0 {
        return Err(SqlError::data_exception(
            "22011",
            "negative substring length not allowed",
            None,
        ));
    }

    if bytea {
        let bytes = bytea_argument(&value)?.unwrap();
        let replacement = bytea_argument(&replacement)?.unwrap();
        let remove = match length.as_ref() {
            Some(value) => sql_value_i64(value).ok_or_else(|| {
                SqlError::InvalidSql("overlay length must be an integer".to_string())
            })?,
            None => replacement.len() as i64,
        };
        if remove < 0 {
            return Err(SqlError::data_exception(
                "22011",
                "negative substring length not allowed",
                None,
            ));
        }
        let split = usize::try_from(start - 1)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        let suffix = split
            .saturating_add(usize::try_from(remove).unwrap_or(usize::MAX))
            .min(bytes.len());
        let mut output = Vec::with_capacity(bytes.len() + replacement.len());
        output.extend_from_slice(&bytes[..split]);
        output.extend_from_slice(&replacement);
        output.extend_from_slice(&bytes[suffix..]);
        return Ok(SqlValue::String(format_bytea_hex(&output)));
    }

    let value = value.to_cell().chars().collect::<Vec<_>>();
    let replacement = replacement.to_cell().chars().collect::<Vec<_>>();
    let remove = match length.as_ref() {
        Some(value) => sql_value_i64(value)
            .ok_or_else(|| SqlError::InvalidSql("overlay length must be an integer".to_string()))?,
        None => replacement.len() as i64,
    };
    if remove < 0 {
        return Err(SqlError::data_exception(
            "22011",
            "negative substring length not allowed",
            None,
        ));
    }
    let split = usize::try_from(start - 1)
        .unwrap_or(usize::MAX)
        .min(value.len());
    let suffix = split
        .saturating_add(usize::try_from(remove).unwrap_or(usize::MAX))
        .min(value.len());
    Ok(SqlValue::String(
        value[..split]
            .iter()
            .chain(&replacement)
            .chain(&value[suffix..])
            .collect(),
    ))
}

pub(crate) fn pg_type_regtype_name(name: &str) -> Option<String> {
    let normalized = name
        .trim()
        .trim_matches('"')
        .strip_prefix("pg_catalog.")
        .unwrap_or_else(|| name.trim().trim_matches('"'))
        .to_ascii_lowercase();
    if normalized.ends_with("[]")
        || normalized.ends_with(" array")
        || normalized.starts_with("array<")
    {
        let element = normalized
            .strip_suffix("[]")
            .or_else(|| normalized.strip_suffix(" array"))
            .or_else(|| {
                normalized
                    .strip_prefix("array<")
                    .and_then(|value| value.strip_suffix('>'))
            })?
            .trim();
        let spec = pg_type_spec(element)?;
        return spec.array_oid.map(|_| format!("{}[]", spec.name));
    }
    let pg_type = match normalized.as_str() {
        "boolean" => "bool",
        "\"char\"" => "char",
        "smallint" => "int2",
        "integer" => "int4",
        "bigint" => "int8",
        "real" => "float4",
        "float" => "float8",
        "double precision" => "float8",
        "decimal" | "dec" => "numeric",
        "character varying" => "varchar",
        "time without time zone" => "time",
        "time with time zone" => "timetz",
        "timestamp without time zone" => "timestamp",
        "timestamp with time zone" => "timestamptz",
        other => other,
    };
    pg_type_spec(pg_type).map(|spec| spec.name.to_string())
}

pub(crate) fn sql_value_i64(value: &SqlValue) -> Option<i64> {
    match value {
        SqlValue::Int(value) => Some(*value),
        SqlValue::Float(value) => Some(*value as i64),
        SqlValue::String(value) => value.parse().ok(),
        _ => None,
    }
}

pub(crate) fn sql_value_bool(value: &SqlValue) -> Option<bool> {
    match value {
        SqlValue::Bool(value) => Some(*value),
        SqlValue::String(value) => parse_pg_bool_text(value),
        _ => None,
    }
}

pub(crate) fn sql_value_truth(value: SqlValue) -> Result<Option<bool>> {
    match value {
        SqlValue::Bool(value) => Ok(Some(value)),
        SqlValue::Null => Ok(None),
        other => Err(SqlError::Unsupported(format!(
            "boolean expression returned non-boolean {}",
            other.to_cell()
        ))),
    }
}

pub(crate) fn parse_pg_bool_text(value: &str) -> Option<bool> {
    let value = value.trim().to_ascii_lowercase();
    match value.as_str() {
        "1" => return Some(true),
        "0" => return Some(false),
        "" => return None,
        _ => {}
    }
    let mut matched = None;
    for (word, result) in [
        ("true", true),
        ("yes", true),
        ("on", true),
        ("false", false),
        ("no", false),
        ("off", false),
    ] {
        if word.starts_with(&value) {
            if matched.is_some_and(|matched| matched != result) {
                return None;
            }
            matched = Some(result);
        }
    }
    matched
}

/// A minimal in-record raster grid: north-up row-major values.
pub(crate) struct RasterGrid {
    pub(crate) min_lon: f64,
    pub(crate) min_lat: f64,
    pub(crate) max_lon: f64,
    pub(crate) max_lat: f64,
    pub(crate) width: usize,
    pub(crate) height: usize,
    pub(crate) values: Vec<f64>,
}

impl RasterGrid {
    pub(crate) fn from_record(record: &bicdb_core::Record) -> Result<Self> {
        let field = |name: &str| -> Result<f64> {
            record
                .metadata
                .get(name)
                .and_then(serde_json::Value::as_f64)
                .ok_or_else(|| {
                    SqlError::InvalidSql(format!("raster `{}` missing {name}", record.id))
                })
        };
        let width = field("width")? as usize;
        let height = field("height")? as usize;
        if width < 1 || height < 1 || width.saturating_mul(height) > 4_000_000 {
            return Err(SqlError::InvalidSql(format!(
                "raster `{}` has unsupported dimensions",
                record.id
            )));
        }
        let values: Vec<f64> = record
            .metadata
            .get("values")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(serde_json::Value::as_f64)
                    .collect()
            })
            .unwrap_or_default();
        if values.len() != width * height {
            return Err(SqlError::InvalidSql(format!(
                "raster `{}` has {} values for {}x{}",
                record.id,
                values.len(),
                width,
                height
            )));
        }
        Ok(Self {
            min_lon: field("min_lon")?,
            min_lat: field("min_lat")?,
            max_lon: field("max_lon")?,
            max_lat: field("max_lat")?,
            width,
            height,
            values,
        })
    }

    pub(crate) fn covers(&self, lon: f64, lat: f64) -> bool {
        (self.min_lon..=self.max_lon).contains(&lon) && (self.min_lat..=self.max_lat).contains(&lat)
    }

    pub(crate) fn cell_center(&self, col: usize, row: usize) -> (f64, f64) {
        let dx = (self.max_lon - self.min_lon) / self.width as f64;
        let dy = (self.max_lat - self.min_lat) / self.height as f64;
        (
            self.min_lon + (col as f64 + 0.5) * dx,
            self.max_lat - (row as f64 + 0.5) * dy,
        )
    }

    /// Bilinear interpolation between cell centers, clamped at edges.
    pub(crate) fn sample(&self, lon: f64, lat: f64) -> Option<f64> {
        if !self.covers(lon, lat) {
            return None;
        }
        let dx = (self.max_lon - self.min_lon) / self.width as f64;
        let dy = (self.max_lat - self.min_lat) / self.height as f64;
        let fx = ((lon - self.min_lon) / dx - 0.5).clamp(0.0, (self.width - 1) as f64);
        let fy = ((self.max_lat - lat) / dy - 0.5).clamp(0.0, (self.height - 1) as f64);
        let (x0, y0) = (fx.floor() as usize, fy.floor() as usize);
        let (x1, y1) = ((x0 + 1).min(self.width - 1), (y0 + 1).min(self.height - 1));
        let (tx, ty) = (fx - x0 as f64, fy - y0 as f64);
        let at = |x: usize, y: usize| self.values[y * self.width + x];
        Some(
            at(x0, y0) * (1.0 - tx) * (1.0 - ty)
                + at(x1, y0) * tx * (1.0 - ty)
                + at(x0, y1) * (1.0 - tx) * ty
                + at(x1, y1) * tx * ty,
        )
    }

    /// Terrain slope in degrees from central differences at cell pitch.
    pub(crate) fn slope_degrees(&self, lon: f64, lat: f64) -> Option<f64> {
        let dx_deg = (self.max_lon - self.min_lon) / self.width as f64;
        let dy_deg = (self.max_lat - self.min_lat) / self.height as f64;
        let east = self.sample((lon + dx_deg).min(self.max_lon), lat)?;
        let west = self.sample((lon - dx_deg).max(self.min_lon), lat)?;
        let north = self.sample(lon, (lat + dy_deg).min(self.max_lat))?;
        let south = self.sample(lon, (lat - dy_deg).max(self.min_lat))?;
        let meters_per_deg_lat = 111_320.0;
        let meters_per_deg_lon = meters_per_deg_lat * lat.to_radians().cos().abs().max(0.01);
        let dzdx = (east - west) / (2.0 * dx_deg * meters_per_deg_lon);
        let dzdy = (north - south) / (2.0 * dy_deg * meters_per_deg_lat);
        Some((dzdx.powi(2) + dzdy.powi(2)).sqrt().atan().to_degrees())
    }
}

pub(crate) fn raster_covering(
    db: &BicDb,
    table: &str,
    lon: f64,
    lat: f64,
) -> Result<Option<RasterGrid>> {
    let mut records = db.scan_collection(table).map_err(SqlError::from)?;
    records.sort_by(|a, b| a.id.cmp(&b.id));
    for record in &records {
        if let Ok(raster) = RasterGrid::from_record(record) {
            if raster.covers(lon, lat) {
                return Ok(Some(raster));
            }
        }
    }
    Ok(None)
}

pub(crate) fn sql_value_f64(value: &SqlValue) -> Option<f64> {
    value.as_f64().or_else(|| match value {
        SqlValue::String(value) if is_valid_numeric_text(value) => value.parse::<f64>().ok(),
        _ => None,
    })
}

pub(crate) fn cast_value(value: SqlValue, data_type: &DataType) -> Result<SqlValue> {
    if data_type.to_string().eq_ignore_ascii_case("record") && matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let (pg_type, _) = pg_type_from_data_type(data_type)?;
    let value = decode_typed_storage_envelope(value);
    let value = match explicit_boolean_cast(value.clone(), None, &pg_type) {
        Some(value) => value?,
        None => cast_value_to_pg_type(value, &pg_type)?,
    };
    apply_pg_type_modifier(
        value,
        &pg_type,
        pg_type_modifier_from_data_type(data_type)?.as_ref(),
    )
}

pub(crate) fn cast_value_with_assignment_semantics(
    value: SqlValue,
    data_type: &DataType,
) -> Result<SqlValue> {
    if data_type.to_string().eq_ignore_ascii_case("record") && matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let (pg_type, _) = pg_type_from_data_type(data_type)?;
    let value = decode_typed_storage_envelope(value);
    let value = match explicit_boolean_cast(value.clone(), None, &pg_type) {
        Some(value) => value?,
        None => cast_value_to_pg_type(value, &pg_type)?,
    };
    apply_pg_type_modifier_with_context(
        value,
        &pg_type,
        pg_type_modifier_from_data_type(data_type)?.as_ref(),
        false,
    )
}

pub(crate) fn decode_typed_storage_envelope(value: SqlValue) -> SqlValue {
    let stored = match &value {
        SqlValue::Json(stored) => Some(stored.clone()),
        SqlValue::String(text) => serde_json::from_str::<JsonValue>(text).ok(),
        _ => None,
    };
    let Some(stored) = stored else {
        return value;
    };
    let Some(pg_type) = stored
        .get(TYPED_STORAGE_KEY)
        .and_then(|envelope| envelope.get("pg_type"))
        .and_then(JsonValue::as_str)
    else {
        return value;
    };
    storage_json_to_sql_value(&stored, pg_type)
}

pub(crate) fn cast_value_with_db(
    db: &BicDb,
    value: SqlValue,
    data_type: &DataType,
) -> Result<SqlValue> {
    if data_type.to_string().eq_ignore_ascii_case("record") && matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let value = decode_typed_storage_envelope(value);
    if let Some(user_type) = user_type_column_from_data_type(db, data_type)? {
        return cast_value_to_user_type(value, &user_type);
    }
    if let Ok((pg_type, _)) = pg_type_from_data_type(data_type) {
        if let Some(element_type) = pg_type.strip_suffix("[]") {
            if is_oid_alias_type(element_type) {
                return resolve_oid_alias_array_value(db, element_type, value);
            }
        }
        if is_oid_alias_type(&pg_type) {
            return resolve_oid_alias_value(db, &pg_type, value);
        }
    }
    cast_value(value, data_type)
}

pub(crate) fn cast_expr_value(
    value: SqlValue,
    expr: &Expr,
    data_type: &DataType,
    schema: Option<&TableSchema>,
) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) && data_type.to_string().eq_ignore_ascii_case("record") {
        return Ok(SqlValue::Null);
    }
    let (target_type, _) = pg_type_from_data_type(data_type)?;
    let source_type = projected_arithmetic_operand_pg_type(expr, schema);
    if matches!(target_type.as_str(), "text" | "varchar" | "bpchar" | "name") {
        let projected_source_type = projected_expr_pg_type(expr, schema);
        if source_type.as_deref().or(projected_source_type.as_deref()) == Some("bpchar")
            && matches!(target_type.as_str(), "text" | "varchar" | "name")
        {
            return Ok(match value {
                SqlValue::String(value) => {
                    SqlValue::String(value.trim_end_matches(' ').to_string())
                }
                value => value,
            });
        }
        if let Some(element_type) = source_type
            .as_deref()
            .or(projected_source_type.as_deref())
            .and_then(|source| source.strip_suffix("[]"))
        {
            if matches!(value, SqlValue::Null) {
                return Ok(SqlValue::Null);
            }
            let delimiter = projected_expr_user_type(expr, schema)
                .filter(|user_type| user_type.array)
                .map(|user_type| user_type.scalar_delimiter())
                .or_else(|| pg_type_delimiter(element_type))
                .unwrap_or(',');
            return postgres_array_text_value(&value, delimiter).map(SqlValue::String);
        }
    }
    if target_type == "oid" {
        if let Some(value) = explicit_oid_cast(value.clone(), expr, schema) {
            return value;
        }
    }
    if source_type.as_deref() == Some("oid") {
        match target_type.as_str() {
            "int4" => {
                let value = oid_value(&value)?;
                return Ok(SqlValue::Int(i64::from(value as i32)));
            }
            "int8" => return Ok(SqlValue::Int(i64::from(oid_value(&value)?))),
            "oid" | "text" | "varchar" | "bpchar" | "name" | "regproc" | "regprocedure"
            | "regoper" | "regoperator" | "regclass" | "regcollation" | "regtype" | "regrole"
            | "regnamespace" | "regconfig" | "regdictionary" => {}
            _ => {
                return Err(SqlError::cannot_coerce(format!(
                    "cannot cast type oid to {}",
                    pg_cast_display_type(&target_type)
                )));
            }
        }
    }
    let character_types = ["text", "varchar", "bpchar", "name"];
    if target_type == "xml"
        && source_type
            .as_deref()
            .is_some_and(|source| source != "xml" && !character_types.contains(&source))
    {
        return Err(SqlError::cannot_coerce(format!(
            "cannot cast type {} to xml",
            pg_cast_display_type(source_type.as_deref().expect("source type checked above"))
        )));
    }
    if source_type.as_deref() == Some("xml")
        && target_type != "xml"
        && !character_types.contains(&target_type.as_str())
    {
        return Err(SqlError::cannot_coerce(format!(
            "cannot cast type xml to {}",
            pg_cast_display_type(&target_type)
        )));
    }
    let source_is_bits = matches!(source_type.as_deref(), Some("bit" | "varbit"));
    let target_is_bits = matches!(target_type.as_str(), "bit" | "varbit");
    let json_text_types = ["text", "varchar", "bpchar", "name"];
    if target_type == "json"
        && source_type.as_deref().is_some_and(|source| {
            !matches!(source, "json" | "jsonb") && !json_text_types.contains(&source)
        })
    {
        return Err(SqlError::cannot_coerce(format!(
            "cannot cast type {} to json",
            pg_cast_display_type(source_type.as_deref().expect("source type checked above"))
        )));
    }
    if source_type.as_deref() == Some("json")
        && !matches!(target_type.as_str(), "json" | "jsonb")
        && !json_text_types.contains(&target_type.as_str())
    {
        return Err(SqlError::cannot_coerce(format!(
            "cannot cast type json to {}",
            pg_cast_display_type(&target_type)
        )));
    }
    let integer_to_bit = target_type == "bit"
        && matches!(source_type.as_deref(), Some("int4" | "int8"))
        && matches!(value, SqlValue::Int(_));
    if target_is_bits
        && !integer_to_bit
        && !matches!(
            source_type.as_deref(),
            None | Some("bit" | "varbit" | "text" | "varchar" | "bpchar")
        )
    {
        return Err(SqlError::cannot_coerce(format!(
            "cannot cast type {} to {}",
            pg_cast_display_type(source_type.as_deref().unwrap()),
            pg_cast_display_type(&target_type)
        )));
    }
    if source_is_bits
        && !matches!(
            target_type.as_str(),
            "bit" | "varbit" | "text" | "varchar" | "bpchar"
        )
        && !(source_type.as_deref() == Some("bit")
            && matches!(target_type.as_str(), "int4" | "int8"))
    {
        return Err(SqlError::cannot_coerce(format!(
            "cannot cast type {} to {}",
            pg_cast_display_type(source_type.as_deref().unwrap()),
            pg_cast_display_type(&target_type)
        )));
    }
    if integer_to_bit {
        let SqlValue::Int(integer) = value else {
            unreachable!("integer cast was matched")
        };
        let width = if source_type.as_deref() == Some("int8") {
            64
        } else {
            32
        };
        let source = if width == 64 {
            format!("{:064b}", integer as u64)
        } else {
            format!("{:032b}", integer as i32 as u32)
        };
        let required = match pg_type_modifier_from_data_type(data_type)? {
            Some(PgTypeModifier::Bit { length }) => length as usize,
            _ => 1,
        };
        let bits = if required <= width {
            source[source.len() - required..].to_string()
        } else {
            format!(
                "{}{}",
                if integer < 0 { '1' } else { '0' }
                    .to_string()
                    .repeat(required - width),
                source
            )
        };
        return Ok(SqlValue::String(bits));
    }
    if source_type.as_deref() == Some("bit") && matches!(target_type.as_str(), "int4" | "int8") {
        if matches!(value, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        let bits = PgBitString::from_bit_text(&value.to_cell())
            .map_err(|_| SqlError::invalid_text_representation("bit", "invalid bit string"))?;
        let width = if target_type == "int4" { 32 } else { 64 };
        if bits.bit_len() > width {
            return Err(SqlError::numeric_value_out_of_range(if width == 32 {
                "integer out of range"
            } else {
                "bigint out of range"
            }));
        }
        let unsigned = bits
            .bytes()
            .iter()
            .fold(0_u64, |value, byte| (value << 8) | u64::from(*byte))
            >> (bits.bytes().len() * 8 - bits.bit_len());
        let integer = if bits.bit_len() == width && unsigned & (1_u64 << (width - 1)) != 0 {
            if width == 32 {
                i64::from(unsigned as u32 as i32)
            } else {
                unsigned as i64
            }
        } else {
            unsigned as i64
        };
        return enforce_integer_value_type(SqlValue::Int(integer), Some(&target_type));
    }
    if source_type.as_deref() == Some("char") {
        if let Some(byte) = pg_internal_char_byte(&value) {
            match target_type.as_str() {
                "int4" => return Ok(SqlValue::Int(i64::from(byte as i8))),
                "char" => return Ok(pg_internal_char_value(byte)),
                "text" | "varchar" | "bpchar" | "name" => {
                    return cast_value_to_pg_type(
                        SqlValue::String(pg_internal_char_text(byte)),
                        &target_type,
                    );
                }
                _ => {}
            }
        }
    }
    let value = match explicit_boolean_cast(value.clone(), source_type.as_deref(), &target_type) {
        Some(value) => value?,
        None => value,
    };
    if matches!(
        projected_expr_pg_type_rc(expr, schema).as_ref().as_deref(),
        Some("money")
    ) {
        if matches!(target_type.as_str(), "text" | "varchar" | "bpchar") {
            let cents = crate::pg_money_cents_from_text(&value.to_cell())
                .map_err(|_| SqlError::money_out_of_range())?;
            return Ok(SqlValue::String(crate::pg_money_display_from_cents(cents)));
        }
        if !matches!(target_type.as_str(), "money" | "numeric") {
            return Err(SqlError::cannot_coerce(format!(
                "cannot cast type money to {target_type}"
            )));
        }
    }
    if matches!(target_type.as_str(), "text" | "varchar" | "bpchar" | "name")
        && source_type.as_deref() == Some("regtype")
    {
        if matches!(value, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        let oid = sql_value_i64(&value).or_else(|| match &value {
            SqlValue::String(name) => pg_type_oid_by_name(name).map(i64::from),
            _ => None,
        });
        let Some(oid) = oid else {
            if let SqlValue::String(name) = value {
                return Ok(SqlValue::String(name));
            }
            return Err(SqlError::InvalidSql(format!(
                "cannot render {} as regtype",
                value.to_cell()
            )));
        };
        return Ok(SqlValue::String(
            i32::try_from(oid)
                .ok()
                .and_then(pg_type_name_by_oid)
                .map(str::to_string)
                .unwrap_or_else(|| oid.to_string()),
        ));
    }
    if matches!(target_type.as_str(), "text" | "varchar") {
        if let Some(source_type) = projected_expr_pg_type(expr, schema)
            .filter(|pg_type| matches!(pg_type.as_str(), "float4" | "float8"))
        {
            return match value {
                SqlValue::Null => Ok(SqlValue::Null),
                SqlValue::Float(value) => {
                    Ok(SqlValue::String(postgres_float_text(value, &source_type)))
                }
                value => cast_value_to_pg_type(value, &target_type),
            };
        }
    }
    if source_type.as_deref() == Some("macaddr8") && target_type == "macaddr" {
        if matches!(value, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        let PgMacAddress::Mac64(address) = PgMacAddress::from_postgres_text(&value.to_cell(), true)
            .map_err(|error| postgres_range_input_error("macaddr8", &value.to_cell(), error))?
        else {
            unreachable!("macaddr8 parser returns an EUI-64 value")
        };
        if address[3..5] != [0xff, 0xfe] {
            return Err(SqlError::numeric_value_out_of_range(
                "macaddr8 data out of range to convert to macaddr",
            ));
        }
        return Ok(SqlValue::String(
            PgMacAddress::Mac48([
                address[0], address[1], address[2], address[5], address[6], address[7],
            ])
            .to_postgres_text(),
        ));
    }
    let value = cast_value_to_pg_type(value, &target_type)?;
    apply_pg_type_modifier(
        value,
        &target_type,
        pg_type_modifier_from_data_type(data_type)?.as_ref(),
    )
}

pub(crate) fn postgres_array_text_value(value: &SqlValue, delimiter: char) -> Result<String> {
    pub(crate) fn quote_scalar(value: &str, delimiter: char, force: bool) -> String {
        let quote = force
            || value.is_empty()
            || value.eq_ignore_ascii_case("null")
            || value.chars().any(|character| {
                character == delimiter
                    || matches!(character, '{' | '}' | '"' | '\\')
                    || character.is_whitespace()
            });
        if quote {
            format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
        } else {
            value.to_string()
        }
    }

    pub(crate) fn scalar(value: &JsonValue, delimiter: char) -> Result<String> {
        match value {
            JsonValue::Null => Ok("NULL".to_string()),
            JsonValue::Array(values) => Ok(format!(
                "{{{}}}",
                values
                    .iter()
                    .map(|value| scalar(value, delimiter))
                    .collect::<Result<Vec<_>>>()?
                    .join(&delimiter.to_string())
            )),
            JsonValue::String(value) => Ok(quote_scalar(value, delimiter, false)),
            JsonValue::Bool(value) => Ok(value.to_string()),
            JsonValue::Number(value) => Ok(value.to_string()),
            JsonValue::Object(_) => crate::pg_composite_from_array_json(value)
                .map(|composite| quote_scalar(&composite.to_postgres_text(), delimiter, true))
                .ok_or_else(|| invalid_array_text("internal array value")),
        }
    }

    let value = match value {
        SqlValue::Json(JsonValue::Object(object)) => object
            .get("$bicdb_array_input")
            .and_then(JsonValue::as_object)
            .and_then(|input| input.get("value"))
            .ok_or_else(|| invalid_array_text("internal array value"))?,
        SqlValue::Json(value) => value,
        _ => return Err(invalid_array_text(&value.to_cell())),
    };
    scalar(value, delimiter)
}

pub(crate) fn explicit_oid_cast(
    value: SqlValue,
    expr: &Expr,
    schema: Option<&TableSchema>,
) -> Option<Result<SqlValue>> {
    match projected_arithmetic_operand_pg_type(expr, schema).as_deref() {
        Some("int2" | "int4") => Some(match value {
            SqlValue::Int(value) => i32::try_from(value)
                .map(|value| SqlValue::Int(i64::from(value as u32)))
                .map_err(|_| SqlError::numeric_value_out_of_range("OID out of range")),
            _ => Err(SqlError::cannot_coerce("cannot cast value to oid")),
        }),
        Some("int8") => Some(match value {
            SqlValue::Int(value) => u32::try_from(value)
                .map(|value| SqlValue::Int(i64::from(value)))
                .map_err(|_| SqlError::numeric_value_out_of_range("OID out of range")),
            _ => Err(SqlError::cannot_coerce("cannot cast value to oid")),
        }),
        Some(source @ ("numeric" | "float4" | "float8")) => Some(Err(SqlError::cannot_coerce(
            format!("cannot cast type {} to oid", pg_cast_display_type(source)),
        ))),
        _ => None,
    }
}

pub(crate) fn pg_cast_display_type(pg_type: &str) -> &str {
    match pg_type {
        "int2" => "smallint",
        "int4" => "integer",
        "int8" => "bigint",
        "varbit" => "bit varying",
        other => other,
    }
}

pub(crate) fn explicit_boolean_cast(
    value: SqlValue,
    source_type: Option<&str>,
    target_type: &str,
) -> Option<Result<SqlValue>> {
    if matches!(value, SqlValue::Null) {
        return None;
    }
    let source_is_bool = source_type == Some("bool") || matches!(&value, SqlValue::Bool(_));
    if source_is_bool {
        return match target_type {
            "int4" => Some(Ok(SqlValue::Int(i64::from(matches!(
                value,
                SqlValue::Bool(true)
            ))))),
            "bool" | "text" | "varchar" | "bpchar" => None,
            _ => Some(Err(SqlError::cannot_coerce(format!(
                "cannot cast type boolean to {target_type}"
            )))),
        };
    }
    if target_type != "bool" {
        return None;
    }
    match source_type {
        Some("int4") => Some(match value {
            SqlValue::Int(value) => Ok(SqlValue::Bool(value != 0)),
            value => Err(SqlError::cannot_coerce(format!(
                "cannot cast type int4 to boolean: {}",
                value.to_cell()
            ))),
        }),
        Some("int2" | "int8" | "float4" | "float8" | "numeric" | "money") => {
            Some(Err(SqlError::cannot_coerce(format!(
                "cannot cast type {} to boolean",
                source_type.expect("matched source type")
            ))))
        }
        None => match value {
            SqlValue::Int(value) => Some(Ok(SqlValue::Bool(value != 0))),
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn cast_value_to_column_type(
    value: SqlValue,
    column: &ColumnSchema,
) -> Result<SqlValue> {
    if let Some(user_type) = &column.user_type {
        return cast_value_to_user_type(value, user_type);
    }
    let value = cast_value_to_pg_type(value, &column.pg_type)?;
    apply_pg_type_modifier_with_context(
        value,
        &column.pg_type,
        column.type_modifier.as_ref(),
        false,
    )
}

pub(crate) fn cast_value_to_user_type(
    value: SqlValue,
    user_type: &UserTypeColumnSchema,
) -> Result<SqlValue> {
    match &user_type.kind {
        UserTypeKind::Shell => Err(SqlError::data_exception(
            "42704",
            format!("type \"{}\" is only a shell", user_type.name),
            Some(user_type.name.clone()),
        )),
        UserTypeKind::Base {
            codec_type,
            delimiter,
            ..
        } if user_type.array => cast_value_to_array_with_delimiter(value, codec_type, *delimiter),
        UserTypeKind::Base { codec_type, .. } => cast_value_to_pg_type(value, codec_type),
        UserTypeKind::Enum { labels } => {
            if matches!(value, SqlValue::Null) {
                return Ok(SqlValue::Null);
            }
            let valid_labels = labels
                .iter()
                .map(|label| label.label.as_str())
                .collect::<BTreeSet<_>>();
            if user_type.array {
                let value = cast_value_to_array(value, "text")?;
                validate_enum_array_value(&value, &valid_labels, &user_type.formatted_name())?;
                return Ok(value);
            }
            let SqlValue::String(label) = value else {
                return Err(SqlError::cannot_coerce(format!(
                    "cannot cast value to type {}",
                    user_type.formatted_name()
                )));
            };
            if !valid_labels.contains(label.as_str()) {
                return Err(SqlError::invalid_text_representation(
                    user_type.formatted_name(),
                    format!("\"{label}\""),
                ));
            }
            Ok(SqlValue::String(label))
        }
        UserTypeKind::Composite { attributes, .. } if user_type.array => {
            cast_value_to_composite_array(value, user_type, attributes)
        }
        UserTypeKind::Composite { attributes, .. } => {
            cast_value_to_composite_scalar(value, user_type, attributes)
        }
        UserTypeKind::Domain { .. } if user_type.array => {
            cast_value_to_domain_array(value, user_type)
        }
        UserTypeKind::Domain { .. } => cast_value_to_domain_scalar(value, user_type),
        UserTypeKind::Range { value: range, .. } if user_type.array => {
            cast_value_to_user_range_array(value, user_type, range, false)
        }
        UserTypeKind::Range { value: range, .. } => {
            cast_value_to_user_range(value, user_type, range, false)
        }
        UserTypeKind::Multirange { value: range, .. } if user_type.array => {
            cast_value_to_user_range_array(value, user_type, range, true)
        }
        UserTypeKind::Multirange { value: range, .. } => {
            cast_value_to_user_range(value, user_type, range, true)
        }
    }
}

pub(crate) fn cast_value_to_user_range(
    value: SqlValue,
    user_type: &UserTypeColumnSchema,
    definition: &UserRangeValueSchema,
    multirange: bool,
) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let text = match value {
        SqlValue::String(value) => value,
        value => {
            return Err(SqlError::cannot_coerce(format!(
                "cannot cast value to type {}: {}",
                user_type.formatted_name(),
                value.to_cell()
            )));
        }
    };
    if multirange {
        let ranges = parse_pg_multirange_with_policy(
            &text,
            &user_type.formatted_name(),
            &definition.subtype,
            definition.canonical_discrete,
        )
        .map_err(|error| postgres_range_input_error(&user_type.formatted_name(), &text, error))?;
        return Ok(SqlValue::String(format_pg_multirange(&ranges)));
    }
    let range = PgRange::from_postgres_text_with_policy(
        &text,
        &user_type.formatted_name(),
        &definition.subtype,
        definition.canonical_discrete,
    )
    .map_err(|error| postgres_range_input_error(&user_type.formatted_name(), &text, error))?;
    Ok(SqlValue::String(render_range_for_session(
        &range,
        &user_type.formatted_name(),
    )))
}

pub(crate) fn cast_value_to_user_range_array(
    value: SqlValue,
    user_type: &UserTypeColumnSchema,
    definition: &UserRangeValueSchema,
    multirange: bool,
) -> Result<SqlValue> {
    let value = cast_value_to_array(value, "text")?;
    let SqlValue::Json(mut json) = value else {
        return Ok(value);
    };
    pub(crate) fn canonicalize(
        value: &mut JsonValue,
        user_type: &UserTypeColumnSchema,
        definition: &UserRangeValueSchema,
        multirange: bool,
    ) -> Result<()> {
        match value {
            JsonValue::Null => Ok(()),
            JsonValue::String(text) => {
                let canonical = cast_value_to_user_range(
                    SqlValue::String(text.clone()),
                    user_type,
                    definition,
                    multirange,
                )?;
                *text = canonical.to_cell();
                Ok(())
            }
            JsonValue::Array(values) => {
                for value in values {
                    canonicalize(value, user_type, definition, multirange)?;
                }
                Ok(())
            }
            JsonValue::Object(object) => {
                let value = object
                    .get_mut("$bicdb_array_input")
                    .and_then(JsonValue::as_object_mut)
                    .and_then(|input| input.get_mut("value"))
                    .ok_or_else(|| {
                        SqlError::invalid_text_representation(
                            user_type.formatted_name(),
                            "invalid range array",
                        )
                    })?;
                canonicalize(value, user_type, definition, multirange)
            }
            _ => Err(SqlError::invalid_text_representation(
                user_type.formatted_name(),
                "invalid range array",
            )),
        }
    }
    let scalar_type = UserTypeColumnSchema {
        array: false,
        ..user_type.clone()
    };
    canonicalize(&mut json, &scalar_type, definition, multirange)?;
    Ok(SqlValue::Json(json))
}

pub(crate) fn cast_value_to_composite_scalar(
    value: SqlValue,
    user_type: &UserTypeColumnSchema,
    attributes: &[CompositeAttributeSchema],
) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let attributes = attributes
        .iter()
        .filter(|attribute| !attribute.dropped)
        .collect::<Vec<_>>();
    let values = match value {
        SqlValue::Composite(composite) => composite
            .fields
            .into_iter()
            .map(|field| field.value)
            .collect::<Vec<_>>(),
        SqlValue::String(value) => parse_composite_text_fields(&value)?
            .into_iter()
            .map(|value| value.map(SqlValue::String).unwrap_or(SqlValue::Null))
            .collect(),
        SqlValue::Json(value) => {
            if let Some(composite) = pg_composite_from_array_json(&value) {
                composite
                    .fields
                    .into_iter()
                    .map(|field| field.value)
                    .collect()
            } else if let JsonValue::Object(values) = value {
                attributes
                    .iter()
                    .map(|attribute| {
                        values
                            .get(&attribute.name)
                            .map(json_to_sql_value)
                            .unwrap_or(SqlValue::Null)
                    })
                    .collect()
            } else {
                return Err(SqlError::cannot_coerce(format!(
                    "cannot cast json value to {}",
                    user_type.formatted_name()
                )));
            }
        }
        value => {
            return Err(SqlError::cannot_coerce(format!(
                "cannot cast type {} to {}",
                projected_value_pg_type(&value),
                user_type.formatted_name()
            )));
        }
    };
    if values.len() != attributes.len() {
        return Err(SqlError::invalid_text_representation(
            user_type.formatted_name(),
            format!(
                "wrong number of columns: expected {}, got {}",
                attributes.len(),
                values.len()
            ),
        ));
    }
    let fields = attributes
        .iter()
        .zip(values)
        .map(|(attribute, value)| {
            let value = if let Some(attribute_type) = &attribute.user_type {
                cast_value_to_user_type(value, attribute_type)?
            } else {
                let value = cast_value_to_pg_type(value, &attribute.pg_type)?;
                apply_pg_type_modifier_with_context(
                    value,
                    &attribute.pg_type,
                    attribute.type_modifier.as_ref(),
                    false,
                )?
            };
            Ok(SqlCompositeField {
                name: attribute.name.clone(),
                pg_type: attribute.pg_type.clone(),
                value,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SqlValue::Composite(SqlComposite {
        type_oid: u32::try_from(user_type.oid).ok(),
        type_name: user_type.formatted_name(),
        fields,
    }))
}

pub(crate) fn cast_value_to_composite_array(
    value: SqlValue,
    user_type: &UserTypeColumnSchema,
    attributes: &[CompositeAttributeSchema],
) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let value = match value {
        SqlValue::Json(value) => SqlValue::Json(value),
        value => cast_value_to_array(value, "text")?,
    };
    let scalar_type = UserTypeColumnSchema {
        array: false,
        ..user_type.clone()
    };
    pub(crate) fn cast_elements(
        value: JsonValue,
        scalar_type: &UserTypeColumnSchema,
        attributes: &[CompositeAttributeSchema],
    ) -> Result<JsonValue> {
        match value {
            JsonValue::Array(values) => values
                .into_iter()
                .map(|value| cast_elements(value, scalar_type, attributes))
                .collect::<Result<Vec<_>>>()
                .map(JsonValue::Array),
            JsonValue::Null => Ok(JsonValue::Null),
            value => {
                cast_value_to_composite_scalar(json_to_sql_value(&value), scalar_type, attributes)
                    .map(composite_array_element_json)
            }
        }
    }
    let SqlValue::Json(mut value) = value else {
        unreachable!("array coercion always returns JSON")
    };
    if let Some(input) = value
        .as_object_mut()
        .and_then(|object| object.get_mut("$bicdb_array_input"))
        .and_then(JsonValue::as_object_mut)
    {
        let array = input
            .remove("value")
            .ok_or_else(|| invalid_array_text("internal composite array"))?;
        input.insert(
            "value".to_string(),
            cast_elements(array, &scalar_type, attributes)?,
        );
        return Ok(SqlValue::Json(value));
    }
    Ok(SqlValue::Json(cast_elements(
        value,
        &scalar_type,
        attributes,
    )?))
}

pub(crate) fn composite_array_element_json(value: SqlValue) -> JsonValue {
    let SqlValue::Composite(composite) = value else {
        return sql_value_to_json(value);
    };
    serde_json::json!({
        "$bicdb_composite": {
            "type_oid": composite.type_oid,
            "type_name": composite.type_name,
            "fields": composite.fields.into_iter().map(|field| {
                serde_json::json!({
                    "name": field.name,
                    "pg_type": field.pg_type,
                    "value": match field.value {
                        SqlValue::Composite(composite) => {
                            composite_array_element_json(SqlValue::Composite(composite))
                        }
                        value => sql_value_to_json(value),
                    },
                })
            }).collect::<Vec<_>>(),
        }
    })
}

pub(crate) fn parse_composite_text_fields(value: &str) -> Result<Vec<Option<String>>> {
    let value = value.trim();
    if !value.starts_with('(') || !value.ends_with(')') {
        return Err(SqlError::invalid_text_representation(
            "record",
            format!("\"{value}\""),
        ));
    }
    let inner = &value[1..value.len() - 1];
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut was_quoted = false;
    let mut depth = 0usize;
    for character in inner.chars().chain(std::iter::once(',')) {
        if escaped {
            field.push(character);
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
            was_quoted = true;
            continue;
        }
        if !quoted {
            match character {
                '(' | '{' | '[' => depth += 1,
                ')' | '}' | ']' if depth > 0 => depth -= 1,
                ',' if depth == 0 => {
                    fields.push((was_quoted || !field.is_empty()).then_some(field));
                    field = String::new();
                    was_quoted = false;
                    continue;
                }
                _ => {}
            }
        }
        field.push(character);
    }
    if quoted || escaped || depth != 0 {
        return Err(SqlError::invalid_text_representation(
            "record",
            format!("\"{value}\""),
        ));
    }
    Ok(fields)
}

pub(crate) fn projected_value_pg_type(value: &SqlValue) -> &'static str {
    match value {
        SqlValue::Bool(_) => "boolean",
        SqlValue::Int(_) => "bigint",
        SqlValue::Float(_) => "double precision",
        SqlValue::Composite(_) => "record",
        SqlValue::Json(_) | SqlValue::JsonText(_) => "json",
        SqlValue::Null => "unknown",
        _ => "text",
    }
}

pub(crate) fn cast_value_to_domain_array(
    value: SqlValue,
    user_type: &UserTypeColumnSchema,
) -> Result<SqlValue> {
    pub(crate) fn cast_elements(
        value: JsonValue,
        scalar_type: &UserTypeColumnSchema,
    ) -> Result<JsonValue> {
        match value {
            JsonValue::Array(values) => values
                .into_iter()
                .map(|value| cast_elements(value, scalar_type))
                .collect::<Result<Vec<_>>>()
                .map(JsonValue::Array),
            value => cast_value_to_domain_scalar(json_to_sql_value(&value), scalar_type)
                .map(sql_value_to_json),
        }
    }

    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let value = cast_value_to_array(value, "text")?;
    let scalar_type = UserTypeColumnSchema {
        array: false,
        ..user_type.clone()
    };
    let SqlValue::Json(mut value) = value else {
        unreachable!("array coercion always returns JSON")
    };
    if let Some(input) = value
        .as_object_mut()
        .and_then(|object| object.get_mut("$bicdb_array_input"))
        .and_then(JsonValue::as_object_mut)
    {
        let array = input
            .remove("value")
            .ok_or_else(|| invalid_array_text("internal domain array"))?;
        input.insert("value".to_string(), cast_elements(array, &scalar_type)?);
        return Ok(SqlValue::Json(value));
    }
    Ok(SqlValue::Json(cast_elements(value, &scalar_type)?))
}

pub(crate) fn cast_value_to_domain_scalar(
    value: SqlValue,
    user_type: &UserTypeColumnSchema,
) -> Result<SqlValue> {
    let UserTypeKind::Domain {
        base_type,
        base_user_type,
        type_modifier,
        not_null,
        constraints,
        ..
    } = &user_type.kind
    else {
        unreachable!("domain coercion requires a domain type")
    };
    if matches!(value, SqlValue::Null) {
        if *not_null {
            return Err(SqlError::data_exception(
                "23502",
                format!("domain {} does not allow null values", user_type.name),
                Some(user_type.name.clone()),
            ));
        }
        return Ok(SqlValue::Null);
    }
    let value = if let Some(base_user_type) = base_user_type {
        cast_value_to_user_type(value, base_user_type)?
    } else {
        let value = cast_value_to_pg_type(value, base_type)?;
        apply_pg_type_modifier_with_context(value, base_type, type_modifier.as_ref(), false)?
    };
    if !constraints.is_empty() {
        let record = Record::new("domain-value").with_metadata(serde_json::json!({
            "value": sql_value_to_json(value.clone()),
        }));
        for constraint in constraints {
            let expression = parse_check_expression(&constraint.expression)?;
            if matches!(eval_predicate_truth(&record, &expression)?, Some(false)) {
                return Err(SqlError::data_exception(
                    "23514",
                    format!(
                        "value for domain {} violates check constraint \"{}\"",
                        user_type.name, constraint.name
                    ),
                    Some(constraint.name.clone()),
                ));
            }
        }
    }
    Ok(value)
}

pub(crate) fn validate_enum_array_value(
    value: &SqlValue,
    valid_labels: &BTreeSet<&str>,
    type_name: &str,
) -> Result<()> {
    pub(crate) fn validate_json(
        value: &JsonValue,
        valid_labels: &BTreeSet<&str>,
        type_name: &str,
    ) -> Result<()> {
        match value {
            JsonValue::Null => Ok(()),
            JsonValue::String(label) if valid_labels.contains(label.as_str()) => Ok(()),
            JsonValue::String(label) => Err(SqlError::invalid_text_representation(
                type_name,
                format!("\"{label}\""),
            )),
            JsonValue::Array(values) => values
                .iter()
                .try_for_each(|value| validate_json(value, valid_labels, type_name)),
            other => Err(SqlError::invalid_text_representation(
                type_name,
                format!("\"{other}\""),
            )),
        }
    }

    let SqlValue::Json(value) = value else {
        return Err(SqlError::invalid_text_representation(
            type_name,
            format!("\"{}\"", value.to_cell()),
        ));
    };
    if let Some(value) = value
        .as_object()
        .and_then(|object| object.get("$bicdb_array_input"))
        .and_then(JsonValue::as_object)
        .and_then(|input| input.get("value"))
    {
        return validate_json(value, valid_labels, type_name);
    }
    validate_json(value, valid_labels, type_name)
}

pub(crate) fn apply_pg_type_modifier(
    value: SqlValue,
    pg_type: &str,
    modifier: Option<&PgTypeModifier>,
) -> Result<SqlValue> {
    apply_pg_type_modifier_with_context(value, pg_type, modifier, true)
}

pub(crate) fn apply_pg_type_modifier_with_context(
    value: SqlValue,
    pg_type: &str,
    modifier: Option<&PgTypeModifier>,
    explicit_cast: bool,
) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(value);
    }
    if modifier.is_some() {
        if let Some(element_type) = pg_type.strip_suffix("[]") {
            if let SqlValue::Json(mut array) = value {
                apply_array_element_modifier(&mut array, element_type, modifier, explicit_cast)?;
                return Ok(SqlValue::Json(array));
            }
            return Ok(value);
        }
    }
    if pg_type == "vector" {
        let vector = sql_value_to_vector(&value)?;
        if let Some(PgTypeModifier::Vector { dimensions }) = modifier {
            if vector.len() != usize::from(*dimensions) {
                return Err(SqlError::data_exception(
                    "22000",
                    format!("expected {} dimensions, not {}", dimensions, vector.len()),
                    Some("vector".to_string()),
                ));
            }
        }
        return Ok(SqlValue::Json(vector_json(&vector)));
    }
    if matches!(pg_type, "bpchar" | "varchar") {
        let Some(PgTypeModifier::Character { length }) = modifier else {
            return Ok(value);
        };
        let text = match value {
            SqlValue::String(text) => text,
            other => return Ok(other),
        };
        let length = *length as usize;
        let mut coerced = if text.chars().count() <= length {
            text
        } else {
            let excess_is_spaces = text.chars().skip(length).all(|character| character == ' ');
            if !explicit_cast && !excess_is_spaces {
                let type_name = if pg_type == "bpchar" {
                    "character"
                } else {
                    "character varying"
                };
                return Err(SqlError::string_data_right_truncation(format!(
                    "value too long for type {type_name}({length})"
                )));
            }
            text.chars().take(length).collect()
        };
        if pg_type == "bpchar" {
            coerced.extend(std::iter::repeat(' ').take(length - coerced.chars().count()));
        }
        return Ok(SqlValue::String(coerced));
    }
    if matches!(pg_type, "bit" | "varbit") {
        let Some(PgTypeModifier::Bit { length }) = modifier else {
            return Ok(value);
        };
        let text = value.to_cell();
        let bits = PgBitString::from_bit_text(&text).map_err(|_| {
            SqlError::invalid_text_representation(
                pg_type,
                format!(
                    "invalid input syntax for type {}: \"{text}\"",
                    pg_cast_display_type(pg_type)
                ),
            )
        })?;
        let required = *length as usize;
        if pg_type == "varbit" {
            if bits.bit_len() <= required {
                return Ok(SqlValue::String(bits.to_bit_text()));
            }
            if !explicit_cast {
                return Err(SqlError::string_data_right_truncation(format!(
                    "bit string too long for type bit varying({required})"
                )));
            }
            let mut coerced = bits.to_bit_text();
            coerced.truncate(required);
            return Ok(SqlValue::String(coerced));
        }
        if bits.bit_len() == required {
            return Ok(SqlValue::String(bits.to_bit_text()));
        }
        if !explicit_cast {
            return Err(SqlError::data_exception(
                "22026",
                format!(
                    "bit string length {} does not match type bit({required})",
                    bits.bit_len()
                ),
                Some("bit".to_string()),
            ));
        }
        let mut coerced = bits.to_bit_text();
        coerced.truncate(required);
        coerced.extend(std::iter::repeat('0').take(required.saturating_sub(coerced.len())));
        return Ok(SqlValue::String(coerced));
    }
    if pg_type == "time" {
        let Some(PgTypeModifier::Temporal { precision }) = modifier else {
            return Ok(value);
        };
        let text = value.to_cell();
        return PgTime::from_postgres_text(&text)
            .and_then(|time| time.with_precision(*precision))
            .map(|time| SqlValue::String(time.to_iso_text()))
            .map_err(|error| postgres_time_input_error(&text, error));
    }
    if pg_type == "timetz" {
        let Some(PgTypeModifier::Temporal { precision }) = modifier else {
            return Ok(value);
        };
        let text = value.to_cell();
        return PgTimeTz::from_postgres_text(&text, current_timezone_offset_seconds())
            .and_then(|time| time.with_precision(*precision))
            .map(|time| SqlValue::String(time.to_iso_text()))
            .map_err(|error| postgres_timetz_input_error(&text, error));
    }
    if pg_type == "timestamp" {
        let Some(PgTypeModifier::Temporal { precision }) = modifier else {
            return Ok(value);
        };
        let text = value.to_cell();
        return PgTimestamp::from_postgres_text(&text, false)
            .and_then(|timestamp| timestamp.with_precision(*precision))
            .map(|timestamp| SqlValue::String(timestamp.to_iso_text(false)))
            .map_err(|error| postgres_timestamp_input_error(&text, error));
    }
    if pg_type == "timestamptz" {
        let Some(PgTypeModifier::Temporal { precision }) = modifier else {
            return Ok(value);
        };
        let text = value.to_cell();
        return parse_timestamptz(&text)
            .and_then(|timestamp| timestamp.with_precision(*precision))
            .map(|timestamp| SqlValue::String(render_timestamptz(timestamp)))
            .map_err(|error| postgres_timestamptz_input_error(&text, error));
    }
    if pg_type == "interval" {
        let Some(PgTypeModifier::Interval { fields, precision }) = modifier else {
            return Ok(value);
        };
        let text = value.to_cell();
        return PgInterval::from_postgres_text(&text)
            .and_then(|interval| interval.with_typmod(fields.as_deref(), *precision))
            .map(|interval| SqlValue::String(render_interval(interval)))
            .map_err(|error| postgres_interval_input_error(&text, error));
    }
    let Some(PgTypeModifier::Numeric { precision, scale }) = modifier else {
        return Ok(value);
    };
    if pg_type != "numeric" {
        return Ok(value);
    }
    let text = value.to_cell();
    // Already in the column's form — canonical text with exactly `scale`
    // fraction digits and the integer digits within `precision - scale` —
    // parses and rounds to itself: keep the value without the parse and
    // the re-render (the common case: a stored NUMERIC(p,s) plus or minus
    // another value of the same scale).
    if let SqlValue::String(_) = &value {
        if let Some((_, whole, fraction)) = crate::type_codec::canonical_numeric_parts(&text) {
            let target_scale = i32::from(*scale);
            let integer_digits = if whole == "0" { 0 } else { whole.len() as i32 };
            if target_scale >= 0
                && fraction.len() as i32 == target_scale
                && integer_digits <= i32::from(*precision) - target_scale
            {
                return Ok(value);
            }
        }
    }
    let numeric = PgNumeric::from_postgres_text(&text).map_err(|error| match error {
        PgCanonicalValueError::NumericOverflow => {
            SqlError::numeric_value_out_of_range("numeric field overflow")
        }
        _ => SqlError::InvalidTextRepresentation(format!(
            "invalid input syntax for type numeric: \"{text}\""
        )),
    })?;
    numeric
        .with_typmod(*precision, *scale)
        .map(|value| SqlValue::String(value.to_decimal_text()))
        .map_err(|_| SqlError::numeric_value_out_of_range("numeric field overflow"))
}

/// Apply the element modifier without changing dimensions or lower bounds.
fn apply_array_element_modifier(
    value: &mut JsonValue,
    element_type: &str,
    modifier: Option<&PgTypeModifier>,
    explicit_cast: bool,
) -> Result<()> {
    match value {
        JsonValue::Array(values) => {
            for value in values {
                apply_array_element_modifier(value, element_type, modifier, explicit_cast)?;
            }
        }
        JsonValue::Object(object) => {
            let values = object
                .get_mut("$bicdb_array_input")
                .and_then(JsonValue::as_object_mut)
                .and_then(|input| input.get_mut("value"))
                .ok_or_else(|| SqlError::InvalidSql("invalid internal array value".into()))?;
            apply_array_element_modifier(values, element_type, modifier, explicit_cast)?;
        }
        _ => {
            let coerced = apply_pg_type_modifier_with_context(
                json_to_sql_value(value),
                element_type,
                modifier,
                explicit_cast,
            )?;
            *value = sql_value_to_json(coerced);
        }
    }
    Ok(())
}

pub(crate) fn cast_expr_value_with_db(
    db: &BicDb,
    value: SqlValue,
    expr: &Expr,
    data_type: &DataType,
    schema: Option<&TableSchema>,
) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) && data_type.to_string().eq_ignore_ascii_case("record") {
        return Ok(SqlValue::Null);
    }
    let target_pg_type = pg_type_from_data_type(data_type).ok().map(|value| value.0);
    let source_pg_type = projected_expr_pg_type(expr, schema)
        .or_else(|| projected_expr_pg_type_with_db(db, expr))
        .or_else(|| catalog_oid_alias_expr_type(expr).map(str::to_string));
    let internal_array_input = target_pg_type
        .as_deref()
        .is_some_and(|target| target.ends_with("[]"))
        && matches!(
            &value,
            SqlValue::Json(JsonValue::Object(object)) if object.contains_key("$bicdb_array_input")
        );
    if !internal_array_input {
        if let (Some(source), Some(target)) = (source_pg_type.as_deref(), target_pg_type.as_deref())
        {
            validate_catalog_cast(source, target, PgCastContext::Explicit)?;
        }
    }
    if target_pg_type
        .as_deref()
        .is_some_and(|target| matches!(target, "text" | "varchar"))
        && source_pg_type.as_deref().is_some_and(is_oid_alias_type)
    {
        return render_oid_alias_value(
            db,
            source_pg_type
                .as_deref()
                .expect("alias source type checked"),
            &value,
        )
        .map(SqlValue::String);
    }
    if target_pg_type.as_deref() == Some("oid")
        && source_pg_type.as_deref().is_some_and(is_oid_alias_type)
    {
        return resolve_oid_alias_value(
            db,
            source_pg_type
                .as_deref()
                .expect("OID alias source type checked"),
            value,
        );
    }
    if target_pg_type
        .as_deref()
        .is_some_and(|target| matches!(target, "text" | "varchar" | "bpchar" | "name"))
    {
        if let Some(element_type) = source_pg_type
            .as_deref()
            .and_then(|source| source.strip_suffix("[]"))
        {
            if matches!(value, SqlValue::Null) {
                return Ok(SqlValue::Null);
            }
            let delimiter = projected_expr_user_type_with_db(db, expr, schema)
                .filter(|user_type| user_type.array)
                .map(|user_type| user_type.scalar_delimiter())
                .map(Ok)
                .unwrap_or_else(|| pg_type_delimiter_with_db(db, element_type))?;
            return postgres_array_text_value(&value, delimiter).map(SqlValue::String);
        }
    }
    if matches!(target_pg_type.as_deref(), Some("text" | "varchar"))
        && matches!(
            expr,
            Expr::Cast { data_type, .. }
                if pg_type_from_data_type(data_type)
                    .is_ok_and(|(pg_type, _)| pg_type == "regtype")
        )
    {
        return regtype_text_value(db, value);
    }
    if let Some(user_type) = user_type_column_from_data_type(db, data_type)? {
        return cast_value_to_user_type(value, &user_type);
    }
    if let Some(element_type) = target_pg_type
        .as_deref()
        .and_then(|value| value.strip_suffix("[]"))
    {
        if is_oid_alias_type(element_type) {
            return resolve_oid_alias_array_value(db, element_type, value);
        }
    }
    if target_pg_type.as_deref().is_some_and(is_oid_alias_type) {
        return resolve_oid_alias_value(
            db,
            target_pg_type.as_deref().expect("alias type checked"),
            value,
        );
    }
    cast_expr_value(value, expr, data_type, schema)
}

pub(crate) fn catalog_oid_alias_expr_type(expr: &Expr) -> Option<&'static str> {
    match expr {
        Expr::Nested(expr) | Expr::Collate { expr, .. } => catalog_oid_alias_expr_type(expr),
        Expr::Identifier(identifier) => match identifier.value.as_str() {
            "typinput" | "typoutput" | "typreceive" | "typsend" | "typmodin" | "typmodout"
            | "typanalyze" | "typsubscript" => Some("regproc"),
            _ => None,
        },
        Expr::CompoundIdentifier(identifiers) => match identifiers.last()?.value.as_str() {
            "typinput" | "typoutput" | "typreceive" | "typsend" | "typmodin" | "typmodout"
            | "typanalyze" | "typsubscript" => Some("regproc"),
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn projected_expr_user_type_with_db(
    db: &BicDb,
    expr: &Expr,
    schema: Option<&TableSchema>,
) -> Option<UserTypeColumnSchema> {
    if let Some(user_type) = projected_expr_user_type(expr, schema) {
        return Some(user_type);
    }
    match expr {
        Expr::Nested(expr) | Expr::Collate { expr, .. } => {
            projected_expr_user_type_with_db(db, expr, schema)
        }
        Expr::Cast { data_type, .. } | Expr::TypedString(TypedString { data_type, .. }) => {
            user_type_column_from_data_type(db, data_type)
                .ok()
                .flatten()
        }
        _ => None,
    }
}

pub(crate) fn projected_expr_user_type(
    expr: &Expr,
    schema: Option<&TableSchema>,
) -> Option<UserTypeColumnSchema> {
    match expr {
        Expr::Nested(expr) | Expr::Collate { expr, .. } => projected_expr_user_type(expr, schema),
        Expr::Identifier(identifier) => schema?.column(&identifier.value)?.user_type.clone(),
        Expr::CompoundIdentifier(identifiers) => schema?
            .column(&identifiers.last()?.value)?
            .user_type
            .clone(),
        Expr::CompoundFieldAccess { .. } => {
            projected_access_type_state(expr, schema).and_then(|(_, user_type)| user_type)
        }
        _ => None,
    }
}

#[cfg(test)]
mod numeric_typmod_fast_path_tests {
    use super::*;

    #[test]
    fn typmod_fast_path_agrees_with_the_parsing_path() {
        // Every (text, precision, scale) either takes the no-op fast path —
        // in which case the parsing path must return the identical text — or
        // falls through to it; both must agree on errors as well.
        let dir = tempfile::tempdir().unwrap();
        let mut db = bicdb_core::BicDb::open_with_config(
            dir.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let mut sql = crate::SqlSession::new(&mut db);
        let cases = [
            ("12.50", 12u16, 2i16),
            ("-12.50", 12, 2),
            ("0.00", 12, 2),
            ("0", 12, 2),
            ("12.5", 12, 2),
            ("12.505", 12, 2),
            ("123456789012.34", 12, 2),
            ("1234567890.12", 12, 2),
            ("99999.99", 7, 2),
            ("100000.00", 7, 2),
            ("3000", 8, 0),
            ("3000.0", 8, 0),
            ("0.5", 5, 4),
            ("0.5000", 5, 4),
        ];
        for (text, precision, scale) in cases {
            let column_type = format!("numeric({precision},{scale})");
            sql.execute(&format!(
                "DROP TABLE IF EXISTS t; CREATE TABLE t (v {column_type})"
            ))
            .unwrap();
            let inserted = sql.execute(&format!("INSERT INTO t VALUES ('{text}')"));
            let expected = PgNumeric::from_postgres_text(text)
                .unwrap()
                .with_typmod(precision, scale)
                .map(|value| value.to_decimal_text());
            match (inserted, expected) {
                (Ok(_), Ok(expected)) => {
                    let stored =
                        sql.execute("SELECT v::text FROM t").unwrap().rows.remove(0)[0].to_cell();
                    assert_eq!(stored, expected, "{text} into {column_type}");
                }
                (Err(_), Err(_)) => {}
                (inserted, expected) => panic!(
                    "{text} into {column_type}: insert {:?} vs typmod {:?}",
                    inserted.map(|_| ()),
                    expected
                ),
            }
        }
    }
}
