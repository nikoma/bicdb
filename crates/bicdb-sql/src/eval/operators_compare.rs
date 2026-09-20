//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn oid_comparison_value(
    value: &SqlValue,
    value_type: Option<&str>,
    other_type: Option<&str>,
) -> Result<SqlValue> {
    let oid = match value_type {
        Some("oid") => oid_value(value)?,
        Some("int2" | "int4") => {
            let value = sql_value_i64(value).ok_or_else(|| {
                SqlError::undefined_function("OID comparison requires an integer operand")
            })?;
            i32::try_from(value)
                .map_err(|_| SqlError::numeric_value_out_of_range("OID out of range"))?
                as u32
        }
        Some("int8") => u32::try_from(sql_value_i64(value).ok_or_else(|| {
            SqlError::undefined_function("OID comparison requires an integer operand")
        })?)
        .map_err(|_| SqlError::numeric_value_out_of_range("OID out of range"))?,
        None if other_type == Some("oid") => match value {
            SqlValue::String(value) => parse_pg_oid(value).map_err(|error| match error {
                PgOidParseError::InvalidSyntax => {
                    SqlError::invalid_text_representation("oid", format!("\"{value}\""))
                }
                PgOidParseError::OutOfRange => {
                    SqlError::numeric_value_out_of_range("OID out of range")
                }
            })?,
            SqlValue::Int(value) => i32::try_from(*value)
                .map(|value| value as u32)
                .or_else(|_| u32::try_from(*value))
                .map_err(|_| SqlError::numeric_value_out_of_range("OID out of range"))?,
            _ => {
                return Err(SqlError::undefined_function(
                    "OID comparison requires an integer operand",
                ));
            }
        },
        other => {
            return Err(SqlError::undefined_function(format!(
                "operator does not exist: {} = oid",
                other.unwrap_or("unknown")
            )));
        }
    };
    Ok(SqlValue::Int(i64::from(oid)))
}

pub(crate) fn type_without_comparison_operators(pg_type: Option<&str>) -> Option<&'static str> {
    match pg_type {
        Some("xml") => Some("xml"),
        Some("refcursor") => Some("refcursor"),
        Some("pg_snapshot") => Some("pg_snapshot"),
        Some("txid_snapshot") => Some("txid_snapshot"),
        _ => None,
    }
}

pub(crate) fn reject_undefined_comparison(
    left_type: Option<&str>,
    op: &BinaryOperator,
    right_type: Option<&str>,
) -> Result<()> {
    let unsupported = type_without_comparison_operators(left_type)
        .or_else(|| type_without_comparison_operators(right_type));
    let Some(unsupported) = unsupported else {
        return Ok(());
    };
    let left = if left_type == Some(unsupported)
        || left_type.is_none() && right_type == Some(unsupported)
    {
        unsupported
    } else {
        left_type.unwrap_or("unknown")
    };
    let right = if right_type == Some(unsupported)
        || right_type.is_none() && left_type == Some(unsupported)
    {
        unsupported
    } else {
        right_type.unwrap_or("unknown")
    };
    Err(SqlError::undefined_function(format!(
        "operator does not exist: {left} {op} {right}"
    )))
}

pub(crate) fn comparison_from_ordering(op: &BinaryOperator, ordering: Ordering) -> bool {
    match op {
        BinaryOperator::Eq => ordering == Ordering::Equal,
        BinaryOperator::NotEq => ordering != Ordering::Equal,
        BinaryOperator::Gt => ordering == Ordering::Greater,
        BinaryOperator::GtEq => ordering != Ordering::Less,
        BinaryOperator::Lt => ordering == Ordering::Less,
        BinaryOperator::LtEq => ordering != Ordering::Greater,
        _ => false,
    }
}

pub(crate) fn comparison_collation(
    left: &Expr,
    right: &Expr,
    schema: Option<&TableSchema>,
) -> Result<Option<String>> {
    let left = expr_collation(left, schema)?;
    let right = expr_collation(right, schema)?;
    match (left, right) {
        (Some(left), Some(right)) if left != "default" && right != "default" && left != right => {
            Err(SqlError::data_exception(
                "42P21",
                format!(
                    "collation mismatch between explicit collations \"{left}\" and \"{right}\""
                ),
                None,
            ))
        }
        (Some(left), Some(right)) if left == "default" => Ok(Some(right)),
        (Some(left), _) => Ok(Some(left)),
        (_, Some(right)) => Ok(Some(right)),
        _ => Ok(None),
    }
}

pub(crate) fn expr_collation(expr: &Expr, schema: Option<&TableSchema>) -> Result<Option<String>> {
    match expr {
        Expr::Collate { collation, .. } => normalize_column_collation(collation).map(Some),
        Expr::Identifier(ident) => Ok(schema
            .and_then(|schema| schema.column(&ident.value))
            .filter(|column| pg_type_is_collatable(&column.pg_type))
            .map(|column| {
                column
                    .collation
                    .clone()
                    .unwrap_or_else(|| "default".to_string())
            })),
        Expr::CompoundIdentifier(idents) => Ok(idents
            .last()
            .and_then(|ident| schema.and_then(|schema| schema.column(&ident.value)))
            .filter(|column| pg_type_is_collatable(&column.pg_type))
            .map(|column| {
                column
                    .collation
                    .clone()
                    .unwrap_or_else(|| "default".to_string())
            })),
        Expr::Nested(expr) => expr_collation(expr, schema),
        _ => Ok(None),
    }
}

pub(crate) fn explicit_expr_collation(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Collate { collation, .. } => normalize_column_collation(collation).ok(),
        Expr::Nested(expr) => explicit_expr_collation(expr),
        _ => None,
    }
}

pub(crate) fn locale_text_ordering(collation: &str, left: &str, right: &str) -> Option<Ordering> {
    if collation != "en-x-icu" {
        return None;
    }
    thread_local! {
        static EN_COLLATOR: RefCell<Option<CollatorBorrowed<'static>>> = RefCell::new(
            Collator::try_new(Default::default(), Default::default()).ok()
        );
    }
    let ordering = EN_COLLATOR.with(|collator| {
        collator
            .borrow()
            .as_ref()
            .map(|collator| collator.compare(left, right))
    })?;
    Some(if ordering == Ordering::Equal {
        left.cmp(right)
    } else {
        ordering
    })
}

pub(crate) fn eval_money_arithmetic(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let left_money = matches!(left_type, Some("money"));
    let right_money = matches!(right_type, Some("money"));
    let result = match op {
        BinaryOperator::Plus | BinaryOperator::Minus if left_money && right_money => {
            let left = money_cents(&left)?;
            let right = money_cents(&right)?;
            let cents = if matches!(op, BinaryOperator::Plus) {
                left.checked_add(right)
            } else {
                left.checked_sub(right)
            }
            .ok_or_else(SqlError::money_out_of_range)?;
            SqlValue::String(crate::pg_money_text_from_cents(cents))
        }
        BinaryOperator::Multiply if left_money ^ right_money => {
            let (money, scalar, scalar_type) = if left_money {
                (&left, &right, right_type)
            } else {
                (&right, &left, left_type)
            };
            if !scalar_type.is_some_and(is_money_scalar_pg_type) {
                return Err(money_operator_error(op, left_type, right_type));
            }
            eval_money_scaled(money, scalar, op)?
        }
        BinaryOperator::Divide if left_money && right_money => {
            let right = money_cents(&right)?;
            if right == 0 {
                return Err(division_by_zero());
            }
            SqlValue::Float(money_cents(&left)? as f64 / right as f64)
        }
        BinaryOperator::Divide if left_money && right_type.is_some_and(is_money_scalar_pg_type) => {
            eval_money_scaled(&left, &right, op)?
        }
        _ => return Err(money_operator_error(op, left_type, right_type)),
    };
    Ok(result)
}

pub(crate) fn eval_money_scaled(
    money: &SqlValue,
    scalar: &SqlValue,
    op: &BinaryOperator,
) -> Result<SqlValue> {
    let money = Decimal {
        mantissa: BigInt::from(money_cents(money)?),
        scale: 2,
    };
    let scalar = match scalar {
        SqlValue::Float(value) => parse_decimal(&canonical_float_decimal(*value)?)?,
        value => decimal_value(value)
            .ok_or_else(|| SqlError::InvalidTextRepresentation("invalid money scalar".into()))??,
    };
    let value = eval_decimal_arithmetic(money, op, scalar)?;
    let cents = crate::pg_money_cents_from_text(&value.to_cell()).map_err(|error| match error {
        PgCanonicalValueError::NumericOverflow => SqlError::money_out_of_range(),
        _ => SqlError::InvalidTextRepresentation("invalid money arithmetic result".into()),
    })?;
    Ok(SqlValue::String(crate::pg_money_text_from_cents(cents)))
}

pub(crate) fn money_cents(value: &SqlValue) -> Result<i64> {
    crate::pg_money_cents_from_text(&value.to_cell()).map_err(|error| match error {
        PgCanonicalValueError::NumericOverflow => SqlError::money_out_of_range(),
        _ => SqlError::InvalidTextRepresentation(format!(
            "invalid input syntax for type money: \"{}\"",
            value.to_cell()
        )),
    })
}

pub(crate) fn is_money_scalar_pg_type(pg_type: &str) -> bool {
    matches!(
        pg_type,
        "int2" | "int4" | "int8" | "numeric" | "float4" | "float8"
    )
}

pub(crate) fn money_operator_error(
    op: &BinaryOperator,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> SqlError {
    SqlError::undefined_function(format!(
        "operator does not exist: {} {op} {}",
        left_type.unwrap_or("unknown"),
        right_type.unwrap_or("unknown")
    ))
}

pub(crate) fn eval_pg_numeric_arithmetic(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
) -> Result<SqlValue> {
    // Same fast path as the untyped arithmetic: two small canonical decimals
    // under +, - or * produce the identical text the PgNumeric path renders,
    // without parsing into PgNumeric/BigInt.
    if let (Some(small_left), Some(small_right)) =
        (small_decimal_value(&left), small_decimal_value(&right))
    {
        if let Some(value) = small_decimal_arithmetic(small_left, op, small_right) {
            return Ok(value);
        }
    }
    let left = pg_numeric_from_sql_value(left)?;
    let right = pg_numeric_from_sql_value(right)?;
    if matches!(left, PgNumeric::NaN) || matches!(right, PgNumeric::NaN) {
        return Ok(SqlValue::String("NaN".to_string()));
    }
    if let (
        PgNumeric::Finite { .. },
        PgNumeric::Finite {
            coefficient: right_coefficient,
            ..
        },
    ) = (&left, &right)
    {
        if matches!(op, BinaryOperator::Divide | BinaryOperator::Modulo) && right_coefficient == "0"
        {
            return Err(division_by_zero());
        }
        let result = eval_decimal_arithmetic(
            decimal_from_pg_numeric(left)?,
            op,
            decimal_from_pg_numeric(right)?,
        )?;
        let SqlValue::String(text) = &result else {
            unreachable!();
        };
        PgNumeric::from_postgres_text(text).map_err(|error| match error {
            PgCanonicalValueError::NumericOverflow => {
                SqlError::numeric_value_out_of_range("value overflows numeric format")
            }
            _ => SqlError::InvalidTextRepresentation(format!(
                "invalid input syntax for type numeric: \"{text}\""
            )),
        })?;
        return Ok(result);
    }

    let result = match op {
        BinaryOperator::Plus | BinaryOperator::Minus => {
            let right = if matches!(op, BinaryOperator::Minus) {
                negate_pg_numeric(right)
            } else {
                right
            };
            match (left, right) {
                (PgNumeric::PositiveInfinity, PgNumeric::NegativeInfinity)
                | (PgNumeric::NegativeInfinity, PgNumeric::PositiveInfinity) => PgNumeric::NaN,
                (value @ (PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity), _)
                | (_, value @ (PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity)) => value,
                _ => unreachable!(),
            }
        }
        BinaryOperator::Multiply => {
            if pg_numeric_is_zero(&left) || pg_numeric_is_zero(&right) {
                PgNumeric::NaN
            } else if pg_numeric_is_negative(&left) == pg_numeric_is_negative(&right) {
                PgNumeric::PositiveInfinity
            } else {
                PgNumeric::NegativeInfinity
            }
        }
        BinaryOperator::Divide => {
            if pg_numeric_is_zero(&right) {
                return Err(division_by_zero());
            }
            match (&left, &right) {
                (
                    PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity,
                    PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity,
                ) => PgNumeric::NaN,
                (PgNumeric::Finite { .. }, _) => {
                    PgNumeric::finite(false, "0", 0).expect("zero is a valid numeric")
                }
                _ if pg_numeric_is_negative(&left) == pg_numeric_is_negative(&right) => {
                    PgNumeric::PositiveInfinity
                }
                _ => PgNumeric::NegativeInfinity,
            }
        }
        BinaryOperator::Modulo => match (&left, &right) {
            (PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity, _) => PgNumeric::NaN,
            (value @ PgNumeric::Finite { .. }, _) => value.clone(),
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };
    Ok(SqlValue::String(result.to_decimal_text()))
}

pub(crate) fn pg_numeric_from_sql_value(value: SqlValue) -> Result<PgNumeric> {
    let value = cast_value_to_pg_type(value, "numeric")?.to_cell();
    PgNumeric::from_postgres_text(&value).map_err(|error| match error {
        PgCanonicalValueError::NumericOverflow => {
            SqlError::numeric_value_out_of_range("value overflows numeric format")
        }
        _ => SqlError::InvalidTextRepresentation(format!(
            "invalid input syntax for type numeric: \"{value}\""
        )),
    })
}

pub(crate) fn decimal_from_pg_numeric(value: PgNumeric) -> Result<Decimal> {
    let PgNumeric::Finite {
        negative,
        coefficient,
        display_scale,
    } = value
    else {
        return Err(SqlError::InvalidSql(
            "numeric value is not finite".to_string(),
        ));
    };
    let mut mantissa = BigInt::parse_bytes(coefficient.as_bytes(), 10)
        .expect("PgNumeric coefficients contain decimal digits");
    if display_scale < 0 {
        mantissa *= pow10_bigint(display_scale.unsigned_abs());
    }
    if negative {
        mantissa = -mantissa;
    }
    Ok(Decimal {
        mantissa,
        scale: display_scale.max(0) as u32,
    })
}

pub(crate) fn negate_pg_numeric(value: PgNumeric) -> PgNumeric {
    match value {
        PgNumeric::Finite {
            negative,
            coefficient,
            display_scale,
        } => PgNumeric::finite(!negative, coefficient, display_scale)
            .expect("existing PgNumeric values remain canonical when negated"),
        PgNumeric::PositiveInfinity => PgNumeric::NegativeInfinity,
        PgNumeric::NegativeInfinity => PgNumeric::PositiveInfinity,
        PgNumeric::NaN => PgNumeric::NaN,
    }
}

pub(crate) fn pg_numeric_is_zero(value: &PgNumeric) -> bool {
    matches!(value, PgNumeric::Finite { coefficient, .. } if coefficient == "0")
}

pub(crate) fn pg_numeric_is_negative(value: &PgNumeric) -> bool {
    matches!(
        value,
        PgNumeric::NegativeInfinity | PgNumeric::Finite { negative: true, .. }
    )
}

pub(crate) fn eval_float_arithmetic_value(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
    pg_type: &str,
) -> Result<SqlValue> {
    let left = sql_value_f64(&left).ok_or_else(|| {
        SqlError::InvalidSql("floating-point arithmetic requires numeric operands".to_string())
    })?;
    let right = sql_value_f64(&right).ok_or_else(|| {
        SqlError::InvalidSql("floating-point arithmetic requires numeric operands".to_string())
    })?;
    if matches!(op, BinaryOperator::Divide | BinaryOperator::Modulo) && right == 0.0 {
        return Err(division_by_zero());
    }
    if pg_type == "float4" {
        let left = left as f32;
        let right = right as f32;
        let result = match op {
            BinaryOperator::Plus => left + right,
            BinaryOperator::Minus => left - right,
            BinaryOperator::Multiply => left * right,
            BinaryOperator::Divide => left / right,
            BinaryOperator::Modulo => left % right,
            _ => unreachable!(),
        };
        validate_float_arithmetic_result(f64::from(left), op, f64::from(right), f64::from(result))?;
        Ok(SqlValue::Float(f64::from(result)))
    } else {
        let result = match op {
            BinaryOperator::Plus => left + right,
            BinaryOperator::Minus => left - right,
            BinaryOperator::Multiply => left * right,
            BinaryOperator::Divide => left / right,
            BinaryOperator::Modulo => left % right,
            _ => unreachable!(),
        };
        validate_float_arithmetic_result(left, op, right, result)?;
        Ok(SqlValue::Float(result))
    }
}

pub(crate) fn validate_float_arithmetic_result(
    left: f64,
    op: &BinaryOperator,
    right: f64,
    result: f64,
) -> Result<()> {
    if result.is_infinite() && left.is_finite() && right.is_finite() {
        return Err(float_arithmetic_out_of_range("overflow"));
    }
    let underflow = result == 0.0
        && match op {
            BinaryOperator::Multiply => left != 0.0 && right != 0.0,
            BinaryOperator::Divide => left != 0.0 && right.is_finite(),
            _ => false,
        };
    if underflow {
        return Err(float_arithmetic_out_of_range("underflow"));
    }
    Ok(())
}

pub(crate) fn float_arithmetic_out_of_range(kind: &str) -> SqlError {
    SqlError::DataException {
        sqlstate: "22003",
        message: format!("value out of range: {kind}"),
        data_type: None,
    }
}

pub(crate) fn remap_integer_overflow(error: SqlError, pg_type: Option<&str>) -> SqlError {
    match (error, pg_type) {
        (
            SqlError::DataException {
                sqlstate: "22003", ..
            },
            Some(pg_type @ ("int2" | "smallint" | "int4" | "int" | "integer" | "int8" | "bigint")),
        ) => integer_out_of_range(pg_type),
        (error, _) => error,
    }
}

pub(crate) fn division_by_zero() -> SqlError {
    SqlError::DataException {
        sqlstate: "22012",
        message: "division by zero".to_string(),
        data_type: None,
    }
}

pub(crate) fn eval_json_concat(left: &SqlValue, right: &SqlValue) -> Option<SqlValue> {
    match (left, right) {
        (SqlValue::Json(JsonValue::Object(left)), SqlValue::Json(JsonValue::Object(right))) => {
            let mut merged = left.clone();
            for (key, value) in right {
                merged.insert(key.clone(), value.clone());
            }
            Some(SqlValue::Json(JsonValue::Object(merged)))
        }
        (SqlValue::Json(JsonValue::Array(left)), SqlValue::Json(JsonValue::Array(right))) => {
            let mut merged = left.clone();
            merged.extend(right.iter().cloned());
            Some(SqlValue::Json(JsonValue::Array(merged)))
        }
        (SqlValue::Json(JsonValue::Array(left)), right) => {
            let mut merged = left.clone();
            merged.push(sql_value_to_json(right.clone()));
            Some(SqlValue::Json(JsonValue::Array(merged)))
        }
        (left, SqlValue::Json(JsonValue::Array(right))) => {
            let mut merged = vec![sql_value_to_json(left.clone())];
            merged.extend(right.iter().cloned());
            Some(SqlValue::Json(JsonValue::Array(merged)))
        }
        (SqlValue::Json(left), SqlValue::Json(right)) => {
            Some(SqlValue::Json(JsonValue::Array(vec![
                left.clone(),
                right.clone(),
            ])))
        }
        (SqlValue::Json(left), right) => Some(SqlValue::Json(JsonValue::Array(vec![
            left.clone(),
            sql_value_to_json(right.clone()),
        ]))),
        (left, SqlValue::Json(right)) => Some(SqlValue::Json(JsonValue::Array(vec![
            sql_value_to_json(left.clone()),
            right.clone(),
        ]))),
        _ => None,
    }
}

pub(crate) fn eval_containment_truth(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
) -> Result<Option<bool>> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(None);
    }
    let left_range_type = inferred_range_type_from_value(&left);
    let right_range_type = inferred_range_type_from_value(&right);
    if matches!(op, BinaryOperator::AtArrow) {
        if let Some(range_type) = left_range_type {
            let range = range_from_sql_value(&left, range_type)?.unwrap();
            return if right_range_type == Some(range_type) {
                let right = range_from_sql_value(&right, range_type)?.unwrap();
                Ok(Some(range_contains_range(&range, &right)))
            } else {
                Ok(Some(range_contains_element(
                    &range,
                    &range_element_from_value(range_type, &right)?,
                )))
            };
        }
    } else if matches!(op, BinaryOperator::ArrowAt) {
        if let Some(range_type) = right_range_type {
            let range = range_from_sql_value(&right, range_type)?.unwrap();
            return if left_range_type == Some(range_type) {
                let left = range_from_sql_value(&left, range_type)?.unwrap();
                Ok(Some(range_contains_range(&range, &left)))
            } else {
                Ok(Some(range_contains_element(
                    &range,
                    &range_element_from_value(range_type, &left)?,
                )))
            };
        }
    }
    let left = sql_value_to_json_ref(&left);
    let right = sql_value_to_json_ref(&right);
    match op {
        BinaryOperator::AtArrow => Ok(Some(json_contains(&left, &right)?)),
        BinaryOperator::ArrowAt => Ok(Some(json_contains(&right, &left)?)),
        other => Err(SqlError::Unsupported(format!(
            "unsupported containment operator {other}"
        ))),
    }
}

pub(crate) fn eval_arithmetic_value(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
) -> Result<SqlValue> {
    eval_arithmetic_value_ref(&left, op, &right)
}

/// Arithmetic does not consume its operands. Prepared expressions can borrow
/// columns and variables directly instead of cloning their numeric text.
pub(crate) fn eval_arithmetic_value_ref(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
) -> Result<SqlValue> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    if let Some(value) = eval_interval_scale(&left, op, &right) {
        return Ok(value);
    }
    if let (&SqlValue::Int(left), &SqlValue::Int(right)) = (left, right) {
        return match op {
            BinaryOperator::Plus => left
                .checked_add(right)
                .map(SqlValue::Int)
                .ok_or_else(|| integer_out_of_range("int8")),
            BinaryOperator::Minus => left
                .checked_sub(right)
                .map(SqlValue::Int)
                .ok_or_else(|| integer_out_of_range("int8")),
            BinaryOperator::Multiply => left
                .checked_mul(right)
                .map(SqlValue::Int)
                .ok_or_else(|| integer_out_of_range("int8")),
            BinaryOperator::Modulo => {
                if right == 0 {
                    Err(division_by_zero())
                } else if left == i64::MIN && right == -1 {
                    Ok(SqlValue::Int(0))
                } else {
                    left.checked_rem(right)
                        .map(SqlValue::Int)
                        .ok_or_else(|| integer_out_of_range("int8"))
                }
            }
            BinaryOperator::Divide => {
                if right == 0 {
                    Err(division_by_zero())
                } else {
                    left.checked_div(right)
                        .map(SqlValue::Int)
                        .ok_or_else(|| integer_out_of_range("int8"))
                }
            }
            _ => unreachable!(),
        };
    }

    if let (Some(small_left), Some(small_right)) =
        (small_decimal_value(&left), small_decimal_value(&right))
    {
        if let Some(value) = small_decimal_arithmetic(small_left, op, small_right) {
            return Ok(value);
        }
    }
    if let (Some(left), Some(right)) = (decimal_value(&left), decimal_value(&right)) {
        return eval_decimal_arithmetic(left?, op, right?);
    }
    if let Some(value) = eval_date_interval_arithmetic(&left, op, &right)? {
        return Ok(value);
    }

    let left = sql_value_f64(&left)
        .ok_or_else(|| SqlError::InvalidSql("arithmetic operands must be numeric".to_string()))?;
    let right = sql_value_f64(&right)
        .ok_or_else(|| SqlError::InvalidSql("arithmetic operands must be numeric".to_string()))?;
    match op {
        BinaryOperator::Plus => Ok(SqlValue::Float(left + right)),
        BinaryOperator::Minus => Ok(SqlValue::Float(left - right)),
        BinaryOperator::Multiply => Ok(SqlValue::Float(left * right)),
        BinaryOperator::Divide => {
            if right == 0.0 {
                Err(division_by_zero())
            } else {
                Ok(SqlValue::Float(left / right))
            }
        }
        BinaryOperator::Modulo => {
            if right == 0.0 {
                Err(division_by_zero())
            } else {
                Ok(SqlValue::Float(left % right))
            }
        }
        _ => unreachable!(),
    }
}

pub(crate) fn eval_interval_scale(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
) -> Option<SqlValue> {
    match op {
        BinaryOperator::Multiply => {
            if let (Some(multiplier), SqlValue::String(interval)) = (sql_value_f64(left), right) {
                if is_unambiguous_interval_text(interval) {
                    return PgInterval::from_postgres_text(interval)
                        .and_then(|interval| interval.checked_scale(multiplier))
                        .ok()
                        .map(render_interval)
                        .map(SqlValue::String);
                }
            }
            if let (SqlValue::String(interval), Some(multiplier)) = (left, sql_value_f64(right)) {
                if is_unambiguous_interval_text(interval) {
                    return PgInterval::from_postgres_text(interval)
                        .and_then(|interval| interval.checked_scale(multiplier))
                        .ok()
                        .map(render_interval)
                        .map(SqlValue::String);
                }
            }
            None
        }
        BinaryOperator::Divide => {
            if let (SqlValue::String(interval), Some(divisor)) = (left, sql_value_f64(right)) {
                if is_unambiguous_interval_text(interval) {
                    if divisor == 0.0 {
                        return None;
                    }
                    return PgInterval::from_postgres_text(interval)
                        .and_then(|interval| interval.checked_div(divisor))
                        .ok()
                        .map(render_interval)
                        .map(SqlValue::String);
                }
            }
            None
        }
        _ => None,
    }
}

pub(crate) fn is_unambiguous_interval_text(value: &str) -> bool {
    let value = value.trim();
    value.contains(':')
        || value
            .chars()
            .any(|character| character.is_ascii_alphabetic())
        || value.split_whitespace().count() > 1
}

pub(crate) fn compare_values(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
) -> Result<Option<bool>> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(None);
    }
    if let (SqlValue::Composite(left), SqlValue::Composite(right)) = (left, right) {
        let left = left
            .fields
            .iter()
            .map(|field| field.value.clone())
            .collect::<Vec<_>>();
        let right = right
            .fields
            .iter()
            .map(|field| field.value.clone())
            .collect::<Vec<_>>();
        return compare_tuple_values(&left, op, &right);
    }
    let ordering = value_ordering(left, right);
    let equal = values_equal(left, right);
    Ok(Some(match op {
        BinaryOperator::Eq => equal,
        BinaryOperator::NotEq => !equal,
        BinaryOperator::Gt => ordering.is_some_and(|ordering| ordering == Ordering::Greater),
        BinaryOperator::GtEq => {
            equal || ordering.is_some_and(|ordering| ordering == Ordering::Greater)
        }
        BinaryOperator::Lt => ordering.is_some_and(|ordering| ordering == Ordering::Less),
        BinaryOperator::LtEq => {
            equal || ordering.is_some_and(|ordering| ordering == Ordering::Less)
        }
        _ => false,
    }))
}

pub(crate) fn eval_tuple_comparison<F>(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    eval: &mut F,
) -> Result<Option<Option<bool>>>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    let left = eval_tuple_values(left, eval)?;
    let right = eval_tuple_values(right, eval)?;
    match (left, right) {
        (None, None) => Ok(None),
        (Some(left), Some(right)) => compare_tuple_values(&left, op, &right).map(Some),
        _ => Err(row_value_side_error()),
    }
}

pub(crate) fn eval_tuple_in_list_truth<F>(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    eval: &mut F,
) -> Result<Option<Option<bool>>>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    let Some(value) = eval_tuple_values(expr, eval)? else {
        return Ok(None);
    };
    let mut saw_null = false;
    for candidate in list {
        let Some(candidate) = eval_tuple_values(candidate, eval)? else {
            return Err(row_value_side_error());
        };
        match compare_tuple_values(&value, &BinaryOperator::Eq, &candidate)? {
            Some(true) => return Ok(Some(Some(!negated))),
            Some(false) => {}
            None => saw_null = true,
        }
    }
    Ok(Some(if saw_null { None } else { Some(negated) }))
}

pub(crate) fn eval_tuple_between_truth<F>(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    eval: &mut F,
) -> Result<Option<Option<bool>>>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    let Some(value) = eval_tuple_values(expr, eval)? else {
        return Ok(None);
    };
    let Some(low) = eval_tuple_values(low, eval)? else {
        return Err(row_value_side_error());
    };
    let Some(high) = eval_tuple_values(high, eval)? else {
        return Err(row_value_side_error());
    };
    let lower = compare_tuple_values(&value, &BinaryOperator::GtEq, &low)?;
    let upper = compare_tuple_values(&value, &BinaryOperator::LtEq, &high)?;
    Ok(Some(sql_and(lower, upper).map(|matched| {
        if negated {
            !matched
        } else {
            matched
        }
    })))
}

pub(crate) fn eval_tuple_not_distinct<F>(
    left: &Expr,
    right: &Expr,
    eval: &mut F,
) -> Result<Option<bool>>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    let left = eval_tuple_values(left, eval)?;
    let right = eval_tuple_values(right, eval)?;
    match (left, right) {
        (None, None) => Ok(None),
        (Some(left), Some(right)) => tuple_values_not_distinct(&left, &right).map(Some),
        _ => Err(row_value_side_error()),
    }
}

pub(crate) fn eval_tuple_values<F>(expr: &Expr, eval: &mut F) -> Result<Option<Vec<SqlValue>>>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    match unwrap_nested_expr(expr) {
        Expr::Tuple(exprs) => exprs.iter().map(eval).collect::<Result<Vec<_>>>().map(Some),
        _ => Ok(None),
    }
}

pub(crate) fn eval_tuple_in_rows_truth(
    value: &[SqlValue],
    rows: Vec<Vec<SqlValue>>,
    negated: bool,
) -> Result<Option<bool>> {
    let mut saw_null = false;
    for row in rows {
        match compare_tuple_values(value, &BinaryOperator::Eq, &row)? {
            Some(true) => return Ok(Some(!negated)),
            Some(false) => {}
            None => saw_null = true,
        }
    }
    Ok(if saw_null { None } else { Some(negated) })
}

pub(crate) fn compare_tuple_values(
    left: &[SqlValue],
    op: &BinaryOperator,
    right: &[SqlValue],
) -> Result<Option<bool>> {
    if left.len() != right.len() {
        return Err(row_value_length_error());
    }
    match op {
        BinaryOperator::Eq => tuple_values_equal(left, right),
        BinaryOperator::NotEq => tuple_values_equal(left, right).map(|value| value.map(|v| !v)),
        BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq => {
            tuple_values_ordered(left, op, right)
        }
        _ => Err(SqlError::Unsupported(format!(
            "unsupported row value comparison operator {op}"
        ))),
    }
}

pub(crate) fn tuple_values_equal(left: &[SqlValue], right: &[SqlValue]) -> Result<Option<bool>> {
    if left.len() != right.len() {
        return Err(row_value_length_error());
    }
    let mut saw_null = false;
    for (left, right) in left.iter().zip(right) {
        match compare_values(left, &BinaryOperator::Eq, right)? {
            Some(true) => {}
            Some(false) => return Ok(Some(false)),
            None => saw_null = true,
        }
    }
    Ok(if saw_null { None } else { Some(true) })
}

pub(crate) fn tuple_values_ordered(
    left: &[SqlValue],
    op: &BinaryOperator,
    right: &[SqlValue],
) -> Result<Option<bool>> {
    if left.len() != right.len() {
        return Err(row_value_length_error());
    }
    for (left, right) in left.iter().zip(right) {
        match compare_values(left, &BinaryOperator::Eq, right)? {
            Some(true) => continue,
            Some(false) => return compare_values(left, op, right),
            None => return Ok(None),
        }
    }
    Ok(Some(matches!(
        op,
        BinaryOperator::GtEq | BinaryOperator::LtEq
    )))
}

pub(crate) fn tuple_values_not_distinct(left: &[SqlValue], right: &[SqlValue]) -> Result<bool> {
    if left.len() != right.len() {
        return Err(row_value_length_error());
    }
    Ok(left
        .iter()
        .zip(right)
        .all(|(left, right)| values_not_distinct(left, right)))
}

pub(crate) fn row_value_side_error() -> SqlError {
    SqlError::InvalidSql("row value comparisons require row constructors on both sides".to_string())
}

pub(crate) fn row_value_length_error() -> SqlError {
    SqlError::InvalidSql("row value expressions must have the same number of fields".to_string())
}

pub(crate) fn values_equal(left: &SqlValue, right: &SqlValue) -> bool {
    match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => false,
        (SqlValue::Composite(left), SqlValue::Composite(right)) => {
            left.fields.len() == right.fields.len()
                && left
                    .fields
                    .iter()
                    .zip(&right.fields)
                    .all(|(left, right)| values_equal(&left.value, &right.value))
        }
        (SqlValue::Json(left), SqlValue::Json(right)) => json_values_equal(left, right),
        _ => {
            if let (Some(left), Some(right)) = (
                array_values_for_comparison(left),
                array_values_for_comparison(right),
            ) {
                return left.len() == right.len()
                    && left
                        .iter()
                        .zip(right.iter())
                        .all(|(left, right)| values_equal(left, right));
            }
            values_equal_scalar(left, right)
        }
    }
}

pub(crate) fn values_equal_scalar(left: &SqlValue, right: &SqlValue) -> bool {
    match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => false,
        (SqlValue::Bool(left), SqlValue::Bool(right)) => left == right,
        (SqlValue::Bool(left), right) => sql_value_bool(right).is_some_and(|right| *left == right),
        (left, SqlValue::Bool(right)) => sql_value_bool(left).is_some_and(|left| left == *right),
        (SqlValue::String(left), SqlValue::String(right)) => left == right,
        _ if small_decimal_value(left)
            .zip(small_decimal_value(right))
            .and_then(|(l, r)| small_decimal_cmp(l, r))
            .is_some() =>
        {
            small_decimal_value(left)
                .zip(small_decimal_value(right))
                .and_then(|(l, r)| small_decimal_cmp(l, r))
                == Some(Ordering::Equal)
        }
        _ if decimal_value(left).is_some() && decimal_value(right).is_some() => {
            match (decimal_value(left), decimal_value(right)) {
                (Some(Ok(left)), Some(Ok(right))) => left.cmp(&right) == Ordering::Equal,
                _ => false,
            }
        }
        _ => match (left.as_f64(), right.as_f64()) {
            (Some(left), Some(right)) => postgres_float_ordering(left, right) == Ordering::Equal,
            _ => left == right,
        },
    }
}

pub(crate) fn array_values_for_comparison(value: &SqlValue) -> Option<Vec<SqlValue>> {
    match value {
        SqlValue::Json(JsonValue::Array(values)) => {
            Some(values.iter().map(json_to_sql_value).collect())
        }
        SqlValue::String(value) if value.trim_start().starts_with(['{', '[']) => {
            parse_array_literal(value).ok().map(|values| {
                values
                    .into_iter()
                    .map(|value| json_to_sql_value(&value))
                    .collect()
            })
        }
        _ => None,
    }
}

pub(crate) fn values_not_distinct(left: &SqlValue, right: &SqlValue) -> bool {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => true,
        (SqlValue::Null, _) | (_, SqlValue::Null) => false,
        (SqlValue::Composite(left), SqlValue::Composite(right)) => {
            left.fields.len() == right.fields.len()
                && left
                    .fields
                    .iter()
                    .zip(&right.fields)
                    .all(|(left, right)| values_not_distinct(&left.value, &right.value))
        }
        _ => values_equal(left, right),
    }
}

pub(crate) fn value_is_null_predicate(value: &SqlValue) -> bool {
    match value {
        SqlValue::Null => true,
        SqlValue::Composite(composite) => composite
            .fields
            .iter()
            .all(|field| matches!(field.value, SqlValue::Null)),
        SqlValue::Json(value) => pg_composite_from_array_json(value).is_some_and(|composite| {
            composite
                .fields
                .iter()
                .all(|field| matches!(field.value, SqlValue::Null))
        }),
        _ => false,
    }
}

pub(crate) fn value_is_not_null_predicate(value: &SqlValue) -> bool {
    match value {
        SqlValue::Null => false,
        SqlValue::Composite(composite) => composite
            .fields
            .iter()
            .all(|field| !matches!(field.value, SqlValue::Null)),
        SqlValue::Json(value) => pg_composite_from_array_json(value).map_or(true, |composite| {
            composite
                .fields
                .iter()
                .all(|field| !matches!(field.value, SqlValue::Null))
        }),
        _ => true,
    }
}

pub(crate) fn sql_and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

pub(crate) fn sql_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

pub(crate) fn sql_not(value: Option<bool>) -> Option<bool> {
    value.map(|value| !value)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Decimal {
    pub(crate) mantissa: BigInt,
    pub(crate) scale: u32,
}

impl Decimal {
    pub(crate) fn neg(self) -> Self {
        Self {
            mantissa: -self.mantissa,
            scale: self.scale,
        }
    }

    pub(crate) fn cmp(&self, other: &Self) -> Ordering {
        let scale = self.scale.max(other.scale);
        let left = &self.mantissa * pow10_bigint(scale - self.scale);
        let right = &other.mantissa * pow10_bigint(scale - other.scale);
        left.cmp(&right)
    }

    pub(crate) fn format(self) -> String {
        let negative = self.mantissa.is_negative();
        let digits = self.mantissa.abs().to_string();
        let rendered = if self.scale == 0 {
            digits
        } else {
            let scale = self.scale as usize;
            if digits.len() <= scale {
                format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
            } else {
                let split = digits.len() - scale;
                format!("{}.{}", &digits[..split], &digits[split..])
            }
        };
        let rendered = if rendered.is_empty() {
            "0".to_string()
        } else {
            rendered
        };
        if negative && rendered != "0" {
            format!("-{rendered}")
        } else {
            rendered
        }
    }
}

/// Rounds a decimal to `scale` fractional digits, half away from zero —
/// PostgreSQL's `round(numeric, int)`. A negative scale rounds left of the
/// decimal point (`round(1234, -2)` = 1200).
pub(crate) fn round_decimal_half_away(value: Decimal, scale: i64) -> String {
    let target = scale.clamp(-1000, 1000);
    let current = i64::from(value.scale);
    if target >= current {
        return value.format();
    }
    let drop = (current - target) as u32;
    let divisor = pow10_bigint(drop);
    let negative = value.mantissa.is_negative();
    let magnitude = value.mantissa.abs();
    let (quotient, remainder) = (&magnitude / &divisor, &magnitude % &divisor);
    let mut rounded = quotient;
    if &remainder * 2_i32 >= divisor {
        rounded += 1_i32;
    }
    if negative {
        rounded = -rounded;
    }
    let result = if target >= 0 {
        Decimal {
            mantissa: rounded,
            scale: target as u32,
        }
    } else {
        Decimal {
            mantissa: rounded * pow10_bigint((-target) as u32),
            scale: 0,
        }
    };
    result.format()
}

pub(crate) fn parse_decimal(value: &str) -> Result<Decimal> {
    let numeric = PgNumeric::from_postgres_text(value).map_err(|error| match error {
        PgCanonicalValueError::NumericOverflow => {
            SqlError::numeric_value_out_of_range("value overflows numeric format")
        }
        _ => SqlError::InvalidTextRepresentation(format!(
            "invalid input syntax for type numeric: \"{}\"",
            value.trim()
        )),
    })?;
    decimal_from_pg_numeric(numeric)
}

/// Cheap syntactic pre-filter for `is_valid_numeric_text`: PgNumeric text can
/// only start (after whitespace) with a digit, sign, point, or the first
/// letter of nan/infinity. Everything else — names, codes, UUIDs — is rejected
/// here for free instead of after a trim + lowercase allocation inside the
/// real parser. Every string comparison used to pay that allocation twice.
fn numeric_text_plausible(text: &str) -> bool {
    text.bytes()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| {
            matches!(
                byte,
                b'0'..=b'9' | b'+' | b'-' | b'.' | b'n' | b'N' | b'i' | b'I'
            )
        })
}

pub(crate) fn decimal_value(value: &SqlValue) -> Option<Result<Decimal>> {
    match value {
        SqlValue::Int(value) => Some(Ok(Decimal {
            mantissa: BigInt::from(*value),
            scale: 0,
        })),
        SqlValue::String(value)
            if numeric_text_plausible(value) && is_valid_numeric_text(value) =>
        {
            Some(parse_decimal(value))
        }
        _ => None,
    }
}

/// A NUMERIC that fits a machine word: `(mantissa, scale)` with at most
/// `SMALL_DECIMAL_MAX_DIGITS` significant digits. This is the shape every
/// money/quantity column takes (NUMERIC(12,2) and friends), and it is the
/// canonical text form the engine stores and emits, so the fast path below
/// covers the overwhelming majority of arithmetic and comparisons without a
/// single heap allocation. Anything outside the grammar (`-?\d+(\.\d+)?`),
/// or wider than the digit budget, returns None and takes the exact BigInt
/// path — the two paths are differentially tested to agree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SmallDecimal {
    mantissa: i128,
    scale: u32,
}

/// 18 digits per operand keeps rescale-to-common-scale and multiply inside
/// i128 without checked arithmetic ever failing in practice (10^36 < 2^127).
const SMALL_DECIMAL_MAX_DIGITS: usize = 18;

pub(crate) fn small_decimal_text(text: &str) -> Option<SmallDecimal> {
    let bytes = text.as_bytes();
    let (negative, digits) = match bytes.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, bytes),
    };
    if digits.is_empty() || digits.len() > SMALL_DECIMAL_MAX_DIGITS + 1 {
        return None;
    }
    let mut mantissa: i128 = 0;
    let mut scale: u32 = 0;
    let mut seen_point = false;
    let mut int_digits = 0usize;
    let mut frac_digits = 0usize;
    for &byte in digits {
        match byte {
            b'0'..=b'9' => {
                mantissa = mantissa * 10 + i128::from(byte - b'0');
                if seen_point {
                    frac_digits += 1;
                } else {
                    int_digits += 1;
                }
            }
            b'.' if !seen_point => seen_point = true,
            _ => return None,
        }
    }
    if int_digits == 0 || (seen_point && frac_digits == 0) {
        return None;
    }
    if int_digits + frac_digits > SMALL_DECIMAL_MAX_DIGITS {
        return None;
    }
    scale += frac_digits as u32;
    Some(SmallDecimal {
        mantissa: if negative { -mantissa } else { mantissa },
        scale,
    })
}

pub(crate) fn small_decimal_value(value: &SqlValue) -> Option<SmallDecimal> {
    match value {
        SqlValue::Int(value) => Some(SmallDecimal {
            mantissa: i128::from(*value),
            scale: 0,
        }),
        SqlValue::String(text) => small_decimal_text(text),
        _ => None,
    }
}

fn pow10_i128(exp: u32) -> Option<i128> {
    10_i128.checked_pow(exp)
}

/// Both operands rescaled to the larger scale; None when that would overflow
/// (only possible with pathological scales — the caller then falls back).
fn small_decimal_aligned(left: SmallDecimal, right: SmallDecimal) -> Option<(i128, i128, u32)> {
    let scale = left.scale.max(right.scale);
    let left_m = left.mantissa.checked_mul(pow10_i128(scale - left.scale)?)?;
    let right_m = right
        .mantissa
        .checked_mul(pow10_i128(scale - right.scale)?)?;
    Some((left_m, right_m, scale))
}

pub(crate) fn small_decimal_cmp(left: SmallDecimal, right: SmallDecimal) -> Option<Ordering> {
    let (left, right, _) = small_decimal_aligned(left, right)?;
    Some(left.cmp(&right))
}

/// Byte-identical to `Decimal::format` for the same (mantissa, scale).
pub(crate) fn small_decimal_format(value: SmallDecimal) -> String {
    // Digits into a stack buffer, then exactly one String at its final size.
    let magnitude = value.mantissa.unsigned_abs();
    let mut buffer = [0u8; 40];
    let mut start = buffer.len();
    let mut rest = magnitude;
    loop {
        start -= 1;
        buffer[start] = b'0' + (rest % 10) as u8;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let digits = &buffer[start..];
    let scale = value.scale as usize;
    let negative = value.mantissa < 0;
    let mut out = String::with_capacity(digits.len().max(scale + 1) + 2);
    if negative {
        out.push('-');
    }
    if scale == 0 {
        out.push_str(std::str::from_utf8(digits).expect("ascii digits"));
    } else if digits.len() <= scale {
        out.push_str("0.");
        for _ in 0..scale - digits.len() {
            out.push('0');
        }
        out.push_str(std::str::from_utf8(digits).expect("ascii digits"));
    } else {
        let split = digits.len() - scale;
        out.push_str(std::str::from_utf8(&digits[..split]).expect("ascii digits"));
        out.push('.');
        out.push_str(std::str::from_utf8(&digits[split..]).expect("ascii digits"));
    }
    out
}

/// Plus/Minus/Multiply on small decimals with the exact scale rules of
/// `eval_decimal_arithmetic`. Divide and Modulo keep the BigInt path (their
/// result-scale rules are PostgreSQL-specific and not worth duplicating).
pub(crate) fn small_decimal_arithmetic(
    left: SmallDecimal,
    op: &BinaryOperator,
    right: SmallDecimal,
) -> Option<SqlValue> {
    small_decimal_result(left, op, right)
        .map(|result| SqlValue::String(small_decimal_format(result)))
}

/// Keep exact numeric intermediates typed until an operator or output actually
/// needs the SQL text representation. Overflow still takes the BigInt path.
pub(crate) fn small_decimal_result(
    left: SmallDecimal,
    op: &BinaryOperator,
    right: SmallDecimal,
) -> Option<SmallDecimal> {
    let result = match op {
        BinaryOperator::Plus | BinaryOperator::Minus => {
            let (l, r, scale) = small_decimal_aligned(left, right)?;
            let mantissa = if matches!(op, BinaryOperator::Plus) {
                l.checked_add(r)?
            } else {
                l.checked_sub(r)?
            };
            SmallDecimal { mantissa, scale }
        }
        BinaryOperator::Multiply => SmallDecimal {
            mantissa: left.mantissa.checked_mul(right.mantissa)?,
            scale: left.scale.checked_add(right.scale)?,
        },
        _ => return None,
    };
    Some(result)
}

pub(crate) fn decimal_to_i64_rounded(value: Decimal) -> Result<i64> {
    let rounded = if value.scale == 0 {
        value.mantissa
    } else {
        let divisor = pow10_bigint(value.scale);
        let quotient = &value.mantissa / &divisor;
        let remainder = (&value.mantissa % &divisor).abs();
        let increment = if &remainder * 2 >= divisor {
            if value.mantissa.is_negative() {
                BigInt::from(-1)
            } else {
                BigInt::from(1)
            }
        } else {
            BigInt::ZERO
        };
        quotient + increment
    };
    rounded
        .to_i64()
        .ok_or_else(|| SqlError::InvalidSql("integer out of range".to_string()))
}

pub(crate) fn float_to_i64_rounded(value: f64) -> Result<i64> {
    if !value.is_finite() {
        return Err(SqlError::InvalidSql("integer out of range".to_string()));
    }
    let rounded = value.round();
    if rounded < i64::MIN as f64 || rounded > i64::MAX as f64 {
        return Err(SqlError::InvalidSql("integer out of range".to_string()));
    }
    Ok(rounded as i64)
}

pub(crate) fn eval_decimal_arithmetic(
    left: Decimal,
    op: &BinaryOperator,
    right: Decimal,
) -> Result<SqlValue> {
    let result = match op {
        BinaryOperator::Plus | BinaryOperator::Minus => {
            let scale = left.scale.max(right.scale);
            let left = left.mantissa * pow10_bigint(scale - left.scale);
            let right = right.mantissa * pow10_bigint(scale - right.scale);
            Decimal {
                mantissa: if matches!(op, BinaryOperator::Plus) {
                    left + right
                } else {
                    left - right
                },
                scale,
            }
        }
        BinaryOperator::Multiply => Decimal {
            mantissa: left.mantissa * right.mantissa,
            scale: left.scale + right.scale,
        },
        BinaryOperator::Divide => {
            if right.mantissa.is_zero() {
                return Err(division_by_zero());
            }
            let quotient_exponent = decimal_quotient_exponent(&left, &right)?;
            let quotient_weight = quotient_exponent.div_euclid(4);
            let scale = (16 - quotient_weight * 4)
                .max(i32::try_from(left.scale).unwrap_or(i32::MAX))
                .max(i32::try_from(right.scale).unwrap_or(i32::MAX))
                .max(0)
                .min(PG_NUMERIC_MAX_FRACTIONAL_DIGITS) as u32;
            let exponent = i64::from(scale) + i64::from(right.scale) - i64::from(left.scale);
            let (numerator, denominator) = if exponent >= 0 {
                (
                    left.mantissa * pow10_bigint(exponent as u32),
                    right.mantissa,
                )
            } else {
                (
                    left.mantissa,
                    right.mantissa * pow10_bigint((-exponent) as u32),
                )
            };
            let mut quotient = &numerator / &denominator;
            let remainder = (&numerator % &denominator).abs();
            if &remainder * 2 >= denominator.abs() {
                quotient += if numerator.sign() == denominator.sign() {
                    1
                } else {
                    -1
                };
            }
            Decimal {
                mantissa: quotient,
                scale,
            }
        }
        BinaryOperator::Modulo => {
            if right.mantissa.is_zero() {
                return Err(division_by_zero());
            }
            let scale = left.scale.max(right.scale);
            let left = left.mantissa * pow10_bigint(scale - left.scale);
            let right = right.mantissa * pow10_bigint(scale - right.scale);
            Decimal {
                mantissa: left % right,
                scale,
            }
        }
        _ => unreachable!(),
    };
    Ok(SqlValue::String(result.format()))
}

pub(crate) fn decimal_quotient_exponent(left: &Decimal, right: &Decimal) -> Result<i32> {
    if left.mantissa.is_zero() {
        return Ok(0);
    }
    let left_digits = left.mantissa.abs().to_string().len() as i64;
    let right_digits = right.mantissa.abs().to_string().len() as i64;
    let guess = left_digits - i64::from(left.scale) - right_digits + i64::from(right.scale);
    let delta = i64::from(right.scale) - i64::from(left.scale) - guess;
    let left_scaled = if delta >= 0 {
        left.mantissa.abs() * pow10_bigint(delta as u32)
    } else {
        left.mantissa.abs()
    };
    let right_scaled = if delta < 0 {
        right.mantissa.abs() * pow10_bigint((-delta) as u32)
    } else {
        right.mantissa.abs()
    };
    i32::try_from(if left_scaled < right_scaled {
        guess - 1
    } else {
        guess
    })
    .map_err(|_| SqlError::numeric_value_out_of_range("value overflows numeric format"))
}

pub(crate) fn pow10_bigint(exp: u32) -> BigInt {
    BigInt::from(10_u8).pow(exp)
}

pub(crate) fn canonical_float_decimal(value: f64) -> Result<String> {
    if !value.is_finite() {
        return Err(SqlError::InvalidTextRepresentation(format!(
            "invalid input syntax for type numeric: \"{value}\""
        )));
    }
    Ok(value.to_string())
}

pub(crate) fn eval_in_list_truth(
    value: SqlValue,
    candidates: Vec<SqlValue>,
    negated: bool,
) -> Result<Option<bool>> {
    let matched = candidates
        .iter()
        .any(|candidate| values_equal(&value, candidate));
    if matched {
        Ok(Some(!negated))
    } else if matches!(value, SqlValue::Null)
        || candidates
            .iter()
            .any(|candidate| matches!(candidate, SqlValue::Null))
    {
        Ok(None)
    } else {
        Ok(Some(negated))
    }
}

pub(crate) fn eval_between_truth(
    value: SqlValue,
    low: SqlValue,
    high: SqlValue,
    negated: bool,
) -> Result<Option<bool>> {
    let lower = compare_values(&value, &BinaryOperator::GtEq, &low)?;
    let upper = compare_values(&value, &BinaryOperator::LtEq, &high)?;
    Ok(sql_and(lower, upper).map(|matched| if negated { !matched } else { matched }))
}

pub(crate) fn eval_quantified_truth(
    left: SqlValue,
    op: &BinaryOperator,
    right: SqlValue,
    all: bool,
) -> Result<Option<bool>> {
    if matches!(right, SqlValue::Null) {
        return Ok(None);
    }
    let values: Vec<SqlValue> = match &right {
        SqlValue::Json(_) => {
            let (array, _) = array_json_parts(&right, "ANY/ALL")?.ok_or_else(|| {
                SqlError::InvalidSql("right operand of ANY/ALL must be an array".to_string())
            })?;
            let mut flattened = Vec::new();
            flatten_array_json(array, &mut flattened);
            flattened.into_iter().map(json_to_sql_value).collect()
        }
        SqlValue::String(value) => {
            let parsed = parse_array_literal(value).map_err(|_| {
                SqlError::InvalidSql("right operand of ANY/ALL must be an array".to_string())
            })?;
            let mut flattened = Vec::new();
            for value in &parsed {
                flatten_array_json(value, &mut flattened);
            }
            flattened.into_iter().map(json_to_sql_value).collect()
        }
        _ => {
            return Err(SqlError::InvalidSql(
                "right operand of ANY/ALL must be an array".to_string(),
            ));
        }
    };
    if values.is_empty() {
        return Ok(Some(all));
    }
    let mut saw_null = false;
    let mut any_true = false;
    for value in values {
        match compare_values(&left, op, &value)? {
            Some(true) if !all => return Ok(Some(true)),
            Some(false) if all => return Ok(Some(false)),
            Some(true) => any_true = true,
            Some(false) => {}
            None => saw_null = true,
        }
    }
    if saw_null {
        Ok(None)
    } else if all {
        Ok(Some(true))
    } else {
        Ok(Some(any_true))
    }
}

pub(crate) fn array_like_values(value: &SqlValue) -> Option<Vec<SqlValue>> {
    match value {
        SqlValue::Json(JsonValue::Array(values)) => {
            Some(values.iter().map(json_to_sql_value).collect())
        }
        SqlValue::Json(JsonValue::Object(object)) => object
            .get("$bicdb_array_input")?
            .get("value")?
            .as_array()
            .map(|values| values.iter().map(json_to_sql_value).collect()),
        SqlValue::String(value) => parse_array_literal(value)
            .ok()
            .map(|values| values.iter().map(json_to_sql_value).collect())
            .or_else(|| pg_int_vector_values(value)),
        _ => None,
    }
}

pub(crate) fn eval_access_chain_expr<F>(
    root: &Expr,
    access_chain: &[AccessExpr],
    jsonb_subscript: bool,
    mut eval: F,
) -> Result<SqlValue>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    let mut next_access = 0;
    let mut compound_root = None;
    let mut parts = match root {
        Expr::Identifier(ident) => vec![ident.clone()],
        Expr::CompoundIdentifier(idents) => idents.clone(),
        _ => Vec::new(),
    };
    if !parts.is_empty() {
        while let Some(AccessExpr::Dot(Expr::Identifier(ident))) = access_chain.get(next_access) {
            parts.push(ident.clone());
            next_access += 1;
        }
        if next_access > 0 {
            compound_root = Some(Expr::CompoundIdentifier(parts));
        }
    }

    let mut value = if let Some(root) = compound_root.as_ref() {
        eval(root)?
    } else {
        eval(root)?
    };
    let array_slice_mode = !jsonb_subscript
        && access_chain[next_access..]
            .iter()
            .any(|access| matches!(access, AccessExpr::Subscript(Subscript::Slice { .. })));
    let mut array_subscript_dimension = 0usize;
    for access in &access_chain[next_access..] {
        if let SqlValue::Json(json) = &value {
            if let Some(composite) = pg_composite_from_array_json(json) {
                value = SqlValue::Composite(composite);
            }
        }
        value = match access {
            AccessExpr::Subscript(subscript) => eval_subscript_value(
                value,
                subscript,
                jsonb_subscript,
                array_slice_mode,
                array_subscript_dimension,
                &mut eval,
            )?,
            AccessExpr::Dot(Expr::Identifier(ident)) => match value {
                SqlValue::Null => SqlValue::Null,
                SqlValue::Composite(composite) => composite
                    .field(&ident.value)
                    .cloned()
                    .ok_or_else(|| SqlError::UndefinedColumn {
                        table: composite.type_name.clone(),
                        column: ident.value.clone(),
                    })?,
                SqlValue::Json(JsonValue::Object(object)) => object
                    .get(&ident.value)
                    .map(json_to_sql_value)
                    .unwrap_or(SqlValue::Null),
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "cannot select field {} from non-composite value {}",
                        ident.value,
                        other.to_cell()
                    )));
                }
            },
            AccessExpr::Dot(expr) => {
                return Err(SqlError::Unsupported(format!(
                    "unsupported compound field access .{expr}"
                )));
            }
        };
        if matches!(access, AccessExpr::Subscript(_)) && array_slice_mode {
            array_subscript_dimension += 1;
        }
    }
    if let SqlValue::Json(JsonValue::Object(object)) = &value {
        if let Some(input) = object
            .get("$bicdb_array_input")
            .and_then(JsonValue::as_object)
        {
            if !array_slice_mode
                && matches!(
                    access_chain.last(),
                    Some(AccessExpr::Subscript(Subscript::Index { .. }))
                )
            {
                return Ok(SqlValue::Null);
            }
            if array_slice_mode {
                if let Some(array) = input.get("value") {
                    return Ok(SqlValue::Json(array.clone()));
                }
            }
        }
    }
    Ok(value)
}

/// One `value[index]` step on a non-jsonb value with the index already
/// evaluated: exactly what `eval_access_chain_expr` does for a root followed
/// by a single `Subscript::Index` access (composite conversion, the array
/// subscript, and the array-input-wrapper tail). The bound routine-expression
/// evaluator uses it for `array_var[index]`.
pub(crate) fn eval_single_index_value(
    value: SqlValue,
    subscript: &Subscript,
    index: SqlValue,
) -> Result<SqlValue> {
    let mut value = value;
    if let SqlValue::Json(json) = &value {
        if let Some(composite) = pg_composite_from_array_json(json) {
            value = SqlValue::Composite(composite);
        }
    }
    let value = eval_subscript_value(
        value,
        subscript,
        false,
        false,
        0,
        &mut |_| Ok(index.clone()),
    )?;
    if let SqlValue::Json(JsonValue::Object(object)) = &value {
        if object
            .get("$bicdb_array_input")
            .and_then(JsonValue::as_object)
            .is_some()
        {
            return Ok(SqlValue::Null);
        }
    }
    Ok(value)
}

pub(crate) fn eval_subscript_value<F>(
    value: SqlValue,
    subscript: &Subscript,
    jsonb_subscript: bool,
    array_slice_mode: bool,
    array_subscript_dimension: usize,
    eval: &mut F,
) -> Result<SqlValue>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    if matches!(value, SqlValue::JsonText(_)) {
        return Err(SqlError::ConstraintViolation {
            sqlstate: "42804",
            message: "cannot subscript type json because it does not support subscripting"
                .to_string(),
            table: None,
            column: None,
            constraint: None,
        });
    }
    if jsonb_subscript {
        return eval_jsonb_subscript_value(value, subscript, eval);
    }
    if let SqlValue::String(text) = &value {
        if pg_int_vector_values(text).is_none() {
            return eval_string_subscript_value(text, subscript, eval);
        }
    }
    let value = match value {
        SqlValue::String(text) => array_value(
            pg_int_vector_values(&text)
                .expect("non-vector strings returned through string subscripting"),
        ),
        value => value,
    };
    let Some((array, lower_bounds)) = array_json_parts(&value, "array subscript")? else {
        return Ok(SqlValue::Null);
    };
    let dimensions = array_dimensions(array);
    let dimension = if array_slice_mode {
        array_subscript_dimension
    } else {
        0
    };
    let dimension_length = dimensions.get(dimension).copied().unwrap_or(0);
    let array_lower = i64::from(lower_bounds.get(dimension).copied().unwrap_or(1));
    let array_upper = array_lower + dimension_length as i64 - 1;
    if array_slice_mode && dimension > 0 {
        let (lower, upper) = match subscript {
            Subscript::Index { index } => {
                let index = eval_subscript_bound(eval(index)?)?;
                (index, index)
            }
            Subscript::Slice {
                lower_bound,
                upper_bound,
                stride,
            } => {
                if stride.is_some() {
                    return Err(SqlError::Unsupported(
                        "array slice stride is not supported by PostgreSQL".to_string(),
                    ));
                }
                let lower = lower_bound
                    .as_ref()
                    .map(|expr| eval(expr).and_then(eval_subscript_bound))
                    .transpose()?
                    .unwrap_or(array_lower);
                let upper = upper_bound
                    .as_ref()
                    .map(|expr| eval(expr).and_then(eval_subscript_bound))
                    .transpose()?
                    .unwrap_or(array_upper);
                (lower, upper)
            }
        };
        let start = lower.max(array_lower);
        let end = upper.min(array_upper);
        let mut sliced = array.clone();
        slice_array_json_dimension(
            &mut sliced,
            dimension,
            usize::try_from(start.saturating_sub(array_lower)).unwrap_or(usize::MAX),
            if start > end {
                0
            } else {
                usize::try_from(end - start + 1).unwrap_or(0)
            },
        )?;
        let mut bounds = lower_bounds;
        bounds[dimension] = 1;
        return Ok(array_json_value_with_lower_bounds(sliced, bounds));
    }
    let values = array.as_array().expect("validated array");
    match subscript {
        Subscript::Index { index } if !array_slice_mode => {
            let index = eval_subscript_bound(eval(index)?)?;
            if index < array_lower || index > array_upper {
                return Ok(SqlValue::Null);
            }
            let value = values
                .get((index - array_lower) as usize)
                .map(json_to_sql_value)
                .unwrap_or(SqlValue::Null);
            if matches!(value, SqlValue::Json(JsonValue::Array(_))) && lower_bounds.len() > 1 {
                let SqlValue::Json(value) = value else {
                    unreachable!()
                };
                Ok(array_json_value_with_lower_bounds(
                    value,
                    lower_bounds[1..].to_vec(),
                ))
            } else {
                Ok(value)
            }
        }
        Subscript::Index { index } => {
            let index = eval_subscript_bound(eval(index)?)?;
            if index < array_lower || index > array_upper {
                return Ok(SqlValue::Json(JsonValue::Array(Vec::new())));
            }
            let value = values[(index - array_lower) as usize].clone();
            let mut bounds = vec![1];
            bounds.extend_from_slice(&lower_bounds[1..]);
            Ok(array_json_value_with_lower_bounds(
                JsonValue::Array(vec![value]),
                bounds,
            ))
        }
        Subscript::Slice {
            lower_bound,
            upper_bound,
            stride,
        } => {
            let lower = lower_bound
                .as_ref()
                .map(|expr| eval(expr).and_then(eval_subscript_bound))
                .transpose()?
                .unwrap_or(array_lower);
            let upper = upper_bound
                .as_ref()
                .map(|expr| eval(expr).and_then(eval_subscript_bound))
                .transpose()?
                .unwrap_or(array_upper);
            let stride = stride
                .as_ref()
                .map(|expr| eval(expr).and_then(eval_subscript_bound))
                .transpose()?
                .unwrap_or(1);
            if stride < 1 {
                return Err(SqlError::InvalidSql(
                    "array slice stride must be positive".to_string(),
                ));
            }
            let start = lower.max(array_lower);
            let end = upper.min(array_upper);
            if start > end {
                return Ok(SqlValue::Json(JsonValue::Array(Vec::new())));
            }
            let mut sliced = Vec::new();
            let mut idx = start;
            while idx <= end {
                if let Some(value) = values.get((idx - array_lower) as usize) {
                    sliced.push(value.clone());
                }
                idx += stride;
            }
            let mut bounds = vec![1];
            bounds.extend_from_slice(&lower_bounds[1..]);
            Ok(array_json_value_with_lower_bounds(
                JsonValue::Array(sliced),
                bounds,
            ))
        }
    }
}

pub(crate) fn slice_array_json_dimension(
    value: &mut JsonValue,
    dimension: usize,
    start: usize,
    length: usize,
) -> Result<()> {
    let values = value
        .as_array_mut()
        .ok_or_else(|| SqlError::InvalidSql("wrong number of array subscripts".to_string()))?;
    if dimension == 0 {
        let end = start.saturating_add(length).min(values.len());
        *values = if start >= values.len() {
            Vec::new()
        } else {
            values[start..end].to_vec()
        };
        return Ok(());
    }
    for child in values {
        slice_array_json_dimension(child, dimension - 1, start, length)?;
    }
    Ok(())
}

pub(crate) fn eval_jsonb_subscript_value<F>(
    value: SqlValue,
    subscript: &Subscript,
    eval: &mut F,
) -> Result<SqlValue>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    let Subscript::Index { index } = subscript else {
        return Err(SqlError::Unsupported(
            "jsonb does not support array slices".to_string(),
        ));
    };
    let index = eval(index)?;
    let key = match index {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::String(value) => value,
        value => value.to_cell(),
    };
    let selected = match value {
        SqlValue::Json(JsonValue::Array(values)) => key.parse::<i64>().ok().and_then(|index| {
            let index = if index < 0 {
                i64::try_from(values.len()).ok()?.checked_add(index)?
            } else {
                index
            };
            usize::try_from(index)
                .ok()
                .and_then(|index| values.get(index))
                .cloned()
        }),
        SqlValue::Json(JsonValue::Object(values)) => values.get(&key).cloned(),
        SqlValue::Json(_) => None,
        value => {
            return Err(SqlError::Unsupported(format!(
                "cannot subscript non-jsonb value {}",
                value.to_cell()
            )));
        }
    };
    Ok(selected.map(SqlValue::Json).unwrap_or(SqlValue::Null))
}

pub(crate) fn eval_string_subscript_value<F>(
    value: &str,
    subscript: &Subscript,
    eval: &mut F,
) -> Result<SqlValue>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    match subscript {
        Subscript::Index { index } => {
            let index = eval_subscript_bound(eval(index)?)?;
            if index < 0 {
                return Ok(SqlValue::Null);
            }
            Ok(value
                .chars()
                .nth(index as usize)
                .map(|ch| SqlValue::String(ch.to_string()))
                .unwrap_or(SqlValue::Null))
        }
        Subscript::Slice { .. } => Err(SqlError::Unsupported(
            "string slices are not supported".to_string(),
        )),
    }
}

pub(crate) fn eval_subscript_bound(value: SqlValue) -> Result<i64> {
    sql_value_i64(&value).ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "array subscript must be an integer, got {}",
            value.to_cell()
        ))
    })
}

pub(crate) fn pg_int_vector_values(value: &str) -> Option<Vec<SqlValue>> {
    value
        .split_whitespace()
        .map(|part| part.parse::<i64>().ok().map(SqlValue::Int))
        .collect()
}

pub(crate) fn eval_pg_like_operator(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
) -> Result<Option<bool>> {
    let (negated, case_insensitive) = match op {
        BinaryOperator::PGLikeMatch => (false, false),
        BinaryOperator::PGILikeMatch => (false, true),
        BinaryOperator::PGNotLikeMatch => (true, false),
        BinaryOperator::PGNotILikeMatch => (true, true),
        _ => unreachable!(),
    };
    eval_like_values(left.clone(), right.clone(), negated, case_insensitive, None)
}

pub(crate) fn eval_pg_pattern_operator(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
) -> Result<Option<bool>> {
    match op {
        BinaryOperator::PGLikeMatch
        | BinaryOperator::PGILikeMatch
        | BinaryOperator::PGNotLikeMatch
        | BinaryOperator::PGNotILikeMatch => eval_pg_like_operator(left, op, right),
        BinaryOperator::PGRegexMatch
        | BinaryOperator::PGRegexIMatch
        | BinaryOperator::PGRegexNotMatch
        | BinaryOperator::PGRegexNotIMatch => eval_pg_regex_operator(left, op, right),
        _ => unreachable!(),
    }
}

pub(crate) fn eval_pg_regex_operator(
    left: &SqlValue,
    op: &BinaryOperator,
    right: &SqlValue,
) -> Result<Option<bool>> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(None);
    }
    let (negated, case_insensitive) = match op {
        BinaryOperator::PGRegexMatch => (false, false),
        BinaryOperator::PGRegexIMatch => (false, true),
        BinaryOperator::PGRegexNotMatch => (true, false),
        BinaryOperator::PGRegexNotIMatch => (true, true),
        _ => unreachable!(),
    };
    let pattern = right.to_cell();
    crate::reject_oversized_regex(&pattern)?;
    let regex = RegexBuilder::new(&pattern)
        .case_insensitive(case_insensitive)
        .size_limit(1 << 20)
        .dfa_size_limit(1 << 20)
        .build()
        .map_err(|error| {
            SqlError::InvalidSql(format!("invalid regular expression {pattern:?}: {error}"))
        })?;
    let matched = regex.is_match(&left.to_cell());
    Ok(Some(if negated { !matched } else { matched }))
}

pub(crate) fn eval_like_values(
    value: SqlValue,
    pattern: SqlValue,
    negated: bool,
    case_insensitive: bool,
    escape_char: Option<&ValueWithSpan>,
) -> Result<Option<bool>> {
    if matches!(value, SqlValue::Null) || matches!(pattern, SqlValue::Null) {
        return Ok(None);
    }
    let escape = match escape_char {
        None => Some('\\'),
        Some(value) => {
            let value = literal_to_value(value)?.to_cell();
            if value.chars().count() > 1 {
                return Err(SqlError::data_exception(
                    "22025",
                    "invalid escape string",
                    Some("text".to_string()),
                ));
            }
            value.chars().next()
        }
    };
    let matched = like_matches(
        &value.to_cell(),
        &pattern.to_cell(),
        case_insensitive,
        escape,
    )?;
    Ok(Some(if negated { !matched } else { matched }))
}

pub(crate) fn like_matches(
    value: &str,
    pattern: &str,
    case_insensitive: bool,
    escape: Option<char>,
) -> Result<bool> {
    let value = if case_insensitive {
        value.to_lowercase()
    } else {
        value.to_string()
    };
    let pattern = if case_insensitive {
        pattern.to_lowercase()
    } else {
        pattern.to_string()
    };
    let value = value.chars().collect::<Vec<_>>();
    let pattern = pattern.chars().collect::<Vec<_>>();
    if let Some(escape) = escape {
        let trailing_escapes = pattern
            .iter()
            .rev()
            .take_while(|character| **character == escape)
            .count();
        if trailing_escapes % 2 == 1 {
            return Err(SqlError::data_exception(
                "22025",
                "LIKE pattern must not end with escape character",
                Some("text".to_string()),
            ));
        }
    }
    Ok(like_match_chars(&value, &pattern, escape))
}

pub(crate) fn like_match_chars(value: &[char], pattern: &[char], escape: Option<char>) -> bool {
    let (mut vi, mut pi) = (0usize, 0usize);
    let mut star = None::<usize>;
    let mut star_value = 0usize;
    while vi < value.len() {
        if pi < pattern.len() {
            if Some(pattern[pi]) == escape && pi + 1 < pattern.len() {
                pi += 1;
                if pattern[pi] == value[vi] {
                    vi += 1;
                    pi += 1;
                    continue;
                }
            } else if pattern[pi] == '_' || pattern[pi] == value[vi] {
                vi += 1;
                pi += 1;
                continue;
            } else if pattern[pi] == '%' {
                star = Some(pi);
                pi += 1;
                star_value = vi;
                continue;
            }
        }
        if let Some(star_idx) = star {
            pi = star_idx + 1;
            star_value += 1;
            vi = star_value;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == '%' {
        pi += 1;
    }
    pi == pattern.len()
}

pub(crate) fn value_ordering(left: &SqlValue, right: &SqlValue) -> Option<Ordering> {
    if let (SqlValue::Composite(left), SqlValue::Composite(right)) = (left, right) {
        if left.fields.len() != right.fields.len() {
            return None;
        }
        for (left, right) in left.fields.iter().zip(&right.fields) {
            if values_equal(&left.value, &right.value) {
                continue;
            }
            return value_ordering(&left.value, &right.value);
        }
        return Some(Ordering::Equal);
    }
    if let (SqlValue::Json(left), SqlValue::Json(right)) = (left, right) {
        return Some(jsonb_value_ordering(left, right));
    }
    if let (Some(small_left), Some(small_right)) =
        (small_decimal_value(left), small_decimal_value(right))
    {
        if let Some(ordering) = small_decimal_cmp(small_left, small_right) {
            return Some(ordering);
        }
    }
    if let (Some(Ok(left)), Some(Ok(right))) = (decimal_value(left), decimal_value(right)) {
        return Some(left.cmp(&right));
    }
    match (left, right) {
        (SqlValue::Bool(left), right) => {
            return sql_value_bool(right).map(|right| left.cmp(&right));
        }
        (left, SqlValue::Bool(right)) => {
            return sql_value_bool(left).map(|left| left.cmp(right));
        }
        _ => {}
    }
    match (left.as_f64(), right.as_f64()) {
        (Some(left), Some(right)) => Some(postgres_float_ordering(left, right)),
        _ => match (left, right) {
            (SqlValue::String(left), SqlValue::String(right)) => Some(left.cmp(right)),
            (SqlValue::Bool(left), SqlValue::Bool(right)) => Some(left.cmp(right)),
            _ => None,
        },
    }
}

pub(crate) fn postgres_float_ordering(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    }
}

pub(crate) fn has_aggregates(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_contains_aggregate(expr)
        }
        _ => false,
    })
}

pub(crate) fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(function) if function.over.is_none() && is_aggregate_function(function) => {
            true
        }
        Expr::Function(function) => function_args(function).iter().any(expr_contains_aggregate),
        Expr::BinaryOp { left, right, .. }
        | Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right) => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Extract { expr, .. } => expr_contains_aggregate(expr),
        Expr::Position { expr, r#in } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(r#in)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(pattern)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(expr_contains_aggregate)
                || conditions.iter().any(|condition| {
                    expr_contains_aggregate(&condition.condition)
                        || expr_contains_aggregate(&condition.result)
                })
                || else_result.as_deref().is_some_and(expr_contains_aggregate)
        }
        Expr::Array(array) => array.elem.iter().any(expr_contains_aggregate),
        Expr::CompoundFieldAccess { root, access_chain } => {
            expr_contains_aggregate(root)
                || access_chain.iter().any(|access| match access {
                    sqlparser::ast::AccessExpr::Dot(expr) => expr_contains_aggregate(expr),
                    sqlparser::ast::AccessExpr::Subscript(subscript) => {
                        subscript_contains_aggregate(subscript)
                    }
                })
        }
        Expr::Interval(interval) => expr_contains_aggregate(&interval.value),
        Expr::Trim {
            trim_what,
            expr,
            trim_characters,
            ..
        } => {
            trim_what.as_deref().is_some_and(expr_contains_aggregate)
                || expr_contains_aggregate(expr)
                || trim_characters
                    .as_deref()
                    .is_some_and(|characters| characters.iter().any(expr_contains_aggregate))
        }
        Expr::JsonAccess { value, .. } => expr_contains_aggregate(value),
        Expr::InSubquery { expr, .. } | Expr::InUnnest { expr, .. } => {
            expr_contains_aggregate(expr)
        }
        Expr::Convert { expr, styles, .. } => {
            expr_contains_aggregate(expr) || styles.iter().any(expr_contains_aggregate)
        }
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => expr_contains_aggregate(timestamp) || expr_contains_aggregate(time_zone),
        Expr::Ceil { expr, .. } | Expr::Floor { expr, .. } => expr_contains_aggregate(expr),
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            expr_contains_aggregate(expr)
                || substring_from
                    .as_deref()
                    .is_some_and(expr_contains_aggregate)
                || substring_for
                    .as_deref()
                    .is_some_and(expr_contains_aggregate)
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(overlay_what)
                || expr_contains_aggregate(overlay_from)
                || overlay_for.as_deref().is_some_and(expr_contains_aggregate)
        }
        Expr::Collate { expr, .. } => expr_contains_aggregate(expr),
        Expr::Prefixed { value, .. } => expr_contains_aggregate(value),
        Expr::Tuple(exprs) => exprs.iter().any(expr_contains_aggregate),
        Expr::Struct { values, .. } => values.iter().any(expr_contains_aggregate),
        Expr::Named { expr, .. } => expr_contains_aggregate(expr),
        Expr::GroupingSets(groups) | Expr::Cube(groups) | Expr::Rollup(groups) => {
            groups.iter().flatten().any(expr_contains_aggregate)
        }
        Expr::OuterJoin(expr) | Expr::Prior(expr) => expr_contains_aggregate(expr),
        Expr::Lambda(lambda) => expr_contains_aggregate(&lambda.body),
        Expr::SimilarTo { expr, pattern, .. } | Expr::RLike { expr, pattern, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(pattern)
        }
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::TypedString(_)
        | Expr::Dictionary(_)
        | Expr::Map(_)
        | Expr::MatchAgainst { .. }
        | Expr::Wildcard(_)
        | Expr::QualifiedWildcard(_, _)
        | Expr::MemberOf(_) => false,
        _ => false,
    }
}

pub(crate) fn subscript_contains_aggregate(subscript: &sqlparser::ast::Subscript) -> bool {
    match subscript {
        sqlparser::ast::Subscript::Index { index } => expr_contains_aggregate(index),
        sqlparser::ast::Subscript::Slice {
            lower_bound,
            upper_bound,
            stride,
        } => {
            lower_bound.as_ref().is_some_and(expr_contains_aggregate)
                || upper_bound.as_ref().is_some_and(expr_contains_aggregate)
                || stride.as_ref().is_some_and(expr_contains_aggregate)
        }
    }
}

pub(crate) fn is_aggregate_function(function: &Function) -> bool {
    matches!(
        object_name(&function.name)
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "bool_and"
            | "pg_catalog.bool_and"
            | "bool_or"
            | "pg_catalog.bool_or"
            | "every"
            | "pg_catalog.every"
            | "string_agg"
            | "pg_catalog.string_agg"
            | "array_agg"
            | "range_agg"
            | "pg_catalog.range_agg"
            | "range_intersect_agg"
            | "pg_catalog.range_intersect_agg"
            | "xmlagg"
            | "pg_catalog.xmlagg"
            | "json_agg"
            | "pg_catalog.json_agg"
            | "json_agg_strict"
            | "pg_catalog.json_agg_strict"
            | "json_arrayagg"
            | "pg_catalog.json_arrayagg"
            | "json_arrayagg_encoding_error"
            | "pg_catalog.json_arrayagg_encoding_error"
            | "jsonb_agg"
            | "pg_catalog.jsonb_agg"
            | "jsonb_agg_strict"
            | "pg_catalog.jsonb_agg_strict"
            | "json_object_agg"
            | "pg_catalog.json_object_agg"
            | "json_object_agg_strict"
            | "pg_catalog.json_object_agg_strict"
            | "json_object_agg_unique"
            | "pg_catalog.json_object_agg_unique"
            | "json_object_agg_unique_strict"
            | "pg_catalog.json_object_agg_unique_strict"
            | "json_objectagg"
            | "pg_catalog.json_objectagg"
            | "json_objectagg_unique"
            | "pg_catalog.json_objectagg_unique"
            | "json_objectagg_encoding_error"
            | "pg_catalog.json_objectagg_encoding_error"
            | "json_objectagg_unique_encoding_error"
            | "pg_catalog.json_objectagg_unique_encoding_error"
            | "jsonb_object_agg"
            | "pg_catalog.jsonb_object_agg"
            | "jsonb_object_agg_strict"
            | "pg_catalog.jsonb_object_agg_strict"
            | "jsonb_object_agg_unique"
            | "pg_catalog.jsonb_object_agg_unique"
            | "jsonb_object_agg_unique_strict"
            | "pg_catalog.jsonb_object_agg_unique_strict"
    )
}

pub(crate) fn execute_aggregates(
    items: &[SelectItem],
    records: &[Arc<Record>],
    schema: Option<&TableSchema>,
) -> Result<SqlResult> {
    // Aggregates fold over the whole record set; materialize owned records once
    // here so the existing &[Record] aggregate machinery is unchanged.
    let records: Vec<Record> = records
        .iter()
        .map(|record| record.as_ref().clone())
        .collect();
    let mut columns = Vec::new();
    let mut row = Vec::new();

    for item in items {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            other => {
                return Err(SqlError::Unsupported(format!(
                    "aggregate SELECT cannot mix unsupported projection {other}"
                )));
            }
        };
        columns.push(alias.unwrap_or_else(|| aggregate_column_name(expr, schema)));
        row.push(eval_record_aggregate_expr(&records, expr, schema)?);
    }

    let column_types = aggregate_projection_column_types(items, schema);
    validate_integer_result_types(
        SqlResult::new(columns, vec![row]).with_column_types(column_types),
    )
}

pub(crate) fn eval_record_aggregate_expr(
    records: &[Record],
    expr: &Expr,
    schema: Option<&TableSchema>,
) -> Result<SqlValue> {
    match expr {
        Expr::Function(function) if is_aggregate_function(function) => {
            Aggregate::from_function(function, schema)?.evaluate(records)
        }
        Expr::Function(function) => eval_record_aggregate_function_value(records, function, schema),
        Expr::Value(value) => literal_to_value(value),
        Expr::TypedString(value) => typed_string_to_value(value),
        Expr::BinaryOp { left, op, right } => eval_binary_expr_value(
            left,
            op,
            right,
            eval_record_aggregate_expr(records, left, schema)?,
            eval_record_aggregate_expr(records, right, schema)?,
            schema,
        ),
        Expr::Cast {
            expr, data_type, ..
        } => cast_expr_value(
            eval_record_aggregate_expr(records, expr, schema)?,
            expr,
            data_type,
            schema,
        ),
        Expr::Position { expr, r#in } => eval_position_typed_value(
            eval_record_aggregate_expr(records, expr, schema)?,
            eval_record_aggregate_expr(records, r#in, schema)?,
            projected_expr_pg_type(expr, schema).as_deref() == Some("bytea")
                || projected_expr_pg_type(r#in, schema).as_deref() == Some("bytea"),
        ),
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => eval_overlay_expr(
            expr,
            overlay_what,
            overlay_from,
            overlay_for.as_deref(),
            |expr| eval_record_aggregate_expr(records, expr, schema),
            projected_expr_pg_type(expr, schema).as_deref() == Some("bytea"),
        ),
        Expr::Extract { field, expr, .. } => {
            eval_extract_value(field, eval_record_aggregate_expr(records, expr, schema)?)
        }
        Expr::Array(array) => sql_array_value(
            array
                .elem
                .iter()
                .map(|expr| eval_record_aggregate_expr(records, expr, schema))
                .collect::<Result<Vec<_>>>()?,
        ),
        Expr::CompoundFieldAccess { root, access_chain } => eval_access_chain_expr(
            root,
            access_chain,
            projected_expr_pg_type(root, schema).as_deref() == Some("jsonb"),
            |expr| eval_record_aggregate_expr(records, expr, schema),
        ),
        Expr::Interval(interval) => interval_literal_value(interval, |expr| {
            eval_record_aggregate_expr(records, expr, schema)
        }),
        Expr::Trim {
            trim_where,
            trim_what,
            expr,
            trim_characters,
        } => eval_trim_expr(
            trim_where.as_ref(),
            trim_what.as_deref(),
            expr,
            trim_characters.as_deref(),
            |expr| eval_record_aggregate_expr(records, expr, schema),
        ),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => eval_record_aggregate_case(
            records,
            operand.as_deref(),
            conditions,
            else_result.as_deref(),
            schema,
        ),
        Expr::Like { .. }
        | Expr::ILike { .. }
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. }
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotUnknown(_) => eval_record_aggregate_truth(records, expr, schema)
            .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
        Expr::Nested(expr) => eval_record_aggregate_expr(records, expr, schema),
        Expr::Collate { expr, .. } => eval_record_aggregate_expr(records, expr, schema),
        Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
            eval_record_aggregate_truth(records, expr, schema)
                .map(sql_not)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null))
        }
        Expr::UnaryOp { op, expr } if op.to_string() == "-" => eval_unary_minus_expr_value(
            expr,
            eval_record_aggregate_expr(records, expr, schema)?,
            schema,
        ),
        Expr::UnaryOp { op, expr } if op.to_string() == "+" => eval_unary_plus_expr_value(
            expr,
            eval_record_aggregate_expr(records, expr, schema)?,
            schema,
        ),
        Expr::UnaryOp { op, expr }
            if matches!(
                op,
                UnaryOperator::BitwiseNot | UnaryOperator::PGPrefixFactorial
            ) || is_geometric_unary_operator(op) =>
        {
            eval_unary_bit_not_expr_value(
                op,
                expr,
                eval_record_aggregate_expr(records, expr, schema)?,
                schema,
            )
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Err(SqlError::Unsupported(
            "aggregate SELECT cannot mix raw fields and aggregates without GROUP BY".to_string(),
        )),
        other => Err(SqlError::Unsupported(format!(
            "unsupported aggregate expression {other}"
        ))),
    }
}

pub(crate) fn eval_record_aggregate_function_value(
    records: &[Record],
    function: &Function,
    schema: Option<&TableSchema>,
) -> Result<SqlValue> {
    let name = object_name(&function.name)?.to_ascii_lowercase();
    let arg_exprs = function_args(function);
    let args = arg_exprs
        .iter()
        .map(|arg| eval_record_aggregate_expr(records, arg, schema))
        .collect::<Result<Vec<_>>>()?;
    let arg_types = arg_exprs
        .iter()
        .map(|arg| projected_expr_pg_type(arg, schema))
        .collect::<Vec<_>>();
    if matches!(name.as_str(), "pg_typeof" | "pg_catalog.pg_typeof") {
        return Ok(pg_typeof_result(
            arg_types.first().and_then(Option::as_ref),
            args.first(),
        ));
    }
    if let Some(value) = eval_network_function_value(&name, &args, &arg_types)? {
        return Ok(value);
    }
    if let Some(value) = eval_json_function_call_value(function, &args)? {
        return Ok(value);
    }
    if let Some(value) = crate::eval_xml_function_value(&name, &args, Some(&arg_types))? {
        return Ok(value);
    }
    if let Some(value) = eval_fts_function_value(&name, &args, Some(&arg_types))? {
        return Ok(value);
    }
    if let Some(value) = eval_catalog_function_value(&name, &args) {
        return Ok(value);
    }
    if let Some(value) = eval_compatibility_function_value(&name, &args, Some(&arg_types))? {
        return Ok(value);
    }
    if let Some(value) = eval_spatial_function_value(&name, &args, &arg_types)? {
        return Ok(value);
    }
    Err(SqlError::Unsupported(format!(
        "function {} is not supported in aggregate projection",
        function.name
    )))
}

pub(crate) fn eval_record_aggregate_case(
    records: &[Record],
    operand: Option<&Expr>,
    conditions: &[sqlparser::ast::CaseWhen],
    else_result: Option<&Expr>,
    schema: Option<&TableSchema>,
) -> Result<SqlValue> {
    let operand_value = operand
        .map(|expr| eval_record_aggregate_expr(records, expr, schema))
        .transpose()?;
    for condition in conditions {
        let matched = if let Some(operand_value) = &operand_value {
            values_equal(
                operand_value,
                &eval_record_aggregate_expr(records, &condition.condition, schema)?,
            )
        } else {
            eval_record_aggregate_truth(records, &condition.condition, schema)?.unwrap_or(false)
        };
        if matched {
            return eval_record_aggregate_expr(records, &condition.result, schema);
        }
    }
    else_result
        .map(|expr| eval_record_aggregate_expr(records, expr, schema))
        .unwrap_or(Ok(SqlValue::Null))
}

pub(crate) fn eval_record_aggregate_truth(
    records: &[Record],
    expr: &Expr,
    schema: Option<&TableSchema>,
) -> Result<Option<bool>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(sql_and(
                eval_record_aggregate_truth(records, left, schema)?,
                eval_record_aggregate_truth(records, right, schema)?,
            )),
            BinaryOperator::Or => Ok(sql_or(
                eval_record_aggregate_truth(records, left, schema)?,
                eval_record_aggregate_truth(records, right, schema)?,
            )),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => compare_values(
                &eval_record_aggregate_expr(records, left, schema)?,
                op,
                &eval_record_aggregate_expr(records, right, schema)?,
            ),
            BinaryOperator::PGLikeMatch
            | BinaryOperator::PGILikeMatch
            | BinaryOperator::PGNotLikeMatch
            | BinaryOperator::PGNotILikeMatch
            | BinaryOperator::PGRegexMatch
            | BinaryOperator::PGRegexIMatch
            | BinaryOperator::PGRegexNotMatch
            | BinaryOperator::PGRegexNotIMatch => eval_pg_pattern_operator(
                &eval_record_aggregate_expr(records, left, schema)?,
                op,
                &eval_record_aggregate_expr(records, right, schema)?,
            ),
            BinaryOperator::AtArrow | BinaryOperator::ArrowAt => eval_containment_truth(
                eval_record_aggregate_expr(records, left, schema)?,
                op,
                eval_record_aggregate_expr(records, right, schema)?,
            ),
            _ => eval_record_aggregate_expr(records, expr, schema).and_then(sql_value_truth),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => eval_in_list_truth(
            eval_record_aggregate_expr(records, expr, schema)?,
            list.iter()
                .map(|expr| eval_record_aggregate_expr(records, expr, schema))
                .collect::<Result<Vec<_>>>()?,
            *negated,
        ),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => eval_between_truth(
            eval_record_aggregate_expr(records, expr, schema)?,
            eval_record_aggregate_expr(records, low, schema)?,
            eval_record_aggregate_expr(records, high, schema)?,
            *negated,
        ),
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => eval_quantified_truth(
            eval_record_aggregate_expr(records, left, schema)?,
            compare_op,
            eval_record_aggregate_expr(records, right, schema)?,
            false,
        ),
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => eval_quantified_truth(
            eval_record_aggregate_expr(records, left, schema)?,
            compare_op,
            eval_record_aggregate_expr(records, right, schema)?,
            true,
        ),
        Expr::IsNull(expr) => Ok(Some(value_is_null_predicate(&eval_record_aggregate_expr(
            records, expr, schema,
        )?))),
        Expr::IsNotNull(expr) => Ok(Some(value_is_not_null_predicate(
            &eval_record_aggregate_expr(records, expr, schema)?,
        ))),
        Expr::IsDistinctFrom(left, right) => Ok(Some(!values_not_distinct(
            &eval_record_aggregate_expr(records, left, schema)?,
            &eval_record_aggregate_expr(records, right, schema)?,
        ))),
        Expr::IsNotDistinctFrom(left, right) => Ok(Some(values_not_distinct(
            &eval_record_aggregate_expr(records, left, schema)?,
            &eval_record_aggregate_expr(records, right, schema)?,
        ))),
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any {
                return Err(SqlError::Unsupported(
                    "LIKE ANY is not supported".to_string(),
                ));
            }
            eval_like_values(
                eval_record_aggregate_expr(records, expr, schema)?,
                eval_record_aggregate_expr(records, pattern, schema)?,
                *negated,
                false,
                escape_char.as_ref(),
            )
        }
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any {
                return Err(SqlError::Unsupported(
                    "ILIKE ANY is not supported".to_string(),
                ));
            }
            eval_like_values(
                eval_record_aggregate_expr(records, expr, schema)?,
                eval_record_aggregate_expr(records, pattern, schema)?,
                *negated,
                true,
                escape_char.as_ref(),
            )
        }
        Expr::SimilarTo { .. } => Err(SqlError::Unsupported(
            "SIMILAR TO is not supported".to_string(),
        )),
        Expr::IsTrue(expr) => Ok(Some(matches!(
            eval_record_aggregate_truth(records, expr, schema)?,
            Some(true)
        ))),
        Expr::IsNotTrue(expr) => Ok(Some(!matches!(
            eval_record_aggregate_truth(records, expr, schema)?,
            Some(true)
        ))),
        Expr::IsFalse(expr) => Ok(Some(matches!(
            eval_record_aggregate_truth(records, expr, schema)?,
            Some(false)
        ))),
        Expr::IsNotFalse(expr) => Ok(Some(!matches!(
            eval_record_aggregate_truth(records, expr, schema)?,
            Some(false)
        ))),
        Expr::IsUnknown(expr) => Ok(Some(
            eval_record_aggregate_truth(records, expr, schema)?.is_none(),
        )),
        Expr::IsNotUnknown(expr) => Ok(Some(
            eval_record_aggregate_truth(records, expr, schema)?.is_some(),
        )),
        Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
            Ok(sql_not(eval_record_aggregate_truth(records, expr, schema)?))
        }
        Expr::Nested(expr) => eval_record_aggregate_truth(records, expr, schema),
        _ => eval_record_aggregate_expr(records, expr, schema).and_then(sql_value_truth),
    }
}

pub(crate) fn aggregate_from_expr<'a>(
    expr: &'a Expr,
    schema: Option<&TableSchema>,
) -> Result<(Aggregate, Option<&'a DataType>)> {
    match expr {
        Expr::Function(function) => Ok((Aggregate::from_function(function, schema)?, None)),
        Expr::Cast {
            expr, data_type, ..
        } => {
            let (aggregate, _) = aggregate_from_expr(expr, schema)?;
            Ok((aggregate, Some(data_type)))
        }
        Expr::Nested(expr) => aggregate_from_expr(expr, schema),
        _ => Err(SqlError::Unsupported(
            "aggregate SELECT cannot mix raw fields and aggregates".to_string(),
        )),
    }
}

#[cfg(test)]
mod small_decimal_tests {
    use super::*;

    const SAMPLES: &[&str] = &[
        "0",
        "1",
        "-1",
        "0.00",
        "-0.00",
        "12.50",
        "-12.50",
        "0.05",
        "-0.05",
        "007",
        "007.50",
        "999999999999999999",
        "123456789012.345678",
        "3.14159",
        "100",
        "1.0",
        "1.00",
        "2.5",
        "4500.00",
        "0.0001",
        "-99999.99",
    ];

    #[test]
    fn fast_parse_agrees_with_pg_numeric() {
        for text in SAMPLES {
            let fast = small_decimal_text(text).expect(text);
            let slow = parse_decimal(text).expect(text);
            assert_eq!(BigInt::from(fast.mantissa), slow.mantissa, "{text}");
            assert_eq!(fast.scale, slow.scale, "{text}");
            assert_eq!(small_decimal_format(fast), slow.format(), "{text}");
        }
        for text in [
            "",
            "-",
            ".",
            "1.",
            ".5",
            "+1",
            " 1",
            "1 ",
            "1e3",
            "NaN",
            "abc",
            "1.2.3",
            "--1",
            "1234567890123456789",
            "1234567890.123456789",
        ] {
            assert!(
                small_decimal_text(text).is_none(),
                "{text:?} must fall back"
            );
        }
    }

    #[test]
    fn fast_arithmetic_and_ordering_agree_with_bigint() {
        for left in SAMPLES {
            for right in SAMPLES {
                let (l, r) = (
                    small_decimal_text(left).unwrap(),
                    small_decimal_text(right).unwrap(),
                );
                let (sl, sr) = (parse_decimal(left).unwrap(), parse_decimal(right).unwrap());
                assert_eq!(
                    small_decimal_cmp(l, r),
                    Some(sl.cmp(&sr)),
                    "{left} vs {right}"
                );
                for op in [
                    BinaryOperator::Plus,
                    BinaryOperator::Minus,
                    BinaryOperator::Multiply,
                ] {
                    let fast = small_decimal_arithmetic(l, &op, r).expect("in budget");
                    let slow = eval_decimal_arithmetic(
                        parse_decimal(left).unwrap(),
                        &op,
                        parse_decimal(right).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(fast, slow, "{left} {op:?} {right}");
                }
                assert!(small_decimal_arithmetic(l, &BinaryOperator::Divide, r).is_none());
            }
        }
    }

    #[test]
    fn plausibility_prefilter_never_rejects_valid_numeric_text() {
        for text in [
            "1", " 1", "+1", "-1", ".5", "NaN", "nan", "Infinity", "-inf", "1e5",
        ] {
            assert!(numeric_text_plausible(text), "{text:?}");
        }
        assert!(!numeric_text_plausible("BARBAR"));
        assert!(!numeric_text_plausible(""));
        assert!(!numeric_text_plausible("x1"));
    }
}
