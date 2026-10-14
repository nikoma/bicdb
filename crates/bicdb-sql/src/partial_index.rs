//! `CREATE [UNIQUE] INDEX ... WHERE predicate`: translates the SQL predicate of a partial index into the
//! storage engine's [`IndexPredicate`], keeping PostgreSQL's NULL semantics (a row belongs to the index
//! only when the predicate is *true*; NULL and false both leave it out).
//!
//! Supported (what PostgreSQL schemas use for partial indexes): `col IS [NOT] NULL`, `col IS [NOT]
//! TRUE/FALSE`, a bare boolean column and `NOT col`, `col = / <> / < / <= / > / >= literal` (either side),
//! `col IN (...)` / `col = ANY (ARRAY[...])` / `NOT IN`, `AND`, `OR`, parentheses and casts of literals.
//! Comparisons are limited to columns stored as plain JSON (text, boolean, integer and float types);
//! `IS [NOT] NULL` works on every column. Anything else is reported as unsupported rather than
//! silently indexing every row, which would make a partial UNIQUE index reject valid rows.
use crate::*;
use bicdb_core::{IndexPredicate, IndexPredicateBinaryOperator, IndexPredicateValueType};

pub(crate) fn partial_index_predicate(
    schema: &TableSchema,
    predicate: &Expr,
) -> Result<IndexPredicate> {
    translate(schema, predicate)
}

fn unsupported(predicate: &Expr) -> SqlError {
    SqlError::Unsupported(format!(
        "partial index predicate {predicate} is not supported; supported forms are IS [NOT] NULL, \
         boolean columns, comparisons of a column with a constant, IN lists, AND and OR"
    ))
}

fn binary(
    operator: IndexPredicateBinaryOperator,
    left: IndexPredicate,
    right: IndexPredicate,
    value_type: IndexPredicateValueType,
) -> IndexPredicate {
    IndexPredicate::Binary {
        operator,
        left: Box::new(left),
        right: Box::new(right),
        value_type,
    }
}

fn literal(value: JsonValue) -> IndexPredicate {
    IndexPredicate::Literal { value }
}

fn and(left: IndexPredicate, right: IndexPredicate) -> IndexPredicate {
    binary(IndexPredicateBinaryOperator::And, left, right, IndexPredicateValueType::Bool)
}

fn or(left: IndexPredicate, right: IndexPredicate) -> IndexPredicate {
    binary(IndexPredicateBinaryOperator::Or, left, right, IndexPredicateValueType::Bool)
}

/// A column reference of this table, with its type.
struct ColumnRef<'a> {
    field: IndexPredicate,
    column: &'a ColumnSchema,
}

fn strip(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => strip(inner),
        _ => expr,
    }
}

fn column_ref<'a>(schema: &'a TableSchema, expr: &Expr) -> Option<ColumnRef<'a>> {
    let name = match strip(expr) {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts.last()?.value.clone(),
        _ => return None,
    };
    let column = schema.column(&name)?;
    let field = index_field_from_expr_for_schema(schema, strip(expr)).ok()?;
    Some(ColumnRef {
        field: IndexPredicate::Field { field },
        column,
    })
}

/// How the column's values are stored, when predicates may compare them with constants.
fn comparable_type(column: &ColumnSchema) -> Option<IndexPredicateValueType> {
    let pg_type = column.pg_type.to_ascii_lowercase();
    if column.user_type.is_some() || uses_typed_storage(&pg_type) {
        return None;
    }
    match pg_type.as_str() {
        "text" | "varchar" | "name" | "citext" => Some(IndexPredicateValueType::String),
        "bool" | "boolean" => Some(IndexPredicateValueType::Bool),
        "int2" | "int4" | "int8" | "smallint" | "integer" | "bigint" => {
            Some(IndexPredicateValueType::Int64)
        }
        "float4" | "float8" | "real" | "double precision" => Some(IndexPredicateValueType::Float64),
        _ => None,
    }
}

/// A constant (possibly cast) converted to the JSON the column stores.
fn constant_json(expr: &Expr, value_type: IndexPredicateValueType) -> Option<JsonValue> {
    let value = eval_constant_expr(strip(expr)).ok()?;
    let json = match (value_type, value) {
        (_, SqlValue::Null) => return None,
        (IndexPredicateValueType::String, SqlValue::String(text)) => JsonValue::String(text),
        (IndexPredicateValueType::Bool, SqlValue::Bool(flag)) => JsonValue::Bool(flag),
        (IndexPredicateValueType::Bool, SqlValue::String(text)) => {
            JsonValue::Bool(match text.to_ascii_lowercase().as_str() {
                "t" | "true" | "y" | "yes" | "on" | "1" => true,
                "f" | "false" | "n" | "no" | "off" | "0" => false,
                _ => return None,
            })
        }
        (IndexPredicateValueType::Int64, SqlValue::Int(number)) => JsonValue::from(number),
        (IndexPredicateValueType::Int64, SqlValue::String(text)) => {
            JsonValue::from(text.trim().parse::<i64>().ok()?)
        }
        (IndexPredicateValueType::Float64, SqlValue::Int(number)) => JsonValue::from(number as f64),
        (IndexPredicateValueType::Float64, SqlValue::Float(number)) => JsonValue::from(number),
        _ => return None,
    };
    Some(json)
}

fn is_null(field: IndexPredicate) -> IndexPredicate {
    binary(
        IndexPredicateBinaryOperator::Equal,
        field,
        literal(JsonValue::Null),
        IndexPredicateValueType::Json,
    )
}

fn is_not_null(field: IndexPredicate) -> IndexPredicate {
    binary(
        IndexPredicateBinaryOperator::NotEqual,
        field,
        literal(JsonValue::Null),
        IndexPredicateValueType::Json,
    )
}

/// `column <op> constant` (true only for non-NULL column values, as in SQL).
fn compare(
    column: &ColumnRef<'_>,
    operator: IndexPredicateBinaryOperator,
    constant: JsonValue,
    value_type: IndexPredicateValueType,
) -> IndexPredicate {
    let comparison = binary(operator, column.field.clone(), literal(constant), value_type);
    match operator {
        // JSON inequality would be true for a NULL column; SQL's `<>` is not.
        IndexPredicateBinaryOperator::NotEqual => and(is_not_null(column.field.clone()), comparison),
        _ => comparison,
    }
}

fn flip(operator: &BinaryOperator) -> Option<IndexPredicateBinaryOperator> {
    Some(match operator {
        BinaryOperator::Eq => IndexPredicateBinaryOperator::Equal,
        BinaryOperator::NotEq => IndexPredicateBinaryOperator::NotEqual,
        BinaryOperator::Lt => IndexPredicateBinaryOperator::Greater,
        BinaryOperator::LtEq => IndexPredicateBinaryOperator::GreaterEqual,
        BinaryOperator::Gt => IndexPredicateBinaryOperator::Less,
        BinaryOperator::GtEq => IndexPredicateBinaryOperator::LessEqual,
        _ => return None,
    })
}

fn comparison_operator(operator: &BinaryOperator) -> Option<IndexPredicateBinaryOperator> {
    Some(match operator {
        BinaryOperator::Eq => IndexPredicateBinaryOperator::Equal,
        BinaryOperator::NotEq => IndexPredicateBinaryOperator::NotEqual,
        BinaryOperator::Lt => IndexPredicateBinaryOperator::Less,
        BinaryOperator::LtEq => IndexPredicateBinaryOperator::LessEqual,
        BinaryOperator::Gt => IndexPredicateBinaryOperator::Greater,
        BinaryOperator::GtEq => IndexPredicateBinaryOperator::GreaterEqual,
        _ => return None,
    })
}

/// `column IN (a, b)` / `column = ANY (ARRAY[a, b])`: one equality per constant.
fn membership(schema: &TableSchema, column_expr: &Expr, values: &[Expr], negated: bool, whole: &Expr) -> Result<IndexPredicate> {
    let column = column_ref(schema, column_expr).ok_or_else(|| unsupported(whole))?;
    let value_type = comparable_type(column.column).ok_or_else(|| unsupported(whole))?;
    let mut constants = Vec::with_capacity(values.len());
    for value in values {
        constants.push(constant_json(value, value_type).ok_or_else(|| unsupported(whole))?);
    }
    let Some(first) = constants.first().cloned() else {
        // `IN ()` is never true; `NOT IN ()` is true for every row, NULL included.
        return Ok(literal(JsonValue::Bool(negated)));
    };
    let operator = if negated {
        IndexPredicateBinaryOperator::NotEqual
    } else {
        IndexPredicateBinaryOperator::Equal
    };
    let mut result = compare(&column, operator, first, value_type);
    for constant in constants.into_iter().skip(1) {
        let next = compare(&column, operator, constant, value_type);
        result = if negated { and(result, next) } else { or(result, next) };
    }
    Ok(result)
}

fn array_elements(expr: &Expr) -> Option<Vec<Expr>> {
    match strip(expr) {
        Expr::Array(array) => Some(array.elem.clone()),
        Expr::Cast { expr, .. } => array_elements(expr),
        _ => None,
    }
}

fn translate(schema: &TableSchema, predicate: &Expr) -> Result<IndexPredicate> {
    let expr = strip(predicate);
    match expr {
        Expr::IsNull(inner) => {
            let column = column_ref(schema, inner).ok_or_else(|| unsupported(predicate))?;
            Ok(is_null(column.field))
        }
        Expr::IsNotNull(inner) => {
            let column = column_ref(schema, inner).ok_or_else(|| unsupported(predicate))?;
            Ok(is_not_null(column.field))
        }
        Expr::IsTrue(inner) | Expr::IsNotFalse(inner) | Expr::IsFalse(inner) | Expr::IsNotTrue(inner) => {
            let column = column_ref(schema, inner).ok_or_else(|| unsupported(predicate))?;
            if comparable_type(column.column) != Some(IndexPredicateValueType::Bool) {
                return Err(unsupported(predicate));
            }
            let equal = |flag: bool| {
                binary(
                    IndexPredicateBinaryOperator::Equal,
                    column.field.clone(),
                    literal(JsonValue::Bool(flag)),
                    IndexPredicateValueType::Bool,
                )
            };
            Ok(match expr {
                Expr::IsTrue(_) => equal(true),
                Expr::IsFalse(_) => equal(false),
                // IS NOT FALSE / IS NOT TRUE are true for NULL as well.
                Expr::IsNotFalse(_) => or(equal(true), is_null(column.field.clone())),
                _ => or(equal(false), is_null(column.field.clone())),
            })
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let column = column_ref(schema, expr).ok_or_else(|| unsupported(predicate))?;
            if comparable_type(column.column) != Some(IndexPredicateValueType::Bool) {
                return Err(unsupported(predicate));
            }
            Ok(compare(&column, IndexPredicateBinaryOperator::Equal, JsonValue::Bool(true), IndexPredicateValueType::Bool))
        }
        Expr::UnaryOp { op: UnaryOperator::Not, expr: inner } => match strip(inner) {
            // NOT of these is again a two-valued test, so it stays exact.
            Expr::IsNull(column) => translate(schema, &Expr::IsNotNull(column.clone())),
            Expr::IsNotNull(column) => translate(schema, &Expr::IsNull(column.clone())),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                let column = column_ref(schema, inner).ok_or_else(|| unsupported(predicate))?;
                if comparable_type(column.column) != Some(IndexPredicateValueType::Bool) {
                    return Err(unsupported(predicate));
                }
                Ok(compare(&column, IndexPredicateBinaryOperator::Equal, JsonValue::Bool(false), IndexPredicateValueType::Bool))
            }
            Expr::InList { expr: column, list, negated } => {
                membership(schema, column, list, !negated, predicate)
            }
            Expr::BinaryOp { left, op, right } if comparison_operator(op).is_some() => {
                // NOT (a op b) is (a negated-op b) for non-NULL a, and not true for NULL a.
                let negated = match op {
                    BinaryOperator::Eq => BinaryOperator::NotEq,
                    BinaryOperator::NotEq => BinaryOperator::Eq,
                    BinaryOperator::Lt => BinaryOperator::GtEq,
                    BinaryOperator::LtEq => BinaryOperator::Gt,
                    BinaryOperator::Gt => BinaryOperator::LtEq,
                    _ => BinaryOperator::Lt,
                };
                translate(schema, &Expr::BinaryOp { left: left.clone(), op: negated, right: right.clone() })
            }
            _ => Err(unsupported(predicate)),
        },
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            Ok(and(translate(schema, left)?, translate(schema, right)?))
        }
        Expr::BinaryOp { left, op: BinaryOperator::Or, right } => {
            Ok(or(translate(schema, left)?, translate(schema, right)?))
        }
        Expr::BinaryOp { left, op, right } => {
            let (column, constant, operator) = if let Some(column) = column_ref(schema, left) {
                (column, right, comparison_operator(op))
            } else if let Some(column) = column_ref(schema, right) {
                (column, left, flip(op))
            } else {
                return Err(unsupported(predicate));
            };
            let operator = operator.ok_or_else(|| unsupported(predicate))?;
            let value_type = comparable_type(column.column).ok_or_else(|| unsupported(predicate))?;
            if matches!(value_type, IndexPredicateValueType::Bool | IndexPredicateValueType::String)
                && !matches!(operator, IndexPredicateBinaryOperator::Equal | IndexPredicateBinaryOperator::NotEqual)
            {
                // Text ordering depends on the collation; the engine compares bytes.
                return Err(unsupported(predicate));
            }
            let constant = constant_json(constant, value_type).ok_or_else(|| unsupported(predicate))?;
            Ok(compare(&column, operator, constant, value_type))
        }
        Expr::InList { expr: column, list, negated } => membership(schema, column, list, *negated, predicate),
        Expr::AnyOp { left, compare_op: BinaryOperator::Eq, right, .. } => {
            let values = array_elements(right).ok_or_else(|| unsupported(predicate))?;
            membership(schema, left, &values, false, predicate)
        }
        Expr::AllOp { left, compare_op: BinaryOperator::NotEq, right } => {
            let values = array_elements(right).ok_or_else(|| unsupported(predicate))?;
            membership(schema, left, &values, true, predicate)
        }
        Expr::Value(_) => match eval_constant_expr(expr)? {
            SqlValue::Bool(flag) => Ok(literal(JsonValue::Bool(flag))),
            _ => Err(unsupported(predicate)),
        },
        _ => Err(unsupported(predicate)),
    }
}
