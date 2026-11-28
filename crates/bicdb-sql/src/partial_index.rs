//! `CREATE [UNIQUE] INDEX ... WHERE predicate`: translates the SQL predicate of a partial index into the
//! storage engine's [`IndexPredicate`], keeping PostgreSQL's NULL semantics (a row belongs to the index
//! only when the predicate is *true*; NULL and false both leave it out).
//!
//! Supported (what PostgreSQL schemas use for partial indexes): `col IS [NOT] NULL`, `col IS [NOT]
//! TRUE/FALSE`, a bare boolean column and `NOT col`, `col = / <> / < / <= / > / >= literal` (either side),
//! JSONB key existence (`col ? 'key'`), `col IN (...)` / `col = ANY (ARRAY[...])` / `NOT IN`, `AND`, `OR`, parentheses and casts of literals.
//! String predicates can use constant LIKE patterns, and comparisons can use an indexable COALESCE
//! with one type-preserving column and one constant fallback.
//! Comparisons are limited to columns stored as plain JSON (text, boolean, integer and float types);
//! `IS [NOT] NULL` works on every column. Anything else is reported as unsupported rather than
//! silently indexing every row, which would make a partial UNIQUE index reject valid rows.
use crate::*;
use bicdb_core::{
    IndexField, IndexPredicate, IndexPredicateBinaryOperator, IndexPredicateValueType,
};

pub(crate) fn partial_index_predicate(
    schema: &TableSchema,
    predicate: &Expr,
) -> Result<IndexPredicate> {
    translate(schema, predicate)
}

fn unsupported(predicate: &Expr) -> SqlError {
    SqlError::Unsupported(format!(
        "partial index predicate {predicate} is not supported; supported forms are IS [NOT] NULL, \
         boolean columns, comparisons with constants, LIKE patterns, JSONB key existence, IN lists, AND and OR"
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
    binary(
        IndexPredicateBinaryOperator::And,
        left,
        right,
        IndexPredicateValueType::Bool,
    )
}

fn or(left: IndexPredicate, right: IndexPredicate) -> IndexPredicate {
    binary(
        IndexPredicateBinaryOperator::Or,
        left,
        right,
        IndexPredicateValueType::Bool,
    )
}

/// A column reference of this table (or a text field of a json/jsonb
/// column, `column ->> 'key'`), with how its values compare to constants.
struct ColumnRef {
    field: IndexPredicate,
    value_type: Option<IndexPredicateValueType>,
}

fn strip(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => strip(inner),
        _ => expr,
    }
}

fn column_ident(expr: &Expr) -> Option<&Ident> {
    match strip(expr) {
        Expr::Identifier(ident) => Some(ident),
        Expr::CompoundIdentifier(parts) => parts.last(),
        _ => None,
    }
}

fn coalesce_ref(schema: &TableSchema, expression: &Expr) -> Option<ColumnRef> {
    let Expr::Function(function) = strip(expression) else {
        return None;
    };
    if !function_name_is(function, "coalesce") {
        return None;
    }
    let mut function = function.clone();
    let FunctionArguments::List(args) = &mut function.args else {
        return None;
    };
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(input)), _] = args.args.as_mut_slice() else {
        return None;
    };
    let ident = column_ident(input)?.clone();
    let column = schema.column(&ident_value(&ident))?;
    *input = Expr::Identifier(ident);
    // Use the same typed constant fallback and field identity as index keys.
    let field = index_field_from_expr_for_schema(schema, &Expr::Function(function)).ok()?;
    if !matches!(field, IndexField::Coalesce { .. }) {
        return None;
    }
    Some(ColumnRef {
        field: IndexPredicate::Field { field },
        value_type: comparable_type(column),
    })
}

fn column_ref(schema: &TableSchema, expr: &Expr) -> Option<ColumnRef> {
    if let Some(reference) =
        json_text_field_ref(schema, expr).or_else(|| coalesce_ref(schema, expr))
    {
        return Some(reference);
    }
    let ident = column_ident(expr)?;
    let column = schema.column(&ident_value(ident))?;
    // A validated SQL qualifier names the table, not a field in its record.
    let field = index_field_from_expr_for_schema(schema, &Expr::Identifier(ident.clone())).ok()?;
    Some(ColumnRef {
        field: IndexPredicate::Field { field },
        value_type: comparable_type(column),
    })
}

/// `json_column ->> 'key'`: the key's value inside a json/jsonb column, which
/// is stored as plain JSON, so the predicate reads it by path. Like
/// PostgreSQL's `->>`, a missing key or JSON null reads as SQL NULL. Only
/// string values compare equal to a text constant (`->>` would also render a
/// JSON number or boolean as text; those rows stay outside the index).
fn json_text_field_ref(schema: &TableSchema, expr: &Expr) -> Option<ColumnRef> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::LongArrow,
        right,
    } = strip(expr)
    else {
        return None;
    };
    let ident = column_ident(left)?;
    let column = schema.column(&ident_value(ident))?;
    if !matches!(
        column.pg_type.to_ascii_lowercase().as_str(),
        "json" | "jsonb"
    ) {
        return None;
    }
    let SqlValue::String(key) = eval_constant_expr(strip(right)).ok()? else {
        return None;
    };
    let IndexField::MetadataPath(mut path) =
        index_field_from_expr_for_schema(schema, &Expr::Identifier(ident.clone())).ok()?
    else {
        return None;
    };
    path.push(key);
    Some(ColumnRef {
        field: IndexPredicate::Field {
            field: IndexField::MetadataPath(path),
        },
        value_type: Some(IndexPredicateValueType::String),
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
    column: &ColumnRef,
    operator: IndexPredicateBinaryOperator,
    constant: JsonValue,
    value_type: IndexPredicateValueType,
) -> IndexPredicate {
    let comparison = binary(
        operator,
        column.field.clone(),
        literal(constant),
        value_type,
    );
    match operator {
        // JSON inequality would be true for a NULL column; SQL's `<>` is not.
        IndexPredicateBinaryOperator::NotEqual => {
            and(is_not_null(column.field.clone()), comparison)
        }
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
fn membership(
    schema: &TableSchema,
    column_expr: &Expr,
    values: &[Expr],
    negated: bool,
    whole: &Expr,
) -> Result<IndexPredicate> {
    let column = column_ref(schema, column_expr).ok_or_else(|| unsupported(whole))?;
    let value_type = column.value_type.ok_or_else(|| unsupported(whole))?;
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
        result = if negated {
            and(result, next)
        } else {
            or(result, next)
        };
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

fn pattern_literal(expr: &Expr) -> Option<SqlValue> {
    match strip(expr) {
        Expr::Value(_) => eval_constant_expr(expr).ok(),
        Expr::Cast { expr: inner, .. } => {
            pattern_literal(inner)?;
            eval_constant_expr(expr).ok()
        }
        _ => None,
    }
}

fn like_predicate(
    schema: &TableSchema,
    value: &Expr,
    pattern: &Expr,
    escape: Option<&ValueWithSpan>,
    negated: bool,
    whole: &Expr,
) -> Result<IndexPredicate> {
    let ident = column_ident(value).ok_or_else(|| unsupported(whole))?;
    let column = schema
        .column(&ident_value(ident))
        .ok_or_else(|| unsupported(whole))?;
    if column.user_type.is_some()
        || !matches!(column.pg_type.as_str(), "text" | "varchar" | "name")
        || column
            .collation
            .as_deref()
            .is_some_and(|collation| !matches!(collation, "default" | "C" | "POSIX" | "ucs_basic"))
    {
        return Err(unsupported(whole));
    }
    let pattern = match pattern_literal(pattern) {
        Some(SqlValue::String(pattern)) => pattern,
        Some(SqlValue::Null) => return Ok(literal(JsonValue::Bool(false))),
        _ => return Err(unsupported(whole)),
    };
    let escape = match escape.map(literal_to_value).transpose()? {
        None => Some('\\'),
        Some(SqlValue::Null) => return Ok(literal(JsonValue::Bool(false))),
        Some(SqlValue::String(escape)) => {
            let mut chars = escape.chars();
            let escape = chars.next();
            if chars.next().is_some() {
                return Err(invalid_like_escape());
            }
            escape
        }
        _ => return Err(unsupported(whole)),
    };
    if bicdb_core::like_pattern_ends_with_escape(&pattern, escape) {
        return Err(SqlError::data_exception(
            "22025",
            "LIKE pattern must not end with escape character",
            None,
        ));
    }
    let value = column_ref(schema, value)
        .ok_or_else(|| unsupported(whole))?
        .field;
    Ok(IndexPredicate::Like {
        value: Box::new(value),
        pattern,
        escape,
        negated,
    })
}

fn translate(schema: &TableSchema, predicate: &Expr) -> Result<IndexPredicate> {
    let expr = strip(predicate);
    match expr {
        Expr::Function(function) if function_name_is(function, "coalesce") => {
            let column = coalesce_ref(schema, expr).ok_or_else(|| unsupported(predicate))?;
            if column.value_type != Some(IndexPredicateValueType::Bool) {
                return Err(unsupported(predicate));
            }
            Ok(compare(
                &column,
                IndexPredicateBinaryOperator::Equal,
                JsonValue::Bool(true),
                IndexPredicateValueType::Bool,
            ))
        }
        Expr::Like {
            negated,
            any: false,
            expr,
            pattern,
            escape_char,
        } => like_predicate(
            schema,
            expr,
            pattern,
            escape_char.as_ref(),
            *negated,
            predicate,
        ),
        Expr::IsNull(inner) => {
            let column = column_ref(schema, inner).ok_or_else(|| unsupported(predicate))?;
            Ok(is_null(column.field))
        }
        Expr::IsNotNull(inner) => {
            let column = column_ref(schema, inner).ok_or_else(|| unsupported(predicate))?;
            Ok(is_not_null(column.field))
        }
        Expr::IsTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner) => {
            let column = column_ref(schema, inner).ok_or_else(|| unsupported(predicate))?;
            if column.value_type != Some(IndexPredicateValueType::Bool) {
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
            if column.value_type != Some(IndexPredicateValueType::Bool) {
                return Err(unsupported(predicate));
            }
            Ok(compare(
                &column,
                IndexPredicateBinaryOperator::Equal,
                JsonValue::Bool(true),
                IndexPredicateValueType::Bool,
            ))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => match strip(inner) {
            // NOT of these is again a two-valued test, so it stays exact.
            Expr::IsNull(column) => translate(schema, &Expr::IsNotNull(column.clone())),
            Expr::IsNotNull(column) => translate(schema, &Expr::IsNull(column.clone())),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                let column = column_ref(schema, inner).ok_or_else(|| unsupported(predicate))?;
                if column.value_type != Some(IndexPredicateValueType::Bool) {
                    return Err(unsupported(predicate));
                }
                Ok(compare(
                    &column,
                    IndexPredicateBinaryOperator::Equal,
                    JsonValue::Bool(false),
                    IndexPredicateValueType::Bool,
                ))
            }
            Expr::InList {
                expr: column,
                list,
                negated,
            } => membership(schema, column, list, !negated, predicate),
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
                translate(
                    schema,
                    &Expr::BinaryOp {
                        left: left.clone(),
                        op: negated,
                        right: right.clone(),
                    },
                )
            }
            _ => Err(unsupported(predicate)),
        },
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Ok(and(translate(schema, left)?, translate(schema, right)?)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Ok(or(translate(schema, left)?, translate(schema, right)?)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Question,
            right,
        } => {
            let ident = column_ident(left).ok_or_else(|| unsupported(predicate))?;
            let column = schema
                .column(&ident_value(ident))
                .ok_or_else(|| unsupported(predicate))?;
            if column.pg_type != "jsonb" || column.user_type.is_some() {
                return Err(unsupported(predicate));
            }
            let SqlValue::String(key) = eval_constant_expr(strip(right))? else {
                return Err(unsupported(predicate));
            };
            let field = index_field_from_expr_for_schema(schema, &Expr::Identifier(ident.clone()))?;
            Ok(binary(
                IndexPredicateBinaryOperator::JsonHasKey,
                IndexPredicate::Field { field },
                literal(JsonValue::String(key)),
                IndexPredicateValueType::Json,
            ))
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
            let value_type = column.value_type.ok_or_else(|| unsupported(predicate))?;
            if matches!(
                value_type,
                IndexPredicateValueType::Bool | IndexPredicateValueType::String
            ) && !matches!(
                operator,
                IndexPredicateBinaryOperator::Equal | IndexPredicateBinaryOperator::NotEqual
            ) {
                // Text ordering depends on the collation; the engine compares bytes.
                return Err(unsupported(predicate));
            }
            let constant =
                constant_json(constant, value_type).ok_or_else(|| unsupported(predicate))?;
            Ok(compare(&column, operator, constant, value_type))
        }
        Expr::InList {
            expr: column,
            list,
            negated,
        } => membership(schema, column, list, *negated, predicate),
        Expr::AnyOp {
            left,
            compare_op: BinaryOperator::Eq,
            right,
            ..
        } => {
            let values = array_elements(right).ok_or_else(|| unsupported(predicate))?;
            membership(schema, left, &values, false, predicate)
        }
        Expr::AllOp {
            left,
            compare_op: BinaryOperator::NotEq,
            right,
        } => {
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

/// Lower the usable conditions of a conflict inference predicate. An
/// unrecognized conjunct may strengthen known conditions. An unrecognized
/// disjunct cannot establish a bound on the rows the expression describes.
pub(crate) fn inference_predicate(schema: &TableSchema, expr: &Expr) -> Option<IndexPredicate> {
    match strip(expr) {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (
            inference_predicate(schema, left),
            inference_predicate(schema, right),
        ) {
            (Some(left), Some(right)) => Some(and(left, right)),
            (other, None) | (None, other) => other,
        },
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Some(or(
            inference_predicate(schema, left)?,
            inference_predicate(schema, right)?,
        )),
        _ => {
            let static_atom = sqlparser::ast::visit_expressions(expr, |expr| {
                if matches!(expr, Expr::Function(function) if !function_name_is(function, "coalesce"))
                    || matches!(expr, Expr::Identifier(ident) if ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case("current_date"))
                {
                    std::ops::ControlFlow::Break(())
                } else {
                    std::ops::ControlFlow::Continue(())
                }
            }).is_continue();
            static_atom
                .then(|| partial_index_predicate(schema, expr).ok())
                .flatten()
        }
    }
}

/// Test that every true row of the supplied inference predicate can enter
/// the index. Stay conservative when the storage predicates give us no safe
/// proof, including ordered text comparisons with an unknown collation.
pub(crate) fn inference_implies(supplied: &IndexPredicate, index: &IndexPredicate) -> bool {
    implies(supplied, index, &mut 16_384)
}

fn implies(supplied: &IndexPredicate, index: &IndexPredicate, budget: &mut usize) -> bool {
    use IndexPredicateBinaryOperator::{And, Or};

    if *budget == 0 {
        return false;
    }
    *budget -= 1;
    if supplied == index || matches!(index, IndexPredicate::Literal { value } if value == &true) {
        return true;
    }
    if let IndexPredicate::Binary {
        operator: And,
        left,
        right,
        ..
    } = index
    {
        return implies(supplied, left, budget) && implies(supplied, right, budget);
    }
    if let IndexPredicate::Binary {
        operator: Or,
        left,
        right,
        ..
    } = supplied
    {
        return implies(left, index, budget) && implies(right, index, budget);
    }
    if let IndexPredicate::Binary {
        operator: Or,
        left,
        right,
        ..
    } = index
    {
        if implies(supplied, left, budget) || implies(supplied, right, budget) {
            return true;
        }
    }
    if let IndexPredicate::Binary {
        operator: And,
        left,
        right,
        ..
    } = supplied
    {
        return implies(left, index, budget) || implies(right, index, budget);
    }
    comparison_implies(supplied, index)
}

fn comparison_parts(
    predicate: &IndexPredicate,
) -> Option<(
    &IndexField,
    IndexPredicateBinaryOperator,
    &JsonValue,
    IndexPredicateValueType,
)> {
    let IndexPredicate::Binary {
        operator,
        left,
        right,
        value_type,
    } = predicate
    else {
        return None;
    };
    let (IndexPredicate::Field { field }, IndexPredicate::Literal { value }) =
        (left.as_ref(), right.as_ref())
    else {
        return None;
    };
    Some((field, *operator, value, *value_type))
}

fn comparison_implies(supplied: &IndexPredicate, index: &IndexPredicate) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    use IndexPredicateBinaryOperator::{
        Equal as Eq, Greater as Gt, GreaterEqual as Ge, Less as Lt, LessEqual as Le, NotEqual as Ne,
    };

    let Some((source, source_op, source_value, source_type)) = comparison_parts(supplied) else {
        return false;
    };
    let Some((target, target_op, target_value, target_type)) = comparison_parts(index) else {
        return false;
    };
    if source != target || source_value.is_null() {
        return false;
    }
    if target_op == Ne && target_value.is_null() {
        // Comparisons to non-null constants reject SQL NULL (the compiled
        // inequality also carries its own IS NOT NULL guard).
        return matches!(source_op, Eq | Gt | Ge | Lt | Le);
    }
    if source_type != target_type {
        return false;
    }
    let ordering = match source_type {
        IndexPredicateValueType::Int64 => source_value
            .as_i64()
            .zip(target_value.as_i64())
            .map(|(left, right)| left.cmp(&right)),
        IndexPredicateValueType::Float64 => source_value
            .as_f64()
            .zip(target_value.as_f64())
            .and_then(|(left, right)| left.partial_cmp(&right)),
        _ => None,
    };
    let Some(ordering) = ordering else {
        return false;
    };
    match (source_op, target_op) {
        (Eq, Eq) => ordering == Equal,
        (Eq, Ne) => ordering != Equal,
        (Eq, Gt) => ordering == Greater,
        (Eq, Ge) => ordering != Less,
        (Eq, Lt) => ordering == Less,
        (Eq, Le) => ordering != Greater,
        (Gt, Gt | Ge | Ne) | (Ge, Ge) => ordering != Less,
        (Ge, Gt | Ne) => ordering == Greater,
        (Lt, Lt | Le | Ne) | (Le, Le) => ordering != Greater,
        (Le, Lt | Ne) => ordering == Less,
        _ => false,
    }
}
