//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

#[derive(Clone, Debug)]
pub(crate) enum Aggregate {
    CountAll,
    CountField(FieldRef, bool),
    Sum(FieldRef, bool, bool),
    Avg(FieldRef, bool),
    Min(FieldRef, Option<String>),
    Max(FieldRef, Option<String>),
    BoolAnd(FieldRef, bool),
    BoolOr(FieldRef, bool),
    StringAgg {
        expr: Expr,
        delimiter: Expr,
        distinct: bool,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        filter: Option<Expr>,
    },
    ArrayAgg {
        expr: Expr,
        distinct: bool,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        filter: Option<Expr>,
    },
    RangeAgg {
        field: FieldRef,
        distinct: bool,
        intersect: bool,
        input_type: String,
    },
    XmlAgg {
        expr: Expr,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        filter: Option<Expr>,
    },
    JsonAgg {
        expr: Expr,
        distinct: bool,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        filter: Option<Expr>,
        binary: bool,
        strict: bool,
        standard: bool,
        returning: Option<DataType>,
    },
    JsonObjectAgg {
        key: Expr,
        value: Expr,
        distinct: bool,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        filter: Option<Expr>,
        binary: bool,
        strict: bool,
        unique: bool,
        standard: bool,
        returning: Option<DataType>,
    },
}

impl Aggregate {
    pub(crate) fn from_function(function: &Function, schema: Option<&TableSchema>) -> Result<Self> {
        let name = object_name(&function.name)?.to_ascii_lowercase();
        if name.ends_with("_encoding_error") {
            return Err(crate::jsonb::json_output_encoding_error());
        }
        let distinct = function_args_are_distinct(function);
        match name.as_str() {
            "count" => {
                if is_count_star(function) {
                    Ok(Self::CountAll)
                } else {
                    Ok(Self::CountField(
                        single_function_field(function, schema)?,
                        distinct,
                    ))
                }
            }
            name if json_array_aggregate_flags(name).is_some() => {
                let (binary, default_strict) = json_array_aggregate_flags(name).unwrap();
                let standard = name.strip_prefix("pg_catalog.").unwrap_or(name) == "json_arrayagg";
                let (strict, returning) =
                    json_standard_aggregate_options(function, default_strict, standard)?;
                let (expr, distinct, order_by) = json_agg_function_parts(function)?;
                Ok(Self::JsonAgg {
                    expr,
                    distinct,
                    order_by,
                    filter: function.filter.as_deref().cloned(),
                    binary,
                    strict,
                    standard,
                    returning,
                })
            }
            name if json_object_aggregate_flags(name).is_some() => {
                let (binary, default_strict, unique) = json_object_aggregate_flags(name).unwrap();
                let standard = matches!(
                    name.strip_prefix("pg_catalog.").unwrap_or(name),
                    "json_objectagg" | "json_objectagg_unique"
                );
                let (strict, returning) =
                    json_standard_aggregate_options(function, default_strict, standard)?;
                let (key, value, distinct, order_by) = json_object_agg_function_parts(function)?;
                Ok(Self::JsonObjectAgg {
                    key,
                    value,
                    distinct,
                    order_by,
                    filter: function.filter.as_deref().cloned(),
                    binary,
                    strict,
                    unique,
                    standard,
                    returning,
                })
            }
            "xmlagg" | "pg_catalog.xmlagg" => {
                let (expr, distinct, order_by) = json_agg_function_parts(function)?;
                if distinct {
                    return Err(SqlError::undefined_function(
                        "could not identify an equality operator for type xml",
                    ));
                }
                Ok(Self::XmlAgg {
                    expr,
                    order_by,
                    filter: function.filter.as_deref().cloned(),
                })
            }
            "bool_and"
            | "pg_catalog.bool_and"
            | "every"
            | "pg_catalog.every"
            | "bool_or"
            | "pg_catalog.bool_or" => {
                let field = single_function_field(function, schema)?;
                if field_pg_type(&field, schema).is_some_and(|pg_type| pg_type != "bool") {
                    return Err(SqlError::undefined_function(format!(
                        "function {name} requires boolean"
                    )));
                }
                if name.ends_with("bool_or") {
                    Ok(Self::BoolOr(field, distinct))
                } else {
                    Ok(Self::BoolAnd(field, distinct))
                }
            }
            "string_agg" | "pg_catalog.string_agg" => {
                let (expr, delimiter, distinct, order_by) = string_agg_function_parts(function)?;
                Ok(Self::StringAgg {
                    expr,
                    delimiter,
                    distinct,
                    order_by,
                    filter: function.filter.as_deref().cloned(),
                })
            }
            "range_agg"
            | "pg_catalog.range_agg"
            | "range_intersect_agg"
            | "pg_catalog.range_intersect_agg" => {
                let field = single_function_field(function, schema)?;
                let input_type = field_pg_type(&field, schema).ok_or_else(|| {
                    SqlError::undefined_function(format!(
                        "function {name} requires a range or multirange argument"
                    ))
                })?;
                if range_family(&input_type).is_none() {
                    return Err(SqlError::undefined_function(format!(
                        "function {name}({input_type}) does not exist"
                    )));
                }
                Ok(Self::RangeAgg {
                    field,
                    distinct,
                    intersect: name.ends_with("range_intersect_agg"),
                    input_type,
                })
            }
            "array_agg" => {
                let (expr, distinct, order_by) = json_agg_function_parts(function)?;
                Ok(Self::ArrayAgg {
                    expr,
                    distinct,
                    order_by,
                    filter: function.filter.as_deref().cloned(),
                })
            }
            "sum" | "avg" | "min" | "max" => {
                let field = single_function_field(function, schema)?;
                let field_type = field_pg_type(&field, schema);
                if matches!(name.as_str(), "sum" | "avg") && field_type.as_deref() == Some("oid") {
                    return Err(SqlError::undefined_function(format!(
                        "function {name}(oid) does not exist"
                    )));
                }
                if name == "avg" && field_type.as_deref() == Some("money") {
                    return Err(SqlError::undefined_function(
                        "function avg(money) does not exist",
                    ));
                }
                match name.as_str() {
                    "sum" => Ok(Self::Sum(
                        field,
                        distinct,
                        field_type.as_deref() == Some("money"),
                    )),
                    "avg" => Ok(Self::Avg(field, distinct)),
                    "min" => Ok(Self::Min(field, field_type)),
                    "max" => Ok(Self::Max(field, field_type)),
                    _ => unreachable!(),
                }
            }
            other => Err(SqlError::Unsupported(format!(
                "aggregate {other} is not supported"
            ))),
        }
    }

    pub(crate) fn column_name(&self) -> String {
        match self {
            Self::CountAll => "COUNT(*)".to_string(),
            Self::CountField(field, distinct) => {
                if *distinct {
                    format!("COUNT(DISTINCT {})", field.name())
                } else {
                    format!("COUNT({})", field.name())
                }
            }
            Self::Sum(_, _, _) => "sum".to_string(),
            Self::Avg(_, _) => "avg".to_string(),
            Self::Min(_, _) => "min".to_string(),
            Self::Max(_, _) => "max".to_string(),
            Self::BoolAnd(_, _) => "bool_and".to_string(),
            Self::BoolOr(_, _) => "bool_or".to_string(),
            Self::StringAgg { .. } => "string_agg".to_string(),
            Self::ArrayAgg { .. } => "array_agg".to_string(),
            Self::RangeAgg { intersect, .. } => {
                if *intersect {
                    "range_intersect_agg".to_string()
                } else {
                    "range_agg".to_string()
                }
            }
            Self::XmlAgg { .. } => "xmlagg".to_string(),
            Self::JsonAgg {
                binary,
                strict,
                standard,
                ..
            } => {
                if *standard {
                    return "json_arrayagg".to_string();
                }
                format!(
                    "{}{}",
                    if *binary { "jsonb_agg" } else { "json_agg" },
                    if *strict { "_strict" } else { "" }
                )
            }
            Self::JsonObjectAgg {
                binary,
                strict,
                unique,
                standard,
                ..
            } => {
                if *standard {
                    return "json_objectagg".to_string();
                }
                format!(
                    "{}{}",
                    if *binary {
                        "jsonb_object_agg"
                    } else {
                        "json_object_agg"
                    },
                    match (*unique, *strict) {
                        (true, true) => "_unique_strict",
                        (true, false) => "_unique",
                        (false, true) => "_strict",
                        (false, false) => "",
                    }
                )
            }
        }
    }

    pub(crate) fn evaluate(&self, records: &[Record]) -> Result<SqlValue> {
        match self {
            Self::CountAll => Ok(SqlValue::Int(records.len() as i64)),
            Self::CountField(field, distinct) => {
                record_aggregate_count(records, field, *distinct).map(SqlValue::Int)
            }
            Self::Sum(field, distinct, money) => {
                let values = record_aggregate_values(records, field, *distinct, false)?;
                if *money {
                    sum_money_aggregate_values(values)
                } else {
                    sum_aggregate_values(values)
                }
            }
            Self::Avg(field, distinct) => {
                average_aggregate_values(record_aggregate_values(records, field, *distinct, false)?)
            }
            Self::Min(field, pg_type) => {
                extreme_record_value(records, field, pg_type.as_deref(), false)
            }
            Self::Max(field, pg_type) => {
                extreme_record_value(records, field, pg_type.as_deref(), true)
            }
            Self::BoolAnd(field, distinct) => bool_aggregate_values(
                record_aggregate_values(records, field, *distinct, false)?,
                true,
            ),
            Self::BoolOr(field, distinct) => bool_aggregate_values(
                record_aggregate_values(records, field, *distinct, false)?,
                false,
            ),
            Self::StringAgg {
                expr,
                delimiter,
                distinct,
                order_by,
                filter,
            } => record_string_agg_value(
                records,
                expr,
                delimiter,
                *distinct,
                order_by,
                filter.as_ref(),
            ),
            Self::ArrayAgg {
                expr,
                distinct,
                order_by,
                filter,
            } => record_array_agg_value(records, expr, *distinct, order_by, filter.as_ref()),
            Self::RangeAgg {
                field,
                distinct,
                intersect,
                input_type,
            } => range_aggregate_values(
                record_aggregate_values(records, field, *distinct, false)?,
                input_type,
                *intersect,
            ),
            Self::XmlAgg {
                expr,
                order_by,
                filter,
            } => record_xml_agg_value(records, expr, order_by, filter.as_ref()),
            Self::JsonAgg {
                expr,
                distinct,
                order_by,
                filter,
                binary,
                strict,
                returning,
                ..
            } => crate::jsonb::apply_json_returning(
                record_json_agg_value(
                    records,
                    expr,
                    *distinct,
                    order_by,
                    filter.as_ref(),
                    *binary,
                    *strict,
                )?,
                returning.as_ref(),
            ),
            Self::JsonObjectAgg {
                key,
                value,
                distinct,
                order_by,
                filter,
                binary,
                strict,
                unique,
                returning,
                ..
            } => crate::jsonb::apply_json_returning(
                record_json_object_agg_value(
                    records,
                    key,
                    value,
                    *distinct,
                    order_by,
                    filter.as_ref(),
                    *binary,
                    *strict,
                    *unique,
                )?,
                returning.as_ref(),
            ),
        }
    }
}

/// Declared PostgreSQL type name of a field, resolved against the table schema.
/// Returns `None` for non-column fields (JSON paths, metadata, etc.) whose type
/// can't be determined statically.
/// One FROM relation's typed columns for row-path type resolution: the relation's
/// binding alias (lowercased) plus its visible columns in wildcard order, each as
/// `(unqualified name lowercased, logical pg type)`.
#[derive(Clone, Debug)]
pub(crate) struct RelationColumns {
    pub(crate) alias: String,
    pub(crate) row_type: Option<String>,
    pub(crate) columns: Vec<(String, Option<String>)>,
}

/// Apply a relation alias's optional column-rename list to a typed column list,
/// preserving types positionally. Returns `None` when an explicit list is present
/// but its length doesn't match (mirrors `table_alias_columns`' error).
pub(crate) fn rename_relation_columns(
    columns: Vec<(String, Option<String>)>,
    alias_cols: &[TableAliasColumnDef],
) -> Option<Vec<(String, Option<String>)>> {
    if alias_cols.is_empty() {
        return Some(columns);
    }
    if alias_cols.len() != columns.len() {
        return None;
    }
    Some(
        alias_cols
            .iter()
            .zip(columns)
            .map(|(alias, (_, ty))| (ident_value(&alias.name), ty))
            .collect(),
    )
}

/// Look up an unqualified column name across all FROM relations. Returns the type
/// only when the name resolves to exactly one column (ambiguous names, which
/// PostgreSQL would reject, resolve to `None` -> value-independent fallback).
pub(crate) fn env_lookup_unqualified(env: &[RelationColumns], name: &str) -> Option<String> {
    let needle = name.to_ascii_lowercase();
    let mut found: Option<Option<String>> = None;
    for relation in env {
        for (column, ty) in &relation.columns {
            if column == &needle {
                if found.is_some() {
                    return None;
                }
                found = Some(ty.clone());
            }
        }
    }
    found.flatten()
}

/// Look up a `qualifier.column` reference in the FROM environment.
pub(crate) fn env_lookup_qualified(
    env: &[RelationColumns],
    qualifier: &str,
    column: &str,
) -> Option<String> {
    env.iter()
        .find(|relation| relation.alias == qualifier)?
        .columns
        .iter()
        .find(|(name, _)| name == column)
        .and_then(|(_, ty)| ty.clone())
}

/// `SUM` result type per PostgreSQL: small ints -> int8, int8 -> numeric, floats
/// -> float8, numeric -> numeric. `None` for non-numeric inputs.
pub(crate) fn aggregate_pg_type_for_sum(arg_type: String) -> Option<String> {
    Some(
        if is_small_integer_pg_type(&arg_type) {
            "int8"
        } else if matches!(arg_type.as_str(), "int8" | "bigint") {
            "numeric"
        } else if matches!(
            arg_type.as_str(),
            "float4" | "real" | "float8" | "double" | "double precision"
        ) {
            "float8"
        } else if matches!(arg_type.as_str(), "numeric" | "decimal" | "money") {
            if arg_type == "money" {
                "money"
            } else {
                "numeric"
            }
        } else {
            return None;
        }
        .to_string(),
    )
}

/// `AVG` result type per PostgreSQL: floats -> float8, integer/numeric -> numeric.
pub(crate) fn aggregate_pg_type_for_avg(arg_type: String) -> Option<String> {
    Some(
        if matches!(
            arg_type.as_str(),
            "float4" | "real" | "float8" | "double" | "double precision"
        ) {
            "float8"
        } else if is_small_integer_pg_type(&arg_type)
            || matches!(arg_type.as_str(), "int8" | "bigint" | "numeric" | "decimal")
        {
            "numeric"
        } else {
            return None;
        }
        .to_string(),
    )
}

/// Result type of arithmetic on two operands, by PostgreSQL operator promotion.
/// Float4 is preserved only when both operands are float4; mixed float4
/// arithmetic resolves through float8. Unknown/non-numeric operands yield the
/// other operand's type, or `None` if neither is numeric.
pub(crate) fn numeric_combine_pg_type(left: Option<&str>, right: Option<&str>) -> Option<String> {
    pub(crate) fn rank(ty: &str) -> Option<u8> {
        Some(match ty {
            "int2" | "smallint" => 1,
            "int4" | "int" | "integer" => 2,
            "int8" | "bigint" => 3,
            "numeric" | "decimal" => 4,
            "float4" | "real" => 5,
            "float8" | "double" | "double precision" => 6,
            _ => return None,
        })
    }
    pub(crate) fn canonical(rank: u8) -> &'static str {
        match rank {
            1 => "int2",
            2 => "int4",
            3 => "int8",
            4 => "numeric",
            5 => "float4",
            _ => "float8",
        }
    }
    match (left.and_then(rank), right.and_then(rank)) {
        (Some(5), Some(5)) => Some("float4".to_string()),
        (Some(left), Some(right)) if left >= 5 || right >= 5 => Some("float8".to_string()),
        (Some(left), Some(right)) => Some(canonical(left.max(right)).to_string()),
        (Some(left), None) => Some(canonical(left).to_string()),
        (None, Some(right)) => Some(canonical(right).to_string()),
        (None, None) => None,
    }
}

pub(crate) fn common_type_user_schema(
    db: &BicDb,
    pg_type: &str,
) -> Result<Option<UserTypeColumnSchema>> {
    let (scalar, array) = pg_type
        .strip_suffix("[]")
        .map(|scalar| (scalar, true))
        .unwrap_or((pg_type, false));
    let (schema_name, name) = scalar
        .rsplit_once('.')
        .map(|(schema, name)| (schema, name))
        .unwrap_or(("public", scalar));
    Ok(load_user_type(db, schema_name, name)?.map(|schema| schema.column_type(array)))
}

pub(crate) fn common_type_base_name(db: &BicDb, pg_type: &str) -> Result<String> {
    let Some(user_type) = common_type_user_schema(db, pg_type)? else {
        let (scalar, array) = pg_type
            .strip_suffix("[]")
            .map(|scalar| (scalar, true))
            .unwrap_or((pg_type, false));
        let scalar = pg_type_spec(scalar).map(|spec| spec.name).unwrap_or(scalar);
        return Ok(if array {
            format!("{scalar}[]")
        } else {
            scalar.to_string()
        });
    };
    match &user_type.kind {
        UserTypeKind::Domain {
            base_type,
            base_user_type,
            ..
        } => {
            let base = base_user_type
                .as_deref()
                .map(UserTypeColumnSchema::formatted_name)
                .unwrap_or_else(|| base_type.clone());
            let base = common_type_base_name(db, &base)?;
            Ok(if user_type.array {
                format!("{}[]", base.trim_end_matches("[]"))
            } else {
                base
            })
        }
        _ => Ok(user_type.formatted_name()),
    }
}

pub(crate) fn common_type_category(db: &BicDb, pg_type: &str) -> Result<char> {
    if pg_type.ends_with("[]") {
        return Ok('A');
    }
    if let Some(spec) = pg_type_spec(pg_type) {
        return Ok(spec.category);
    }
    let Some(user_type) = common_type_user_schema(db, pg_type)? else {
        return Ok('U');
    };
    Ok(match user_type.kind {
        UserTypeKind::Base { category, .. } => category,
        UserTypeKind::Enum { .. } => 'E',
        UserTypeKind::Range { .. } | UserTypeKind::Multirange { .. } => 'R',
        UserTypeKind::Composite { .. } => 'C',
        UserTypeKind::Domain { .. } => unreachable!("domains are flattened before categorizing"),
        UserTypeKind::Shell => 'P',
    })
}

pub(crate) fn common_type_implicit_direction(from: &str, to: &str) -> bool {
    pg_cast_allows(from, to, PgCastContext::Implicit)
}

pub(crate) fn select_common_builtin_projected_type(pg_types: &[Option<String>]) -> Option<String> {
    let known = pg_types
        .iter()
        .flatten()
        .map(|pg_type| {
            let (scalar, array) = pg_type
                .strip_suffix("[]")
                .map(|scalar| (scalar, true))
                .unwrap_or((pg_type.as_str(), false));
            let scalar = pg_type_spec(scalar).map(|spec| spec.name).unwrap_or(scalar);
            if array {
                format!("{scalar}[]")
            } else {
                scalar.to_string()
            }
        })
        .collect::<Vec<_>>();
    if known.is_empty() {
        return Some("text".to_string());
    }
    if known.iter().all(|pg_type| pg_type == &known[0]) {
        return Some(known[0].clone());
    }
    if known.iter().all(|pg_type| pg_type.ends_with("[]")) {
        let elements = known
            .iter()
            .map(|pg_type| Some(pg_type.trim_end_matches("[]").to_string()))
            .collect::<Vec<_>>();
        return select_common_builtin_projected_type(&elements)
            .map(|element| format!("{element}[]"));
    }
    let category = pg_type_spec(&known[0])?.category;
    if known
        .iter()
        .skip(1)
        .any(|pg_type| pg_type_spec(pg_type).map(|spec| spec.category) != Some(category))
    {
        return None;
    }
    let mut candidate = known[0].clone();
    for pg_type in known.iter().skip(1) {
        if common_type_implicit_direction(&candidate, pg_type)
            && !common_type_implicit_direction(pg_type, &candidate)
        {
            candidate = pg_type.clone();
        }
        if pg_type_spec(&candidate).is_some_and(|spec| spec.preferred()) {
            break;
        }
    }
    known
        .iter()
        .all(|pg_type| common_type_implicit_direction(pg_type, &candidate))
        .then_some(candidate)
}

/// PostgreSQL common-type candidate selection used by conditional expressions,
/// VALUES, set operations, arrays, parameters, and anycompatible functions.
/// `None` inputs are PostgreSQL's unknown type; an all-unknown list resolves to
/// text. Identical domains survive, while a mixed domain list is compared using
/// its base type.
pub(crate) fn select_common_pg_type(
    db: &BicDb,
    pg_types: &[Option<String>],
    construct: &str,
) -> Result<String> {
    let known = pg_types.iter().flatten().cloned().collect::<Vec<_>>();
    if known.is_empty() {
        return Ok("text".to_string());
    }
    if known.iter().all(|pg_type| pg_type == &known[0]) {
        return Ok(known[0].clone());
    }

    let flattened = known
        .iter()
        .map(|pg_type| common_type_base_name(db, pg_type))
        .collect::<Result<Vec<_>>>()?;
    if flattened.iter().all(|pg_type| pg_type == &flattened[0]) {
        return Ok(flattened[0].clone());
    }
    if flattened.iter().all(|pg_type| pg_type.ends_with("[]")) {
        let element_types = flattened
            .iter()
            .map(|pg_type| Some(pg_type.trim_end_matches("[]").to_string()))
            .collect::<Vec<_>>();
        return select_common_pg_type(db, &element_types, construct)
            .map(|element| format!("{element}[]"));
    }

    let category = common_type_category(db, &flattened[0])?;
    if flattened
        .iter()
        .skip(1)
        .any(|pg_type| common_type_category(db, pg_type).ok() != Some(category))
    {
        return Err(SqlError::data_exception_public(
            "42804",
            format!(
                "{construct} types {} and {} cannot be matched",
                flattened[0],
                flattened
                    .iter()
                    .find(|pg_type| common_type_category(db, pg_type).ok() != Some(category))
                    .unwrap_or(&flattened[1])
            ),
            None,
        ));
    }

    let mut candidate = flattened[0].clone();
    for pg_type in flattened.iter().skip(1) {
        if common_type_implicit_direction(&candidate, pg_type)
            && !common_type_implicit_direction(pg_type, &candidate)
        {
            candidate = pg_type.clone();
        }
        if pg_type_spec(&candidate).is_some_and(|spec| spec.preferred()) {
            break;
        }
    }
    if flattened
        .iter()
        .any(|pg_type| !common_type_implicit_direction(pg_type, &candidate))
    {
        return Err(SqlError::data_exception_public(
            "42804",
            format!(
                "{construct} types {} and {} cannot be matched",
                candidate,
                flattened
                    .iter()
                    .find(|pg_type| !common_type_implicit_direction(pg_type, &candidate))
                    .unwrap_or(&flattened[0])
            ),
            None,
        ));
    }
    Ok(candidate)
}

pub(crate) fn validate_common_type_expr(db: &BicDb, expr: &Expr) -> Result<()> {
    match expr {
        Expr::Nested(inner) | Expr::Collate { expr: inner, .. } => {
            validate_common_type_expr(db, inner)
        }
        Expr::Case {
            conditions,
            else_result,
            ..
        } => {
            let mut types = Vec::with_capacity(conditions.len() + 1);
            types.push(
                else_result
                    .as_deref()
                    .and_then(|result| projected_expr_pg_type_with_db(db, result)),
            );
            types.extend(
                conditions
                    .iter()
                    .map(|condition| projected_expr_pg_type_with_db(db, &condition.result)),
            );
            select_common_pg_type(db, &types, "CASE")?;
            for condition in conditions {
                validate_common_type_expr(db, &condition.result)?;
            }
            if let Some(result) = else_result {
                validate_common_type_expr(db, result)?;
            }
            Ok(())
        }
        Expr::Array(array) => {
            let types = array
                .elem
                .iter()
                .map(|element| projected_expr_pg_type_with_db(db, element))
                .collect::<Vec<_>>();
            select_common_pg_type(db, &types, "ARRAY")?;
            for element in &array.elem {
                validate_common_type_expr(db, element)?;
            }
            Ok(())
        }
        Expr::Function(function) => {
            let name = object_name(&function.name)?.to_ascii_lowercase();
            let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
            let args = function_arg_list(function);
            for arg in args.iter() {
                validate_common_type_expr(db, arg)?;
            }
            if matches!(
                bare_name,
                "coalesce" | "nullif" | "greatest" | "least" | "ifnull"
            ) {
                let types = args
                    .iter()
                    .map(|arg| projected_expr_pg_type_with_db(db, arg))
                    .collect::<Vec<_>>();
                select_common_pg_type(db, &types, &bare_name.to_ascii_uppercase())?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn money_arithmetic_pg_type(
    left: Option<&str>,
    op: &BinaryOperator,
    right: Option<&str>,
) -> Option<String> {
    let left_money = left == Some("money");
    let right_money = right == Some("money");
    match op {
        BinaryOperator::Plus | BinaryOperator::Minus if left_money && right_money => {
            Some("money".to_string())
        }
        BinaryOperator::Multiply if left_money ^ right_money => Some("money".to_string()),
        BinaryOperator::Divide if left_money && right_money => Some("float8".to_string()),
        BinaryOperator::Divide if left_money => Some("money".to_string()),
        _ => None,
    }
}

pub(crate) fn field_pg_type(field: &FieldRef, schema: Option<&TableSchema>) -> Option<String> {
    pub(crate) fn projected_column_type(column: &ColumnSchema) -> String {
        column
            .user_type
            .as_ref()
            .map(UserTypeColumnSchema::formatted_name)
            .unwrap_or_else(|| column.pg_type.clone())
    }

    match field {
        FieldRef::PrimaryKey { pg_type, .. } => Some(pg_type.clone()),
        FieldRef::TypedColumn { pg_type, .. } => Some(pg_type.clone()),
        FieldRef::Column(name) | FieldRef::JsonColumn(name) => schema?
            .column(name)
            .filter(|column| !column.hidden)
            .map(projected_column_type),
        FieldRef::Id => schema?
            .column("id")
            .filter(|column| !column.hidden)
            .map(projected_column_type),
        // `payload` / `metadata` / `timestamp` parse as event-native built-in
        // field variants, but on an explicit-schema table they are ordinary
        // declared columns — report the declared type so extended-protocol
        // Describe(statement) (e.g. sqlx) sees jsonb/timestamptz, not text.
        FieldRef::Payload => schema?
            .column("payload")
            .filter(|column| !column.hidden)
            .map(projected_column_type),
        FieldRef::Metadata => schema?
            .column("metadata")
            .filter(|column| !column.hidden)
            .map(projected_column_type),
        FieldRef::Timestamp => schema?
            .column("timestamp")
            .filter(|column| !column.hidden)
            .map(projected_column_type),
        FieldRef::JsonTextPath(_, _) => Some("text".to_string()),
        FieldRef::JsonPath(base, _) => field_pg_type(base, schema)
            .filter(|pg_type| matches!(pg_type.as_str(), "json" | "jsonb")),
        _ => None,
    }
}

/// Whether a normalized pg type name is an integer narrower than (or equal to)
/// int4 — i.e. smallint/int2 or integer/int4.
pub(crate) fn is_small_integer_pg_type(pg_type: &str) -> bool {
    matches!(pg_type, "int2" | "smallint" | "int4" | "int" | "integer")
}

/// Result type of an aggregate per PostgreSQL semantics:
/// - `COUNT(*)` / `COUNT(x)` -> bigint (int8)
/// - `MIN(x)` / `MAX(x)`     -> same type as x
/// - `SUM(int2|int4)`        -> bigint; `SUM(int8)` -> numeric; `SUM(float)` ->
///   float8; `SUM(numeric)` -> numeric
/// - `AVG(integer|numeric)`  -> numeric; `AVG(float)` -> float8
///
/// Returns `None` when the input type is unknown (the wire layer then falls back
/// to its value-width heuristic), or for aggregates whose type we don't model.
pub(crate) fn aggregate_result_pg_type(
    aggregate: &Aggregate,
    schema: Option<&TableSchema>,
) -> Option<String> {
    match aggregate {
        Aggregate::CountAll | Aggregate::CountField(_, _) => Some("int8".to_string()),
        Aggregate::Min(field, _) | Aggregate::Max(field, _) => field_pg_type(field, schema),
        Aggregate::BoolAnd(_, _) | Aggregate::BoolOr(_, _) => Some("bool".to_string()),
        Aggregate::StringAgg { .. } => Some("text".to_string()),
        Aggregate::Sum(field, _, _) => {
            let pg_type = field_pg_type(field, schema)?;
            Some(
                if is_small_integer_pg_type(&pg_type) {
                    "int8"
                } else if matches!(pg_type.as_str(), "int8" | "bigint") {
                    "numeric"
                } else if matches!(
                    pg_type.as_str(),
                    "float4" | "real" | "float8" | "double" | "double precision"
                ) {
                    "float8"
                } else if matches!(pg_type.as_str(), "numeric" | "decimal" | "money") {
                    if pg_type == "money" {
                        "money"
                    } else {
                        "numeric"
                    }
                } else {
                    return None;
                }
                .to_string(),
            )
        }
        Aggregate::Avg(field, _) => {
            let pg_type = field_pg_type(field, schema)?;
            Some(
                if matches!(
                    pg_type.as_str(),
                    "float4" | "real" | "float8" | "double" | "double precision"
                ) {
                    "float8"
                } else if is_small_integer_pg_type(&pg_type)
                    || matches!(pg_type.as_str(), "int8" | "bigint" | "numeric" | "decimal")
                {
                    "numeric"
                } else {
                    return None;
                }
                .to_string(),
            )
        }
        Aggregate::ArrayAgg { expr, .. } => projected_expr_pg_type(expr, schema).map(|pg_type| {
            if pg_type.ends_with("[]") {
                pg_type
            } else {
                format!("{pg_type}[]")
            }
        }),
        Aggregate::RangeAgg {
            intersect,
            input_type,
            ..
        } => {
            if *intersect && is_builtin_range_type(input_type) {
                Some(input_type.clone())
            } else {
                range_family(input_type).map(|(_, multirange_type)| multirange_type.to_string())
            }
        }
        Aggregate::XmlAgg { .. } => Some("xml".to_string()),
        Aggregate::JsonAgg {
            binary, returning, ..
        }
        | Aggregate::JsonObjectAgg {
            binary, returning, ..
        } => returning
            .as_ref()
            .and_then(|data_type| pg_type_from_data_type(data_type).ok().map(|(name, _)| name))
            .or_else(|| Some(if *binary { "jsonb" } else { "json" }.to_string())),
    }
}

/// Logical PostgreSQL type name of a single projected expression, resolved
/// against the (single-table) schema. Handles aggregates (per
/// [`aggregate_result_pg_type`]), explicit `::type` casts, and column references
/// (including qualified ones and primary keys). Returns `None` for dynamic/JSON
/// or otherwise unmodeled expressions, so the wire layer falls back to its
/// value-width heuristic.
thread_local! {
    /// `(generation, routine IR id)` of the routine currently executing on
    /// this thread, set by the routine entry points. While a scope is active,
    /// `projected_expr_pg_type` memoizes per expression node: routine IR
    /// nodes have stable addresses for the life of the compiled routine, and
    /// the generation (routine + schema catalogs) guards recompiles and DDL.
    static EXPR_TYPE_SCOPE: std::cell::Cell<Option<(u64, usize)>> = const { std::cell::Cell::new(None) };
    static EXPR_TYPE_MEMO: std::cell::RefCell<FxHashMap<(u64, usize, usize, u64), std::rc::Rc<Option<String>>>> =
        std::cell::RefCell::new(FxHashMap::default());
    #[cfg(test)]
    static EXPR_TYPE_UNCACHED_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

const EXPR_TYPE_MEMO_MAX: usize = 65_536;

/// Restores the previous scope on drop (nested routine calls).
pub(crate) struct ExprTypeScopeGuard(Option<(u64, usize)>);

impl Drop for ExprTypeScopeGuard {
    fn drop(&mut self) {
        EXPR_TYPE_SCOPE.with(|scope| scope.set(self.0));
    }
}

/// Enter the type-inference memo scope of a routine invocation.
pub(crate) fn enter_expr_type_scope(generation: u64, routine_ir: usize) -> ExprTypeScopeGuard {
    EXPR_TYPE_SCOPE.with(|scope| ExprTypeScopeGuard(scope.replace(Some((generation, routine_ir)))))
}

/// The active scope's generation, for other per-node memos to share.
pub(crate) fn expr_type_scope() -> Option<(u64, usize)> {
    EXPR_TYPE_SCOPE.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn expr_type_uncached_calls() -> usize {
    EXPR_TYPE_UNCACHED_CALLS.with(std::cell::Cell::get)
}

fn schema_identity(schema: Option<&TableSchema>) -> u64 {
    use std::hash::{Hash, Hasher};
    let Some(schema) = schema else {
        return 0;
    };
    let mut hasher = rustc_hash::FxHasher::default();
    schema.schema_name.hash(&mut hasher);
    schema.name.hash(&mut hasher);
    hasher.finish() | 1
}

/// The PostgreSQL type an expression projects to, given the table it is
/// evaluated against. Inside a routine the answer is memoized per IR node
/// (see `EXPR_TYPE_SCOPE`); it used to be recomputed for every evaluated
/// row from the comparison, cast and operator paths, ~8% of TPC-C CPU.
pub(crate) fn projected_expr_pg_type(expr: &Expr, schema: Option<&TableSchema>) -> Option<String> {
    let Some((generation, routine_ir)) = expr_type_scope() else {
        return projected_expr_pg_type_uncached(expr, schema);
    };
    (*memoized_expr_pg_type(
        generation,
        routine_ir,
        expr,
        schema_identity(schema),
        || projected_expr_pg_type_uncached(expr, schema),
    ))
    .clone()
}

/// `projected_expr_pg_type` sharing the memo entry: callers that only
/// compare the answer borrow it instead of cloning a `String` per hit.
pub(crate) fn projected_expr_pg_type_rc(
    expr: &Expr,
    schema: Option<&TableSchema>,
) -> std::rc::Rc<Option<String>> {
    let Some((generation, routine_ir)) = expr_type_scope() else {
        return std::rc::Rc::new(projected_expr_pg_type_uncached(expr, schema));
    };
    memoized_expr_pg_type(
        generation,
        routine_ir,
        expr,
        schema_identity(schema),
        || projected_expr_pg_type_uncached(expr, schema),
    )
}

/// The memo tag for `projected_expr_pg_type_with_db` answers: real schema
/// identities are odd and the schema-less typer uses 0, so 2 never collides.
const WITH_DB_TYPE_TAG: u64 = 2;

fn memoized_expr_pg_type(
    generation: u64,
    routine_ir: usize,
    expr: &Expr,
    tag: u64,
    compute: impl FnOnce() -> Option<String>,
) -> std::rc::Rc<Option<String>> {
    let key = (generation, routine_ir, expr as *const Expr as usize, tag);
    if let Some(hit) = EXPR_TYPE_MEMO.with(|memo| memo.borrow().get(&key).cloned()) {
        return hit;
    }
    let computed = std::rc::Rc::new(compute());
    EXPR_TYPE_MEMO.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= EXPR_TYPE_MEMO_MAX {
            memo.clear();
        }
        memo.insert(key, std::rc::Rc::clone(&computed));
    });
    computed
}

fn projected_expr_pg_type_uncached(expr: &Expr, schema: Option<&TableSchema>) -> Option<String> {
    #[cfg(test)]
    EXPR_TYPE_UNCACHED_CALLS.with(|calls| calls.set(calls.get() + 1));
    if let Expr::Nested(expr) | Expr::Collate { expr, .. } = expr {
        return projected_expr_pg_type(expr, schema);
    }
    if let Expr::Value(value) = expr {
        match &value.value {
            Value::Number(value, _) => {
                return Some(
                    if value.contains(['.', 'e', 'E']) {
                        "numeric"
                    } else if value.parse::<i32>().is_ok() {
                        "int4"
                    } else if value.parse::<i64>().is_ok() {
                        "int8"
                    } else {
                        "numeric"
                    }
                    .to_string(),
                );
            }
            Value::Boolean(_) => return Some("bool".to_string()),
            _ => {}
        }
        if matches!(
            &value.value,
            Value::SingleQuotedByteStringLiteral(_)
                | Value::DoubleQuotedByteStringLiteral(_)
                | Value::TripleSingleQuotedByteStringLiteral(_)
                | Value::TripleDoubleQuotedByteStringLiteral(_)
                | Value::HexStringLiteral(_)
        ) {
            return Some("bit".to_string());
        }
    }
    if matches!(expr, Expr::Interval(_)) {
        return Some("interval".to_string());
    }
    if matches!(expr, Expr::Tuple(_)) {
        return Some("record".to_string());
    }
    if let Expr::Array(array) = expr {
        let element_types = array
            .elem
            .iter()
            .map(|element| projected_expr_pg_type(element, schema))
            .collect::<Vec<_>>();
        return select_common_builtin_projected_type(&element_types)
            .map(|element_type| format!("{element_type}[]"));
    }
    if let Expr::CompoundFieldAccess { root, .. } = expr {
        if let Some((pg_type, _)) = projected_access_type_state(expr, schema) {
            return Some(pg_type);
        }
        return projected_expr_pg_type(root, schema).and_then(|pg_type| {
            if pg_type == "jsonb" {
                Some(pg_type)
            } else {
                pg_type.strip_suffix("[]").map(str::to_string)
            }
        });
    }
    if matches!(expr, Expr::Extract { .. }) {
        return Some("numeric".to_string());
    }
    if let Expr::Case {
        conditions,
        else_result,
        ..
    } = expr
    {
        let mut branch_types = Vec::with_capacity(conditions.len() + 1);
        branch_types.push(
            else_result
                .as_deref()
                .and_then(|result| projected_expr_pg_type(result, schema)),
        );
        branch_types.extend(
            conditions
                .iter()
                .map(|condition| projected_expr_pg_type(&condition.result, schema)),
        );
        return select_common_builtin_projected_type(&branch_types);
    }
    if let Expr::AtTimeZone { timestamp, .. } = expr {
        return match projected_expr_pg_type(timestamp, schema).as_deref() {
            Some("timestamptz") => Some("timestamp".to_string()),
            Some("timestamp") => Some("timestamptz".to_string()),
            _ => None,
        };
    }
    if let Ok((aggregate, cast)) = aggregate_from_expr(expr, schema) {
        return match cast {
            // An explicit cast (e.g. `COUNT(*)::bigint`) determines the result type.
            Some(data_type) => pg_type_from_data_type(data_type).ok().map(|(name, _)| name),
            None => aggregate_result_pg_type(&aggregate, schema),
        };
    }
    if let Expr::Cast { data_type, .. } = expr {
        return pg_type_from_data_type(data_type).ok().map(|(name, _)| name);
    }
    if let Expr::TypedString(value) = expr {
        return pg_type_from_data_type(&value.data_type)
            .ok()
            .map(|(name, _)| name);
    }
    if let Expr::Identifier(ident) = expr {
        if ident.value.eq_ignore_ascii_case("current_date") && ident.quote_style.is_none() {
            return Some("date".to_string());
        }
        // Only the unquoted reserved spellings are the session-identity
        // constructs; `"current_user"` is an ordinary column name.
        if ident.quote_style.is_none()
            && matches!(
                ident.value.to_ascii_lowercase().as_str(),
                "current_user" | "current_role" | "session_user" | "user"
            )
        {
            return Some("name".to_string());
        }
    }
    let system_column = match expr {
        Expr::Identifier(ident) => Some(ident.value.as_str()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.as_str()),
        _ => None,
    };
    if let Some(pg_type) =
        system_column.and_then(|column| match column.to_ascii_lowercase().as_str() {
            "tableoid" => Some("oid"),
            "xmin" | "xmax" => Some("xid"),
            "cmin" | "cmax" => Some("cid"),
            "ctid" => Some("tid"),
            _ => None,
        })
    {
        return Some(pg_type.to_string());
    }
    if let Expr::Function(function) = expr {
        let name = object_name(&function.name).ok()?.to_ascii_lowercase();
        let unqualified_name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
        if matches!(
            unqualified_name,
            "current_user" | "current_role" | "session_user" | "user"
        ) && function_args(function).is_empty()
        {
            return Some("name".to_string());
        }
        if matches!(
            unqualified_name,
            "now" | "current_timestamp" | "transaction_timestamp" | "clock_timestamp"
        ) && function_args(function).is_empty()
        {
            return Some("timestamptz".to_string());
        }
        if matches!(unqualified_name, "version" | "bicdb_version")
            && function_args(function).is_empty()
        {
            return Some("text".to_string());
        }
        // to_timestamp(text, text) and to_timestamp(double precision) both
        // yield timestamptz; without this entry the CALL-argument cast
        // `TO_TIMESTAMP(...)::timestamp` re-walked the catalog-aware typer.
        if unqualified_name == "to_timestamp" {
            return Some("timestamptz".to_string());
        }
        if unqualified_name == "regexp_replace" {
            return Some("text".to_string());
        }
        if unqualified_name == "pg_backend_pid" && function_args(function).is_empty() {
            return Some("int4".to_string());
        }
        if matches!(
            unqualified_name,
            "pg_try_advisory_lock"
                | "pg_try_advisory_lock_shared"
                | "pg_try_advisory_xact_lock"
                | "pg_try_advisory_xact_lock_shared"
                | "pg_advisory_unlock"
                | "pg_advisory_unlock_shared"
        ) {
            return Some("bool".to_string());
        }
        if matches!(
            unqualified_name,
            "pg_advisory_lock"
                | "pg_advisory_lock_shared"
                | "pg_advisory_xact_lock"
                | "pg_advisory_xact_lock_shared"
                | "pg_advisory_unlock_all"
        ) {
            return Some("void".to_string());
        }
        if matches!(
            unqualified_name,
            "coalesce" | "nullif" | "greatest" | "least" | "ifnull"
        ) {
            let arg_types = function_args(function)
                .iter()
                .map(|arg| projected_expr_pg_type(arg, schema))
                .collect::<Vec<_>>();
            return select_common_builtin_projected_type(&arg_types);
        }
        if unqualified_name == "abs" {
            return projected_expr_pg_type(function_args(function).first()?, schema);
        }
        if matches!(
            unqualified_name,
            "array_append" | "array_remove" | "array_replace"
        ) {
            let args = function_args(function);
            let array_type = projected_expr_pg_type(args.first()?, schema)?;
            let mut element_types = vec![array_type.strip_suffix("[]").map(str::to_string)];
            element_types.extend(
                args.iter()
                    .skip(1)
                    .map(|arg| projected_expr_pg_type(arg, schema)),
            );
            return select_common_builtin_projected_type(&element_types)
                .map(|element| format!("{element}[]"));
        }
        let geometric_arg_types = function_args(function)
            .iter()
            .map(|arg| projected_expr_pg_type(arg, schema))
            .collect::<Vec<_>>();
        if let Some(pg_type) = geometric_function_pg_type(&name, &geometric_arg_types) {
            return Some(pg_type);
        }
        if unqualified_name == "row" {
            return Some("record".to_string());
        }
        if unqualified_name == "current_date" {
            return Some("date".to_string());
        }
        if unqualified_name == "date_part" {
            return Some("float8".to_string());
        }
        if unqualified_name == "date_trunc" {
            return projected_expr_pg_type(function_args(function).get(1)?, schema);
        }
        if matches!(
            unqualified_name,
            "justify_hours" | "justify_days" | "justify_interval"
        ) {
            return Some("interval".to_string());
        }
        if matches!(
            unqualified_name,
            "row_number" | "rank" | "dense_rank" | "ntile"
        ) {
            return Some("int8".to_string());
        }
        if matches!(
            name.as_str(),
            "uuidv4"
                | "pg_catalog.uuidv4"
                | "gen_random_uuid"
                | "pg_catalog.gen_random_uuid"
                | "uuid_generate_v4"
                | "public.uuid_generate_v4"
                | "uuidv7"
                | "pg_catalog.uuidv7"
        ) {
            return Some("uuid".to_string());
        }
        if matches!(
            name.as_str(),
            "uuid_extract_version" | "pg_catalog.uuid_extract_version"
        ) {
            return Some("int2".to_string());
        }
        if matches!(
            name.as_str(),
            "uuid_extract_timestamp" | "pg_catalog.uuid_extract_timestamp"
        ) {
            return Some("timestamptz".to_string());
        }
        if matches!(
            unqualified_name,
            "lag" | "lead" | "first_value" | "last_value" | "nth_value"
        ) {
            return projected_expr_pg_type(function_args(function).first()?, schema);
        }
        if matches!(unqualified_name, "percent_rank" | "cume_dist") {
            return Some("float8".to_string());
        }
        if let Some(pg_type) = broker_function_pg_type(&name) {
            return Some(pg_type.to_string());
        }
        if let Some(pg_type) = json_function_call_pg_type(function) {
            return Some(pg_type);
        }
        if let Some(pg_type) = crate::xml_function_pg_type(&name) {
            return Some(pg_type.to_string());
        }
        if matches!(name.as_str(), "pg_typeof" | "pg_catalog.pg_typeof") {
            return Some("regtype".to_string());
        }
        if unqualified_name == "pg_has_role" {
            return Some("bool".to_string());
        }
        if matches!(
            unqualified_name,
            "has_schema_privilege"
                | "has_table_privilege"
                | "has_function_privilege"
                | "has_type_privilege"
                | "has_column_privilege"
                | "has_any_column_privilege"
                | "has_sequence_privilege"
                | "has_database_privilege"
                | "has_language_privilege"
                | "has_tablespace_privilege"
                | "has_parameter_privilege"
        ) {
            return Some("bool".to_string());
        }
        if matches!(
            unqualified_name,
            "pg_current_wal_lsn"
                | "pg_current_wal_insert_lsn"
                | "pg_current_wal_flush_lsn"
                | "pg_last_wal_receive_lsn"
                | "pg_last_wal_replay_lsn"
        ) {
            return Some("pg_lsn".to_string());
        }
        if unqualified_name == "pg_current_snapshot" {
            return Some("pg_snapshot".to_string());
        }
        if unqualified_name == "txid_current_snapshot" {
            return Some("txid_snapshot".to_string());
        }
        if unqualified_name == "txid_current" {
            return Some("int8".to_string());
        }
        if matches!(
            unqualified_name,
            "pg_snapshot_xmin" | "pg_snapshot_xmax" | "pg_snapshot_xip"
        ) {
            return Some("xid8".to_string());
        }
        if matches!(
            unqualified_name,
            "txid_snapshot_xmin" | "txid_snapshot_xmax" | "txid_snapshot_xip"
        ) {
            return Some("int8".to_string());
        }
        if matches!(
            unqualified_name,
            "pg_visible_in_snapshot" | "txid_visible_in_snapshot"
        ) {
            return Some("bool".to_string());
        }
        if unqualified_name == "pg_wal_lsn_diff" {
            return Some("numeric".to_string());
        }
        if unqualified_name == "pg_is_in_recovery" {
            return Some("bool".to_string());
        }
        if unqualified_name == "bicdb_variadic_call" {
            return Some("text".to_string());
        }
        if is_builtin_range_type(unqualified_name) {
            return Some(unqualified_name.to_string());
        }
        if is_builtin_multirange_type(unqualified_name) {
            return Some(unqualified_name.to_string());
        }
        if matches!(
            unqualified_name,
            "isempty" | "lower_inc" | "upper_inc" | "lower_inf" | "upper_inf"
        ) && function_args(function)
            .first()
            .and_then(|arg| projected_expr_pg_type(arg, schema))
            .is_some_and(|pg_type| {
                is_builtin_range_type(&pg_type) || range_type_from_multirange(&pg_type).is_some()
            })
        {
            return Some("bool".to_string());
        }
        if matches!(unqualified_name, "lower" | "upper") {
            if let Some(range_type) = function_args(function)
                .first()
                .and_then(|arg| projected_expr_pg_type(arg, schema))
            {
                if let Some(subtype) = range_subtype_name(&range_type) {
                    return Some(subtype.to_string());
                }
            }
        }
        if unqualified_name == "range_merge" {
            if let Some(arg_type) = function_args(function)
                .first()
                .and_then(|arg| projected_expr_pg_type(arg, schema))
            {
                if is_builtin_range_type(&arg_type) {
                    return Some(arg_type);
                }
                if let Some(range_type) = range_type_from_multirange(&arg_type) {
                    return Some(range_type.to_string());
                }
            }
        }
        let function_arg_types = function_args(function)
            .iter()
            .map(|arg| projected_expr_pg_type(arg, schema))
            .collect::<Vec<_>>();
        if let Some(pg_type) = network_function_pg_type(&name, &function_arg_types) {
            return Some(pg_type);
        }
        if let Some(pg_type) = fts_function_pg_type(&name, &function_arg_types) {
            return Some(pg_type.to_string());
        }
        if matches!(
            name.as_str(),
            "json_typeof" | "pg_catalog.json_typeof" | "jsonb_typeof" | "pg_catalog.jsonb_typeof"
        ) {
            return Some("text".to_string());
        }
        if matches!(
            unqualified_name,
            "decode"
                | "convert"
                | "convert_to"
                | "digest"
                | "hmac"
                | "set_byte"
                | "sha224"
                | "sha256"
                | "sha384"
                | "sha512"
        ) {
            return Some("bytea".to_string());
        }
        if unqualified_name == "set_bit" {
            return Some(
                match projected_expr_pg_type(function_args(function).first()?, schema) {
                    Some(pg_type) if matches!(pg_type.as_str(), "bit" | "varbit") => {
                        "bit".to_string()
                    }
                    _ => "bytea".to_string(),
                },
            );
        }
        if matches!(
            unqualified_name,
            "length" | "octet_length" | "bit_length" | "get_byte" | "get_bit"
        ) {
            return Some("int4".to_string());
        }
        if matches!(unqualified_name, "bit_count" | "crc32" | "crc32c") {
            return Some("int8".to_string());
        }
        if matches!(unqualified_name, "encode" | "md5" | "convert_from") {
            return Some("text".to_string());
        }
        if unqualified_name == "split_part" {
            return Some("text".to_string());
        }
        if matches!(
            unqualified_name,
            "array_append" | "array_remove" | "array_replace" | "array_cat" | "bicdb_array_assign"
        ) {
            return projected_expr_pg_type(function_args(function).first()?, schema);
        }
        if unqualified_name == "array_prepend" {
            return projected_expr_pg_type(function_args(function).get(1)?, schema);
        }
        if matches!(
            unqualified_name,
            "trim_array" | "array_sample" | "array_shuffle"
        ) {
            return projected_expr_pg_type(function_args(function).first()?, schema);
        }
        if unqualified_name == "array_fill" {
            return projected_arithmetic_operand_pg_type(function_args(function).first()?, schema)
                .map(|pg_type| {
                    if pg_type.ends_with("[]") {
                        pg_type
                    } else {
                        format!("{pg_type}[]")
                    }
                });
        }
        if unqualified_name == "string_to_array" {
            return Some("text[]".to_string());
        }
        if unqualified_name == "array_to_string" {
            return Some("text".to_string());
        }
        if matches!(
            unqualified_name,
            "array_position"
                | "cardinality"
                | "array_ndims"
                | "array_length"
                | "array_lower"
                | "array_upper"
        ) {
            return Some("int4".to_string());
        }
        if unqualified_name == "array_positions" {
            return Some("int4[]".to_string());
        }
        if unqualified_name == "array_dims" {
            return Some("text".to_string());
        }
        if matches!(
            unqualified_name,
            "substr" | "substring" | "reverse" | "trim" | "btrim" | "ltrim" | "rtrim"
        ) {
            return match projected_expr_pg_type(function_args(function).first()?, schema).as_deref()
            {
                Some("bytea") => Some("bytea".to_string()),
                Some("bit" | "varbit") if matches!(unqualified_name, "substr" | "substring") => {
                    Some("bit".to_string())
                }
                _ => None,
            };
        }
    }
    if matches!(expr, Expr::Position { .. }) {
        return Some("int4".to_string());
    }
    if let Expr::Substring { expr, .. } | Expr::Overlay { expr, .. } = expr {
        return match projected_expr_pg_type(expr, schema).as_deref() {
            Some("bit" | "varbit") => Some("bit".to_string()),
            _ => projected_expr_pg_type(expr, schema),
        };
    }
    if let Expr::BinaryOp { left, op, right } = expr {
        return projected_binary_expr_pg_type(left, op, right, schema);
    }
    if let Expr::UnaryOp { op, expr } = expr {
        if let Some(pg_type) =
            geometric_unary_result_pg_type(op, projected_expr_pg_type(expr, schema).as_deref())
        {
            return Some(pg_type);
        }
        if matches!(op, UnaryOperator::BitwiseNot) {
            match projected_expr_pg_type(expr, schema).as_deref() {
                Some("bit" | "varbit") => return Some("bit".to_string()),
                Some("inet" | "cidr") => return Some("inet".to_string()),
                Some(pg_type @ ("macaddr" | "macaddr8")) => return Some(pg_type.to_string()),
                _ => {}
            }
        }
        if matches!(op, UnaryOperator::PGPrefixFactorial)
            && projected_expr_pg_type(expr, schema).as_deref() == Some("tsquery")
        {
            return Some("tsquery".to_string());
        }
        if matches!(op.to_string().as_str(), "+" | "-") {
            return projected_expr_pg_type(expr, schema);
        }
        if op.to_string().eq_ignore_ascii_case("NOT") {
            return Some("bool".to_string());
        }
    }
    // This helper is called only with a single target schema. The qualifier may
    // therefore be that table's SQL alias rather than its persisted relation
    // name (notably in UPDATE/DELETE ... RETURNING alias.column).
    if let (Some(schema), Expr::CompoundIdentifier(idents)) = (schema, expr) {
        if let Some(column) = idents
            .last()
            .and_then(|ident| schema.column(&ident.value))
            .filter(|column| !column.hidden)
        {
            return Some(
                column
                    .user_type
                    .as_ref()
                    .map(UserTypeColumnSchema::formatted_name)
                    .unwrap_or_else(|| column.pg_type.clone()),
            );
        }
    }
    let field = schema_projected_field(FieldRef::from_expr_opt(expr)?, schema);
    field_pg_type(&field, schema)
}

pub(crate) fn projected_access_type_state(
    expr: &Expr,
    schema: Option<&TableSchema>,
) -> Option<(String, Option<UserTypeColumnSchema>)> {
    match expr {
        Expr::Nested(expr) => projected_access_type_state(expr, schema),
        Expr::Identifier(ident) => {
            let column = schema?.column(&ident.value)?;
            Some((column.pg_type.clone(), column.user_type.clone()))
        }
        Expr::CompoundFieldAccess { root, access_chain } => {
            let (mut pg_type, mut user_type) = projected_access_type_state(root, schema)?;
            let array_slice_mode = access_chain
                .iter()
                .any(|access| matches!(access, AccessExpr::Subscript(Subscript::Slice { .. })));
            for access in access_chain {
                match access {
                    AccessExpr::Subscript(Subscript::Slice { .. }) => {}
                    AccessExpr::Subscript(Subscript::Index { .. }) if array_slice_mode => {}
                    AccessExpr::Subscript(Subscript::Index { .. }) => {
                        if let Some(current) = &mut user_type {
                            if !current.array {
                                return None;
                            }
                            current.array = false;
                            pg_type = current.formatted_name();
                        } else {
                            pg_type = pg_type.strip_suffix("[]")?.to_string();
                        }
                    }
                    AccessExpr::Dot(Expr::Identifier(field)) => {
                        let current = user_type.as_ref()?;
                        let UserTypeKind::Composite { attributes, .. } = &current.kind else {
                            return None;
                        };
                        let attribute = attributes.iter().find(|attribute| {
                            !attribute.dropped && attribute.name.eq_ignore_ascii_case(&field.value)
                        })?;
                        pg_type = attribute.pg_type.clone();
                        user_type = attribute.user_type.clone();
                    }
                    AccessExpr::Dot(_) => return None,
                }
            }
            Some((pg_type, user_type))
        }
        _ => None,
    }
}

/// The catalog-aware typer (user types, CASE/ARRAY common types), memoized
/// per IR node inside a routine like `projected_expr_pg_type`; the scope
/// generation covers the user-type catalog, so the answer is a pure function
/// of the key.
pub(crate) fn projected_expr_pg_type_with_db(db: &BicDb, expr: &Expr) -> Option<String> {
    let Some((generation, routine_ir)) = expr_type_scope() else {
        return projected_expr_pg_type_with_db_uncached(db, expr);
    };
    (*memoized_expr_pg_type(generation, routine_ir, expr, WITH_DB_TYPE_TAG, || {
        projected_expr_pg_type_with_db_uncached(db, expr)
    }))
    .clone()
}

#[cfg(test)]
thread_local! {
    /// Catalog-aware typings computed from scratch (memo misses or no
    /// scope); tests assert repeated CALL arguments stop adding to it.
    pub(crate) static SQL_EXPR_TYPE_UNCACHED_CALLS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

fn projected_expr_pg_type_with_db_uncached(db: &BicDb, expr: &Expr) -> Option<String> {
    #[cfg(test)]
    SQL_EXPR_TYPE_UNCACHED_CALLS.with(|calls| calls.set(calls.get() + 1));
    if let Expr::Nested(inner) | Expr::Collate { expr: inner, .. } = expr {
        return projected_expr_pg_type_with_db(db, inner);
    }
    if let Expr::Cast { data_type, .. } | Expr::TypedString(TypedString { data_type, .. }) = expr {
        if let Ok(Some(user_type)) = user_type_column_from_data_type(db, data_type) {
            return Some(user_type.formatted_name());
        }
    }
    if let Expr::Case {
        conditions,
        else_result,
        ..
    } = expr
    {
        // PostgreSQL considers ELSE first for historical compatibility, then
        // each WHEN result from left to right.
        let mut types = Vec::with_capacity(conditions.len() + 1);
        types.push(
            else_result
                .as_deref()
                .and_then(|result| projected_expr_pg_type_with_db(db, result)),
        );
        types.extend(
            conditions
                .iter()
                .map(|condition| projected_expr_pg_type_with_db(db, &condition.result)),
        );
        return select_common_pg_type(db, &types, "CASE").ok();
    }
    if let Expr::Array(array) = expr {
        let types = array
            .elem
            .iter()
            .map(|element| projected_expr_pg_type_with_db(db, element))
            .collect::<Vec<_>>();
        return select_common_pg_type(db, &types, "ARRAY")
            .ok()
            .map(|element| format!("{element}[]"));
    }
    if let Expr::Function(function) = expr {
        let name = object_name(&function.name).ok()?.to_ascii_lowercase();
        let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
        // Borrow the positional arguments; `function_args` deep-clones them.
        let owned_args;
        let args: Vec<&Expr> = match unnamed_function_arg_exprs(function) {
            Some(exprs) => exprs.collect(),
            None => {
                owned_args = function_args(function);
                owned_args.iter().collect()
            }
        };
        if matches!(
            bare_name,
            "current_user" | "current_role" | "session_user" | "user"
        ) && args.is_empty()
        {
            return Some("name".to_string());
        }
        let arg_types = args
            .iter()
            .map(|arg| projected_expr_pg_type_with_db(db, arg))
            .collect::<Vec<_>>();
        if matches!(
            bare_name,
            "coalesce" | "nullif" | "greatest" | "least" | "ifnull"
        ) {
            return select_common_pg_type(db, &arg_types, &bare_name.to_ascii_uppercase()).ok();
        }
        if matches!(bare_name, "array_append" | "array_remove" | "array_replace") {
            let array_type = arg_types.first().cloned().flatten()?;
            let mut element_types = vec![array_type.strip_suffix("[]").map(str::to_string)];
            element_types.extend(arg_types.iter().skip(1).cloned());
            return select_common_pg_type(db, &element_types, bare_name)
                .ok()
                .map(|element| format!("{element}[]"));
        }
        if bare_name == "array_prepend" {
            let array_type = arg_types.get(1).cloned().flatten()?;
            let element_types = vec![
                arg_types.first().cloned().flatten(),
                array_type.strip_suffix("[]").map(str::to_string),
            ];
            return select_common_pg_type(db, &element_types, bare_name)
                .ok()
                .map(|element| format!("{element}[]"));
        }
        if bare_name == "array_cat" {
            let element_types = arg_types
                .iter()
                .map(|pg_type| {
                    pg_type
                        .as_deref()
                        .and_then(|pg_type| pg_type.strip_suffix("[]"))
                        .map(str::to_string)
                })
                .collect::<Vec<_>>();
            return select_common_pg_type(db, &element_types, bare_name)
                .ok()
                .map(|element| format!("{element}[]"));
        }
    }
    if let Some(pg_type) = projected_expr_pg_type(expr, None) {
        return Some(pg_type);
    }
    None
}

pub(crate) fn projected_binary_expr_pg_type(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    schema: Option<&TableSchema>,
) -> Option<String> {
    let left_type = projected_expr_pg_type(left, schema);
    let right_type = projected_expr_pg_type(right, schema);
    if let Some(pg_type) =
        geometric_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
    {
        return Some(pg_type);
    }
    if vector_distance_operator(op).is_some() {
        return Some("float8".to_string());
    }
    if let Some(pg_type) =
        network_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
    {
        return Some(pg_type);
    }
    // Reuse the operand types computed above: re-deriving them (and again in
    // the arms below) re-walked each subtree several times per nesting level,
    // which is exponential on left-nested chains like the 13-term
    // setweight(..) || setweight(..) || … expressions BicDB application migrations emit.
    let arithmetic_left_type = left_type
        .clone()
        .or_else(|| arithmetic_operand_fallback_pg_type(left, schema));
    let arithmetic_right_type = right_type
        .clone()
        .or_else(|| arithmetic_operand_fallback_pg_type(right, schema));
    if let Some(pg_type) = pg_lsn_binary_result_pg_type(
        op,
        arithmetic_left_type.as_deref(),
        arithmetic_right_type.as_deref(),
    ) {
        return Some(pg_type.to_string());
    }
    if let Some(pg_type) =
        range_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
    {
        return Some(pg_type);
    }
    match op {
        BinaryOperator::Arrow | BinaryOperator::HashArrow | BinaryOperator::HashMinus => {
            left_type.filter(|pg_type| matches!(pg_type.as_str(), "json" | "jsonb"))
        }
        BinaryOperator::LongArrow | BinaryOperator::HashLongArrow => Some("text".to_string()),
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::AtArrow
        | BinaryOperator::ArrowAt
        | BinaryOperator::AtAt
        | BinaryOperator::Question
        | BinaryOperator::QuestionAnd
        | BinaryOperator::QuestionPipe => Some("bool".to_string()),
        BinaryOperator::PGOverlap => {
            if left_type.as_deref() == Some("tsquery") && right_type.as_deref() == Some("tsquery") {
                Some("tsquery".to_string())
            } else {
                Some("bool".to_string())
            }
        }
        BinaryOperator::StringConcat => match (left_type, right_type) {
            (Some(left), _) if left.ends_with("[]") => Some(left),
            (_, Some(right)) if right.ends_with("[]") => Some(right),
            (Some(left), _)
                if matches!(
                    left.as_str(),
                    "json" | "jsonb" | "bytea" | "tsvector" | "tsquery"
                ) =>
            {
                Some(left)
            }
            (Some(left), _) if matches!(left.as_str(), "bit" | "varbit") => {
                Some("varbit".to_string())
            }
            _ => Some("text".to_string()),
        },
        BinaryOperator::Minus if matches!(left_type.as_deref(), Some("json" | "jsonb")) => {
            left_type
        }
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo => {
            let left_type = arithmetic_left_type;
            let right_type = arithmetic_right_type;
            let left_interval = left_type.as_deref() == Some("interval");
            let right_interval = right_type.as_deref() == Some("interval");
            let left_scalar = left_type.as_deref().is_some_and(is_interval_scalar_type);
            let right_scalar = right_type.as_deref().is_some_and(is_interval_scalar_type);
            if ((left_interval && right_interval)
                && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus))
                || ((left_interval && right_scalar) || (left_scalar && right_interval))
                    && matches!(op, BinaryOperator::Multiply)
                || (left_interval && right_scalar && matches!(op, BinaryOperator::Divide))
            {
                return Some("interval".to_string());
            }
            if matches!(op, BinaryOperator::Minus)
                && left_type.as_deref() == Some("date")
                && right_type.as_deref() == Some("date")
            {
                return Some("int4".to_string());
            }
            if (left_type.as_deref() == Some("date")
                && right_type.as_deref().is_some_and(is_date_integer_type)
                && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus))
                || (left_type.as_deref().is_some_and(is_date_integer_type)
                    && right_type.as_deref() == Some("date")
                    && matches!(op, BinaryOperator::Plus))
            {
                return Some("date".to_string());
            }
            if matches!(op, BinaryOperator::Minus)
                && left_type.as_deref() == Some("time")
                && right_type.as_deref() == Some("time")
            {
                return Some("interval".to_string());
            }
            if ((left_type.as_deref() == Some("time") && right_type.as_deref() == Some("interval"))
                || (left_type.as_deref() == Some("interval")
                    && right_type.as_deref() == Some("time")))
                && matches!(op, BinaryOperator::Plus)
                || (left_type.as_deref() == Some("time")
                    && right_type.as_deref() == Some("interval")
                    && matches!(op, BinaryOperator::Minus))
            {
                return Some("time".to_string());
            }
            if (((left_type.as_deref() == Some("timetz")
                && right_type.as_deref() == Some("interval"))
                || (left_type.as_deref() == Some("interval")
                    && right_type.as_deref() == Some("timetz")))
                && matches!(op, BinaryOperator::Plus))
                || (left_type.as_deref() == Some("timetz")
                    && right_type.as_deref() == Some("interval")
                    && matches!(op, BinaryOperator::Minus))
            {
                return Some("timetz".to_string());
            }
            let timestamp_interval_plus = matches!(op, BinaryOperator::Plus)
                && ((left_type.as_deref() == Some("timestamp")
                    && right_type.as_deref() == Some("interval"))
                    || (left_type.as_deref() == Some("interval")
                        && right_type.as_deref() == Some("timestamp")));
            let timestamp_interval_minus = matches!(op, BinaryOperator::Minus)
                && left_type.as_deref() == Some("timestamp")
                && right_type.as_deref() == Some("interval");
            if timestamp_interval_plus || timestamp_interval_minus {
                return Some("timestamp".to_string());
            }
            if left_type.as_deref() == Some("timestamp")
                && right_type.as_deref() == Some("timestamp")
                && matches!(op, BinaryOperator::Minus)
            {
                return Some("interval".to_string());
            }
            let timestamptz_interval_plus = matches!(op, BinaryOperator::Plus)
                && ((left_type.as_deref() == Some("timestamptz")
                    && right_type.as_deref() == Some("interval"))
                    || (left_type.as_deref() == Some("interval")
                        && right_type.as_deref() == Some("timestamptz")));
            let timestamptz_interval_minus = matches!(op, BinaryOperator::Minus)
                && left_type.as_deref() == Some("timestamptz")
                && right_type.as_deref() == Some("interval");
            if timestamptz_interval_plus || timestamptz_interval_minus {
                return Some("timestamptz".to_string());
            }
            if left_type.as_deref() == Some("timestamptz")
                && right_type.as_deref() == Some("timestamptz")
                && matches!(op, BinaryOperator::Minus)
            {
                return Some("interval".to_string());
            }
            if matches!(left_type.as_deref(), Some("money"))
                || matches!(right_type.as_deref(), Some("money"))
            {
                return money_arithmetic_pg_type(left_type.as_deref(), op, right_type.as_deref());
            }
            numeric_combine_pg_type(left_type.as_deref(), right_type.as_deref())
        }
        BinaryOperator::BitwiseAnd
        | BinaryOperator::BitwiseOr
        | BinaryOperator::PGBitwiseXor
        | BinaryOperator::PGBitwiseShiftLeft
        | BinaryOperator::PGBitwiseShiftRight => projected_expr_pg_type(left, schema)
            .filter(|pg_type| matches!(pg_type.as_str(), "bit" | "varbit"))
            .map(|_| "bit".to_string()),
        _ => None,
    }
}

pub(crate) fn projected_arithmetic_operand_pg_type(
    expr: &Expr,
    schema: Option<&TableSchema>,
) -> Option<String> {
    projected_expr_pg_type(expr, schema)
        .or_else(|| arithmetic_operand_fallback_pg_type(expr, schema))
}

// The untyped-literal fallback alone, for callers that already hold the
// operand's `projected_expr_pg_type` result and must not re-derive it.
fn arithmetic_operand_fallback_pg_type(
    expr: &Expr,
    schema: Option<&TableSchema>,
) -> Option<String> {
    if let Expr::Nested(expr) | Expr::Collate { expr, .. } = expr {
        return projected_arithmetic_operand_pg_type(expr, schema);
    }
    if let Expr::UnaryOp { op, expr } = expr {
        if matches!(op.to_string().as_str(), "+" | "-") {
            return projected_arithmetic_operand_pg_type(expr, schema);
        }
    }
    let Expr::Value(value) = expr else {
        return None;
    };
    let Value::Number(value, _) = &value.value else {
        return None;
    };
    Some(
        if value.contains(['.', 'e', 'E']) {
            "numeric"
        } else if value.parse::<i32>().is_ok() {
            "int4"
        } else if value.parse::<i64>().is_ok() {
            "int8"
        } else {
            "numeric"
        }
        .to_string(),
    )
}

/// Per-column type names for a SELECT projection (one entry per item), resolved
/// against an optional single-table schema. Plain expressions that can't be
/// typed statically resolve to `None`. Returns `None` for the whole projection
/// when it contains a wildcard, whose expanded column count can't be aligned
/// here (the wire layer then falls back per column).
pub(crate) fn explicit_projection_column_types(
    items: &[SelectItem],
    schema: Option<&TableSchema>,
) -> Option<Vec<Option<String>>> {
    items
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                Some(projected_expr_pg_type(expr, schema))
            }
            _ => None,
        })
        .collect()
}

/// Attach schema/expression-derived column type names to a row-query result,
/// when the projection is fully explicit (no wildcard) and aligns 1:1 with the
/// result columns. Never overrides types a more specific path already set.
pub(crate) fn with_projection_column_types(
    result: SqlResult,
    projection: &[SelectItem],
    schema: Option<&TableSchema>,
) -> SqlResult {
    if !result.column_types.is_empty() {
        return result;
    }
    match explicit_projection_column_types(projection, schema) {
        Some(types) if types.len() == result.columns.len() => result.with_column_types(types),
        _ => result,
    }
}

/// Per-column type names for an aggregate SELECT projection (one entry per item).
pub(crate) fn aggregate_projection_column_types(
    items: &[SelectItem],
    schema: Option<&TableSchema>,
) -> Vec<Option<String>> {
    items
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                projected_expr_pg_type(expr, schema)
            }
            _ => None,
        })
        .collect()
}

pub(crate) fn record_array_agg_value(
    records: &[Record],
    expr: &Expr,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
) -> Result<SqlValue> {
    let mut entries = Vec::with_capacity(records.len());
    for record in records {
        if let Some(filter) = filter {
            if !matches!(eval_predicate_truth(record, filter)?, Some(true)) {
                continue;
            }
        }
        let value = eval_value(record, expr)?;
        let keys = order_by
            .iter()
            .map(|order| eval_value(record, &order.expr))
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, keys));
    }
    finish_array_agg(entries, distinct, order_by)
}

pub(crate) fn record_string_agg_value(
    records: &[Record],
    expr: &Expr,
    delimiter: &Expr,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
) -> Result<SqlValue> {
    let mut entries = Vec::with_capacity(records.len());
    for record in records {
        if let Some(filter) = filter {
            if !matches!(eval_predicate_truth(record, filter)?, Some(true)) {
                continue;
            }
        }
        let value = eval_value(record, expr)?;
        let delimiter = eval_value(record, delimiter)?;
        let keys = order_by
            .iter()
            .map(|order| eval_value(record, &order.expr))
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, delimiter, keys));
    }
    finish_string_agg(entries, distinct, order_by)
}

pub(crate) fn record_json_agg_value(
    records: &[Record],
    expr: &Expr,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
    binary: bool,
    strict: bool,
) -> Result<SqlValue> {
    let mut entries = Vec::with_capacity(records.len());
    for record in records {
        if let Some(filter) = filter {
            if !matches!(eval_predicate_truth(record, filter)?, Some(true)) {
                continue;
            }
        }
        let value = eval_value(record, expr)?;
        let keys = order_by
            .iter()
            .map(|order| eval_value(record, &order.expr))
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, keys));
    }
    finish_json_agg(entries, distinct, order_by, binary, strict)
}

pub(crate) fn record_xml_agg_value(
    records: &[Record],
    expr: &Expr,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
) -> Result<SqlValue> {
    let mut entries = Vec::with_capacity(records.len());
    for record in records {
        if let Some(filter) = filter {
            if !matches!(eval_predicate_truth(record, filter)?, Some(true)) {
                continue;
            }
        }
        let value = eval_value(record, expr)?;
        let keys = order_by
            .iter()
            .map(|order| eval_value(record, &order.expr))
            .collect::<Result<Vec<_>>>()?;
        entries.push((value, keys));
    }
    finish_xml_agg(entries, order_by)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_json_object_agg_value(
    records: &[Record],
    key_expr: &Expr,
    value_expr: &Expr,
    distinct: bool,
    order_by: &[sqlparser::ast::OrderByExpr],
    filter: Option<&Expr>,
    binary: bool,
    strict: bool,
    unique: bool,
) -> Result<SqlValue> {
    let mut entries = Vec::with_capacity(records.len());
    for record in records {
        if let Some(filter) = filter {
            if !matches!(eval_predicate_truth(record, filter)?, Some(true)) {
                continue;
            }
        }
        let key = eval_value(record, key_expr)?;
        let value = eval_value(record, value_expr)?;
        let keys = order_by
            .iter()
            .map(|order| eval_value(record, &order.expr))
            .collect::<Result<Vec<_>>>()?;
        entries.push((key, value, keys));
    }
    finish_json_object_agg(entries, distinct, order_by, binary, strict, unique)
}

pub(crate) fn aggregate_column_name(expr: &Expr, schema: Option<&TableSchema>) -> String {
    aggregate_from_expr(expr, schema)
        .map(|(aggregate, _)| aggregate.column_name())
        .unwrap_or_else(|_| row_expr_column_name(expr))
}

pub(crate) fn sum_aggregate_values(values: impl IntoIterator<Item = SqlValue>) -> Result<SqlValue> {
    let mut int_sum = BigInt::zero();
    let mut numeric_sum = None::<SqlValue>;
    let mut float_sum = 0.0_f64;
    let mut saw_value = false;
    let mut saw_float = false;
    for value in values {
        match value {
            SqlValue::Int(value) if !saw_float && numeric_sum.is_none() => {
                saw_value = true;
                int_sum += value;
            }
            value @ (SqlValue::Int(_) | SqlValue::String(_)) if !saw_float => {
                saw_value = true;
                let sum = numeric_sum
                    .take()
                    .unwrap_or_else(|| SqlValue::String(int_sum.to_string()));
                numeric_sum = Some(eval_pg_numeric_arithmetic(
                    sum,
                    &BinaryOperator::Plus,
                    value,
                )?);
            }
            value => {
                let Some(numeric) = sql_value_f64(&value) else {
                    return Err(SqlError::InvalidSql(format!(
                        "SUM argument must be numeric, got {}",
                        value.to_cell()
                    )));
                };
                if !saw_float {
                    float_sum = numeric_sum
                        .as_ref()
                        .and_then(sql_value_f64)
                        .unwrap_or_else(|| int_sum.to_f64().unwrap_or(f64::INFINITY));
                    saw_float = true;
                }
                saw_value = true;
                float_sum += numeric;
            }
        }
    }
    if !saw_value {
        Ok(SqlValue::Null)
    } else if saw_float {
        Ok(SqlValue::Float(float_sum))
    } else if let Some(value) = numeric_sum {
        Ok(value)
    } else if let Some(value) = int_sum.to_i64() {
        Ok(SqlValue::Int(value))
    } else {
        Ok(SqlValue::String(int_sum.to_string()))
    }
}

pub(crate) fn sum_money_aggregate_values(
    values: impl IntoIterator<Item = SqlValue>,
) -> Result<SqlValue> {
    let mut sum = 0_i64;
    let mut saw_value = false;
    for value in values {
        if matches!(value, SqlValue::Null) {
            continue;
        }
        let cents = pg_money_cents_from_text(&value.to_cell()).map_err(|error| match error {
            PgCanonicalValueError::NumericOverflow => SqlError::money_out_of_range(),
            _ => SqlError::InvalidTextRepresentation(format!(
                "invalid input syntax for type money: \"{}\"",
                value.to_cell()
            )),
        })?;
        sum = sum
            .checked_add(cents)
            .ok_or_else(SqlError::money_out_of_range)?;
        saw_value = true;
    }
    if saw_value {
        Ok(SqlValue::String(pg_money_text_from_cents(sum)))
    } else {
        Ok(SqlValue::Null)
    }
}

pub(crate) fn bool_aggregate_values(
    values: impl IntoIterator<Item = SqlValue>,
    every: bool,
) -> Result<SqlValue> {
    let mut result = every;
    let mut saw_value = false;
    for value in values {
        if matches!(value, SqlValue::Null) {
            continue;
        }
        let SqlValue::Bool(value) = value else {
            return Err(SqlError::undefined_function(
                "boolean aggregate requires boolean input",
            ));
        };
        saw_value = true;
        if every {
            result &= value;
        } else {
            result |= value;
        }
    }
    if saw_value {
        Ok(SqlValue::Bool(result))
    } else {
        Ok(SqlValue::Null)
    }
}

pub(crate) fn range_aggregate_values(
    values: impl IntoIterator<Item = SqlValue>,
    input_type: &str,
    intersect: bool,
) -> Result<SqlValue> {
    let (range_type, multirange_type) = range_family(input_type).ok_or_else(|| {
        SqlError::undefined_function(format!("range aggregate does not accept type {input_type}"))
    })?;
    let range_input = is_builtin_range_type(input_type);
    let mut state: Option<Vec<PgRange>> = None;
    for value in values {
        if matches!(value, SqlValue::Null) {
            continue;
        }
        let incoming = range_set_from_sql_value(&value, input_type)?.unwrap();
        state = Some(match state {
            None => incoming,
            Some(current) if intersect && range_input => {
                vec![intersect_range_values(&current[0], &incoming[0])?]
            }
            Some(current) if intersect => intersect_range_sets(&current, &incoming)?,
            Some(current) => {
                canonicalize_pg_multirange(current.into_iter().chain(incoming).collect::<Vec<_>>())
                    .map_err(|error| {
                        postgres_range_input_error(multirange_type, "aggregate result", error)
                    })?
            }
        });
    }
    let Some(mut ranges) = state else {
        return Ok(SqlValue::Null);
    };
    if intersect && range_input {
        return Ok(SqlValue::String(render_range_for_session(
            &ranges[0], range_type,
        )));
    }
    if !intersect {
        ranges = canonicalize_pg_multirange(ranges).map_err(|error| {
            postgres_range_input_error(multirange_type, "aggregate result", error)
        })?;
    }
    Ok(render_range_set(&ranges, multirange_type))
}

/// Incremental AVG accumulator.
///
/// Factored out of [`average_aggregate_values`] so the streaming aggregate
/// path can fold one bounded batch at a time without materializing the input,
/// while the batch and streaming routes share one implementation and therefore
/// cannot drift apart.
#[derive(Debug, Clone)]
pub(crate) struct AverageState {
    numeric_sum: SqlValue,
    float_sum: f64,
    count: u64,
    saw_float: bool,
}

impl AverageState {
    pub(crate) fn new() -> Self {
        Self {
            numeric_sum: SqlValue::String("0".to_string()),
            float_sum: 0.0_f64,
            count: 0_u64,
            saw_float: false,
        }
    }

    pub(crate) fn fold_value(&mut self, value: SqlValue) -> Result<()> {
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| SqlError::numeric_value_out_of_range("numeric out of range"))?;
        match value {
            value @ (SqlValue::Int(_) | SqlValue::String(_)) if !self.saw_float => {
                let sum = std::mem::replace(&mut self.numeric_sum, SqlValue::Null);
                self.numeric_sum = eval_pg_numeric_arithmetic(sum, &BinaryOperator::Plus, value)?;
            }
            value => {
                let Some(numeric) = sql_value_f64(&value) else {
                    return Err(SqlError::InvalidSql(format!(
                        "AVG argument must be numeric, got {}",
                        value.to_cell()
                    )));
                };
                if !self.saw_float {
                    self.float_sum = sql_value_f64(&self.numeric_sum).unwrap_or(f64::INFINITY);
                    self.saw_float = true;
                }
                self.float_sum += numeric;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<SqlValue> {
        if self.count == 0 {
            Ok(SqlValue::Null)
        } else if self.saw_float {
            Ok(SqlValue::Float(self.float_sum / self.count as f64))
        } else {
            eval_pg_numeric_arithmetic(
                self.numeric_sum,
                &BinaryOperator::Divide,
                SqlValue::Int(self.count as i64),
            )
        }
    }
}

pub(crate) fn average_aggregate_values(
    values: impl IntoIterator<Item = SqlValue>,
) -> Result<SqlValue> {
    let mut state = AverageState::new();
    for value in values {
        state.fold_value(value)?;
    }
    state.finish()
}

pub(crate) fn is_count_star(function: &Function) -> bool {
    let FunctionArguments::List(args) = &function.args else {
        return false;
    };
    matches!(
        args.args.as_slice(),
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
    )
}

pub(crate) fn record_aggregate_values(
    records: &[Record],
    field: &FieldRef,
    distinct: bool,
    include_null: bool,
) -> Result<Vec<SqlValue>> {
    let mut values = Vec::new();
    let mut seen = BTreeSet::new();
    for record in records {
        let value = field.value(record)?;
        push_aggregate_value(&mut values, &mut seen, value, distinct, include_null);
    }
    Ok(values)
}

pub(crate) fn record_aggregate_count(
    records: &[Record],
    field: &FieldRef,
    distinct: bool,
) -> Result<i64> {
    let mut count = 0_i64;
    let mut seen = BTreeSet::new();
    for record in records {
        let value = field.value(record)?;
        if matches!(value, SqlValue::Null) {
            continue;
        }
        if distinct && !seen.insert(sql_value_distinct_key(&value)) {
            continue;
        }
        count += 1;
    }
    Ok(count)
}

pub(crate) fn extreme_record_value(
    records: &[Record],
    field: &FieldRef,
    pg_type: Option<&str>,
    greatest: bool,
) -> Result<SqlValue> {
    let mut best = SqlValue::Null;
    for record in records {
        let value = field.value(record)?;
        best = combine_extreme_value(best, value, pg_type, greatest)?;
    }
    Ok(best)
}

/// Fold one candidate into a running MIN/MAX, with exactly
/// [`extreme_record_value`]'s comparison rules -- shared so an incremental
/// (streaming) fold cannot diverge from the whole-slice aggregate.
pub(crate) fn combine_extreme_value(
    best: SqlValue,
    value: SqlValue,
    pg_type: Option<&str>,
    greatest: bool,
) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(best);
    }
    if matches!(best, SqlValue::Null) {
        return Ok(value);
    }
    let ordering = if matches!(pg_type, Some("inet" | "cidr")) {
        Some(pg_typed_compare("inet", &value, &best)?)
    } else if matches!(pg_type, Some("macaddr" | "macaddr8")) {
        Some(pg_typed_compare(pg_type.unwrap(), &value, &best)?)
    } else {
        value_ordering(&value, &best)
    };
    if ordering.is_some_and(|ordering| {
        if greatest {
            ordering == Ordering::Greater
        } else {
            ordering == Ordering::Less
        }
    }) {
        return Ok(value);
    }
    Ok(best)
}

pub(crate) fn single_function_field(
    function: &Function,
    schema: Option<&TableSchema>,
) -> Result<FieldRef> {
    let args = function_args(function);
    let [arg] = args.as_slice() else {
        return Err(SqlError::Unsupported(format!(
            "{} expects exactly one field argument",
            function.name
        )));
    };
    Ok(schema_projected_field(FieldRef::from_expr(arg)?, schema))
}

/// Argument expressions of a call, borrowed from the AST when the call has the
/// ordinary shape and owned only for the SQL-standard `json_object(k: v)`
/// spelling that rewrites named arguments into positional pairs. Routine
/// bodies evaluate the same function nodes on every row, and deep-cloning
/// every argument expression per evaluation was ~2% of TPC-C CPU.
pub(crate) enum FunctionArgList<'a> {
    Borrowed(Vec<&'a Expr>),
    Owned(Vec<Expr>),
}

impl<'a> FunctionArgList<'a> {
    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = &Expr> + '_> {
        match self {
            Self::Borrowed(args) => Box::new(args.iter().copied()),
            Self::Owned(args) => Box::new(args.iter()),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Borrowed(args) => args.len(),
            Self::Owned(args) => args.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn get(&self, index: usize) -> Option<&Expr> {
        match self {
            Self::Borrowed(args) => args.get(index).copied(),
            Self::Owned(args) => args.get(index),
        }
    }

    pub(crate) fn first(&self) -> Option<&Expr> {
        self.get(0)
    }
}

pub(crate) fn standard_json_object_call(
    function: &Function,
    args: &sqlparser::ast::FunctionArgumentList,
) -> bool {
    args.args.iter().any(|arg| {
        matches!(
            arg,
            FunctionArg::ExprNamed { .. } | FunctionArg::Named { .. }
        )
    }) && object_name(&function.name).ok().is_some_and(|name| {
        matches!(
            name.strip_prefix("pg_catalog.").unwrap_or(&name),
            "json_object" | "json_object_unique" | "json_objectagg" | "json_objectagg_unique"
        )
    })
}

/// `function_args` without the per-argument clone: the borrowed list for the
/// ordinary call shape, the owned rewrite only for standard `json_object`.
pub(crate) fn function_arg_list(function: &Function) -> FunctionArgList<'_> {
    let FunctionArguments::List(args) = &function.args else {
        return FunctionArgList::Borrowed(Vec::new());
    };
    if standard_json_object_call(function, args) {
        return FunctionArgList::Owned(function_args(function));
    }
    FunctionArgList::Borrowed(
        args.args
            .iter()
            .filter_map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
                _ => None,
            })
            .collect(),
    )
}

/// The positional argument expressions of an ordinary call, borrowed — the
/// shape `CALL proc(...)` always has. `None` for the JSON-object calls whose
/// named arguments `function_args` rewrites into values.
pub(crate) fn unnamed_function_arg_exprs(
    function: &Function,
) -> Option<impl Iterator<Item = &Expr>> {
    let FunctionArguments::List(args) = &function.args else {
        return None;
    };
    if standard_json_object_call(function, args) {
        return None;
    }
    Some(args.args.iter().filter_map(|arg| match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
        _ => None,
    }))
}

pub(crate) fn function_args(function: &Function) -> Vec<Expr> {
    let FunctionArguments::List(args) = &function.args else {
        return Vec::new();
    };
    let standard_json_object = standard_json_object_call(function, args);
    if standard_json_object {
        let mut values = Vec::with_capacity(args.args.len() * 2);
        for arg in &args.args {
            match arg {
                FunctionArg::ExprNamed { name, arg, .. } => {
                    values.push(name.clone());
                    if let FunctionArgExpr::Expr(expr) = arg {
                        values.push(expr.clone());
                    }
                }
                FunctionArg::Named { name, arg, .. } => {
                    values.push(Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(
                        name.value.clone(),
                    ))));
                    if let FunctionArgExpr::Expr(expr) = arg {
                        values.push(expr.clone());
                    }
                }
                FunctionArg::Unnamed(_) => {}
            }
        }
        return values;
    }
    args.args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr.clone()),
            _ => None,
        })
        .collect()
}

pub(crate) fn function_args_are_distinct(function: &Function) -> bool {
    let FunctionArguments::List(args) = &function.args else {
        return false;
    };
    matches!(args.duplicate_treatment, Some(DuplicateTreatment::Distinct))
}

pub(crate) fn json_agg_function_parts(
    function: &Function,
) -> Result<(Expr, bool, Vec<sqlparser::ast::OrderByExpr>)> {
    let args = function_args(function);
    let [expr] = args.as_slice() else {
        return Err(SqlError::Unsupported(format!(
            "{} expects exactly one argument",
            function.name
        )));
    };
    let FunctionArguments::List(arguments) = &function.args else {
        unreachable!("a function with one parsed argument has a list argument node");
    };
    let mut order_by = Vec::new();
    let standard = object_name(&function.name)
        .ok()
        .is_some_and(|name| name.strip_prefix("pg_catalog.").unwrap_or(&name) == "json_arrayagg");
    for clause in &arguments.clauses {
        match clause {
            sqlparser::ast::FunctionArgumentClause::OrderBy(expressions) => {
                order_by.extend(expressions.iter().cloned());
            }
            sqlparser::ast::FunctionArgumentClause::JsonNullClause(_)
            | sqlparser::ast::FunctionArgumentClause::JsonReturningClause(_)
                if standard => {}
            other => {
                return Err(SqlError::Unsupported(format!(
                    "unsupported {} aggregate clause {other}",
                    function.name
                )));
            }
        }
    }
    if !function.within_group.is_empty() {
        return Err(SqlError::Unsupported(format!(
            "{} does not support WITHIN GROUP",
            function.name
        )));
    }

    let distinct = function_args_are_distinct(function);
    if distinct && order_by.iter().any(|order| &order.expr != expr) {
        return Err(SqlError::ConstraintViolation {
            sqlstate: "42P10",
            message:
                "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list"
                    .to_string(),
            table: None,
            column: None,
            constraint: None,
        });
    }
    Ok((expr.clone(), distinct, order_by))
}

pub(crate) fn json_object_agg_function_parts(
    function: &Function,
) -> Result<(Expr, Expr, bool, Vec<sqlparser::ast::OrderByExpr>)> {
    let args = function_args(function);
    let [key, value] = args.as_slice() else {
        return Err(SqlError::Unsupported(format!(
            "{} expects exactly two arguments",
            function.name
        )));
    };
    let order_by = json_aggregate_order_by(function)?;
    let distinct = function_args_are_distinct(function);
    if distinct
        && order_by
            .iter()
            .any(|order| &order.expr != key && &order.expr != value)
    {
        return Err(SqlError::ConstraintViolation {
            sqlstate: "42P10",
            message:
                "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list"
                    .to_string(),
            table: None,
            column: None,
            constraint: None,
        });
    }
    Ok((key.clone(), value.clone(), distinct, order_by))
}

pub(crate) fn string_agg_function_parts(
    function: &Function,
) -> Result<(Expr, Expr, bool, Vec<sqlparser::ast::OrderByExpr>)> {
    let args = function_args(function);
    let [expr, delimiter] = args.as_slice() else {
        return Err(SqlError::Unsupported(format!(
            "{} expects exactly two arguments",
            function.name
        )));
    };
    let order_by = json_aggregate_order_by(function)?;
    let distinct = function_args_are_distinct(function);
    if distinct
        && order_by
            .iter()
            .any(|order| &order.expr != expr && &order.expr != delimiter)
    {
        return Err(SqlError::ConstraintViolation {
            sqlstate: "42P10",
            message:
                "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list"
                    .to_string(),
            table: None,
            column: None,
            constraint: None,
        });
    }
    Ok((expr.clone(), delimiter.clone(), distinct, order_by))
}

pub(crate) fn json_aggregate_order_by(
    function: &Function,
) -> Result<Vec<sqlparser::ast::OrderByExpr>> {
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(SqlError::Unsupported(format!(
            "{} requires an argument list",
            function.name
        )));
    };
    let standard = object_name(&function.name).ok().is_some_and(|name| {
        matches!(
            name.strip_prefix("pg_catalog.").unwrap_or(&name),
            "json_objectagg" | "json_objectagg_unique"
        )
    });
    let mut order_by = Vec::new();
    for clause in &arguments.clauses {
        match clause {
            sqlparser::ast::FunctionArgumentClause::OrderBy(expressions) => {
                order_by.extend(expressions.iter().cloned());
            }
            sqlparser::ast::FunctionArgumentClause::JsonNullClause(_)
            | sqlparser::ast::FunctionArgumentClause::JsonReturningClause(_)
                if standard => {}
            other => {
                return Err(SqlError::Unsupported(format!(
                    "unsupported {} aggregate clause {other}",
                    function.name
                )));
            }
        }
    }
    if !function.within_group.is_empty() {
        return Err(SqlError::Unsupported(format!(
            "{} does not support WITHIN GROUP",
            function.name
        )));
    }
    Ok(order_by)
}

pub(crate) fn json_standard_aggregate_options(
    function: &Function,
    default_strict: bool,
    standard: bool,
) -> Result<(bool, Option<DataType>)> {
    if !standard {
        return Ok((default_strict, None));
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(SqlError::Unsupported(format!(
            "{} requires an argument list",
            function.name
        )));
    };
    let mut strict = default_strict;
    let mut returning = None;
    for clause in &arguments.clauses {
        match clause {
            sqlparser::ast::FunctionArgumentClause::JsonNullClause(clause) => {
                strict = matches!(clause, sqlparser::ast::JsonNullClause::AbsentOnNull);
            }
            sqlparser::ast::FunctionArgumentClause::JsonReturningClause(clause) => {
                returning = Some(clause.data_type.clone());
            }
            _ => {}
        }
    }
    if let Some(data_type) = returning.as_ref() {
        let pg_type = pg_type_from_data_type(data_type)?.0;
        if !matches!(
            pg_type.as_str(),
            "json" | "jsonb" | "text" | "varchar" | "bpchar" | "bytea"
        ) {
            return Err(SqlError::cannot_coerce(format!(
                "cannot use RETURNING {pg_type} with JSON aggregate"
            )));
        }
    }
    Ok((strict, returning))
}

pub(crate) fn json_array_aggregate_flags(name: &str) -> Option<(bool, bool)> {
    match name.strip_prefix("pg_catalog.").unwrap_or(name) {
        "json_agg" => Some((false, false)),
        "json_agg_strict" => Some((false, true)),
        "json_arrayagg" => Some((false, true)),
        "jsonb_agg" => Some((true, false)),
        "jsonb_agg_strict" => Some((true, true)),
        _ => None,
    }
}

pub(crate) fn json_object_aggregate_flags(name: &str) -> Option<(bool, bool, bool)> {
    match name.strip_prefix("pg_catalog.").unwrap_or(name) {
        "json_object_agg" => Some((false, false, false)),
        "json_object_agg_strict" => Some((false, true, false)),
        "json_object_agg_unique" => Some((false, false, true)),
        "json_object_agg_unique_strict" => Some((false, true, true)),
        "json_objectagg" => Some((false, false, false)),
        "json_objectagg_unique" => Some((false, false, true)),
        "jsonb_object_agg" => Some((true, false, false)),
        "jsonb_object_agg_strict" => Some((true, true, false)),
        "jsonb_object_agg_unique" => Some((true, false, true)),
        "jsonb_object_agg_unique_strict" => Some((true, true, true)),
        _ => None,
    }
}

pub(crate) fn partitioning_schema_from_expr(expr: &Expr) -> Result<PartitioningSchema> {
    let Expr::Function(function) = expr else {
        return Err(SqlError::Unsupported(format!(
            "unsupported PARTITION BY expression {expr}"
        )));
    };
    let strategy = object_name(&function.name)?.to_ascii_lowercase();
    if !matches!(strategy.as_str(), "range" | "list" | "hash") {
        return Err(SqlError::Unsupported(format!(
            "unsupported PARTITION BY strategy {strategy}"
        )));
    }
    let key_columns = function_args(function)
        .into_iter()
        .map(|arg| partition_key_column_name(&arg))
        .collect::<Result<Vec<_>>>()?;
    if key_columns.is_empty() {
        return Err(SqlError::InvalidSql(
            "PARTITION BY expects at least one key column".to_string(),
        ));
    }
    Ok(PartitioningSchema {
        strategy,
        key_columns,
    })
}

pub(crate) fn partition_key_column_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| SqlError::InvalidSql("empty partition key".to_string())),
        Expr::Nested(expr) | Expr::Cast { expr, .. } => partition_key_column_name(expr),
        other => Err(SqlError::Unsupported(format!(
            "unsupported partition key expression {other}"
        ))),
    }
}

pub(crate) fn apply_order_by_cancellable(
    records: &mut [Arc<Record>],
    order_by: Option<&OrderBy>,
    schema: Option<&TableSchema>,
    cancellation: &CancellationToken,
) -> Result<()> {
    cancellation.check()?;
    let Some(order_by) = order_by else {
        return Ok(());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(SqlError::Unsupported(
            "ORDER BY ALL is not supported".to_string(),
        ));
    };
    if expressions.is_empty() {
        return Ok(());
    }
    let drivers = expressions
        .iter()
        .map(|order| {
            Ok(match VectorOrder::from_expr(&order.expr)? {
                Some(vector) => (
                    Some(vector),
                    None,
                    None,
                    Some("float8".to_string()),
                    None,
                    None,
                ),
                None if GeometricOrder::from_expr(&order.expr, schema)?.is_some() => (
                    None,
                    GeometricOrder::from_expr(&order.expr, schema)?,
                    None,
                    Some("float8".to_string()),
                    None,
                    None,
                ),
                None => (
                    None,
                    None,
                    Some(schema_projected_field(
                        FieldRef::from_expr(&order.expr)?,
                        schema,
                    )),
                    projected_expr_pg_type(&order.expr, schema),
                    expr_collation(&order.expr, schema)?,
                    schema.and_then(|schema| {
                        partition_key_column_name(&order.expr)
                            .ok()
                            .and_then(|column| schema.column(&column))
                    }),
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if let Some(pg_type) = drivers
        .iter()
        .find_map(|(_, _, _, pg_type, _, _)| type_without_comparison_operators(pg_type.as_deref()))
    {
        return Err(SqlError::undefined_function(format!(
            "could not identify an ordering operator for type {pg_type}"
        )));
    }
    let keyed = records
        .iter()
        .map(|record| {
            let keys = drivers
                .iter()
                .map(|(vector, geometric, field, pg_type, _, column)| {
                    let value = match (vector, geometric, field) {
                        (Some(vector), _, _) => vector
                            .score(record)
                            .map(|value| SqlValue::Float(value as f64))
                            .unwrap_or(SqlValue::Null),
                        (_, Some(geometric), _) => geometric.score(record)?,
                        (_, _, Some(field)) => field.value(record).unwrap_or(SqlValue::Null),
                        _ => unreachable!("ORDER BY driver"),
                    };
                    let typed_key = if matches!(value, SqlValue::Null) {
                        None
                    } else if let Some(column) = column.filter(|column| column.user_type.is_some())
                    {
                        Some(column_typed_index_key(column, &value)?)
                    } else {
                        pg_type
                            .as_deref()
                            .map(|pg_type| pg_typed_index_key(pg_type, &value))
                            .transpose()?
                    };
                    Ok((value, typed_key))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((record.id.clone(), keys))
        })
        .collect::<Result<FxHashMap<_, _>>>()?;
    cancellation.check()?;
    records.sort_by(|left, right| {
        let left_keys = &keyed[&left.id];
        let right_keys = &keyed[&right.id];
        expressions
            .iter()
            .zip(left_keys.iter().zip(right_keys))
            .enumerate()
            .map(|(index, (order, (left, right)))| {
                typed_order_collation_value_ordering(
                    drivers[index].4.as_deref(),
                    &left.0,
                    left.1.as_deref(),
                    &right.0,
                    right.1.as_deref(),
                    &order.options,
                )
            })
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    });
    cancellation.check()?;
    Ok(())
}

pub(crate) struct GeometricOrder {
    field: FieldRef,
    query: SqlValue,
    field_type: String,
    query_type: String,
    field_on_left: bool,
    op: BinaryOperator,
}

impl GeometricOrder {
    pub(crate) fn from_expr(expr: &Expr, schema: Option<&TableSchema>) -> Result<Option<Self>> {
        let Expr::BinaryOp { left, op, right } = expr else {
            return Ok(None);
        };
        if op.to_string() != "<->" {
            return Ok(None);
        }
        let left_type = projected_expr_pg_type(left, schema);
        let right_type = projected_expr_pg_type(right, schema);
        if geometric_binary_result_pg_type(op, left_type.as_deref(), right_type.as_deref())
            .as_deref()
            != Some("float8")
        {
            return Ok(None);
        }
        if let Some(field) = FieldRef::from_expr_opt(left) {
            return Ok(Some(Self {
                field: schema_projected_field(field, schema),
                query: eval_constant_expr(right)?,
                field_type: left_type.unwrap(),
                query_type: right_type.unwrap(),
                field_on_left: true,
                op: op.clone(),
            }));
        }
        if let Some(field) = FieldRef::from_expr_opt(right) {
            return Ok(Some(Self {
                field: schema_projected_field(field, schema),
                query: eval_constant_expr(left)?,
                field_type: right_type.unwrap(),
                query_type: left_type.unwrap(),
                field_on_left: false,
                op: op.clone(),
            }));
        }
        Ok(None)
    }

    pub(crate) fn score(&self, record: &Record) -> Result<SqlValue> {
        let value = self.field.value(record).unwrap_or(SqlValue::Null);
        let result = if self.field_on_left {
            eval_geometric_binary_value(
                &value,
                &self.op,
                &self.query,
                Some(&self.field_type),
                Some(&self.query_type),
            )?
        } else {
            eval_geometric_binary_value(
                &self.query,
                &self.op,
                &value,
                Some(&self.query_type),
                Some(&self.field_type),
            )?
        };
        Ok(result.unwrap_or(SqlValue::Null))
    }
}

pub(crate) struct VectorOrder {
    pub(crate) field: FieldRef,
    pub(crate) query: Vec<f32>,
    pub(crate) metric: VectorOrderMetric,
}

pub(crate) enum VectorOrderMetric {
    CosineDistance,
    NegativeInnerProduct,
    L2Distance,
    L1Distance,
}

impl VectorOrderMetric {
    pub(crate) fn vector_metric(&self) -> Option<VectorMetric> {
        Some(match self {
            Self::CosineDistance => VectorMetric::Cosine,
            Self::NegativeInnerProduct => VectorMetric::Dot,
            Self::L2Distance => VectorMetric::L2,
            Self::L1Distance => return None,
        })
    }
}

impl VectorOrder {
    pub(crate) fn from_expr(expr: &Expr) -> Result<Option<Self>> {
        let Expr::BinaryOp { left, op, right } = expr else {
            return Ok(None);
        };
        let metric = match op.to_string().as_str() {
            "<=>" => VectorOrderMetric::CosineDistance,
            "<#>" => VectorOrderMetric::NegativeInnerProduct,
            "<->" => VectorOrderMetric::L2Distance,
            "<+>" | "<^" => VectorOrderMetric::L1Distance,
            _ => return Ok(None),
        };
        let field = FieldRef::from_expr(left)?;
        let value = eval_constant_expr(right)?;
        let Ok(query) = sql_value_to_vector(&value) else {
            // PostgreSQL overloads distance/operator spellings for planar
            // geometry. Leave non-vector operands to normal typed evaluation.
            return Ok(None);
        };
        Ok(Some(Self {
            field,
            query,
            metric,
        }))
    }

    pub(crate) fn score(&self, record: &Record) -> Result<f32> {
        let vector = match &self.field {
            FieldRef::Vector | FieldRef::Column(_) => record.vector.as_deref(),
            _ => None,
        }
        .ok_or_else(|| SqlError::InvalidSql("record has no vector".to_string()))?;
        Ok(match self.metric {
            VectorOrderMetric::CosineDistance => 1.0 - cosine_similarity(vector, &self.query)?,
            VectorOrderMetric::NegativeInnerProduct => -dot_product(vector, &self.query)?,
            VectorOrderMetric::L2Distance => l2_distance(vector, &self.query)?,
            VectorOrderMetric::L1Distance => vector
                .iter()
                .zip(&self.query)
                .map(|(left, right)| (left - right).abs())
                .sum(),
        })
    }
}

/// The `(offset, limit)` a query's LIMIT/OFFSET clause resolves to, for a
/// streaming consumer that must know how many rows to stop after.
///
/// `None` limit means unbounded. Shares its clause handling with
/// [`apply_limit`] so a streaming path and a materializing path can never
/// disagree about what a clause means.
pub(crate) fn limit_offset_bounds(query: &Query) -> Result<(usize, Option<usize>)> {
    let Some(limit_clause) = &query.limit_clause else {
        return Ok((0, None));
    };
    let (limit, offset) = match limit_clause {
        LimitClause::LimitOffset {
            limit: Some(limit),
            offset,
            ..
        } => (Some(limit), offset.as_ref().map(|offset| &offset.value)),
        LimitClause::OffsetCommaLimit { offset, limit } => (Some(limit), Some(offset)),
        LimitClause::LimitOffset {
            limit: None,
            offset,
            ..
        } => (None, offset.as_ref().map(|offset| &offset.value)),
    };
    let offset = offset.map(integer_expr).transpose()?.unwrap_or_default();
    let limit = match limit {
        Some(limit) => optional_count_expr(limit)?,
        None => None,
    };
    Ok((offset, limit))
}

pub(crate) fn apply_limit<R>(records: &mut Vec<R>, query: &Query) -> Result<()> {
    let Some(limit_clause) = &query.limit_clause else {
        return Ok(());
    };
    let (limit, offset) = match limit_clause {
        LimitClause::LimitOffset {
            limit: Some(limit),
            offset,
            ..
        } => (limit, offset.as_ref().map(|offset| &offset.value)),
        LimitClause::OffsetCommaLimit { offset, limit } => (limit, Some(offset)),
        LimitClause::LimitOffset {
            limit: None,
            offset,
            ..
        } => {
            if let Some(offset) = offset {
                let offset = integer_expr(&offset.value)?;
                records.drain(0..offset.min(records.len()));
            }
            return Ok(());
        }
    };
    let offset = offset.map(integer_expr).transpose()?.unwrap_or_default();
    if offset > 0 {
        records.drain(0..offset.min(records.len()));
    }
    if let Some(limit) = optional_count_expr(limit)? {
        records.truncate(limit);
    }
    Ok(())
}

pub(crate) fn integer_expr(expr: &Expr) -> Result<usize> {
    Ok(optional_count_expr(expr)?.unwrap_or(0))
}

/// Resolve a `LIMIT`/`OFFSET` clause expression to a row count, matching
/// PostgreSQL's implicit coercion of the clause to `bigint`.
///
/// Returns `Ok(None)` for `NULL` (PostgreSQL treats `LIMIT NULL` as no limit and
/// `OFFSET NULL` as `0`). Accepts integer, float (rounded), and string values —
/// the string case covers `LIMIT '500'` and text/unknown-typed bind parameters
/// (`LIMIT $1` where the driver sends the value as text), both of which
/// PostgreSQL coerces rather than rejects. Negative values clamp to `0`.
pub(crate) fn optional_count_expr(expr: &Expr) -> Result<Option<usize>> {
    let count = match eval_constant_expr(expr)? {
        SqlValue::Null => return Ok(None),
        SqlValue::Int(value) => value,
        SqlValue::Float(value) => value.round() as i64,
        SqlValue::String(value) => parse_count_string(&value)?,
        _ => return Err(non_negative_integer_error()),
    };
    Ok(Some(count.max(0) as usize))
}

pub(crate) fn parse_count_string(value: &str) -> Result<i64> {
    let trimmed = value.trim();
    if let Ok(value) = trimmed.parse::<i64>() {
        return Ok(value);
    }
    if let Ok(value) = trimmed.parse::<f64>() {
        return Ok(value.round() as i64);
    }
    Err(non_negative_integer_error())
}

/// Decodes the backslash escapes PostgreSQL allows inside `E'...'`.
pub(crate) fn unescape_pg_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('b') => out.push('\u{8}'),
            Some('f') => out.push('\u{c}'),
            Some('0') => out.push('\0'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

pub(crate) fn non_negative_integer_error() -> SqlError {
    SqlError::Unsupported("LIMIT/OFFSET must be non-negative integer literals".to_string())
}

/// Whether an aggregate must hold O(rows) state and therefore cannot stream:
/// the collecting aggregates (string_agg/array_agg/json_agg/...) and any
/// DISTINCT aggregate (which must retain every distinct value). count/sum/
/// min/max/avg without DISTINCT fold in O(1) state and are excluded.
pub(crate) fn is_collecting_aggregate(aggregate: &Aggregate) -> bool {
    matches!(
        aggregate,
        Aggregate::StringAgg { .. }
            | Aggregate::ArrayAgg { .. }
            | Aggregate::RangeAgg { .. }
            | Aggregate::XmlAgg { .. }
            | Aggregate::JsonAgg { .. }
            | Aggregate::JsonObjectAgg { .. }
            | Aggregate::CountField(_, true)
            | Aggregate::Sum(_, true, _)
            | Aggregate::Avg(_, true)
            | Aggregate::BoolAnd(_, true)
            | Aggregate::BoolOr(_, true)
    )
}

/// Whether any aggregate in the projection requires materializing the whole
/// input (see [`is_collecting_aggregate`]). Used to bound an unfiltered
/// full-table aggregate before it allocates O(table) memory.
pub(crate) fn any_collecting_aggregate(
    functions: &[Function],
    schema: Option<&TableSchema>,
) -> bool {
    functions.iter().any(|function| {
        Aggregate::from_function(function, schema)
            .map(|aggregate| is_collecting_aggregate(&aggregate))
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod function_arg_list_tests {
    use super::*;

    fn call(sql: &str) -> Function {
        let statement = crate::routines::parse_single_statement(sql).expect("parses");
        let Statement::Query(query) = statement else {
            panic!("expected a query");
        };
        let SetExpr::Select(select) = *query.body else {
            panic!("expected a select");
        };
        match select.projection.into_iter().next() {
            Some(SelectItem::UnnamedExpr(Expr::Function(function))) => function,
            other => panic!("expected a function call, got {other:?}"),
        }
    }

    fn owned(list: &FunctionArgList<'_>) -> Vec<Expr> {
        list.iter().cloned().collect()
    }

    #[test]
    fn borrowed_list_matches_owned_args_for_ordinary_calls() {
        let function = call("SELECT coalesce(a, b + 1, 'x')");
        let list = function_arg_list(&function);
        assert!(matches!(list, FunctionArgList::Borrowed(_)));
        assert_eq!(list.len(), 3);
        assert_eq!(owned(&list), function_args(&function));
        assert_eq!(list.first(), function_args(&function).first());
    }

    #[test]
    fn named_json_object_arguments_still_take_the_owned_rewrite() {
        let function = call("SELECT json_object('a': 1, 'b': x)");
        let list = function_arg_list(&function);
        assert!(matches!(list, FunctionArgList::Owned(_)));
        assert_eq!(list.len(), 4);
        assert_eq!(owned(&list), function_args(&function));
    }

    #[test]
    fn named_arguments_outside_json_object_are_skipped_like_before() {
        let function = call("SELECT some_fn(a => 1, 2)");
        let list = function_arg_list(&function);
        assert!(matches!(list, FunctionArgList::Borrowed(_)));
        assert_eq!(owned(&list), function_args(&function));
    }
}
