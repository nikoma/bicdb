//! Dense collection values, bounds and versioned procedural lowering.
use super::*;

#[derive(Clone, Debug)]
pub(crate) struct CollectionBinding {
    pub(crate) type_name: String,
    pub(crate) schema: CollectionSchema,
}

pub(crate) fn schema_for_type(db: &BicDb, name: &str) -> Result<Option<CollectionSchema>> {
    if pg_type_oid_by_name(name).is_some() {
        return Ok(None);
    }
    let Some(oid) = pg_user_type_oid_by_name(db, name)? else {
        return Ok(None);
    };
    let Some(user_type) = load_user_type_by_oid(db, i64::from(oid))? else {
        return Ok(None);
    };
    if user_type.oid != i64::from(oid) {
        return Ok(None);
    }
    match user_type.kind {
        UserTypeKind::Domain {
            oracle_collection, ..
        } => Ok(oracle_collection),
        _ => Ok(None),
    }
}

pub(crate) fn integer_argument(value: &SqlValue) -> Result<Option<i32>> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    PgNumeric::from_postgres_text(&value.to_cell())
        .and_then(|number| number.with_typmod(10, 0))
        .ok()
        .and_then(|number| number.to_decimal_text().parse::<i32>().ok())
        .map(Some)
        .ok_or_else(|| {
            errors::database_error(
                -6502,
                "22003",
                "collection argument is outside PLS_INTEGER range",
            )
        })
}

pub(crate) fn dense_elements(value: &SqlValue) -> Result<&Vec<JsonValue>> {
    if matches!(value, SqlValue::Null) {
        return Err(errors::database_error(
            -6531,
            "55000",
            "reference to an uninitialized collection",
        ));
    }
    require_one_dimensional_array(
        value,
        "2202E",
        "nested tables require one-dimensional elements",
    )?;
    let Some((array, bounds)) = array_json_parts(value, "collection method")? else {
        unreachable!()
    };
    if bounds.iter().any(|bound| *bound != 1) {
        return Err(unsupported("collection indices must start at one"));
    }
    array
        .as_array()
        .ok_or_else(|| unsupported("collection value must be an array"))
}

pub(crate) fn count(value: &SqlValue) -> Result<usize> {
    Ok(dense_elements(value)?.len())
}

pub(crate) fn get(value: &SqlValue, index: &SqlValue) -> Result<SqlValue> {
    let index = integer_argument(index)?
        .ok_or_else(|| errors::database_error(-6502, "22004", "collection subscript is NULL"))?;
    let values = dense_elements(value)?;
    if index < 1 {
        return Err(errors::database_error(
            -6532,
            "2202E",
            "collection subscript is outside the valid range",
        ));
    }
    values
        .get(index as usize - 1)
        .map(json_to_sql_value)
        .ok_or_else(|| {
            errors::database_error(-6533, "2202E", "collection subscript is beyond count")
        })
}

const MAX_DENSE_ELEMENTS: usize = 100_000;
const MAX_DENSE_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn extend(
    value: &SqlValue,
    amount: &SqlValue,
    copy: Option<&SqlValue>,
) -> Result<SqlValue> {
    let values = dense_elements(value)?;
    let amount = integer_argument(amount)?.unwrap_or(0);
    if amount < 0 {
        return Err(errors::database_error(
            -6502,
            "22003",
            "collection extension count is negative",
        ));
    }
    if amount == 0 {
        return Ok(value.clone());
    }
    let amount = amount as usize;
    let element = if let Some(copy) = copy {
        let index = integer_argument(copy)?.ok_or_else(|| {
            errors::database_error(-6502, "22004", "collection copy subscript is NULL")
        })?;
        if index < 1 {
            return Err(errors::database_error(
                -6532,
                "2202E",
                "collection copy subscript is outside the valid range",
            ));
        }
        values.get(index as usize - 1).ok_or_else(|| {
            errors::database_error(-6533, "2202E", "collection copy subscript is beyond count")
        })?
    } else {
        &JsonValue::Null
    };
    fn estimated_bytes(value: &JsonValue) -> usize {
        std::mem::size_of::<JsonValue>()
            + match value {
                JsonValue::String(text) => text.len(),
                _ => 0,
            }
    }
    let total = values.len().checked_add(amount);
    let bytes = values
        .iter()
        .try_fold(0usize, |total, value| {
            total.checked_add(estimated_bytes(value))
        })
        .and_then(|bytes| {
            amount
                .checked_mul(estimated_bytes(element))
                .and_then(|extra| bytes.checked_add(extra))
        });
    if total.is_none_or(|total| total > MAX_DENSE_ELEMENTS)
        || bytes.is_none_or(|bytes| bytes > MAX_DENSE_BYTES)
    {
        return Err(SqlError::data_exception(
            "54000",
            "collection extension exceeds platform element or byte limits",
            None,
        ));
    }
    let mut output = Vec::new();
    output.try_reserve(total.unwrap()).map_err(|_| {
        SqlError::data_exception("54000", "collection extension allocation failed", None)
    })?;
    output.extend(values.iter().cloned());
    output.extend(std::iter::repeat_with(|| element.clone()).take(amount));
    Ok(SqlValue::Json(JsonValue::Array(output).into()))
}

pub(crate) fn assignment_element(value: SqlValue, schema: &CollectionSchema) -> Result<SqlValue> {
    let value = cast_value_to_pg_type(value, &schema.element.pg_type).and_then(|value| {
        apply_pg_type_modifier_with_context(
            value,
            &schema.element.pg_type,
            schema.element.type_modifier.as_ref(),
            false,
        )
    });
    value.map_err(|_| {
        errors::database_error(
            -6502,
            "22001",
            "collection element assignment violates its declared type",
        )
    })
}

pub(crate) fn declaration_bindings(
    db: &BicDb,
    bindings: &mut BTreeMap<String, CollectionBinding>,
    declarations: &[RoutineDecl],
) -> Result<()> {
    for declaration in declarations {
        match declaration {
            RoutineDecl::Variable { name, pg_type, .. } => {
                let key = normalize_object_name(name);
                bindings.remove(&key);
                if let Some(kind) = pg_type {
                    if let Some(collection) = schema_for_type(db, kind)? {
                        bindings.insert(
                            key,
                            CollectionBinding {
                                type_name: kind.clone(),
                                schema: collection,
                            },
                        );
                    }
                }
            }
            RoutineDecl::Alias { name, position } => {
                let collection = bindings.get(&format!("${position}")).cloned();
                let key = normalize_object_name(name);
                bindings.remove(&key);
                if let Some(collection) = collection {
                    bindings.insert(key, collection);
                }
            }
            RoutineDecl::Cursor { name, .. } => {
                bindings.remove(&normalize_object_name(name));
            }
        }
    }
    Ok(())
}

fn scalar_arguments(function: &sqlparser::ast::Function) -> Result<Vec<&Expr>> {
    let FunctionArguments::List(list) = &function.args else {
        return Err(unsupported("collection methods require scalar arguments"));
    };
    if list.duplicate_treatment.is_some()
        || !list.clauses.is_empty()
        || function.filter.is_some()
        || function.over.is_some()
    {
        return Err(unsupported(
            "collection methods require positional scalar arguments",
        ));
    }
    list.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
            _ => Err(unsupported(
                "collection methods require positional scalar arguments",
            )),
        })
        .collect()
}

pub(crate) fn method_expression(
    expr: &mut Expr,
    bindings: &BTreeMap<String, CollectionBinding>,
) -> Result<()> {
    if let Expr::CompoundIdentifier(parts) = expr {
        if let [variable, method] = parts.as_slice() {
            if bindings.contains_key(&normalize_object_name(&variable.value)) {
                if !method.value.eq_ignore_ascii_case("COUNT") {
                    return Err(unsupported("collection property is not implemented"));
                }
                *expr = expression(&format!(
                    "CAST(pg_catalog.bicdb_oracle_collection_count({variable}) AS BIGINT)"
                ))?;
                return Ok(());
            }
        }
    }
    let Expr::Function(function) = expr else {
        return Ok(());
    };
    let parts = object_name_parts(&function.name);
    match parts.as_slice() {
        [variable] if bindings.contains_key(&normalize_object_name(variable)) => {
            let collection = &bindings[&normalize_object_name(variable)];
            let args = scalar_arguments(function)?;
            let [index] = args.as_slice() else {
                return Err(unsupported(
                    "collection variables require one scalar subscript",
                ));
            };
            let variable = function.name.to_string();
            *expr = expression(&format!(
                "CAST(pg_catalog.bicdb_oracle_collection_get({variable},{}) AS {})",
                render_sql(*index),
                collection.schema.element.pg_type
            ))?;
        }
        [variable, method] if bindings.contains_key(&normalize_object_name(variable)) => {
            if !method.eq_ignore_ascii_case("COUNT") || !scalar_arguments(function)?.is_empty() {
                return Err(unsupported(
                    "collection method expression is not implemented",
                ));
            }
            let variable = function.name.0[0].to_string();
            *expr = expression(&format!(
                "CAST(pg_catalog.bicdb_oracle_collection_count({variable}) AS BIGINT)"
            ))?;
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn method_statement(
    statement: &mut RoutineStmt,
    bindings: &BTreeMap<String, CollectionBinding>,
) -> Result<()> {
    let RoutineStmt::Sql(Statement::Call(function)) = statement else {
        return Ok(());
    };
    let parts = object_name_parts(&function.name);
    let [variable, method] = parts.as_slice() else {
        return Ok(());
    };
    if !bindings.contains_key(&normalize_object_name(variable)) {
        return Ok(());
    }
    if !method.eq_ignore_ascii_case("EXTEND") {
        return Err(unsupported("collection mutator is not implemented"));
    }
    let args = scalar_arguments(function)?;
    if args.len() > 2 {
        return Err(unsupported("EXTEND accepts zero, one or two arguments"));
    }
    let variable_sql = function.name.0[0].to_string();
    let amount = args
        .first()
        .map(|expr| render_sql(*expr))
        .unwrap_or_else(|| "1".into());
    let copy = args
        .get(1)
        .map(|expr| format!(",{}", render_sql(*expr)))
        .unwrap_or_default();
    let expr = expression(&format!(
        "pg_catalog.bicdb_oracle_collection_extend({variable_sql},{amount}{copy})"
    ))?;
    *statement = RoutineStmt::Assignment {
        target: RoutineAssignmentTarget::Variable(normalize_object_name(variable)),
        expr: RoutineExpr::unbound(expr),
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dense_extension_checks_element_and_byte_limits_before_copying() {
        let small = array_value(vec![SqlValue::String("x".into())]);
        assert_eq!(
            extend(&small, &SqlValue::Int(i64::from(i32::MAX)), None)
                .unwrap_err()
                .sqlstate(),
            "54000"
        );
        assert_eq!(count(&small).unwrap(), 1);
        let wide = array_value(vec![SqlValue::String("x".repeat(4000))]);
        assert_eq!(
            extend(&wide, &SqlValue::Int(10_000), Some(&SqlValue::Int(1)))
                .unwrap_err()
                .sqlstate(),
            "54000"
        );
        assert_eq!(count(&wide).unwrap(), 1);
        assert_eq!(
            extend(&small, &SqlValue::Null, Some(&SqlValue::Int(99))).unwrap(),
            small
        );
        assert_eq!(
            integer_argument(&SqlValue::String("1.5".into())).unwrap(),
            Some(2)
        );
        assert_eq!(
            integer_argument(&SqlValue::String("-1.5".into())).unwrap(),
            Some(-2)
        );
    }
}
