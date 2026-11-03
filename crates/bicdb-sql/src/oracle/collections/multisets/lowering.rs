//! Resolve collection operator identities before lowering to pure value helpers.
use super::*;

fn registered_type(
    db: &BicDb,
    name: &str,
    namespace: Option<&str>,
) -> Result<Option<UserTypeSchema>> {
    if pg_type_oid_by_name(name).is_some() {
        return Ok(None);
    }
    let name = if name.contains('.') {
        name.to_string()
    } else {
        format!("{}.{name}", namespace.unwrap_or("public"))
    };
    let Some(oid) = pg_user_type_oid_by_name(db, &name)? else {
        return Ok(None);
    };
    let Some(value) = load_user_type_by_oid(db, i64::from(oid))? else {
        return Ok(None);
    };
    if value.oid != i64::from(oid)
        || !matches!(
            value.kind,
            UserTypeKind::Domain {
                oracle_collection: Some(_),
                ..
            }
        )
    {
        return Ok(None);
    }
    Ok(Some(value))
}

fn expression_type(
    db: &BicDb,
    expr: &Expr,
    bindings: &BTreeMap<String, CollectionBinding>,
    namespace: Option<&str>,
    in_query: bool,
) -> Result<Option<UserTypeSchema>> {
    match expr {
        Expr::Nested(expr) => expression_type(db, expr, bindings, namespace, in_query),
        Expr::Identifier(name) if !in_query => {
            let Some(binding) = bindings.get(&normalize_object_name(&name.value)) else {
                return Ok(None);
            };
            registered_type(db, &binding.type_name, namespace)
        }
        Expr::Cast {
            data_type: DataType::Custom(name, _),
            ..
        } => registered_type(db, &object_name(name)?, namespace),
        Expr::Function(function) => {
            let parts = object_name_parts(&function.name);
            let expected_schema = match parts.as_slice() {
                [_] => namespace.unwrap_or("public"),
                [schema, _] => schema.as_str(),
                _ => return Ok(None),
            };
            let Some(routine) =
                load_routine(db, RoutineKind::Function, &object_name(&function.name)?)?
            else {
                return Ok(None);
            };
            if routine.returns_set || routine_schema_name(&routine) != expected_schema {
                return Ok(None);
            }
            registered_type(db, &routine.return_type, None)
        }
        _ => Ok(None),
    }
}
fn null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(value) => matches!(value.value, Value::Null),
        Expr::Nested(expr) => null_literal(expr),
        _ => false,
    }
}

pub(crate) fn lower_expression(
    db: &BicDb,
    expr: &mut Expr,
    bindings: &BTreeMap<String, CollectionBinding>,
    namespace: Option<&str>,
    in_query: bool,
) -> Result<()> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Custom(marker),
        right,
    } = expr
    else {
        return Ok(());
    };
    let Some(operation) = grammar::Operation::from_marker(marker) else {
        return Ok(());
    };
    let lhs = expression_type(db, left, bindings, namespace, in_query)?;
    if matches!(
        operation,
        grammar::Operation::IsEmpty | grammar::Operation::IsNotEmpty
    ) {
        if lhs.is_none() {
            return Err(unsupported(
                "IS EMPTY requires a resolved named nested-table operand",
            ));
        }
        let negated = operation == grammar::Operation::IsNotEmpty;
        *expr = expression(&format!(
            "CAST(pg_catalog.bicdb_oracle_collection_is_empty({}, {negated}) AS BOOLEAN)",
            render_sql(left.as_ref())
        ))?;
        return Ok(());
    }
    let rhs = expression_type(db, right, bindings, namespace, in_query)?;
    let kind=match (lhs,rhs) {
        (Some(lhs),Some(rhs)) if lhs.oid==rhs.oid=>lhs,
        (Some(_),Some(_))=>return Err(errors::database_error(-932,"42804","multiset operands require the same named nested-table type")),
        (Some(kind),None) if null_literal(right)=>kind,
        (None,Some(kind)) if null_literal(left)=>kind,
        _=>return Err(unsupported("multiset operands require resolved named nested-table types; use an explicit collection cast")),
    };
    let UserTypeKind::Domain {
        oracle_collection: Some(collection),
        ..
    } = &kind.kind
    else {
        unreachable!()
    };
    let name = format!(
        "\"{}\".\"{}\"",
        kind.schema_name.replace('"', "\"\""),
        kind.name.replace('"', "\"\"")
    );
    *expr = expression(&format!(
        "CAST(pg_catalog.bicdb_oracle_multiset({}, {}, '{}', '{}') AS {name})",
        render_sql(left.as_ref()),
        render_sql(right.as_ref()),
        operation.marker(),
        collection.element.pg_type
    ))?;
    Ok(())
}
