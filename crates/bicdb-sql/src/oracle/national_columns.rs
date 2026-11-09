//! Durable AL16UTF16 national table declarations under the STANDARD profile.
//! Routine buffers retain their separate OracleNationalV1 compiler contract.
use super::*;
pub(crate) fn modifier(db: &BicDb, data_type: &DataType) -> Result<Option<PgTypeModifier>> {
    if !preview_routine_language_enabled(db)? {
        return Ok(None);
    }
    let source = tokens(&data_type.to_string())?;
    let Some(Token::Word(word)) = source.first() else {
        return Ok(None);
    };
    let is = |name: &str| {
        if word.quote_style.is_some() {
            word.value == name
        } else {
            word.value.eq_ignore_ascii_case(name)
        }
    };
    let fixed = if is("NCHAR") {
        true
    } else if is("NVARCHAR2") {
        false
    } else {
        return Ok(None);
    };
    let length = match &source[1..] {
        [] if fixed => 1,
        [Token::LParen, Token::Number(n, _), Token::RParen] => n
            .parse::<u32>()
            .map_err(|_| errors::database_error(-907, "42601", "missing right parenthesis"))?,
        _ => {
            return Err(errors::database_error(
                -907,
                "42601",
                "national column declaration requires a scalar length",
            ))
        }
    };
    let maximum = if fixed { 1000 } else { 2000 };
    if length == 0 || length > maximum {
        return Err(errors::database_error(
            -910,
            "42601",
            "specified length too long for its datatype",
        ));
    }
    Ok(Some(PgTypeModifier::OracleNationalColumnV1 {
        fixed,
        length,
    }))
}
pub(crate) fn physical_type(modifier: &PgTypeModifier) -> &'static str {
    if matches!(
        modifier,
        PgTypeModifier::OracleNationalColumnV1 { fixed: true, .. }
    ) {
        "bpchar"
    } else {
        "varchar"
    }
}
pub(crate) fn coerce(value: SqlValue, fixed: bool, length: u32) -> Result<SqlValue> {
    let mut text = match value {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::String(value) => value,
        _ => {
            return Err(unsupported(
                "national column assignment requires resolved text",
            ))
        }
    };
    if text.is_empty() {
        return Ok(SqlValue::Null);
    }
    let units = text.encode_utf16().count();
    if units > length as usize {
        return Err(errors::database_error(
            -12899,
            "22001",
            "value too large for Oracle national character column",
        ));
    }
    if fixed {
        text.extend(std::iter::repeat(' ').take(length as usize - units));
    }
    Ok(SqlValue::String(text))
}
pub(crate) fn validate(db: &BicDb, schema: &TableSchema) -> Result<()> {
    for column in &schema.columns {
        if let Some(modifier @ PgTypeModifier::OracleNationalColumnV1 { fixed, length }) =
            &column.type_modifier
        {
            if column.pg_type != physical_type(modifier)
                || column.user_type.is_some()
                || column.array_ndims != 0
                || *length == 0
                || *length > if *fixed { 1000 } else { 2000 }
                || !metadata_v9_enabled(db)?
            {
                return Err(unsupported(
                    "invalid or unfenced Oracle national column metadata",
                ));
            }
        }
    }
    Ok(())
}
pub(crate) fn prepare_save(db: &mut BicDb, schema: &TableSchema) -> Result<()> {
    if schema.columns.iter().any(|column| {
        matches!(
            column.type_modifier,
            Some(PgTypeModifier::OracleNationalColumnV1 { .. })
        )
    }) {
        enable_metadata_v9(db)?;
    }
    validate(db, schema)
}

/// Preserve national provenance through SQL scopes before wire type inference
/// erases the distinction between AL32UTF8 and AL16UTF16 character values.
pub(super) fn in_query(db: &BicDb, query: &Query, expr: &Expr) -> Option<bool> {
    projected(db, query, expr, 0)
}
fn projected(db: &BicDb, query: &Query, expr: &Expr, depth: usize) -> Option<bool> {
    if depth > 16 {
        return None;
    }
    match expr {
        Expr::Nested(expr) | Expr::Trim { expr, .. } => {
            return projected(db, query, expr, depth + 1)
        }
        Expr::Subquery(query) => {
            let SetExpr::Select(select) = query.body.as_ref() else {
                return None;
            };
            let [item] = select.projection.as_slice() else {
                return None;
            };
            let (SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. }) = item
            else {
                return None;
            };
            return projected(db, query, expr, depth + 1);
        }
        Expr::Function(function) => {
            let name = object_name_lowercase(&function.name).ok()?;
            match name.as_str() {
                "bicdb_oracle_national_literal" => return Some(true),
                "bicdb_oracle_national_value"
                | "bicdb_oracle_national_upper_v1"
                | "bicdb_oracle_national_lower_v1" => return Some(false),
                "upper" | "lower" | "trim" | "btrim" | "ltrim" | "rtrim" => {
                    return projected(db, query, function_args(function).first()?, depth + 1)
                }
                _ => return None,
            }
        }
        _ => {}
    }
    let (qualifier, column) = match expr {
        Expr::Identifier(column) => (None, column),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            (Some(&parts[parts.len() - 2]), parts.last()?)
        }
        _ => return None,
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let mut found = Vec::new();
    for from in &select.from {
        for factor in
            std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
        {
            if let Some(value) = relation_column(db, query, factor, qualifier, column, depth + 1) {
                found.push(value);
            }
        }
    }
    if found.len() == 1 {
        found.pop().flatten()
    } else {
        None
    }
}
fn matches(ident: &Ident, name: &str) -> bool {
    if ident.quote_style.is_some() {
        ident.value == name
    } else {
        ident.value.eq_ignore_ascii_case(name)
    }
}
fn relation_column(
    db: &BicDb,
    query: &Query,
    factor: &TableFactor,
    qualifier: Option<&Ident>,
    column: &Ident,
    depth: usize,
) -> Option<Option<bool>> {
    if depth > 16 {
        return None;
    }
    match factor {
        TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } => {
            let parts = object_name_parts(name);
            let relation = parts.last()?;
            let visible = alias
                .as_ref()
                .map_or(relation.as_str(), |alias| alias.name.value.as_str());
            if qualifier.is_some_and(|name| !matches(name, visible)) {
                return None;
            }
            if parts.len() == 1 {
                if let Some(cte) = query.with.as_ref().and_then(|with| {
                    with.cte_tables
                        .iter()
                        .find(|cte| cte.alias.name.value == *relation)
                }) {
                    return projection_column(
                        db,
                        &cte.query,
                        column,
                        &cte.alias.columns,
                        depth + 1,
                    );
                }
            }
            let schema = load_schema(db, &relation_name(name).ok()?).ok()??;
            let mut columns = schema
                .columns
                .iter()
                .filter(|field| !field.hidden && matches(column, &field.name));
            let field = columns.next()?;
            if columns.next().is_some() {
                return Some(None);
            }
            Some(match field.type_modifier {
                Some(PgTypeModifier::OracleNationalColumnV1 { fixed, .. }) => Some(fixed),
                _ => None,
            })
        }
        TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } => {
            if qualifier.is_some_and(|name| !matches(name, &alias.name.value)) {
                return None;
            }
            projection_column(db, subquery, column, &alias.columns, depth + 1)
        }
        _ => None,
    }
}
fn projection_column(
    db: &BicDb,
    query: &Query,
    column: &Ident,
    aliases: &[sqlparser::ast::TableAliasColumnDef],
    depth: usize,
) -> Option<Option<bool>> {
    if depth > 16 {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    for (index, item) in select.projection.iter().enumerate() {
        if let Some(alias) = aliases.get(index) {
            if matches(column, &alias.name.value) {
                return match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        Some(projected(db, query, expr, depth + 1))
                    }
                    _ => None,
                };
            }
            continue;
        }
        match item {
            SelectItem::ExprWithAlias { expr, alias } if matches(column, &alias.value) => {
                return Some(projected(db, query, expr, depth + 1))
            }
            SelectItem::UnnamedExpr(expr) if matches(column, &select_expr_column_name(expr)) => {
                return Some(projected(db, query, expr, depth + 1))
            }
            SelectItem::Wildcard(_) => {
                return Some(projected(
                    db,
                    query,
                    &Expr::Identifier(column.clone()),
                    depth + 1,
                ))
            }
            _ => {}
        }
    }
    None
}
