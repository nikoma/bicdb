//! Durable logical temporal types backed by ordinary PostgreSQL timestamps.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Source {
    Date,
    Timestamp,
    TimestampWithTimeZone,
}

pub(crate) fn modifier(db: &BicDb, data_type: &DataType) -> Result<Option<PgTypeModifier>> {
    if !preview_routine_language_enabled(db)? {
        return Ok(None);
    }
    let (source, precision) = match data_type {
        DataType::Date => (Source::Date, 0),
        DataType::Timestamp(
            precision,
            sqlparser::ast::TimezoneInfo::None | sqlparser::ast::TimezoneInfo::WithoutTimeZone,
        ) => {
            let precision = precision.unwrap_or(6);
            if precision > 6 {
                return Err(unsupported(
                    "Oracle TIMESTAMP precision above six digits is not supported",
                ));
            }
            (Source::Timestamp, precision as u8)
        }
        DataType::Timestamp(
            precision,
            sqlparser::ast::TimezoneInfo::WithTimeZone | sqlparser::ast::TimezoneInfo::Tz,
        ) => {
            let precision = precision.unwrap_or(6);
            if precision > 6 {
                return Err(unsupported(
                    "Oracle TIMESTAMP precision above six digits is not supported",
                ));
            }
            (Source::TimestampWithTimeZone, precision as u8)
        }
        _ => return Ok(None),
    };
    Ok(Some(PgTypeModifier::OracleTemporalV1 { source, precision }))
}

pub(crate) fn physical_type(modifier: &PgTypeModifier) -> &'static str {
    match modifier {
        PgTypeModifier::OracleTemporalV1 {
            source: Source::TimestampWithTimeZone,
            ..
        } => "timestamptz",
        _ => "timestamp",
    }
}

pub(crate) fn source(column: &ColumnSchema) -> Option<Source> {
    match column.type_modifier {
        Some(PgTypeModifier::OracleTemporalV1 { source, .. }) => Some(source),
        _ => None,
    }
}

pub(crate) fn catalog_type(db: &BicDb, args: &[SqlValue]) -> Result<SqlValue> {
    if args.len() != 2 {
        return Err(SqlError::InvalidSql(
            "bicdb_oracle_column_type requires a relation name and column name".into(),
        ));
    }
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let relation = args[0].to_cell();
    let mut parser = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(&relation)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    let name = parser
        .parse_object_name(false)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(SqlError::InvalidSql(
            "unexpected suffix in relation name".into(),
        ));
    }
    let name = relation_name(&name)?;
    let Some(schema) = load_schema(db, &name)? else {
        return Ok(SqlValue::Null);
    };
    let Some(column) = schema
        .columns
        .iter()
        .find(|column| column.name == args[1].to_cell())
    else {
        return Ok(SqlValue::Null);
    };
    if let Some(PgTypeModifier::OracleCharacterV1 {
        fixed,
        length,
        unit,
    }) = column.type_modifier
    {
        let kind = if fixed { "CHAR" } else { "VARCHAR2" };
        let unit = if unit == super::character_columns::Unit::Bytes {
            "BYTE"
        } else {
            "CHAR"
        };
        return Ok(SqlValue::String(format!("{kind}({length} {unit})")));
    }
    let value = match column.type_modifier {
        Some(PgTypeModifier::OracleBlobV1) => Some("BLOB".into()),
        Some(PgTypeModifier::OracleTemporalV1 {
            source: Source::Date,
            ..
        }) => Some("DATE".into()),
        Some(PgTypeModifier::OracleTemporalV1 {
            source: Source::Timestamp,
            precision,
        }) => Some(format!("TIMESTAMP({precision})")),
        Some(PgTypeModifier::OracleTemporalV1 {
            source: Source::TimestampWithTimeZone,
            precision,
        }) => Some(format!("TIMESTAMP({precision}) WITH TIME ZONE")),
        _ => None,
    };
    Ok(value.map(SqlValue::String).unwrap_or(SqlValue::Null))
}

pub(crate) fn validate(db: &BicDb, schema: &TableSchema) -> Result<()> {
    for column in &schema.columns {
        if let Some(PgTypeModifier::OracleTemporalV1 { source, precision }) = column.type_modifier {
            if column.pg_type
                != physical_type(column.type_modifier.as_ref().expect("matched modifier"))
                || column.user_type.is_some()
                || column.array_ndims != 0
                || precision > 6
                || source == Source::Date && precision != 0
                || !metadata_v2_enabled(db)?
            {
                return Err(unsupported(
                    "invalid or unfenced Oracle temporal column metadata",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn prepare_save(db: &mut BicDb, schema: &TableSchema) -> Result<()> {
    if schema.columns.iter().any(|column| source(column).is_some()) {
        enable_metadata_v2(db)?;
    }
    validate(db, schema)
}

pub(crate) fn prepare_user_type_save(db: &mut BicDb, user_type: &UserTypeSchema) -> Result<()> {
    if let UserTypeKind::Composite { attributes, .. } = &user_type.kind {
        if attributes.iter().any(|attribute| {
            matches!(
                attribute.type_modifier,
                Some(PgTypeModifier::OracleTemporalV1 { .. })
            )
        }) {
            enable_metadata_v2(db)?;
        }
    }
    Ok(())
}

pub(super) fn lower_date_expression(expr: &mut Expr) -> Result<()> {
    let sql = match expr {
        Expr::Cast {
            expr,
            data_type: DataType::Date,
            ..
        } => Some(format!("bicdb_oracle_date_coerce({expr})")),
        Expr::TypedString(value) if value.data_type == DataType::Date => {
            Some(format!("bicdb_oracle_date_coerce({})", value.value))
        }
        _ => None,
    };
    if let Some(sql) = sql {
        *expr = expression(&sql)?;
    }
    Ok(())
}

/// Trace a temporal projection to its declared source type. Unknown lineage
/// remains unknown; physical timestamp precision never implies Oracle DATE.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LogicalTemporal {
    pub source: Source,
    pub precision: Option<u8>,
}
impl LogicalTemporal {
    fn date() -> Self {
        Self {
            source: Source::Date,
            precision: Some(0),
        }
    }
    fn timestamp(precision: Option<u8>) -> Self {
        Self {
            source: Source::Timestamp,
            precision,
        }
    }
}
pub(super) fn in_query(db: &BicDb, query: &Query, expr: &Expr) -> Option<Source> {
    logical_in_query(db, query, expr).map(|value| value.source)
}
pub(super) fn logical_in_query(db: &BicDb, query: &Query, expr: &Expr) -> Option<LogicalTemporal> {
    projected(db, query, expr, 0)
}
fn projected(db: &BicDb, query: &Query, expr: &Expr, depth: usize) -> Option<LogicalTemporal> {
    if depth > 16 {
        return None;
    }
    match expr {
        Expr::Nested(inner) => return projected(db, query, inner, depth + 1),
        Expr::Subquery(inner) => {
            let SetExpr::Select(select) = inner.body.as_ref() else {
                return None;
            };
            let [item] = select.projection.as_slice() else {
                return None;
            };
            let (SelectItem::UnnamedExpr(value) | SelectItem::ExprWithAlias { expr: value, .. }) =
                item
            else {
                return None;
            };
            // SQL transport exposes both Oracle DATE and TIMESTAMP as a
            // timestamp. Preserve the projected column's logical provenance
            // before generic type inference loses that distinction.
            return projected(db, inner, value, depth + 1);
        }
        Expr::Cast {
            data_type: DataType::Date,
            ..
        } => return Some(LogicalTemporal::date()),
        Expr::Cast {
            data_type:
                DataType::Timestamp(
                    precision,
                    sqlparser::ast::TimezoneInfo::None
                    | sqlparser::ast::TimezoneInfo::WithoutTimeZone,
                ),
            ..
        } => {
            return Some(LogicalTemporal::timestamp(Some(
                precision.unwrap_or(6) as u8
            )))
        }
        Expr::TypedString(value) => {
            return match value.data_type {
                DataType::Date => Some(LogicalTemporal::date()),
                DataType::Timestamp(
                    precision,
                    sqlparser::ast::TimezoneInfo::None
                    | sqlparser::ast::TimezoneInfo::WithoutTimeZone,
                ) => Some(LogicalTemporal::timestamp(Some(
                    precision.unwrap_or(9) as u8
                ))),
                _ => None,
            }
        }
        Expr::Function(function) => {
            let name = object_name_lowercase(&function.name).ok()?;
            match name.as_ref() {
                "bicdb_oracle_date_coerce"
                | "bicdb_oracle_date_add_days"
                | "bicdb_oracle_to_date"
                | "bicdb_oracle_trunc_date"
                | "bicdb_oracle_plsql_trunc_date"
                | "bicdb_oracle_add_months"
                | "bicdb_oracle_decode_date"
                | "bicdb_oracle_nvl_date"
                | "bicdb_oracle_nvl2_date" => return Some(LogicalTemporal::date()),
                "bicdb_oracle_decode_timestamp" => return Some(LogicalTemporal::timestamp(None)),
                "bicdb_oracle_nvl_timestamp" | "bicdb_oracle_nvl2_timestamp" => {
                    let precision = function_args(function)
                        .get(if name == "bicdb_oracle_nvl2_timestamp" {
                            3
                        } else {
                            2
                        })
                        .and_then(|expr| match expr {
                            Expr::Value(value) => match &value.value {
                                Value::Number(n, _) => n.parse::<u8>().ok(),
                                _ => None,
                            },
                            _ => None,
                        });
                    return Some(LogicalTemporal::timestamp(precision));
                }
                _ => {}
            }
            let routine = load_routine(
                db,
                RoutineKind::Function,
                &object_name(&function.name).ok()?,
            )
            .ok()??;
            if typed_preview_routine_language(&routine.language) {
                if routine_arithmetic::returns_date(&routine).ok()? {
                    return Some(LogicalTemporal::date());
                }
                if routine.return_type == "timestamp" {
                    // Oracle SQL exposes unconstrained stored TIMESTAMP
                    // results with nine digits, even when the return value
                    // was rounded by its anchored declaration.
                    return Some(LogicalTemporal::timestamp(Some(9)));
                }
            }
            return None;
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
            if let Some(result) = relation_column(db, query, factor, qualifier, column, depth + 1) {
                found.push(result);
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
) -> Option<Option<LogicalTemporal>> {
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
                    return projection_column(db, &cte.query, column, depth + 1);
                }
            }
            let schema = load_schema(db, &relation_name(name).ok()?).ok()??;
            let mut fields = schema
                .columns
                .iter()
                .filter(|field| !field.hidden && matches(column, &field.name));
            let field = fields.next()?;
            if fields.next().is_some() {
                return Some(None);
            }
            Some(match field.type_modifier {
                Some(PgTypeModifier::OracleTemporalV1 { source, precision }) => {
                    Some(LogicalTemporal {
                        source,
                        precision: Some(precision),
                    })
                }
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
            // Positional derived-column renames need their own lineage map.
            if !alias.columns.is_empty() {
                return None;
            }
            projection_column(db, subquery, column, depth + 1)
        }
        _ => None,
    }
}

fn projection_column(
    db: &BicDb,
    query: &Query,
    column: &Ident,
    depth: usize,
) -> Option<Option<LogicalTemporal>> {
    if depth > 16 {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    for item in &select.projection {
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

#[cfg(test)]
mod tests;
