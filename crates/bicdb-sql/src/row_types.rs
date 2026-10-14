//! Table and composite row types in queries. PostgreSQL gives every table a
//! composite type of the same name; three forms need that type at analysis
//! time and are resolved here against the catalogs, at every query level:
//!
//! 1. `(expr).*` in a select list. sqlparser cannot parse it, so the pre-parse
//!    rewrite `rewrite_record_star_items` turns it into the marker call
//!    `__bicdb_record_star((expr))`; [`rewrite_row_type_constructs`] replaces
//!    each marker item with one `(expr).field::type AS field` item per
//!    attribute of `expr`'s row type, so every execution and Describe path
//!    sees ordinary typed field accesses (`expr` is evaluated once per field,
//!    as PostgreSQL does for `(f(x)).*`).
//! 2. `json[b]_populate_record[set](base, json)` in FROM without a column
//!    definition list takes the list from `base`'s row type (`NULL::t`).
//! 3. `json[b]_populate_record(base, json)` as a scalar ([`eval_populate_record_call`]).
use crate::*;
use sqlparser::ast::{
    AccessExpr, CastKind, TableAliasColumnDef, TableFunctionArgs, Visit, VisitMut, Visitor,
    VisitorMut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

pub(crate) const RECORD_STAR_MARKER: &str = "__bicdb_record_star";

fn is_record_star_marker(function: &Function) -> bool {
    object_name(&function.name).is_ok_and(|name| name.eq_ignore_ascii_case(RECORD_STAR_MARKER))
}

/// Some(set-returning) for the populate_record family.
fn populate_record_kind(name: &str) -> Option<bool> {
    let name = name.to_ascii_lowercase();
    match name.strip_prefix("pg_catalog.").unwrap_or(&name) {
        "json_populate_record" | "jsonb_populate_record" => Some(false),
        "json_populate_recordset" | "jsonb_populate_recordset" => Some(true),
        _ => None,
    }
}

/// The function name (without schema) of a FROM-clause populate_record call
/// that has no column definition list.
fn untyped_populate_call(factor: &TableFactor) -> Option<String> {
    let (name, alias) = match factor {
        TableFactor::Function { name, alias, .. } => (name, alias),
        TableFactor::Table {
            name,
            args: Some(_),
            alias,
            ..
        } => (name, alias),
        _ => return None,
    };
    let name = object_name(name).ok()?;
    populate_record_kind(&name)?;
    if alias.as_ref().is_some_and(|alias| !alias.columns.is_empty()) {
        return None;
    }
    Some(name.rsplit('.').next().unwrap_or(&name).to_ascii_lowercase())
}

fn table_factor_args(factor: &TableFactor) -> &[FunctionArg] {
    match factor {
        TableFactor::Function { args, .. }
        | TableFactor::Table {
            args: Some(TableFunctionArgs { args, .. }),
            ..
        } => args,
        _ => &[],
    }
}

fn table_factor_alias(factor: &mut TableFactor) -> Option<&mut Option<TableAlias>> {
    match factor {
        TableFactor::Function { alias, .. } | TableFactor::Table { alias, .. } => Some(alias),
        _ => None,
    }
}

/// Finds record-star markers and (unless `markers_only`) untyped FROM-clause
/// populate_record calls.
struct Finder {
    markers_only: bool,
    found: bool,
}

impl Visitor for Finder {
    type Break = ();
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if matches!(expr, Expr::Function(function) if is_record_star_marker(function)) {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        if !self.markers_only && untyped_populate_call(factor).is_some() {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn query_has_row_type_constructs(query: &Query, markers_only: bool) -> bool {
    let mut finder = Finder {
        markers_only,
        found: false,
    };
    let _ = Visit::visit(query, &mut finder);
    finder.found
}

/// The query with `(expr).*` items expanded and untyped FROM-clause
/// populate_record calls typed; None when it has neither.
pub(crate) fn rewrite_row_type_constructs(db: &BicDb, query: &Query) -> Result<Option<Query>> {
    if !query_has_row_type_constructs(query, false) {
        return Ok(None);
    }
    let mut rewritten = query.clone();
    let mut rewriter = Rewriter {
        db,
        ctes: Vec::new(),
        error: None,
    };
    let _ = VisitMut::visit(&mut rewritten, &mut rewriter);
    if let Some(error) = rewriter.error {
        return Err(error);
    }
    if query_has_row_type_constructs(&rewritten, true) {
        return Err(SqlError::Unsupported(
            "(expression).* is supported only as a select-list item".to_string(),
        ));
    }
    Ok(Some(rewritten))
}

struct Rewriter<'a> {
    db: &'a BicDb,
    /// CTE names in scope, innermost query last.
    ctes: Vec<Vec<String>>,
    error: Option<SqlError>,
}

impl VisitorMut for Rewriter<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        self.ctes.push(
            query
                .with
                .iter()
                .flat_map(|with| &with.cte_tables)
                .map(|cte| cte.alias.name.value.to_ascii_lowercase())
                .collect(),
        );
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.ctes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        let Some(function) = untyped_populate_call(factor) else {
            return ControlFlow::Continue(());
        };
        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(base))) =
            table_factor_args(factor).first()
        else {
            return ControlFlow::Continue(());
        };
        let columns = match self.row_type_columns(base, &BTreeMap::new()) {
            Ok(Some(columns)) if columns.iter().all(|(_, data_type)| data_type.is_some()) => {
                columns
            }
            Ok(_) => return ControlFlow::Continue(()),
            Err(error) => {
                self.error = Some(error);
                return ControlFlow::Break(());
            }
        };
        let Some(alias) = table_factor_alias(factor) else {
            return ControlFlow::Continue(());
        };
        let name = alias
            .as_ref()
            .map(|alias| alias.name.clone())
            .unwrap_or_else(|| Ident::new(function));
        *alias = Some(TableAlias {
            explicit: false,
            name,
            columns: columns
                .into_iter()
                .map(|(name, data_type)| TableAliasColumnDef {
                    name: column_ident(&name),
                    data_type,
                })
                .collect(),
            at: None,
        });
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
        if !select.projection.iter().any(|item| {
            matches!(item, SelectItem::UnnamedExpr(Expr::Function(function)) if is_record_star_marker(function))
        }) {
            return ControlFlow::Continue(());
        }
        let bindings = self.from_bindings(&select.from);
        let mut projection = Vec::with_capacity(select.projection.len());
        for item in std::mem::take(&mut select.projection) {
            let SelectItem::UnnamedExpr(Expr::Function(function)) = &item else {
                projection.push(item);
                continue;
            };
            if !is_record_star_marker(function) {
                projection.push(item);
                continue;
            }
            let Ok([record]) = <[Expr; 1]>::try_from(function_args(function)) else {
                self.error = Some(SqlError::InvalidSql(format!(
                    "{RECORD_STAR_MARKER} takes one expression"
                )));
                return ControlFlow::Break(());
            };
            let columns = match self.row_type_columns(&record, &bindings) {
                Ok(Some(columns)) => columns,
                Ok(None) => {
                    self.error = Some(SqlError::Unsupported(format!(
                        "cannot determine the row type of {record}.*; \
                         cast it to a table or composite type"
                    )));
                    return ControlFlow::Break(());
                }
                Err(error) => {
                    self.error = Some(error);
                    return ControlFlow::Break(());
                }
            };
            for (name, data_type) in columns {
                let field = Expr::CompoundFieldAccess {
                    root: Box::new(record.clone()),
                    access_chain: vec![AccessExpr::Dot(Expr::Identifier(column_ident(&name)))],
                };
                let expr = match data_type {
                    Some(data_type) => Expr::Cast {
                        kind: CastKind::DoubleColon,
                        expr: Box::new(field),
                        data_type,
                        array: false,
                        format: None,
                    },
                    None => field,
                };
                projection.push(SelectItem::ExprWithAlias {
                    expr,
                    alias: column_ident(&name),
                });
            }
        }
        select.projection = projection;
        ControlFlow::Continue(())
    }
}

/// A column name as an identifier that prints back to the same name.
fn column_ident(name: &str) -> Ident {
    if name
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
        && !name.starts_with(|ch: char| ch.is_ascii_digit())
    {
        Ident::new(name)
    } else {
        Ident::with_quote('"', name)
    }
}

impl Rewriter<'_> {
    /// Relation alias (or bare table name) -> base table, for whole-row references.
    fn from_bindings(&self, from: &[TableWithJoins]) -> BTreeMap<String, String> {
        let mut bindings = BTreeMap::new();
        let factors = from
            .iter()
            .flat_map(|table| std::iter::once(&table.relation).chain(table.joins.iter().map(|join| &join.relation)));
        for factor in factors {
            let TableFactor::Table {
                name, alias, args: None, ..
            } = factor
            else {
                continue;
            };
            let Ok(table) = relation_name(name) else {
                continue;
            };
            if self
                .ctes
                .iter()
                .flatten()
                .any(|cte| cte.eq_ignore_ascii_case(&table))
            {
                continue;
            }
            let key = alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table).to_string())
                .to_ascii_lowercase();
            bindings.insert(key, table);
        }
        bindings
    }

    /// The attributes of `expr`'s row type with a cast target for each, when
    /// the type can be determined without evaluating `expr`.
    fn row_type_columns(
        &self,
        expr: &Expr,
        bindings: &BTreeMap<String, String>,
    ) -> Result<Option<Vec<(String, Option<DataType>)>>> {
        match expr {
            Expr::Nested(inner) => self.row_type_columns(inner, bindings),
            Expr::Cast { data_type, .. } | Expr::TypedString(TypedString { data_type, .. }) => {
                Ok(user_type_column_from_data_type(self.db, data_type)?
                    .as_ref()
                    .and_then(composite_columns))
            }
            Expr::Identifier(ident) => {
                let Some(table) = bindings.get(&ident.value.to_ascii_lowercase()) else {
                    return Ok(None);
                };
                Ok(load_schema(self.db, table)?
                    .map(|schema| table_row_type_column(&schema, false))
                    .as_ref()
                    .and_then(composite_columns))
            }
            Expr::Function(function) => {
                let Ok(name) = object_name(&function.name) else {
                    return Ok(None);
                };
                if populate_record_kind(&name) != Some(false) {
                    return Ok(None);
                }
                match function_args(function).first() {
                    Some(base) => self.row_type_columns(base, bindings),
                    None => Ok(None),
                }
            }
            Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                for result in conditions
                    .iter()
                    .map(|condition| &condition.result)
                    .chain(else_result.as_deref())
                {
                    if let Some(columns) = self.row_type_columns(result, bindings)? {
                        return Ok(Some(columns));
                    }
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}

fn composite_columns(user_type: &UserTypeColumnSchema) -> Option<Vec<(String, Option<DataType>)>> {
    if user_type.array {
        return None;
    }
    let UserTypeKind::Composite { attributes, .. } = &user_type.kind else {
        return None;
    };
    Some(
        attributes
            .iter()
            .filter(|attribute| !attribute.dropped)
            .map(|attribute| (attribute.name.clone(), attribute_data_type(attribute)))
            .collect(),
    )
}

fn attribute_data_type(attribute: &CompositeAttributeSchema) -> Option<DataType> {
    let spelling = match &attribute.user_type {
        Some(user_type) => user_type.formatted_name(),
        None => attribute.pg_type.clone(),
    };
    Parser::new(&PostgreSqlDialect {})
        .try_with_sql(&spelling)
        .ok()?
        .parse_data_type()
        .ok()
}

/// The composite row type a populate_record base names: from its expression
/// (`NULL::t`, `'(..)'::t`, `ROW(..)::t`) or else from a typed composite value.
fn populate_record_row_type(
    db: &BicDb,
    base_expr: &Expr,
    base: &SqlValue,
) -> Result<Option<UserTypeColumnSchema>> {
    let mut expr = base_expr;
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    if let Expr::Cast { data_type, .. } | Expr::TypedString(TypedString { data_type, .. }) = expr {
        if let Some(user_type) = user_type_column_from_data_type(db, data_type)? {
            return Ok(Some(user_type));
        }
    }
    let SqlValue::Composite(composite) = base else {
        return Ok(None);
    };
    if let Some(oid) = composite.type_oid {
        if let Some(user_type) = load_user_type_by_oid(db, i64::from(oid))? {
            return Ok(Some(user_type.column_type(false)));
        }
    }
    Ok(load_schema(db, &composite.type_name)?.map(|schema| table_row_type_column(&schema, false)))
}

/// A scalar `json[b]_populate_record(base, from_json)` call: its name without
/// schema and its two argument expressions.
pub(crate) fn populate_record_call(function: &Function) -> Result<Option<(String, [Expr; 2])>> {
    let Ok(name) = object_name(&function.name) else {
        return Ok(None);
    };
    if populate_record_kind(&name) != Some(false) {
        return Ok(None);
    }
    let short = name.rsplit('.').next().unwrap_or(&name).to_ascii_lowercase();
    let Ok(args) = <[Expr; 2]>::try_from(function_args(function)) else {
        return Err(SqlError::undefined_function(format!(
            "function {short} expects two arguments"
        )));
    };
    Ok(Some((short, args)))
}

/// `json[b]_populate_record(base, from_json)` for evaluators that take `&self`.
pub(crate) fn eval_populate_record_call(
    db: &BicDb,
    function: &Function,
    mut eval: impl FnMut(&Expr) -> Result<SqlValue>,
) -> Result<Option<SqlValue>> {
    let Some((name, [base_expr, json_expr])) = populate_record_call(function)? else {
        return Ok(None);
    };
    let base = eval(&base_expr)?;
    let json = eval(&json_expr)?;
    populate_record_value(db, &name, &base_expr, base, json).map(Some)
}

/// The record `json[b]_populate_record` returns. JSON keys that name an
/// attribute replace the base record's value (or NULL) for it; other keys are
/// ignored. The row type comes from `base_expr` (`NULL::t`) or a typed `base`.
pub(crate) fn populate_record_value(
    db: &BicDb,
    short: &str,
    base_expr: &Expr,
    base: SqlValue,
    json: SqlValue,
) -> Result<SqlValue> {
    let Some(row_type) = populate_record_row_type(db, base_expr, &base)? else {
        return Err(SqlError::data_exception_public(
            "42804",
            format!("could not determine the row type for {short}; pass NULL::type_name as its first argument"),
            None,
        ));
    };
    let UserTypeKind::Composite { attributes, .. } = &row_type.kind else {
        return Err(SqlError::data_exception_public(
            "42804",
            format!("first argument of {short} must be a row type"),
            None,
        ));
    };
    let json = match json {
        SqlValue::Null => return Ok(base),
        SqlValue::Json(json) => json,
        SqlValue::JsonText(json) => json.parsed().clone(),
        SqlValue::String(text) => serde_json::from_str(&text).map_err(|error| {
            SqlError::invalid_text_representation("json", error.to_string())
        })?,
        other => {
            return Err(SqlError::undefined_function(format!(
                "function {short} does not accept {}",
                projected_value_pg_type(&other)
            )));
        }
    };
    let JsonValue::Object(object) = json else {
        return Err(SqlError::data_exception_public(
            "22023",
            format!(
                "cannot call {short} on {}",
                if json.is_array() { "an array" } else { "a scalar" }
            ),
            None,
        ));
    };
    let base_fields = match &base {
        SqlValue::Composite(composite) => Some(composite),
        _ => None,
    };
    let fields = attributes
        .iter()
        .filter(|attribute| !attribute.dropped)
        .map(|attribute| {
            let value = match object.get(&attribute.name) {
                // json/jsonb attributes keep the JSON value itself.
                Some(value) if matches!(attribute.pg_type.as_str(), "json" | "jsonb") => {
                    SqlValue::Json(value.clone())
                }
                Some(value) => json_to_sql_value(value),
                None => base_fields
                    .and_then(|composite| composite.field(&attribute.name).cloned())
                    .unwrap_or(SqlValue::Null),
            };
            SqlCompositeField {
                name: attribute.name.clone(),
                pg_type: attribute.pg_type.clone(),
                value,
            }
        })
        .collect();
    let record = SqlValue::Composite(SqlComposite {
        type_oid: None,
        type_name: row_type.formatted_name(),
        fields,
    });
    cast_value_to_composite_scalar(record, &row_type, attributes)
}
