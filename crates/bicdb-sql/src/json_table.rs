//! `JSON_TABLE(context, path COLUMNS (...)) AS alias` in FROM.
//!
//! Each JSON_TABLE becomes the LATERAL subquery PostgreSQL's definition
//! describes: one row per item of the row path (`jsonb_path_query ... WITH
//! ORDINALITY`), each regular column the `JSON_VALUE` (or, for json/jsonb
//! columns, `JSON_QUERY`) of its path over the item, `EXISTS PATH` columns
//! `JSON_EXISTS`, `FOR ORDINALITY` the item's position, and a `NESTED PATH`
//! a LEFT JOIN over the nested items so a row without nested items keeps
//! NULL nested columns.

use crate::*;
use sqlparser::ast::{
    DataType, JsonTableColumn, JsonTableColumnErrorHandling, JsonTableNamedColumn, TableAlias,
    TableFactor, VisitMut, VisitorMut,
};
use std::ops::ControlFlow;

fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn literal_text(value: &sqlparser::ast::ValueWithSpan) -> Option<String> {
    match &value.value {
        sqlparser::ast::Value::SingleQuotedString(text)
        | sqlparser::ast::Value::EscapedStringLiteral(text)
        | sqlparser::ast::Value::DollarQuotedString(sqlparser::ast::DollarQuotedString {
            value: text,
            ..
        }) => Some(text.clone()),
        _ => None,
    }
}

struct Builder {
    /// Select-list items, in declaration order.
    columns: Vec<String>,
    /// `FROM` clause pieces after the root row source.
    joins: Vec<String>,
    next_level: usize,
}

fn behaviour(
    handling: Option<&JsonTableColumnErrorHandling>,
) -> Option<(&'static str, Option<String>)> {
    Some(match handling? {
        JsonTableColumnErrorHandling::Null => ("null", None),
        JsonTableColumnErrorHandling::Error => ("error", None),
        JsonTableColumnErrorHandling::Default(value) => ("default", Some(value.to_string())),
    })
}

fn named_column(item: &str, column: &JsonTableNamedColumn) -> Result<String> {
    let path = literal_text(&column.path)
        .ok_or_else(|| SqlError::Unsupported("JSON_TABLE column paths must be literals".into()))?;
    let kind = column.r#type.to_string();
    let json_kind = matches!(column.r#type, DataType::JSON | DataType::JSONB);
    let builtin = if column.exists {
        "bicdb_json_exists"
    } else if json_kind {
        "bicdb_json_query"
    } else {
        "bicdb_json_value"
    };
    let mut spec = serde_json::Map::new();
    spec.insert("returning".into(), JsonValue::String(kind.clone()));
    let mut empty_default = "NULL".to_string();
    let mut error_default = "NULL".to_string();
    if let Some((mode, default)) = behaviour(column.on_empty.as_ref()) {
        spec.insert("on_empty".into(), JsonValue::String(mode.into()));
        if let Some(default) = default {
            empty_default = default;
        }
    }
    if let Some((mode, default)) = behaviour(column.on_error.as_ref()) {
        spec.insert("on_error".into(), JsonValue::String(mode.into()));
        if let Some(default) = default {
            error_default = default;
        }
    }
    Ok(format!(
        "CAST({builtin}({item}, {}, {}, NULL, {empty_default}, {error_default}) AS {kind}) AS {}",
        quote_literal(&path),
        quote_literal(&JsonValue::Object(spec).to_string()),
        quote_ident(&column.name.value)
    ))
}

impl Builder {
    fn level(&mut self, alias: &str, columns: &[JsonTableColumn]) -> Result<()> {
        let item = format!("{alias}.v");
        let mut nested_seen = false;
        for column in columns {
            match column {
                JsonTableColumn::ForOrdinality(name) => self.columns.push(format!(
                    "CAST({alias}.o AS integer) AS {}",
                    quote_ident(&name.value)
                )),
                JsonTableColumn::Named(column) => self.columns.push(named_column(&item, column)?),
                JsonTableColumn::Nested(nested) => {
                    if nested_seen {
                        return Err(SqlError::Unsupported(
                            "JSON_TABLE with sibling NESTED PATH clauses is not supported".into(),
                        ));
                    }
                    nested_seen = true;
                    let path = literal_text(&nested.path).ok_or_else(|| {
                        SqlError::Unsupported("JSON_TABLE nested paths must be literals".into())
                    })?;
                    self.next_level += 1;
                    let nested_alias = format!("bicdb_jt{}", self.next_level);
                    self.joins.push(format!(
                        "LEFT JOIN LATERAL jsonb_path_query({item}, {}) WITH ORDINALITY AS {nested_alias}(v, o) ON true",
                        quote_literal(&path)
                    ));
                    self.level(&nested_alias, &nested.columns)?;
                }
            }
        }
        Ok(())
    }
}

/// The LATERAL subquery for one JSON_TABLE.
fn json_table_subquery(
    json_expr: &Expr,
    json_path: &sqlparser::ast::ValueWithSpan,
    columns: &[JsonTableColumn],
) -> Result<Query> {
    let path = literal_text(json_path)
        .ok_or_else(|| SqlError::Unsupported("JSON_TABLE row paths must be literals".into()))?;
    let mut builder = Builder {
        columns: Vec::new(),
        joins: Vec::new(),
        next_level: 0,
    };
    builder.level("bicdb_jt0", columns)?;
    let sql = format!(
        "SELECT {} FROM jsonb_path_query(CAST({json_expr} AS jsonb), {}) WITH ORDINALITY AS bicdb_jt0(v, o) {}",
        builder.columns.join(", "),
        quote_literal(&path),
        builder.joins.join(" ")
    );
    let mut statements = crate::table_queries::parse(&sql)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    match statements.pop() {
        Some(Statement::Query(query)) => Ok(*query),
        _ => Err(SqlError::InvalidSql("invalid JSON_TABLE expansion".into())),
    }
}

struct JsonTableRewriter {
    error: Option<SqlError>,
}

impl VisitorMut for JsonTableRewriter {
    type Break = ();

    fn post_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        let TableFactor::JsonTable {
            json_expr,
            json_path,
            columns,
            alias,
        } = factor
        else {
            return ControlFlow::Continue(());
        };
        // Only a context that reads columns of earlier FROM items needs
        // LATERAL; a constant document is an ordinary derived table.
        let correlated = {
            use sqlparser::ast::{Visit, Visitor};
            struct Columns;
            impl Visitor for Columns {
                type Break = ();
                fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
                    if matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                }
            }
            Visit::visit(&*json_expr, &mut Columns).is_break()
        };
        match json_table_subquery(json_expr, json_path, columns) {
            Ok(subquery) => {
                let alias = alias.take().unwrap_or_else(|| TableAlias {
                    explicit: true,
                    name: Ident::new("json_table"),
                    columns: Vec::new(),
                    at: None,
                });
                *factor = TableFactor::Derived {
                    lateral: correlated,
                    subquery: Box::new(subquery),
                    alias: Some(alias),
                    sample: None,
                };
                ControlFlow::Continue(())
            }
            Err(error) => {
                self.error = Some(error);
                ControlFlow::Break(())
            }
        }
    }
}

/// Replace every JSON_TABLE of `statements` by its LATERAL subquery.
pub(crate) fn rewrite_json_tables(statements: &mut [Statement]) -> Result<()> {
    for statement in statements {
        let mut rewriter = JsonTableRewriter { error: None };
        let _ = statement.visit(&mut rewriter);
        if let Some(error) = rewriter.error {
            return Err(error);
        }
    }
    Ok(())
}
