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
    /// The PASSING variables (a jsonb object expression) or `NULL`.
    vars: String,
    /// Table-level `ERROR ON ERROR`: errors are raised instead of empty.
    raise_errors: bool,
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

fn named_column(
    item: &str,
    column: &JsonTableNamedColumn,
    vars: &str,
    raise_errors: bool,
) -> Result<(String, String)> {
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
    } else if raise_errors {
        spec.insert("on_error".into(), JsonValue::String("error".into()));
    }
    Ok((
        format!(
            "CAST({builtin}({item}, {}, {}, {vars}, {empty_default}, {error_default}) AS {kind})",
            quote_literal(&path),
            quote_literal(&JsonValue::Object(spec).to_string()),
        ),
        quote_ident(&column.name.value),
    ))
}

impl Builder {
    fn vars_object(&self) -> String {
        if self.vars == "NULL" {
            "'{}'::jsonb".to_string()
        } else {
            self.vars.clone()
        }
    }

    fn level(&mut self, alias: &str, columns: &[JsonTableColumn]) -> Result<()> {
        let item = format!("{alias}.v");
        let nested = columns
            .iter()
            .filter_map(|column| match column {
                JsonTableColumn::Nested(nested) => Some(nested),
                _ => None,
            })
            .collect::<Vec<_>>();
        // Sibling NESTED PATH clauses produce the union of their rows (each
        // with the other branches' columns NULL), one LEFT JOIN over a
        // branch-tagged UNION ALL.
        let union_alias = if nested.len() > 1 {
            if nested.iter().any(|branch| {
                branch
                    .columns
                    .iter()
                    .any(|column| matches!(column, JsonTableColumn::Nested(_)))
            }) {
                return Err(SqlError::Unsupported(
                    "JSON_TABLE with nested NESTED PATH under sibling NESTED PATH clauses is not supported"
                        .into(),
                ));
            }
            self.next_level += 1;
            let union_alias = format!("bicdb_jt{}", self.next_level);
            let branches = nested
                .iter()
                .enumerate()
                .map(|(index, branch)| {
                    let path = literal_text(&branch.path).ok_or_else(|| {
                        SqlError::Unsupported("JSON_TABLE nested paths must be literals".into())
                    })?;
                    Ok(format!(
                        "SELECT {} AS b, bicdb_jb.v AS v, bicdb_jb.o AS o FROM jsonb_path_query({item}, {}, {}, {}) WITH ORDINALITY AS bicdb_jb(v, o)",
                        index + 1,
                        quote_literal(&path),
                        self.vars_object(),
                        !self.raise_errors
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            self.joins.push(format!(
                "LEFT JOIN LATERAL ({}) AS {union_alias}(b, v, o) ON true",
                branches.join(" UNION ALL ")
            ));
            Some(union_alias)
        } else {
            None
        };
        let mut branch = 0usize;
        for column in columns {
            match column {
                JsonTableColumn::ForOrdinality(name) => self.columns.push(format!(
                    "CAST({alias}.o AS integer) AS {}",
                    quote_ident(&name.value)
                )),
                JsonTableColumn::Named(column) => {
                    let (expr, name) = named_column(&item, column, &self.vars, self.raise_errors)?;
                    self.columns.push(format!("{expr} AS {name}"));
                }
                JsonTableColumn::Nested(nested) => {
                    if let Some(union_alias) = &union_alias {
                        branch += 1;
                        let branch_item = format!("{union_alias}.v");
                        for column in &nested.columns {
                            let (expr, name) = match column {
                                JsonTableColumn::ForOrdinality(name) => (
                                    format!("CAST({union_alias}.o AS integer)"),
                                    quote_ident(&name.value),
                                ),
                                JsonTableColumn::Named(column) => named_column(
                                    &branch_item,
                                    column,
                                    &self.vars,
                                    self.raise_errors,
                                )?,
                                JsonTableColumn::Nested(_) => unreachable!("refused above"),
                            };
                            self.columns.push(format!(
                                "CASE WHEN {union_alias}.b = {branch} THEN {expr} END AS {name}"
                            ));
                        }
                        continue;
                    }
                    let path = literal_text(&nested.path).ok_or_else(|| {
                        SqlError::Unsupported("JSON_TABLE nested paths must be literals".into())
                    })?;
                    self.next_level += 1;
                    let nested_alias = format!("bicdb_jt{}", self.next_level);
                    self.joins.push(format!(
                        "LEFT JOIN LATERAL jsonb_path_query({item}, {}, {}, {}) WITH ORDINALITY AS {nested_alias}(v, o) ON true",
                        quote_literal(&path),
                        self.vars_object(),
                        !self.raise_errors
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
    // `bicdb_jt_args(context, passing, on_error)` carries the clauses the
    // parser does not know (see `rewrite_json_table_clauses`).
    let (json_expr, vars, raise_errors) = match json_expr {
        Expr::Function(function)
            if object_name(&function.name).is_ok_and(|name| name == JT_ARGS_FUNCTION) =>
        {
            let args = function_arg_list(function);
            let context = args
                .first()
                .map(|arg| (*arg).clone())
                .ok_or_else(|| SqlError::InvalidSql("invalid JSON_TABLE clauses".into()))?;
            let vars = args
                .get(1)
                .map(|arg| arg.to_string())
                .unwrap_or_else(|| "NULL".into());
            let raise = args.get(2).is_some_and(|arg| arg.to_string() == "'error'");
            (context, vars, raise)
        }
        other => (other.clone(), "NULL".to_string(), false),
    };
    let json_expr = &json_expr;
    let mut builder = Builder {
        columns: Vec::new(),
        joins: Vec::new(),
        next_level: 0,
        vars,
        raise_errors,
    };
    builder.level("bicdb_jt0", columns)?;
    let sql = format!(
        "SELECT {} FROM jsonb_path_query(CAST({json_expr} AS jsonb), {}, {}, {}) WITH ORDINALITY AS bicdb_jt0(v, o) {}",
        builder.columns.join(", "),
        quote_literal(&path),
        builder.vars_object(),
        !builder.raise_errors,
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

/// Function wrapping a JSON_TABLE context with its PASSING and table-level
/// ON ERROR clauses.
pub(crate) const JT_ARGS_FUNCTION: &str = "bicdb_jt_args";

/// `JSON_TABLE(ctx, path [AS name] [PASSING v AS n, ...] COLUMNS (...)
/// [{ERROR | EMPTY [ARRAY]} ON ERROR])`: the clauses sqlparser cannot read
/// move into `bicdb_jt_args(ctx, jsonb_build_object(...), 'error'|'empty')`.
pub(crate) fn rewrite_json_table_clauses(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("json_table")
        || !(lower.contains("passing") || lower.contains("on error") || lower.contains(" as "))
    {
        return None;
    }
    let mut output = String::new();
    let mut copied = 0usize;
    let mut search = 0usize;
    let mut changed = false;
    while let Some(found) = lower[search..].find("json_table") {
        let start = search + found;
        search = start + 10;
        let after = sql[start + 10..].trim_start();
        let open = start + 10 + (sql[start + 10..].len() - after.len());
        if !after.starts_with('(') {
            continue;
        }
        let close = crate::find_matching_paren_nested(sql, open)?;
        let inner = &sql[open + 1..close];
        let columns_at = crate::find_top_level_keyword(inner, "COLUMNS")?;
        let columns_open = inner[columns_at..].find('(')? + columns_at;
        let columns_close = crate::find_matching_paren_nested(inner, columns_open)?;
        let head = &inner[..columns_at];
        let tail = inner[columns_close + 1..].trim();
        let head_parts = crate::split_top_level_commas_nested(head);
        let (context, rest) = match head_parts.split_first() {
            Some((context, rest)) if !rest.is_empty() => {
                (context.trim().to_string(), rest.join(","))
            }
            _ => continue,
        };
        let mut path_text = rest.trim().to_string();
        let mut passing = Vec::new();
        if let Some(at) = crate::find_top_level_keyword(&path_text, "PASSING") {
            let clause = path_text[at + 7..].trim().to_string();
            path_text = path_text[..at].trim().to_string();
            for item in crate::split_top_level_commas_nested(&clause) {
                let as_at = crate::find_top_level_keyword(&item, "AS")?;
                let value = item[..as_at].trim();
                let raw = item[as_at + 2..].trim();
                let name = if raw.starts_with('"') {
                    raw.trim_matches('"').to_string()
                } else {
                    raw.to_ascii_lowercase()
                };
                passing.push(format!("'{}', {value}", name.replace('\'', "''")));
            }
        }
        // `path AS name` names the path; the name is not needed.
        if let Some(at) = crate::find_top_level_keyword(&path_text, "AS") {
            path_text = path_text[..at].trim().to_string();
        }
        let on_error = if tail.is_empty() {
            None
        } else {
            let upper = tail.to_ascii_uppercase();
            let words = upper.split_whitespace().collect::<Vec<_>>();
            match words.as_slice() {
                ["ERROR", "ON", "ERROR"] => Some("error"),
                ["EMPTY", "ON", "ERROR"] | ["EMPTY", "ARRAY", "ON", "ERROR"] => Some("empty"),
                _ => return None,
            }
        };
        if passing.is_empty() && on_error.is_none() && path_text == rest.trim() {
            continue;
        }
        let vars = if passing.is_empty() {
            "NULL".to_string()
        } else {
            format!("jsonb_build_object({})", passing.join(", "))
        };
        output.push_str(&sql[copied..open + 1]);
        output.push_str(&format!(
            "{JT_ARGS_FUNCTION}({context}, {vars}, '{}'), {path_text} {}",
            on_error.unwrap_or("empty"),
            &inner[columns_at..=columns_close]
        ));
        output.push(')');
        copied = close + 1;
        search = close + 1;
        changed = true;
    }
    if !changed {
        return None;
    }
    output.push_str(&sql[copied..]);
    Some(output)
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
