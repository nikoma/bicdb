//! EXPLAIN in PostgreSQL's node vocabulary and layout: the plan tree is
//! derived from the statement and the access paths BicDB's executor takes
//! (the same predictions as the native EXPLAIN), never invented. Nodes
//! BicDB does not have (Materialize, Bitmap scans, Gather, ...) never
//! appear; where BicDB runs a statement differently from PostgreSQL the plan
//! shows BicDB's way (a derived table is a Subquery Scan, a CTE is a CTE
//! Scan over its materialized result).

use super::*;
use crate::explain_format::{ExplainOptions, PlanNode, Prop};
#[allow(unused_imports)]
use crate::*;
use sqlparser::ast::{
    DataType, Distinct, FromTable, GroupByExpr, NamedWindowExpr, OnConflictAction, OnInsert,
    OrderByExpr, OrderByKind, SetOperator, SetQuantifier, TableObject, UnaryOperator, WindowSpec,
    WindowType,
};

/// A base relation (or derived table) in scope for column references.
#[derive(Clone, Debug)]
struct Rel {
    /// The reference name PostgreSQL prints (`t`, `t1`, `tenk1_1`).
    refname: String,
    /// Column names and their types.
    columns: Vec<(String, String)>,
}

/// One access path of a base-table scan, as the executor will take it.
#[derive(Clone, Debug)]
enum Access {
    Seq,
    /// An index scan: index name, the equality-prefix columns used as
    /// index conditions, and whether a range on the next column is used.
    Index {
        index: String,
        columns: Vec<String>,
        range: bool,
        backward: bool,
    },
}

pub(crate) struct ExplainBuilder<'e, 'db> {
    engine: &'e SqlEngine<'db>,
    verbose: bool,
    /// Rendering of date/time constants in the session's DateStyle.
    temporal: crate::datestyle_output::TemporalOutput,
    /// How often each relation name has been used (`tenk1`, `tenk1_1`).
    refnames: HashMap<String, usize>,
    /// Whether the statement has more than one range table entry, so
    /// upper-node expressions are qualified.
    multi_relation: bool,
    /// The scopes being built (innermost last).
    scopes: Vec<Vec<Rel>>,
    init_plans: usize,
    /// CTE names of the query being built.
    ctes: Vec<String>,
    /// An index the executor reads in ORDER BY order for the next scan
    /// (so the query needs no Sort): index name and direction.
    ordered_scan: Option<(String, bool)>,
    /// The index and condition the next scan probes per outer row (the
    /// inner side of an indexed nested loop).
    probe_scan: Option<(String, String)>,
}

fn count_relations(statement: &Statement) -> usize {
    use std::ops::ControlFlow;
    let mut count = 0usize;
    let _ = sqlparser::ast::visit_relations(statement, |_| {
        count += 1;
        ControlFlow::<()>::Continue(())
    });
    count
}

/// `quote_identifier`.
fn quote_ident(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_lowercase() || ch == '_')
        && name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
        && !crate::eval::is_pg_reserved_identifier(name);
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// PostgreSQL's `format_type` spelling of a BicDB column type.
fn type_display(pg_type: &str) -> String {
    let lower = pg_type.to_ascii_lowercase();
    if let Some(element) = lower.strip_suffix("[]") {
        return format!("{}[]", type_display(element));
    }
    crate::pg_types::pg_type_oid_by_name(&lower)
        .and_then(|oid| crate::pg_types::pg_format_type(oid, -1))
        .unwrap_or(lower)
}

fn canonical_type(pg_type: &str) -> String {
    crate::pg_types::pg_type_oid_by_name(&pg_type.to_ascii_lowercase())
        .and_then(crate::pg_types::pg_type_spec_by_oid)
        .map(|spec| spec.name.to_string())
        .unwrap_or_else(|| pg_type.to_ascii_lowercase())
}

/// The BicDB type name of a cast target.
fn data_type_name(data_type: &DataType) -> String {
    crate::pg_type_from_data_type(data_type)
        .map(|(name, _)| name)
        .unwrap_or_else(|_| data_type.to_string().to_ascii_lowercase())
}

/// `get_const_expr` for a literal of a known type.
fn const_text(text: &str, pg_type: &str) -> String {
    match canonical_type(pg_type).as_str() {
        "int4" if !text.starts_with('-') => text.to_string(),
        "int4" => format!("'{text}'::integer"),
        "numeric"
            if text.starts_with(|ch: char| ch.is_ascii_digit())
                && text.contains(['.', 'e', 'E']) =>
        {
            text.to_string()
        }
        "bool" => {
            if matches!(text, "t" | "true") {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        other => format!("'{}'::{}", text.replace('\'', "''"), type_display(other)),
    }
}

fn is_numeric_type(pg_type: &str) -> bool {
    matches!(
        canonical_type(pg_type).as_str(),
        "int2" | "int4" | "int8" | "numeric" | "float4" | "float8"
    )
}

fn binary_operator_text(op: &BinaryOperator) -> Option<&'static str> {
    Some(match op {
        BinaryOperator::Eq => "=",
        BinaryOperator::NotEq => "<>",
        BinaryOperator::Lt => "<",
        BinaryOperator::LtEq => "<=",
        BinaryOperator::Gt => ">",
        BinaryOperator::GtEq => ">=",
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        BinaryOperator::StringConcat => "||",
        BinaryOperator::PGRegexMatch => "~",
        BinaryOperator::PGRegexIMatch => "~*",
        BinaryOperator::PGRegexNotMatch => "!~",
        BinaryOperator::PGRegexNotIMatch => "!~*",
        BinaryOperator::PGLikeMatch => "~~",
        BinaryOperator::PGILikeMatch => "~~*",
        BinaryOperator::PGNotLikeMatch => "!~~",
        BinaryOperator::PGNotILikeMatch => "!~~*",
        BinaryOperator::AtArrow => "@>",
        BinaryOperator::ArrowAt => "<@",
        BinaryOperator::PGOverlap => "&&",
        _ => return None,
    })
}

fn conjuncts(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        Expr::Nested(inner)
            if matches!(
                inner.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ) =>
        {
            conjuncts(inner, out)
        }
        other => out.push(other.clone()),
    }
}

/// The column names an expression references, with their qualifiers.
fn referenced_columns(expr: &Expr, out: &mut Vec<(Option<String>, String)>) {
    use std::ops::ControlFlow;
    let _ = sqlparser::ast::visit_expressions(expr, |expr| {
        match expr {
            Expr::Identifier(ident) => out.push((None, ident.value.clone())),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => out.push((
                Some(parts[parts.len() - 2].value.clone()),
                parts[parts.len() - 1].value.clone(),
            )),
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
}

fn contains_subquery(expr: &Expr) -> bool {
    use std::ops::ControlFlow;
    sqlparser::ast::visit_expressions(expr, |expr| {
        if matches!(
            expr,
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. }
        ) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .is_break()
}

fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "string_agg"
            | "array_agg"
            | "bool_and"
            | "bool_or"
            | "every"
            | "json_agg"
            | "jsonb_agg"
            | "json_object_agg"
            | "jsonb_object_agg"
            | "stddev"
            | "stddev_pop"
            | "stddev_samp"
            | "variance"
            | "var_pop"
            | "var_samp"
            | "bit_and"
            | "bit_or"
            | "bit_xor"
            | "corr"
            | "covar_pop"
            | "covar_samp"
            | "regr_count"
            | "percentile_cont"
            | "percentile_disc"
            | "mode"
            | "xmlagg"
            | "any_value"
    )
}

/// Whether the select list or HAVING aggregates (outside window calls).
fn has_aggregates(select: &Select) -> bool {
    use std::ops::ControlFlow;
    let mut found = false;
    let mut check = |expr: &Expr| {
        let _ = sqlparser::ast::visit_expressions(expr, |expr| {
            if let Expr::Function(function) = expr {
                if function.over.is_none()
                    && is_aggregate_name(&function.name.to_string().to_ascii_lowercase())
                {
                    found = true;
                    return ControlFlow::Break(());
                }
            }
            if matches!(expr, Expr::Subquery(_) | Expr::Exists { .. }) {
                // Aggregates of a subquery belong to it.
                return ControlFlow::Continue(());
            }
            ControlFlow::<()>::Continue(())
        });
    };
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => check(expr),
            _ => {}
        }
    }
    if let Some(having) = &select.having {
        check(having);
    }
    found
}

fn window_specs(select: &Select) -> Vec<WindowSpec> {
    use std::ops::ControlFlow;
    let mut specs: Vec<WindowSpec> = Vec::new();
    for item in &select.projection {
        let (SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. }) = item else {
            continue;
        };
        let _ = sqlparser::ast::visit_expressions(expr, |expr| {
            if let Expr::Function(function) = expr {
                let spec = match &function.over {
                    Some(WindowType::WindowSpec(spec)) => Some(spec.clone()),
                    Some(WindowType::NamedWindow(name)) => select
                        .named_window
                        .iter()
                        .find(|window| window.0.value == name.value)
                        .and_then(|window| match &window.1 {
                            NamedWindowExpr::WindowSpec(spec) => Some(spec.clone()),
                            _ => None,
                        }),
                    None => None,
                };
                if let Some(spec) = spec {
                    if !specs.contains(&spec) {
                        specs.push(spec);
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    specs
}

/// Conjunct texts as one qualification: one clause, or their AND.
fn join_quals(parts: Vec<String>) -> Option<String> {
    match parts.len() {
        0 => None,
        1 => parts.into_iter().next(),
        _ => Some(format!("({})", parts.join(" AND "))),
    }
}

fn literal_text(value: &Value) -> Option<String> {
    match value {
        Value::SingleQuotedString(text)
        | Value::EscapedStringLiteral(text)
        | Value::DollarQuotedString(sqlparser::ast::DollarQuotedString { value: text, .. }) => {
            Some(text.clone())
        }
        _ => None,
    }
}

impl<'e, 'db> ExplainBuilder<'e, 'db> {
    pub(crate) fn new(
        engine: &'e SqlEngine<'db>,
        options: &ExplainOptions,
        statement: &Statement,
    ) -> Self {
        Self {
            engine,
            verbose: options.verbose,
            temporal: crate::datestyle_output::TemporalOutput::from_gucs(&engine.session_gucs),
            refnames: HashMap::new(),
            multi_relation: count_relations(statement) > 1,
            scopes: Vec::new(),
            init_plans: 0,
            ctes: Vec::new(),
            ordered_scan: None,
            probe_scan: None,
        }
    }

    // ----- expressions -------------------------------------------------

    fn column_type(&self, qualifier: Option<&str>, column: &str) -> Option<String> {
        for scope in self.scopes.iter().rev() {
            for rel in scope.iter() {
                if qualifier.is_some_and(|qualifier| qualifier != rel.refname) {
                    continue;
                }
                if let Some((_, pg_type)) = rel.columns.iter().find(|(name, _)| name == column) {
                    return Some(pg_type.clone());
                }
            }
        }
        None
    }

    fn column_rel(&self, column: &str) -> Option<String> {
        for scope in self.scopes.iter().rev() {
            for rel in scope.iter() {
                if rel.columns.iter().any(|(name, _)| name == column) {
                    return Some(rel.refname.clone());
                }
            }
        }
        None
    }

    /// The type an expression has, when it is evident.
    fn expr_type(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => self.column_type(None, &ident.value),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => self.column_type(
                Some(&parts[parts.len() - 2].value),
                &parts[parts.len() - 1].value,
            ),
            Expr::Nested(inner) => self.expr_type(inner),
            Expr::Cast { data_type, .. } => Some(data_type_name(data_type)),
            Expr::TypedString(typed) => Some(data_type_name(&typed.data_type)),
            Expr::Value(value) => match &value.value {
                Value::Number(text, _) => Some(
                    if text.contains(['.', 'e', 'E']) {
                        "numeric"
                    } else if text.parse::<i32>().is_ok() {
                        "int4"
                    } else if text.parse::<i64>().is_ok() {
                        "int8"
                    } else {
                        "numeric"
                    }
                    .to_string(),
                ),
                Value::Boolean(_) => Some("bool".to_string()),
                _ => None,
            },
            _ => None,
        }
    }

    /// A column reference: qualified when `prefix`.
    fn column_text(&self, qualifier: Option<&str>, column: &str, prefix: bool) -> String {
        if !prefix {
            return quote_ident(column);
        }
        let rel = qualifier
            .map(str::to_string)
            .or_else(|| self.column_rel(column));
        match rel {
            Some(rel) => format!("{}.{}", quote_ident(&rel), quote_ident(column)),
            None => quote_ident(column),
        }
    }

    /// A constant compared with (or assigned to) a value of `context_type`.
    fn typed_const(&self, expr: &Expr, context_type: Option<&str>) -> Option<String> {
        let Expr::Value(value) = expr else {
            return None;
        };
        let context = context_type.map(canonical_type);
        match &value.value {
            Value::Number(text, _) => {
                let own = self.expr_type(expr)?;
                match context.as_deref() {
                    Some("numeric") if own != "numeric" => Some(const_text(text, "numeric")),
                    Some(float @ ("float8" | "float4")) => Some(const_text(text, float)),
                    _ => Some(const_text(text, &own)),
                }
            }
            Value::Null => Some(match context {
                Some(context) => format!("NULL::{}", type_display(&context)),
                None => "NULL".to_string(),
            }),
            Value::Boolean(flag) => Some(flag.to_string()),
            other => {
                let text = literal_text(other)?;
                let pg_type = match context.as_deref() {
                    Some("varchar" | "name") | None => "text".to_string(),
                    Some(other) => other.to_string(),
                };
                let canonical =
                    crate::eval::cast_value_to_pg_type(SqlValue::String(text.clone()), &pg_type)
                        .map(|value| value.to_cell())
                        .unwrap_or(text);
                let oid = crate::pg_types::pg_type_oid_by_name(&pg_type).unwrap_or(25);
                let styled = self.temporal.render(oid, &canonical).unwrap_or(canonical);
                Some(const_text(&styled, &pg_type))
            }
        }
    }

    /// `deparse_expression` of a qualification or key, with PostgreSQL's
    /// parenthesization (every operator clause is parenthesized).
    fn expr(&mut self, expr: &Expr, prefix: bool) -> String {
        match expr {
            Expr::Identifier(ident) => self.column_text(None, &ident.value, prefix),
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => self.column_text(
                Some(&parts[parts.len() - 2].value),
                &parts[parts.len() - 1].value,
                prefix,
            ),
            Expr::Nested(inner) => self.expr(inner, prefix),
            Expr::Value(_) => self
                .typed_const(expr, None)
                .unwrap_or_else(|| expr.to_string()),
            Expr::TypedString(typed) => {
                let pg_type = data_type_name(&typed.data_type);
                match literal_text(&typed.value.value) {
                    Some(text) => {
                        let value = Expr::value(Value::SingleQuotedString(text));
                        self.typed_const(&value, Some(&pg_type))
                            .unwrap_or_else(|| expr.to_string())
                    }
                    None => expr.to_string(),
                }
            }
            Expr::Cast {
                expr: inner,
                data_type,
                ..
            } => {
                let pg_type = data_type_name(data_type);
                if matches!(inner.as_ref(), Expr::Value(_)) {
                    if let Some(text) = self.typed_const(inner, Some(&pg_type)) {
                        return text;
                    }
                }
                format!("({})::{}", self.expr(inner, prefix), type_display(&pg_type))
            }
            Expr::BinaryOp {
                left,
                op: op @ (BinaryOperator::And | BinaryOperator::Or),
                right,
            } => {
                let word = if matches!(op, BinaryOperator::And) {
                    "AND"
                } else {
                    "OR"
                };
                let mut items = Vec::new();
                let mut flatten = |expr: &Expr, items: &mut Vec<Expr>| {
                    fn walk(expr: &Expr, op: &BinaryOperator, items: &mut Vec<Expr>) {
                        match expr {
                            Expr::BinaryOp {
                                left,
                                op: inner,
                                right,
                            } if inner == op => {
                                walk(left, op, items);
                                walk(right, op, items);
                            }
                            Expr::Nested(inner) if matches!(inner.as_ref(), Expr::BinaryOp { op: inner_op, .. } if inner_op == op) => {
                                walk(inner, op, items)
                            }
                            other => items.push(other.clone()),
                        }
                    }
                    walk(expr, op, items);
                };
                flatten(left, &mut items);
                flatten(right, &mut items);
                let parts = items
                    .iter()
                    .map(|item| self.expr(item, prefix))
                    .collect::<Vec<_>>();
                format!("({})", parts.join(&format!(" {word} ")))
            }
            Expr::BinaryOp { left, op, right } => {
                let Some(op_text) = binary_operator_text(op) else {
                    return format!("({expr})");
                };
                let comparison = matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                );
                let left_type = self.expr_type(left);
                let right_type = self.expr_type(right);
                let left_text = self
                    .typed_const(left, right_type.as_deref())
                    .unwrap_or_else(|| {
                        self.operand(left, right_type.as_deref(), comparison, prefix)
                    });
                let right_text = self
                    .typed_const(right, left_type.as_deref())
                    .unwrap_or_else(|| {
                        self.operand(right, left_type.as_deref(), comparison, prefix)
                    });
                format!("({left_text} {op_text} {right_text})")
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: inner,
            } => format!("(NOT {})", self.expr(inner, prefix)),
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } if matches!(inner.as_ref(), Expr::Value(_)) => {
                let negated = Expr::value(Value::Number(
                    format!("-{}", inner.to_string().trim_start_matches('+')),
                    false,
                ));
                self.expr(&negated, prefix)
            }
            Expr::IsNull(inner) => format!("({} IS NULL)", self.expr(inner, prefix)),
            Expr::IsNotNull(inner) => format!("({} IS NOT NULL)", self.expr(inner, prefix)),
            Expr::IsTrue(inner) => format!("({} IS TRUE)", self.expr(inner, prefix)),
            Expr::IsFalse(inner) => format!("({} IS FALSE)", self.expr(inner, prefix)),
            Expr::IsNotTrue(inner) => format!("({} IS NOT TRUE)", self.expr(inner, prefix)),
            Expr::IsNotFalse(inner) => format!("({} IS NOT FALSE)", self.expr(inner, prefix)),
            Expr::Between {
                expr: inner,
                negated,
                low,
                high,
            } => {
                let (low_op, high_op, join) = if *negated {
                    (BinaryOperator::Lt, BinaryOperator::Gt, BinaryOperator::Or)
                } else {
                    (
                        BinaryOperator::GtEq,
                        BinaryOperator::LtEq,
                        BinaryOperator::And,
                    )
                };
                let expanded = Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: inner.clone(),
                        op: low_op,
                        right: low.clone(),
                    }),
                    op: join,
                    right: Box::new(Expr::BinaryOp {
                        left: inner.clone(),
                        op: high_op,
                        right: high.clone(),
                    }),
                };
                self.expr(&expanded, prefix)
            }
            Expr::Like {
                negated,
                expr: inner,
                pattern,
                escape_char: None,
                any: false,
            }
            | Expr::ILike {
                negated,
                expr: inner,
                pattern,
                escape_char: None,
                any: false,
            } => {
                let insensitive = matches!(expr, Expr::ILike { .. });
                let op = match (negated, insensitive) {
                    (false, false) => "~~",
                    (true, false) => "!~~",
                    (false, true) => "~~*",
                    (true, true) => "!~~*",
                };
                let target_type = self.expr_type(inner);
                let left = self.operand(inner, Some("text"), true, prefix);
                let pattern_type = match target_type.as_deref().map(canonical_type).as_deref() {
                    Some("bpchar") | Some("name") | None | Some("text") | Some("varchar") => "text",
                    Some(_) => "text",
                };
                let right = self
                    .typed_const(pattern, Some(pattern_type))
                    .unwrap_or_else(|| self.expr(pattern, prefix));
                format!("({left} {op} {right})")
            }
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => {
                let element_type = self
                    .expr_type(inner)
                    .or_else(|| list.iter().find_map(|item| self.expr_type(item)))
                    .unwrap_or_else(|| "text".to_string());
                let constants = list.iter().all(|item| matches!(item, Expr::Value(_)));
                let left = self.expr(inner, prefix);
                let op = if *negated { "<>" } else { "=" };
                let quantifier = if *negated { "ALL" } else { "ANY" };
                if constants {
                    let items = list
                        .iter()
                        .map(|item| match item {
                            Expr::Value(value) => match &value.value {
                                Value::Number(text, _) => text.clone(),
                                Value::Null => "NULL".to_string(),
                                other => {
                                    let text = literal_text(other).unwrap_or_default();
                                    crate::eval::cast_value_to_pg_type(
                                        SqlValue::String(text.clone()),
                                        &element_type,
                                    )
                                    .map(|value| value.to_cell())
                                    .unwrap_or(text)
                                }
                            },
                            _ => String::new(),
                        })
                        .map(|text| {
                            if text.is_empty() || text.contains([',', '{', '}', '"', '\\', ' ']) {
                                format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
                            } else {
                                text
                            }
                        })
                        .collect::<Vec<_>>();
                    format!(
                        "({left} {op} {quantifier} ('{{{}}}'::{}[]))",
                        items.join(",").replace('\'', "''"),
                        type_display(&element_type)
                    )
                } else {
                    let items = list
                        .iter()
                        .map(|item| self.expr(item, prefix))
                        .collect::<Vec<_>>();
                    format!("({left} {op} {quantifier} (ARRAY[{}]))", items.join(", "))
                }
            }
            Expr::Function(function) => self.function(function, prefix),
            other => other.to_string(),
        }
    }

    /// An operand of an operator: a column compared with a column of
    /// another type is shown with its implicit cast.
    fn operand(
        &mut self,
        expr: &Expr,
        other_type: Option<&str>,
        comparison: bool,
        prefix: bool,
    ) -> String {
        let text = self.expr(expr, prefix);
        let own = self.expr_type(expr).map(|pg_type| canonical_type(&pg_type));
        let other = other_type.map(canonical_type);
        match (own.as_deref(), other.as_deref()) {
            (Some("varchar"), Some("text")) | (Some("varchar"), None) if comparison => {
                format!("({text})::text")
            }
            (Some(own @ ("int2" | "int4" | "int8")), Some("numeric"))
                if comparison && own != "numeric" =>
            {
                format!("({text})::numeric")
            }
            _ => text,
        }
    }

    fn function(&mut self, function: &Function, prefix: bool) -> String {
        let name = function.name.to_string().to_ascii_lowercase();
        let args = match &function.args {
            FunctionArguments::List(list) => list
                .args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => self.expr(expr, prefix),
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => "*".to_string(),
                    other => other.to_string(),
                })
                .collect::<Vec<_>>(),
            FunctionArguments::None => return name,
            other => vec![other.to_string()],
        };
        format!("{name}({})", args.join(", "))
    }

    /// The `Filter:` text of a conjunct list: one clause, or their AND.
    fn qual(&mut self, quals: &[Expr], prefix: bool) -> Option<String> {
        let parts = quals
            .iter()
            .map(|qual| self.expr(qual, prefix))
            .collect::<Vec<_>>();
        join_quals(parts)
    }

    /// A sort or group key: columns bare, expressions parenthesized.
    fn key(&mut self, expr: &Expr) -> String {
        let prefix = self.multi_relation || self.verbose;
        let text = self.expr(expr, prefix);
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => text,
            Expr::Nested(inner)
                if matches!(
                    inner.as_ref(),
                    Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                ) =>
            {
                text
            }
            _ if text.starts_with('(') && text.ends_with(')') => text,
            _ => format!("({text})"),
        }
    }

    // ----- relations ------------------------------------------------------

    fn refname(&mut self, relation: &str, alias: Option<&str>) -> String {
        if let Some(alias) = alias {
            *self.refnames.entry(alias.to_string()).or_default() += 1;
            return alias.to_string();
        }
        let count = self.refnames.entry(relation.to_string()).or_default();
        *count += 1;
        if *count == 1 {
            relation.to_string()
        } else {
            format!("{relation}_{}", *count - 1)
        }
    }

    fn table_columns(&self, collection: &str) -> Result<Vec<(String, String)>> {
        if let Some(schema) = load_schema(self.engine.db_ref(), collection)? {
            return Ok(schema
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .map(|column| (column.name.clone(), column.pg_type.clone()))
                .collect());
        }
        Ok(Vec::new())
    }

    /// The access path the executor takes for `quals` on the table.
    fn access(
        &self,
        name: &ObjectName,
        collection: &str,
        alias: &str,
        quals: &[Expr],
    ) -> Result<Access> {
        if quals.is_empty() || is_virtual_table(collection) {
            return Ok(Access::Seq);
        }
        let mut predicate = quals[0].clone();
        for qual in &quals[1..] {
            predicate = Expr::BinaryOp {
                left: Box::new(predicate),
                op: BinaryOperator::And,
                right: Box::new(qual.clone()),
            };
        }
        let sql = format!(
            "SELECT * FROM {name} AS {} WHERE {predicate}",
            quote_ident(alias)
        );
        let Ok(mut statements) =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, &sql)
        else {
            return Ok(Access::Seq);
        };
        let Some(Statement::Query(query)) = statements.pop() else {
            return Ok(Access::Seq);
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(Access::Seq);
        };
        let plan = self.engine.plan_query(collection, alias, select, &query)?;
        let executed = self.engine.predicted_executed_access(
            &select.from[0],
            select,
            &query,
            collection,
            alias,
        )?;
        let signature = match executed {
            Some(executed) => executed.signature,
            None => crate::plan_diagnostics::signature_of(&plan.kind),
        };
        let schema = load_schema(self.engine.db_ref(), collection)?;
        let index_columns = |index: &str| -> Vec<String> {
            self.engine
                .db_ref()
                .index_definitions()
                .into_iter()
                .find(|definition| definition.collection == collection && definition.name == index)
                .map(|definition| {
                    definition
                        .fields
                        .iter()
                        .map(|field| match field {
                            IndexField::MetadataPath(path) => path.join("."),
                            IndexField::Id => schema
                                .as_ref()
                                .and_then(|schema| {
                                    schema
                                        .columns
                                        .iter()
                                        .find(|column| column.primary_key)
                                        .map(|column| column.name.clone())
                                })
                                .unwrap_or_else(|| "id".to_string()),
                            _ => String::new(),
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        Ok(match signature.kind {
            "IndexLookup" | "IndexPrefixOrderedScan" | "IndexRange" => {
                let index = signature.index.clone().unwrap_or_default();
                let columns = index_columns(&index);
                let range = signature.kind == "IndexRange";
                let used = if range {
                    signature.prefix_len.max(1)
                } else {
                    signature.prefix_len
                };
                Access::Index {
                    index,
                    columns: columns.into_iter().take(used).collect(),
                    range,
                    backward: false,
                }
            }
            "PrimaryKeyLookup" | "PrimaryKeyPrefixLookup" | "PrimaryKeyInLookup" => {
                let Some(schema) = schema.as_ref() else {
                    return Ok(Access::Seq);
                };
                let index = schema
                    .primary_key_name
                    .clone()
                    .unwrap_or_else(|| format!("{}_pkey", schema.name));
                Access::Index {
                    index,
                    columns: schema
                        .columns
                        .iter()
                        .filter(|column| column.primary_key)
                        .map(|column| column.name.clone())
                        .collect(),
                    range: false,
                    backward: false,
                }
            }
            _ => Access::Seq,
        })
    }

    fn index_columns(&self, collection: &str, index: &str) -> Vec<String> {
        self.engine
            .db_ref()
            .index_definitions()
            .into_iter()
            .find(|definition| definition.collection == collection && definition.name == index)
            .map(|definition| {
                definition
                    .fields
                    .iter()
                    .map(|field| match field {
                        IndexField::MetadataPath(path) => path.join("."),
                        _ => String::new(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The index a single-table query reads in its ORDER BY order, when the
    /// executor takes that path instead of sorting.
    fn ordered_index_scan(&self, query: &Query) -> Result<Option<(String, bool)>> {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(None);
        };
        let [from] = select.from.as_slice() else {
            return Ok(None);
        };
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = &from.relation
        else {
            return Ok(None);
        };
        if !from.joins.is_empty()
            || select.distinct.is_some()
            || has_aggregates(select)
            || !matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if exprs.is_empty())
            || !window_specs(select).is_empty()
        {
            return Ok(None);
        }
        let collection = relation_name(name)?;
        if is_virtual_table(&collection) || load_view(self.engine.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let alias = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| relation_default_alias(name));
        let plan = self.engine.plan_query(&collection, &alias, select, query)?;
        Ok(match plan.kind {
            PlanKind::OrderedIndexScan {
                index_name,
                descending,
                ..
            }
            | PlanKind::IndexPrefixOrderedScan {
                index_name,
                descending,
                ..
            } => Some((index_name, descending)),
            _ => None,
        })
    }

    /// Which conjuncts an index scan applies as index conditions.
    fn index_conditions(
        &self,
        columns: &[String],
        range: bool,
        quals: &[Expr],
    ) -> (Vec<Expr>, Vec<Expr>) {
        let mut index = Vec::new();
        let mut filter = Vec::new();
        for qual in quals {
            let column_of = |expr: &Expr| match expr {
                Expr::Identifier(ident) => Some(ident.value.clone()),
                Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.clone()),
                _ => None,
            };
            let indexed = match qual {
                Expr::BinaryOp { left, op, right } => {
                    let column = column_of(left).or_else(|| column_of(right));
                    let constant = matches!(
                        left.as_ref(),
                        Expr::Value(_) | Expr::Cast { .. } | Expr::TypedString(_)
                    ) || matches!(
                        right.as_ref(),
                        Expr::Value(_) | Expr::Cast { .. } | Expr::TypedString(_)
                    );
                    column.is_some_and(|column| {
                        let position = columns.iter().position(|indexed| indexed == &column);
                        constant
                            && match op {
                                BinaryOperator::Eq => position.is_some(),
                                BinaryOperator::Lt
                                | BinaryOperator::LtEq
                                | BinaryOperator::Gt
                                | BinaryOperator::GtEq => {
                                    range && position == Some(columns.len().saturating_sub(1))
                                }
                                _ => false,
                            }
                    })
                }
                Expr::InList {
                    expr,
                    negated: false,
                    ..
                } => column_of(expr).is_some_and(|column| columns.first() == Some(&column)),
                Expr::Between {
                    expr,
                    negated: false,
                    ..
                } => range && column_of(expr).is_some_and(|column| columns.last() == Some(&column)),
                _ => false,
            };
            if indexed {
                index.push(qual.clone());
            } else {
                filter.push(qual.clone());
            }
        }
        (index, filter)
    }

    /// A base-table scan with its qualifications.
    fn table_scan(
        &mut self,
        name: &ObjectName,
        alias: Option<&TableAlias>,
        quals: Vec<Expr>,
    ) -> Result<PlanNode> {
        let collection = relation_name(name)?;
        let relname = relation_default_alias(name);
        if let Some(view) = load_view(self.engine.db_ref(), &collection)? {
            if !view.materialized {
                return self.view_scan(&view, &relname, alias, quals);
            }
        }
        let refname = self.refname(&relname, alias.map(|alias| alias.name.value.as_str()));
        let columns = self.table_columns(&collection)?;
        self.scopes.last_mut().expect("scope").push(Rel {
            refname: refname.clone(),
            columns,
        });
        let probe = self.probe_scan.take();
        let access = match self.ordered_scan.take() {
            Some((index, backward)) => {
                let columns = self.index_columns(&collection, &index);
                Access::Index {
                    index,
                    columns,
                    range: false,
                    backward,
                }
            }
            None if probe.is_some() => Access::Index {
                index: probe
                    .as_ref()
                    .map(|(index, _)| index.clone())
                    .unwrap_or_default(),
                columns: Vec::new(),
                range: false,
                backward: false,
            },
            None => self.access(name, &collection, &refname, &quals)?,
        };
        let prefix = self.verbose;
        let shown_relation = if self.verbose {
            format!("public.{}", quote_ident(&relname))
        } else {
            quote_ident(&relname)
        };
        let alias_text = if refname != relname {
            format!(" {}", quote_ident(&refname))
        } else {
            String::new()
        };
        let mut node = match &access {
            Access::Seq => {
                let mut node = PlanNode::new("Seq Scan");
                node.target = format!(" on {shown_relation}{alias_text}");
                node = node.head("Relation Name", Prop::Text(relname.clone()));
                node.head("Alias", Prop::Text(refname.clone()))
            }
            Access::Index {
                index, backward, ..
            } => {
                let name = if *backward {
                    "Index Scan Backward"
                } else {
                    "Index Scan"
                };
                let mut node = PlanNode::new("Index Scan").text_name(name);
                node.target = format!(
                    " using {} on {shown_relation}{alias_text}",
                    quote_ident(index)
                );
                node.head(
                    "Scan Direction",
                    Prop::Text(if *backward { "Backward" } else { "Forward" }.to_string()),
                )
                .head("Index Name", Prop::Text(index.clone()))
                .head("Relation Name", Prop::Text(relname.clone()))
                .head("Alias", Prop::Text(refname.clone()))
            }
        };
        let (index_quals, filter) = match &access {
            Access::Seq => (Vec::new(), quals),
            Access::Index { columns, range, .. } => self.index_conditions(columns, *range, &quals),
        };
        if let Some((_, condition)) = probe {
            node = node.prop("Index Cond", condition);
        } else if let Some(text) = self.qual(&index_quals, prefix) {
            node = node.prop("Index Cond", text);
        }
        node = self.with_filter(node, &filter, prefix)?;
        Ok(node)
    }

    /// A Filter with the subplans its subqueries run.
    fn with_filter(
        &mut self,
        mut node: PlanNode,
        quals: &[Expr],
        prefix: bool,
    ) -> Result<PlanNode> {
        let mut plain = Vec::new();
        for qual in quals {
            if contains_subquery(qual) {
                let (text, subplans) = self.subquery_qual(qual, prefix)?;
                plain.push(text);
                for (name, plan) in subplans {
                    node = node.subplan(
                        if name.starts_with("InitPlan") {
                            "InitPlan"
                        } else {
                            "SubPlan"
                        },
                        name,
                        plan,
                    );
                }
            } else {
                plain.push(self.expr(qual, prefix));
            }
        }
        if let Some(text) = join_quals(plain) {
            node = node.prop("Filter", text);
        }
        // Subplans are listed before the node's own children in PostgreSQL
        // only for InitPlans; SubPlans come last. Keep insertion order.
        Ok(node)
    }

    /// A qualification with subqueries: the subqueries become InitPlans
    /// (uncorrelated scalar subqueries) or SubPlans.
    fn subquery_qual(
        &mut self,
        qual: &Expr,
        prefix: bool,
    ) -> Result<(String, Vec<(String, PlanNode)>)> {
        let mut subplans = Vec::new();
        let text = match qual {
            Expr::Exists { subquery, negated } => {
                self.init_plans += 1;
                let name = format!("SubPlan {}", self.init_plans);
                let plan = self.nested_query(subquery)?;
                subplans.push((name.clone(), plan));
                if *negated {
                    format!("(NOT EXISTS({name}))")
                } else {
                    format!("EXISTS({name})")
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                self.init_plans += 1;
                let name = format!("SubPlan {}", self.init_plans);
                let left = self.expr(expr, prefix);
                let plan = self.nested_query(subquery)?;
                subplans.push((name.clone(), plan));
                if *negated {
                    format!("(NOT (ANY ({left} = ({name}).col1)))")
                } else {
                    format!("(ANY ({left} = ({name}).col1))")
                }
            }
            Expr::BinaryOp { left, op, right } if binary_operator_text(op).is_some() => {
                let op_text = binary_operator_text(op).expect("checked");
                let mut side = |expr: &Expr, this: &mut Self| -> Result<String> {
                    match expr {
                        Expr::Subquery(subquery) => {
                            this.init_plans += 1;
                            let name = format!("InitPlan {}", this.init_plans);
                            let plan = this.nested_query(subquery)?;
                            subplans.push((name.clone(), plan));
                            Ok(format!("({name}).col1"))
                        }
                        other => Ok(this.expr(other, prefix)),
                    }
                };
                let left = side(left, self)?;
                let right = side(right, self)?;
                format!("({left} {op_text} {right})")
            }
            other => self.expr(other, prefix),
        };
        Ok((text, subplans))
    }

    fn nested_query(&mut self, query: &Query) -> Result<PlanNode> {
        let ctes = std::mem::take(&mut self.ctes);
        let plan = self.query(query);
        self.ctes = ctes;
        plan
    }

    fn view_scan(
        &mut self,
        view: &ViewSchema,
        relname: &str,
        alias: Option<&TableAlias>,
        quals: Vec<Expr>,
    ) -> Result<PlanNode> {
        let statements = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::PostgreSqlDialect {},
            &view.query_sql,
        )
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
        let Some(Statement::Query(query)) = statements.into_iter().next() else {
            return Err(SqlError::Unsupported(
                "EXPLAIN of this view definition is not supported".to_string(),
            ));
        };
        let refname = self.refname(relname, alias.map(|alias| alias.name.value.as_str()));
        let child = self.nested_query(&query)?;
        let columns = view
            .columns
            .iter()
            .map(|column| (column.name.clone(), column.pg_type.clone()))
            .collect();
        self.scopes.last_mut().expect("scope").push(Rel {
            refname: refname.clone(),
            columns,
        });
        let mut node = PlanNode::new("Subquery Scan").head("Alias", Prop::Text(refname.clone()));
        node.target = format!(" on {}", quote_ident(&refname));
        let prefix = true;
        node = self.with_filter(node, &quals, prefix)?;
        Ok(node.child("Subquery", child))
    }

    fn values_scan(&mut self, rows: usize) -> PlanNode {
        if rows <= 1 {
            return PlanNode::new("Result");
        }
        let refname = self.refname("*VALUES*", None);
        let mut node = PlanNode::new("Values Scan").head("Alias", Prop::Text(refname.clone()));
        node.target = format!(" on {}", quote_ident(&refname));
        node
    }

    /// One FROM item; `quals` are the conjuncts that reference only it.
    fn from_factor(&mut self, factor: &TableFactor, quals: Vec<Expr>) -> Result<PlanNode> {
        match factor {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                if let Some(args) = args {
                    let relname = relation_default_alias(name);
                    let refname = self.refname(
                        &relname,
                        alias.as_ref().map(|alias| alias.name.value.as_str()),
                    );
                    self.scopes.last_mut().expect("scope").push(Rel {
                        refname: refname.clone(),
                        columns: Vec::new(),
                    });
                    let mut node = PlanNode::new("Function Scan")
                        .head("Function Name", Prop::Text(relname.clone()))
                        .head("Alias", Prop::Text(refname.clone()));
                    node.target = if refname == relname {
                        format!(" on {}", quote_ident(&relname))
                    } else {
                        format!(" on {} {}", quote_ident(&relname), quote_ident(&refname))
                    };
                    if self.verbose {
                        let call = format!(
                            "{}({})",
                            relname,
                            args.args
                                .iter()
                                .map(|arg| arg.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                        node = node.prop("Function Call", call);
                    }
                    let prefix = self.verbose;
                    return self.with_filter(node, &quals, prefix);
                }
                let simple = name.0.len() == 1;
                let cte = simple
                    .then(|| relation_default_alias(name))
                    .filter(|cte| self.ctes.contains(cte));
                if let Some(cte) = cte {
                    let refname =
                        self.refname(&cte, alias.as_ref().map(|alias| alias.name.value.as_str()));
                    self.scopes.last_mut().expect("scope").push(Rel {
                        refname: refname.clone(),
                        columns: Vec::new(),
                    });
                    let mut node = PlanNode::new("CTE Scan")
                        .head("CTE Name", Prop::Text(cte.clone()))
                        .head("Alias", Prop::Text(refname.clone()));
                    node.target = if refname == cte {
                        format!(" on {}", quote_ident(&cte))
                    } else {
                        format!(" on {} {}", quote_ident(&cte), quote_ident(&refname))
                    };
                    let prefix = self.verbose;
                    return self.with_filter(node, &quals, prefix);
                }
                self.table_scan(name, alias.as_ref(), quals)
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let child = self.nested_query(subquery)?;
                let refname = alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| "unnamed_subquery".to_string());
                *self.refnames.entry(refname.clone()).or_default() += 1;
                self.scopes.last_mut().expect("scope").push(Rel {
                    refname: refname.clone(),
                    columns: Vec::new(),
                });
                let mut node =
                    PlanNode::new("Subquery Scan").head("Alias", Prop::Text(refname.clone()));
                node.target = format!(" on {}", quote_ident(&refname));
                node = self.with_filter(node, &quals, true)?;
                Ok(node.child("Subquery", child))
            }
            TableFactor::Function {
                name, args, alias, ..
            } => {
                let relname = name.to_string().to_ascii_lowercase();
                let refname = self.refname(
                    &relname,
                    alias.as_ref().map(|alias| alias.name.value.as_str()),
                );
                self.scopes.last_mut().expect("scope").push(Rel {
                    refname: refname.clone(),
                    columns: Vec::new(),
                });
                let mut node = PlanNode::new("Function Scan")
                    .head("Function Name", Prop::Text(relname.clone()))
                    .head("Alias", Prop::Text(refname.clone()));
                node.target = format!(" on {} {}", quote_ident(&relname), quote_ident(&refname));
                if self.verbose {
                    let call = format!(
                        "{relname}({})",
                        args.iter()
                            .map(|arg| arg.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    node = node.prop("Function Call", call);
                }
                let prefix = self.verbose;
                self.with_filter(node, &quals, prefix)
            }
            TableFactor::UNNEST { alias, .. } => {
                let refname = self.refname(
                    "unnest",
                    alias.as_ref().map(|alias| alias.name.value.as_str()),
                );
                self.scopes.last_mut().expect("scope").push(Rel {
                    refname: refname.clone(),
                    columns: Vec::new(),
                });
                let mut node = PlanNode::new("Function Scan")
                    .head("Function Name", Prop::Text("unnest".to_string()))
                    .head("Alias", Prop::Text(refname.clone()));
                node.target = if refname == "unnest" {
                    " on unnest".to_string()
                } else {
                    format!(" on unnest {}", quote_ident(&refname))
                };
                let prefix = self.verbose;
                self.with_filter(node, &quals, prefix)
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.join_tree(table_with_joins, quals),
            other => Err(SqlError::Unsupported(format!(
                "EXPLAIN of FROM item {other} is not supported"
            ))),
        }
    }

    /// The names a FROM item makes visible (for assigning conjuncts).
    fn factor_names(factor: &TableFactor) -> Vec<String> {
        match factor {
            TableFactor::Table { name, alias, .. } => vec![alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_else(|| relation_default_alias(name))],
            TableFactor::Derived { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::UNNEST { alias, .. } => alias
                .as_ref()
                .map(|alias| vec![alias.name.value.clone()])
                .unwrap_or_default(),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => std::iter::once(&table_with_joins.relation)
                .chain(table_with_joins.joins.iter().map(|join| &join.relation))
                .flat_map(Self::factor_names)
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Whether a conjunct references only the named FROM items (unqualified
    /// columns are resolved against the item's table columns).
    fn qual_belongs(&self, qual: &Expr, names: &[String], columns: &[String]) -> bool {
        if contains_subquery(qual) {
            return false;
        }
        let mut references = Vec::new();
        referenced_columns(qual, &mut references);
        references
            .iter()
            .all(|(qualifier, column)| match qualifier {
                Some(qualifier) => names.contains(qualifier),
                None => columns.contains(column),
            })
    }

    fn factor_columns(&self, factor: &TableFactor) -> Vec<String> {
        match factor {
            TableFactor::Table {
                name, args: None, ..
            } => relation_name(name)
                .ok()
                .and_then(|collection| self.table_columns(&collection).ok())
                .unwrap_or_default()
                .into_iter()
                .map(|(name, _)| name)
                .collect(),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => std::iter::once(&table_with_joins.relation)
                .chain(table_with_joins.joins.iter().map(|join| &join.relation))
                .flat_map(|factor| self.factor_columns(factor))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// `FROM a JOIN b ON ... JOIN c ...`: a left-deep join tree. Conjuncts
    /// of the WHERE that reference one item are pushed into its scan.
    fn join_tree(&mut self, from: &TableWithJoins, quals: Vec<Expr>) -> Result<PlanNode> {
        let mut remaining = quals;
        let mut take_for = |factor: &TableFactor, this: &Self, remaining: &mut Vec<Expr>| {
            let names = Self::factor_names(factor);
            let columns = this.factor_columns(factor);
            let (mine, rest): (Vec<Expr>, Vec<Expr>) = std::mem::take(remaining)
                .into_iter()
                .partition(|qual| this.qual_belongs(qual, &names, &columns));
            *remaining = rest;
            mine
        };
        let left_quals = if from.joins.iter().all(|join| {
            matches!(
                join.join_operator,
                JoinOperator::Join(_)
                    | JoinOperator::Inner(_)
                    | JoinOperator::CrossJoin(_)
                    | JoinOperator::Left(_)
                    | JoinOperator::LeftOuter(_)
            )
        }) {
            take_for(&from.relation, self, &mut remaining)
        } else {
            Vec::new()
        };
        let mut plan = self.from_factor(&from.relation, left_quals)?;
        for (position, join) in from.joins.iter().enumerate() {
            let (kind, constraint) = match &join.join_operator {
                JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                    ("Inner", Some(constraint))
                }
                JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                    ("Left", Some(constraint))
                }
                JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
                    ("Right", Some(constraint))
                }
                JoinOperator::FullOuter(constraint) => ("Full", Some(constraint)),
                JoinOperator::CrossJoin(_) => ("Inner", None),
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "EXPLAIN of join {other:?} is not supported"
                    )))
                }
            };
            let right_quals = if kind == "Inner" || kind == "Left" {
                if kind == "Inner" {
                    take_for(&join.relation, self, &mut remaining)
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            let mut join_quals = Vec::new();
            if let Some(JoinConstraint::On(on)) = constraint {
                conjuncts(on, &mut join_quals);
            }
            if position + 1 == from.joins.len() && kind == "Inner" {
                join_quals.append(&mut remaining);
            }
            // Join-constraint conjuncts on the inner side alone filter its scan.
            let (inner_only, join_quals): (Vec<Expr>, Vec<Expr>) =
                if matches!(kind, "Inner" | "Left") {
                    let names = Self::factor_names(&join.relation);
                    let columns = self.factor_columns(&join.relation);
                    join_quals
                        .into_iter()
                        .partition(|qual| self.qual_belongs(qual, &names, &columns))
                } else {
                    (Vec::new(), join_quals)
                };
            let mut inner_quals = right_quals;
            inner_quals.extend(inner_only);
            let equi = join_quals.iter().position(|qual| {
                matches!(qual, Expr::BinaryOp { op: BinaryOperator::Eq, left, right }
                    if matches!(left.as_ref(), Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                        && matches!(right.as_ref(), Expr::Identifier(_) | Expr::CompoundIdentifier(_)))
            });
            // The executor probes an index of the inner table by the join
            // key per outer row when the key leads one of its indexes.
            let probe = match (equi, kind) {
                (Some(index), "Inner" | "Left") => {
                    self.join_probe(&join.relation, &join_quals[index])?
                }
                _ => None,
            };
            let probe_cond = probe
                .as_ref()
                .map(|_| equi.expect("probe needs an equi key"));
            self.probe_scan = probe;
            let inner = self.from_factor(&join.relation, inner_quals);
            self.probe_scan = None;
            let inner = inner?;
            let mut join_quals = join_quals;
            if let Some(index) = probe_cond {
                join_quals.remove(index);
            }
            let equi = if probe_cond.is_some() { None } else { equi };
            let prefix = true;
            let join_type = match kind {
                "Inner" => "",
                other => other,
            };
            let mut node = match (equi, kind) {
                (Some(index), "Inner" | "Left") => {
                    let mut join_quals = join_quals;
                    let cond = join_quals.remove(index);
                    let mut node = PlanNode::new("Hash Join").text_name(if join_type.is_empty() {
                        "Hash Join".to_string()
                    } else {
                        format!("Hash {join_type} Join")
                    });
                    node = node
                        .head("Join Type", Prop::Text(kind.to_string()))
                        .head("Inner Unique", Prop::Bool(false));
                    let cond_text = self.expr(&cond, prefix);
                    node = node.prop("Hash Cond", cond_text);
                    if let Some(text) = self.qual(&join_quals, prefix) {
                        node = node.prop("Join Filter", text);
                    }
                    node.child("Outer", plan)
                        .child("Inner", PlanNode::new("Hash").child("Outer", inner))
                }
                _ => {
                    let mut node =
                        PlanNode::new("Nested Loop").text_name(if join_type.is_empty() {
                            "Nested Loop".to_string()
                        } else {
                            format!("Nested Loop {join_type} Join")
                        });
                    node = node
                        .head("Join Type", Prop::Text(kind.to_string()))
                        .head("Inner Unique", Prop::Bool(false));
                    if let Some(text) = self.qual(&join_quals, prefix) {
                        node = node.prop("Join Filter", text);
                    }
                    node.child("Outer", plan).child("Inner", inner)
                }
            };
            if !remaining.is_empty() && position + 1 == from.joins.len() {
                let rest = std::mem::take(&mut remaining);
                node = self.with_filter(node, &rest, prefix)?;
            }
            plan = node;
        }
        if !remaining.is_empty() {
            let rest = std::mem::take(&mut remaining);
            let prefix = self.multi_relation || self.verbose;
            plan = self.with_filter(plan, &rest, prefix)?;
        }
        Ok(plan)
    }

    /// The index probe of an indexed nested loop: the inner table's index
    /// led by the join key column, and the condition as PostgreSQL prints a
    /// parameterized index qualification (`(id = t1.ref_id)`).
    fn join_probe(
        &mut self,
        factor: &TableFactor,
        cond: &Expr,
    ) -> Result<Option<(String, String)>> {
        let TableFactor::Table {
            name, args: None, ..
        } = factor
        else {
            return Ok(None);
        };
        let collection = relation_name(name)?;
        if is_virtual_table(&collection) || load_view(self.engine.db_ref(), &collection)?.is_some()
        {
            return Ok(None);
        }
        let Expr::BinaryOp { left, right, .. } = cond else {
            return Ok(None);
        };
        let names = Self::factor_names(factor);
        let columns = self.factor_columns(factor);
        let (inner, outer) = if self.qual_belongs(left, &names, &columns) {
            (left, right)
        } else if self.qual_belongs(right, &names, &columns) {
            (right, left)
        } else {
            return Ok(None);
        };
        let column = match inner.as_ref() {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .map(|part| part.value.clone())
                .unwrap_or_default(),
            _ => return Ok(None),
        };
        let Some(schema) = load_schema(self.engine.db_ref(), &collection)? else {
            return Ok(None);
        };
        let primary = schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let index = if primary.first() == Some(&column) {
            Some(
                schema
                    .primary_key_name
                    .clone()
                    .unwrap_or_else(|| format!("{}_pkey", schema.name)),
            )
        } else {
            self.engine
                .db_ref()
                .index_definitions()
                .into_iter()
                .find(|definition| {
                    definition.collection == collection
                        && matches!(definition.fields.first(), Some(IndexField::MetadataPath(path)) if path.len() == 1 && path[0] == column)
                })
                .map(|definition| definition.name)
        };
        let Some(index) = index else {
            return Ok(None);
        };
        let outer_text = self.expr(outer, true);
        Ok(Some((
            index,
            format!("({} = {outer_text})", quote_ident(&column)),
        )))
    }

    // ----- queries ----------------------------------------------------------

    pub(crate) fn query(&mut self, query: &Query) -> Result<PlanNode> {
        self.scopes.push(Vec::new());
        let result = self.query_in_scope(query);
        self.scopes.pop();
        result
    }

    fn query_in_scope(&mut self, query: &Query) -> Result<PlanNode> {
        let mut cte_plans = Vec::new();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let name = cte.alias.name.value.clone();
                let plan = self.nested_query(&cte.query)?;
                self.ctes.push(name.clone());
                cte_plans.push((format!("CTE {name}"), plan));
            }
        }
        let ordered = if query.order_by.is_some() {
            self.ordered_index_scan(query)?
        } else {
            None
        };
        let sorted_by_index = ordered.is_some();
        self.ordered_scan = ordered;
        let plan = self.set_expr(&query.body, query);
        self.ordered_scan = None;
        let mut plan = plan?;
        if let Some(order_by) = query.order_by.as_ref().filter(|_| !sorted_by_index) {
            if let OrderByKind::Expressions(exprs) = &order_by.kind {
                if !exprs.is_empty() {
                    let keys = exprs
                        .iter()
                        .map(|item| self.sort_key(item, query))
                        .collect::<Vec<_>>();
                    plan = PlanNode::new("Sort")
                        .prop_value("Sort Key", Prop::List(keys))
                        .child("Outer", plan);
                }
            }
        }
        let limited = query.limit_clause.is_some() || query.fetch.is_some();
        if limited {
            plan = PlanNode::new("Limit").child("Outer", plan);
        }
        if !cte_plans.is_empty() {
            let mut children = Vec::new();
            for (name, cte) in cte_plans {
                children.push(("InitPlan", Some(name), cte));
            }
            children.append(&mut plan.children);
            plan.children = children;
        }
        Ok(plan)
    }

    fn sort_key(&mut self, item: &OrderByExpr, query: &Query) -> String {
        // An output column referenced by position or alias sorts by its expression.
        let mut expr = item.expr.clone();
        if let SetExpr::Select(select) = query.body.as_ref() {
            match &item.expr {
                Expr::Value(value) => {
                    if let Value::Number(text, _) = &value.value {
                        if let Some(
                            SelectItem::UnnamedExpr(target)
                            | SelectItem::ExprWithAlias { expr: target, .. },
                        ) = text
                            .parse::<usize>()
                            .ok()
                            .and_then(|position| select.projection.get(position.wrapping_sub(1)))
                        {
                            expr = target.clone();
                        }
                    }
                }
                Expr::Identifier(ident) => {
                    if let Some(target) =
                        select
                            .projection
                            .iter()
                            .find_map(|projection| match projection {
                                SelectItem::ExprWithAlias { expr, alias }
                                    if alias.value == ident.value =>
                                {
                                    Some(expr.clone())
                                }
                                _ => None,
                            })
                    {
                        expr = target;
                    }
                }
                _ => {}
            }
        }
        let mut text = self.key(&expr);
        match item.options.asc {
            Some(false) => {
                text.push_str(" DESC");
                if item.options.nulls_first == Some(false) {
                    text.push_str(" NULLS LAST");
                }
            }
            _ => {
                if item.options.nulls_first == Some(true) {
                    text.push_str(" NULLS FIRST");
                }
            }
        }
        text
    }

    fn set_expr(&mut self, body: &SetExpr, query: &Query) -> Result<PlanNode> {
        match body {
            SetExpr::Select(select) => self.select(select, query),
            SetExpr::Query(inner) => self.nested_query(inner),
            SetExpr::Values(values) => Ok(self.values_scan(values.rows.len())),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let all = matches!(set_quantifier, SetQuantifier::All);
                // Members of a chain of the same UNION are one Append.
                let mut members = Vec::new();
                fn collect<'q>(
                    expr: &'q SetExpr,
                    op: &SetOperator,
                    all: bool,
                    out: &mut Vec<&'q SetExpr>,
                ) {
                    match expr {
                        SetExpr::SetOperation {
                            op: inner_op,
                            set_quantifier,
                            left,
                            right,
                        } if inner_op == op
                            && matches!(op, SetOperator::Union)
                            && matches!(set_quantifier, SetQuantifier::All) == all =>
                        {
                            collect(left, op, all, out);
                            collect(right, op, all, out);
                        }
                        other => out.push(other),
                    }
                }
                collect(left, op, all, &mut members);
                collect(right, op, all, &mut members);
                let mut plans = Vec::new();
                for member in &members {
                    self.scopes.push(Vec::new());
                    let plan = self.set_expr(member, query);
                    self.scopes.pop();
                    plans.push(plan?);
                }
                match op {
                    SetOperator::Union => {
                        let mut append = PlanNode::new("Append");
                        for plan in plans {
                            append = append.child("Member", plan);
                        }
                        if all {
                            Ok(append)
                        } else {
                            Ok(PlanNode::new("Aggregate")
                                .text_name("HashAggregate")
                                .head("Strategy", Prop::Text("Hashed".to_string()))
                                .head("Partial Mode", Prop::Text("Simple".to_string()))
                                .child("Outer", append))
                        }
                    }
                    SetOperator::Intersect | SetOperator::Except | SetOperator::Minus => {
                        let command = if matches!(op, SetOperator::Intersect) {
                            "Intersect"
                        } else {
                            "Except"
                        };
                        let command = if all {
                            format!("{command} All")
                        } else {
                            command.to_string()
                        };
                        let mut node = PlanNode::new("SetOp")
                            .text_name(format!("HashSetOp {command}"))
                            .head("Strategy", Prop::Text("Hashed".to_string()))
                            .head("Command", Prop::Text(command));
                        let mut plans = plans.into_iter();
                        if let Some(first) = plans.next() {
                            node = node.child("Outer", first);
                        }
                        for plan in plans {
                            node = node.child("Inner", plan);
                        }
                        Ok(node)
                    }
                }
            }
            other => Err(SqlError::Unsupported(format!(
                "EXPLAIN of {other} is not supported"
            ))),
        }
    }

    fn select(&mut self, select: &Select, query: &Query) -> Result<PlanNode> {
        let _ = query;
        let mut quals = Vec::new();
        if let Some(selection) = &select.selection {
            conjuncts(selection, &mut quals);
        }
        let mut plan = if select.from.is_empty() {
            let mut node = PlanNode::new("Result");
            if !quals.is_empty() {
                let prefix = self.verbose;
                let text = self.qual(&quals, prefix).unwrap_or_default();
                node = node.prop("One-Time Filter", text);
            }
            node
        } else {
            // Comma-separated FROM items join as an inner nested loop.
            let mut items = select.from.iter();
            let first = items.next().expect("non-empty FROM");
            let rest = items.collect::<Vec<_>>();
            if rest.is_empty() {
                self.join_tree(first, quals)?
            } else {
                let mut remaining = quals;
                let mut take_for =
                    |from: &TableWithJoins, this: &Self, remaining: &mut Vec<Expr>| {
                        let names = std::iter::once(&from.relation)
                            .chain(from.joins.iter().map(|join| &join.relation))
                            .flat_map(Self::factor_names)
                            .collect::<Vec<_>>();
                        let columns = std::iter::once(&from.relation)
                            .chain(from.joins.iter().map(|join| &join.relation))
                            .flat_map(|factor| this.factor_columns(factor))
                            .collect::<Vec<_>>();
                        let (mine, rest): (Vec<Expr>, Vec<Expr>) = std::mem::take(remaining)
                            .into_iter()
                            .partition(|qual| this.qual_belongs(qual, &names, &columns));
                        *remaining = rest;
                        mine
                    };
                let first_quals = take_for(first, self, &mut remaining);
                let mut plan = self.join_tree(first, first_quals)?;
                for (position, item) in rest.iter().enumerate() {
                    let item_quals = take_for(item, self, &mut remaining);
                    let inner = self.join_tree(item, item_quals)?;
                    let last = position + 1 == rest.len();
                    let join_quals = if last {
                        std::mem::take(&mut remaining)
                    } else {
                        Vec::new()
                    };
                    let equi = join_quals.iter().position(|qual| {
                        matches!(qual, Expr::BinaryOp { op: BinaryOperator::Eq, left, right }
                            if matches!(left.as_ref(), Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                                && matches!(right.as_ref(), Expr::Identifier(_) | Expr::CompoundIdentifier(_)))
                    });
                    plan = match equi {
                        Some(index) => {
                            let mut join_quals = join_quals;
                            let cond = join_quals.remove(index);
                            let cond_text = self.expr(&cond, true);
                            let mut node = PlanNode::new("Hash Join")
                                .head("Join Type", Prop::Text("Inner".to_string()))
                                .head("Inner Unique", Prop::Bool(false))
                                .prop("Hash Cond", cond_text);
                            if let Some(text) = self.qual(&join_quals, true) {
                                node = node.prop("Join Filter", text);
                            }
                            node.child("Outer", plan)
                                .child("Inner", PlanNode::new("Hash").child("Outer", inner))
                        }
                        None => {
                            let mut node = PlanNode::new("Nested Loop")
                                .head("Join Type", Prop::Text("Inner".to_string()))
                                .head("Inner Unique", Prop::Bool(false));
                            if let Some(text) = self.qual(&join_quals, true) {
                                node = node.prop("Join Filter", text);
                            }
                            node.child("Outer", plan).child("Inner", inner)
                        }
                    };
                }
                plan
            }
        };
        let group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => exprs.clone(),
            GroupByExpr::All(_) => Vec::new(),
        };
        let aggregated = has_aggregates(select) || !group_by.is_empty();
        if aggregated {
            let mut node = if group_by.is_empty() {
                PlanNode::new("Aggregate")
                    .head("Strategy", Prop::Text("Plain".to_string()))
                    .head("Partial Mode", Prop::Text("Simple".to_string()))
            } else {
                let keys = group_by.iter().map(|expr| self.key(expr)).collect();
                PlanNode::new("Aggregate")
                    .text_name("HashAggregate")
                    .head("Strategy", Prop::Text("Hashed".to_string()))
                    .head("Partial Mode", Prop::Text("Simple".to_string()))
                    .prop_value("Group Key", Prop::List(keys))
            };
            if let Some(having) = &select.having {
                let mut having_quals = Vec::new();
                conjuncts(having, &mut having_quals);
                let prefix = self.multi_relation || self.verbose;
                node = self.with_filter(node, &having_quals, prefix)?;
            }
            plan = node.child("Outer", plan);
        }
        let windows = window_specs(select);
        for (position, spec) in windows.iter().enumerate() {
            let mut keys = Vec::new();
            for partition in &spec.partition_by {
                keys.push(self.key(partition));
            }
            for order in &spec.order_by {
                let mut text = self.key(&order.expr);
                if order.options.asc == Some(false) {
                    text.push_str(" DESC");
                }
                keys.push(text);
            }
            if !keys.is_empty() {
                plan = PlanNode::new("Sort")
                    .prop_value("Sort Key", Prop::List(keys))
                    .child("Outer", plan);
            }
            let mut definition = Vec::new();
            if !spec.partition_by.is_empty() {
                let parts = spec
                    .partition_by
                    .iter()
                    .map(|expr| self.key(expr))
                    .collect::<Vec<_>>();
                definition.push(format!("PARTITION BY {}", parts.join(", ")));
            }
            if !spec.order_by.is_empty() {
                let parts = spec
                    .order_by
                    .iter()
                    .map(|order| {
                        let mut text = self.key(&order.expr);
                        if order.options.asc == Some(false) {
                            text.push_str(" DESC");
                        }
                        text
                    })
                    .collect::<Vec<_>>();
                definition.push(format!("ORDER BY {}", parts.join(", ")));
            }
            if let Some(frame) = &spec.window_frame {
                definition.push(match &frame.end_bound {
                    Some(end) => format!("{} BETWEEN {} AND {end}", frame.units, frame.start_bound),
                    None => format!("{} {}", frame.units, frame.start_bound),
                });
            }
            plan = PlanNode::new("WindowAgg")
                .prop(
                    "Window",
                    format!("w{} AS ({})", position + 1, definition.join(" ")),
                )
                .child("Outer", plan);
        }
        match &select.distinct {
            Some(Distinct::Distinct) => {
                let mut keys = Vec::new();
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                            keys.push(self.key(expr))
                        }
                        _ => {
                            let rels = self.scopes.last().cloned().unwrap_or_default();
                            let prefix = self.multi_relation || self.verbose;
                            for rel in rels {
                                for (column, _) in &rel.columns {
                                    keys.push(if prefix {
                                        format!(
                                            "{}.{}",
                                            quote_ident(&rel.refname),
                                            quote_ident(column)
                                        )
                                    } else {
                                        quote_ident(column)
                                    });
                                }
                            }
                        }
                    }
                }
                plan = PlanNode::new("Aggregate")
                    .text_name("HashAggregate")
                    .head("Strategy", Prop::Text("Hashed".to_string()))
                    .head("Partial Mode", Prop::Text("Simple".to_string()))
                    .prop_value("Group Key", Prop::List(keys))
                    .child("Outer", plan);
            }
            Some(Distinct::On(exprs)) => {
                let keys = exprs.iter().map(|expr| self.key(expr)).collect::<Vec<_>>();
                plan = PlanNode::new("Unique").child(
                    "Outer",
                    PlanNode::new("Sort")
                        .prop_value("Sort Key", Prop::List(keys))
                        .child("Outer", plan),
                );
            }
            _ => {}
        }
        Ok(plan)
    }

    // ----- statements -------------------------------------------------------

    fn modify(
        &mut self,
        operation: &str,
        relname: &str,
        alias: Option<String>,
        child: PlanNode,
    ) -> PlanNode {
        let refname = alias.unwrap_or_else(|| relname.to_string());
        let mut node = PlanNode::new("ModifyTable")
            .text_name(operation)
            .head("Operation", Prop::Text(operation.to_string()))
            .head("Relation Name", Prop::Text(relname.to_string()))
            .head("Alias", Prop::Text(refname.clone()));
        let shown = if self.verbose {
            format!("public.{}", quote_ident(relname))
        } else {
            quote_ident(relname)
        };
        node.target = if refname == relname {
            format!(" on {shown}")
        } else {
            format!(" on {shown} {}", quote_ident(&refname))
        };
        node.child("Outer", child)
    }

    pub(crate) fn statement(&mut self, statement: &Statement) -> Result<PlanNode> {
        match statement {
            Statement::Query(query) => self.query(query),
            Statement::Insert(insert) => {
                let TableObject::TableName(name) = &insert.table else {
                    return Err(SqlError::Unsupported(
                        "EXPLAIN of INSERT into a table function is not supported".to_string(),
                    ));
                };
                let relname = relation_default_alias(name);
                *self.refnames.entry(relname.clone()).or_default() += 1;
                let child = match &insert.source {
                    Some(source) => match source.body.as_ref() {
                        SetExpr::Values(values)
                            if source.order_by.is_none() && source.limit_clause.is_none() =>
                        {
                            self.values_scan(values.rows.len())
                        }
                        _ => self.query(source)?,
                    },
                    None => PlanNode::new("Result"),
                };
                let alias = insert
                    .table_alias
                    .as_ref()
                    .map(|alias| alias.alias.value.clone());
                let mut node = self.modify("Insert", &relname, alias, child);
                if let Some(OnInsert::OnConflict(conflict)) = &insert.on {
                    let resolution = match &conflict.action {
                        OnConflictAction::DoNothing => "NOTHING",
                        OnConflictAction::DoUpdate(_) => "UPDATE",
                    };
                    node = node.prop("Conflict Resolution", resolution);
                    let collection = relation_name(name)?;
                    if let Some(schema) = load_schema(self.engine.db_ref(), &collection)? {
                        let mut arbiters = Vec::new();
                        if schema.columns.iter().any(|column| column.primary_key) {
                            arbiters.push(
                                schema
                                    .primary_key_name
                                    .clone()
                                    .unwrap_or_else(|| format!("{}_pkey", schema.name)),
                            );
                        }
                        arbiters.extend(
                            schema
                                .indexes
                                .iter()
                                .filter(|index| index.unique)
                                .map(|index| index.name.clone()),
                        );
                        if !arbiters.is_empty() && conflict.conflict_target.is_some() {
                            node =
                                node.prop_value("Conflict Arbiter Indexes", Prop::List(arbiters));
                        }
                    }
                }
                Ok(node)
            }
            Statement::Update(update) => {
                let TableFactor::Table { name, alias, .. } = &update.table.relation else {
                    return Err(SqlError::Unsupported(
                        "EXPLAIN of this UPDATE target is not supported".to_string(),
                    ));
                };
                self.scopes.push(Vec::new());
                let mut quals = Vec::new();
                if let Some(selection) = &update.selection {
                    conjuncts(selection, &mut quals);
                }
                let scan = self.table_scan(name, alias.as_ref(), quals);
                self.scopes.pop();
                let relname = relation_default_alias(name);
                Ok(self.modify(
                    "Update",
                    &relname,
                    alias.as_ref().map(|alias| alias.name.value.clone()),
                    scan?,
                ))
            }
            Statement::Delete(delete) => {
                let from = match &delete.from {
                    FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
                };
                let Some(TableWithJoins {
                    relation: TableFactor::Table { name, alias, .. },
                    joins,
                }) = from.first()
                else {
                    return Err(SqlError::Unsupported(
                        "EXPLAIN of this DELETE target is not supported".to_string(),
                    ));
                };
                if !joins.is_empty() || delete.using.is_some() {
                    return Err(SqlError::Unsupported(
                        "EXPLAIN of DELETE ... USING is not supported".to_string(),
                    ));
                }
                self.scopes.push(Vec::new());
                let mut quals = Vec::new();
                if let Some(selection) = &delete.selection {
                    conjuncts(selection, &mut quals);
                }
                let scan = self.table_scan(name, alias.as_ref(), quals);
                self.scopes.pop();
                let relname = relation_default_alias(name);
                Ok(self.modify(
                    "Delete",
                    &relname,
                    alias.as_ref().map(|alias| alias.name.value.clone()),
                    scan?,
                ))
            }
            Statement::CreateTable(create) => match &create.query {
                Some(query) => self.query(query),
                None => Err(SqlError::Unsupported(
                    "EXPLAIN of CREATE TABLE without AS is not supported".to_string(),
                )),
            },
            Statement::CreateView(create) if create.materialized => self.query(&create.query),
            other => Err(SqlError::Unsupported(format!(
                "EXPLAIN of {} statements is not supported",
                other
                    .to_string()
                    .split_whitespace()
                    .next()
                    .unwrap_or("these")
            ))),
        }
    }
}

/// Sets actual row counts on a plan for EXPLAIN ANALYZE: the top node's
/// count is the statement's result; nodes below are marked executed with
/// unknown counts only when the statement produced rows.
pub(crate) fn annotate_actual_rows(plan: &mut PlanNode, rows: u64) {
    fn walk(node: &mut PlanNode, rows: Option<u64>) {
        node.actual = Some((rows.unwrap_or(0) as f64, 1.0));
        for (_, _, child) in &mut node.children {
            walk(child, None);
        }
    }
    walk(plan, Some(rows));
}
