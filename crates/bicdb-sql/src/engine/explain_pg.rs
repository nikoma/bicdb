//! EXPLAIN in PostgreSQL's node vocabulary and layout: the plan tree is
//! derived from the statement and the decisions BicDB's executor takes, made
//! by the executor's own choosers (the row-set locator, the join order and
//! join-method rules), never invented. A node name appears only where it is
//! true of what runs: the locator collects row ids before fetching rows (a
//! Bitmap Heap Scan), a nested loop over stored inner rows reads them once (a
//! Materialize), an uncorrelated IN list is probed through a hash table (a
//! hashed SubPlan). Where BicDB runs a statement differently from PostgreSQL
//! the plan shows BicDB's way (a derived table is a Subquery Scan, a CTE is a
//! CTE Scan over its materialized result).

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
    /// The executor proved the qualification can never be true and reads
    /// nothing.
    Empty,
    /// An index scan: rows are fetched as the index yields them (in index
    /// order, or at most one row). Index name, the equality-prefix columns
    /// used as index conditions, and whether a range on the next column is
    /// used.
    Index {
        index: String,
        columns: Vec<String>,
        range: bool,
        backward: bool,
    },
    /// A bitmap heap scan: the executor first collects every matching row
    /// id from the index, then fetches those rows in row-id order and
    /// re-applies the qualification (the row-set locator's path).
    Bitmap(BitmapSource),
    /// The union of one index probe per OR branch, then one fetch in row-id
    /// order (the locator's OR route).
    BitmapOr(Vec<BitmapSource>),
}

/// One index probe that contributes row ids to a bitmap heap scan.
#[derive(Clone, Debug)]
struct BitmapSource {
    index: String,
    /// Index columns its conditions may bind (equality prefix, then the
    /// range column when `range`).
    columns: Vec<String>,
    range: bool,
    /// An operator-class probe of a non-B-tree index: every qual on the
    /// indexed columns alone drives it.
    leading_only: bool,
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
    /// The per-outer-row lookup the executor runs for the next scan (the
    /// inner side of an indexed nested loop, as the join predictor decided).
    join_lookup: Option<JoinLookup>,
}

/// The inner side of an indexed nested loop: the index the executor looks
/// rows up in per outer row and the rendered terms that bind it.
#[derive(Clone, Debug)]
struct JoinLookup {
    index: String,
    primary_key: bool,
    probe: Vec<String>,
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

/// The branches of an OR, flattened.
fn disjuncts(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            disjuncts(left, out);
            disjuncts(right, out);
        }
        Expr::Nested(inner) => disjuncts(inner, out),
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
            join_lookup: None,
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
        // The planner folds similar_to_escape of constants into the
        // translated pattern (the SIMILAR TO rewrite's argument).
        if name == "similar_to_escape" {
            if let FunctionArguments::List(list) = &function.args {
                let texts = list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) => {
                            literal_text(&value.value)
                        }
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>();
                if let Some(texts) = texts.filter(|texts| (1..=2).contains(&texts.len())) {
                    if let Ok(pattern) = crate::similar_to::similar_to_escape(
                        &texts[0],
                        texts.get(1).map(String::as_str),
                    ) {
                        return const_text(&pattern, "text");
                    }
                }
            }
        }
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
        let (signature, executed_chooser, executed_parts) = match executed {
            Some(executed) => (executed.signature, Some(executed.chooser), executed.parts),
            None => (
                crate::plan_diagnostics::signature_of(&plan.kind),
                None,
                Vec::new(),
            ),
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
        let unique_full_equality = |index: &str, used: usize| {
            self.engine
                .db_ref()
                .index_definitions()
                .into_iter()
                .find(|definition| definition.collection == collection && definition.name == index)
                .is_some_and(|definition| definition.unique && definition.fields.len() == used)
        };
        // Which chooser's path runs decides the node: the row-set locator
        // (and the pending-writes chooser) collect row ids first and fetch
        // afterwards in row-id order, a bitmap heap scan; the planner's own
        // B-tree loads and the bounded ordered path fetch in index order.
        let collects_ids =
            executed_chooser.is_some_and(|chooser| matches!(chooser, "locator" | "pending-writes"));
        let source = |signature: &crate::plan_diagnostics::PlanSignature| -> Option<BitmapSource> {
            if matches!(signature.kind, "PrimaryKeyLocator" | "PrimaryKeyInLookup") {
                let schema = schema.as_ref()?;
                let columns = schema
                    .columns
                    .iter()
                    .filter(|column| column.primary_key)
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>();
                return Some(BitmapSource {
                    index: schema
                        .primary_key_name
                        .clone()
                        .unwrap_or_else(|| format!("{}_pkey", schema.name)),
                    leading_only: false,
                    columns,
                    range: false,
                });
            }
            let index = signature.index.clone()?;
            let columns = index_columns(&index);
            Some(match signature.kind {
                "IndexLookup" => BitmapSource {
                    columns: columns.into_iter().take(signature.prefix_len).collect(),
                    index,
                    range: false,
                    leading_only: false,
                },
                "IndexRange" => BitmapSource {
                    columns: columns
                        .into_iter()
                        .take(signature.prefix_len.max(1))
                        .collect(),
                    index,
                    range: true,
                    leading_only: false,
                },
                "IndexInLookup" => BitmapSource {
                    columns: columns.into_iter().take(1).collect(),
                    index,
                    range: false,
                    leading_only: false,
                },
                _ => return None,
            })
        };
        let primary_key_index = || {
            let schema = schema.as_ref()?;
            Some(Access::Index {
                index: schema
                    .primary_key_name
                    .clone()
                    .unwrap_or_else(|| format!("{}_pkey", schema.name)),
                columns: schema
                    .columns
                    .iter()
                    .filter(|column| column.primary_key)
                    .map(|column| column.name.clone())
                    .collect(),
                range: false,
                backward: false,
            })
        };
        Ok(match signature.kind {
            "Contradiction" => Access::Empty,
            "IndexLookup" | "IndexRange" | "IndexInLookup" if collects_ids => {
                let source = source(&signature).expect("index signature");
                let used = source.columns.len();
                if signature.kind == "IndexLookup" && unique_full_equality(&source.index, used) {
                    // A full-key equality on a unique index yields at most
                    // one row: collecting it first and fetching it is the
                    // same work as fetching it from the index entry.
                    Access::Index {
                        index: source.index,
                        columns: source.columns,
                        range: false,
                        backward: false,
                    }
                } else {
                    Access::Bitmap(source)
                }
            }
            "BitmapOr" if collects_ids => {
                let mut sources = Vec::new();
                for part in &executed_parts {
                    match source(part) {
                        Some(source) => sources.push(source),
                        None => {
                            return Err(SqlError::Unsupported(
                                "EXPLAIN of this OR access path is not supported".to_string(),
                            ))
                        }
                    }
                }
                Access::BitmapOr(sources)
            }
            // The planner's IN-list load unions one lookup per value and
            // fetches the rows in row-id order.
            "IndexInLookup" => Access::Bitmap(source(&signature).expect("index signature")),
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
            "PrimaryKeyLookup"
            | "PrimaryKeyPrefixLookup"
            | "PrimaryKeyInLookup"
            | "PrimaryKeyLocator" => match primary_key_index() {
                Some(access) => access,
                None => Access::Seq,
            },
            // The planner's non-B-tree loads collect candidate row ids from
            // the index, then fetch and re-check them.
            "JsonbIndexScan" | "ArrayIndexScan" | "TrigramIndexScan" | "GeometricIndexScan"
            | "SpatialIndexScan" | "FullTextIndexScan" => {
                let index = signature.index.clone().unwrap_or_default();
                let columns = index_columns(&index);
                Access::Bitmap(BitmapSource {
                    index,
                    columns,
                    range: false,
                    leading_only: true,
                })
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
        // The route that executes decides: the bounded ordered path reads
        // the index in ORDER BY order; any other executing chooser (the
        // row-set locator, a streamed scan) fetches rows that are sorted
        // afterwards. Only when no executing route decides does the
        // planner's own ordered load run.
        if let Some(bounded) = self
            .engine
            .bounded_ordered_index_plan(from, select, query)?
        {
            return Ok(Some(bounded.index_and_direction()));
        }
        if self
            .engine
            .predicted_executed_access(from, select, query, &collection, &alias)?
            .is_some()
        {
            return Ok(None);
        }
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
        if !is_virtual_table(&collection)
            && load_schema(self.engine.db_ref(), &collection)?.is_none()
        {
            // Reports a missing relation as executing the statement would.
            self.engine
                .require_relation_privilege(&collection, "SELECT")?;
        }
        let refname = self.refname(&relname, alias.map(|alias| alias.name.value.as_str()));
        let columns = self.table_columns(&collection)?;
        self.scopes.last_mut().expect("scope").push(Rel {
            refname: refname.clone(),
            columns,
        });
        if let Some(lookup) = self.join_lookup.take() {
            return self.join_lookup_scan(
                &collection,
                &relname,
                &refname,
                alias.is_some(),
                lookup,
                quals,
            );
        }
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
            Access::Empty => {
                return Ok(PlanNode::new("Result").prop("One-Time Filter", "false"));
            }
            Access::Seq => {
                let mut node = PlanNode::new("Seq Scan");
                node.target = format!(" on {shown_relation}{alias_text}");
                node = node.head("Relation Name", Prop::Text(relname.clone()));
                node.head("Alias", Prop::Text(refname.clone()))
            }
            Access::Bitmap(source) => {
                let (index_quals, filter) = self.bitmap_conditions(source, &quals);
                return self.bitmap_heap_scan(
                    &relname,
                    &refname,
                    format!(" on {shown_relation}{alias_text}"),
                    index_quals.clone(),
                    filter,
                    vec![(source.clone(), index_quals)],
                    prefix,
                );
            }
            Access::BitmapOr(sources) => {
                let mut rest = quals.clone();
                let position = rest.iter().position(|qual| {
                    let mut branches = Vec::new();
                    disjuncts(qual, &mut branches);
                    branches.len() == sources.len()
                });
                let Some(position) = position else {
                    return Err(SqlError::Unsupported(
                        "EXPLAIN of this OR access path is not supported".to_string(),
                    ));
                };
                let or_qual = rest.remove(position);
                let mut branches = Vec::new();
                disjuncts(&or_qual, &mut branches);
                let probes = sources
                    .iter()
                    .zip(branches)
                    .map(|(source, branch)| {
                        let mut branch_quals = Vec::new();
                        conjuncts(&branch, &mut branch_quals);
                        let (index_quals, _) = self.bitmap_conditions(source, &branch_quals);
                        (source.clone(), index_quals)
                    })
                    .collect();
                return self.bitmap_heap_scan(
                    &relname,
                    &refname,
                    format!(" on {shown_relation}{alias_text}"),
                    vec![or_qual],
                    rest,
                    probes,
                    prefix,
                );
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
            Access::Index { columns, range, .. } => self.index_conditions(columns, *range, &quals),
            _ => (Vec::new(), quals),
        };
        if let Some((_, condition)) = probe {
            node = node.prop("Index Cond", condition);
        } else if let Some(text) = self.qual(&index_quals, prefix) {
            node = node.prop("Index Cond", text);
        }
        node = self.with_filter(node, &filter, prefix)?;
        Ok(node)
    }

    /// The inner scan of an indexed nested loop: per outer row the executor
    /// looks the rows up in `lookup.index` and fetches them; a primary-key
    /// lookup (or a full-key unique one) yields at most one row (an Index
    /// Scan), any other collects row ids first (a Bitmap Heap Scan).
    fn join_lookup_scan(
        &mut self,
        collection: &str,
        relname: &str,
        refname: &str,
        _aliased: bool,
        lookup: JoinLookup,
        quals: Vec<Expr>,
    ) -> Result<PlanNode> {
        let prefix = self.verbose;
        let shown_relation = if self.verbose {
            format!("public.{}", quote_ident(relname))
        } else {
            quote_ident(relname)
        };
        let alias_text = if refname != relname {
            format!(" {}", quote_ident(refname))
        } else {
            String::new()
        };
        let index_columns = if lookup.primary_key {
            Vec::new()
        } else {
            self.index_columns(collection, &lookup.index)
        };
        let unique = !lookup.primary_key
            && self
                .engine
                .db_ref()
                .index_definitions()
                .into_iter()
                .find(|definition| {
                    definition.collection == collection && definition.name == lookup.index
                })
                .is_some_and(|definition| {
                    definition.unique
                        && definition.fields.len() == lookup.probe.len()
                        && lookup.probe.iter().all(|term| term.contains(" = "))
                });
        let cond = match lookup.probe.as_slice() {
            [] => None,
            [one] => Some(one.clone()),
            many => Some(format!("({})", many.join(" AND "))),
        };
        let _ = index_columns;
        if lookup.primary_key || unique {
            let mut node = PlanNode::new("Index Scan");
            node.target = format!(
                " using {} on {shown_relation}{alias_text}",
                quote_ident(&lookup.index)
            );
            node = node
                .head("Scan Direction", Prop::Text("Forward".to_string()))
                .head("Index Name", Prop::Text(lookup.index.clone()))
                .head("Relation Name", Prop::Text(relname.to_string()))
                .head("Alias", Prop::Text(refname.to_string()));
            if let Some(cond) = cond {
                node = node.prop("Index Cond", cond);
            }
            return self.with_filter(node, &quals, prefix);
        }
        let mut node = PlanNode::new("Bitmap Heap Scan");
        node.target = format!(" on {shown_relation}{alias_text}");
        node = node
            .head("Relation Name", Prop::Text(relname.to_string()))
            .head("Alias", Prop::Text(refname.to_string()));
        if let Some(cond) = &cond {
            node = node.prop("Recheck Cond", cond.clone());
        }
        node = self.with_filter(node, &quals, prefix)?;
        let mut child = PlanNode::new("Bitmap Index Scan");
        child.target = format!(" on {}", quote_ident(&lookup.index));
        child = child.head("Index Name", Prop::Text(lookup.index.clone()));
        if let Some(cond) = cond {
            child = child.prop("Index Cond", cond);
        }
        Ok(node.child("Outer", child))
    }

    /// The quals a bitmap index probe applies as index conditions, and the
    /// rest.
    fn bitmap_conditions(&self, source: &BitmapSource, quals: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
        if !source.leading_only {
            return self.index_conditions(&source.columns, source.range, quals);
        }
        if source.columns.is_empty() {
            // An index whose columns the catalog names internally (the
            // geometric operator-class indexes): its probe is the qual with
            // an indexable operator.
            return quals.iter().cloned().partition(|qual| {
                matches!(qual, Expr::BinaryOp { op, .. } if matches!(
                    op,
                    BinaryOperator::PGOverlap
                        | BinaryOperator::AtArrow
                        | BinaryOperator::ArrowAt
                        | BinaryOperator::PGBitwiseShiftLeft
                        | BinaryOperator::PGBitwiseShiftRight
                        | BinaryOperator::Custom(_)
                ))
            });
        }
        quals.iter().cloned().partition(|qual| {
            let mut columns = Vec::new();
            referenced_columns(qual, &mut columns);
            !contains_subquery(qual)
                && !columns.is_empty()
                && columns
                    .iter()
                    .all(|(_, column)| source.columns.iter().any(|indexed| indexed == column))
        })
    }

    /// A Bitmap Heap Scan over one Bitmap Index Scan per probe (under a
    /// BitmapOr when there are several).
    #[allow(clippy::too_many_arguments)]
    fn bitmap_heap_scan(
        &mut self,
        relname: &str,
        refname: &str,
        target: String,
        recheck: Vec<Expr>,
        filter: Vec<Expr>,
        probes: Vec<(BitmapSource, Vec<Expr>)>,
        prefix: bool,
    ) -> Result<PlanNode> {
        let mut node = PlanNode::new("Bitmap Heap Scan");
        node.target = target;
        node = node
            .head("Relation Name", Prop::Text(relname.to_string()))
            .head("Alias", Prop::Text(refname.to_string()));
        if let Some(text) = self.qual(&recheck, prefix) {
            node = node.prop("Recheck Cond", text);
        }
        node = self.with_filter(node, &filter, prefix)?;
        let mut children = Vec::new();
        for (source, index_quals) in probes {
            let mut child = PlanNode::new("Bitmap Index Scan");
            child.target = format!(" on {}", quote_ident(&source.index));
            child = child.head("Index Name", Prop::Text(source.index.clone()));
            if let Some(text) = self.qual(&index_quals, prefix) {
                child = child.prop("Index Cond", text);
            }
            children.push(child);
        }
        let child = if children.len() == 1 {
            children.pop().expect("one probe")
        } else {
            children
                .into_iter()
                .fold(PlanNode::new("BitmapOr"), |or, child| {
                    or.child("Member", child)
                })
        };
        Ok(node.child("Outer", child))
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
            Expr::Exists { subquery, negated } if self.hashed_exists(subquery).is_some() => {
                // The executor answers it from a hash table of the inner
                // keys: PostgreSQL's hashed SubPlan over the inner scan.
                let plan = self.hashed_exists(subquery).expect("checked");
                self.init_plans += 1;
                let name = format!("SubPlan {}", self.init_plans);
                let outer = self.expr(&plan.outer, prefix);
                let inner = self.nested_query(&plan.keys_query)?;
                subplans.push((name.clone(), inner));
                if *negated {
                    format!("(NOT (ANY ({outer} = (hashed {name}).col1)))")
                } else {
                    format!("(ANY ({outer} = (hashed {name}).col1))")
                }
            }
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
                // The executor answers an uncorrelated one-column subquery
                // of exactly hashable values from a hash table.
                let hashed =
                    super::row_eval::hashed_in_subquery_column_type(self.engine.db_ref(), subquery)
                        .is_some_and(|column_type| {
                            typed_equality_uses_plain_values(
                                self.expr_type(expr).as_deref(),
                                Some(column_type.as_str()),
                            )
                        });
                let shown = if hashed {
                    format!("hashed {name}")
                } else {
                    name.clone()
                };
                let left = self.expr(expr, prefix);
                let plan = self.nested_query(subquery)?;
                subplans.push((name.clone(), plan));
                if *negated {
                    format!("(NOT (ANY ({left} = ({shown}).col1)))")
                } else {
                    format!("(ANY ({left} = ({shown}).col1))")
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
            // Boolean combinations: each branch renders its own subqueries.
            Expr::BinaryOp {
                op: op @ (BinaryOperator::Or | BinaryOperator::And),
                ..
            } => {
                let mut branches = Vec::new();
                fn flatten(expr: &Expr, op: &BinaryOperator, out: &mut Vec<Expr>) {
                    match expr {
                        Expr::BinaryOp {
                            left,
                            op: inner,
                            right,
                        } if inner == op => {
                            flatten(left, op, out);
                            flatten(right, op, out);
                        }
                        Expr::Nested(nested) if matches!(nested.as_ref(), Expr::BinaryOp { op: inner, .. } if inner == op) => {
                            flatten(nested, op, out)
                        }
                        other => out.push(other.clone()),
                    }
                }
                flatten(qual, op, &mut branches);
                let mut texts = Vec::new();
                for branch in &branches {
                    let branch = match branch {
                        Expr::Nested(inner) => inner.as_ref(),
                        other => other,
                    };
                    if contains_subquery(branch) {
                        let (text, mut nested) = self.subquery_qual(branch, prefix)?;
                        texts.push(text);
                        subplans.append(&mut nested);
                    } else {
                        texts.push(self.expr(branch, prefix));
                    }
                }
                let word = if matches!(op, BinaryOperator::Or) {
                    " OR "
                } else {
                    " AND "
                };
                format!("({})", texts.join(word))
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } if contains_subquery(expr) => {
                let (text, mut nested) = self.subquery_qual(expr, prefix)?;
                subplans.append(&mut nested);
                format!("(NOT {text})")
            }
            Expr::Nested(inner) if contains_subquery(inner) => {
                let (text, mut nested) = self.subquery_qual(inner, prefix)?;
                subplans.append(&mut nested);
                text
            }
            other => self.expr(other, prefix),
        };
        Ok((text, subplans))
    }

    /// The hash semi-join plan the executor runs for `EXISTS (query)` at
    /// this level, when it runs one (see `hashed_exists_plan`).
    fn hashed_exists(&self, query: &Query) -> Option<super::row_eval::HashedExists> {
        // Inside a correlated subquery the executor keeps the per-row path.
        if self.scopes.len() > 1 {
            return None;
        }
        let plan = super::row_eval::hashed_exists_plan(self.engine.db_ref(), query)?;
        typed_equality_uses_plain_values(
            self.expr_type(&plan.outer).as_deref(),
            Some(plan.inner_type.as_str()),
        )
        .then_some(plan)
    }

    /// Whether the executor probes `expr IN (query)` through a hash table.
    fn hashed_in(&self, expr: &Expr, query: &Query) -> Option<String> {
        let column_type =
            super::row_eval::hashed_in_subquery_column_type(self.engine.db_ref(), query)?;
        typed_equality_uses_plain_values(
            self.expr_type(expr).as_deref(),
            Some(column_type.as_str()),
        )
        .then(|| {
            let SetExpr::Select(select) = query.body.as_ref() else {
                return None;
            };
            match select.projection.first()? {
                SelectItem::UnnamedExpr(item) | SelectItem::ExprWithAlias { expr: item, .. } => {
                    match item {
                        Expr::Identifier(ident) => Some(ident.value.clone()),
                        Expr::CompoundIdentifier(parts) => {
                            parts.last().map(|part| part.value.clone())
                        }
                        _ => None,
                    }
                }
                _ => None,
            }
        })
        .flatten()
    }

    /// The name the last relation `relation` (unaliased) was given.
    fn last_refname(&self, relation: &str, alias: Option<&str>) -> String {
        if let Some(alias) = alias {
            return alias.to_string();
        }
        match self.refnames.get(relation).copied().unwrap_or(1) {
            0 | 1 => relation.to_string(),
            count => format!("{relation}_{}", count - 1),
        }
    }

    /// The single base table a subquery reads, with its alias.
    fn subquery_relation(query: &Query) -> Option<(String, Option<String>)> {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        let TableFactor::Table { name, alias, .. } = &select.from.first()?.relation else {
            return None;
        };
        Some((
            relation_default_alias(name),
            alias.as_ref().map(|alias| alias.name.value.clone()),
        ))
    }

    /// A top-level `EXISTS`, `NOT EXISTS` or `IN` term the executor answers
    /// through a hash table of the subquery's keys: a Hash Semi Join (Hash
    /// Anti Join for `NOT EXISTS`) over the rows of the FROM clause.
    fn hash_semi_join(&mut self, outer: PlanNode, qual: &Expr) -> Result<PlanNode> {
        let (anti, outer_expr, inner_query, inner_column) = match qual {
            Expr::Exists { subquery, negated } => {
                let plan = self.hashed_exists(subquery).expect("checked by caller");
                (
                    *negated,
                    plan.outer.clone(),
                    plan.keys_query.clone(),
                    plan.inner_column,
                )
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated: false,
            } => {
                let column = self.hashed_in(expr, subquery).expect("checked by caller");
                (false, (**expr).clone(), (**subquery).clone(), column)
            }
            _ => unreachable!("hash semi-join of {qual}"),
        };
        let outer_text = self.expr(&outer_expr, true);
        let inner = self.nested_query(&inner_query)?;
        let (relation, alias) = Self::subquery_relation(&inner_query).unwrap_or_default();
        let inner_ref = self.last_refname(&relation, alias.as_deref());
        let name = if anti {
            "Hash Anti Join"
        } else {
            "Hash Semi Join"
        };
        let node = PlanNode::new("Hash Join")
            .text_name(name)
            .head(
                "Join Type",
                Prop::Text(if anti { "Anti" } else { "Semi" }.to_string()),
            )
            .head("Inner Unique", Prop::Bool(false))
            .prop(
                "Hash Cond",
                format!(
                    "({outer_text} = {}.{})",
                    quote_ident(&inner_ref),
                    quote_ident(&inner_column)
                ),
            );
        Ok(node
            .child("Outer", outer)
            .child("Inner", PlanNode::new("Hash").child("Outer", inner)))
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
        // The executor joins in the order its join planner chooses, judging
        // the whole qualification; describe that order.
        let selection = quals.iter().cloned().reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        });
        let (relation, joins) = if from.joins.is_empty() {
            (from.relation.clone(), Vec::new())
        } else {
            self.engine.plan_row_join_order(from, selection.as_ref())?
        };
        let mut remaining = quals;
        let take_for = |factor: &TableFactor, this: &Self, remaining: &mut Vec<Expr>| {
            let names = Self::factor_names(factor);
            let columns = this.factor_columns(factor);
            let (mine, rest): (Vec<Expr>, Vec<Expr>) = std::mem::take(remaining)
                .into_iter()
                .partition(|qual| this.qual_belongs(qual, &names, &columns));
            *remaining = rest;
            mine
        };
        let left_quals = if joins.iter().all(|join| {
            matches!(
                join.join_operator,
                JoinOperator::Join(_)
                    | JoinOperator::Inner(_)
                    | JoinOperator::CrossJoin(_)
                    | JoinOperator::Left(_)
                    | JoinOperator::LeftOuter(_)
            )
        }) {
            take_for(&relation, self, &mut remaining)
        } else {
            Vec::new()
        };
        let mut left_names = Self::factor_names(&relation);
        let mut left_factor_columns = self.factor_columns(&relation);
        let mut left_columns = self
            .engine
            .row_set_columns_from_table_factor_without_rows(&relation)?;
        let all_inner = joins.iter().all(is_reorderable_inner_join);
        let mut cumulative = if all_inner { selection.clone() } else { None };
        let mut plan = self.from_factor(&relation, left_quals)?;
        for join in &joins {
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
            let names = Self::factor_names(&join.relation);
            let columns = self.factor_columns(&join.relation);
            let right_quals = if kind == "Inner" {
                take_for(&join.relation, self, &mut remaining)
            } else {
                Vec::new()
            };
            let mut join_quals = Vec::new();
            if let Some(JoinConstraint::On(on)) = constraint {
                conjuncts(on, &mut join_quals);
            }
            if kind == "Inner" {
                // WHERE terms over the tables joined so far join them here.
                let mut joined_names = left_names.clone();
                joined_names.extend(names.iter().cloned());
                let mut joined_columns = left_factor_columns.clone();
                joined_columns.extend(columns.iter().cloned());
                let (mine, rest): (Vec<Expr>, Vec<Expr>) = std::mem::take(&mut remaining)
                    .into_iter()
                    .partition(|qual| self.qual_belongs(qual, &joined_names, &joined_columns));
                remaining = rest;
                join_quals.extend(mine);
            }
            // Join-constraint conjuncts on the inner side alone filter its scan.
            let (inner_only, join_quals): (Vec<Expr>, Vec<Expr>) =
                if matches!(kind, "Inner" | "Left") {
                    join_quals
                        .into_iter()
                        .partition(|qual| self.qual_belongs(qual, &names, &columns))
                } else {
                    (Vec::new(), join_quals)
                };
            let mut inner_quals = right_quals;
            inner_quals.extend(inner_only);
            let prediction = match &left_columns {
                Some(left_columns) => self.engine.predict_row_join(
                    left_columns,
                    join,
                    selection.as_ref(),
                    cumulative.as_ref(),
                )?,
                None => crate::engine::RowJoinPrediction::Unknown,
            };
            let join_type = match kind {
                "Inner" => "",
                other => other,
            };
            let prefix = true;
            let equi = join_quals.iter().position(|qual| {
                matches!(qual, Expr::BinaryOp { op: BinaryOperator::Eq, left, right }
                    if matches!(left.as_ref(), Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                        && matches!(right.as_ref(), Expr::Identifier(_) | Expr::CompoundIdentifier(_)))
            });
            let hash_node = |this: &mut Self,
                             mut join_quals: Vec<Expr>,
                             index: usize,
                             plan: PlanNode,
                             inner: PlanNode|
             -> PlanNode {
                let cond = join_quals.remove(index);
                let mut node = PlanNode::new("Hash Join").text_name(if join_type.is_empty() {
                    "Hash Join".to_string()
                } else {
                    format!("Hash {join_type} Join")
                });
                node = node
                    .head("Join Type", Prop::Text(kind.to_string()))
                    .head("Inner Unique", Prop::Bool(false));
                let cond_text = this.expr(&cond, prefix);
                node = node.prop("Hash Cond", cond_text);
                if let Some(text) = this.qual(&join_quals, prefix) {
                    node = node.prop("Join Filter", text);
                }
                node.child("Outer", plan)
                    .child("Inner", PlanNode::new("Hash").child("Outer", inner))
            };
            let nested_node = |this: &mut Self,
                               join_quals: Vec<Expr>,
                               plan: PlanNode,
                               inner: PlanNode|
             -> PlanNode {
                let mut node = PlanNode::new("Nested Loop").text_name(if join_type.is_empty() {
                    "Nested Loop".to_string()
                } else {
                    format!("Nested Loop {join_type} Join")
                });
                node = node
                    .head("Join Type", Prop::Text(kind.to_string()))
                    .head("Inner Unique", Prop::Bool(false));
                if let Some(text) = this.qual(&join_quals, prefix) {
                    node = node.prop("Join Filter", text);
                }
                node.child("Outer", plan).child("Inner", inner)
            };
            let mut node = match prediction {
                crate::engine::RowJoinPrediction::IndexNestedLoop {
                    index, primary_key, ..
                } => {
                    // The inner table is looked up per outer row by the join
                    // terms that bind the index; the other join terms are
                    // checked on each joined pair.
                    let index_columns = if primary_key {
                        self.primary_key_columns(&join.relation)
                    } else {
                        self.factor_index_columns(&join.relation, &index)
                    };
                    let mut candidates = join_quals.clone();
                    candidates.extend(inner_quals.iter().cloned());
                    let (probe, rest) =
                        self.probe_conditions(&index_columns, &candidates, &names, &columns);
                    let join_rest = join_quals
                        .into_iter()
                        .filter(|qual| rest.contains(qual))
                        .collect::<Vec<_>>();
                    let inner_rest = inner_quals
                        .into_iter()
                        .filter(|qual| rest.contains(qual))
                        .collect::<Vec<_>>();
                    self.join_lookup = Some(JoinLookup {
                        index,
                        primary_key,
                        probe,
                    });
                    let inner = self.from_factor(&join.relation, inner_rest);
                    self.join_lookup = None;
                    nested_node(self, join_rest, plan, inner?)
                }
                crate::engine::RowJoinPrediction::Hash { .. } if equi.is_some() => {
                    let inner = self.from_factor(&join.relation, inner_quals)?;
                    hash_node(self, join_quals, equi.expect("checked"), plan, inner)
                }
                crate::engine::RowJoinPrediction::Hash { .. }
                | crate::engine::RowJoinPrediction::NestedLoop { .. } => {
                    // The executor reads the inner side once and compares
                    // every outer row with the stored rows.
                    let inner = self.from_factor(&join.relation, inner_quals)?;
                    let inner = PlanNode::new("Materialize").child("Outer", inner);
                    nested_node(self, join_quals, plan, inner)
                }
                crate::engine::RowJoinPrediction::Unknown => {
                    // Shapes the join predictor does not model keep the
                    // description by join condition.
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
                    match (equi, kind) {
                        (Some(index), "Inner" | "Left") => {
                            hash_node(self, join_quals, index, plan, inner)
                        }
                        _ => nested_node(self, join_quals, plan, inner),
                    }
                }
            };
            if let (Some(columns), Some(right)) = (
                left_columns.as_mut(),
                self.engine
                    .row_set_columns_from_table_factor_without_rows(&join.relation)?,
            ) {
                for column in right {
                    if !columns.contains(&column) {
                        columns.push(column);
                    }
                }
            } else {
                left_columns = None;
            }
            if let (Some(cumulative), Some(JoinConstraint::On(on))) =
                (cumulative.as_mut(), constraint)
            {
                *cumulative = Expr::BinaryOp {
                    left: Box::new(cumulative.clone()),
                    op: BinaryOperator::And,
                    right: Box::new(on.clone()),
                };
            } else if let (None, true, Some(JoinConstraint::On(on))) =
                (cumulative.as_ref(), all_inner, constraint)
            {
                cumulative = Some(on.clone());
            }
            left_names.extend(names);
            left_factor_columns.extend(columns);
            if !remaining.is_empty() && kind != "Inner" {
                // Terms over an outer join's result filter above it.
                let (mine, rest): (Vec<Expr>, Vec<Expr>) = std::mem::take(&mut remaining)
                    .into_iter()
                    .partition(|qual| self.qual_belongs(qual, &left_names, &left_factor_columns));
                remaining = rest;
                if !mine.is_empty() {
                    node = self.with_filter(node, &mine, prefix)?;
                }
            }
            plan = node;
        }
        if !remaining.is_empty() {
            let rest = std::mem::take(&mut remaining);
            // A scan's own qualification is shown unqualified (PostgreSQL
            // qualifies scan quals only in VERBOSE); a join's is qualified.
            let prefix = if joins.is_empty() {
                self.verbose
            } else {
                self.multi_relation || self.verbose
            };
            plan = self.with_filter(plan, &rest, prefix)?;
        }
        Ok(plan)
    }

    /// The primary-key column names of a base table FROM item.
    fn primary_key_columns(&self, factor: &TableFactor) -> Vec<String> {
        let TableFactor::Table { name, .. } = factor else {
            return Vec::new();
        };
        let Ok(collection) = relation_name(name) else {
            return Vec::new();
        };
        load_schema(self.engine.db_ref(), &collection)
            .ok()
            .flatten()
            .map(|schema| {
                schema
                    .columns
                    .iter()
                    .filter(|column| column.primary_key)
                    .map(|column| column.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// An index's column names for a base table FROM item.
    fn factor_index_columns(&self, factor: &TableFactor, index: &str) -> Vec<String> {
        let TableFactor::Table { name, .. } = factor else {
            return Vec::new();
        };
        let Ok(collection) = relation_name(name) else {
            return Vec::new();
        };
        self.index_columns(&collection, index)
    }

    /// The join terms that bind `columns` (an index's leading columns, in
    /// order: equalities, then one range) to values of the outer side, and
    /// the rest.
    fn probe_conditions(
        &mut self,
        columns: &[String],
        quals: &[Expr],
        names: &[String],
        factor_columns: &[String],
    ) -> (Vec<String>, Vec<Expr>) {
        let inner_column = |expr: &Expr| -> Option<String> {
            let column = match expr {
                Expr::Identifier(ident) => ident.value.clone(),
                Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                    if !names.iter().any(|name| name == &parts[0].value) {
                        return None;
                    }
                    parts[1].value.clone()
                }
                _ => return None,
            };
            factor_columns.contains(&column).then_some(column)
        };
        let references_inner = |expr: &Expr| {
            let mut references = Vec::new();
            referenced_columns(expr, &mut references);
            contains_subquery(expr)
                || references
                    .iter()
                    .any(|(qualifier, column)| match qualifier {
                        Some(qualifier) => names.contains(qualifier),
                        None => factor_columns.contains(column),
                    })
        };
        let mut used = vec![false; quals.len()];
        let mut probe = Vec::new();
        for column in columns {
            let mut equality = None;
            let mut ranges = Vec::new();
            for (position, qual) in quals.iter().enumerate() {
                if used[position] {
                    continue;
                }
                let Expr::BinaryOp { left, op, right } = qual else {
                    continue;
                };
                let (inner, outer, op) = match (inner_column(left), inner_column(right)) {
                    (Some(inner), None) if !references_inner(right) => (inner, right, op.clone()),
                    (None, Some(inner)) if !references_inner(left) => (
                        inner,
                        left,
                        match op {
                            BinaryOperator::Lt => BinaryOperator::Gt,
                            BinaryOperator::LtEq => BinaryOperator::GtEq,
                            BinaryOperator::Gt => BinaryOperator::Lt,
                            BinaryOperator::GtEq => BinaryOperator::LtEq,
                            other => other.clone(),
                        },
                    ),
                    _ => continue,
                };
                if &inner != column {
                    continue;
                }
                match op {
                    BinaryOperator::Eq if equality.is_none() => equality = Some((position, outer)),
                    BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq => ranges.push((position, op, outer)),
                    _ => {}
                }
            }
            if let Some((position, outer)) = equality {
                used[position] = true;
                let outer = self.expr(&outer.as_ref().clone(), true);
                probe.push(format!("({} = {outer})", quote_ident(column)));
                continue;
            }
            for (position, op, outer) in ranges {
                used[position] = true;
                let outer = self.expr(&outer.as_ref().clone(), true);
                let op = binary_operator_text(&op).unwrap_or("=");
                probe.push(format!("({} {op} {outer})", quote_ident(column)));
            }
            break;
        }
        let rest = quals
            .iter()
            .zip(used)
            .filter(|(_, used)| !used)
            .map(|(qual, _)| qual.clone())
            .collect();
        (probe, rest)
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
            // Comma-separated FROM items are cross joins the executor
            // orders and joins like explicit ones.
            let from = if select.from.len() == 1 {
                select.from[0].clone()
            } else {
                SqlEngine::comma_from_items_to_cross_join(&select.from)
            };
            // Top-level EXISTS / IN terms the executor answers from a hash
            // table of the subquery's keys are hash semi-joins.
            let (semi, quals): (Vec<Expr>, Vec<Expr>) =
                quals.into_iter().partition(|qual| match qual {
                    Expr::Exists { subquery, .. } => self.hashed_exists(subquery).is_some(),
                    Expr::InSubquery {
                        expr,
                        subquery,
                        negated: false,
                    } => self.scopes.len() == 1 && self.hashed_in(expr, subquery).is_some(),
                    _ => false,
                });
            let mut plan = self.join_tree(&from, quals)?;
            for qual in &semi {
                plan = self.hash_semi_join(plan, qual)?;
            }
            plan
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
