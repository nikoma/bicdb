//! INSERT, UPDATE and DELETE through automatically updatable views, and
//! `WITH [CASCADED | LOCAL] CHECK OPTION`.
//!
//! A view qualifies under PostgreSQL's rules: one base table or view in FROM
//! and no DISTINCT, GROUP BY, HAVING, LIMIT/OFFSET, set operation, WITH,
//! aggregate, window or set-returning function. A statement on such a view
//! is rewritten level by level into the same statement on the base relation:
//! view columns become their defining expressions, the view's WHERE joins an
//! UPDATE or DELETE, and the checks a CHECK OPTION requires are returned with
//! the written rows and verified before the statement completes.
//!
//! Privileges follow PostgreSQL: the caller needs the privilege on the view,
//! and each underlying relation is checked as the owner of the view above it
//! (as the same role for a `security_invoker` view). Row-level security of the
//! base table applies to the role that writes it; a view whose base table has
//! row security and whose checking role differs from the caller is refused
//! (0A000) rather than evaluated with the wrong role's policies.
use super::*;
use crate::column_privileges::ReferencedNames;
use crate::conflict_inference::{index_inference, index_target, is_index_inference};
use sqlparser::ast::{OnConflict, TableAliasWithoutColumns};
use sqlparser::ast::{Visit, VisitMut, Visitor, VisitorMut};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ViewDml {
    Insert,
    Update,
    Delete,
}

impl ViewDml {
    fn verb(self) -> &'static str {
        match self {
            Self::Insert => "insert into",
            Self::Update => "update",
            Self::Delete => "delete from",
        }
    }

    fn privilege(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CheckOption {
    Local,
    Cascaded,
}

impl CheckOption {
    pub(crate) fn parse(text: &str) -> Option<Self> {
        match text.to_ascii_lowercase().as_str() {
            "local" => Some(Self::Local),
            "cascaded" => Some(Self::Cascaded),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Cascaded => "cascaded",
        }
    }
}

#[derive(Clone, Debug)]
struct ViewColumn {
    name: String,
    /// The defining expression, qualified for the rewritten statement.
    expr: Expr,
    /// The base column when the view column is a plain reference to it.
    base_column: Option<String>,
}

/// One level of an updatable view.
#[derive(Clone, Debug)]
pub(crate) struct UpdatableView {
    /// The view's name as PostgreSQL prints it in messages.
    short: String,
    /// The base relation as written in the view.
    base_name: ObjectName,
    /// The base relation's catalog key.
    base: String,
    /// The name that qualifies base columns in the rewritten statement.
    qualifier: String,
    base_sql_name: String,
    /// The view's WHERE, qualified for the rewritten statement.
    selection: Option<Expr>,
    columns: Vec<ViewColumn>,
    base_columns: Vec<String>,
    check_option: Option<CheckOption>,
    owner: Option<String>,
    security_invoker: bool,
}

fn not_updatable(dml: ViewDml, view: &str, detail: &str) -> SqlError {
    SqlError::RaisedException {
        sqlstate: "0A000".to_string(),
        message: format!("cannot {} view \"{view}\"", dml.verb()),
        detail: Some(detail.to_string()),
    }
}

fn column_not_updatable(dml: ViewDml, column: &str, view: &str) -> SqlError {
    let verb = match dml {
        ViewDml::Insert => "insert into",
        _ => "update",
    };
    SqlError::RaisedException {
        sqlstate: "0A000".to_string(),
        message: format!("cannot {verb} column \"{column}\" of view \"{view}\""),
        detail: Some(
            "View columns that are not columns of their base relation are not updatable."
                .to_string(),
        ),
    }
}

fn unsupported(message: impl Into<String>) -> SqlError {
    SqlError::Unsupported(message.into())
}

fn short_relation(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}

fn view_sql_name(name: &str) -> String {
    crate::eval::rewrite_helpers::logical_relation_name(name)
        .unwrap_or_else(|| short_relation(name))
}

fn normalized(ident: &Ident) -> String {
    ident_value(ident)
}

const SET_RETURNING_FUNCTIONS: &[&str] = &[
    "generate_series",
    "generate_subscripts",
    "unnest",
    "regexp_matches",
    "regexp_split_to_table",
    "string_to_table",
    "json_array_elements",
    "json_array_elements_text",
    "jsonb_array_elements",
    "jsonb_array_elements_text",
    "json_each",
    "json_each_text",
    "jsonb_each",
    "jsonb_each_text",
    "json_object_keys",
    "jsonb_object_keys",
    "json_populate_recordset",
    "jsonb_populate_recordset",
    "json_to_recordset",
    "jsonb_to_recordset",
    "jsonb_path_query",
    "pg_get_keywords",
];

/// Aggregate, window and set-returning calls at the top level of a select
/// list (calls inside a scalar subquery belong to that subquery).
#[derive(Default)]
struct ProjectionCalls {
    depth: usize,
    aggregate: bool,
    window: bool,
    set_returning: bool,
    user_set_returning: Vec<String>,
}

impl Visitor for ProjectionCalls {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if self.depth > 0 {
            return ControlFlow::Continue(());
        }
        if let Expr::Function(function) = expr {
            if function.over.is_some() {
                self.window = true;
            } else if is_aggregate_function(function) {
                self.aggregate = true;
            }
            let name = object_name(&function.name)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let short = name.rsplit('.').next().unwrap_or(&name).to_string();
            if SET_RETURNING_FUNCTIONS.contains(&short.as_str()) {
                self.set_returning = true;
            } else {
                self.user_set_returning.push(name);
            }
        }
        ControlFlow::Continue(())
    }
}

/// Re-qualifies a view definition's references to its base relation for the
/// rewritten statement.
struct Requalify<'a> {
    from: &'a [String],
    to: &'a str,
    /// Base columns: a bare reference to one at the top level is qualified,
    /// so it keeps its binding inside the caller's subqueries.
    columns: &'a [String],
    /// Qualifiers declared by enclosing subqueries' FROM lists.
    scopes: Vec<BTreeSet<String>>,
}

impl VisitorMut for Requalify<'_> {
    type Break = ();

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
        self.scopes.push(select_qualifiers(select));
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _select: &mut Select) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::Identifier(ident) = expr {
            let name = normalized(ident);
            if self.scopes.is_empty() && self.columns.contains(&name) {
                *expr = Expr::CompoundIdentifier(vec![
                    Ident::with_quote('"', self.to),
                    Ident::with_quote('"', name),
                ]);
            }
            return ControlFlow::Continue(());
        }
        if let Expr::CompoundIdentifier(parts) = expr {
            if parts.len() >= 2 {
                let qualifier = normalized(&parts[parts.len() - 2]);
                let shadowed = self.scopes.iter().any(|scope| scope.contains(&qualifier));
                if !shadowed && self.from.contains(&qualifier) {
                    let column = parts[parts.len() - 1].clone();
                    *parts = vec![Ident::with_quote('"', self.to), column];
                }
            }
        }
        ControlFlow::Continue(())
    }
}

pub(super) fn select_qualifiers(select: &Select) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for from in &select.from {
        for factor in std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation))
        {
            factor_qualifiers(factor, &mut names);
        }
    }
    names
}

fn factor_qualifiers(factor: &TableFactor, names: &mut BTreeSet<String>) {
    match factor {
        TableFactor::Table { name, alias, .. } => match alias {
            Some(alias) => {
                names.insert(normalized(&alias.name));
            }
            None => {
                if let Some(last) = name.0.last().and_then(|part| part.as_ident()) {
                    names.insert(normalized(last));
                }
            }
        },
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            if let Some(alias) = alias {
                names.insert(normalized(&alias.name));
            }
            factor_qualifiers(&table_with_joins.relation, names);
            for join in &table_with_joins.joins {
                factor_qualifiers(&join.relation, names);
            }
        }
        other => {
            if let Some(alias) = table_factor_alias(other) {
                names.insert(normalized(&alias.name));
            }
        }
    }
}

fn table_factor_alias(factor: &TableFactor) -> Option<&TableAlias> {
    match factor {
        TableFactor::Derived { alias, .. }
        | TableFactor::TableFunction { alias, .. }
        | TableFactor::Function { alias, .. }
        | TableFactor::UNNEST { alias, .. } => alias.as_ref(),
        _ => None,
    }
}

/// Column names a subquery's FROM list provides, `None` when unknown.
fn select_columns(db: &BicDb, select: &Select) -> Option<BTreeSet<String>> {
    let mut columns = BTreeSet::new();
    for from in &select.from {
        for factor in std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation))
        {
            factor_columns(db, factor, &mut columns)?;
        }
    }
    Some(columns)
}

fn factor_columns(db: &BicDb, factor: &TableFactor, columns: &mut BTreeSet<String>) -> Option<()> {
    match factor {
        TableFactor::Table {
            name, alias, args, ..
        } if args.is_none() => {
            if let Some(alias) = alias.as_ref().filter(|alias| !alias.columns.is_empty()) {
                columns.extend(alias.columns.iter().map(|column| normalized(&column.name)));
                return Some(());
            }
            let relation = relation_name(name).ok()?;
            let relation = resolve_session_relation_name_if_exists(db, &relation);
            if let Some(schema) = load_schema(db, &relation).ok()? {
                columns.extend(
                    schema
                        .columns
                        .iter()
                        .filter(|column| !column.hidden)
                        .map(|column| column.name.clone()),
                );
                return Some(());
            }
            if let Some(view) = load_view(db, &relation).ok()? {
                columns.extend(view.columns.iter().map(|column| column.name.clone()));
                return Some(());
            }
            None
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            factor_columns(db, &table_with_joins.relation, columns)?;
            for join in &table_with_joins.joins {
                factor_columns(db, &join.relation, columns)?;
            }
            Some(())
        }
        other => {
            let alias = table_factor_alias(other)?;
            if alias.columns.is_empty() {
                return None;
            }
            columns.extend(alias.columns.iter().map(|column| normalized(&column.name)));
            Some(())
        }
    }
}

pub(crate) fn analyze_updatable_view(
    db: &BicDb,
    view: &ViewSchema,
) -> Result<std::result::Result<UpdatableView, &'static str>> {
    let short = view_sql_name(&view.name);
    if view.materialized {
        return Ok(Err("materialized"));
    }
    let query = parse_query(&view.query_sql)?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(Err(match query.body.as_ref() {
            SetExpr::SetOperation { .. } => {
                "Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable."
            }
            _ => "Views that do not select from a single table or view are not automatically updatable.",
        }));
    };
    if select.distinct.is_some() {
        return Ok(Err(
            "Views containing DISTINCT are not automatically updatable.",
        ));
    }
    if !matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if exprs.is_empty()) {
        return Ok(Err(
            "Views containing GROUP BY are not automatically updatable.",
        ));
    }
    if select.having.is_some() {
        return Ok(Err(
            "Views containing HAVING are not automatically updatable.",
        ));
    }
    if query.with.is_some() {
        return Ok(Err(
            "Views containing WITH are not automatically updatable.",
        ));
    }
    if query.limit_clause.is_some() || query.fetch.is_some() {
        return Ok(Err(
            "Views containing LIMIT or OFFSET are not automatically updatable.",
        ));
    }
    let mut calls = ProjectionCalls::default();
    for item in &select.projection {
        let _ = Visit::visit(item, &mut calls);
    }
    for name in &calls.user_set_returning {
        let short_name = name.rsplit('.').next().unwrap_or(name);
        if routines_named(db, RoutineKind::Function, short_name)?
            .iter()
            .any(|routine| routine.returns_set)
        {
            calls.set_returning = true;
        }
    }
    if calls.aggregate {
        return Ok(Err(
            "Views that return aggregate functions are not automatically updatable.",
        ));
    }
    if calls.window || !select.named_window.is_empty() {
        return Ok(Err(
            "Views that return window functions are not automatically updatable.",
        ));
    }
    if calls.set_returning {
        return Ok(Err(
            "Views that return set-returning functions are not automatically updatable.",
        ));
    }
    let single =
        "Views that do not select from a single table or view are not automatically updatable.";
    let [TableWithJoins { relation, joins }] = select.from.as_slice() else {
        return Ok(Err(single));
    };
    if !joins.is_empty() {
        return Ok(Err(single));
    }
    let TableFactor::Table {
        name: base_name,
        alias,
        args: None,
        sample,
        ..
    } = relation
    else {
        return Ok(Err(single));
    };
    if sample.is_some() {
        return Ok(Err(
            "Views containing TABLESAMPLE are not automatically updatable.",
        ));
    }
    let base = resolve_session_relation_name_if_exists(db, &relation_name(base_name)?);
    let base_columns: Vec<String> = if let Some(schema) = load_schema(db, &base)? {
        schema
            .columns
            .iter()
            .filter(|column| !column.hidden)
            .map(|column| column.name.clone())
            .collect()
    } else if let Some(base_view) = load_view(db, &base)? {
        if base_view.materialized {
            return Ok(Err(single));
        }
        base_view
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    } else {
        return Ok(Err(single));
    };
    let base_short = short_relation(&base);
    let base_sql_name = view_sql_name(&base);
    let mut old_qualifiers = vec![base_short.clone(), base_sql_name.clone()];
    if let Some(alias) = alias {
        old_qualifiers = vec![normalized(&alias.name)];
    }
    let qualifier = base_short;
    let requalify = |expr: &Expr| -> Expr {
        let mut expr = expr.clone();
        let mut visitor = Requalify {
            from: &old_qualifiers,
            to: &qualifier,
            columns: &base_columns,
            scopes: Vec::new(),
        };
        let _ = VisitMut::visit(&mut expr, &mut visitor);
        expr
    };
    let mut definitions = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for column in &base_columns {
                    definitions.push(Expr::CompoundIdentifier(vec![
                        Ident::with_quote('"', qualifier.clone()),
                        Ident::with_quote('"', column.clone()),
                    ]));
                }
            }
            SelectItem::QualifiedWildcard(_, _) => {
                for column in &base_columns {
                    definitions.push(Expr::CompoundIdentifier(vec![
                        Ident::with_quote('"', qualifier.clone()),
                        Ident::with_quote('"', column.clone()),
                    ]));
                }
            }
            SelectItem::UnnamedExpr(expr)
            | SelectItem::ExprWithAlias { expr, .. }
            | SelectItem::ExprWithAliases { expr, .. } => definitions.push(requalify(expr)),
        }
    }
    if definitions.len() < view.columns.len() {
        return Err(unsupported(format!(
            "view \"{short}\" definition no longer matches its columns"
        )));
    }
    let columns = view
        .columns
        .iter()
        .zip(definitions)
        .map(|(column, expr)| {
            let base_column = plain_base_column(&expr, &qualifier, &base_columns);
            ViewColumn {
                name: column.name.clone(),
                expr,
                base_column,
            }
        })
        .collect();
    Ok(Ok(UpdatableView {
        short,
        base_name: base_name.clone(),
        base,
        selection: select.selection.as_ref().map(requalify),
        qualifier,
        base_sql_name,
        columns,
        base_columns,
        check_option: view.check_option.as_deref().and_then(CheckOption::parse),
        owner: view.owner.clone(),
        security_invoker: view.security_invoker,
    }))
}

fn plain_base_column(expr: &Expr, qualifier: &str, base_columns: &[String]) -> Option<String> {
    let mut expr = expr;
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    let column = match expr {
        Expr::Identifier(ident) => normalized(ident),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            if normalized(&parts[0]) != qualifier {
                return None;
            }
            normalized(&parts[1])
        }
        _ => return None,
    };
    base_columns.contains(&column).then_some(column)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ViewExpression {
    Ordinary,
    ConflictTarget,
    ConflictUpdate,
    ConflictReturning,
}

/// Replaces references to a view's columns in a statement's expressions with
/// their definitions on the base relation.
struct Substitute<'a> {
    db: &'a BicDb,
    view: &'a UpdatableView,
    /// The name the statement gives the view (its alias or its name).
    target: String,
    expression: ViewExpression,
    /// One entry per enclosing subquery SELECT: its qualifiers and columns.
    scopes: Vec<(BTreeSet<String>, Option<BTreeSet<String>>)>,
    error: Option<SqlError>,
}

impl Substitute<'_> {
    fn column(&self, name: &str) -> Option<&ViewColumn> {
        self.view.columns.iter().find(|column| column.name == name)
    }

    fn fail(&mut self, error: SqlError) -> ControlFlow<()> {
        self.error = Some(error);
        ControlFlow::Break(())
    }

    fn replacement(&self, column: &ViewColumn) -> Expr {
        Expr::Nested(Box::new(column.expr.clone()))
    }

    fn shadows(&self, qualifier: &str) -> bool {
        self.scopes
            .iter()
            .any(|(qualifiers, _)| qualifiers.contains(qualifier))
    }

    fn conflict_whole_row(&self, qualifier: &str) -> bool {
        self.expression != ViewExpression::Ordinary
            && (qualifier == self.target
                || qualifier == self.view.qualifier
                || qualifier == self.view.base_sql_name
                || qualifier == "excluded")
            && !self.shadows(qualifier)
    }

    fn conflict_column_replacement(
        &mut self,
        expr: &mut Expr,
        column: &ViewColumn,
    ) -> ControlFlow<()> {
        if self.expression != ViewExpression::Ordinary && self.shadows(&self.view.qualifier) {
            return self.fail(unsupported(
                "a view column reference shadowed by a subquery is not supported in ON CONFLICT",
            ));
        }
        *expr = self.replacement(column);
        ControlFlow::Continue(())
    }

    fn excluded_replacement(&self, column: &ViewColumn) -> Result<Expr> {
        let mut expr = column.expr.clone();
        if crate::planner::expr_contains_subquery(&expr) {
            return Err(unsupported(
                "EXCLUDED view columns defined by subqueries are not supported",
            ));
        }
        let mut requalify = Requalify {
            from: std::slice::from_ref(&self.view.qualifier),
            to: "excluded",
            columns: &self.view.base_columns,
            scopes: Vec::new(),
        };
        let _ = VisitMut::visit(&mut expr, &mut requalify);
        Ok(Expr::Nested(Box::new(expr)))
    }
}

impl VisitorMut for Substitute<'_> {
    type Break = ();

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
        self.scopes
            .push((select_qualifiers(select), select_columns(self.db, select)));
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _select: &mut Select) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(ident) => {
                if self.expression == ViewExpression::ConflictUpdate
                    && ident.quote_style.is_none()
                    && ident.value.eq_ignore_ascii_case("default")
                {
                    return ControlFlow::Continue(());
                }
                let name = normalized(ident);
                // An inner FROM list that provides the name binds it first.
                let mut unknown_scope = false;
                for (_, columns) in self.scopes.iter().rev() {
                    match columns {
                        Some(columns) if columns.contains(&name) => {
                            return ControlFlow::Continue(());
                        }
                        Some(_) => {}
                        None => unknown_scope = true,
                    }
                }
                let hidden = self.view.base_columns.contains(&name);
                match self.column(&name).cloned() {
                    Some(column) => {
                        if self.expression == ViewExpression::ConflictUpdate && !unknown_scope {
                            return self.fail(SqlError::data_exception(
                                "42702",
                                format!("column reference \"{name}\" is ambiguous"),
                                None,
                            ));
                        }

                        if unknown_scope && column.base_column.as_deref() != Some(name.as_str()) {
                            return self.fail(unsupported(format!(
                                "a reference to view column \"{name}\" inside this subquery is not supported"
                            )));
                        }
                        if !unknown_scope {
                            return self.conflict_column_replacement(expr, &column);
                        }
                    }
                    None if hidden => {
                        if unknown_scope {
                            return self.fail(unsupported(format!(
                                "a reference to \"{name}\" inside this subquery is not supported on a view"
                            )));
                        }
                        return self.fail(SqlError::RaisedException {
                            sqlstate: "42703".to_string(),
                            message: format!("column \"{name}\" does not exist"),
                            detail: None,
                        });
                    }
                    None if self.conflict_whole_row(&name) => {
                        if name != self.target
                            && (name != "excluded"
                                || !matches!(self.expression, ViewExpression::ConflictUpdate))
                        {
                            return self.fail(SqlError::data_exception(
                                "42703",
                                format!("column \"{name}\" does not exist"),
                                None,
                            ));
                        }
                        return self.fail(unsupported(
                            "whole-row expressions in ON CONFLICT on a view are not supported",
                        ));
                    }
                    None => {}
                }
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let qualifier = normalized(&parts[0]);
                let column = normalized(&parts[1]);
                if self.shadows(&qualifier) {
                    return ControlFlow::Continue(());
                }
                if qualifier == "excluded" && self.expression == ViewExpression::ConflictUpdate {
                    if self.target == "excluded" {
                        return self.fail(SqlError::data_exception(
                            "42P09",
                            "table reference \"excluded\" is ambiguous",
                            None,
                        ));
                    }
                    let result = self.column(&column).map_or_else(
                        || {
                            Err(SqlError::data_exception(
                                "42703",
                                format!("column excluded.{column} does not exist"),
                                None,
                            ))
                        },
                        |column| self.excluded_replacement(column),
                    );
                    match result {
                        Ok(replacement) => *expr = replacement,
                        Err(error) => return self.fail(error),
                    }
                } else if qualifier == self.target {
                    match self.column(&column).cloned() {
                        Some(found) => return self.conflict_column_replacement(expr, &found),
                        None => {
                            return self.fail(SqlError::RaisedException {
                                sqlstate: "42703".to_string(),
                                message: format!(
                                    "column {}.{} does not exist",
                                    self.target, column
                                ),
                                detail: None,
                            });
                        }
                    }
                } else if qualifier == self.view.qualifier
                    || self.expression != ViewExpression::Ordinary
                        && (qualifier != "excluded" || qualifier == self.view.base_sql_name)
                {
                    // The base relation is not in the user's expression scope.
                    return self.fail(SqlError::RaisedException {
                        sqlstate: "42P01".to_string(),
                        message: format!("missing FROM-clause entry for table \"{qualifier}\""),
                        detail: None,
                    });
                }
            }
            Expr::CompoundIdentifier(parts)
                if self.expression != ViewExpression::Ordinary && parts.len() > 2 =>
            {
                let qualifier = normalized(&parts[parts.len() - 2]);
                if (qualifier == self.view.qualifier || qualifier == self.view.base_sql_name)
                    && qualifier != self.target
                {
                    return self.fail(SqlError::data_exception(
                        "42P01",
                        format!("missing FROM-clause entry for table \"{qualifier}\""),
                        None,
                    ));
                }
                return self.fail(unsupported(
                    "ON CONFLICT on a view supports two-part column references only",
                ));
            }
            Expr::QualifiedWildcard(name, _)
                if object_name_parts(name)
                    .last()
                    .is_some_and(|name| self.conflict_whole_row(name)) =>
            {
                return self.fail(unsupported(
                    "whole-row expressions in ON CONFLICT on a view are not supported",
                ));
            }
            Expr::Function(function)
                if self.expression != ViewExpression::Ordinary
                    && matches!(&function.args, FunctionArguments::List(args)
                    if args.args.iter().any(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(name))
                        | FunctionArg::Named { arg: FunctionArgExpr::QualifiedWildcard(name), .. }
                        | FunctionArg::ExprNamed { arg: FunctionArgExpr::QualifiedWildcard(name), .. } =>
                            object_name_parts(name).last().is_some_and(|name| self.conflict_whole_row(name)),
                        _ => false,
                    })) =>
            {
                return self.fail(unsupported(
                    "whole-row expressions in ON CONFLICT on a view are not supported",
                ));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// The level-by-level rewrite of one statement down a view chain.
struct Chain {
    /// Checks returned with the written rows: (view name, expression on the
    /// relation the statement currently targets).
    checks: Vec<(String, Expr)>,
    cascade: bool,
}

pub(super) fn reject_view_conflict_constraint(conflict: &OnConflict, view: &str) -> Result<()> {
    if let Some(target @ ConflictTarget::OnConstraint(name)) = &conflict.conflict_target {
        if !is_index_inference(target) {
            return Err(SqlError::data_exception(
                "42704",
                format!(
                    "constraint \"{}\" for table \"{}\" does not exist",
                    relation_name(name)?,
                    view_sql_name(view)
                ),
                None,
            ));
        }
    }
    Ok(())
}

impl SqlSession<'_> {
    fn substitute_expr(&self, view: &UpdatableView, target: &str, expr: &mut Expr) -> Result<()> {
        self.substitute_view_expr(view, target, expr, ViewExpression::Ordinary)
    }

    fn substitute_view_expr(
        &self,
        view: &UpdatableView,
        target: &str,
        expr: &mut Expr,
        expression: ViewExpression,
    ) -> Result<()> {
        let mut visitor = Substitute {
            db: self.db_ref(),
            view,
            target: target.to_string(),
            expression,
            scopes: Vec::new(),
            error: None,
        };
        let _ = VisitMut::visit(expr, &mut visitor);
        match visitor.error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn substitute_returning(
        &self,
        view: &UpdatableView,
        target: &str,
        items: &[SelectItem],
        expression: ViewExpression,
    ) -> Result<Vec<SelectItem>> {
        let mut out = Vec::new();
        for item in items {
            match item {
                SelectItem::Wildcard(_) => {
                    for column in &view.columns {
                        out.push(SelectItem::ExprWithAlias {
                            expr: column.expr.clone(),
                            alias: Ident::with_quote('"', column.name.clone()),
                        });
                    }
                }
                SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(name),
                    _,
                ) if object_name_parts(name)
                    .last()
                    .is_some_and(|last| last == target) =>
                {
                    for column in &view.columns {
                        out.push(SelectItem::ExprWithAlias {
                            expr: column.expr.clone(),
                            alias: Ident::with_quote('"', column.name.clone()),
                        });
                    }
                }
                SelectItem::QualifiedWildcard(..) => {
                    return Err(unsupported(
                        "this RETURNING wildcard is not supported on a view",
                    ));
                }
                SelectItem::UnnamedExpr(expr) => {
                    // Keep PostgreSQL's output name for a bare column reference.
                    let name = match expr {
                        Expr::Identifier(ident) => Some(normalized(ident)),
                        Expr::CompoundIdentifier(parts) => parts.last().map(normalized),
                        _ => None,
                    };
                    let mut substituted = expr.clone();
                    self.substitute_view_expr(view, target, &mut substituted, expression)?;
                    match name {
                        Some(name) if substituted != *expr => out.push(SelectItem::ExprWithAlias {
                            expr: substituted,
                            alias: Ident::with_quote('"', name),
                        }),
                        _ => out.push(SelectItem::UnnamedExpr(substituted)),
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let mut substituted = expr.clone();
                    self.substitute_view_expr(view, target, &mut substituted, expression)?;
                    out.push(SelectItem::ExprWithAlias {
                        expr: substituted,
                        alias: alias.clone(),
                    });
                }
                SelectItem::ExprWithAliases { .. } => {
                    return Err(unsupported(
                        "this RETURNING item is not supported on a view",
                    ));
                }
            }
        }
        Ok(out)
    }

    /// The view levels from `view` down to its base table, with the role
    /// that checks each level's base relation.
    fn updatable_view_levels(
        &self,
        dml: ViewDml,
        view: ViewSchema,
        reads: bool,
        updates_conflict: bool,
    ) -> Result<Vec<(UpdatableView, String)>> {
        let mut levels = Vec::new();
        let mut checking_role = current_user_from_gucs(&self.session_gucs);
        let mut current = view;
        loop {
            let level = match analyze_updatable_view(self.db_ref(), &current)? {
                Ok(level) => level,
                Err(detail) => {
                    return Err(not_updatable(dml, &short_relation(&current.name), detail));
                }
            };
            if matches!(dml, ViewDml::Insert | ViewDml::Update)
                && level
                    .columns
                    .iter()
                    .all(|column| column.base_column.is_none())
            {
                return Err(not_updatable(
                    dml,
                    &level.short,
                    "Views that have no updatable columns are not automatically updatable.",
                ));
            }
            // The view's owner (or, for a security_invoker view, the role
            // checking the view itself) needs the privilege on the base.
            let base_role = if level.security_invoker {
                checking_role.clone()
            } else {
                normalize_role_name(level.owner.as_deref().unwrap_or(BOOTSTRAP_ROLE_NAME))
            };
            let denied = || {
                SqlError::BicDb(BicDbError::Authorization(format!(
                    "permission denied for table {}",
                    relation_name_for_message(&level.base)
                )))
            };
            if !role_has_table_privilege(self.db_ref(), &base_role, &level.base, dml.privilege())? {
                return Err(denied());
            }
            if updates_conflict
                && !role_has_table_privilege(self.db_ref(), &base_role, &level.base, "UPDATE")?
            {
                return Err(denied());
            }
            if reads && !role_has_table_privilege(self.db_ref(), &base_role, &level.base, "SELECT")?
            {
                return Err(denied());
            }
            if levels.len() > 32 {
                return Err(unsupported("view nesting is too deep"));
            }
            let next = load_view(self.db_ref(), &level.base)?;
            checking_role = base_role.clone();
            levels.push((level, base_role));
            match next {
                Some(next) => current = next,
                None => break,
            }
        }
        // Row security of the base table applies to the role that writes it.
        let (bottom, role) = levels.last().expect("at least one level");
        if let Some(schema) = load_schema(self.db_ref(), &bottom.base)? {
            let caller = current_user_from_gucs(&self.session_gucs);
            if schema.rls_enabled && *role != caller {
                return Err(unsupported(format!(
                    "writing through view \"{}\" to table \"{}\" with row-level security as a different role is not supported",
                    levels[0].0.short,
                    short_relation(&bottom.base)
                )));
            }
        }
        Ok(levels)
    }

    fn add_level_check(chain: &mut Chain, level: &UpdatableView) {
        if level.check_option == Some(CheckOption::Cascaded) {
            chain.cascade = true;
        }
        if level.check_option.is_some() || chain.cascade {
            if let Some(selection) = &level.selection {
                chain.checks.push((level.short.clone(), selection.clone()));
            }
        }
    }

    /// Runs the rewritten statement and verifies the CHECK OPTION columns
    /// appended to its RETURNING list.
    fn finish_view_statement(
        &mut self,
        mut result: SqlResult,
        user_columns: Option<usize>,
        checks: &[String],
        verb: &str,
    ) -> Result<SqlResult> {
        let visible = user_columns.unwrap_or(0);
        if !checks.is_empty() {
            for row in &result.rows {
                // Innermost view first, as PostgreSQL checks them.
                for (index, view) in checks.iter().enumerate().rev() {
                    if row.get(visible + index) != Some(&SqlValue::Bool(true)) {
                        return Err(SqlError::RaisedException {
                            sqlstate: "44000".to_string(),
                            message: format!("new row violates check option for view \"{view}\""),
                            detail: None,
                        });
                    }
                }
            }
        }
        if user_columns.is_none() {
            if checks.is_empty() {
                // No RETURNING was added: the base statement's own tag counts.
                return Ok(result);
            }
            return Ok(SqlResult::command(format!("{verb} {}", result.rows.len())));
        }
        result.columns.truncate(visible);
        result.column_types.truncate(visible);
        result.column_metadata.truncate(visible);
        for row in &mut result.rows {
            row.truncate(visible);
        }
        Ok(result)
    }

    fn check_items(chain: &Chain) -> Vec<SelectItem> {
        chain
            .checks
            .iter()
            .enumerate()
            .map(|(index, (_, expr))| SelectItem::ExprWithAlias {
                expr: Expr::IsTrue(Box::new(Expr::Nested(Box::new(expr.clone())))),
                alias: Ident::new(format!("__bicdb_view_check_{index}")),
            })
            .collect()
    }

    fn require_view_conflict_privileges(
        &self,
        insert: &Insert,
        view: &ViewSchema,
        conflict: &OnConflict,
    ) -> Result<()> {
        let mut reads = ReferencedNames::default();
        if let Some(inference) = conflict.conflict_target.as_ref().and_then(index_inference) {
            for element in inference.elements {
                // A key that names a column does not read the whole row,
                // even if that column shares the name of the target.
                if let Expr::Identifier(column) = element.expr {
                    reads.add_name(normalized(&column));
                } else {
                    reads.add_expr(&element.expr);
                }
            }
            if let Some(predicate) = inference.predicate {
                reads.add_expr(&predicate);
            }
        }
        if let OnConflictAction::DoUpdate(update) = &conflict.action {
            self.require_update_privileges(&view.name, &update.assignments)?;
            for assignment in &update.assignments {
                reads.add_expr(&assignment.value);
            }
            if let Some(selection) = &update.selection {
                reads.add_expr(selection);
            }
        }
        let mut aliases = vec!["excluded".to_string()];
        if let Some(alias) = &insert.table_alias {
            aliases.push(normalized(&alias.alias));
        }
        self.require_select_on_names(&view.name, &reads, &aliases)
    }

    fn substitute_view_conflict(
        &self,
        level: &UpdatableView,
        target: &str,
        conflict: &mut OnConflict,
    ) -> Result<()> {
        if let Some(mut inference) = conflict.conflict_target.as_ref().and_then(index_inference) {
            for element in &mut inference.elements {
                if let Expr::Identifier(key) = &element.expr {
                    let name = normalized(key);
                    let Some(column) = level.columns.iter().find(|column| column.name == name)
                    else {
                        return Err(SqlError::data_exception(
                            "42703",
                            format!("column \"{name}\" does not exist"),
                            None,
                        ));
                    };
                    element.expr = column.base_column.as_ref().map_or_else(
                        || column.expr.clone(),
                        |base| Expr::Identifier(Ident::with_quote('"', base.clone())),
                    );
                } else {
                    self.substitute_view_expr(
                        level,
                        target,
                        &mut element.expr,
                        ViewExpression::ConflictTarget,
                    )?;
                }
            }
            if let Some(predicate) = &mut inference.predicate {
                self.substitute_view_expr(
                    level,
                    target,
                    predicate,
                    ViewExpression::ConflictTarget,
                )?;
            }
            conflict.conflict_target = Some(index_target(inference));
        }
        if let OnConflictAction::DoUpdate(update) = &mut conflict.action {
            for assignment in &mut update.assignments {
                let targets: Vec<&mut ObjectName> = match &mut assignment.target {
                    AssignmentTarget::ColumnName(name) => vec![name],
                    AssignmentTarget::Tuple(names) => names.iter_mut().collect(),
                };
                for name in targets {
                    let parts = object_name_parts(name);
                    let [column] = parts.as_slice() else {
                        return Err(unsupported(
                            "ON CONFLICT on a view supports plain column assignments only",
                        ));
                    };
                    let Some(found) = level
                        .columns
                        .iter()
                        .find(|candidate| candidate.name == *column)
                    else {
                        return Err(SqlError::UndefinedColumn {
                            table: level.short.clone(),
                            column: column.clone(),
                        });
                    };
                    let Some(base_column) = &found.base_column else {
                        return Err(column_not_updatable(ViewDml::Update, column, &level.short));
                    };
                    *name = ObjectName::from(vec![Ident::with_quote('"', base_column.clone())]);
                }
                self.substitute_view_expr(
                    level,
                    target,
                    &mut assignment.value,
                    ViewExpression::ConflictUpdate,
                )?;
            }
            if let Some(selection) = &mut update.selection {
                self.substitute_view_expr(
                    level,
                    target,
                    selection,
                    ViewExpression::ConflictUpdate,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn execute_insert_into_view(
        &mut self,
        insert: &Insert,
        view: ViewSchema,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<SqlResult> {
        if view.materialized {
            return Err(super::materialized_views::cannot_change(&view.name));
        }
        let mut conflict_reads = false;
        let mut updates_conflict = false;
        if let Some(on_insert) = &insert.on {
            let OnInsert::OnConflict(conflict) = on_insert else {
                return Err(unsupported(
                    "INSERT ON DUPLICATE KEY UPDATE is not supported",
                ));
            };
            reject_view_conflict_constraint(conflict, &view.name)?;
            self.require_view_conflict_privileges(insert, &view, conflict)?;
            conflict_reads = conflict.conflict_target.is_some();
            updates_conflict = matches!(conflict.action, OnConflictAction::DoUpdate(_));
        }
        let levels = self.updatable_view_levels(
            ViewDml::Insert,
            view,
            insert.returning.is_some() || conflict_reads,
            updates_conflict,
        )?;
        if insert.on.is_some() {
            for (level, _) in levels.iter().take(levels.len() - 1) {
                if !self.instead_of_triggers(&level.base, "insert")?.is_empty() {
                    return Err(unsupported(
                        "ON CONFLICT through nested INSTEAD OF triggers is not supported",
                    ));
                }
            }
            if updates_conflict
                && levels
                    .iter()
                    .any(|(level, _)| level.qualifier == "excluded")
            {
                return Err(unsupported(
                    "ON CONFLICT on a view over a relation named excluded is not supported",
                ));
            }
        }
        let mut statement = insert.clone();
        let mut target = match &insert.table_alias {
            Some(alias) => normalized(&alias.alias),
            None => levels[0].0.short.clone(),
        };
        statement.table_alias = None;
        let mut columns: Vec<String> = insert
            .columns
            .iter()
            .map(|name| object_name_parts(name).last().cloned().unwrap_or_default())
            .collect();
        if columns.is_empty() && insert.source.is_some() {
            let count = insert
                .source
                .as_deref()
                .and_then(source_width)
                .unwrap_or(levels[0].0.columns.len());
            columns = levels[0]
                .0
                .columns
                .iter()
                .take(count)
                .map(|column| column.name.clone())
                .collect();
        }
        let mut chain = Chain {
            checks: Vec::new(),
            cascade: false,
        };
        let mut returning = insert.returning.clone();
        let mut view_visibility = Vec::new();
        let returning_expression = if insert.on.is_some() {
            ViewExpression::ConflictReturning
        } else {
            ViewExpression::Ordinary
        };
        for (level, _) in &levels {
            if let Some(OnInsert::OnConflict(conflict)) = &mut statement.on {
                self.substitute_view_conflict(level, &target, conflict)?;
            }
            if updates_conflict {
                for (_, selection) in &mut view_visibility {
                    self.substitute_expr(level, &target, selection)?;
                }
                if let Some(selection) = &level.selection {
                    view_visibility.push((level.short.clone(), selection.clone()));
                }
            }
            let mut mapped = Vec::with_capacity(columns.len());
            for column in &columns {
                let Some(found) = level
                    .columns
                    .iter()
                    .find(|candidate| &candidate.name == column)
                else {
                    return Err(SqlError::UndefinedColumn {
                        table: level.short.clone(),
                        column: column.clone(),
                    });
                };
                let Some(base_column) = &found.base_column else {
                    return Err(column_not_updatable(ViewDml::Insert, column, &level.short));
                };
                if mapped.contains(base_column) {
                    return Err(SqlError::InvalidSql(format!(
                        "multiple assignments to same column \"{base_column}\""
                    )));
                }
                mapped.push(base_column.clone());
            }
            columns = mapped;
            if let Some(items) = &returning {
                returning =
                    Some(self.substitute_returning(level, &target, items, returning_expression)?);
            }
            let mut checks = Vec::with_capacity(chain.checks.len());
            for (name, mut expr) in std::mem::take(&mut chain.checks) {
                self.substitute_expr(level, &target, &mut expr)?;
                checks.push((name, expr));
            }
            chain.checks = checks;
            Self::add_level_check(&mut chain, level);
            target = level.qualifier.clone();
        }
        let (bottom, _) = levels.last().expect("at least one level");
        statement.table = TableObject::TableName(bottom.base_name.clone());
        if insert.on.is_some() && bottom.qualifier != bottom.base_sql_name {
            statement.table_alias = Some(TableAliasWithoutColumns {
                explicit: true,
                alias: Ident::with_quote('"', bottom.qualifier.clone()),
            });
        }
        statement.columns = columns
            .iter()
            .map(|column| ObjectName::from(vec![Ident::with_quote('"', column.clone())]))
            .collect();
        let user_columns = returning.as_ref().map(Vec::len);
        let mut items = returning.unwrap_or_default();
        items.extend(Self::check_items(&chain));
        statement.returning = (!items.is_empty()).then_some(items);
        let check_names: Vec<String> = chain.checks.iter().map(|(name, _)| name.clone()).collect();
        let base = bottom.base.clone();
        self.with_statement_transaction(|session| {
            session.view_target_preauthorized = Some(base);
            let result =
                session.execute_insert_with_view_visibility(&statement, ctes, &view_visibility);
            session.view_target_preauthorized = None;
            let result = result?;
            session.finish_view_statement(result, user_columns, &check_names, "INSERT 0")
        })
    }

    pub(crate) fn execute_update_of_view(
        &mut self,
        update: &sqlparser::ast::Update,
        view: ViewSchema,
        ctes: BTreeMap<String, CteResult>,
        match_policy: UpdateMatchPolicy,
    ) -> Result<SqlResult> {
        if view.materialized {
            return Err(super::materialized_views::cannot_change(&view.name));
        }
        if update.from.is_some() {
            return Err(unsupported("UPDATE ... FROM on a view is not supported"));
        }
        if !update.table.joins.is_empty() {
            return Err(unsupported("UPDATE of a joined view is not supported"));
        }
        let reads = update.selection.is_some() || update.returning.is_some();
        let levels = self.updatable_view_levels(ViewDml::Update, view, reads, false)?;
        let TableFactor::Table { alias, .. } = &update.table.relation else {
            return Err(unsupported("UPDATE target must be a view name"));
        };
        let mut target = alias
            .as_ref()
            .map(|alias| normalized(&alias.name))
            .unwrap_or_else(|| levels[0].0.short.clone());
        let mut statement = update.clone();
        let mut chain = Chain {
            checks: Vec::new(),
            cascade: false,
        };
        for (level, _) in &levels {
            for assignment in &mut statement.assignments {
                let targets: Vec<&mut ObjectName> = match &mut assignment.target {
                    AssignmentTarget::ColumnName(name) => vec![name],
                    AssignmentTarget::Tuple(names) => names.iter_mut().collect(),
                };
                for name in targets {
                    let parts = object_name_parts(name);
                    if parts.len() != 1 {
                        return Err(unsupported(
                            "UPDATE of a view supports plain column targets only",
                        ));
                    }
                    let column = parts[0].clone();
                    let Some(found) = level
                        .columns
                        .iter()
                        .find(|candidate| candidate.name == column)
                    else {
                        return Err(SqlError::UndefinedColumn {
                            table: level.short.clone(),
                            column,
                        });
                    };
                    let Some(base_column) = &found.base_column else {
                        return Err(column_not_updatable(ViewDml::Update, &column, &level.short));
                    };
                    *name = ObjectName::from(vec![Ident::with_quote('"', base_column.clone())]);
                }
                self.substitute_expr(level, &target, &mut assignment.value)?;
            }
            let mut selection = statement.selection.take();
            if let Some(expr) = selection.as_mut() {
                self.substitute_expr(level, &target, expr)?;
            }
            statement.selection = match (level.selection.clone(), selection) {
                (Some(view), Some(user)) => Some(Expr::BinaryOp {
                    left: Box::new(Expr::Nested(Box::new(view))),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::Nested(Box::new(user))),
                }),
                (view, user) => view.or(user),
            };
            if let Some(items) = &statement.returning {
                statement.returning = Some(self.substitute_returning(
                    level,
                    &target,
                    items,
                    ViewExpression::Ordinary,
                )?);
            }
            let mut checks = Vec::with_capacity(chain.checks.len());
            for (name, mut expr) in std::mem::take(&mut chain.checks) {
                self.substitute_expr(level, &target, &mut expr)?;
                checks.push((name, expr));
            }
            chain.checks = checks;
            Self::add_level_check(&mut chain, level);
            target = level.qualifier.clone();
        }
        let (bottom, _) = levels.last().expect("at least one level");
        statement.table = TableWithJoins {
            relation: TableFactor::Table {
                name: bottom.base_name.clone(),
                alias: None,
                args: None,
                with_hints: Vec::new(),
                version: None,
                with_ordinality: false,
                partitions: Vec::new(),
                json_path: None,
                sample: None,
                index_hints: Vec::new(),
            },
            joins: Vec::new(),
        };
        let user_columns = statement.returning.as_ref().map(Vec::len);
        let mut items = statement.returning.take().unwrap_or_default();
        items.extend(Self::check_items(&chain));
        statement.returning = (!items.is_empty()).then_some(items);
        let check_names: Vec<String> = chain.checks.iter().map(|(name, _)| name.clone()).collect();
        let base = bottom.base.clone();
        self.with_statement_transaction(|session| {
            session.view_target_preauthorized = Some(base);
            let result =
                session.execute_update_with_match_policy(&statement, ctes, match_policy, None);
            session.view_target_preauthorized = None;
            let result = result?;
            session.finish_view_statement(result, user_columns, &check_names, "UPDATE")
        })
    }

    pub(crate) fn execute_delete_from_view(
        &mut self,
        delete: &Delete,
        view: ViewSchema,
        ctes: BTreeMap<String, CteResult>,
    ) -> Result<SqlResult> {
        if view.materialized {
            return Err(super::materialized_views::cannot_change(&view.name));
        }
        if delete.using.is_some() {
            return Err(unsupported("DELETE ... USING on a view is not supported"));
        }
        let reads = delete.selection.is_some() || delete.returning.is_some();
        let levels = self.updatable_view_levels(ViewDml::Delete, view, reads, false)?;
        let mut statement = delete.clone();
        let tables = match &mut statement.from {
            FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
        };
        let [TableWithJoins { relation, joins }] = tables.as_mut_slice() else {
            return Err(unsupported("DELETE from a view supports one target"));
        };
        if !joins.is_empty() {
            return Err(unsupported("DELETE from a joined view is not supported"));
        }
        let TableFactor::Table { alias, .. } = &*relation else {
            return Err(unsupported("DELETE target must be a view name"));
        };
        let mut target = alias
            .as_ref()
            .map(|alias| normalized(&alias.name))
            .unwrap_or_else(|| levels[0].0.short.clone());
        for (level, _) in &levels {
            let mut selection = statement.selection.take();
            if let Some(expr) = selection.as_mut() {
                self.substitute_expr(level, &target, expr)?;
            }
            statement.selection = match (level.selection.clone(), selection) {
                (Some(view), Some(user)) => Some(Expr::BinaryOp {
                    left: Box::new(Expr::Nested(Box::new(view))),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::Nested(Box::new(user))),
                }),
                (view, user) => view.or(user),
            };
            if let Some(items) = &statement.returning {
                statement.returning = Some(self.substitute_returning(
                    level,
                    &target,
                    items,
                    ViewExpression::Ordinary,
                )?);
            }
            target = level.qualifier.clone();
        }
        let (bottom, _) = levels.last().expect("at least one level");
        let tables = match &mut statement.from {
            FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
        };
        tables[0].relation = TableFactor::Table {
            name: bottom.base_name.clone(),
            alias: None,
            args: None,
            with_hints: Vec::new(),
            version: None,
            with_ordinality: false,
            partitions: Vec::new(),
            json_path: None,
            sample: None,
            index_hints: Vec::new(),
        };
        let user_columns = statement.returning.as_ref().map(Vec::len);
        let base = bottom.base.clone();
        self.with_statement_transaction(|session| {
            session.view_target_preauthorized = Some(base);
            let result = session.execute_delete_with_ctes(&statement, ctes);
            session.view_target_preauthorized = None;
            let result = result?;
            session.finish_view_statement(result, user_columns, &[], "DELETE")
        })
    }

    /// Consumes the one-time authorization a view rewrite grants its base
    /// relation's statement; the view levels were checked already.
    pub(crate) fn take_view_target_preauthorization(&mut self, table: &str) -> bool {
        self.view_target_preauthorized
            .take()
            .is_some_and(|authorized| authorized == table)
    }

    /// `CREATE VIEW ... WITH CHECK OPTION` requires an automatically
    /// updatable view.
    pub(crate) fn validate_view_check_option(&self, view: &ViewSchema) -> Result<()> {
        if view.check_option.is_none() {
            return Ok(());
        }
        if analyze_updatable_view(self.db_ref(), view)?.is_err() {
            return Err(SqlError::RaisedException {
                sqlstate: "0A000".to_string(),
                message: "WITH CHECK OPTION is supported only on automatically updatable views"
                    .to_string(),
                detail: None,
            });
        }
        Ok(())
    }
}

/// The number of columns an INSERT source produces, when it is evident.
fn source_width(source: &Query) -> Option<usize> {
    match source.body.as_ref() {
        SetExpr::Values(values) => values.rows.iter().map(|row| row.len()).max(),
        SetExpr::Select(select)
            if select.projection.iter().all(|item| {
                matches!(
                    item,
                    SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }
                )
            }) =>
        {
            Some(select.projection.len())
        }
        _ => None,
    }
}

/// Splits `CREATE [OR REPLACE] VIEW ... WITH [CASCADED | LOCAL] CHECK OPTION`
/// into the statement without the clause and the option.
pub(crate) fn strip_view_check_option(sql: &str) -> Option<(String, CheckOption)> {
    let trimmed = sql.trim_end().trim_end_matches(';').trim_end();
    let upper = trimmed.to_ascii_uppercase();
    let head = upper.trim_start();
    if !head.starts_with("CREATE") || !upper.ends_with("CHECK OPTION") {
        return None;
    }
    let before = upper[..upper.len() - "CHECK OPTION".len()].trim_end();
    let (before, option) = if let Some(rest) = before.strip_suffix("CASCADED") {
        (rest.trim_end(), CheckOption::Cascaded)
    } else if let Some(rest) = before.strip_suffix("LOCAL") {
        (rest.trim_end(), CheckOption::Local)
    } else {
        (before, CheckOption::Cascaded)
    };
    let before = before.strip_suffix("WITH")?;
    if !before.ends_with(|c: char| c.is_whitespace() || c == ')') {
        return None;
    }
    Some((trimmed[..before.len()].trim_end().to_string(), option))
}

/// (is_updatable, is_insertable_into, check_option) as
/// `information_schema.views` reports them.
pub(crate) fn view_updatability(db: &BicDb, view: &ViewSchema) -> (bool, bool, &'static str) {
    let check = match view.check_option.as_deref().and_then(CheckOption::parse) {
        Some(CheckOption::Local) => "LOCAL",
        Some(CheckOption::Cascaded) => "CASCADED",
        None => "NONE",
    };
    match analyze_updatable_view(db, view) {
        Ok(Ok(level)) => {
            let insertable = level
                .columns
                .iter()
                .any(|column| column.base_column.is_some());
            // UPDATE needs an updatable column as INSERT does; DELETE alone
            // does not make a view "updatable" here.
            (insertable, insertable, check)
        }
        _ => (false, false, check),
    }
}
