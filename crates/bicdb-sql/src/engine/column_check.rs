//! Name resolution for column references, checked before a statement runs.
//!
//! PostgreSQL resolves every column reference while it plans a statement and
//! raises 42703 (or 42P01 for an unknown qualifier) for a name no relation in
//! scope provides. Row evaluation here falls back to NULL for a name it cannot
//! find, so without this check a misspelled column in WHERE, JOIN ON, ORDER BY
//! or GROUP BY — or anywhere over a derived table, CTE or table function —
//! silently matched nothing (and `UPDATE ... WHERE typo` updated nothing).
//!
//! The check is deliberately conservative: a relation whose columns cannot be
//! determined statically (catalog views, unknown table functions, derived
//! tables the output-column inference cannot type) makes its scope "open",
//! and nothing is rejected that an open scope might provide.
use super::*;
use crate::*;
use sqlparser::ast::{Visit, Visitor};
use std::ops::ControlFlow;

#[derive(Clone, Debug)]
struct ScopeRelation {
    alias: String,
    /// `None`: the relation's columns are not known statically.
    columns: Option<Vec<String>>,
    dependency: Option<std::sync::Arc<DependencyRelation>>,
}

#[derive(Debug)]
struct DependencyRelation {
    table: String,
    columns: Vec<(String, i64)>,
}

#[derive(Default)]
struct ViewDependencies {
    relations: BTreeSet<String>,
    columns: BTreeSet<(String, i64)>,
}

thread_local! {
    static VIEW_DEPENDENCIES: RefCell<Option<ViewDependencies>> = const { RefCell::new(None) };
}

fn collecting_view_dependencies() -> bool {
    VIEW_DEPENDENCIES.with(|slot| slot.borrow().is_some())
}

/// Observe the same scoped walk used for column binding, without executing a
/// view or expanding its sources into transitive (rather than direct) edges.
pub(crate) fn view_column_dependencies(
    db: &BicDb,
    query: &Query,
) -> Result<BTreeSet<(String, i64)>> {
    struct Restore(Option<ViewDependencies>);
    impl Drop for Restore {
        fn drop(&mut self) {
            VIEW_DEPENDENCIES.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore =
        Restore(VIEW_DEPENDENCIES.with(|slot| slot.replace(Some(ViewDependencies::default()))));
    SqlEngine::new(db).check_query_columns(query, &[])?;
    let mut collected = VIEW_DEPENDENCIES.with(|slot| slot.borrow_mut().take().unwrap());
    for table in collected.relations {
        if !collected.columns.iter().any(|(name, _)| name == &table) {
            collected.columns.insert((table, 0));
        }
    }
    Ok(collected.columns)
}

fn dependency_ident_matches(name: &str, ident: &Ident) -> bool {
    name == ident_value(ident)
}

fn dependency_system_column(column: &Ident) -> Option<i64> {
    match ident_value(column).as_str() {
        "ctid" => Some(-1),
        "xmin" => Some(-2),
        "cmin" => Some(-3),
        "xmax" => Some(-4),
        "cmax" => Some(-5),
        "tableoid" => Some(-6),
        _ => None,
    }
}

fn dependency_relation_has_column(relation: &ScopeRelation, column: &Ident) -> bool {
    match &relation.dependency {
        Some(source) => {
            source
                .columns
                .iter()
                .any(|(name, _)| dependency_ident_matches(name, column))
                || dependency_system_column(column).is_some()
        }
        None => relation.columns.as_ref().is_some_and(|columns| {
            columns
                .iter()
                .any(|name| dependency_ident_matches(name, column))
        }),
    }
}

fn record_dependency(relation: &ScopeRelation, column: Option<&Ident>) {
    let Some(source) = &relation.dependency else {
        return;
    };
    VIEW_DEPENDENCIES.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(dependencies) = slot.as_mut() else {
            return;
        };
        if let Some(column) = column {
            if let Some(number) = dependency_system_column(column) {
                dependencies.columns.insert((source.table.clone(), number));
            }
            for (name, number) in &source.columns {
                if dependency_ident_matches(name, column) {
                    dependencies.columns.insert((source.table.clone(), *number));
                }
            }
        } else {
            dependencies.columns.insert((source.table.clone(), 0));
        }
    });
}

fn record_wildcard_dependencies(scope: &Scope, qualifier: Option<&str>) {
    if !collecting_view_dependencies() {
        return;
    }
    VIEW_DEPENDENCIES.with(|slot| {
        let mut slot = slot.borrow_mut();
        let dependencies = slot.as_mut().unwrap();
        for relation in &scope.relations {
            if qualifier.is_some_and(|qualifier| relation.alias != qualifier) {
                continue;
            }
            if let Some(source) = &relation.dependency {
                for (_, number) in &source.columns {
                    dependencies.columns.insert((source.table.clone(), *number));
                }
            }
        }
    });
}

fn record_reference_dependencies(parts: &[Ident], scope: &Scope, outer: &[Scope], outputs: bool) {
    if !collecting_view_dependencies() {
        return;
    }
    let Some(column) = parts.last() else {
        return;
    };
    if parts.len() == 1
        && outputs
        && scope
            .output_names
            .iter()
            .any(|name| dependency_ident_matches(name, column))
    {
        return;
    }
    for scope in std::iter::once(scope).chain(outer) {
        if parts.len() == 1 {
            let matches = scope
                .relations
                .iter()
                .filter(|relation| dependency_relation_has_column(relation, column))
                .collect::<Vec<_>>();
            if !matches.is_empty() {
                for relation in matches {
                    record_dependency(relation, Some(column));
                }
                return;
            }
            if let Some(relation) = scope
                .relations
                .iter()
                .find(|relation| dependency_ident_matches(&relation.alias, column))
            {
                record_dependency(relation, None);
                return;
            }
            if scope.is_open() {
                return;
            }
        } else {
            let qualifier = &parts[parts.len() - 2];
            let qualified_table = (parts.len() == 3)
                .then(|| {
                    relation_name_from_parts(
                        &parts[..2].iter().map(ident_value).collect::<Vec<_>>(),
                    )
                    .ok()
                })
                .flatten();
            let matches = scope
                .relations
                .iter()
                .filter(|relation| {
                    if let Some(table) = &qualified_table {
                        relation
                            .dependency
                            .as_ref()
                            .is_some_and(|source| &source.table == table)
                    } else {
                        dependency_ident_matches(&relation.alias, qualifier)
                    }
                })
                .collect::<Vec<_>>();
            if !matches.is_empty() {
                for relation in matches {
                    record_dependency(relation, Some(column));
                }
                return;
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Scope {
    relations: Vec<ScopeRelation>,
    /// Select-list output names, visible to ORDER BY / GROUP BY / HAVING.
    output_names: Vec<String>,
}

impl Scope {
    fn is_open(&self) -> bool {
        self.relations
            .iter()
            .any(|relation| relation.columns.is_none())
    }
}

/// Names that are not column references (SQL value functions written without
/// parentheses, literals the parser can surface as identifiers).
const NON_COLUMN_NAMES: &[&str] = &[
    "current_date",
    "current_time",
    "current_timestamp",
    "localtime",
    "localtimestamp",
    "current_user",
    "session_user",
    "user",
    "current_role",
    "current_schema",
    "current_catalog",
    "default",
    "null",
    "true",
    "false",
];

/// System columns every base table carries.
const SYSTEM_COLUMNS: &[&str] = &["ctid", "xmin", "xmax", "cmin", "cmax", "tableoid", "oid"];

/// Qualifiers that name rule/trigger pseudo relations rather than FROM items.
const PSEUDO_QUALIFIERS: &[&str] = &["new", "old", "excluded"];

/// Set `BICDB_STRICT_COLUMN_REFERENCES=0` to disable the check (escape hatch).
pub(crate) fn column_reference_check_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var("BICDB_STRICT_COLUMN_REFERENCES").as_deref(),
            Ok("0" | "off" | "false" | "no")
        )
    })
}

fn same_name(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn undefined_column(name: &str) -> SqlError {
    SqlError::data_exception_public("42703", format!("column \"{name}\" does not exist"), None)
}

fn missing_from_entry(qualifier: &str) -> SqlError {
    SqlError::data_exception_public(
        "42P01",
        format!("missing FROM-clause entry for table \"{qualifier}\""),
        None,
    )
}

/// Column references and subqueries of one expression, not descending into
/// the subqueries (they have their own scope). Owned copies: the check runs
/// once per cached statement template, not per execution.
#[derive(Default)]
struct ReferenceCollector {
    depth: usize,
    references: Vec<Vec<Ident>>,
    subqueries: Vec<Query>,
    /// Field names of `(expr).field` access chains: not column references.
    field_names: std::collections::HashSet<*const Expr>,
}

impl Visitor for ReferenceCollector {
    type Break = ();
    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if self.depth == 0 {
            self.subqueries.push(query.clone());
        }
        self.depth += 1;
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if let Expr::CompoundFieldAccess { access_chain, .. } = expr {
            for access in access_chain {
                if let AccessExpr::Dot(field) = access {
                    self.field_names.insert(field as *const Expr);
                }
            }
        }
        // `f(name => value)`: the argument name is not a column reference.
        if let Expr::Function(function) = expr {
            if let FunctionArguments::List(list) = &function.args {
                for arg in &list.args {
                    if let FunctionArg::ExprNamed { name, .. } = arg {
                        self.field_names.insert(name as *const Expr);
                    }
                }
            }
        }
        if self.field_names.contains(&(expr as *const Expr)) {
            return ControlFlow::Continue(());
        }
        if self.depth == 0 {
            match expr {
                Expr::Identifier(ident) => self.references.push(vec![ident.clone()]),
                Expr::CompoundIdentifier(idents) => self.references.push(idents.clone()),
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }
}

/// Column name a CTE carries when its output columns cannot be inferred;
/// relations that expose it are treated as having unknown columns.
const UNKNOWN_CTE_COLUMN: &str = "\u{0}bicdb_unknown_cte_columns";

impl SqlEngine<'_> {
    /// Reject column references no relation in scope provides (42703/42P01).
    pub(crate) fn check_statement_column_references(&self, statement: &Statement) -> Result<()> {
        if !column_reference_check_enabled() {
            return Ok(());
        }
        match statement {
            Statement::Query(query) => self.check_query_columns(query, &[]),
            Statement::Insert(insert) => match &insert.source {
                Some(source) => self.check_query_columns(source, &[]),
                None => Ok(()),
            },
            Statement::Update(update) => {
                let mut scope = Scope::default();
                let mut deferred = Vec::new();
                self.scope_table_with_joins(&update.table, &[], &mut scope, &mut deferred)?;
                if let Some(
                    UpdateTableFromKind::BeforeSet(from) | UpdateTableFromKind::AfterSet(from),
                ) = &update.from
                {
                    for table in from {
                        self.scope_table_with_joins(table, &[], &mut scope, &mut deferred)?;
                    }
                }
                self.check_deferred(&deferred, &scope, &[])?;
                for assignment in &update.assignments {
                    self.check_expr_columns(&assignment.value, &scope, &[], false)?;
                }
                if let Some(selection) = &update.selection {
                    self.check_expr_columns(selection, &scope, &[], false)?;
                }
                Ok(())
            }
            Statement::Delete(delete) => {
                let from = match &delete.from {
                    FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
                };
                let mut scope = Scope::default();
                let mut deferred = Vec::new();
                for table in from.iter().chain(delete.using.iter().flatten()) {
                    self.scope_table_with_joins(table, &[], &mut scope, &mut deferred)?;
                }
                self.check_deferred(&deferred, &scope, &[])?;
                if let Some(selection) = &delete.selection {
                    self.check_expr_columns(selection, &scope, &[], false)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn check_query_columns(&self, query: &Query, outer: &[Scope]) -> Result<()> {
        let Some(with) = &query.with else {
            return self.check_query_body(query, outer);
        };
        // CTE bodies are checked in order; each later CTE and the main body
        // see the earlier ones' output columns.
        let mut engine = self
            .inherit_transaction(SqlEngine::with_ctes_and_context(
                self.db_ref(),
                self.settings,
                self.ctes.clone(),
                self.security_context.clone(),
                self.session_gucs.clone(),
            ))
            .with_shared_routine_vars(self.routine_vars.clone());
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.clone();
            let anchor_body = match cte.query.body.as_ref() {
                SetExpr::SetOperation {
                    left,
                    op: SetOperator::Union,
                    ..
                } if with.recursive => Some(left.as_ref()),
                _ if with.recursive => None,
                body => Some(body),
            };
            let anchor = anchor_body.and_then(|body| {
                let inferred = if with.recursive {
                    engine.set_expr_output_columns(body)
                } else {
                    engine.query_output_columns(&cte.query)
                };
                output_names_of(body, inferred)
            });
            let columns = anchor.and_then(|names| {
                rename_relation_columns(
                    names.into_iter().map(|name| (name, None)).collect(),
                    &cte.alias.columns,
                )
            });
            let collect_nonrecursive_body = collecting_view_dependencies() && !with.recursive;
            if collect_nonrecursive_body {
                // A non-recursive CTE's own name is not visible in its body.
                engine.check_query_columns(&cte.query, outer)?;
            }
            match columns {
                Some(columns) => {
                    let (names, types): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
                    engine.ctes.insert(
                        cte_key(&name),
                        CteResult::new(name.clone(), names, Vec::new()).with_column_types(types),
                    );
                    if !collect_nonrecursive_body {
                        engine.check_query_columns(&cte.query, outer)?;
                    }
                }
                None => {
                    // Unknown output columns: references to this CTE stay
                    // unchecked, and the body (recursive or not) is still
                    // checked with the CTE visible as an open relation.
                    engine.ctes.insert(
                        cte_key(&name),
                        CteResult::new(
                            name.clone(),
                            vec![UNKNOWN_CTE_COLUMN.to_string()],
                            Vec::new(),
                        ),
                    );
                    if !collect_nonrecursive_body {
                        engine.check_query_columns(&cte.query, outer)?;
                    }
                }
            }
        }
        engine.check_query_body(query, outer)
    }

    fn check_query_body(&self, query: &Query, outer: &[Scope]) -> Result<()> {
        match query.body.as_ref() {
            SetExpr::Select(select) => {
                self.check_select_columns(select, query.order_by.as_ref(), outer)
            }
            body => {
                self.check_set_expr_columns(body, outer)?;
                // ORDER BY over a set operation sees only the output columns.
                if let Some(order_by) = &query.order_by {
                    let output = self.set_expr_output_columns(body);
                    let scope = Scope {
                        relations: vec![ScopeRelation {
                            alias: String::new(),
                            dependency: None,
                            columns: output
                                .map(|columns| columns.into_iter().map(|(name, _)| name).collect()),
                        }],
                        output_names: Vec::new(),
                    };
                    if let OrderByKind::Expressions(exprs) = &order_by.kind {
                        for order in exprs {
                            self.check_expr_columns(&order.expr, &scope, outer, true)?;
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn check_set_expr_columns(&self, body: &SetExpr, outer: &[Scope]) -> Result<()> {
        match body {
            SetExpr::Select(select) => self.check_select_columns(select, None, outer),
            SetExpr::Query(query) => self.check_query_columns(query, outer),
            SetExpr::SetOperation { left, right, .. } => {
                self.check_set_expr_columns(left, outer)?;
                self.check_set_expr_columns(right, outer)
            }
            SetExpr::Values(values) => {
                let scope = Scope::default();
                for row in &values.rows {
                    for expr in &row.content {
                        self.check_expr_columns(expr, &scope, outer, false)?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn check_select_columns(
        &self,
        select: &Select,
        order_by: Option<&OrderBy>,
        outer: &[Scope],
    ) -> Result<()> {
        let mut scope = Scope::default();
        let mut deferred = Vec::new();
        for table in &select.from {
            self.scope_table_with_joins(table, outer, &mut scope, &mut deferred)?;
        }
        self.check_deferred(&deferred, &scope, outer)?;
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    self.check_expr_columns(expr, &scope, outer, false)?;
                    if let Expr::Identifier(ident) = expr {
                        scope.output_names.push(ident.value.clone());
                    }
                    if let Expr::CompoundIdentifier(idents) = expr {
                        if let Some(last) = idents.last() {
                            scope.output_names.push(last.value.clone());
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    self.check_expr_columns(expr, &scope, outer, false)?;
                    scope.output_names.push(alias.value.clone());
                }
                SelectItem::Wildcard(_) => {
                    record_wildcard_dependencies(&scope, None);
                }
                SelectItem::QualifiedWildcard(
                    sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                    _,
                ) => {
                    let parts = object_name_parts(name);
                    record_wildcard_dependencies(&scope, parts.last().map(String::as_str));
                }
                _ => {}
            }
        }
        if let Some(sqlparser::ast::Distinct::On(exprs)) = &select.distinct {
            for expr in exprs {
                self.check_expr_columns(expr, &scope, outer, true)?;
            }
        }
        if let Some(selection) = &select.selection {
            self.check_expr_columns(selection, &scope, outer, false)?;
        }
        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            for expr in exprs {
                // GROUP BY prefers an input column over an output alias.
                if collecting_view_dependencies() {
                    let mut collector = ReferenceCollector::default();
                    let _ = expr.visit(&mut collector);
                    for parts in collector.references {
                        record_reference_dependencies(&parts, &scope, outer, false);
                    }
                }
                self.check_expr_columns(expr, &scope, outer, true)?;
            }
        }
        if let Some(having) = &select.having {
            self.check_expr_columns(having, &scope, outer, true)?;
        }
        if let Some(qualify) = &select.qualify {
            self.check_expr_columns(qualify, &scope, outer, true)?;
        }
        if let Some(order_by) = order_by {
            if let OrderByKind::Expressions(exprs) = &order_by.kind {
                for order in exprs {
                    self.check_expr_columns(&order.expr, &scope, outer, true)?;
                }
            }
        }
        Ok(())
    }

    /// Add a FROM item (and its joins) to `scope`; join conditions are
    /// deferred until every relation of the FROM list is in scope.
    fn scope_table_with_joins<'q>(
        &self,
        table: &'q TableWithJoins,
        outer: &[Scope],
        scope: &mut Scope,
        deferred: &mut Vec<&'q Expr>,
    ) -> Result<()> {
        self.scope_table_factor(&table.relation, outer, scope, deferred)?;
        for join in &table.joins {
            let left_columns = collecting_view_dependencies().then(|| {
                scope
                    .relations
                    .iter()
                    .flat_map(|relation| relation.columns.iter().flatten().cloned())
                    .collect::<BTreeSet<_>>()
            });
            let left_relations = scope.relations.len();
            self.scope_table_factor(&join.relation, outer, scope, deferred)?;
            if let Some(JoinConstraint::On(expr)) = join_constraint(&join.join_operator) {
                deferred.push(expr);
            }
            if collecting_view_dependencies() {
                let names = match join_constraint(&join.join_operator) {
                    Some(JoinConstraint::Using(names)) => names
                        .iter()
                        .filter_map(|name| object_name_parts(name).last().cloned())
                        .collect::<BTreeSet<_>>(),
                    Some(JoinConstraint::Natural) => scope.relations[left_relations..]
                        .iter()
                        .flat_map(|relation| relation.columns.iter().flatten())
                        .filter(|name| left_columns.as_ref().unwrap().contains(*name))
                        .cloned()
                        .collect(),
                    _ => BTreeSet::new(),
                };
                for name in names {
                    let ident = Ident::with_quote('"', name);
                    for relation in &scope.relations {
                        record_dependency(relation, Some(&ident));
                    }
                }
            }
        }
        Ok(())
    }

    fn check_deferred(&self, deferred: &[&Expr], scope: &Scope, outer: &[Scope]) -> Result<()> {
        for expr in deferred {
            self.check_expr_columns(expr, scope, outer, false)?;
        }
        Ok(())
    }

    fn scope_table_factor<'q>(
        &self,
        factor: &'q TableFactor,
        outer: &[Scope],
        scope: &mut Scope,
        deferred: &mut Vec<&'q Expr>,
    ) -> Result<()> {
        match factor {
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                return self.scope_table_with_joins(table_with_joins, outer, scope, deferred);
            }
            TableFactor::Derived {
                lateral, subquery, ..
            } => {
                // A LATERAL subquery also sees the FROM items before it.
                if *lateral {
                    let mut lateral_outer = vec![scope.clone()];
                    lateral_outer.extend_from_slice(outer);
                    self.check_query_columns(subquery, &lateral_outer)?;
                } else {
                    self.check_query_columns(subquery, outer)?;
                }
            }
            _ => {}
        }
        let alias = table_factor_alias(factor);
        let source = if collecting_view_dependencies() {
            if let TableFactor::Table {
                name, args: None, ..
            } = factor
            {
                let name = relation_name(name)?;
                if self.cte(&name).is_some() {
                    None
                } else if let Some(schema) = load_schema(self.db_ref(), &name)? {
                    Some((
                        name,
                        schema
                            .columns
                            .iter()
                            .enumerate()
                            .filter(|(_, column)| !column.hidden)
                            .map(|(index, column)| (column.name.clone(), index as i64 + 1))
                            .collect::<Vec<_>>(),
                    ))
                } else {
                    load_view(self.db_ref(), &name)?.map(|view| {
                        (
                            name,
                            view.columns
                                .iter()
                                .enumerate()
                                .filter(|(_, column)| !column.hidden)
                                .map(|(index, column)| (column.name.clone(), index as i64 + 1))
                                .collect::<Vec<_>>(),
                        )
                    })
                }
            } else {
                None
            }
        } else {
            None
        };
        if let Some((name, _)) = &source {
            VIEW_DEPENDENCIES.with(|slot| {
                slot.borrow_mut()
                    .as_mut()
                    .unwrap()
                    .relations
                    .insert(name.clone())
            });
        }
        // Catalog and virtual relations, CTEs without known columns and
        // anything the column inference does not cover stay open.
        let virtual_relation = matches!(factor, TableFactor::Table { name, args: None, .. }
            if relation_name(name).is_ok_and(|table| is_virtual_table(&table)
                || table.starts_with("pg_") || table.starts_with("information_schema")));
        let mut env = Vec::new();
        let known = !virtual_relation
            && self.collect_relation_columns(factor, &mut env).is_some()
            && !env.iter().any(|relation| {
                relation
                    .columns
                    .iter()
                    .any(|(name, _)| name == UNKNOWN_CTE_COLUMN)
            });
        if !known {
            scope.relations.push(ScopeRelation {
                alias: alias.unwrap_or_default(),
                columns: None,
                dependency: None,
            });
            return Ok(());
        }
        if let TableFactor::Derived {
            subquery, alias, ..
        } = factor
        {
            if alias.as_ref().is_none_or(|alias| alias.columns.is_empty()) {
                let inferred = env.first().map(|relation| relation.columns.clone());
                match (
                    env.first_mut(),
                    output_names_of(subquery.body.as_ref(), inferred),
                ) {
                    (Some(relation), Some(names)) => {
                        relation.columns = names.into_iter().map(|name| (name, None)).collect();
                    }
                    _ => {
                        scope.relations.push(ScopeRelation {
                            alias: table_factor_alias(factor).unwrap_or_default(),
                            columns: None,
                            dependency: None,
                        });
                        return Ok(());
                    }
                }
            }
        }
        let base_table = env.iter().any(|relation| relation.row_type.is_some());
        for relation in env {
            let mut columns: Vec<String> =
                relation.columns.into_iter().map(|(name, _)| name).collect();
            let dependency = source.as_ref().map(|(table, original)| {
                std::sync::Arc::new(DependencyRelation {
                    table: table.clone(),
                    columns: columns
                        .iter()
                        .zip(original)
                        .map(|(name, (_, number))| (name.clone(), *number))
                        .collect(),
                })
            });
            if base_table {
                columns.extend(SYSTEM_COLUMNS.iter().map(|name| (*name).to_string()));
                // Record fields every BicDB collection row carries.
                columns.extend(
                    [
                        "id",
                        "metadata",
                        "timestamp",
                        "payload",
                        "vector",
                        "geometry",
                    ]
                    .iter()
                    .map(|name| (*name).to_string()),
                );
            }
            // An aliased base table stays reachable under its own name too
            // (the executor materializes both spellings).
            if let Some(table) = &relation.row_type {
                if !same_name(table, &relation.alias) {
                    scope.relations.push(ScopeRelation {
                        alias: table.clone(),
                        columns: Some(columns.clone()),
                        dependency: dependency.clone(),
                    });
                }
            }
            scope.relations.push(ScopeRelation {
                alias: if collecting_view_dependencies() {
                    alias.clone().unwrap_or(relation.alias)
                } else {
                    relation.alias
                },
                columns: Some(columns),
                dependency,
            });
        }
        Ok(())
    }

    fn check_expr_columns(
        &self,
        expr: &Expr,
        scope: &Scope,
        outer: &[Scope],
        output_names_visible: bool,
    ) -> Result<()> {
        let mut collected = ReferenceCollector::default();
        let _ = expr.visit(&mut collected);
        for parts in &collected.references {
            record_reference_dependencies(parts, scope, outer, output_names_visible);
            let parts: Vec<&Ident> = parts.iter().collect();
            self.check_reference(&parts, scope, outer, output_names_visible)?;
        }
        if !collected.subqueries.is_empty() {
            let mut nested_outer = vec![scope.clone()];
            nested_outer.extend_from_slice(outer);
            for subquery in &collected.subqueries {
                self.check_query_columns(subquery, &nested_outer)?;
            }
        }
        Ok(())
    }

    fn check_reference(
        &self,
        parts: &[&Ident],
        scope: &Scope,
        outer: &[Scope],
        output_names_visible: bool,
    ) -> Result<()> {
        let scopes = std::iter::once(scope).chain(outer.iter());
        match parts {
            [ident] => {
                let name = ident.value.as_str();
                if NON_COLUMN_NAMES
                    .iter()
                    .any(|candidate| same_name(candidate, name))
                    || self.session_identity_value(name).is_some()
                    || routine_var_from_name(&self.routine_vars, name).is_some()
                {
                    return Ok(());
                }
                if output_names_visible
                    && scope
                        .output_names
                        .iter()
                        .any(|alias| same_name(alias, name))
                {
                    return Ok(());
                }
                for scope in scopes {
                    for relation in &scope.relations {
                        match &relation.columns {
                            None => return Ok(()),
                            Some(columns)
                                if columns.iter().any(|column| same_name(column, name)) =>
                            {
                                return Ok(());
                            }
                            // A bare relation name is a whole-row reference.
                            Some(_) if same_name(&relation.alias, name) => return Ok(()),
                            Some(_) => {}
                        }
                    }
                }
                // One relation in scope: name it, like the existing column errors.
                match scope.relations.as_slice() {
                    [relation] if !relation.alias.is_empty() => Err(SqlError::UndefinedColumn {
                        table: relation.alias.clone(),
                        column: name.to_string(),
                    }),
                    _ => Err(undefined_column(name)),
                }
            }
            [.., qualifier, column] if parts.len() <= 3 => {
                let (qualifier, column) = (qualifier.value.as_str(), column.value.as_str());
                if PSEUDO_QUALIFIERS
                    .iter()
                    .any(|candidate| same_name(candidate, qualifier))
                    || routine_var_from_name(&self.routine_vars, qualifier).is_some()
                {
                    return Ok(());
                }
                let mut any_open = false;
                for scope in scopes {
                    any_open |= scope.is_open();
                    for relation in scope
                        .relations
                        .iter()
                        .filter(|relation| same_name(&relation.alias, qualifier))
                    {
                        return match &relation.columns {
                            None => Ok(()),
                            Some(columns) if columns.iter().any(|name| same_name(name, column)) => {
                                Ok(())
                            }
                            Some(_) => Err(SqlError::UndefinedColumn {
                                table: relation.alias.clone(),
                                column: column.to_string(),
                            }),
                        };
                    }
                }
                if any_open {
                    return Ok(());
                }
                // `composite_column.field` written without parentheses.
                let is_column = std::iter::once(scope).chain(outer.iter()).any(|scope| {
                    scope.relations.iter().any(|relation| {
                        relation.columns.as_ref().is_some_and(|columns| {
                            columns.iter().any(|name| same_name(name, qualifier))
                        })
                    })
                });
                if is_column || parts.len() == 3 {
                    return Ok(());
                }
                Err(missing_from_entry(qualifier))
            }
            _ => Ok(()),
        }
    }
}

/// The name a FROM item is referenced by: its alias, else its own name.
fn table_factor_alias(factor: &TableFactor) -> Option<String> {
    let alias = match factor {
        TableFactor::Table { alias, .. }
        | TableFactor::Derived { alias, .. }
        | TableFactor::Function { alias, .. }
        | TableFactor::UNNEST { alias, .. }
        | TableFactor::TableFunction { alias, .. }
        | TableFactor::NestedJoin { alias, .. } => alias.as_ref(),
        _ => None,
    };
    if let Some(alias) = alias {
        return Some(alias.name.value.clone());
    }
    match factor {
        TableFactor::Table { name, .. } => Some(relation_default_alias(name)),
        _ => None,
    }
}

/// Output column names of a query body by PostgreSQL's rules where they are
/// unambiguous: an alias, or a plain column reference's name. Other items take
/// the inferred name. The inference cannot see outer scopes, so it misnames a
/// correlated `SELECT outer.col` (a recursive CTE anchor over the enclosing
/// row); a list it cannot align with the select list is unknown (`None`).
fn output_names_of(
    body: &SetExpr,
    inferred: Option<Vec<(String, Option<String>)>>,
) -> Option<Vec<String>> {
    let select = match body {
        SetExpr::Select(select) => select,
        SetExpr::SetOperation { left, .. } => return output_names_of(left, inferred),
        SetExpr::Query(query) => return output_names_of(query.body.as_ref(), inferred),
        _ => return inferred.map(|columns| columns.into_iter().map(|(name, _)| name).collect()),
    };
    if select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    }) {
        return inferred.map(|columns| columns.into_iter().map(|(name, _)| name).collect());
    }
    let inferred = inferred.filter(|columns| columns.len() == select.projection.len());
    select
        .projection
        .iter()
        .enumerate()
        .map(|(index, item)| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_ascii_lowercase()),
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                Some(ident.value.to_ascii_lowercase())
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                idents.last().map(|ident| ident.value.to_ascii_lowercase())
            }
            _ => inferred.as_ref().map(|columns| columns[index].0.clone()),
        })
        .collect()
}
