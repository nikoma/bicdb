//! `search_path` name resolution for the PostgreSQL profile.
//!
//! Relations outside `public` are stored under schema-encoded physical names,
//! so an unqualified name only ever reached `public`. PostgreSQL resolves an
//! unqualified relation through the session's `search_path` (with `$user`
//! expanding to the current role): the first schema on the path that holds
//! the relation wins, and a new relation is created in the first schema of
//! the path that exists. This module binds unqualified relation references to
//! that schema before execution, for statement text and for routine bodies
//! (which PostgreSQL resolves with the caller's path at execution time).
//!
//! When the path names no existing schema other than `public`, nothing is
//! rewritten and the previous behaviour is unchanged.
use crate::*;
use std::sync::Arc;

thread_local! {
    /// The binding path of the statement executing on this thread, so routine
    /// bodies compiled during it bind with the caller's path.
    static ACTIVE_PATH: RefCell<Option<Arc<[String]>>> = const { RefCell::new(None) };
}

/// Restores the previously active path when the statement ends.
pub(crate) struct PathScope(Option<Arc<[String]>>);

impl PathScope {
    pub(crate) fn enter(path: Option<Arc<[String]>>) -> Self {
        Self(ACTIVE_PATH.with(|active| std::mem::replace(&mut *active.borrow_mut(), path)))
    }
}

impl Drop for PathScope {
    fn drop(&mut self) {
        let previous = self.0.take();
        ACTIVE_PATH.with(|active| *active.borrow_mut() = previous);
    }
}

pub(crate) fn active_path() -> Option<Arc<[String]>> {
    ACTIVE_PATH.with(|active| active.borrow().clone())
}

/// Schemas of a `search_path` value in order, with `$user` expanded and
/// unquoted names folded to lower case.
pub(crate) fn parse_search_path(path: &str, user: &str) -> Vec<String> {
    let mut schemas = Vec::new();
    for part in path.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let schema = if let Some(quoted) = part
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
        {
            quoted.replace("\"\"", "\"")
        } else if let Some(quoted) = part
            .strip_prefix('\'')
            .and_then(|rest| rest.strip_suffix('\''))
        {
            quoted.to_string()
        } else {
            part.to_ascii_lowercase()
        };
        let schema = if schema == "$user" {
            user.to_string()
        } else {
            schema
        };
        if !schemas.contains(&schema) {
            schemas.push(schema);
        }
    }
    schemas
}

/// The schemas unqualified relations bind through, or `None` when binding
/// would change nothing (no existing schema other than `public` on the path,
/// or a non-PostgreSQL profile, which binds names itself).
pub(crate) fn binding_path(
    db: &BicDb,
    gucs: &HashMap<String, String>,
) -> Result<Option<Arc<[String]>>> {
    if gucs
        .get("bicdb.sql_profile")
        .is_some_and(|profile| profile != oracle::POSTGRES_PROFILE)
    {
        return Ok(None);
    }
    let path = gucs
        .get("search_path")
        .map(String::as_str)
        .unwrap_or("\"$user\", public");
    let user = current_user_from_gucs(gucs);
    let mut schemas = Vec::new();
    let mut other = false;
    for schema in parse_search_path(path, &user) {
        if matches!(schema.as_str(), "pg_catalog" | "information_schema")
            || schema.starts_with("pg_temp")
        {
            continue;
        }
        if schema == "public" {
            schemas.push(schema);
        } else if load_namespace(db, &schema)?.is_some() {
            other = true;
            schemas.push(schema);
        }
    }
    Ok(other.then(|| schemas.into()))
}

fn relation_exists(db: &BicDb, name: &str) -> bool {
    resolve_relation_name_memoized(db, name).is_some()
}

fn sequence_exists(db: &BicDb, name: &str) -> bool {
    load_sequence(db, name).ok().flatten().is_some()
}

fn ident_name(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn schema_ident(schema: &str) -> sqlparser::ast::ObjectNamePart {
    let plain = schema
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    sqlparser::ast::ObjectNamePart::Identifier(if plain {
        Ident::new(schema)
    } else {
        Ident::with_quote('"', schema)
    })
}

struct Binder<'a> {
    db: &'a BicDb,
    path: &'a [String],
    ctes: Vec<BTreeSet<String>>,
    changed: bool,
}

impl Binder<'_> {
    fn single_part(name: &ObjectName) -> Option<String> {
        match name.0.as_slice() {
            [sqlparser::ast::ObjectNamePart::Identifier(ident)] => Some(ident_name(ident)),
            _ => None,
        }
    }

    fn is_cte(&self, name: &str) -> bool {
        self.ctes.iter().rev().any(|ctes| ctes.contains(name))
    }

    fn qualify(&mut self, name: &mut ObjectName, schema: &str) {
        name.0.insert(0, schema_ident(schema));
        self.changed = true;
    }

    /// An existing relation: the first schema on the path that holds it.
    fn bind_reference(&mut self, name: &mut ObjectName, exists: fn(&BicDb, &str) -> bool) {
        let Some(table) = Self::single_part(name) else {
            return;
        };
        if self.is_cte(&table) {
            return;
        }
        for schema in self.path {
            if schema == "public" {
                if exists(self.db, &table) {
                    return;
                }
                continue;
            }
            let Ok(encoded) = relation_name_from_parts(&[schema.clone(), table.clone()]) else {
                return;
            };
            if exists(self.db, &encoded) {
                let schema = schema.clone();
                self.qualify(name, &schema);
                return;
            }
        }
    }

    /// A new relation: the first existing schema on the path.
    fn bind_creation(&mut self, name: &mut ObjectName) {
        if Self::single_part(name).is_none() {
            return;
        }
        if let Some(schema) = self.path.first().filter(|schema| *schema != "public") {
            let schema = schema.clone();
            self.qualify(name, &schema);
        }
    }

    fn bind_foreign_key(&mut self, constraint: &mut sqlparser::ast::TableConstraint) {
        if let sqlparser::ast::TableConstraint::ForeignKey(foreign_key) = constraint {
            self.bind_reference(&mut foreign_key.foreign_table, relation_exists);
        }
    }

    fn bind_column_foreign_key(&mut self, column: &mut sqlparser::ast::ColumnDef) {
        for option in &mut column.options {
            if let sqlparser::ast::ColumnOption::ForeignKey(foreign_key) = &mut option.option {
                self.bind_reference(&mut foreign_key.foreign_table, relation_exists);
            }
        }
    }
}

impl sqlparser::ast::VisitorMut for Binder<'_> {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<()> {
        match statement {
            Statement::CreateTable(table) => {
                // A self-reference names the table being created.
                let own = Self::single_part(&table.name);
                self.bind_creation(&mut table.name);
                let created = table.name.clone();
                for constraint in &mut table.constraints {
                    if let sqlparser::ast::TableConstraint::ForeignKey(foreign_key) = constraint {
                        if own.is_some() && Self::single_part(&foreign_key.foreign_table) == own {
                            foreign_key.foreign_table = created.clone();
                            continue;
                        }
                    }
                    self.bind_foreign_key(constraint);
                }
                for column in &mut table.columns {
                    self.bind_column_foreign_key(column);
                }
            }
            Statement::CreateView(view) => self.bind_creation(&mut view.name),
            Statement::CreateSequence { name, .. } => self.bind_creation(name),
            Statement::AlterTable(table) => {
                for operation in &mut table.operations {
                    match operation {
                        sqlparser::ast::AlterTableOperation::AddConstraint {
                            constraint, ..
                        } => self.bind_foreign_key(constraint),
                        sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
                            self.bind_column_foreign_key(column_def)
                        }
                        _ => {}
                    }
                }
            }
            Statement::Drop {
                object_type, names, ..
            } => {
                let exists: Option<fn(&BicDb, &str) -> bool> = match object_type {
                    sqlparser::ast::ObjectType::Table
                    | sqlparser::ast::ObjectType::View
                    | sqlparser::ast::ObjectType::MaterializedView => Some(relation_exists),
                    sqlparser::ast::ObjectType::Sequence => Some(sequence_exists),
                    _ => None,
                };
                if let Some(exists) = exists {
                    for name in names {
                        self.bind_reference(name, exists);
                    }
                }
            }
            Statement::Copy {
                source: sqlparser::ast::CopySource::Table { table_name, .. },
                ..
            } => self.bind_reference(table_name, relation_exists),
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        self.ctes.push(
            query
                .with
                .as_ref()
                .map(|with| {
                    with.cte_tables
                        .iter()
                        .map(|cte| ident_name(&cte.alias.name))
                        .collect()
                })
                .unwrap_or_default(),
        );
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
        self.ctes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, name: &mut ObjectName) -> ControlFlow<()> {
        self.bind_reference(name, relation_exists);
        ControlFlow::Continue(())
    }
}

/// Binds unqualified relation references in any AST node; reports a change.
pub(crate) fn bind_node<T: sqlparser::ast::VisitMut>(
    db: &BicDb,
    node: &mut T,
    path: &[String],
) -> bool {
    let mut binder = Binder {
        db,
        path,
        ctes: Vec::new(),
        changed: false,
    };
    let _ = sqlparser::ast::VisitMut::visit(node, &mut binder);
    binder.changed
}

/// Statement text with unqualified relations bound through `path`, or `None`
/// when nothing changed (or the text is not ordinary SQL, which then runs as
/// before).
pub(crate) fn bind_sql_text(db: &BicDb, sql: &str, path: &[String]) -> Option<String> {
    // Lock strengths sqlparser lacks become the stronger ones it parses, as
    // statement execution does.
    let weak_locks = rewrite_weak_lock_strengths(sql);
    let sql = weak_locks.as_deref().unwrap_or(sql);
    let lock_order = rewrite_locking_clause_before_limit(sql);
    let sql = lock_order.as_deref().unwrap_or(sql);
    let mut statements = Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, sql).ok()?;
    // Routine bodies bind when they execute, with the caller's path; DDL
    // that defines them is left verbatim.
    if statements.iter().any(|statement| {
        matches!(
            statement,
            Statement::CreateFunction(_)
                | Statement::CreateProcedure { .. }
                | Statement::CreateTrigger(_)
        )
    }) {
        return None;
    }
    if !bind_node(db, &mut statements, path) {
        return None;
    }
    Some(
        statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(";\n"),
    )
}

/// Binds every SQL statement and expression of a routine body, before its
/// expressions are compiled.
pub(crate) fn bind_routine_parts(
    db: &BicDb,
    params: &mut [RoutineParam],
    declarations: &mut [RoutineDecl],
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
    path: &[String],
) {
    let mut visit = |node: RoutineNode<'_>| match node {
        RoutineNode::Expr(expr) => {
            bind_node(db, expr, path);
        }
        RoutineNode::Query(query) => {
            bind_node(db, query, path);
        }
        RoutineNode::Statement(statement) => {
            bind_node(db, statement, path);
        }
    };
    walk_routine_parts(params, declarations, statements, handlers, &mut visit);
}

/// One SQL node of a routine body.
pub(crate) enum RoutineNode<'a> {
    Expr(&'a mut Expr),
    Query(&'a mut Query),
    Statement(&'a mut Statement),
}

/// Visits every SQL statement, query and expression of a routine body.
pub(crate) fn walk_routine_parts(
    params: &mut [RoutineParam],
    declarations: &mut [RoutineDecl],
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
    visit: &mut dyn FnMut(RoutineNode<'_>),
) {
    for param in params {
        if let Some(expr) = &mut param.default_expr {
            visit(RoutineNode::Expr(expr));
        }
    }
    bind_declarations(declarations, visit);
    bind_statements(statements, visit);
    for handler in handlers {
        bind_statements(&mut handler.statements, visit);
    }
}

fn bind_declarations(declarations: &mut [RoutineDecl], visit: &mut dyn FnMut(RoutineNode<'_>)) {
    for declaration in declarations {
        match declaration {
            RoutineDecl::Variable {
                default_expr: Some(expr),
                ..
            } => {
                visit(RoutineNode::Expr(&mut expr.expr));
            }
            RoutineDecl::Cursor { query, params, .. } => {
                for param in params {
                    if let Some(expr) = &mut param.default_expr {
                        visit(RoutineNode::Expr(expr));
                    }
                }
                visit(RoutineNode::Query(query));
            }
            _ => {}
        }
    }
}

pub(crate) fn bind_statements(
    statements: &mut [RoutineStmt],
    visit: &mut dyn FnMut(RoutineNode<'_>),
) {
    for statement in statements {
        match statement {
            RoutineStmt::Block {
                declarations,
                body,
                handlers,
            } => {
                bind_declarations(declarations, visit);
                bind_statements(body, visit);
                for handler in handlers {
                    bind_statements(&mut handler.statements, visit);
                }
            }
            RoutineStmt::Assignment { target, expr } => {
                if let RoutineAssignmentTarget::ArrayElement { index, .. } = target {
                    visit(RoutineNode::Expr(&mut index.expr));
                }
                visit(RoutineNode::Expr(&mut expr.expr));
            }
            RoutineStmt::If {
                condition,
                then_body,
                else_body,
            } => {
                visit(RoutineNode::Expr(&mut condition.expr));
                bind_statements(then_body, visit);
                bind_statements(else_body, visit);
            }
            RoutineStmt::Case {
                expression,
                arms,
                else_body,
            } => {
                visit(RoutineNode::Expr(&mut expression.expr));
                for arm in arms {
                    bind_statements(arm, visit);
                }
                if let Some(body) = else_body {
                    bind_statements(body, visit);
                }
            }
            RoutineStmt::ForLoop {
                lower, upper, body, ..
            } => {
                visit(RoutineNode::Expr(&mut lower.expr));
                visit(RoutineNode::Expr(&mut upper.expr));
                bind_statements(body, visit);
            }
            RoutineStmt::ForeachLoop { array, body, .. } => {
                visit(RoutineNode::Expr(&mut array.expr));
                bind_statements(body, visit);
            }
            RoutineStmt::WhileLoop { condition, body } => {
                visit(RoutineNode::Expr(&mut condition.expr));
                bind_statements(body, visit);
            }
            RoutineStmt::QueryForLoop { query, body, .. } => {
                visit(RoutineNode::Query(query));
                bind_statements(body, visit);
            }
            RoutineStmt::CursorForLoop {
                arguments, body, ..
            } => {
                for arg in arguments {
                    visit(RoutineNode::Expr(&mut arg.expr.expr));
                }
                bind_statements(body, visit);
            }
            RoutineStmt::OpenCursor { arguments, .. } => {
                for arg in arguments {
                    visit(RoutineNode::Expr(&mut arg.expr.expr));
                }
            }
            RoutineStmt::Loop { body } => bind_statements(body, visit),
            RoutineStmt::SelectInto { query, .. }
            | RoutineStmt::Perform { query }
            | RoutineStmt::ReturnQuery(query) => {
                visit(RoutineNode::Query(query));
            }
            RoutineStmt::Sql(statement) | RoutineStmt::SqlInto { statement, .. } => {
                visit(RoutineNode::Statement(statement));
            }
            RoutineStmt::Return(Some(expr)) => {
                visit(RoutineNode::Expr(&mut expr.expr));
            }
            RoutineStmt::OracleBuiltin { arguments, .. } => {
                for arg in arguments {
                    visit(RoutineNode::Expr(&mut arg.expr));
                }
            }
            RoutineStmt::RaiseException {
                arguments, detail, ..
            } => {
                for arg in arguments {
                    visit(RoutineNode::Expr(&mut arg.expr));
                }
                if let Some(detail) = detail {
                    visit(RoutineNode::Expr(&mut detail.expr));
                }
            }
            // Dynamic SQL text binds when it executes, through the session.
            RoutineStmt::OracleDynamicExecute(_)
            | RoutineStmt::DynamicExecute(_)
            | RoutineStmt::PostgresControl(_)
            | RoutineStmt::ContinueLoop
            | RoutineStmt::Rethrow
            | RoutineStmt::ExitLoop
            | RoutineStmt::Null
            | RoutineStmt::FetchCursor { .. }
            | RoutineStmt::CloseCursor { .. }
            | RoutineStmt::ReturnNext
            | RoutineStmt::Return(None) => {}
        }
    }
}

/// Statement text with unqualified relations bound through the session's
/// `search_path` (PostgreSQL profile), for front ends that describe a
/// statement before it executes; `None` when nothing changes.
/// The statements of a multi-statement text when they must bind one at a
/// time: unqualified names already bind through a non-default path, or a
/// statement before the last changes the path or the schemas on it (SET or
/// RESET search_path, set_config, DISCARD ALL, CREATE/ALTER/DROP SCHEMA).
/// Binding the whole text up front used the path in force before the batch.
pub(crate) fn statements_bound_separately(sql: &str, path_binds: bool) -> Option<Vec<String>> {
    if !sql.contains(';') {
        return None;
    }
    if !path_binds && !contains_path_change_word(sql) {
        return None;
    }
    let statements = split_sql_statements(sql);
    if statements.len() < 2 {
        return None;
    }
    let changes_path = statements[..statements.len() - 1]
        .iter()
        .any(|statement| statement_changes_binding_path(statement));
    (path_binds || changes_path).then_some(statements)
}

fn contains_path_change_word(sql: &str) -> bool {
    ["search_path", "discard", "schema", "reset"]
        .iter()
        .any(|word| {
            sql.as_bytes()
                .windows(word.len())
                .any(|window| window.eq_ignore_ascii_case(word.as_bytes()))
        })
}

fn statement_changes_binding_path(statement: &str) -> bool {
    let normalized = statement
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    let set_target = normalized
        .strip_prefix("set ")
        .map(|rest| {
            rest.strip_prefix("session ")
                .or_else(|| rest.strip_prefix("local "))
                .unwrap_or(rest)
        })
        .map(|rest| rest.trim_start_matches('"'));
    set_target.is_some_and(|rest| rest.starts_with("search_path"))
        || normalized.starts_with("reset search_path")
        || normalized.starts_with("reset all")
        || normalized.starts_with("discard all")
        || normalized.starts_with("create schema")
        || normalized.starts_with("alter schema")
        || normalized.starts_with("drop schema")
        || (normalized.contains("set_config") && normalized.contains("search_path"))
}

pub fn bind_session_sql(
    db: &BicDb,
    sql: &str,
    gucs: &HashMap<String, String>,
) -> Result<Option<String>> {
    Ok(binding_path(db, gucs)?
        .as_deref()
        .and_then(|path| bind_sql_text(db, sql, path)))
}
