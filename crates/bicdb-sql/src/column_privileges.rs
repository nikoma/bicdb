//! Column-level privileges (`GRANT SELECT (a), INSERT (b), UPDATE (c),
//! REFERENCES (d) ON t TO r`).
//!
//! A role without the table-level privilege may still run a statement when it
//! holds the privilege on every column of the relation the statement
//! references, as in PostgreSQL. The referenced columns are computed as an
//! over-approximation from the statement's syntax: every identifier that
//! names a column of the relation counts, and any wildcard, NATURAL join or
//! whole-row reference counts as every column. A statement that names a
//! column the role may not use is therefore refused even when PostgreSQL
//! would bind the name to another relation; nothing is ever read through a
//! column grant that PostgreSQL would refuse.
use crate::*;
use sqlparser::ast::{Visit, Visitor};

/// The names a statement uses, gathered from its syntax.
#[derive(Debug, Default)]
pub(crate) struct ReferencedNames {
    /// Every identifier part (qualifiers included: `c.f` may be a composite
    /// column `c` read without parentheses).
    names: BTreeSet<String>,
    /// Bare identifiers, which may be whole-row references to an alias.
    bare: BTreeSet<String>,
    /// A wildcard, NATURAL join or `alias.*` argument appears.
    all: bool,
}

impl ReferencedNames {
    pub(crate) fn of_query(query: &Query) -> Self {
        let mut names = Self::default();
        let _ = query.visit(&mut names);
        names
    }

    pub(crate) fn add_expr(&mut self, expr: &Expr) {
        let _ = expr.visit(self);
    }

    pub(crate) fn add_items(&mut self, items: &[SelectItem]) {
        for item in items {
            match item {
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => self.all = true,
                SelectItem::UnnamedExpr(expr)
                | SelectItem::ExprWithAlias { expr, .. }
                | SelectItem::ExprWithAliases { expr, .. } => self.add_expr(expr),
            }
        }
    }

    pub(crate) fn add_name(&mut self, name: String) {
        self.names.insert(name);
    }

    /// The relation's columns a statement may read or write. `aliases` are
    /// the names the statement gives the relation (a bare alias is a
    /// whole-row reference).
    pub(crate) fn required_columns(&self, columns: &[String], aliases: &[String]) -> Vec<String> {
        if self.all || aliases.iter().any(|alias| self.bare.contains(alias)) {
            return columns.to_vec();
        }
        columns
            .iter()
            .filter(|column| self.names.contains(*column))
            .cloned()
            .collect()
    }
}

impl Visitor for ReferencedNames {
    type Break = ();

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
        self.add_items(&select.projection);
        for from in &select.from {
            for join in &from.joins {
                let constraint = match &join.join_operator {
                    JoinOperator::Join(constraint)
                    | JoinOperator::Inner(constraint)
                    | JoinOperator::Left(constraint)
                    | JoinOperator::LeftOuter(constraint)
                    | JoinOperator::Right(constraint)
                    | JoinOperator::RightOuter(constraint)
                    | JoinOperator::FullOuter(constraint)
                    | JoinOperator::CrossJoin(constraint)
                    | JoinOperator::Semi(constraint)
                    | JoinOperator::LeftSemi(constraint)
                    | JoinOperator::RightSemi(constraint)
                    | JoinOperator::Anti(constraint)
                    | JoinOperator::LeftAnti(constraint)
                    | JoinOperator::RightAnti(constraint)
                    | JoinOperator::StraightJoin(constraint) => Some(constraint),
                    _ => {
                        // Join forms without a plain constraint: be total.
                        self.all = true;
                        None
                    }
                };
                match constraint {
                    Some(JoinConstraint::Natural) => self.all = true,
                    Some(JoinConstraint::Using(names)) => {
                        for name in names {
                            if let Some(last) = object_name_parts(name).last() {
                                self.names.insert(last.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(ident) => {
                self.names.insert(ident_value(ident));
                self.bare.insert(ident_value(ident));
            }
            Expr::CompoundIdentifier(parts) => {
                for part in parts {
                    self.names.insert(ident_value(part));
                }
            }
            Expr::QualifiedWildcard(..) | Expr::Wildcard(_) => self.all = true,
            Expr::Function(function) => {
                if let FunctionArguments::List(list) = &function.args {
                    for arg in &list.args {
                        let arg = match arg {
                            FunctionArg::Unnamed(arg)
                            | FunctionArg::Named { arg, .. }
                            | FunctionArg::ExprNamed { arg, .. } => arg,
                        };
                        if matches!(arg, FunctionArgExpr::QualifiedWildcard(_)) {
                            self.all = true;
                        }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// The columns of `table` the role holds `privilege` on through column grants
/// (its own, its roles' and PUBLIC's).
pub(crate) fn role_column_grants(
    db: &BicDb,
    role: &str,
    table: &str,
    privilege: &str,
) -> Result<BTreeSet<String>> {
    let grants = list_privileges(db)?;
    if !grants.iter().any(|grant| {
        grant.column.is_some()
            && grant.object_type == PrivilegeObjectType::Table
            && grant.object_name == table
            && grant.privilege == privilege
    }) {
        return Ok(BTreeSet::new());
    }
    let roles = role_privilege_closure(db, &normalize_role_name(role))?;
    Ok(grants
        .into_iter()
        .filter(|grant| {
            grant.object_type == PrivilegeObjectType::Table
                && grant.object_name == table
                && grant.privilege == privilege
                && (grant.grantee == "public"
                    || roles.contains(&normalize_role_name(&grant.grantee)))
        })
        .filter_map(|grant| grant.column)
        .collect())
}

/// The visible columns of a table or view.
pub(crate) fn relation_columns(db: &BicDb, table: &str) -> Result<Vec<String>> {
    if let Some(schema) = load_schema_shared(db, table)? {
        return Ok(schema
            .columns
            .iter()
            .filter(|column| !column.hidden)
            .map(|column| column.name.clone())
            .collect());
    }
    if let Some(view) = load_view(db, table)? {
        return Ok(view
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect());
    }
    Ok(Vec::new())
}

/// The names a query gives `table` in its FROM lists (aliases, or the bare
/// table name).
fn relation_aliases(db: &BicDb, query: &Query, table: &str) -> Vec<String> {
    struct Aliases<'a> {
        db: &'a BicDb,
        table: &'a str,
        found: Vec<String>,
    }
    impl Visitor for Aliases<'_> {
        type Break = ();
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table { name, alias, .. } = factor {
                let Ok(relation) = relation_name(name) else {
                    return ControlFlow::Continue(());
                };
                if resolve_session_relation_name_if_exists(self.db, &relation) == self.table {
                    match alias {
                        Some(alias) => self.found.push(ident_value(&alias.name)),
                        None => {
                            if let Some(last) = object_name_parts(name).last() {
                                self.found.push(last.clone());
                            }
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut aliases = Aliases {
        db,
        table,
        found: vec![table.rsplit('.').next().unwrap_or(table).to_string()],
    };
    let _ = query.visit(&mut aliases);
    aliases.found
}

/// Whether column SELECT grants let `role` run `query`, which reads `table`.
pub(crate) fn select_allowed_by_columns(
    db: &BicDb,
    role: &str,
    table: &str,
    query: &Query,
) -> Result<bool> {
    let grants = role_column_grants(db, role, table, "SELECT")?;
    if grants.is_empty() {
        return Ok(false);
    }
    let columns = relation_columns(db, table)?;
    let required = ReferencedNames::of_query(query)
        .required_columns(&columns, &relation_aliases(db, query, table));
    Ok(required.iter().all(|column| grants.contains(column)))
}

/// Whether `role` may read `columns` of `table`: the table-level privilege
/// or a column grant on each of them. No column needs no privilege.
pub(crate) fn role_may_use_columns(
    db: &BicDb,
    role: &str,
    table: &str,
    privilege: &str,
    columns: &[String],
) -> Result<bool> {
    if columns.is_empty() || role_has_table_privilege(db, role, table, privilege)? {
        return Ok(true);
    }
    let grants = role_column_grants(db, role, table, privilege)?;
    Ok(columns.iter().all(|column| grants.contains(column)))
}

thread_local! {
    /// (role, relation) pairs a running query was admitted to through column
    /// grants; scans of that query consult them. Scoped by `AllowanceScope`.
    static ALLOWANCES: RefCell<Vec<(String, String)>> = const { RefCell::new(Vec::new()) };
    static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Holds the allowances recorded while one query is checked and executed;
/// dropping it forgets them.
pub(crate) struct AllowanceScope {
    len: usize,
}

impl AllowanceScope {
    pub(crate) fn enter() -> Self {
        DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self {
            len: ALLOWANCES.with(|allowances| allowances.borrow().len()),
        }
    }
}

impl Drop for AllowanceScope {
    fn drop(&mut self) {
        ALLOWANCES.with(|allowances| allowances.borrow_mut().truncate(self.len));
        DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Records an admission for the current scope; without a scope nothing is
/// recorded and later scans require the table privilege.
pub(crate) fn allow(role: &str, table: &str) {
    if DEPTH.with(|depth| depth.get()) == 0 {
        return;
    }
    ALLOWANCES.with(|allowances| {
        allowances
            .borrow_mut()
            .push((normalize_role_name(role), table.to_string()))
    });
}

pub(crate) fn allowed(role: &str, table: &str) -> bool {
    let role = normalize_role_name(role);
    ALLOWANCES.with(|allowances| {
        allowances
            .borrow()
            .iter()
            .any(|(allowed_role, allowed_table)| *allowed_role == role && allowed_table == table)
    })
}

/// `has_column_privilege([role,] table, column, privilege)` and
/// `has_any_column_privilege([role,] table, privilege)`.
pub(crate) fn has_column_privilege_value(
    db: &BicDb,
    name: &str,
    args: &[SqlValue],
    current_user: &str,
) -> Result<Option<SqlValue>> {
    let any = match name.rsplit('.').next().unwrap_or(name) {
        "has_column_privilege" => false,
        "has_any_column_privilege" => true,
        _ => return Ok(None),
    };
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(Some(SqlValue::Null));
    }
    let width = if any { 2 } else { 3 };
    let (role, rest) = match args.len() {
        len if len == width => (normalize_role_name(current_user), args),
        len if len == width + 1 => (
            normalize_role_name(&sql_value_text(&args[0]).unwrap_or_default()),
            &args[1..],
        ),
        _ => {
            return Err(SqlError::InvalidSql(format!(
                "{name} expects {width} or {} arguments",
                width + 1
            )))
        }
    };
    let table_text = sql_value_text(&rest[0]).unwrap_or_default();
    let table = match &rest[0] {
        SqlValue::Int(oid) => table_oids(db)
            .iter()
            .find(|(_, table_oid)| **table_oid == *oid)
            .map(|(name, _)| name.clone())
            .unwrap_or_default(),
        _ => resolve_session_relation_name_if_exists(db, &table_text.to_ascii_lowercase()),
    };
    if !relation_exists(db, &table) && load_view(db, &table)?.is_none() {
        return Err(SqlError::InvalidCollection(table_text));
    }
    let privileges_text = sql_value_text(&rest[rest.len() - 1]).unwrap_or_default();
    let privileges = privileges_text
        .split(',')
        .map(|privilege| privilege.trim().to_ascii_uppercase())
        .map(|privilege| {
            privilege
                .strip_suffix(" WITH GRANT OPTION")
                .unwrap_or(&privilege)
                .trim()
                .to_string()
        })
        .collect::<Vec<_>>();
    for privilege in &privileges {
        if !matches!(
            privilege.as_str(),
            "SELECT" | "INSERT" | "UPDATE" | "REFERENCES"
        ) {
            return Err(SqlError::invalid_parameter_value(format!(
                "unrecognized privilege type: \"{}\"",
                privilege.to_ascii_lowercase()
            )));
        }
    }
    let columns = relation_columns(db, &table)?;
    let column = if any {
        None
    } else {
        let column = match &rest[1] {
            SqlValue::Int(number) => usize::try_from(*number)
                .ok()
                .and_then(|number| number.checked_sub(1))
                .and_then(|index| columns.get(index).cloned()),
            other => sql_value_text(other).filter(|name| columns.contains(name)),
        };
        let Some(column) = column else {
            return Err(SqlError::UndefinedColumn {
                table: relation_name_for_message(&table),
                column: sql_value_text(&rest[1]).unwrap_or_default(),
            });
        };
        Some(column)
    };
    for privilege in &privileges {
        if role_has_table_privilege(db, &role, &table, privilege)? {
            return Ok(Some(SqlValue::Bool(true)));
        }
        let grants = role_column_grants(db, &role, &table, privilege)?;
        let held = match &column {
            Some(column) => grants.contains(column),
            None => !grants.is_empty(),
        };
        if held {
            return Ok(Some(SqlValue::Bool(true)));
        }
    }
    Ok(Some(SqlValue::Bool(false)))
}

/// `information_schema.column_privileges`: table-level grants of SELECT,
/// INSERT, UPDATE and REFERENCES apply to every column, plus column grants.
pub(crate) fn information_schema_column_privileges(
    db: &BicDb,
) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = BTreeMap::new();
    for row in information_schema_role_table_grants(db)? {
        let privilege = sql_value_text(&virtual_cell(&row, "privilege_type")).unwrap_or_default();
        if !matches!(
            privilege.as_str(),
            "SELECT" | "INSERT" | "UPDATE" | "REFERENCES"
        ) {
            continue;
        }
        let schema = sql_value_text(&virtual_cell(&row, "table_schema")).unwrap_or_default();
        let table = sql_value_text(&virtual_cell(&row, "table_name")).unwrap_or_default();
        let relation = if schema == "public" {
            table.clone()
        } else {
            format!("{schema}.{table}")
        };
        let relation = resolve_session_relation_name_if_exists(db, &relation);
        for column in relation_columns(db, &relation)? {
            let grantor = sql_value_text(&virtual_cell(&row, "grantor")).unwrap_or_default();
            let grantee = sql_value_text(&virtual_cell(&row, "grantee")).unwrap_or_default();
            let grantable = sql_value_text(&virtual_cell(&row, "is_grantable")).unwrap_or_default();
            rows.insert(
                (
                    schema.clone(),
                    table.clone(),
                    column,
                    grantee,
                    privilege.clone(),
                ),
                (grantor, grantable),
            );
        }
    }
    for grant in list_privileges(db)? {
        let Some(column) = grant.column.clone() else {
            continue;
        };
        if grant.object_type != PrivilegeObjectType::Table {
            continue;
        }
        let owner = load_schema_shared(db, &grant.object_name)?
            .and_then(|schema| schema.owner.clone())
            .or_else(|| {
                load_view(db, &grant.object_name)
                    .ok()
                    .flatten()
                    .and_then(|view| view.owner)
            })
            .unwrap_or_else(|| BOOTSTRAP_ROLE_NAME.to_string());
        let grantee = if grant.grantee.eq_ignore_ascii_case("public") {
            "PUBLIC".to_string()
        } else {
            grant.grantee.clone()
        };
        rows.entry((
            relation_catalog_namespace(db, &grant.object_name)?,
            catalog_display_name(&grant.object_name),
            column,
            grantee,
            grant.privilege.clone(),
        ))
        .or_insert((owner, "NO".to_string()));
    }
    Ok(rows
        .into_iter()
        .map(
            |((schema, table, column, grantee, privilege), (grantor, grantable))| {
                virtual_row([
                    ("grantor", SqlValue::String(grantor)),
                    ("grantee", SqlValue::String(grantee)),
                    ("table_catalog", SqlValue::String("bicdb".to_string())),
                    ("table_schema", SqlValue::String(schema)),
                    ("table_name", SqlValue::String(table)),
                    ("column_name", SqlValue::String(column)),
                    ("privilege_type", SqlValue::String(privilege)),
                    ("is_grantable", SqlValue::String(grantable)),
                ])
            },
        )
        .collect())
}
