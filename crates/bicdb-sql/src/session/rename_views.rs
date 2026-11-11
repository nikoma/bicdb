//! Bind renamed sources through query scopes, preserving explicit aliases and
//! distinguishing input columns from whole-row aliases. Metadata discovery
//! uses the same nonexecuting Describe path as protocol preparation.
use super::*;
use sqlparser::ast::{SelectItemQualifiedWildcardKind, TableAlias, VisitMut, VisitorMut, With};

#[derive(Clone)]
struct Binding {
    alias: String,
    qualified: Option<String>,
    replacement: Option<String>,
}
#[derive(Clone, Default)]
struct Scope {
    columns: BTreeSet<String>,
    bindings: Vec<Binding>,
}
struct QueryFrame {
    with: Option<With>,
    scope: usize,
    single_select: bool,
}

pub(super) fn query(db: &BicDb, sql: &str, old: &str, new: &str) -> Result<String> {
    let mut query = parse_query(sql)?;
    let mut visitor = Rebinder {
        db,
        old,
        new,
        source: sql.to_ascii_lowercase(),
        counter: 0,
        scopes: Vec::new(),
        ctes: vec![BTreeMap::new()],
        frames: Vec::new(),
        projections: Vec::new(),
        nested: HashMap::new(),
    };
    if let ControlFlow::Break(error) = VisitMut::visit(&mut query, &mut visitor) {
        return Err(error);
    }
    Ok(crate::sql_text::sql_text(&query))
}

struct Rebinder<'a> {
    db: &'a BicDb,
    old: &'a str,
    new: &'a str,
    source: String,
    counter: usize,
    scopes: Vec<Scope>,
    ctes: Vec<BTreeMap<String, CteResult>>,
    frames: Vec<QueryFrame>,
    // Only AST nodes in this one walk are keyed here. No pointer is dereferenced.
    nested: HashMap<usize, Scope>,
    projections: Vec<Vec<Option<String>>>,
}
impl Rebinder<'_> {
    fn engine(&self) -> SqlEngine<'_> {
        SqlEngine::with_ctes_and_context(
            self.db,
            SqlSettings::default(),
            self.ctes.last().cloned().unwrap_or_default(),
            None,
            Arc::new(HashMap::new()),
        )
    }
    fn fresh(&mut self) -> String {
        loop {
            let alias = format!("__bicdb_view_source_{}", self.counter);
            self.counter += 1;
            if !self.source.contains(&alias) {
                return alias;
            }
        }
    }
    fn replacement(&self, parts: &[Ident], bare: bool) -> Option<String> {
        let names = parts.iter().map(ident_value).collect::<Vec<_>>();
        for scope in self.scopes.iter().rev() {
            if names.len() == 1 {
                if bare && scope.columns.contains(&names[0]) {
                    return None;
                }
                if let Some(binding) = scope.bindings.iter().find(|b| b.alias == names[0]) {
                    return binding.replacement.clone();
                }
            } else if let Ok(qualified) = relation_name_from_parts(&names) {
                if let Some(binding) = scope
                    .bindings
                    .iter()
                    .find(|b| b.qualified.as_ref() == Some(&qualified))
                {
                    return binding.replacement.clone();
                }
            }
        }
        None
    }
    fn wildcard(&self, name: &mut ObjectName) {
        let parts = name
            .0
            .iter()
            .filter_map(|part| part.as_ident().cloned())
            .collect::<Vec<_>>();
        if parts.len() != name.0.len() {
            return;
        }
        if let Some(alias) = self.replacement(&parts, false) {
            name.0 = vec![sqlparser::ast::ObjectNamePart::Identifier(
                Ident::with_quote('"', alias),
            )];
        }
    }
    fn scope(&self, from: &[TableWithJoins]) -> Result<Scope> {
        let labels = self.engine().describe_from_labels(from).ok_or_else(|| {
            SqlError::Unsupported(
                "cannot resolve view source columns without executing its query".into(),
            )
        })?;
        Ok(Scope {
            columns: labels.into_iter().flat_map(|(_, cols)| cols).collect(),
            bindings: Vec::new(),
        })
    }
    fn factor(&mut self, factor: &mut TableFactor, bindings: &mut Vec<Binding>) -> Result<()> {
        let address = factor as *const TableFactor as usize;
        if let TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } = factor
        {
            let mut inner = self.scope(std::slice::from_ref(table_with_joins.as_ref()))?;
            self.factor(&mut table_with_joins.relation, &mut inner.bindings)?;
            for join in &mut table_with_joins.joins {
                self.factor(&mut join.relation, &mut inner.bindings)?;
            }
            if let Some(alias) = alias {
                bindings.push(Binding {
                    alias: ident_value(&alias.name),
                    qualified: None,
                    replacement: None,
                });
                self.nested.insert(address, inner);
            } else {
                bindings.extend(inner.bindings);
            }
            return Ok(());
        }
        if let TableFactor::Table {
            name, alias, args, ..
        } = factor
        {
            let parts = object_name_parts(name);
            let original = parts.last().cloned().unwrap_or_default();
            let physical = relation_name(name)?;
            let cte = parts.len() == 1
                && self
                    .ctes
                    .last()
                    .is_some_and(|ctes| ctes.values().any(|cte| cte.name == original));
            let implicit = alias.is_none();
            let mut binding = Binding {
                alias: alias
                    .as_ref()
                    .map(|a| ident_value(&a.name))
                    .unwrap_or(original),
                qualified: if implicit && args.is_none() && !cte {
                    Some(physical.clone())
                } else {
                    None
                },
                replacement: None,
            };
            if args.is_none() && !cte && physical == self.old {
                let logical = logical_relation_name(self.new).unwrap_or_else(|| self.new.into());
                if let Some(last) = name.0.last_mut() {
                    *last =
                        sqlparser::ast::ObjectNamePart::Identifier(Ident::with_quote('"', logical));
                }
                if implicit {
                    let replacement = self.fresh();
                    *alias = Some(TableAlias {
                        explicit: true,
                        name: Ident::with_quote('"', &replacement),
                        columns: Vec::new(),
                        at: None,
                    });
                    binding.replacement = Some(replacement);
                }
            }
            bindings.push(binding);
            return Ok(());
        }
        let (alias, default) = match factor {
            TableFactor::Derived { alias, .. } => (alias, "subquery".to_string()),
            TableFactor::Function { name, alias, .. } => (
                alias,
                object_name_parts(name).last().cloned().unwrap_or_default(),
            ),
            TableFactor::TableFunction { alias, .. } => (alias, "tablefunction".to_string()),
            TableFactor::UNNEST { alias, .. } => (alias, "unnest".to_string()),
            TableFactor::JsonTable { alias, .. }
            | TableFactor::OpenJsonTable { alias, .. }
            | TableFactor::XmlTable { alias, .. } => (alias, "jsontable".to_string()),
            TableFactor::Pivot { .. }
            | TableFactor::Unpivot { .. }
            | TableFactor::MatchRecognize { .. } => {
                return Err(SqlError::Unsupported(
                    "view rename binding of transformed FROM sources is not implemented".into(),
                ))
            }
            _ => return Err(SqlError::Unsupported("unknown view FROM binding".into())),
        };
        bindings.push(Binding {
            alias: alias
                .as_ref()
                .map(|a| ident_value(&a.name))
                .unwrap_or(default),
            qualified: None,
            replacement: None,
        });
        Ok(())
    }
    fn cte_result(&self, cte: &sqlparser::ast::Cte) -> Option<CteResult> {
        let mut anchor = cte.query.as_ref().clone();
        if let SetExpr::SetOperation {
            left,
            op: SetOperator::Union,
            ..
        } = anchor.body.as_ref()
        {
            anchor.body = Box::new(left.as_ref().clone());
        }
        let columns = self
            .engine()
            .query_describe_output_columns(&anchor)
            .or_else(|| {
                (!cte.alias.columns.is_empty()).then(|| {
                    cte.alias
                        .columns
                        .iter()
                        .map(|c| (ident_value(&c.name), None))
                        .collect()
                })
            })?;
        let columns = rename_relation_columns(columns, &cte.alias.columns)?;
        let (names, types) = columns.into_iter().unzip();
        Some(
            CteResult::new(ident_value(&cte.alias.name), names, Vec::new())
                .with_column_types(types),
        )
    }
}
impl VisitorMut for Rebinder<'_> {
    type Break = SqlError;
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
        let mut with = query.with.take();
        let inherited = self.ctes.last().cloned().unwrap_or_default();
        self.ctes.push(inherited);
        if let Some(with) = with.as_mut() {
            if with.recursive {
                for cte in &with.cte_tables {
                    let Some(result) = self.cte_result(cte) else {
                        return ControlFlow::Break(SqlError::Unsupported(
                            "cannot describe recursive view CTE anchor".into(),
                        ));
                    };
                    self.ctes
                        .last_mut()
                        .unwrap()
                        .insert(cte_key(&result.name), result);
                }
            }
            for cte in &mut with.cte_tables {
                let result = self.cte_result(cte);
                if let ControlFlow::Break(error) = VisitMut::visit(cte.query.as_mut(), self) {
                    return ControlFlow::Break(error);
                }
                let Some(result) = result else {
                    return ControlFlow::Break(SqlError::Unsupported(
                        "cannot describe view CTE".into(),
                    ));
                };
                self.ctes
                    .last_mut()
                    .unwrap()
                    .insert(cte_key(&result.name), result);
            }
        }
        // TABLE query bodies do not contain a TableFactor for the visitor.
        // Resolve them with the same lexical CTE and schema rules as FROM.
        let mut bodies = vec![query.body.as_mut()];
        while let Some(body) = bodies.pop() {
            match body {
                SetExpr::SetOperation { left, right, .. } => {
                    bodies.push(left.as_mut());
                    bodies.push(right.as_mut());
                }
                SetExpr::Table(table) => {
                    if let Some(name) = &table.table_name {
                        let cte = table.schema_name.is_none()
                            && self
                                .ctes
                                .last()
                                .is_some_and(|ctes| ctes.values().any(|cte| cte.name == *name));
                        let mut parts = Vec::new();
                        if let Some(schema) = &table.schema_name {
                            parts.push(schema.clone());
                        }
                        parts.push(name.clone());
                        match relation_name_from_parts(&parts) {
                            Ok(physical) if !cte && physical == self.old => {
                                table.table_name = Some(
                                    logical_relation_name(self.new)
                                        .unwrap_or_else(|| self.new.into()),
                                );
                            }
                            Err(error) => return ControlFlow::Break(error),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        let scope = self.scopes.len();
        self.scopes.push(Scope::default());
        self.frames.push(QueryFrame {
            with,
            scope,
            single_select: matches!(query.body.as_ref(), SetExpr::Select(_)),
        });
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
        let frame = self.frames.pop().unwrap();
        query.with = frame.with;
        self.scopes.pop();
        self.ctes.pop();
        ControlFlow::Continue(())
    }
    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<SqlError> {
        self.projections.push(
            select
                .projection
                .iter()
                .map(|item| match item {
                    SelectItem::UnnamedExpr(expr) => Some(if select.from.is_empty() {
                        select_expr_column_name(expr)
                    } else {
                        row_expr_column_name(expr)
                    }),
                    _ => None,
                })
                .collect(),
        );
        let mut scope = match self.scope(&select.from) {
            Ok(scope) => scope,
            Err(error) => return ControlFlow::Break(error),
        };
        for from in &mut select.from {
            if let Err(error) = self.factor(&mut from.relation, &mut scope.bindings) {
                return ControlFlow::Break(error);
            }
            for join in &mut from.joins {
                if let Err(error) = self.factor(&mut join.relation, &mut scope.bindings) {
                    return ControlFlow::Break(error);
                }
            }
        }
        if let Some(frame) = self.frames.last().filter(|f| f.single_select) {
            self.scopes[frame.scope] = scope.clone();
        }
        self.scopes.push(scope);
        for item in &mut select.projection {
            if let SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(name),
                _,
            ) = item
            {
                self.wildcard(name);
            }
        }
        ControlFlow::Continue(())
    }
    fn post_visit_select(&mut self, select: &mut Select) -> ControlFlow<SqlError> {
        let labels = self.projections.pop().expect("select projection scope");
        let no_from = select.from.is_empty();
        for (item, label) in select.projection.iter_mut().zip(labels) {
            if let (SelectItem::UnnamedExpr(expr), Some(label)) = (&mut *item, label) {
                let now = if no_from {
                    select_expr_column_name(expr)
                } else {
                    row_expr_column_name(expr)
                };
                if now != label {
                    let expr = std::mem::replace(expr, Expr::Value(Value::Null.into()));
                    *item = SelectItem::ExprWithAlias {
                        expr,
                        alias: Ident::with_quote('"', label),
                    };
                }
            }
        }
        self.scopes.pop();
        if let Some(frame) = self.frames.last().filter(|f| f.single_select) {
            for item in &select.projection {
                if let SelectItem::ExprWithAlias { alias, .. } = item {
                    self.scopes[frame.scope].columns.insert(ident_value(alias));
                }
            }
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<SqlError> {
        if let Some(scope) = self.nested.get(&(factor as *const TableFactor as usize)) {
            self.scopes.push(scope.clone());
        }
        ControlFlow::Continue(())
    }
    fn post_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<SqlError> {
        if self
            .nested
            .remove(&(factor as *const TableFactor as usize))
            .is_some()
        {
            self.scopes.pop();
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<SqlError> {
        match expr {
            Expr::Identifier(ident) => {
                if let Some(alias) = self.replacement(std::slice::from_ref(ident), true) {
                    *ident = Ident::with_quote('"', alias);
                }
            }
            Expr::CompoundIdentifier(parts) if parts.len() > 1 => {
                if let Some(alias) = self.replacement(&parts[..parts.len() - 1], false) {
                    let column = parts.last().unwrap().clone();
                    *parts = vec![Ident::with_quote('"', alias), column];
                }
            }
            Expr::QualifiedWildcard(name, _) => self.wildcard(name),
            Expr::Function(function) => {
                if let FunctionArguments::List(list) = &mut function.args {
                    for arg in &mut list.args {
                        let value = match arg {
                            FunctionArg::Unnamed(arg)
                            | FunctionArg::Named { arg, .. }
                            | FunctionArg::ExprNamed { arg, .. } => arg,
                        };
                        if let FunctionArgExpr::QualifiedWildcard(name) = value {
                            self.wildcard(name);
                        }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Compute before moving the source collection: all metadata-only name binding
/// must still see the old source. Apply through session view undo afterward.
pub(super) fn plan_queries(db: &BicDb, old: &str, new: &str) -> Result<Vec<ViewSchema>> {
    let mut updates = Vec::new();
    for mut view in list_views(db)? {
        if view_dependencies::relations(&view.query_sql)?.contains(old) {
            view.query_sql = query(db, &view.query_sql, old, new)?;
            updates.push(view);
        }
    }
    Ok(updates)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn planning_never_executes_volatile_projections() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE source(id INT); INSERT INTO source VALUES(1); CREATE SEQUENCE view_counter").unwrap();
        let rewritten = query(
            &db,
            "SELECT id,nextval('view_counter') FROM source",
            "source",
            "target",
        )
        .unwrap();
        assert!(rewritten.contains("target"));
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT nextval('view_counter')")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]]
        );
        db.close().unwrap();
    }
}
