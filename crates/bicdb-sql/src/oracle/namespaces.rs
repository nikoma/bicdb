//! Shared relation qualification for preview sessions and stored function SQL.
use super::*;

/// Bind exact quoted local routine names before native scalar dispatch can
/// mistake them for builtins. Routine EXECUTE checks still run in the host.
pub(super) fn bind_source_quoted_functions(
    db: &BicDb,
    source: &str,
    schema: &str,
) -> Result<Option<String>> {
    // Bind the original AST, before canonical lowering quotes ordinary names.
    // Never infer a source quote merely from a canonicalized identifier.
    if !source.contains('"') {
        return Ok(None);
    }
    let Ok(mut original) = Parser::parse_sql(&OracleDialect {}, source) else {
        // Extended grammar and stored legacy SQL need retained source-token
        // provenance before this binding can be qualified for those contexts.
        return Ok(None);
    };
    struct Resolve<'a> {
        db: &'a BicDb,
        schema: &'a str,
        changed: bool,
    }
    impl sqlparser::ast::VisitorMut for Resolve<'_> {
        type Break = SqlError;
        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<SqlError> {
            let Expr::Function(function) = expr else {
                return ControlFlow::Continue(());
            };
            let [part] = function.name.0.as_slice() else {
                return ControlFlow::Continue(());
            };
            let Some(ident) = part.as_ident().filter(|ident| ident.quote_style.is_some()) else {
                return ControlFlow::Continue(());
            };
            let routine = match load_routine(self.db, RoutineKind::Function, &ident.value) {
                Ok(Some(routine)) => routine,
                Ok(None) => return ControlFlow::Continue(()),
                Err(error) => return ControlFlow::Break(error),
            };
            let declarations = match parse_statements(&routine.definition) {
                Ok(declarations) => declarations,
                Err(error) => return ControlFlow::Break(error),
            };
            let [Statement::CreateFunction(declaration)] = declarations.as_slice() else {
                return ControlFlow::Continue(());
            };
            if object_name_parts(&declaration.name).last() != Some(&ident.value) {
                return ControlFlow::Continue(());
            }
            if routine_schema_name(&routine) != self.schema {
                return ControlFlow::Break(SqlError::undefined_function(
                    "quoted routine is not defined in the current schema",
                ));
            }
            function.name.0.insert(
                0,
                sqlparser::ast::ObjectNamePart::Identifier(Ident::with_quote('"', self.schema)),
            );
            self.changed = true;
            ControlFlow::Continue(())
        }
    }
    let mut resolve = Resolve {
        db,
        schema,
        changed: false,
    };
    match sqlparser::ast::VisitMut::visit(&mut original, &mut resolve) {
        ControlFlow::Continue(()) => Ok(resolve.changed.then(|| {
            original
                .iter()
                .map(render_sql)
                .collect::<Vec<_>>()
                .join("; ")
        })),
        ControlFlow::Break(error) => Err(error),
    }
}

pub(super) fn qualify<T: sqlparser::ast::VisitMut>(node: &mut T, schema: &str) {
    qualify_impl(node, schema, false)
}

pub(super) fn qualify_with_calls<T: sqlparser::ast::VisitMut>(node: &mut T, schema: &str) {
    qualify_impl(node, schema, true)
}

fn qualify_impl<T: sqlparser::ast::VisitMut>(node: &mut T, schema: &str, calls: bool) {
    struct Qualify {
        schema: String,
        ctes: Vec<BTreeSet<String>>,
        calls: bool,
    }
    impl sqlparser::ast::VisitorMut for Qualify {
        type Break = ();
        fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<()> {
            match statement {
                Statement::Call(function) if self.calls => {
                    self.pre_visit_relation(&mut function.name)?
                }
                Statement::CreateIndex(index) => {
                    if let Some(name) = index.name.as_mut() {
                        self.pre_visit_relation(name)?;
                    }
                }
                Statement::CreateSequence { name, .. } => self.pre_visit_relation(name)?,
                Statement::CreateView(view) => self.pre_visit_relation(&mut view.name)?,
                // A foreign key's target is a relation reference too, but
                // sqlparser does not visit `REFERENCES <table>` as one. Left
                // unqualified it resolved outside the application schema, so
                // the REFERENCES gate saw a nonexistent relation and denied
                // even the owner of both tables.
                Statement::CreateTable(table) => {
                    for constraint in &mut table.constraints {
                        self.qualify_foreign_key_constraint(constraint)?;
                    }
                    for column in &mut table.columns {
                        self.qualify_foreign_key_column(column)?;
                    }
                }
                Statement::AlterTable(table) => {
                    for operation in &mut table.operations {
                        match operation {
                            sqlparser::ast::AlterTableOperation::AddConstraint {
                                constraint,
                                ..
                            } => self.qualify_foreign_key_constraint(constraint)?,
                            sqlparser::ast::AlterTableOperation::AddColumn {
                                column_def, ..
                            } => self.qualify_foreign_key_column(column_def)?,
                            _ => {}
                        }
                    }
                }
                Statement::CreateFunction(function)
                    if function.language.as_ref().is_some_and(|language| {
                        preview_routine_language_version(&ident_value(language))
                            .is_some_and(|version| version >= 2)
                    }) =>
                {
                    self.pre_visit_relation(&mut function.name)?
                }
                _ => {}
            }
            // sqlparser's relation visitor does not visit DROP's name list.
            // Bind it to the same application schema as CREATE and queries.
            if let Statement::Drop {
                object_type, names, ..
            } = statement
            {
                if matches!(
                    object_type,
                    sqlparser::ast::ObjectType::Table
                        | sqlparser::ast::ObjectType::View
                        | sqlparser::ast::ObjectType::Index
                        | sqlparser::ast::ObjectType::Sequence
                        | sqlparser::ast::ObjectType::Type
                ) {
                    for name in names {
                        self.pre_visit_relation(name)?;
                    }
                }
            }
            if let Statement::Copy {
                source: sqlparser::ast::CopySource::Table { table_name, .. },
                ..
            } = statement
            {
                return self.pre_visit_relation(table_name);
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
                            .map(|cte| ident_value(&cte.alias.name))
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
        fn pre_visit_table_factor(&mut self, table: &mut TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } = table
            {
                let parts = object_name_parts(name);
                let is_cte =
                    parts.len() == 1 && self.ctes.iter().rev().any(|ctes| ctes.contains(&parts[0]));
                if alias.is_none() && !is_cte {
                    if let Some(table) = parts.last() {
                        // The storage relation is encoded for schema isolation.
                        // Retain Oracle's visible table name for table.column
                        // references, including correlated outer references.
                        *alias = Some(TableAlias {
                            explicit: true,
                            name: Ident::with_quote('"', table),
                            columns: Vec::new(),
                            at: None,
                        });
                    }
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_relation(&mut self, name: &mut ObjectName) -> ControlFlow<()> {
            self.qualify_name(name)
        }
    }
    impl Qualify {
        fn qualify_foreign_key_constraint(
            &mut self,
            constraint: &mut sqlparser::ast::TableConstraint,
        ) -> ControlFlow<()> {
            if let sqlparser::ast::TableConstraint::ForeignKey(foreign_key) = constraint {
                self.qualify_name(&mut foreign_key.foreign_table)?;
            }
            ControlFlow::Continue(())
        }
        fn qualify_foreign_key_column(
            &mut self,
            column: &mut sqlparser::ast::ColumnDef,
        ) -> ControlFlow<()> {
            for option in &mut column.options {
                if let sqlparser::ast::ColumnOption::ForeignKey(foreign_key) = &mut option.option {
                    self.qualify_name(&mut foreign_key.foreign_table)?;
                }
            }
            ControlFlow::Continue(())
        }
        fn qualify_name(&mut self, name: &mut ObjectName) -> ControlFlow<()> {
            let parts = object_name_parts(name);
            if let [table] = parts.as_slice() {
                if !self.ctes.iter().rev().any(|ctes| ctes.contains(table)) {
                    name.0.insert(
                        0,
                        sqlparser::ast::ObjectNamePart::Identifier(Ident::with_quote(
                            '"',
                            &self.schema,
                        )),
                    );
                }
            }
            ControlFlow::Continue(())
        }
    }
    let _ = sqlparser::ast::VisitMut::visit(
        node,
        &mut Qualify {
            schema: schema.to_string(),
            calls,
            ctes: Vec::new(),
        },
    );
}
