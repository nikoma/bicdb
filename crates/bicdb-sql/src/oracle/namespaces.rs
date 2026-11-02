//! Shared relation qualification for preview sessions and stored function SQL.
use super::*;

pub(super) fn qualify<T: sqlparser::ast::VisitMut>(node: &mut T, schema: &str) {
    struct Qualify {
        schema: String,
        ctes: Vec<BTreeSet<String>>,
    }
    impl sqlparser::ast::VisitorMut for Qualify {
        type Break = ();
        fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<()> {
            match statement {
                Statement::CreateIndex(index) => {
                    if let Some(name) = index.name.as_mut() {
                        self.pre_visit_relation(name)?;
                    }
                }
                Statement::CreateSequence { name, .. } => self.pre_visit_relation(name)?,
                Statement::CreateView(view) => self.pre_visit_relation(&mut view.name)?,
                Statement::CreateFunction(function)
                    if function.language.as_ref().is_some_and(|language| {
                        matches!(
                            preview_routine_language_version(&ident_value(language)),
                            Some(2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10)
                        )
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
            ctes: Vec::new(),
        },
    );
}
