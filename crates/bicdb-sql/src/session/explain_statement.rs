//! `EXPLAIN`: BicDB's native access-path report for a bare `EXPLAIN [ANALYZE]
//! SELECT ...`, and PostgreSQL's plan layout (`crate::engine::explain_pg`)
//! for an option list (`EXPLAIN (COSTS OFF) ...`, `(FORMAT JSON)`), a
//! `FORMAT` clause, and every statement kind the native report does not
//! cover (INSERT/UPDATE/DELETE, CREATE TABLE AS, set operations, ...).
use crate::explain_format::{ExplainOptions, Format, PlanNode};
use crate::*;
use sqlparser::ast::{FromTable, TableObject};

/// Rows of a command tag such as `INSERT 0 3` or `UPDATE 2`.
fn command_rows(result: &SqlResult) -> u64 {
    result
        .command_tag
        .as_deref()
        .and_then(|tag| tag.rsplit(' ').next())
        .and_then(|count| count.parse().ok())
        .unwrap_or(result.rows.len() as u64)
}

/// The only node of `plan` that is not a ModifyTable, when the plan is one
/// scan (or one scan under the ModifyTable).
fn single_leaf(plan: &PlanNode) -> bool {
    match plan.children.as_slice() {
        [] => true,
        [(_, None, child)] if plan.node_type == "ModifyTable" => child.children.is_empty(),
        _ => false,
    }
}

impl SqlSession<'_> {
    pub(crate) fn execute_explain_statement(&mut self, explain: &Statement) -> Result<SqlResult> {
        let Statement::Explain {
            analyze,
            verbose,
            statement,
            format,
            options,
            ..
        } = explain
        else {
            unreachable!("execute_explain_statement on a non-EXPLAIN statement");
        };
        let postgres_layout = options.is_some()
            || format.is_some()
            || !matches!(statement.as_ref(), Statement::Query(_));
        if !postgres_layout {
            match self.sql_engine().execute_explain(statement, *analyze) {
                // Shapes the native report does not cover use the PostgreSQL layout.
                Err(SqlError::Unsupported(message)) if message.starts_with("EXPLAIN supports") => {}
                other => return other,
            }
        }
        let mut parsed = match options {
            Some(options) => ExplainOptions::parse(options)?,
            None => ExplainOptions::legacy(*analyze, *verbose),
        };
        if let Some(format) = format {
            let text = format.to_string().to_ascii_uppercase();
            let text = text
                .trim_start_matches("FORMAT")
                .trim_start_matches(['=', ' '])
                .to_string();
            parsed.format = match text.as_str() {
                "TEXT" => Format::Text,
                "JSON" => Format::Json,
                other => {
                    return Err(SqlError::data_exception_public(
                        "22023",
                        format!(
                            "unrecognized value for EXPLAIN option \"format\": \"{}\"",
                            other.to_ascii_lowercase()
                        ),
                        None,
                    ))
                }
            };
        }
        self.authorize_explained_statement(statement)?;
        let mut plan = {
            let engine = self.sql_engine();
            let mut builder = crate::engine::ExplainBuilder::new(&engine, &parsed, statement);
            builder.statement(statement)?
        };
        if parsed.analyze {
            // Only plans whose every node's row count the execution itself
            // reports are shown with actual rows; deeper plans would need
            // per-node instrumentation BicDB's executor does not record.
            if !single_leaf(&plan) {
                return Err(SqlError::data_exception_public(
                    "0A000",
                    "EXPLAIN ANALYZE in this layout is supported only for single-scan plans",
                    None,
                ));
            }
            let result = self.execute_statement(statement)?;
            let rows = command_rows(&result);
            if plan.node_type == "ModifyTable" {
                let returned = result.rows.len() as u64;
                plan.actual = Some((returned as f64, 1.0));
                if let Some((_, _, child)) = plan.children.first_mut() {
                    child.actual = Some((rows as f64, 1.0));
                }
            } else {
                plan.actual = Some((result.rows.len() as f64, 1.0));
            }
        }
        let lines = crate::explain_format::render(&parsed, &plan, &[]);
        let mut result = SqlResult::new(
            vec!["QUERY PLAN".to_string()],
            lines
                .into_iter()
                .map(|line| vec![SqlValue::String(line)])
                .collect(),
        );
        result.column_types = vec![Some(
            if parsed.format == Format::Json {
                "json"
            } else {
                "text"
            }
            .to_string(),
        )];
        Ok(result)
    }

    /// EXPLAIN needs the privileges the statement needs: SELECT on every
    /// relation it reads and the write privilege on a DML target, so a plan
    /// never describes a relation the role cannot use.
    fn authorize_explained_statement(&self, statement: &Statement) -> Result<()> {
        use std::ops::ControlFlow;
        let target = match statement {
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(name) => Some((name.clone(), "INSERT")),
                _ => None,
            },
            Statement::Update(update) => match &update.table.relation {
                TableFactor::Table { name, .. } => Some((name.clone(), "UPDATE")),
                _ => None,
            },
            Statement::Delete(delete) => match &delete.from {
                FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => {
                    match from.first().map(|from| &from.relation) {
                        Some(TableFactor::Table { name, .. }) => Some((name.clone(), "DELETE")),
                        _ => None,
                    }
                }
            },
            _ => None,
        };
        let engine = self.sql_engine();
        if let Some((name, privilege)) = &target {
            let collection = relation_name(name)?;
            engine.require_relation_privilege(&collection, privilege)?;
        }
        let mut relations = Vec::new();
        let mut collect = |name: &ObjectName| {
            relations.push(name.clone());
            ControlFlow::<()>::Continue(())
        };
        // CREATE TABLE AS reads only its query; the new table is not read.
        let _ = match statement {
            Statement::CreateTable(create) => match &create.query {
                Some(query) => sqlparser::ast::visit_relations(query, &mut collect),
                None => ControlFlow::Continue(()),
            },
            Statement::CreateView(create) => {
                sqlparser::ast::visit_relations(&create.query, &mut collect)
            }
            other => sqlparser::ast::visit_relations(other, &mut collect),
        };
        let mut ctes = Vec::new();
        let _ = sqlparser::ast::visit_statements(statement, |statement| {
            if let Statement::Query(query) = statement {
                if let Some(with) = &query.with {
                    ctes.extend(
                        with.cte_tables
                            .iter()
                            .map(|cte| cte.alias.name.value.clone()),
                    );
                }
            }
            ControlFlow::<()>::Continue(())
        });
        // UPDATE and DELETE read their target only through WHERE/RETURNING.
        let target_read = match statement {
            Statement::Update(update) => update.selection.is_some() || update.returning.is_some(),
            Statement::Delete(delete) => delete.selection.is_some() || delete.returning.is_some(),
            _ => false,
        };
        for name in relations {
            if target.as_ref().is_some_and(|(target, _)| target == &name) && !target_read {
                continue;
            }
            if name.0.len() == 1 && ctes.contains(&relation_default_alias(&name)) {
                continue;
            }
            let collection = relation_name(&name)?;
            if is_virtual_table(&collection) {
                continue;
            }
            // A table function (`FROM generate_series(1, 3)`) is no relation;
            // a missing table is reported by the plan builder.
            if load_schema(self.db_ref(), &collection)?.is_none()
                && load_view(self.db_ref(), &collection)?.is_none()
            {
                continue;
            }
            engine.require_relation_privilege(&collection, "SELECT")?;
        }
        Ok(())
    }
}
