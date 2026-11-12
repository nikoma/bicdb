//! Resolve stored view relation dependencies with lexical WITH scopes. Alias
//! names and strings are not base relations. Non-recursive CTE definitions see
//! preceding siblings, while their own name still resolves to an outer table.
use super::*;
use sqlparser::ast::{VisitMut, VisitorMut, With};

pub(super) fn relations(sql: &str) -> Result<BTreeSet<String>> {
    let mut query = parse_query(sql)?;
    let mut visitor = Relations {
        visible: Vec::new(),
        saved_with: Vec::new(),
        found: BTreeSet::new(),
    };
    if let ControlFlow::Break(error) = VisitMut::visit(&mut query, &mut visitor) {
        return Err(error);
    }
    Ok(visitor.found)
}

struct Relations {
    visible: Vec<BTreeSet<String>>,
    saved_with: Vec<Option<With>>,
    found: BTreeSet<String>,
}

impl Relations {
    fn record(&mut self, parts: &[String]) -> Result<()> {
        if parts.len() == 1
            && self
                .visible
                .last()
                .is_some_and(|names| names.contains(&parts[0]))
        {
            return Ok(());
        }
        self.found.insert(relation_name_from_parts(parts)?);
        Ok(())
    }
}

impl VisitorMut for Relations {
    type Break = SqlError;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
        let mut with = query.with.take();
        let mut visible = self.visible.last().cloned().unwrap_or_default();
        if let Some(with) = with.as_mut() {
            let names = with
                .cte_tables
                .iter()
                .map(|cte| ident_value(&cte.alias.name))
                .collect::<BTreeSet<_>>();
            if with.recursive {
                visible.extend(names.iter().cloned());
            }
            for cte in &mut with.cte_tables {
                self.visible.push(visible.clone());
                let result = VisitMut::visit(cte.query.as_mut(), self);
                self.visible.pop();
                if result.is_break() {
                    return result;
                }
                visible.insert(ident_value(&cte.alias.name));
            }
            visible.extend(names);
        }
        self.visible.push(visible);
        self.saved_with.push(with);
        let mut bodies = vec![query.body.as_ref()];
        while let Some(body) = bodies.pop() {
            match body {
                SetExpr::SetOperation { left, right, .. } => {
                    bodies.push(left);
                    bodies.push(right);
                }
                SetExpr::Table(table) => {
                    if let Some(name) = &table.table_name {
                        let mut parts = Vec::new();
                        if let Some(schema) = &table.schema_name {
                            parts.push(schema.clone());
                        }
                        parts.push(name.clone());
                        if let Err(error) = self.record(&parts) {
                            return ControlFlow::Break(error);
                        }
                    }
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
        query.with = self.saved_with.pop().expect("query scope");
        self.visible.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<SqlError> {
        if let TableFactor::Table {
            name, args: None, ..
        } = factor
        {
            if let Err(error) = self.record(&object_name_parts(name)) {
                return ControlFlow::Break(error);
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn check(sql: &str, expected: &[&str]) {
        assert_eq!(
            relations(sql).unwrap(),
            expected.iter().map(|s| s.to_string()).collect(),
            "{sql}"
        );
    }
    #[test]
    fn cte_self_reference_is_a_base_relation_only_when_nonrecursive() {
        check(
            "WITH source AS (SELECT * FROM source) SELECT * FROM source",
            &["source"],
        );
        check("WITH RECURSIVE source AS (SELECT 1 UNION ALL SELECT 1 FROM source) SELECT * FROM source", &[]);
    }
    #[test]
    fn cte_order_and_nested_scopes_do_not_hide_outer_base_tables() {
        check(
            "WITH first AS (SELECT * FROM source), source AS (SELECT 1) SELECT * FROM first",
            &["source"],
        );
        check("WITH source AS (SELECT * FROM actual), second AS (SELECT * FROM source) SELECT * FROM second", &["actual"]);
        check(
            "SELECT * FROM source WHERE EXISTS(WITH source AS (SELECT 1) SELECT * FROM source)",
            &["source"],
        );
        check(
            "WITH source AS (SELECT 1) SELECT (SELECT count(*) FROM source)",
            &[],
        );
    }
    #[test]
    fn schema_qualification_aliases_and_literals_are_not_cte_names() {
        check(
            "WITH source AS (SELECT 1) SELECT b.id FROM public.source b JOIN source x ON TRUE",
            &["source"],
        );
        check("SELECT source.id, 'source' FROM actual source", &["actual"]);
        check(
            "WITH \"Source\" AS (SELECT 1) SELECT * FROM source",
            &["source"],
        );
        check(
            "WITH \"Source\" AS (SELECT 1) SELECT * FROM \"Source\"",
            &[],
        );
    }
    #[test]
    fn table_queries_and_scalar_subqueries_are_relations() {
        check("TABLE source", &["source"]);
        check(
            "SELECT (SELECT id FROM inner_source) FROM outer_source",
            &["inner_source", "outer_source"],
        );
    }

    #[test]
    fn column_type_guard_ignores_a_shadowing_cte_but_keeps_qualified_dependencies() {
        for mode in [
            bicdb_core::StorageMode::EmbeddedMemory,
            bicdb_core::StorageMode::ServerPaged,
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(
                root.path(),
                bicdb_core::DbConfig::default().with_storage_mode(mode),
            )
            .unwrap();
            let mut session = SqlSession::new(&mut db);
            for statement in [
                "CREATE TABLE source(id INT)",
                "CREATE VIEW shadowed AS WITH source AS (SELECT 1 AS id) SELECT id FROM source",
                "ALTER TABLE source ALTER COLUMN id TYPE BIGINT",
                "CREATE TABLE actual(id INT)",
                "CREATE VIEW physical AS WITH actual AS (SELECT 1 AS id) SELECT b.id FROM public.actual b",
            ] { session.execute(statement).unwrap_or_else(|e|panic!("{statement}: {e}")); }
            assert!(session
                .execute("ALTER TABLE actual ALTER COLUMN id TYPE BIGINT")
                .is_err());
        }
    }
}
